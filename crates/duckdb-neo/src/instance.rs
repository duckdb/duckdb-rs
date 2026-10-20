//! DuckDB instances and their global configuration.

use std::{
    borrow::Borrow,
    sync::{Arc, Mutex, OnceLock},
};

use crate::{
    AsRaw, DropHandle, Factory, Result, check_api_call,
    connection::{Connection, HasFactory},
    connection_options::Config,
    environment::{Environment, EnvironmentHandle, StorageLocation},
    ffi,
    raw::RawExt,
};

/// A shared handle to a DuckDB instance.
#[derive(Debug, DropHandle)]
#[drop_handle(ffi::duckdb_v2_instance_destroy)]
pub struct InstanceHandle {
    /// The DuckDB instance handle.
    pub(crate) handle: ffi::duckdb_v2_instance_handle,

    /// The environment kept alive by this instance.
    pub(crate) _env: Arc<EnvironmentHandle>,
}

// SAFETY: an instance handle may be used and destroyed on any thread.
unsafe impl Send for InstanceHandle {}

#[derive(Debug, DropHandle)]
#[drop_handle(ffi::duckdb_v2_attach_options_destroy)]
struct AttachOptions {
    handle: ffi::duckdb_v2_attach_options_handle,
}

/// Configures startup options for an initially empty [`Instance`].
///
/// Options are validated when [`Self::build`] creates the instance.
/// Per-database options belong to [`AttachBuilder`].
///
/// ```
/// use duckdb_neo::{environment::Environment, instance::InstanceBuilder};
///
/// # fn main() -> duckdb_neo::Result<()> {
/// let env = Environment::new()?;
/// let instance = InstanceBuilder::new().option("threads", "2").build(&env)?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Default)]
#[must_use]
pub struct InstanceBuilder {
    options: Vec<(String, String)>,
}

impl InstanceBuilder {
    /// Configure an empty instance with DuckDB's default startup options.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set an instance startup option as text; later values replace earlier ones.
    pub fn option(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.options.push((key.into(), value.into()));
        self
    }

    /// Set instance startup options as text; later values replace earlier ones.
    pub fn options<K, V>(mut self, options: impl IntoIterator<Item = (K, V)>) -> Self
    where
        K: Into<String>,
        V: Into<String>,
    {
        self.options
            .extend(options.into_iter().map(|(key, value)| (key.into(), value.into())));
        self
    }

    /// Create an empty instance in an owned or borrowed environment.
    pub fn build(self, environment: impl Borrow<Environment>) -> Result<Instance> {
        let environment = environment.borrow();
        let names: Vec<ffi::duckdb_v2_str> = self.options.iter().map(|(key, _)| key.into()).collect();
        let values: Vec<ffi::duckdb_v2_str> = self.options.iter().map(|(_, value)| value.into()).collect();
        let handle = check_api_call!(
            ffi::duckdb_v2_instance_create_with_options,
            environment.handle.handle,
            names.as_ptr(),
            values.as_ptr(),
            self.options.len() as u64,
            RET
        )?;
        Ok(Instance {
            handle: Arc::new(Mutex::new(InstanceHandle {
                handle,
                _env: environment.handle.clone(),
            })),
            factory: OnceLock::new(),
        })
    }
}

/// Configures one database attachment independently of an instance.
///
/// The current default is preserved unless [`Self::make_default`] is called.
/// Options are validated when the builder is passed to [`Instance::attach`].
/// A bare [`StorageLocation`] converts into a builder with no options.
/// Startup options belong to [`InstanceBuilder`].
///
/// ```
/// use duckdb_neo::{
///     environment::{Environment, StorageLocation},
///     instance::{AttachBuilder, Instance},
///     Parameters,
/// };
///
/// # fn main() -> duckdb_neo::Result<()> {
/// let attachment = AttachBuilder::new(StorageLocation::InMemory)
///     .options([("read_only", "false")])
///     .alias("analytics")
///     .make_default();
/// let instance = Instance::new(Environment::new()?)?;
/// instance.attach(attachment)?;
/// instance.connect()?.execute("CREATE TABLE example (id INTEGER)", Parameters::None)?;
/// instance.detach("analytics")?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
#[must_use]
pub struct AttachBuilder {
    location: StorageLocation,
    options: Vec<(String, String)>,
    alias: Option<String>,
    make_default: bool,
}

impl AttachBuilder {
    /// Configure an attachment at `location` without changing the default database.
    pub fn new(location: StorageLocation) -> Self {
        Self {
            location,
            options: Vec::new(),
            alias: None,
            make_default: false,
        }
    }

    /// Set a per-database option as text; later values replace earlier ones.
    pub fn option(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.options.push((key.into(), value.into()));
        self
    }

    /// Set per-database options as text; later values replace earlier ones.
    pub fn options<K, V>(mut self, options: impl IntoIterator<Item = (K, V)>) -> Self
    where
        K: Into<String>,
        V: Into<String>,
    {
        self.options
            .extend(options.into_iter().map(|(key, value)| (key.into(), value.into())));
        self
    }

    /// Make the attached database the default for new connections only.
    pub fn make_default(mut self) -> Self {
        self.make_default = true;
        self
    }

    /// Set the attachment's name instead of deriving it from the location.
    pub fn alias(mut self, name: impl Into<String>) -> Self {
        self.alias = Some(name.into());
        self
    }
}

impl From<StorageLocation> for AttachBuilder {
    fn from(location: StorageLocation) -> Self {
        Self::new(location)
    }
}

/// An open DuckDB instance.
///
/// Connections share the instance's catalog, buffer pool, and transaction
/// manager while retaining independent session state.
///
/// # Example
/// ```
/// use duckdb_neo::{
///     FromValue,
///     connection::SettingScope,
///     environment::{Environment, StorageLocation},
/// };
///
/// # fn main() -> duckdb_neo::Result<()> {
/// let env = Environment::new()?;
/// let instance = env.open(StorageLocation::InMemory)?;
///
/// instance.config()?.set_text("threads", "2", SettingScope::Global)?;
///
/// let conn = instance.connect()?;
/// let (threads, _scope) = conn.config()?.get_value("threads")?;
/// assert_eq!(i64::from_value(&threads)?, Some(2));
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct Instance {
    // Locks only raw-handle access, not engine operations.
    pub(crate) handle: Arc<Mutex<InstanceHandle>>,
    pub(crate) factory: OnceLock<InstanceFactory>,
}

/// The factory of an instance, which can be shared across threads.
#[derive(Debug)]
pub(crate) struct InstanceFactory(Factory);

// SAFETY: an instance's factory has no client context: it knows only the built-in types and
// casts, with default settings, so there is no client state to race on.
unsafe impl Sync for InstanceFactory {}

impl AsRaw for Instance {
    type Raw = ffi::duckdb_v2_instance_handle;

    unsafe fn get_raw(&self) -> Self::Raw {
        self.handle.lock().unwrap().handle
    }
}

impl HasFactory for Instance {
    fn factory(&self) -> Result<&Factory> {
        if let Some(factory) = self.factory.get() {
            return Ok(&factory.0);
        }

        let factory = Factory::new(check_api_call!(ffi::duckdb_v2_instance_get_factory, self.raw(), RET)?);
        Ok(&self.factory.get_or_init(|| InstanceFactory(factory)).0)
    }
}

impl Instance {
    /// Create an empty instance with default startup options.
    /// Accepts either an owned or borrowed environment, which the instance keeps alive.
    pub fn new(environment: impl Borrow<Environment>) -> Result<Self> {
        InstanceBuilder::new().build(environment)
    }

    /// Attach a location or configured attachment without changing the default unless requested.
    pub fn attach(&self, attachment: impl Into<AttachBuilder>) -> Result<&Self> {
        let attachment = attachment.into();
        let options = if attachment.options.is_empty() {
            None
        } else {
            let options = AttachOptions {
                handle: check_api_call!(ffi::duckdb_v2_attach_options_create, self.raw(), RET)?,
            };
            for (key, value) in &attachment.options {
                check_api_call!(
                    ffi::duckdb_v2_attach_options_set,
                    options.handle,
                    &key.into(),
                    &value.into()
                )?;
            }
            Some(options)
        };
        let alias: Option<ffi::duckdb_v2_str> = attachment.alias.as_ref().map(Into::into);
        check_api_call!(
            ffi::duckdb_v2_instance_attach,
            self.raw(),
            alias.as_ref().map_or(std::ptr::null(), |name| name as *const _),
            &attachment.location.as_str().into(),
            options.as_ref().map_or(std::ptr::null_mut(), |options| options.handle),
            attachment.make_default
        )?;
        Ok(self)
    }

    /// Make the database attached under `name` the default for future connections.
    /// Existing connections keep their current default.
    pub fn set_default(&self, name: &str) -> Result<&Self> {
        check_api_call!(ffi::duckdb_v2_instance_set_default, self.raw(), &(name).into())?;
        Ok(self)
    }

    /// Detach the database attached as `name`, checkpointing a file database first.
    pub fn detach(&self, name: &str) -> Result<&Self> {
        check_api_call!(ffi::duckdb_v2_instance_detach, self.raw(), &name.into())?;
        Ok(self)
    }

    /// Open a new [`Connection`] to this instance.
    pub fn connect(&self) -> Result<Connection> {
        Connection::new(self)
    }

    /// Access global instance configuration; writes to the session scope return an error.
    pub fn config(&self) -> Result<Config<'_>> {
        check_api_call!(ffi::duckdb_v2_instance_get_config, self.raw(), RET).map(Config::new)
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use crate::{
        Parameters,
        environment::{Environment, StorageLocation},
        instance::{AttachBuilder, Instance},
    };

    #[test]
    fn test_instance_options() -> crate::Result<()> {
        let env = Environment::new()?;
        let instance = env.open(StorageLocation::InMemory)?;

        let mut options = vec![];

        let config = instance.config()?;

        for i in 0..config.count()? {
            options.push(config.get_by_index(i)?);
        }

        assert!(!options.is_empty());

        assert!(options.iter().any(|x| x.canonical_name().unwrap() == "schema"));

        for option in options {
            for alias in option.aliases()? {
                assert_eq!(config.get_by_name(&alias)?.canonical_name()?, option.canonical_name()?);
            }
        }

        Ok(())
    }

    #[test]
    fn test_instance_attach_options() -> crate::Result<()> {
        let env = Environment::new()?;

        let directory = tempfile::tempdir().expect("create temporary database directory");
        let location = StorageLocation::OnDisk(directory.path().join("attach.db").to_str().unwrap().to_owned());
        let instance = Instance::new(&env)?;
        instance.attach(AttachBuilder::new(location.clone()).alias("ondisk").make_default())?;
        instance.detach("ondisk")?;
        instance.attach(
            AttachBuilder::new(location)
                .alias("ro")
                .options([("read_only", "true")]),
        )?;

        instance.set_default("ro")?;

        let conn = instance.connect()?;

        let res = conn.execute("CREATE TABLE test (id INTEGER)", Parameters::None);

        assert!(res.is_err_and(|e| e.to_string().contains("read-only")));

        drop(conn);
        instance.detach("ro")?;

        Ok(())
    }
}

//! DuckDB instances and their global configuration.

use std::sync::{Arc, Mutex, OnceLock};

use crate::{
    AsRaw, DropHandle, Factory, FromRaw, Result, check_api_call,
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

/// A builder for the per-database options of one [`Instance::attach`] call.
///
/// Each `set_option` call adds one `(KEY value)` entry, mirroring the options of
/// SQL `ATTACH`. Nothing is validated until the attach itself.
#[derive(Debug, AsRaw, FromRaw, DropHandle)]
#[drop_handle(ffi::duckdb_v2_attach_options_destroy)]
pub struct AttachOptionsBuilder {
    /// The owned attach-options handle.
    pub(crate) handle: ffi::duckdb_v2_attach_options_handle,
}

impl AttachOptionsBuilder {
    /// Set one attach option, like a `(KEY value)` entry of SQL `ATTACH`.
    ///
    /// Keys are matched case-insensitively; setting the same key again replaces
    /// the previous value.
    pub fn set_option(&mut self, key: &str, value: &str) -> Result<()> {
        check_api_call!(
            ffi::duckdb_v2_attach_options_set,
            self.handle,
            &key.into(),
            &value.into()
        )
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
    /// Create an empty instance under `environment`, with no database attached yet.
    pub fn new(environment: &Environment) -> Result<Self> {
        check_api_call!(ffi::duckdb_v2_instance_create, environment.handle.handle, RET).map(|handle| Instance {
            handle: Arc::new(Mutex::new(InstanceHandle {
                handle,
                _env: environment.handle.clone(),
            })),
            factory: OnceLock::new(),
        })
    }

    /// Create an empty instance with startup options; unknown option names return an error.
    pub fn new_with_options(environment: &Environment, options: impl Into<Vec<(String, String)>>) -> Result<Self> {
        let options = options.into();

        let names: Vec<ffi::duckdb_v2_str> = options.iter().map(|v| (&v.0).into()).collect::<Vec<_>>();
        let values: Vec<ffi::duckdb_v2_str> = options.iter().map(|v| (&v.1).into()).collect::<Vec<_>>();

        check_api_call!(
            ffi::duckdb_v2_instance_create_with_options,
            environment.handle.handle,
            names.as_ptr(),
            values.as_ptr(),
            options.len() as u64,
            RET
        )
        .map(|handle| Instance {
            handle: Arc::new(Mutex::new(InstanceHandle {
                handle,
                _env: environment.handle.clone(),
            })),
            factory: OnceLock::new(),
        })
    }

    /// Return a new [`AttachOptionsBuilder`] for configuring an attach from this instance.
    pub fn get_attach_options_builder(&self) -> Result<AttachOptionsBuilder> {
        check_api_call!(ffi::duckdb_v2_attach_options_create, self.raw(), RET)
            .map(|handle| AttachOptionsBuilder { handle })
    }

    /// Attach the database at `location` under its derived name.
    ///
    /// The attached database becomes the default.
    pub fn attach(&self, location: &StorageLocation) -> Result<&Self> {
        self.attach_with_options(location, None, None, true)?;
        Ok(self)
    }

    /// Attach at `location` using optional alias and options such as `READ_ONLY`.
    /// `make_default` chooses the database for future connections only.
    pub fn attach_with_options(
        &self,
        location: &StorageLocation,
        alias: Option<&str>,
        options: Option<&AttachOptionsBuilder>,
        make_default: bool,
    ) -> Result<&Self> {
        let location = location.as_str();
        let options: ffi::duckdb_v2_attach_options_handle = options.map_or(std::ptr::null_mut(), |o| o.raw());
        let alias_str: Option<ffi::duckdb_v2_str> = alias.map(Into::into);

        check_api_call!(
            ffi::duckdb_v2_instance_attach,
            self.raw(),
            alias_str.as_ref().map_or(std::ptr::null(), |a| a as *const _),
            &location.into(),
            options,
            make_default
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
        let instance = env.instance()?;
        let mut attach_options = instance.get_attach_options_builder()?;
        let directory = tempfile::tempdir().expect("create temporary database directory");
        let location = StorageLocation::OnDisk(directory.path().join("attach.db").to_str().unwrap().to_owned());

        instance.attach_with_options(&location, Some("ondisk"), None, true)?;

        instance.detach("ondisk")?;

        attach_options.set_option("read_only", "true")?;

        // Keep a default database available after the on-disk database is detached.
        instance.attach(&StorageLocation::InMemory)?;

        instance.attach_with_options(&location, Some("ro"), Some(&attach_options), false)?;

        instance.set_default("ro")?;

        let conn = instance.connect()?;

        let res = conn.execute("CREATE TABLE test (id INTEGER)", Parameters::None);

        assert!(res.is_err_and(|e| e.to_string().contains("read-only")));

        drop(conn);
        instance.detach("ro")?;

        Ok(())
    }
}

//! Open DuckDB databases and their global configuration.

use std::{
    ops::Deref,
    sync::{Arc, Mutex},
};

use crate::{
    Result, check_api_call, check_api_call_no_err,
    connection::Connection,
    connection_options::ConfigOption,
    environment::{Environment, EnvironmentHandle, StorageLocation},
    ffi,
};

/// A shared handle to an open DuckDB database.
pub struct DatabaseHandle {
    /// The DuckDB database handle.
    pub handle: ffi::duckdb_v2_instance_handle,

    /// The environment kept alive by this database.
    pub env: Arc<Mutex<EnvironmentHandle>>,
}

impl Drop for DatabaseHandle {
    fn drop(&mut self) {
        check_api_call_no_err!(ffi::duckdb_v2_instance_destroy, &mut self.handle).unwrap();
    }
}

unsafe impl Send for DatabaseHandle {}
unsafe impl Sync for DatabaseHandle {}

/// A builder for the per-database options of one [`Instance::attach`] call.
///
/// Each `set_option` call adds one `(KEY value)` entry, mirroring the options of
/// SQL `ATTACH`. Nothing is validated until the attach itself.
pub struct AttachOptionsBuilder {
    /// The owned attach-options handle.
    pub handle: ffi::duckdb_v2_attach_options_handle,
}

impl AttachOptionsBuilder {
    /// Set one attach option, like a `(KEY value)` entry of SQL `ATTACH`.
    ///
    /// Keys are matched case-insensitively; setting the same key again replaces
    /// the previous value.
    pub fn set_option(&mut self, key: &str, value: &str) -> Result<()> {
        check_api_call!(ffi::duckdb_v2_attach_options_set, self.handle, key.into(), value.into())
    }
}

impl Drop for AttachOptionsBuilder {
    fn drop(&mut self) {
        check_api_call_no_err!(ffi::duckdb_v2_attach_options_destroy, &mut self.handle)
            .expect("Failed to destroy AttachOptionsBuilder");
    }
}

impl Deref for AttachOptionsBuilder {
    type Target = ffi::duckdb_v2_attach_options_handle;

    fn deref(&self) -> &Self::Target {
        &self.handle
    }
}

/// An open DuckDB instance.
///
/// Connections share the database's catalog, buffer pool, and transaction
/// manager while retaining independent session state.
///
/// # Example
/// ```
/// use duckdb_neo::{environment::Environment, environment::StorageLocation};
///
/// # fn main() -> duckdb_neo::Result<()> {
/// let env = Environment::new()?;
/// let db = env.open(StorageLocation::InMemory)?;
///
/// db.set_option(&"threads", "2")?;
/// assert_eq!(db.get_option("threads")?.setting()?, "2");
///
/// let conn = db.connect()?;
/// assert_eq!(conn.get_option("threads")?.setting()?, "2");
/// # Ok(())
/// # }
/// ```
pub struct Instance {
    pub(crate) handle: Arc<Mutex<DatabaseHandle>>,
}

impl Instance {
    /// Create an empty instance under `environment`, with no database attached yet.
    pub fn new(environment: &Environment) -> Result<Self> {
        Ok(Instance {
            handle: Arc::new(Mutex::new(DatabaseHandle {
                handle: check_api_call!(
                    ffi::duckdb_v2_instance_create,
                    environment.handle.lock().unwrap().handle,
                    RET
                )?,
                env: environment.handle.clone(),
            })),
        })
    }

    /// Return a new [`AttachOptionsBuilder`] for configuring an attach from this instance.
    pub fn get_attach_options_builder(&self) -> Result<AttachOptionsBuilder> {
        Ok(AttachOptionsBuilder {
            handle: check_api_call!(
                ffi::duckdb_v2_attach_options_create,
                self.handle.lock().unwrap().handle,
                RET
            )?,
        })
    }

    /// Attach the database at `location` under its derived name.
    ///
    /// The attached database does not become the default.
    pub fn attach(&self, location: &StorageLocation) -> Result<&Self> {
        self.attach_with_options(location, None, None, true)?;
        Ok(self)
    }

    /// Attach the database at `location`, controlling the attach name, options, and default status.
    ///
    /// `alias` is the name to attach under, or `None` for the name derived from
    /// the path. `options` supplies per-database options such as `READ_ONLY`.
    /// When `default` is true, the attached database becomes the default for
    /// connections created afterwards.
    pub fn attach_with_options(
        &self,
        location: &StorageLocation,
        alias: Option<String>,
        options: Option<&AttachOptionsBuilder>,
        default: bool,
    ) -> Result<()> {
        let location: String = location.into();
        let options: ffi::duckdb_v2_attach_options_handle = options.map_or(std::ptr::null_mut(), |o| **o);

        check_api_call!(
            ffi::duckdb_v2_instance_attach,
            self.handle.lock().unwrap().handle,
            (&location).into(),
            alias.as_ref().map_or(std::ptr::null_mut(), |a| &mut (a).into()),
            options,
            default
        )?;
        Ok(())
    }

    /// Make the database attached from `location` the default for new connections.
    pub fn set_default(&self, location: &StorageLocation) -> Result<&Self> {
        let location: String = location.into();
        check_api_call!(
            ffi::duckdb_v2_instance_set_default,
            self.handle.lock().unwrap().handle,
            (&location).into()
        )?;
        Ok(self)
    }

    /// Detach the database attached from `location`, checkpointing a file database first.
    pub fn detach(&self, location: &StorageLocation) -> Result<&Self> {
        let location: String = location.into();

        check_api_call!(
            ffi::duckdb_v2_instance_detach,
            self.handle.lock().unwrap().handle,
            (&location).into()
        )?;
        Ok(self)
    }

    /// Open a new [`Connection`] to this instance.
    pub fn connect(&self) -> Result<Connection> {
        Connection::new(self)
    }

    /// Return the number of registered options, excluding aliases.
    pub fn get_options_count(&self) -> Result<usize> {
        let count: u64 = check_api_call!(
            ffi::duckdb_v2_instance_get_option_count,
            self.handle.lock().unwrap().handle,
            RET
        )?;

        Ok(count as usize)
    }

    /// Return a global option by canonical name or alias.
    pub fn get_option(&self, name: &str) -> Result<ConfigOption> {
        let handle = check_api_call!(
            ffi::duckdb_v2_instance_get_option_by_name,
            self.handle.lock().unwrap().handle,
            name.into(),
            RET
        )?;

        Ok(ConfigOption { handle })
    }

    /// Return the global option at `index`.
    ///
    /// Indices remain stable while no extensions register additional options.
    /// An out-of-range index returns an error.
    pub fn get_option_by_index(&self, index: usize) -> Result<ConfigOption> {
        let handle = check_api_call!(
            ffi::duckdb_v2_instance_get_option_by_index,
            self.handle.lock().unwrap().handle,
            index as u64,
            RET
        )?;

        Ok(ConfigOption { handle })
    }

    /// Return all options registered on the database.
    pub fn get_options(&self) -> Result<Vec<ConfigOption>> {
        let count = self.get_options_count()?;
        let mut options = Vec::with_capacity(count);

        for i in 0..count {
            let option = self.get_option_by_index(i)?;
            options.push(option);
        }

        Ok(options)
    }

    /// Set an option globally, equivalent to SQL `SET GLOBAL`.
    ///
    /// Local-only options are rejected. Unknown names are retained for an
    /// extension to consume when it loads.
    pub fn set_option(&self, key: &str, value: &str) -> Result<()> {
        check_api_call!(
            ffi::duckdb_v2_instance_set_option,
            self.handle.lock().unwrap().handle,
            key.into(),
            value.into()
        )?;

        Ok(())
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
    fn test_database_options() -> crate::Result<()> {
        let env = Environment::new()?;
        let db = env.open(StorageLocation::InMemory)?;

        let options = db.get_options()?;
        assert!(!options.is_empty());

        assert!(
            options
                .iter()
                .find(|x| x.canonical_name().unwrap() == "schema")
                .is_some()
        );

        for option in options {
            let name = option.aliases()?;
            if name.is_empty() {
                continue;
            }
            println!("Aliases for option '{}': {:?}", option.canonical_name()?, name);
        }

        Ok(())
    }

    #[test]
    fn test_instance_attach_options() -> crate::Result<()> {
        let env = Environment::new()?;
        let instance = env.instance()?;
        let mut attach_options = instance.get_attach_options_builder()?;

        let location = StorageLocation::OnDisk("test_instance_attach_options.db".into());

        instance.attach(&location)?;

        instance.detach(&location)?;

        attach_options.set_option("read_only", "true")?;

        // Attach an in-memory database as well. This one can be read.
        instance.attach(&StorageLocation::InMemory)?;

        instance.attach_with_options(
            &StorageLocation::OnDisk("test_instance_attach_options.db".into()),
            Some("ro".into()),
            Some(&attach_options),
            false,
        )?;

        instance.set_default(&location)?;

        let conn = instance.connect()?;

        let res = conn.execute("CREATE TABLE test (id INTEGER)", Parameters::None);

        assert!(res.is_err_and(|e| e.to_string().contains("read-only")));

        instance.detach(&location)?;

        std::fs::remove_file("test_instance_attach_options.db").expect("Failed to remove on-disk database");

        Ok(())
    }
}

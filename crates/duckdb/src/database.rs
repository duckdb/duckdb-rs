//! Open DuckDB databases and their global configuration.

use std::{
    ops::Deref,
    sync::{Arc, Mutex},
};

use crate::{
    Result, check_api_call, check_api_call_no_err, connection::Connection, connection_options::ConfigOption,
    environment::EnvironmentHandle,
};
use libduckdb_sys as ffi;

/// A shared handle to an open DuckDB database.
pub struct DatabaseHandle {
    /// The DuckDB database handle.
    pub handle: ffi::duckdb_v2_database_handle,
}

impl Drop for DatabaseHandle {
    fn drop(&mut self) {
        check_api_call_no_err!(ffi::duckdb_v2_close, &mut self.handle).unwrap();
    }
}

unsafe impl Send for DatabaseHandle {}
unsafe impl Sync for DatabaseHandle {}

/// An open DuckDB database instance.
///
/// Connections share the database's catalog, buffer pool, and transaction
/// manager while retaining independent session state.
///
/// # Example
/// ```
/// use duckdb_rs::{environment::Environment, environment::StorageLocation};
/// use duckdb_rs::connection_options::ConfigOptionValue;
///
/// # fn main() -> duckdb_rs::Result<()> {
/// let env = Environment::new()?;
/// let db = env.open(StorageLocation::InMemory)?;
///
/// db.set_option(&ConfigOptionValue::new("threads", "2")?)?;
/// assert_eq!(db.get_option("threads")?.setting()?, "2");
///
/// let conn = db.connect()?;
/// assert_eq!(conn.get_option("threads")?.setting()?, "2");
/// # Ok(())
/// # }
/// ```
pub struct Database {
    /// The environment kept alive by this database.
    pub env: Arc<Mutex<EnvironmentHandle>>,
    /// The shared database handle.
    pub handle: Arc<Mutex<DatabaseHandle>>,
}

impl Database {
    /// Open a [`Connection`] with independent session state.
    pub fn connect(&self) -> Result<Connection> {
        let conn: ffi::duckdb_v2_connection_handle =
            check_api_call!(ffi::duckdb_v2_connect, self.handle.lock().unwrap().handle, RET)?;

        Ok(Connection {
            handle: conn,
            _db: self.handle.clone(),
        })
    }

    /// Return the number of registered options, excluding aliases.
    pub fn get_options_count(&self) -> Result<usize> {
        let count: u64 = check_api_call!(
            ffi::duckdb_v2_database_option_get_count,
            self.handle.lock().unwrap().handle,
            RET
        )?;

        Ok(count as usize)
    }

    /// Return a global option by canonical name or alias.
    pub fn get_option(&self, name: &str) -> Result<ConfigOption> {
        let handle = check_api_call!(
            ffi::duckdb_v2_database_option_get,
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
            ffi::duckdb_v2_database_option_get_by_index,
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
    pub fn set_option(&self, option: &impl Deref<Target = ffi::duckdb_v2_option_handle>) -> Result<()> {
        check_api_call!(
            ffi::duckdb_v2_database_option_set,
            self.handle.lock().unwrap().handle,
            **option
        )?;

        Ok(())
    }

    /// Set multiple options globally, equivalent to SQL `SET GLOBAL` for each.
    ///
    /// Local-only options are rejected. Unknown names are retained for an
    /// extension to consume when it loads.
    pub fn set_options(&self, options: &[impl Deref<Target = ffi::duckdb_v2_option_handle>]) -> Result<()> {
        for option in options {
            self.set_option(option)?;
        }

        Ok(())
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use crate::environment::{Environment, StorageLocation};

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
}

//! DuckDB environment creation and database storage selection.
//!
//! An [`Environment`] is the root of the handle hierarchy. It owns engine state
//! shared by the databases opened through [`Environment::open`] or
//! [`Environment::open_with_options`]. Databases keep their environment alive,
//! and connections in turn keep their database alive.
//!
//! [`StorageLocation`] selects either a transient in-memory database or a
//! persistent database file.

use std::{
    ops::Deref,
    sync::{Arc, Mutex},
};

use crate::{
    Result, check_api_call, check_api_call_no_err,
    connection_options::ConfigOption,
    database::{Database, DatabaseHandle},
    ffi,
};

/// A shared handle to a DuckDB environment.
pub struct EnvironmentHandle {
    handle: ffi::duckdb_v2_environment_handle,
}
impl EnvironmentHandle {
    fn new() -> Result<Self> {
        let handle = check_api_call!(ffi::duckdb_v2_create_environment, RET)?;
        Ok(EnvironmentHandle { handle })
    }
}
impl Drop for EnvironmentHandle {
    fn drop(&mut self) {
        check_api_call_no_err!(ffi::duckdb_v2_destroy_environment, &mut self.handle).unwrap();
    }
}
unsafe impl Send for EnvironmentHandle {}
unsafe impl Sync for EnvironmentHandle {}

/// The storage backing a DuckDB database.
///
/// # Example
/// ```rust
/// use duckdb_rs::{
///     Parameters,
///     environment::Environment,
///     environment::StorageLocation,
/// };
///
/// let env = Environment::new().expect("Failed to create environment");
/// let db = env.open(StorageLocation::InMemory).expect("Failed to open in-memory database");
/// // Alternatively
/// let path = std::env::temp_dir().join(format!("duckdb-rs-{}.duckdb", std::process::id()));
/// let db = env.open(StorageLocation::OnDisk(path.to_string_lossy().into_owned())).expect("Failed to open on-disk database");
/// drop(db);
/// std::fs::remove_file(path).expect("Failed to remove on-disk database");
/// ```
pub enum StorageLocation {
    /// A transient database that lives only in memory (`:memory:`).
    InMemory,
    /// A persistent database backed by the file at the given path.
    OnDisk(String),
}

impl From<StorageLocation> for String {
    fn from(location: StorageLocation) -> Self {
        match location {
            StorageLocation::InMemory => ":memory:".to_string(),
            StorageLocation::OnDisk(path) => path,
        }
    }
}

/// The root object used to open DuckDB databases.
///
/// An environment owns shared engine state and remains alive while any
/// database opened through it exists.
///
/// # Example
/// ```rust
/// use duckdb_rs::{Parameters, environment::Environment, environment::StorageLocation};
///
/// let env = Environment::new().expect("Failed to create environment");
/// let db = env.open(StorageLocation::InMemory).expect("Failed to open database");
/// let conn = db.connect().expect("Failed to connect");
///
/// let mut statements = conn.parse("SELECT 42").expect("Failed to parse");
/// let stmt = statements.next().expect("Expected a statement").expect("Invalid statement");
///
/// for chunk in conn
///     .query(stmt, Parameters::None)
///     .expect("Failed to query")
/// {
///     let chunk = chunk.expect("Failed to fetch chunk");
///     println!("Fetched {} row(s)", chunk.row_count().expect("Failed to read size"));
/// }
/// ```
pub struct Environment {
    handle: Arc<Mutex<EnvironmentHandle>>,
}

impl Environment {
    /// Create a new DuckDB environment.
    pub fn new() -> Result<Self> {
        let handle = EnvironmentHandle::new()?;
        Ok(Environment {
            handle: Arc::new(Mutex::new(handle)),
        })
    }

    /// Return the number of open databases.
    pub fn get_database_count(&self) -> Result<usize> {
        let count: ffi::idx_t = check_api_call!(
            ffi::duckdb_v2_environment_database_count,
            self.handle.lock().unwrap().handle,
            RET
        )?;
        Ok(count as usize)
    }

    /// Open a database with default configuration at the selected storage location.
    ///
    /// Each in-memory location creates a fresh database. Opening the same
    /// on-disk path twice under one environment returns a resource-in-use error.
    pub fn open(&self, path: StorageLocation) -> Result<Database> {
        self.open_with_options(path, &[] as &[ConfigOption])
    }

    /// Open a database with configuration options applied during construction.
    ///
    /// Use this for pre-open settings and storage options. The option handles
    /// are borrowed and remain owned by the caller.
    pub fn open_with_options(
        &self,
        path: StorageLocation,
        options: &[impl Deref<Target = ffi::duckdb_v2_option_handle>],
    ) -> Result<Database> {
        let path: String = path.into();
        let mut option_handles: Vec<ffi::duckdb_v2_option_handle> = options.iter().map(|opt| **opt).collect();

        let handle: ffi::duckdb_v2_database_handle = check_api_call!(
            ffi::duckdb_v2_open,
            self.handle.lock().unwrap().handle,
            (&path).into(),
            option_handles.as_mut_ptr(),
            options.len() as u64,
            RET
        )?;

        Ok(Database {
            handle: Arc::new(Mutex::new(DatabaseHandle {
                handle,
                env: self.handle.clone(),
            })),
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        connection_options::ConfigOptionValue,
        environment::{Environment, StorageLocation},
    };

    #[test]
    fn test_open_database_options() -> crate::Result<()> {
        let env = Environment::new()?;

        let option = ConfigOptionValue::new("memory_limit", "200MB")?;

        let db = env.open_with_options(StorageLocation::InMemory, &[option])?;

        assert_eq!(db.get_option("memory_limit")?.setting()?, "190.7 MiB".to_string());
        Ok(())
    }
}

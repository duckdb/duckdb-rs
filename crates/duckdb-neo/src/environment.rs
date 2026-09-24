//! DuckDB environment creation and database storage selection.
//!
//! An [`Environment`] is the root of the handle hierarchy. It owns engine state
//! shared by the databases opened through [`Environment::open`] or
//! [`Environment::instance`]. Databases keep their environment alive,
//! and connections in turn keep their database alive.
//!
//! [`StorageLocation`] selects either a transient in-memory database or a
//! persistent database file.

use std::sync::{Arc, Mutex};

use crate::{Result, check_api_call, check_api_call_no_err, database::Instance, ffi};

/// A shared handle to a DuckDB environment.
pub struct EnvironmentHandle {
    pub(crate) handle: ffi::duckdb_v2_environment_handle,
}
impl EnvironmentHandle {
    fn new() -> Result<Self> {
        let handle = check_api_call!(ffi::duckdb_v2_environment_create, RET)?;
        Ok(EnvironmentHandle { handle })
    }
}
impl Drop for EnvironmentHandle {
    fn drop(&mut self) {
        check_api_call_no_err!(ffi::duckdb_v2_environment_destroy, &mut self.handle).unwrap();
    }
}
// SAFETY: exposes only the raw handle, which is always used behind `Arc<Mutex<_>>`; the
// environment's instance count is atomic.
unsafe impl Send for EnvironmentHandle {}
unsafe impl Sync for EnvironmentHandle {}

/// The storage backing a DuckDB database.
///
/// # Example
/// ```rust
/// use duckdb_neo::{
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
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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

impl From<&StorageLocation> for String {
    fn from(location: &StorageLocation) -> Self {
        match location {
            StorageLocation::InMemory => ":memory:".to_string(),
            StorageLocation::OnDisk(path) => path.clone(),
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
/// use duckdb_neo::{Parameters, environment::Environment, environment::StorageLocation};
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
    pub(crate) handle: Arc<Mutex<EnvironmentHandle>>,
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
            ffi::duckdb_v2_environment_get_instance_count,
            self.handle.lock().unwrap().handle,
            RET
        )?;
        Ok(count as usize)
    }

    /// Create an instance and attach a database located at `path`.
    /// For more fine grained control, use [`Self::instance`] and the [`Instance`] API directly.
    pub fn open(&self, path: StorageLocation) -> Result<Instance> {
        let db = Instance::new(self)?;
        db.attach(&path)?;
        Ok(db)
    }

    /// Create a new instance of the DuckDB engine without attaching any database.
    pub fn instance(&self) -> Result<Instance> {
        Instance::new(self)
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use crate::environment::{Environment, StorageLocation};

    #[test]
    fn test_open_database_options() -> crate::Result<()> {
        let env = Environment::new()?;

        let db = env.open(StorageLocation::InMemory)?;

        db.set_option("memory_limit", "200MB")?;

        assert_eq!(db.get_option("memory_limit")?.setting()?, "190.7 MiB".to_string());
        Ok(())
    }
}

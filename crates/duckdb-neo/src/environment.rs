//! DuckDB environment creation and database storage selection.
//!
//! An [`Environment`] is the root of the handle hierarchy. It owns engine state
//! shared by the databases opened through [`Environment::open`] or
//! [`Environment::instance`]. Databases keep their environment alive,
//! and connections in turn keep their database alive.
//!
//! [`StorageLocation`] selects either a transient in-memory database or a
//! persistent database file.

use std::sync::Arc;

use crate::{DropHandle, Result, check_api_call, database::Instance, ffi};

/// A shared handle to a DuckDB environment.
#[derive(Debug, DropHandle)]
#[drop_handle(ffi::duckdb_v2_environment_destroy)]
pub struct EnvironmentHandle {
    pub(crate) handle: ffi::duckdb_v2_environment_handle,
}
impl EnvironmentHandle {
    fn new() -> Result<Self> {
        check_api_call!(ffi::duckdb_v2_environment_create, RET).map(|handle| EnvironmentHandle { handle })
    }
}
// SAFETY: the C API supports concurrent instance creation and uses an atomic instance count.
unsafe impl Send for EnvironmentHandle {}
// SAFETY: shared access only invokes those thread-safe C API operations.
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
/// let db = env.open(StorageLocation::OnDisk(path.to_str().expect("Database path must be UTF-8").to_owned())).expect("Failed to open on-disk database");
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

impl StorageLocation {
    /// Borrow the database path, or `":memory:"` for an in-memory database.
    pub fn as_str(&self) -> &str {
        match self {
            Self::InMemory => ":memory:",
            Self::OnDisk(path) => path,
        }
    }
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
        location.as_str().to_owned()
    }
}

/// The root object used to open DuckDB databases.
///
/// An environment owns shared engine state and remains alive while any
/// database opened through it exists.
///
/// File-conflict detection is per environment: a file can be opened only once across its instances.
/// Applications should normally share one environment; separate environments cannot detect those conflicts.
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
/// let mut result = conn.query(stmt, Parameters::None).expect("Failed to query");
/// while let Some(chunk) = result.next_chunk().expect("Failed to fetch chunk") {
///     println!("Fetched {} row(s)", chunk.row_count().expect("Failed to read size"));
/// }
/// ```
#[derive(Debug)]
pub struct Environment {
    pub(crate) handle: Arc<EnvironmentHandle>,
}

impl Environment {
    /// Create a new DuckDB environment.
    pub fn new() -> Result<Self> {
        let handle = EnvironmentHandle::new()?;
        Ok(Environment {
            handle: Arc::new(handle),
        })
    }

    /// Return a diagnostic snapshot of the number of live instances, not attached databases.
    pub fn instance_count(&self) -> Result<usize> {
        check_api_call!(ffi::duckdb_v2_environment_get_instance_count, self.handle.handle, RET).map(|x| x as usize)
    }

    /// Return the live instance count; prefer [`Self::instance_count`].
    pub fn get_database_count(&self) -> Result<usize> {
        self.instance_count()
    }

    /// Create an instance and attach a database at `location`.
    /// For more fine grained control, use [`Self::instance`] and the [`Instance`] API directly.
    pub fn open(&self, location: StorageLocation) -> Result<Instance> {
        let db = Instance::new(self)?;
        db.attach(&location)?;
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
    use crate::{
        FromValue,
        connection::SettingScope,
        environment::{Environment, StorageLocation},
    };

    #[test]
    fn test_storage_location_as_str() {
        assert_eq!(StorageLocation::InMemory.as_str(), ":memory:");
        let location = StorageLocation::OnDisk("database.duckdb".to_owned());
        assert_eq!(location.as_str(), "database.duckdb");
        assert_eq!(String::from(&location), location.as_str());
    }

    #[test]
    fn test_instance_count() -> crate::Result<()> {
        let env = Environment::new()?;
        assert_eq!(env.instance_count()?, 0);
        let instance = env.instance()?;
        assert_eq!(env.instance_count()?, 1);
        instance.attach(&StorageLocation::InMemory)?;
        assert_eq!(env.instance_count()?, 1);
        drop(instance);
        assert_eq!(env.instance_count()?, 0);
        Ok(())
    }

    #[test]
    fn test_concurrent_instance_creation() -> crate::Result<()> {
        let env = Environment::new()?;
        let (first, second) = std::thread::scope(|scope| {
            let first = scope.spawn(|| env.instance());
            let second = scope.spawn(|| env.instance());
            (first.join().unwrap(), second.join().unwrap())
        });
        let (first, second) = (first?, second?);
        assert_eq!(env.instance_count()?, 2);
        drop(env);
        first.attach(&StorageLocation::InMemory)?;
        second.attach(&StorageLocation::InMemory)?;
        Ok(())
    }

    #[test]
    fn test_open_database_options() -> crate::Result<()> {
        let env = Environment::new()?;

        let db = env.open(StorageLocation::InMemory)?;

        db.config()?.set_text("memory_limit", "200MB", SettingScope::Global)?;

        let (value, _scope) = db.config()?.get_value("memory_limit")?;

        let setting = String::from_value(&value)?;

        assert_eq!(setting.unwrap(), "190.7 MiB".to_string());
        Ok(())
    }
}

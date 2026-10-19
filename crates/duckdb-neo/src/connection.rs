//! Database sessions, SQL execution, and connection-scoped operations.
//!
//! Create a [`Connection`] with [`crate::database::Instance::connect`]. Each
//! connection has independent settings and transaction state while sharing its
//! database's catalog and storage. SQL can be parsed into
//! [`crate::statement::Statement`] values, executed for a changed-row count, or
//! queried as a lazy [`crate::query_result::QueryResult`]. A connection supports
//! one live query result at a time.
//!
//! This module also provides option inspection and mutation, query
//! interruption, and [`Context`], the non-owning connection-like handle passed
//! to extension callbacks.

use std::{
    cell::{Cell, OnceCell},
    marker::PhantomData,
    sync::{Arc, Mutex, Weak},
};

use crate::{
    AsRaw, DropHandle, FromRaw, Parameters, Result,
    builder_helpers::ffi_enum_redeclaration,
    connection_options::Config,
    database::{DatabaseHandle, Instance},
    error::{Error, check_api_call},
    ffi,
    logical_type::{LogicalType, LogicalTypeID},
    query_result::{ArrowQueryResult, QueryResult},
    raw::RawExt,
    statement::{Statement, Statements},
    value::{self, Value, ValueInput},
};

ffi_enum_redeclaration! {
    /// Destination scope for a connection-side configuration option write.
    pub enum SettingScope <- ffi::DUCKDB_V2_SETTING_SCOPE {
        /// Resolve the destination from the option's target scope.
        Default = DUCKDB_V2_SETTING_SCOPE_DEFAULT,
        /// Write through to the database, making the setting visible to all connections.
        Global = DUCKDB_V2_SETTING_SCOPE_GLOBAL,
        /// Write to this connection's session only.
        Session = DUCKDB_V2_SETTING_SCOPE_SESSION
    }
}

/// A single statement accepted by [`Connection::execute`] and [`Connection::query`].
///
/// Implemented for SQL text, [`Statements`], and [`Statement`]. SQL text and
/// statement collections must contain exactly one statement.
pub trait IntoStatement<'a> {
    /// Convert into a single statement, parsing with `conn` if needed.
    fn into_statement(self, conn: &Connection) -> Result<Statement<'a>>;
}

impl<'a> IntoStatement<'a> for &str {
    fn into_statement(self, conn: &Connection) -> Result<Statement<'a>> {
        Statements::parse(conn, self)?.into_statement(conn)
    }
}

impl<'a> IntoStatement<'a> for Statements {
    fn into_statement(mut self, _conn: &Connection) -> Result<Statement<'a>> {
        let statement = self
            .next()
            .ok_or(Error::api_error("No statements found in SQL string"))??;

        if self.next().is_some() {
            return Err(Error::invalid_input("Multiple statements found in SQL string"));
        }

        Ok(statement)
    }
}

impl<'a, 'b: 'a> IntoStatement<'a> for Statement<'b> {
    fn into_statement(self, _conn: &Connection) -> Result<Statement<'a>> {
        Ok(self)
    }
}

/// A handle that allows interrupting long-running queries. Can be used from other threads.
#[derive(Debug)]
pub struct InterruptHandle {
    conn: Weak<InnerConnection>,
}

impl InterruptHandle {
    /// Request cancellation of the active query, or do nothing if idle.
    ///
    /// Does nothing once the connection is closed.
    pub fn interrupt(&self) -> Result<()> {
        // Upgrade once: the connection may close between a liveness check and the call.
        match self.conn.upgrade() {
            Some(conn) => check_api_call!(ffi::duckdb_v2_connection_interrupt, conn.handle),
            None => Ok(()),
        }
    }

    /// Return whether the connection is still open.
    pub fn is_alive(&self) -> bool {
        self.conn.strong_count() > 0
    }
}

#[derive(Debug, DropHandle)]
#[drop_handle(ffi::duckdb_v2_connection_destroy)]
pub(crate) struct InnerConnection {
    pub handle: ffi::duckdb_v2_connection_handle,
}

// SAFETY: `Connection` is `!Sync`, so the only concurrent calls on this handle come from
// `InterruptHandle` (an atomic store) and `QueryProgressTracker` (atomic progress reads).
unsafe impl Send for InnerConnection {}
unsafe impl Sync for InnerConnection {}

/// The scope values, types, data chunks and column data collections are created in.
///
/// A connection's or context's factory is thread-bound, since it reads that client's settings:
///
/// ```compile_fail,E0277
/// fn assert_sync<T: Sync>() {}
/// assert_sync::<duckdb_neo::connection::Factory>();
/// ```
#[derive(Debug, AsRaw, FromRaw)]
pub struct Factory {
    pub(crate) handle: ffi::duckdb_v2_factory_handle,
    // Not `Sync`: casts read the client's settings while `SET` writes them, neither under a lock.
    _not_sync: PhantomData<Cell<()>>,
}

unsafe impl Send for Factory {}

impl Factory {
    pub(crate) fn new(handle: ffi::duckdb_v2_factory_handle) -> Self {
        Factory {
            handle,
            _not_sync: PhantomData,
        }
    }
}

pub trait HasFactory {
    fn factory(&self) -> Result<&Factory>;
}

impl HasFactory for Factory {
    fn factory(&self) -> Result<&Factory> {
        Ok(self)
    }
}

impl<T: HasFactory + ?Sized> HasFactory for &T {
    fn factory(&self) -> Result<&Factory> {
        (**self).factory()
    }
}

/// A session connected to a DuckDB database.
///
/// Connections have independent session settings and transactions while
/// sharing their database's catalog and storage.
///
/// A connection can be moved to another thread but not shared between threads;
/// use [`InterruptHandle`] or [`QueryProgressTracker`](crate::query_progress::QueryProgressTracker)
/// from other threads.
#[derive(Debug)]
pub struct Connection {
    pub(crate) _db: Arc<Mutex<DatabaseHandle>>,
    pub(crate) inner: Arc<InnerConnection>,
    // Not `Sync`: settings and connection-level registrations are not locked on the C side.
    pub(crate) _not_sync: PhantomData<Cell<()>>,

    pub(crate) factory: OnceCell<Factory>,
}

impl Connection {
    pub(crate) fn new(db: &Instance) -> Result<Self> {
        check_api_call!(ffi::duckdb_v2_connection_create, db.handle.lock().unwrap().handle, RET).map(|handle| {
            Connection {
                inner: Arc::new(InnerConnection { handle }),
                _db: db.handle.clone(),
                _not_sync: PhantomData,
                factory: OnceCell::new(),
            }
        })
    }

    /// Parse SQL into an iterator over its statements.
    pub fn parse(&self, query: impl AsRef<str>) -> Result<Statements> {
        Statements::parse(self, query)
    }

    pub fn config(&self) -> Result<Config<'_>> {
        check_api_call!(ffi::duckdb_v2_connection_get_config, self.raw(), RET).map(Config::new)
    }

    /// Execute a single statement to completion and return its changed-row count.
    ///
    /// Row-producing output is discarded. Results that do not report changed
    /// rows return zero.
    pub fn execute<'a>(&'a self, stmt: impl IntoStatement<'a>, params: Parameters<'_>) -> Result<usize> {
        let mut query_result = self.query(stmt, params)?;
        query_result.drain()
    }

    /// Start a single statement and return its lazy, streaming result.
    ///
    /// Execution advances as the result is stepped or iterated. The connection
    /// cannot start another statement while that result remains live.
    pub fn query<'a>(&'a self, stmt: impl IntoStatement<'a>, params: Parameters<'_>) -> Result<QueryResult<'a>> {
        stmt.into_statement(self)?.query(self, params)
    }

    /// Start a single statement and return its lazy, streaming Arrow result.
    ///
    /// Arrays hold at most `batch_size` rows; `None` or `Some(0)` selects
    /// DuckDB's default of 131,072. The connection cannot start another
    /// statement while that result remains live.
    pub fn arrow_query<'a>(
        &'a self,
        stmt: impl IntoStatement<'a>,
        params: Parameters<'_>,
        batch_size: Option<usize>,
    ) -> Result<ArrowQueryResult<'a>> {
        stmt.into_statement(self)?.arrow_query(self, params, batch_size)
    }

    /// Request cancellation of the active query, or do nothing if idle.
    pub fn interrupt_query(&self) -> Result<()> {
        check_api_call!(ffi::duckdb_v2_connection_interrupt, self.inner.handle)
    }

    /// Return an [`InterruptHandle`] that can cancel this connection's active query from another thread.
    pub fn interrupt_handle(&self) -> InterruptHandle {
        InterruptHandle {
            conn: Arc::downgrade(&self.inner),
        }
    }
}

impl HasFactory for Connection {
    fn factory(&self) -> Result<&Factory> {
        if let Some(factory) = self.factory.get() {
            return Ok(factory);
        }

        let factory = Factory::new(check_api_call!(ffi::duckdb_v2_connection_get_factory, self.raw(), RET)?);
        Ok(self.factory.get_or_init(|| factory))
    }
}

// SAFETY: a `ClientContext` has no thread affinity; it is used by one thread at a time.
unsafe impl Send for Connection {}

impl AsRaw for Connection {
    type Raw = ffi::duckdb_v2_connection_handle;

    unsafe fn get_raw(&self) -> Self::Raw {
        self.inner.handle
    }
}

/// Type and value operations available through a connection or callback context.
// Sealed through the crate-private supertrait; only crate types implement it.
#[allow(private_bounds)]
pub trait FFILink: HasFactory {
    /// Create a logical type using this connection or context.
    fn logical_type_create(&self, name: &str, parameters: Parameters<'_>) -> Result<LogicalType>;

    /// Create a logical type from its primitive ID using this connection or context.
    fn logical_type_create_from_id(&self, type_id: LogicalTypeID, parameters: Parameters<'_>) -> Result<LogicalType>;

    /// Cast a value using this connection or context.
    fn value_cast(&self, value: &Value, target_type: LogicalType) -> Result<Value>;

    #[doc(hidden)]
    fn create_value(&self, input: ValueInput<'_>) -> Result<Value>;
}

impl FFILink for Connection {
    fn logical_type_create(&self, name: &str, parameters: Parameters<'_>) -> Result<LogicalType> {
        LogicalType::create(self, name, parameters)
    }

    fn logical_type_create_from_id(&self, type_id: LogicalTypeID, parameters: Parameters<'_>) -> Result<LogicalType> {
        LogicalType::create_from_id(self, type_id, parameters)
    }

    fn value_cast(&self, value: &Value, target_type: LogicalType) -> Result<Value> {
        value.cast(self, target_type)
    }

    fn create_value(&self, input: ValueInput<'_>) -> Result<Value> {
        value::create(self.factory()?, input)
    }
}

/// A non-owning handle to the extension being loaded, available during an
/// extension load callback. Registering through it makes the item visible to
/// every connection on that database.
///
/// Wrapping a handle with [`FromRaw::from_raw`] requires the handle of the
/// extension currently being loaded. The `Extension`, and anything registered
/// through it, must not be used after the extension's entry point returns.
#[derive(AsRaw, FromRaw)]
#[repr(transparent)]
#[derive(Debug)]
pub struct Extension(pub(crate) ffi::duckdb_v2_extension_handle);

/// A non-owning DuckDB context supplied for the duration of a callback.
///
/// This wrapper does not own the underlying handle and must not outlive the
/// callback invocation, including when wrapped with [`FromRaw::from_raw`].
#[derive(AsRaw, FromRaw, Debug)]
pub struct Context {
    pub(crate) handle: ffi::duckdb_v2_context_handle,
    pub(crate) factory: OnceCell<Factory>,
}

impl Context {
    pub fn config(&self) -> Result<Config<'_>> {
        check_api_call!(ffi::duckdb_v2_context_get_config, self.raw(), RET).map(Config::new)
    }
}

impl HasFactory for Context {
    fn factory(&self) -> Result<&Factory> {
        if let Some(factory) = self.factory.get() {
            return Ok(factory);
        }

        let factory = Factory::new(check_api_call!(ffi::duckdb_v2_context_get_factory, self.raw(), RET)?);
        Ok(self.factory.get_or_init(|| factory))
    }
}

impl FFILink for Context {
    fn logical_type_create(&self, name: &str, parameters: Parameters<'_>) -> Result<LogicalType> {
        LogicalType::create(self, name, parameters)
    }

    fn logical_type_create_from_id(&self, type_id: LogicalTypeID, parameters: Parameters<'_>) -> Result<LogicalType> {
        LogicalType::create_from_id(self, type_id, parameters)
    }

    fn value_cast(&self, value: &Value, target_type: LogicalType) -> Result<Value> {
        value.cast(self, target_type)
    }

    fn create_value(&self, input: ValueInput<'_>) -> Result<Value> {
        value::create(self.factory()?, input)
    }
}

impl<T: FFILink + ?Sized> FFILink for &T {
    /// Construct a logical type from a name and value parameters.
    fn logical_type_create(&self, name: &str, parameters: Parameters<'_>) -> Result<LogicalType> {
        (*self).logical_type_create(name, parameters)
    }

    fn logical_type_create_from_id(&self, type_id: LogicalTypeID, parameters: Parameters<'_>) -> Result<LogicalType> {
        (*self).logical_type_create_from_id(type_id, parameters)
    }

    fn value_cast(&self, value: &Value, target_type: LogicalType) -> Result<Value> {
        (*self).value_cast(value, target_type)
    }

    fn create_value(&self, input: ValueInput<'_>) -> Result<Value> {
        (*self).create_value(input)
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
    fn test_connection_execute() -> crate::Result<()> {
        let conn = Environment::new()?.open(StorageLocation::InMemory)?.connect()?;

        conn.execute("CREATE TABLE test(x INTEGER)", Parameters::None)?;

        let count = conn.execute(
            "INSERT INTO test FROM range(0,10000) as x where x % $1 = 0",
            Parameters::positional(&[&10]),
        )?;

        assert_eq!(count, 1000);

        Ok(())
    }

    #[test]
    fn test_connection_query() -> crate::Result<()> {
        let conn = Environment::new()?.open(StorageLocation::InMemory)?.connect()?;

        let chunk = conn
            .query(
                "SELECT $1::INTEGER WHERE $2 = 'hello'",
                Parameters::positional(&[&10_i32, &"hello"]),
            )?
            .next_chunk()?
            .unwrap();
        assert_eq!(chunk.get_vector_at::<i32>(0)?.get(0)?, Some(&10));

        conn.execute("SELECT $1", Parameters::positional(&[&(42_i32, "duck")]))?;

        let mut result = conn.query(
            "SELECT * FROM range(0, 20000) as x where x % $mod = 0",
            Parameters::named(&[("mod", &5)]),
        )?;

        let mut i = 0;

        while let Some(chunk) = result.next_chunk()? {
            let vector = chunk.get_vector_at::<i64>(0)?;

            for value in vector.iter()? {
                assert_eq!(value, Some(&(i as i64 * 5)));
                i += 1;
            }
        }

        Ok(())
    }

    #[test]
    fn test_connection_rejects_multiple_statements() -> crate::Result<()> {
        let conn = Environment::new()?.open(StorageLocation::InMemory)?.connect()?;

        assert!(conn.execute("SELECT 1; SELECT 2", Parameters::None).is_err());
        assert!(conn.query("SELECT 1; SELECT 2", Parameters::None).is_err());
        assert!(conn.parse("SELECT '\0'").is_err());

        Ok(())
    }

    #[test]
    fn test_connection_query_parsed_statements() -> crate::Result<()> {
        let env = Environment::new()?;
        let db = env.open(StorageLocation::InMemory)?;
        let conn = db.connect()?;

        let statements = conn.parse("SELECT 42")?;
        let mut result = conn.query(statements, Parameters::None)?;
        let chunk = result.next_chunk()?.unwrap();

        assert_eq!(chunk.get_vector_at::<i32>(0)?.get(0)?, Some(&42));

        Ok(())
    }

    #[test]
    fn test_interrupt_after_connection_close() -> crate::Result<()> {
        let env = Environment::new()?;
        let db = env.open(StorageLocation::InMemory)?;
        let conn = db.connect()?;

        let interrupt = conn.interrupt_handle();

        let statements = conn.parse("SELECT * FROM RANGE(0, 10_000)")?;

        let mut result = conn.query(statements, Parameters::None)?;

        let _ = result.next_chunk()?.unwrap();

        assert!(interrupt.is_alive());

        interrupt.interrupt()?;

        assert!(result.next_chunk().is_err());

        drop(result);
        drop(conn);

        assert!(!interrupt.is_alive());

        interrupt.interrupt()?;

        Ok(())
    }
}

//! Instance sessions, SQL execution, and connection-scoped operations.
//!
//! Create a [`Connection`] with [`crate::instance::Instance::connect`]. Each
//! connection has independent settings and transaction state while sharing its
//! instance's catalog and storage. SQL can be parsed into
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
    error::{Error, check_api_call},
    ffi,
    instance::{Instance, InstanceHandle},
    logical_type::{LogicalType, LogicalTypeID},
    query_result::{ArrowQueryResult, QueryResult},
    raw::RawExt,
    statement::{PreparedStatement, Statement, Statements},
    value::{self, Value, ValueInput},
};

ffi_enum_redeclaration! {
    /// The scope an option is written at or read from.
    pub enum SettingScope <- ffi::DUCKDB_V2_SETTING_SCOPE {
        /// Write only: resolve from the option's [`default_scope`](crate::connection_options::ConfigOption::default_scope).
        Default = DUCKDB_V2_SETTING_SCOPE_DEFAULT,
        /// Write through to the instance, making the setting visible to all connections.
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

impl<'a> IntoStatement<'a> for String {
    fn into_statement(self, conn: &Connection) -> Result<Statement<'a>> {
        self.as_str().into_statement(conn)
    }
}

impl<'a> IntoStatement<'a> for &String {
    fn into_statement(self, conn: &Connection) -> Result<Statement<'a>> {
        self.as_str().into_statement(conn)
    }
}

impl<'a> IntoStatement<'a> for Statements {
    fn into_statement(mut self, _conn: &Connection) -> Result<Statement<'a>> {
        let statement = self
            .next()
            .ok_or_else(|| Error::invalid_input("No statements found in SQL string"))??;

        match self.next() {
            Some(Err(error)) => return Err(error),
            Some(Ok(_)) => return Err(Error::invalid_input("Multiple statements found in SQL string")),
            None => {}
        }

        Ok(statement)
    }
}

impl<'a, 'b: 'a> IntoStatement<'a> for Statement<'b> {
    fn into_statement(self, _conn: &Connection) -> Result<Statement<'a>> {
        Ok(self)
    }
}

/// A cross-thread query interruption handle; dependent objects can keep the connection open.
#[derive(Debug, Clone)]
pub struct InterruptHandle {
    conn: Weak<InnerConnection>,
}

impl InterruptHandle {
    /// Request cancellation, or do nothing if idle or the connection and its dependent objects are gone.
    pub fn interrupt(&self) -> Result<()> {
        // Upgrade once: the connection may close between a liveness check and the call.
        match self.conn.upgrade() {
            Some(conn) => check_api_call!(ffi::duckdb_v2_connection_interrupt, conn.handle),
            None => Ok(()),
        }
    }

    /// Return whether the connection or an object created through it still keeps the session open.
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

/// Provides the factory used to create types, values, and data containers.
pub trait HasFactory {
    /// Return this object's factory, initializing it if needed.
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

/// A session connected to a DuckDB instance.
///
/// Connections have independent session settings and transactions while
/// sharing their instance's catalog and storage.
///
/// A connection can be moved to another thread but not shared between threads;
/// use [`InterruptHandle`] or [`QueryProgressTracker`](crate::query_progress::QueryProgressTracker)
/// from other threads.
///
/// ```compile_fail,E0277
/// fn assert_sync<T: Sync>() {}
/// assert_sync::<duckdb_neo::connection::Connection>();
/// ```
#[derive(Debug)]
pub struct Connection {
    pub(crate) inner: Arc<InnerConnection>,
    pub(crate) instance: Arc<Mutex<InstanceHandle>>,
    // Not `Sync`: settings and connection-level registrations are not locked on the C side.
    pub(crate) _not_sync: PhantomData<Cell<()>>,

    pub(crate) factory: OnceCell<Factory>,
}

impl Connection {
    pub(crate) fn new(instance: &Instance) -> Result<Self> {
        check_api_call!(ffi::duckdb_v2_connection_create, instance.raw(), RET).map(|handle| Connection {
            inner: Arc::new(InnerConnection { handle }),
            instance: instance.handle.clone(),
            _not_sync: PhantomData,
            factory: OnceCell::new(),
        })
    }

    /// Parse SQL into an iterator over its statements.
    pub fn parse(&self, query: impl AsRef<str>) -> Result<Statements> {
        Statements::parse(self, query)
    }

    /// Prepare a single statement; `require_cacheable` rejects plans that cannot be reused.
    pub fn prepare<'a>(
        &'a self,
        stmt: impl IntoStatement<'a>,
        require_cacheable: bool,
    ) -> Result<PreparedStatement<'a>> {
        stmt.into_statement(self)?.prepare(self, require_cacheable)
    }

    /// Access this connection's session and instance configuration.
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

impl AsRaw for Connection {
    type Raw = ffi::duckdb_v2_connection_handle;

    unsafe fn get_raw(&self) -> Self::Raw {
        self.inner.handle
    }
}

/// Type and value operations available through a connection or callback context.
pub trait FFILink: HasFactory {
    /// Create a logical type using this connection or context.
    fn logical_type_create(&self, name: &str, parameters: Parameters<'_>) -> Result<LogicalType> {
        LogicalType::create(self, name, parameters)
    }

    /// Create a logical type from its primitive ID using this connection or context.
    fn logical_type_create_from_id(&self, type_id: LogicalTypeID, parameters: Parameters<'_>) -> Result<LogicalType> {
        LogicalType::create_from_id(self, type_id, parameters)
    }

    /// Cast a value using this connection or context.
    fn value_cast(&self, value: &Value, target_type: &LogicalType) -> Result<Value> {
        value.cast(self, target_type)
    }

    #[doc(hidden)]
    fn create_value(&self, input: ValueInput<'_>) -> Result<Value> {
        value::create(self.factory()?, input)
    }
}

impl FFILink for Connection {}

/// A non-owning handle valid only during the extension's load callback.
/// [`FromRaw::from_raw`] requires the handle of the extension currently being loaded.
#[derive(AsRaw, FromRaw, Debug)]
#[repr(transparent)]
pub struct Extension(pub(crate) ffi::duckdb_v2_extension_handle);

/// A non-owning DuckDB context supplied for the duration of a callback invocation.
/// The handle must not outlive the invocation, including when wrapped with [`FromRaw::from_raw`].
///
/// Files opened and chunks copied through it may be kept in bind or init data for later callbacks,
/// but must not outlive the connection, for example by being stored in a `static`.
#[derive(AsRaw, FromRaw, Debug)]
pub struct Context {
    pub(crate) handle: ffi::duckdb_v2_context_handle,
    pub(crate) factory: OnceCell<Factory>,
}

impl Context {
    /// Access the callback's session and instance configuration.
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

impl FFILink for Context {}

impl<T: FFILink + ?Sized> FFILink for &T {
    fn logical_type_create(&self, name: &str, parameters: Parameters<'_>) -> Result<LogicalType> {
        (*self).logical_type_create(name, parameters)
    }

    fn logical_type_create_from_id(&self, type_id: LogicalTypeID, parameters: Parameters<'_>) -> Result<LogicalType> {
        (*self).logical_type_create_from_id(type_id, parameters)
    }

    fn value_cast(&self, value: &Value, target_type: &LogicalType) -> Result<Value> {
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
    fn test_connection_owned_sql_and_prepare() -> crate::Result<()> {
        let conn = Environment::new()?.open(StorageLocation::InMemory)?.connect()?;
        let sql = String::from("CREATE TABLE owned_sql(i INTEGER)");
        conn.execute(&sql, Parameters::None)?;
        conn.execute(String::from("INSERT INTO owned_sql VALUES (7)"), Parameters::None)?;
        let chunk = conn
            .query(format!("SELECT i + {} FROM owned_sql", 1), Parameters::None)?
            .next_chunk()?
            .unwrap();
        assert_eq!(chunk.get_vector_at::<i32>(0)?.get(0)?, Some(&8));

        let prepared = conn.prepare(String::from("SELECT $1::INTEGER"), true)?;
        for value in [42_i32, 84] {
            let mut result = prepared.execute(Parameters::positional(&[&value]))?;
            let chunk = result.next_chunk()?.unwrap();
            assert_eq!(chunk.get_vector_at::<i32>(0)?.get(0)?, Some(&value));
            assert!(result.next_chunk()?.is_none());
        }
        assert!(prepared.reuses_plan()?);
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
    fn test_connection_rejects_invalid_statement_counts() -> crate::Result<()> {
        use crate::error::DuckDBError;

        let conn = Environment::new()?.open(StorageLocation::InMemory)?.connect()?;

        for sql in ["", "  ;  ", "SELECT 1; SELECT 2"] {
            let error = conn.execute(sql, Parameters::None).unwrap_err();
            assert_eq!(error.code, DuckDBError::DUCKDB_V2_ERROR_INPUT_INVALID);
            let error = conn.query(sql, Parameters::None).unwrap_err();
            assert_eq!(error.code, DuckDBError::DUCKDB_V2_ERROR_INPUT_INVALID);
        }
        assert!(conn.parse("SELECT '\0'").is_err());

        Ok(())
    }

    #[test]
    fn test_connection_preserves_second_statement_parse_error() -> crate::Result<()> {
        let conn = Environment::new()?.open(StorageLocation::InMemory)?.connect()?;
        let sql = "SELECT 1; SELECT FROM";
        let mut statements = conn.parse(sql)?;
        statements.next().unwrap()?;
        let expected = statements.next().unwrap().unwrap_err();

        let error = conn.execute(sql, Parameters::None).unwrap_err();
        assert_eq!(error.code, expected.code);
        assert_eq!(error.message, expected.message);

        Ok(())
    }

    #[test]
    fn test_connection_query_parsed_statements() -> crate::Result<()> {
        let env = Environment::new()?;
        let instance = env.open(StorageLocation::InMemory)?;
        let conn = instance.connect()?;

        let statements = conn.parse("SELECT 42")?;
        let mut result = conn.query(statements, Parameters::None)?;
        let chunk = result.next_chunk()?.unwrap();

        assert_eq!(chunk.get_vector_at::<i32>(0)?.get(0)?, Some(&42));

        Ok(())
    }

    #[test]
    fn test_connection_idle_interrupt() -> crate::Result<()> {
        let conn = Environment::new()?.open(StorageLocation::InMemory)?.connect()?;
        conn.interrupt_query()?;
        let chunk = conn.query("SELECT 42", Parameters::None)?.next_chunk()?.unwrap();
        assert_eq!(chunk.get_vector_at::<i32>(0)?.get(0)?, Some(&42));
        Ok(())
    }

    #[test]
    fn test_interrupt_handle_kept_alive_by_chunk() -> crate::Result<()> {
        use crate::{DuckDBType, data_chunk::DataChunk};

        let conn = Environment::new()?.open(StorageLocation::InMemory)?.connect()?;
        let handle = conn.interrupt_handle();
        let chunk = DataChunk::create(&conn, [i32::logical_type(&conn)?], true)?.copy(&conn)?;
        drop(conn);
        assert!(handle.is_alive());
        handle.interrupt()?;
        drop(chunk);
        assert!(!handle.is_alive());
        Ok(())
    }

    #[test]
    fn test_context_config() -> crate::Result<()> {
        use crate::{
            DuckDBType, FromValue,
            connection::{Context, SettingScope},
            data_chunk::VectorCollection,
            scalar::{ScalarCallbacks, ScalarFunctionBuilder},
            signature::SignatureBuilder,
            vector::{Unknown, Vector},
        };

        struct ReadThreads;
        impl ScalarCallbacks for ReadThreads {
            type BindData = ();
            type InitData = ();

            fn exec(
                &self,
                _bind_data: Option<&()>,
                _init_data: Option<&mut ()>,
                context: &Context,
                _input: &mut VectorCollection,
                output: Vector<'_, Unknown>,
            ) -> crate::Result<()> {
                let (value, _) = context.config()?.get_value("threads")?;
                let mut output = output.cast::<i64>()?;
                output.set_size(1)?;
                output.write(0, i64::from_value(&value)?)
            }
        }

        let conn = Environment::new()?.open(StorageLocation::InMemory)?.connect()?;
        conn.config()?.set_text("threads", "2", SettingScope::Global)?;
        ScalarFunctionBuilder::new(
            "context_threads",
            SignatureBuilder::new().return_type(i64::logical_type(&conn)?),
            ReadThreads,
        )
        .register(&conn)?;
        let chunk = conn
            .query("SELECT context_threads()", Parameters::None)?
            .next_chunk()?
            .unwrap();
        assert_eq!(chunk.get_vector_at::<i64>(0)?.get(0)?, Some(&2));
        Ok(())
    }

    #[test]
    fn test_interrupt_handle_clone() -> crate::Result<()> {
        let conn = Environment::new()?.open(StorageLocation::InMemory)?.connect()?;
        let handle = conn.interrupt_handle();
        let cloned = handle.clone();
        assert!(handle.is_alive());
        assert!(cloned.is_alive());
        drop(conn);
        assert!(!handle.is_alive());
        assert!(!cloned.is_alive());
        cloned.interrupt()?;
        Ok(())
    }

    #[test]
    fn test_interrupt_after_connection_close() -> crate::Result<()> {
        let env = Environment::new()?;
        let instance = env.open(StorageLocation::InMemory)?;
        let conn = instance.connect()?;

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

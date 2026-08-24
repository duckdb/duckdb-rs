//! Database sessions, SQL execution, and connection-scoped operations.
//!
//! Create a [`Connection`] with [`crate::database::Database::connect`]. Each
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
    ops::Deref,
    sync::{Arc, Mutex},
};

use crate::{
    Parameters, Result, ToValue,
    builder_helpers::ffi_enum_redeclaration,
    connection_options::ConfigOption,
    database::DatabaseHandle,
    error::{DuckDBError, Error, check_api_call, check_api_call_no_err},
    ffi,
    logical_type::{LogicalType, LogicalTypeID},
    parameter::ParameterValue,
    query_result::QueryResult,
    statement::{Statement, Statements},
    value::{self, Value, ValueInput},
};

ffi_enum_redeclaration! {
    /// Destination scope for a connection-side configuration option write.
    pub enum SettingScope <- ffi::DUCKDB_V2_SETTING_SCOPE {
        /// Resolve the destination from the option's target scope.
        Automatic = DUCKDB_V2_SETTING_SCOPE_AUTOMATIC,
        /// Write through to the database, making the setting visible to all connections.
        Global = DUCKDB_V2_SETTING_SCOPE_GLOBAL,
        /// Write to this connection's session only.
        Local = DUCKDB_V2_SETTING_SCOPE_LOCAL
    }
}

/// A single statement accepted by [`Connection::execute`] and [`Connection::query`].
///
/// Implemented for SQL text, [`Statements`], and [`Statement`]. SQL text and
/// statement collections must contain exactly one statement.
pub trait IntoStatement {
    #[doc(hidden)]
    fn execute_statement<'conn>(
        self,
        conn: &'conn Connection,
        names: Option<&[&str]>,
        values: &[&Value],
    ) -> Result<QueryResult<'conn>>;
}

impl IntoStatement for &str {
    fn execute_statement<'conn>(
        self,
        conn: &'conn Connection,
        names: Option<&[&str]>,
        values: &[&Value],
    ) -> Result<QueryResult<'conn>> {
        execute_statements(conn, Statements::parse(conn, self)?, names, values)
    }
}

impl IntoStatement for Statements {
    fn execute_statement<'conn>(
        self,
        conn: &'conn Connection,
        names: Option<&[&str]>,
        values: &[&Value],
    ) -> Result<QueryResult<'conn>> {
        execute_statements(conn, self, names, values)
    }
}

impl IntoStatement for Statement<'_> {
    fn execute_statement<'conn>(
        self,
        conn: &'conn Connection,
        names: Option<&[&str]>,
        values: &[&Value],
    ) -> Result<QueryResult<'conn>> {
        conn.execute_statement(self, names, values)
    }
}

fn execute_statements<'conn>(
    conn: &'conn Connection,
    mut statements: Statements,
    names: Option<&[&str]>,
    values: &[&Value],
) -> Result<QueryResult<'conn>> {
    let statement = statements.next().ok_or(Error {
        code: DuckDBError::DUCKDB_V2_ERROR_API,
        message: "No statements found in SQL string".to_string(),
    })??;

    assert!(
        statements.next().is_none(),
        "Multiple statements found in SQL string"
    );

    conn.execute_statement(statement, names, values)
}

/// A session connected to a DuckDB database.
///
/// Connections have independent session settings and transactions while
/// sharing their database's catalog and storage.
pub struct Connection {
    pub(crate) handle: ffi::duckdb_v2_connection_handle,
    pub(crate) _db: Arc<Mutex<DatabaseHandle>>,
}

impl Connection {
    /// Parse SQL into an iterator over its statements.
    pub fn parse(&self, query: impl AsRef<str>) -> Result<Statements> {
        Statements::parse(self, query)
    }

    /// Execute a single statement to completion and return its changed-row count.
    ///
    /// Row-producing output is discarded. Results that do not report changed
    /// rows return zero.
    pub fn execute(&self, stmt: impl IntoStatement, params: Parameters<'_>) -> Result<usize> {
        let mut query_result = self.query(stmt, params)?;
        query_result.drain()
    }

    /// Start a single statement and return its lazy, streaming result.
    ///
    /// Execution advances as the result is stepped or iterated. The connection
    /// cannot start another statement while that result remains live.
    pub fn query(
        &self,
        stmt: impl IntoStatement,
        params: Parameters<'_>,
    ) -> Result<QueryResult<'_>> {
        let (names, values) = params.into_values(self)?;
        let values = values
            .iter()
            .map(ParameterValue::as_value)
            .collect::<Vec<_>>();

        stmt.execute_statement(self, names.as_deref(), &values)
    }

    fn execute_statement(
        &self,
        stmt: Statement<'_>,
        names: Option<&[&str]>,
        values: &[&Value],
    ) -> Result<QueryResult<'_>> {
        let values = values.iter().map(|value| value.handle).collect::<Vec<_>>();
        let name_strs = names.map(|names| {
            names
                .iter()
                .map(|name| (*name).into())
                .collect::<Vec<ffi::duckdb_v2_str>>()
        });

        let names_ptr = name_strs
            .as_ref()
            .filter(|names| !names.is_empty())
            .map_or(std::ptr::null(), |names| names.as_ptr());

        let values_ptr = if values.is_empty() {
            std::ptr::null()
        } else {
            values.as_ptr()
        };
        let result: ffi::duckdb_v2_result_handle = check_api_call!(
            ffi::duckdb_v2_statement_execute,
            self.handle,
            stmt.handle,
            names_ptr,
            values_ptr,
            values.len() as u64,
            RET
        )?;

        Ok(QueryResult {
            phantom: std::marker::PhantomData {},
            handle: result,
        })
    }

    /// Return the number of options visible to this connection.
    pub fn get_options_count(&self) -> Result<usize> {
        let count: u64 =
            check_api_call!(ffi::duckdb_v2_connection_option_get_count, self.handle, RET)?;

        Ok(count as usize)
    }

    /// Return an effective option by canonical name or alias.
    ///
    /// The setting resolves from this connection's local override, then the
    /// database's global value, then the static default.
    pub fn get_option(&self, name: &str) -> Result<ConfigOption> {
        let handle = check_api_call!(
            ffi::duckdb_v2_connection_option_get,
            self.handle,
            name.into(),
            RET
        )?;

        Ok(ConfigOption { handle })
    }

    /// Return the visible option at `index`.
    ///
    /// An out-of-range index returns an error.
    pub fn get_option_by_index(&self, index: usize) -> Result<ConfigOption> {
        let handle = check_api_call!(
            ffi::duckdb_v2_connection_option_get_by_index,
            self.handle,
            index as u64,
            RET
        )?;

        Ok(ConfigOption { handle })
    }

    /// Return all options visible to this connection.
    pub fn get_options(&self) -> Result<Vec<ConfigOption>> {
        let count = self.get_options_count()?;
        let mut options = Vec::with_capacity(count);

        for i in 0..count {
            let option = self.get_option_by_index(i)?;
            options.push(option);
        }

        Ok(options)
    }

    /// Set an option at `scope`, using its declared target scope when `None`.
    ///
    /// Global writes affect the database; local writes affect only this
    /// session. Unknown options and scopes disallowed by the option return an
    /// error.
    pub fn set_option(
        &self,
        option: &impl Deref<Target = ffi::duckdb_v2_option_handle>,
        scope: Option<SettingScope>,
    ) -> Result<()> {
        let scope = scope.unwrap_or(SettingScope::Automatic);

        check_api_call!(
            ffi::duckdb_v2_connection_option_set,
            self.handle,
            **option,
            scope.into()
        )?;

        Ok(())
    }

    /// Request cancellation of the active query, or do nothing if idle.
    pub fn interrupt_query(&self) -> Result<()> {
        check_api_call!(ffi::duckdb_v2_connection_interrupt, self.handle)
    }
}

impl Deref for Connection {
    type Target = ffi::duckdb_v2_connection_handle;
    fn deref(&self) -> &Self::Target {
        &self.handle
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        check_api_call_no_err!(ffi::duckdb_v2_disconnect, &mut self.handle).unwrap();
    }
}

/// Type and value operations available through a connection or callback context.
pub trait FFILink {
    /// Create a logical type using this connection or context.
    fn logical_type_create(&self, name: &str, parameters: Parameters<'_>) -> Result<LogicalType>;

    /// Create a logical type from its primitive ID using this connection or context.
    fn logical_type_create_from_id(
        &self,
        type_id: LogicalTypeID,
        parameters: Parameters<'_>,
    ) -> Result<LogicalType>;

    /// Create a logical type from its textual representation using this connection or context.
    fn logical_type_from_text(&self, text: &str) -> Result<LogicalType>;

    /// Cast a value using this connection or context.
    fn value_cast(&self, value: &Value, target_type: LogicalType) -> Result<Value>;

    #[doc(hidden)]
    fn create_value(&self, input: ValueInput<'_>) -> Result<Value>;
}

impl FFILink for Connection {
    fn logical_type_create(&self, name: &str, parameters: Parameters<'_>) -> Result<LogicalType> {
        LogicalType::create_with_connection(self, name, parameters)
    }

    fn logical_type_create_from_id(
        &self,
        type_id: LogicalTypeID,
        parameters: Parameters<'_>,
    ) -> Result<LogicalType> {
        LogicalType::create_from_id_with_connection(self, type_id, parameters)
    }

    fn logical_type_from_text(&self, text: &str) -> Result<LogicalType> {
        LogicalType::from_text_with_connection(self, text)
    }

    fn value_cast(&self, value: &Value, target_type: LogicalType) -> Result<Value> {
        value.cast_with_connection(self, target_type)
    }

    fn create_value(&self, input: ValueInput<'_>) -> Result<Value> {
        value::create_with_connection(self, input)
    }
}

/// A non-owning DuckDB context supplied for the duration of a callback.
///
/// This wrapper does not own the underlying handle and must not outlive the callback invocation.
#[repr(transparent)]
pub struct Context(pub(crate) ffi::duckdb_v2_context_handle);

impl Deref for Context {
    type Target = ffi::duckdb_v2_context_handle;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl FFILink for Context {
    fn logical_type_create(&self, name: &str, parameters: Parameters<'_>) -> Result<LogicalType> {
        LogicalType::create_with_context(self, name, parameters)
    }

    fn logical_type_create_from_id(
        &self,
        type_id: LogicalTypeID,
        parameters: Parameters<'_>,
    ) -> Result<LogicalType> {
        LogicalType::create_from_id_with_context(self, type_id, parameters)
    }

    fn logical_type_from_text(&self, text: &str) -> Result<LogicalType> {
        LogicalType::from_text_with_context(self, text)
    }

    fn value_cast(&self, value: &Value, target_type: LogicalType) -> Result<Value> {
        value.cast_with_context(self, target_type)
    }

    fn create_value(&self, input: ValueInput<'_>) -> Result<Value> {
        value::create_with_context(self, input)
    }
}

impl<T: FFILink + ?Sized> FFILink for &T {
    /// Construct a logical type from a name and value parameters.
    fn logical_type_create(&self, name: &str, parameters: Parameters<'_>) -> Result<LogicalType> {
        (*self).logical_type_create(name, parameters)
    }

    fn logical_type_create_from_id(
        &self,
        type_id: LogicalTypeID,
        parameters: Parameters<'_>,
    ) -> Result<LogicalType> {
        (*self).logical_type_create_from_id(type_id, parameters)
    }

    fn logical_type_from_text(&self, text: &str) -> Result<LogicalType> {
        (*self).logical_type_from_text(text)
    }

    fn value_cast(&self, value: &Value, target_type: LogicalType) -> Result<Value> {
        (*self).value_cast(value, target_type)
    }

    fn create_value(&self, input: ValueInput<'_>) -> Result<Value> {
        (*self).create_value(input)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        Parameters,
        environment::{Environment, StorageLocation},
    };

    #[test]
    fn test_connection_execute() -> crate::Result<()> {
        let conn = Environment::new()?
            .open(StorageLocation::InMemory)?
            .connect()?;

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
        let conn = Environment::new()?
            .open(StorageLocation::InMemory)?
            .connect()?;

        let result = conn.query(
            "SELECT $1::INTEGER WHERE $2 = 'hello'",
            Parameters::positional(&[&10_i32, &"hello"]),
        )?;
        let chunk = result.into_iter().next().unwrap()?;
        assert_eq!(chunk.get_vector_at::<i32>(0)?.get(0)?, Some(&10));

        conn.execute("SELECT $1", Parameters::positional(&[&(42_i32, "duck")]))?;

        let result = conn.query(
            "SELECT * FROM range(0, 20000) as x where x % $mod = 0",
            Parameters::named(&[("mod", &5)]),
        )?;

        let mut i = 0;

        for chunk in result {
            let chunk = chunk?;

            let vector = chunk.get_vector_at::<i64>(0)?;

            for value in vector.iter()? {
                assert_eq!(value, Some(&(i as i64 * 5)));
                i += 1;
            }
        }

        Ok(())
    }

    #[test]
    fn test_connection_query_parsed_statements() -> crate::Result<()> {
        let env = Environment::new()?;
        let db = env.open(StorageLocation::InMemory)?;
        let conn = db.connect()?;

        let statements = conn.parse("SELECT 42")?;
        let result = conn.query(statements, Parameters::None)?;
        let chunk = result.into_iter().next().unwrap()?;

        assert_eq!(chunk.get_vector_at::<i32>(0)?.get(0)?, Some(&42));

        Ok(())
    }
}

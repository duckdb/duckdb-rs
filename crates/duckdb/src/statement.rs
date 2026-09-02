//! Parsing, binding, and preparing SQL statements.

use std::marker::PhantomData;

use crate::{Result, check_api_call, check_api_call_no_err, connection::Connection, ffi, schema::Schema};

#[cfg(feature = "capi-v2-p2")]
use crate::{Parameters, column_data_collection::ColumnDataCollection, query_result::QueryResult, value::Value};

/// Schemas resolved while binding a statement.
pub struct SchemaBind {
    /// The statement's result columns.
    pub schema: Schema,
    /// The statement's parameters.
    pub parameters: Schema,
}

/// Parsed statements from one SQL string.
///
/// Parsing does not bind names, access the catalog, or execute SQL. Errors may
/// be reported when the iterator reaches the affected statement.
///
/// # Example
/// ```
/// use duckdb_rs::{environment::Environment, environment::StorageLocation};
///
/// # fn main() -> duckdb_rs::Result<()> {
/// let env = Environment::new()?;
/// let db = env.open(StorageLocation::InMemory)?;
/// let conn = db.connect()?;
///
/// let statements = conn.parse("SELECT 1; SELECT 2; ; SELECT 42")?;
///
/// assert_eq!(statements.count(), 3);
/// # Ok(())
/// # }
/// ```
pub struct Statements {
    /// The owned DuckDB statement-iterator handle.
    pub handle: ffi::duckdb_v2_statement_iterator_handle,
}

impl Statements {
    /// Parse SQL using a connection's parser configuration.
    pub fn parse(conn: &Connection, sql: impl AsRef<str>) -> Result<Statements> {
        let query_str = std::ffi::CString::new(sql.as_ref()).expect("Failed to create CString from query");

        let handle: ffi::duckdb_v2_statement_iterator_handle =
            check_api_call!(ffi::duckdb_v2_parse_sql, **conn, query_str.as_ptr(), RET)?;

        Ok(Statements { handle })
    }
}

impl Drop for Statements {
    fn drop(&mut self) {
        check_api_call_no_err!(ffi::duckdb_v2_statement_iterator_destroy, &mut self.handle).unwrap();
    }
}

impl Iterator for Statements {
    type Item = Result<Statement<'static>>;

    fn next(&mut self) -> Option<Self::Item> {
        let stmt_handle: ffi::duckdb_v2_sql_statement_handle =
            match check_api_call!(ffi::duckdb_v2_statement_iterator_next, self.handle, RET) {
                Ok(handle) => handle,
                Err(e) => return Some(Err(e)),
            };

        if stmt_handle.is_null() {
            None
        } else {
            Some(Ok(Statement {
                handle: stmt_handle,
                #[cfg(feature = "capi-v2-p2")]
                collections: Vec::new(),
                #[cfg(not(feature = "capi-v2-p2"))]
                collections: std::marker::PhantomData,
            }))
        }
    }
}

/// A single parsed SQL statement.
///
/// A statement can be bound to inspect its input and output schemas, extended
/// with in-memory collections, prepared for repeated execution, or passed to
/// [`Connection::query`].
pub struct Statement<'collection> {
    /// The owned DuckDB statement handle.
    pub handle: ffi::duckdb_v2_sql_statement_handle,
    #[cfg(feature = "capi-v2-p2")]
    collections: Vec<&'collection ColumnDataCollection>,
    #[cfg(not(feature = "capi-v2-p2"))]
    collections: std::marker::PhantomData<&'collection ()>,
}

impl<'collection> Statement<'collection> {
    /// Bind the statement and return its result and parameter schemas.
    pub fn bind(&self, conn: &Connection) -> Result<SchemaBind> {
        let mut out_parameters = std::ptr::null_mut();

        let out_schema = check_api_call!(
            ffi::duckdb_v2_statement_bind,
            **conn,
            self.handle,
            RET,
            &mut out_parameters
        )?;

        Ok(SchemaBind {
            schema: Schema { handle: out_schema },
            parameters: Schema { handle: out_parameters },
        })
    }

    /// Make a collection available as a named table in the statement.
    ///
    /// Custom column names must match the collection width; otherwise DuckDB
    /// exposes the columns as `col1`, `col2`, and so on.
    #[cfg(feature = "capi-v2-p2")]
    pub fn add_collection<'new_collection>(
        self,
        name: &str,
        collection: &'new_collection ColumnDataCollection,
        column_names: Option<&[String]>,
    ) -> Result<Statement<'new_collection>>
    where
        'collection: 'new_collection,
    {
        let mut statement: Statement<'new_collection> = self;

        statement.register_collection(name, collection, column_names)?;
        statement.collections.push(collection);

        Ok(statement)
    }

    #[cfg(feature = "capi-v2-p2")]
    fn register_collection(
        &self,
        name: &str,
        collection: &ColumnDataCollection,
        column_names: Option<&[String]>,
    ) -> Result<()> {
        let names = column_names.map_or(vec![], |v| {
            v.iter().map(|name| name.into()).collect::<Vec<ffi::duckdb_v2_str>>()
        });

        check_api_call!(
            ffi::duckdb_v2_statement_add_collection,
            self.handle,
            name.into(),
            collection.handle,
            names.as_ptr(),
            column_names.map_or(0, |v| v.len() as u64)
        )
    }

    #[cfg(feature = "capi-v2-p2")]
    /// Prepare the statement for repeated execution.
    ///
    /// When `require_cacheable` is true, preparation fails unless the compiled
    /// plan can be reused.
    pub fn prepare<'a>(&self, conn: &'a Connection, require_cacheable: bool) -> Result<PreparedStatement<'a>> {
        let prepared_handle = check_api_call!(
            ffi::duckdb_v2_statement_prepare,
            **conn,
            self.handle,
            require_cacheable,
            RET
        )?;

        Ok(PreparedStatement {
            connection: conn,
            handle: prepared_handle,
        })
    }
}

impl Drop for Statement<'_> {
    fn drop(&mut self) {
        check_api_call_no_err!(ffi::duckdb_v2_sql_statement_destroy, &mut self.handle).unwrap();
    }
}

#[cfg(feature = "capi-v2-p2")]
/// A statement bound and planned for repeated execution.
///
/// Execution accepts either named or positional [`Value`] parameters and is
/// lazy: work begins when the returned [`QueryResult`] is consumed. The
/// prepared statement remains associated with the connection used to create it.
pub struct PreparedStatement<'a> {
    connection: &'a Connection,
    /// The owned DuckDB prepared-statement handle.
    pub handle: ffi::duckdb_v2_prepared_statement_handle,
}

#[cfg(feature = "capi-v2-p2")]
impl<'a> PreparedStatement<'a> {
    /// Execute with optional positional or named parameters.
    pub fn execute(&self, parameters: Parameters<'_>) -> Result<QueryResult<'a>> {
        let (param_names, param_values) = parameters.into_values(self.connection)?;
        let param_values = param_values
            .iter()
            .map(|value| value.as_value().handle)
            .collect::<Vec<_>>();

        let result: ffi::duckdb_v2_result_handle = check_api_call!(
            ffi::duckdb_v2_prepared_execute,
            self.handle,
            param_names.as_ref().map_or(std::ptr::null(), |names| names.as_ptr()),
            param_values.as_ptr(),
            param_values.len() as u64,
            RET
        )?;

        Ok(QueryResult {
            phantom: std::marker::PhantomData,
            handle: result,
        })
    }

    /// Return whether executions reuse the compiled plan.
    pub fn reuses_plan(&self) -> Result<bool> {
        let reuses_plan: bool = check_api_call!(ffi::duckdb_v2_prepared_reuses_plan, self.handle, RET)?;
        Ok(reuses_plan)
    }
}

#[cfg(feature = "capi-v2-p2")]
impl<'a> Drop for PreparedStatement<'a> {
    fn drop(&mut self) {
        check_api_call_no_err!(ffi::duckdb_v2_prepared_statement_destroy, &mut self.handle).unwrap();
    }
}

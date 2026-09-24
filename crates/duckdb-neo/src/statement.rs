//! Parsing, binding, and preparing SQL statements.

use libduckdb_sys::v2::DuckDBStr;

use crate::{
    Parameters, Result, check_api_call, check_api_call_no_err,
    connection::Connection,
    ffi,
    query_result::{QueryResult, StatementType},
    schema::Schema,
};

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
/// use duckdb_neo::{environment::Environment, environment::StorageLocation};
///
/// # fn main() -> duckdb_neo::Result<()> {
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
    type Item = Result<Statement>;

    fn next(&mut self) -> Option<Self::Item> {
        let stmt_handle: ffi::duckdb_v2_sql_statement_handle =
            match check_api_call!(ffi::duckdb_v2_statement_iterator_next, self.handle, RET) {
                Ok(handle) => handle,
                Err(e) => return Some(Err(e)),
            };

        if stmt_handle.is_null() {
            None
        } else {
            Some(Ok(Statement { handle: stmt_handle }))
        }
    }
}

/// A single parsed SQL statement.
///
/// A statement can be bound to inspect its input and output schemas,
/// prepared for repeated execution, or passed to
/// [`Connection::query`].
pub struct Statement {
    /// The owned DuckDB statement handle.
    pub handle: ffi::duckdb_v2_sql_statement_handle,
}

impl Statement {
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

    /// Prepare the statement for repeated execution.
    ///
    /// When `require_cacheable` is true, preparation fails unless the compiled
    /// plan can be reused.
    pub fn prepare<'a>(&self, conn: &'a Connection, require_cacheable: bool) -> Result<PreparedStatement<'a>> {
        let prepared_handle = check_api_call!(
            ffi::duckdb_v2_prepared_statement_create,
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

    /// Return a copy of this statement's SQL text.
    ///
    /// Includes the trailing terminator and whitespace, but excludes whitespace
    /// and comments before the first token.
    pub fn get_text(&self) -> Result<String> {
        check_api_call!(ffi::duckdb_v2_sql_statement_get_text, self.handle, RET).map(|x| <&str>::from(x).to_owned())
    }

    /// Return the statement type as classified by the parser, before execution-time rewrites.
    pub fn get_type(&self) -> Result<StatementType> {
        let val = check_api_call!(ffi::duckdb_v2_sql_statement_get_type, self.handle, RET)?;
        val.try_into()
    }

    /// Return the number of distinct parameters found by the parser, counting repeated uses once.
    pub fn parameter_count(&self) -> Result<usize> {
        check_api_call!(ffi::duckdb_v2_sql_statement_get_parameter_count, self.handle, RET).map(|x| x as usize)
    }

    /// Return the parameter's binding key at the zero-based index in binding order.
    ///
    /// Keys omit the `$` prefix: `"1"` for `$1`, or `"name"` for `$name`.
    /// Positional keys may have gaps.
    pub fn parameter_name(&self, index: usize) -> Result<String> {
        check_api_call!(
            ffi::duckdb_v2_sql_statement_get_parameter_name,
            self.handle,
            index as u64,
            RET
        )
        .map(|x| <&str>::from(x).to_owned())
    }
}

impl Drop for Statement {
    fn drop(&mut self) {
        check_api_call_no_err!(ffi::duckdb_v2_sql_statement_destroy, &mut self.handle).unwrap();
    }
}

/// A statement bound and planned for repeated execution.
///
/// Execution accepts named or positional [`Parameters`] and is
/// lazy: work begins when the returned [`QueryResult`] is consumed. The
/// prepared statement remains associated with the connection used to create it.
pub struct PreparedStatement<'a> {
    connection: &'a Connection,
    /// The owned DuckDB prepared-statement handle.
    pub handle: ffi::duckdb_v2_prepared_statement_handle,
}

impl<'a> PreparedStatement<'a> {
    /// Execute with optional positional or named parameters.
    pub fn execute(&self, parameters: Parameters<'_>) -> Result<QueryResult<'a>> {
        let (param_names, param_values) = parameters.into_values(self.connection)?;
        let param_values = param_values
            .iter()
            .map(|value| value.as_value().handle)
            .collect::<Vec<_>>();
        let param_names = param_names.map(|x| x.iter().map(|s| (*s).into()).collect::<Vec<DuckDBStr<'_>>>());

        let result: ffi::duckdb_v2_result_handle = check_api_call!(
            ffi::duckdb_v2_prepared_statement_execute,
            self.handle,
            param_names.as_ref().map_or(std::ptr::null(), |v| v.as_ptr()),
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
        let reuses_plan: bool = check_api_call!(ffi::duckdb_v2_prepared_statement_reuses_plan, self.handle, RET)?;
        Ok(reuses_plan)
    }
}

impl<'a> Drop for PreparedStatement<'a> {
    fn drop(&mut self) {
        check_api_call_no_err!(ffi::duckdb_v2_prepared_statement_destroy, &mut self.handle).unwrap();
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use crate::{
        environment::{Environment, StorageLocation},
        statement::Statements,
    };

    #[test]
    fn test_prepared_statement() -> crate::Result<()> {
        let env = Environment::new()?;
        let db = env.open(StorageLocation::InMemory)?;
        let conn = db.connect()?;

        let mut statements = Statements::parse(&conn, "SELECT * FROM range(0, 100) as t(x) where x < ?")?;
        let statement = statements.next().unwrap()?.prepare(&conn, true)?;

        let query = statement.execute(crate::Parameters::Positional(&[&10]))?;

        for chunk in query {
            let chunk = chunk?;

            let vec = chunk.get_vector_at::<i64>(0)?;

            assert_eq!(vec.len(), 10);
        }

        Ok(())
    }

    #[test]
    fn test_prepared_statement_named_parameters() -> crate::Result<()> {
        let env = Environment::new()?;
        let db = env.open(StorageLocation::InMemory)?;
        let conn = db.connect()?;

        let mut statements = Statements::parse(
            &conn,
            "SELECT x FROM range(0, 100) as t(x) WHERE x >= $lo AND x < $hi ORDER BY x",
        )?;
        let statement = statements.next().unwrap()?.prepare(&conn, true)?;

        // Bind in a different order than they appear in the SQL, so binding
        // only succeeds if the names reach DuckDB intact.
        let query = statement.execute(crate::Parameters::named(&[("hi", &15), ("lo", &10)]))?;

        let mut values = Vec::new();
        for chunk in query {
            let chunk = chunk?;
            values.extend(chunk.get_vector_at::<i64>(0)?.iter()?.flatten().copied());
        }
        assert_eq!(values, (10..15).collect::<Vec<i64>>());

        Ok(())
    }
}

//! Parsing, binding, and preparing SQL statements.

use std::sync::Arc;

use crate::column_data_collection::ColumnDataCollection;
use crate::ffi_str::DuckDBStr;
use crate::{
    AsRaw, DropHandle, FromRaw, Parameters, Result, check_api_call,
    connection::Connection,
    error::Error,
    ffi,
    parameter::ParameterValue,
    query_result::{ArrowQueryResult, QueryResult, StatementType},
    raw::RawExt,
    schema::Schema,
};

/// Parameter buffers passed to an execute call.
///
/// Owns the converted values, so their handles stay valid for the call.
struct ParameterBuffers<'a> {
    _values: Vec<ParameterValue<'a>>,
    handles: Vec<ffi::duckdb_v2_value_handle>,
    names: Option<Vec<ffi::duckdb_v2_str>>,
}

impl ParameterBuffers<'_> {
    fn names_ptr(&self) -> *const ffi::duckdb_v2_str {
        self.names
            .as_ref()
            .filter(|names| !names.is_empty())
            .map_or(std::ptr::null(), |names| names.as_ptr())
    }

    fn values_ptr(&self) -> *const ffi::duckdb_v2_value_handle {
        if self.handles.is_empty() {
            std::ptr::null()
        } else {
            self.handles.as_ptr()
        }
    }

    fn len(&self) -> u64 {
        self.handles.len() as u64
    }
}

/// Schemas resolved while binding a statement.
#[derive(Debug)]
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
#[derive(Debug, AsRaw, FromRaw, DropHandle)]
#[drop_handle(ffi::duckdb_v2_statement_iterator_destroy)]
pub struct Statements {
    pub(crate) handle: ffi::duckdb_v2_statement_iterator_handle,
}

impl Statements {
    /// Parse SQL using a connection's parser configuration.
    pub fn parse(conn: &Connection, sql: impl AsRef<str>) -> Result<Statements> {
        let query_str =
            std::ffi::CString::new(sql.as_ref()).map_err(|_| Error::invalid_input("SQL string contains a NUL byte"))?;

        check_api_call!(ffi::duckdb_v2_parse_sql, conn.raw(), query_str.as_ptr(), RET)
            .map(|handle| Statements { handle })
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
                collections: BoundCollections::default(),
            }))
        }
    }
}

/// The collections bound to a statement with [`Statement::add_collection`].
///
/// The statement, its prepared copies and every result executing it share them,
/// since DuckDB reads them for as long as any of those is live.
#[derive(Debug, Default, Clone)]
pub struct BoundCollections<'a>(Vec<Arc<ColumnDataCollection<'a>>>);

/// A single parsed SQL statement.
///
/// A statement can be bound to inspect its input and output schemas,
/// prepared for repeated execution, or passed to
/// [`Connection::query`].
#[derive(Debug, AsRaw, FromRaw, DropHandle)]
#[drop_handle(ffi::duckdb_v2_sql_statement_destroy)]
pub struct Statement<'a> {
    /// The owned DuckDB statement handle.
    pub(crate) handle: ffi::duckdb_v2_sql_statement_handle,
    collections: BoundCollections<'a>,
}

impl<'a> Statement<'a> {
    /// Bind the statement and return its result and parameter schemas.
    pub fn bind(&self, conn: &Connection) -> Result<SchemaBind> {
        let mut out_parameters = std::ptr::null_mut();

        check_api_call!(
            ffi::duckdb_v2_statement_bind,
            conn.raw(),
            self.handle,
            RET,
            &mut out_parameters
        )
        .map(|out_schema| SchemaBind {
            schema: Schema { handle: out_schema },
            parameters: Schema { handle: out_parameters },
        })
    }

    /// Bind `collection` under `name`, so the statement can read it like a table.
    ///
    /// The statement takes ownership: the collection lives as long as the
    /// statement, its prepared copies, or any result executing it.
    pub fn add_collection(mut self, collection: ColumnDataCollection<'a>, name: &str) -> Result<Self> {
        let name: ffi::duckdb_v2_str = name.into();
        let column_name_ptrs: Option<Vec<ffi::duckdb_v2_str>> = collection
            .column_names
            .as_ref()
            .map(|x| x.iter().map(|x| x.into()).collect::<Vec<_>>());

        check_api_call!(
            ffi::duckdb_v2_statement_add_collection,
            self.raw(),
            &name,
            collection.raw(),
            column_name_ptrs.as_ref().map_or(std::ptr::null(), |x| x.as_ptr()),
            column_name_ptrs.as_ref().map_or(0, |x| x.len() as u64)
        )?;

        self.collections.0.push(Arc::new(collection));
        Ok(self)
    }

    fn prepare_params<'p>(conn: &Connection, params: Parameters<'p>) -> Result<ParameterBuffers<'p>> {
        let (names, values) = params.into_values(conn)?;
        let handles = values.iter().map(|value| value.as_value().handle).collect();
        let names = names.map(|names| names.iter().map(|name| (*name).into()).collect());

        Ok(ParameterBuffers {
            _values: values,
            handles,
            names,
        })
    }

    /// Start the statement and return its lazy, streaming result.
    ///
    /// Execution advances as the result is stepped or iterated. The connection
    /// cannot start another statement while that result remains live.
    pub fn query<'conn>(&self, conn: &'conn Connection, params: Parameters<'_>) -> Result<QueryResult<'conn>>
    where
        'a: 'conn,
    {
        let params = Self::prepare_params(conn, params)?;

        check_api_call!(
            ffi::duckdb_v2_statement_execute,
            conn.raw(),
            self.handle,
            params.names_ptr(),
            params.values_ptr(),
            params.len(),
            RET
        )
        .map(|handle| QueryResult::new(handle, self.collections.clone()))
    }

    /// Start the statement and return its lazy, streaming Arrow result.
    ///
    /// Arrays hold at most `batch_size` rows; `None` or `Some(0)` selects
    /// DuckDB's default of 131,072. The connection cannot start another
    /// statement while that result remains live.
    pub fn arrow_query<'conn>(
        &self,
        conn: &'conn Connection,
        params: Parameters<'_>,
        batch_size: Option<usize>,
    ) -> Result<ArrowQueryResult<'conn>>
    where
        'a: 'conn,
    {
        let params = Self::prepare_params(conn, params)?;

        check_api_call!(
            ffi::duckdb_v2_statement_execute_arrow,
            conn.raw(),
            self.handle,
            params.names_ptr(),
            params.values_ptr(),
            params.len(),
            batch_size.unwrap_or_default() as u64,
            RET
        )
        .map(|handle| ArrowQueryResult::new(handle, self.collections.clone()))
    }

    /// Prepare the statement for repeated execution.
    ///
    /// When `require_cacheable` is true, preparation fails unless the compiled
    /// plan can be reused.
    pub fn prepare<'conn>(&self, conn: &'conn Connection, require_cacheable: bool) -> Result<PreparedStatement<'conn>>
    where
        'a: 'conn,
    {
        check_api_call!(
            ffi::duckdb_v2_prepared_statement_create,
            conn.raw(),
            self.handle,
            require_cacheable,
            RET
        )
        .map(|prepared_handle| PreparedStatement {
            connection: conn,
            handle: prepared_handle,
            collections: self.collections.clone(),
        })
    }

    /// Return a copy of this statement's SQL text.
    ///
    /// Includes the trailing terminator and whitespace, but excludes whitespace
    /// and comments before the first token.
    pub fn get_text(&self) -> Result<String> {
        check_api_call!(ffi::duckdb_v2_sql_statement_get_text, self.handle, RET)
            .map(|x| DuckDBStr::from_raw(x).as_str().unwrap_or_default().to_owned())
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
        .map(|x| DuckDBStr::from_raw(x).as_str().unwrap_or_default().to_owned())
    }
}

/// A statement bound and planned for repeated execution.
///
/// Execution accepts named or positional [`Parameters`] and is
/// lazy: work begins when the returned [`QueryResult`] is consumed. The
/// prepared statement remains associated with the connection used to create it.
#[derive(Debug, AsRaw, DropHandle)]
#[drop_handle(ffi::duckdb_v2_prepared_statement_destroy)]
pub struct PreparedStatement<'a> {
    connection: &'a Connection,
    /// The owned DuckDB prepared-statement handle.
    pub(crate) handle: ffi::duckdb_v2_prepared_statement_handle,
    collections: BoundCollections<'a>,
}

impl<'a> PreparedStatement<'a> {
    /// Execute with optional positional or named parameters.
    pub fn execute(&self, parameters: Parameters<'_>) -> Result<QueryResult<'a>> {
        let (param_names, param_values) = parameters.into_values(self.connection)?;
        let param_values = param_values
            .iter()
            .map(|value| value.as_value().handle)
            .collect::<Vec<_>>();
        let param_names = param_names.map(|x| x.iter().map(|s| (*s).into()).collect::<Vec<ffi::duckdb_v2_str>>());

        check_api_call!(
            ffi::duckdb_v2_prepared_statement_execute,
            self.handle,
            param_names.as_ref().map_or(std::ptr::null(), |v| v.as_ptr()),
            param_values.as_ptr(),
            param_values.len() as u64,
            RET
        )
        .map(|handle| QueryResult::new(handle, self.collections.clone()))
    }

    /// Execute with optional positional or named parameters.
    pub fn execute_arrow(&self, parameters: Parameters<'_>, batch_size: Option<usize>) -> Result<ArrowQueryResult<'a>> {
        let (param_names, param_values) = parameters.into_values(self.connection)?;
        let param_values = param_values
            .iter()
            .map(|value| value.as_value().handle)
            .collect::<Vec<_>>();
        let param_names = param_names.map(|x| x.iter().map(|s| (*s).into()).collect::<Vec<ffi::duckdb_v2_str>>());

        check_api_call!(
            ffi::duckdb_v2_prepared_statement_execute_arrow,
            self.handle,
            param_names.as_ref().map_or(std::ptr::null(), |v| v.as_ptr()),
            param_values.as_ptr(),
            param_values.len() as u64,
            batch_size.unwrap_or_default() as u64,
            RET
        )
        .map(|handle| ArrowQueryResult::new(handle, self.collections.clone()))
    }

    /// Return whether executions reuse the compiled plan.
    pub fn reuses_plan(&self) -> Result<bool> {
        check_api_call!(ffi::duckdb_v2_prepared_statement_reuses_plan, self.handle, RET)
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use arrow_array::ffi::FFI_ArrowArray;

    use crate::{
        DuckDBType, Parameters,
        column_data_collection::ColumnDataCollection,
        data_chunk::DataChunk,
        environment::{Environment, StorageLocation},
        ffi,
        statement::Statements,
    };

    #[test]
    fn test_prepared_statement() -> crate::Result<()> {
        let env = Environment::new()?;
        let db = env.open(StorageLocation::InMemory)?;
        let conn = db.connect()?;

        let mut statements = Statements::parse(&conn, "SELECT * FROM range(0, 100) as t(x) where x < ?")?;
        let statement = statements.next().unwrap()?.prepare(&conn, true)?;

        let mut query = statement.execute(crate::Parameters::Positional(&[&10]))?;

        while let Some(chunk) = query.next_chunk()? {
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
        let mut query = statement.execute(crate::Parameters::named(&[("hi", &15), ("lo", &10)]))?;

        let mut values = Vec::new();
        while let Some(chunk) = query.next_chunk()? {
            values.extend(chunk.get_vector_at::<i64>(0)?.iter()?.flatten().copied());
        }
        assert_eq!(values, (10..15).collect::<Vec<i64>>());

        Ok(())
    }

    #[test]
    fn test_prepared_statement_arrow() -> crate::Result<()> {
        let env = Environment::new()?;
        let db = env.open(StorageLocation::InMemory)?;
        let conn = db.connect()?;

        let mut statements = Statements::parse(
            &conn,
            "SELECT x FROM range(0, 10_000) as t(x) WHERE x >= $lo AND x < $hi ORDER BY x",
        )?;
        let statement = statements.next().unwrap()?.prepare(&conn, true)?;

        // Bind in a different order than they appear in the SQL, so binding
        // only succeeds if the names reach DuckDB intact.
        let mut query = statement.execute_arrow(crate::Parameters::named(&[("hi", &9000), ("lo", &42)]), Some(10))?;

        let mut total_len = 0;
        let mut total_batches = 0;
        while let Some(array) = query.next_array()? {
            // Inspect through arrow-rs, as a caller using its FFI types would.
            let array = unsafe { std::mem::transmute::<ffi::ArrowArray, FFI_ArrowArray>(array.into_raw()) };
            assert_eq!(array.num_children(), 1);
            total_len += array.len();
            total_batches += 1;
        }

        assert_eq!(total_len, 9000 - 42);
        assert_eq!(total_batches, 896);

        Ok(())
    }

    #[test]
    fn test_append_column_data_collection() -> crate::Result<()> {
        let env = Environment::new()?;
        let db = env.open(StorageLocation::InMemory)?;
        let conn = db.connect()?;

        let cdc = ColumnDataCollection::new(&conn, [(String::logical_type(&conn)?, "title")])?;

        let mut cdc = cdc.to_append()?;
        let mut chunk = DataChunk::create(&conn, &[String::logical_type(&conn)?], true)?;

        let mut vec = chunk.get_vector_at_mut::<String>(0)?;
        vec.set_size(3)?;
        vec.write(0, Some("first"))?;
        vec.write(1, None)?;
        vec.write(2, Some("third"))?;

        cdc.append(&mut chunk)?;

        let cdc = cdc.to_normal();

        let mut statements = Statements::parse(&conn, "SELECT title from cdc")?;
        let statement = statements.next().unwrap()?.add_collection(cdc, "cdc")?;

        let mut query = statement.query(&conn, Parameters::None)?;

        while let Some(chunk) = query.next_chunk()? {
            let vec = chunk.get_vector_at::<String>(0)?;

            assert_eq!(vec.len(), 3);

            assert_eq!(vec.get(0)?, Some("first"));
            assert_eq!(vec.get(1)?, None);
            assert_eq!(vec.get(2)?, Some("third"));
        }

        Ok(())
    }

    #[test]
    fn test_collection_outlives_statement() -> crate::Result<()> {
        let env = Environment::new()?;
        let db = env.open(StorageLocation::InMemory)?;
        let conn = db.connect()?;

        let mut cdc = ColumnDataCollection::new(&conn, [(i64::logical_type(&conn)?, "x")])?.to_append()?;
        let mut chunk = DataChunk::create(&conn, &[i64::logical_type(&conn)?], true)?;
        let mut vec = chunk.get_vector_at_mut::<i64>(0)?;
        vec.set_size(2)?;
        vec.write(0, Some(1))?;
        vec.write(1, Some(2))?;
        cdc.append(&mut chunk)?;

        let statement = Statements::parse(&conn, "SELECT sum(x)::BIGINT FROM cdc")?
            .next()
            .unwrap()?
            .add_collection(cdc.to_normal(), "cdc")?;
        // Both outlive the statement that owns the collection.
        let prepared = statement.prepare(&conn, false)?;
        let mut query = statement.query(&conn, Parameters::None)?;
        drop(statement);

        let chunk = query.next_chunk()?.expect("one chunk");
        assert_eq!(chunk.get_vector_at::<i64>(0)?.get(0)?.copied(), Some(3));
        drop(query);

        let chunk = prepared.execute(Parameters::None)?.next_chunk()?.expect("one chunk");
        assert_eq!(chunk.get_vector_at::<i64>(0)?.get(0)?.copied(), Some(3));

        Ok(())
    }
}

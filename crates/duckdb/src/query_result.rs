//! Lazy, streaming query results.

use std::{os::raw::c_char, thread::sleep, time::Duration};

use libduckdb_sys::{self as ffi, ArrowArrayStream, duckdb_v2_str};

use crate::{
    Result,
    builder_helpers::{ffi_enum_redeclaration, into_opaque},
    check_api_call, check_api_call_no_err,
    connection::Connection,
    data_chunk::DataChunk,
    error::{DuckDBError, Error},
    schema::Schema,
};

ffi_enum_redeclaration! {
    #[allow(missing_docs)]
    pub enum StatementType <- ffi::DUCKDB_V2_STATEMENT_TYPE {
            Invalid = DUCKDB_V2_STATEMENT_TYPE_INVALID,
            Select = DUCKDB_V2_STATEMENT_TYPE_SELECT,
            Insert = DUCKDB_V2_STATEMENT_TYPE_INSERT,
            Update = DUCKDB_V2_STATEMENT_TYPE_UPDATE,
            Create = DUCKDB_V2_STATEMENT_TYPE_CREATE,
            Delete = DUCKDB_V2_STATEMENT_TYPE_DELETE,
            Prepare = DUCKDB_V2_STATEMENT_TYPE_PREPARE,
            Execute = DUCKDB_V2_STATEMENT_TYPE_EXECUTE,
            Alter = DUCKDB_V2_STATEMENT_TYPE_ALTER,
            Transaction = DUCKDB_V2_STATEMENT_TYPE_TRANSACTION,
            Copy = DUCKDB_V2_STATEMENT_TYPE_COPY,
            Analyze = DUCKDB_V2_STATEMENT_TYPE_ANALYZE,
            VariableSet = DUCKDB_V2_STATEMENT_TYPE_VARIABLE_SET,
            CreateFunc = DUCKDB_V2_STATEMENT_TYPE_CREATE_FUNC,
            Explain = DUCKDB_V2_STATEMENT_TYPE_EXPLAIN,
            Drop = DUCKDB_V2_STATEMENT_TYPE_DROP,
            Export = DUCKDB_V2_STATEMENT_TYPE_EXPORT,
            Pragma = DUCKDB_V2_STATEMENT_TYPE_PRAGMA,
            Vacuum = DUCKDB_V2_STATEMENT_TYPE_VACUUM,
            Call = DUCKDB_V2_STATEMENT_TYPE_CALL,
            Set = DUCKDB_V2_STATEMENT_TYPE_SET,
            Load = DUCKDB_V2_STATEMENT_TYPE_LOAD,
            Relation = DUCKDB_V2_STATEMENT_TYPE_RELATION,
            Extension = DUCKDB_V2_STATEMENT_TYPE_EXTENSION,
            LogicalPlan = DUCKDB_V2_STATEMENT_TYPE_LOGICAL_PLAN,
            Attach = DUCKDB_V2_STATEMENT_TYPE_ATTACH,
            Detach = DUCKDB_V2_STATEMENT_TYPE_DETACH,
            Multi = DUCKDB_V2_STATEMENT_TYPE_MULTI,
            CopyDatabase = DUCKDB_V2_STATEMENT_TYPE_COPY_DATABASE,
            UpdateExtensions = DUCKDB_V2_STATEMENT_TYPE_UPDATE_EXTENSIONS,
            MergeInto = DUCKDB_V2_STATEMENT_TYPE_MERGE_INTO,
            Connect = DUCKDB_V2_STATEMENT_TYPE_CONNECT,
            Disconnect = DUCKDB_V2_STATEMENT_TYPE_DISCONNECT,
            ExternalResource = DUCKDB_V2_STATEMENT_TYPE_EXTERNAL_RESOURCE,
    }
}

ffi_enum_redeclaration! {
    /// The shape of an executed query's output.
    pub enum ResultType <- ffi::DUCKDB_V2_RESULT_TYPE {
        /// Rows and columns are available from the result stream.
        QueryResult = DUCKDB_V2_RESULT_TYPE_QUERY_RESULT,
        /// The result reports the number of rows changed by a data-modification statement.
        ChangedRows = DUCKDB_V2_RESULT_TYPE_CHANGED_ROWS,
        /// The statement produces no row output.
        Nothing = DUCKDB_V2_RESULT_TYPE_NOTHING,
    }
}

/// The outcome of one incremental [`QueryResult::step`].
#[derive(Debug)]
pub enum QueryResultStep {
    /// No chunk is ready; wait before stepping again.
    Waiting,
    /// The result is exhausted.
    Finished,
    /// The query was interrupted.
    Canceled,
    /// An owned data chunk is ready.
    Chunk(DataChunk),
}

/// A lazy stream produced by executing a statement.
///
/// Iteration blocks for each [`DataChunk`], while [`QueryResult::step`] exposes
/// incremental execution. Side-effecting statements must be consumed or
/// [`QueryResult::drain`]ed to take effect. A connection supports one live
/// result at a time.
///
/// # Example
/// ```
/// use duckdb_rs::{
///     Parameters,
///     environment::Environment,
///     environment::StorageLocation,
/// };
///
/// # fn main() -> duckdb_rs::Result<()> {
/// let env = Environment::new()?;
/// let db = env.open(StorageLocation::InMemory)?;
/// let conn = db.connect()?;
/// let mut statements = conn.parse("SELECT * FROM range(3)")?;
/// let statement = statements.next().expect("expected a statement")?;
///
/// for chunk in conn.query(statement, Parameters::None)? {
///     println!("Fetched {} row(s)", chunk?.row_count()?);
/// }
/// # Ok(())
/// # }
/// ```
pub struct QueryResult<'a> {
    /// Ties the result lifetime to its connection.
    pub phantom: std::marker::PhantomData<&'a mut Connection>,
    /// The owned DuckDB result handle.
    pub handle: ffi::duckdb_v2_result_handle,
}

impl QueryResult<'_> {
    /// Run one bounded unit of execution and return its state.
    pub fn step(&mut self) -> Result<QueryResultStep> {
        let mut step = ffi::DUCKDB_V2_RESULT_STEP_STATUS::DUCKDB_V2_RESULT_STEP_STATUS_WAITING;

        let chunk: ffi::duckdb_v2_data_chunk_handle =
            check_api_call!(ffi::duckdb_v2_result_step, self.handle, RET, &mut step)?;

        Ok(match step {
            ffi::DUCKDB_V2_RESULT_STEP_STATUS::DUCKDB_V2_RESULT_STEP_STATUS_WAITING => QueryResultStep::Waiting,
            ffi::DUCKDB_V2_RESULT_STEP_STATUS::DUCKDB_V2_RESULT_STEP_STATUS_FINISHED => QueryResultStep::Finished,
            ffi::DUCKDB_V2_RESULT_STEP_STATUS::DUCKDB_V2_RESULT_STEP_STATUS_CANCELLED => QueryResultStep::Canceled,
            ffi::DUCKDB_V2_RESULT_STEP_STATUS::DUCKDB_V2_RESULT_STEP_STATUS_CHUNK => {
                QueryResultStep::Chunk(DataChunk {
                    handle: chunk,
                    is_owned: true,
                    is_writable: false,
                })
            }
            _ => unimplemented!("Unknown result step: {:?}", step),
        })
    }

    /// Block until another execution step can make progress.
    pub fn wait(&self) -> Result<()> {
        check_api_call!(ffi::duckdb_v2_result_wait, self.handle)
    }

    /// Run to completion, discarding rows and returning the changed-row count.
    ///
    /// The count is nonzero only for a changed-rows result whose count chunk has
    /// not already been consumed. Draining an already finished result succeeds.
    pub fn drain(&mut self) -> Result<usize> {
        let count: u64 = check_api_call!(ffi::duckdb_v2_result_drain, self.handle, RET)?;

        Ok(count as usize)
    }

    /// Return the SQL statement type that produced this result.
    pub fn statement_type(&self) -> Result<StatementType> {
        check_api_call!(ffi::duckdb_v2_result_get_statement_type, self.handle, RET)?.try_into()
    }

    /// Return an owned copy of the result schema.
    pub fn schema(&self) -> Result<Schema> {
        let handle = check_api_call!(ffi::duckdb_v2_result_get_schema, self.handle, RET)?;

        Ok(Schema { handle })
    }

    #[cfg(feature = "capi-v2-p2")]
    /// Consume the result into a lazy Arrow C Data Interface stream.
    ///
    /// The stream contains only rows not already consumed. A `batch_size` of
    /// zero selects DuckDB's default of 131,072 rows. The caller must invoke
    /// the stream's `release` callback to unpin the transaction and make the
    /// connection available for another query.
    pub fn to_arrow_stream(mut self, batch_size: usize) -> Result<ArrowArrayStream> {
        check_api_call!(
            ffi::duckdb_v2_result_to_arrow_stream,
            &mut self.handle,
            batch_size as u64,
            RET
        )
    }

    /// Return whether the result contains rows, changed rows, or no output.
    pub fn result_type(&self) -> Result<ResultType> {
        check_api_call!(ffi::duckdb_v2_result_get_result_type, self.handle, RET)?.try_into()
    }

    unsafe extern "C" fn copy_render_box(
        text: ffi::duckdb_v2_str,
        user_data: *mut std::os::raw::c_void,
        err: *mut ffi::duckdb_v2_error_info_handle,
    ) {
        let string = unsafe { &mut *(user_data as *mut String) };

        string.push_str(text.into());
    }

    /// Consume the remaining rows and render DuckDB's box table.
    ///
    /// Zero selects sizing defaults; `render_mode` is `0` for rows or `1` for
    /// columns, and `limit` describes a limit already applied by the caller.
    pub fn to_text(
        &mut self,
        max_rows: usize,
        max_width: usize,
        max_col_width: usize,
        render_mode: usize,
        limit: usize,
    ) -> Result<String> {
        let null_value: duckdb_v2_str = "NULL".into();

        let mut string = String::new();

        check_api_call!(
            ffi::duckdb_v2_result_render_box,
            &mut self.handle,
            max_rows as u64,
            max_width as u64,
            max_col_width as u64,
            null_value,
            render_mode as u64,
            limit as u64,
            Some(Self::copy_render_box),
            &mut string as *mut String as *mut std::os::raw::c_void,
        )?;

        Ok(string)
    }
}

impl Drop for QueryResult<'_> {
    fn drop(&mut self) {
        check_api_call_no_err!(ffi::duckdb_v2_result_destroy, &mut self.handle).unwrap();
    }
}

impl Iterator for QueryResult<'_> {
    type Item = Result<DataChunk>;

    /// Block until the next owned chunk or the end of the stream.
    fn next(&mut self) -> Option<Self::Item> {
        let result: Result<ffi::duckdb_v2_data_chunk_handle> =
            check_api_call!(ffi::duckdb_v2_result_fetch_chunk, self.handle, RET);

        match result {
            Ok(out_chunk) => {
                if out_chunk.is_null() {
                    None
                } else {
                    Some(Ok(DataChunk {
                        handle: out_chunk,
                        is_owned: true,
                        is_writable: false,
                    }))
                }
            }
            Err(e) => Some(Err(e)),
        }
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use libduckdb_sys::{ArrowArray, ArrowArrayStream};

    use crate::{
        Parameters,
        environment::{Environment, StorageLocation},
        query_result::{ResultType, StatementType},
    };

    #[test]
    fn test_query_result_helpers() -> crate::Result<()> {
        let env = Environment::new()?;
        let db = env.open(StorageLocation::InMemory)?;
        let conn = db.connect()?;

        let mut statements = conn.parse(
            "
        CREATE TABLE test (id INTEGER);
        INSERT INTO test VALUES (1), (2), (3);
        ",
        )?;
        let stmt = statements.next().unwrap()?;
        let result = conn.execute(stmt, Parameters::None)?;
        assert_eq!(result, 0);

        let stmt = statements.next().unwrap()?;
        let mut result = conn.query(stmt, Parameters::None)?;

        assert_eq!(result.result_type()?, ResultType::ChangedRows);

        assert_eq!(result.statement_type().unwrap(), StatementType::Insert);

        let schema = result.schema().unwrap();

        assert_eq!(schema.len()?, 1);

        let rows_changed = result.drain()?;
        assert_eq!(rows_changed, 3);

        Ok(())
    }

    #[test]
    fn test_query_to_textbox() -> crate::Result<()> {
        let env = Environment::new()?;
        let db = env.open(StorageLocation::InMemory)?;
        let conn = db.connect()?;

        let mut statements = conn.parse(
            "
        CREATE TABLE test (id INTEGER);
        INSERT INTO test VALUES (1), (2), (3);
        SELECT * FROM test;
        ",
        )?;
        let stmt = statements.next().unwrap()?;
        let result = conn.execute(stmt, Parameters::None)?;
        assert_eq!(result, 0);

        let stmt = statements.next().unwrap()?;
        let result = conn.execute(stmt, Parameters::None)?;
        assert_eq!(result, 3);

        let stmt = statements.next().unwrap()?;
        let mut result = conn.query(stmt, Parameters::None)?;

        let text = result.to_text(10, 80, 20, 0, 0)?;
        println!("{}", text);

        assert_eq!(
            text,
            "┌───────┐
│  id   │
│ int32 │
├───────┤
│     1 │
│     2 │
│     3 │
└───────┘
"
        );

        Ok(())
    }

    #[test]
    #[cfg(feature = "capi-v2-p2")]
    fn test_query_to_arrow_stream() -> crate::Result<()> {
        let env = crate::Environment::new()?;
        let db = env.open(crate::StorageLocation::InMemory)?;
        let conn = db.connect()?;

        let mut statements = conn.parse(
            "
        CREATE TABLE test (id INTEGER);
        INSERT INTO test VALUES (1), (2), (3);
        SELECT * FROM test;
        ",
        )?;
        let stmt = statements.next().unwrap()?;
        let result = conn.execute(stmt, Parameters::None)?;
        assert_eq!(result, 0);

        let stmt = statements.next().unwrap()?;
        let result = conn.execute(stmt, Parameters::None)?;
        assert_eq!(result, 3);

        let stmt = statements.next().unwrap()?;
        let result = conn.query(stmt, Parameters::None)?;
        let mut arrow_stream = result.to_arrow_stream(1024)?;

        let mut array = ArrowArray {
            length: 0,
            null_count: 0,
            offset: 0,
            n_buffers: 0,
            n_children: 0,
            buffers: std::ptr::null_mut(),
            children: std::ptr::null_mut(),
            dictionary: std::ptr::null_mut(),
            release: None,
            private_data: std::ptr::null_mut(),
        };

        let res = unsafe {
            arrow_stream.get_next.unwrap()(
                &mut arrow_stream as *mut ArrowArrayStream,
                &mut array as *mut ArrowArray,
            )
        };

        assert_eq!(res, 0);

        assert_eq!(array.length, 3);

        unsafe {
            arrow_stream.release.unwrap()(&mut arrow_stream);
        }

        unsafe {
            array.release.unwrap()(&mut array);
        }

        Ok(())
    }
}

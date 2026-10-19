//! Lazy, streaming query results.

use std::marker::PhantomData;

use libduckdb_sys::v2::duckdb_v2_str;

use crate::builder_helpers::copy_render_box;
use crate::error::Error;
use crate::{
    DropHandle, Result,
    arrow::{ArrowArray, ArrowSchema, ArrowStream},
    builder_helpers::ffi_enum_redeclaration,
    check_api_call,
    connection::Connection,
    data_chunk::DataChunk,
    ffi,
    schema::Schema,
    statement::BoundCollections,
};

/*
duckdb_v2_result_destroy
duckdb_v2_result_drain
duckdb_v2_result_fetch_chunk
duckdb_v2_result_get_result_type
duckdb_v2_result_get_statement_type
duckdb_v2_result_step
duckdb_v2_result_wait
duckdb_v2_arrow_result_destroy
duckdb_v2_arrow_result_drain
duckdb_v2_arrow_result_fetch_array
duckdb_v2_arrow_result_get_result_type
duckdb_v2_arrow_result_get_statement_type
duckdb_v2_arrow_result_step
duckdb_v2_arrow_result_wait
*/

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

/// Output produced by a query result's `step` and fetch calls.
trait ResultItem: Sized {
    /// The C API out-parameter.
    type Raw;

    /// The out-parameter's value before the call, which the C API may leave unwritten.
    fn empty_raw() -> Self::Raw;

    /// Take ownership, or return `None` for the C API's "no output" marker.
    fn from_raw(raw: Self::Raw) -> Result<Option<Self>>;
}

impl ResultItem for DataChunk<'static> {
    type Raw = ffi::duckdb_v2_data_chunk_handle;

    fn empty_raw() -> Self::Raw {
        std::ptr::null_mut()
    }

    fn from_raw(raw: Self::Raw) -> Result<Option<Self>> {
        if raw.is_null() {
            Ok(None)
        } else {
            DataChunk::new(raw, false).map(Some)
        }
    }
}

impl ResultItem for ArrowArray {
    type Raw = ffi::ArrowArray;

    fn empty_raw() -> Self::Raw {
        ffi::ArrowArray::empty()
    }

    fn from_raw(raw: Self::Raw) -> Result<Option<Self>> {
        // SAFETY: DuckDB filled `raw`; a released array means none was ready.
        Ok(raw.release.is_some().then(|| unsafe { ArrowArray::from_raw(raw) }))
    }
}

/// Define a query result type over the `$prefix`-family of C API result functions.
///
/// Generates the struct, its `step` outcome enum, `Drop`, and the methods whose
/// only difference is the item type: `step`, `wait`, `$next`, `drain`,
/// `statement_type`, `result_type`.
macro_rules! query_result {
    (
        $(#[$meta:meta])*
        pub struct $name:ident;
        prefix: $prefix:ident,
        item: $item:ty,
        step: $step:ident::$variant:ident,
        next: $next:ident,
        fetch: $fetch:ident,
        noun: $noun:literal $(,)?
    ) => {
        paste::paste! {
            #[doc = concat!("The outcome of one incremental [`", stringify!($name), "::step`].")]
            #[derive(Debug)]
            pub enum $step {
                #[doc = concat!("No ", $noun, " is ready; wait before stepping again.")]
                Waiting,
                /// The result is exhausted.
                Finished,
                /// The query was interrupted.
                Canceled,
                #[doc = concat!("An owned ", $noun, " is ready.")]
                $variant($item),
            }

            $(#[$meta])*
            ///
            /// A result is not `Send`: stepping it runs the query on the connection, which
            /// must not race with the connection's `&self` methods on another thread.
            ///
            #[doc = concat!(
                "```compile_fail,E0277\n",
                "fn assert_send<T: Send>() {}\n",
                "assert_send::<duckdb_neo::query_result::", stringify!($name), "<'static>>();\n",
                "```",
            )]
            #[derive(Debug, DropHandle)]
            #[drop_handle(ffi::[<$prefix _destroy>])]
            pub struct $name<'a> {
                pub(crate) handle: ffi::[<$prefix _handle>],
                // DuckDB reads these while the result executes.
                _collections: BoundCollections<'a>,
                phantom: PhantomData<&'a mut Connection>,
            }

            impl<'a> $name<'a> {
                pub(crate) fn new(handle: ffi::[<$prefix _handle>], collections: BoundCollections<'a>) -> Self {
                    Self {
                        handle,
                        _collections: collections,
                        phantom: PhantomData,
                    }
                }

                /// Run one bounded unit of execution and return its state.
                pub fn step(&mut self) -> Result<$step> {
                    let mut status = ffi::DUCKDB_V2_RESULT_STEP_STATUS::DUCKDB_V2_RESULT_STEP_STATUS_WAITING;

                    let mut raw = <$item as ResultItem>::empty_raw();
                    check_api_call!(ffi::[<$prefix _step>], self.handle, &mut raw, &mut status)?;

                    Ok(match status {
                        ffi::DUCKDB_V2_RESULT_STEP_STATUS::DUCKDB_V2_RESULT_STEP_STATUS_WAITING => $step::Waiting,
                        ffi::DUCKDB_V2_RESULT_STEP_STATUS::DUCKDB_V2_RESULT_STEP_STATUS_FINISHED => $step::Finished,
                        ffi::DUCKDB_V2_RESULT_STEP_STATUS::DUCKDB_V2_RESULT_STEP_STATUS_CANCELLED => $step::Canceled,
                        ffi::DUCKDB_V2_RESULT_STEP_STATUS::DUCKDB_V2_RESULT_STEP_STATUS_CHUNK => $step::$variant(
                            <$item as ResultItem>::from_raw(raw)?
                                .ok_or_else(|| Error::api_error(concat!("Result step reported a ", $noun, " but returned none")))?,
                        ),
                        _ => unimplemented!("Unknown result step: {:?}", status),
                    })
                }

                /// Block until another execution step can make progress.
                pub fn wait(&self) -> Result<()> {
                    check_api_call!(ffi::[<$prefix _wait>], self.handle)
                }

                #[doc = concat!("Block until the next owned ", $noun, " or the end of the stream.")]
                pub fn $next(&mut self) -> Result<Option<$item>> {
                    let mut raw = <$item as ResultItem>::empty_raw();
                    check_api_call!(ffi::[<$prefix _ $fetch>], self.handle, &mut raw)?;

                    <$item as ResultItem>::from_raw(raw)
                }

                /// Run to completion, discarding rows and returning the changed-row count.
                ///
                #[doc = concat!(
                    "The count is nonzero only for a changed-rows result whose count ", $noun, " has\n",
                    "not already been consumed. Draining an already finished result succeeds.",
                )]
                pub fn drain(&mut self) -> Result<usize> {
                    check_api_call!(ffi::[<$prefix _drain>], self.handle, RET).map(|count| count as usize)
                }

                /// Return the SQL statement type that produced this result.
                pub fn statement_type(&self) -> Result<StatementType> {
                    check_api_call!(ffi::[<$prefix _get_statement_type>], self.handle, RET)?.try_into()
                }

                /// Return whether the result contains rows, changed rows, or no output.
                pub fn result_type(&self) -> Result<ResultType> {
                    check_api_call!(ffi::[<$prefix _get_result_type>], self.handle, RET)?.try_into()
                }
            }
        }
    };
}

query_result! {
    /// A lazy stream of [`DataChunk`]s produced by executing a statement.
    ///
    /// [`QueryResult::next_chunk`] blocks for each [`DataChunk`], while [`QueryResult::step`] exposes
    /// incremental execution. Side-effecting statements must be consumed or
    /// [`QueryResult::drain`]ed to take effect. A connection supports one live
    /// result at a time.
    ///
    /// # Example
    /// ```
    /// use duckdb_neo::{
    ///     Parameters,
    ///     environment::Environment,
    ///     environment::StorageLocation,
    /// };
    ///
    /// # fn main() -> duckdb_neo::Result<()> {
    /// let env = Environment::new()?;
    /// let db = env.open(StorageLocation::InMemory)?;
    /// let conn = db.connect()?;
    /// let mut statements = conn.parse("SELECT * FROM range(3)")?;
    /// let statement = statements.next().expect("expected a statement")?;
    ///
    /// let mut result = conn.query(statement, Parameters::None)?;
    /// while let Some(chunk) = result.next_chunk()? {
    ///     println!("Fetched {} row(s)", chunk.row_count()?);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub struct QueryResult;
    prefix: duckdb_v2_result,
    item: DataChunk<'static>,
    step: QueryResultStep::Chunk,
    next: next_chunk,
    fetch: fetch_chunk,
    noun: "chunk",
}

query_result! {
    /// A lazy stream of Arrow arrays produced by executing a statement.
    ///
    /// Created by [`Connection::arrow_query`]. [`ArrowQueryResult::next_array`]
    /// blocks for each array, while [`ArrowQueryResult::step`] exposes
    /// incremental execution. Arrays and schemas are owned by the caller and
    /// stay valid after the result is dropped. Side-effecting statements must be
    /// consumed or [`ArrowQueryResult::drain`]ed to take effect. A connection
    /// supports one live result at a time.
    ///
    /// # Example
    /// ```
    /// use duckdb_neo::{
    ///     Parameters,
    ///     environment::Environment,
    ///     environment::StorageLocation,
    /// };
    ///
    /// # fn main() -> duckdb_neo::Result<()> {
    /// let env = Environment::new()?;
    /// let db = env.open(StorageLocation::InMemory)?;
    /// let conn = db.connect()?;
    ///
    /// let mut result = conn.arrow_query("SELECT * FROM range(3)", Parameters::None, None)?;
    /// while let Some(array) = result.next_array()? {
    ///     println!("Fetched {} row(s)", array.len());
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub struct ArrowQueryResult;
    prefix: duckdb_v2_arrow_result,
    item: ArrowArray,
    step: ArrowResultStep::Array,
    next: next_array,
    fetch: fetch_array,
    noun: "array",
}

impl QueryResult<'_> {
    /// Return an owned copy of the result schema.
    pub fn schema(&self) -> Result<Schema> {
        check_api_call!(ffi::duckdb_v2_result_get_schema, self.handle, RET).map(|handle| Schema { handle })
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
            &null_value,
            render_mode as u64,
            limit as u64,
            Some(copy_render_box),
            &mut string as *mut String as *mut std::os::raw::c_void,
        )?;

        Ok(string)
    }
}

impl<'a> ArrowQueryResult<'a> {
    /// Return the Arrow schema of the result's arrays.
    pub fn schema(&self) -> Result<ArrowSchema> {
        // SAFETY: DuckDB returns an owned schema.
        check_api_call!(ffi::duckdb_v2_arrow_result_get_schema, self.handle, RET)
            .map(|raw| unsafe { ArrowSchema::from_raw(raw) })
    }

    /// Consume the result into a lazy Arrow C Data Interface stream.
    ///
    /// The stream contains only arrays not already fetched. Dropping the stream
    /// releases it and makes the connection available for another query.
    ///
    /// The query runs while the stream is read, so the stream borrows the connection:
    ///
    /// ```compile_fail
    /// # use duckdb_neo::{Parameters, environment::{Environment, StorageLocation}};
    /// # fn main() -> duckdb_neo::Result<()> {
    /// # let db = Environment::new()?.open(StorageLocation::InMemory)?;
    /// let mut conn = db.connect()?;
    /// let stream = conn.arrow_query("SELECT 42", Parameters::None, None)?.to_arrow_stream()?;
    /// conn.set_option("threads", "1", None)?; // error: `conn` is still borrowed by `stream`
    /// drop(stream);
    /// # Ok(())
    /// # }
    /// ```
    pub fn to_arrow_stream(mut self) -> Result<ArrowStream<'a>> {
        // On success the stream owns the result and the handle is nulled, so `Drop` is a no-op.
        check_api_call!(ffi::duckdb_v2_arrow_result_to_arrow_c_stream, &mut self.handle, RET)
            .map(|stream| ArrowStream::new(stream, std::mem::take(&mut self._collections)))
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use std::mem::transmute;

    use arrow_array::{
        ffi::FFI_ArrowSchema,
        ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream},
    };
    use libduckdb_sys::v2 as ffi;

    use crate::{
        Parameters,
        environment::{Environment, StorageLocation},
        query_result::{ArrowResultStep, ResultType, StatementType},
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
    fn test_chunks_outlive_result() -> crate::Result<()> {
        let env = Environment::new()?;
        let db = env.open(StorageLocation::InMemory)?;
        let conn = db.connect()?;

        let mut result = conn.query("SELECT * FROM range(5000)", Parameters::None)?;
        let first = result.next_chunk()?.unwrap();
        let second = result.next_chunk()?.unwrap();
        drop(result);

        assert_eq!(first.get_vector_at::<i64>(0)?.get(0)?, Some(&0));
        assert_eq!(second.get_vector_at::<i64>(0)?.get(0)?, Some(&2048));

        // The temporary result is dropped at the end of the statement, freeing the connection.
        let chunk = conn.query("SELECT 42", Parameters::None)?.next_chunk()?.unwrap();
        assert_eq!(conn.execute("SELECT 1", Parameters::None)?, 0);
        assert_eq!(chunk.get_vector_at::<i32>(0)?.get(0)?, Some(&42));

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
    fn test_arrow_query_result() -> crate::Result<()> {
        let env = Environment::new()?;
        let db = env.open(StorageLocation::InMemory)?;
        let conn = db.connect()?;

        conn.execute("CREATE TABLE test (id INTEGER)", Parameters::None)?;
        let mut result = conn.arrow_query("INSERT INTO test FROM range(3)", Parameters::None, None)?;
        assert_eq!(result.result_type()?, ResultType::ChangedRows);
        assert_eq!(result.statement_type()?, StatementType::Insert);
        assert_eq!(result.drain()?, 3);
        drop(result);

        let mut result = conn.arrow_query("SELECT * FROM range(10)", Parameters::None, Some(4))?;
        let schema = unsafe { transmute::<ffi::ArrowSchema, FFI_ArrowSchema>(result.schema()?.into_raw()) };
        assert_eq!(schema.children().count(), 1);

        let mut lengths = Vec::new();
        loop {
            match result.step()? {
                ArrowResultStep::Array(array) => lengths.push(array.len()),
                ArrowResultStep::Waiting => result.wait()?,
                ArrowResultStep::Finished => break,
                ArrowResultStep::Canceled => panic!("query canceled unexpectedly"),
            }
        }
        assert_eq!(lengths.iter().sum::<usize>(), 10);
        assert!(lengths.iter().all(|len| *len <= 4));
        assert!(result.next_array()?.is_none());

        let mut result = conn.arrow_query("SELECT $1::INTEGER", Parameters::positional(&[&42_i32]), None)?;
        let array = result.next_array()?.expect("one array");
        assert_eq!(array.len(), 1);
        assert!(result.next_array()?.is_none());

        Ok(())
    }

    #[test]
    fn test_arrow_query_to_arrow_stream() -> crate::Result<()> {
        let env = Environment::new()?;
        let db = env.open(StorageLocation::InMemory)?;
        let conn = db.connect()?;

        let result = conn.arrow_query("SELECT * FROM range(3)", Parameters::None, Some(1024))?;
        let arrow_stream = result.to_arrow_stream()?;

        // SAFETY: the reader is dropped below, before the connection is used again.
        let (stream, _collections) = unsafe { arrow_stream.into_raw() };
        // Read through arrow-rs, as a caller using its FFI types would.
        let stream = unsafe { transmute::<ffi::ArrowArrayStream, FFI_ArrowArrayStream>(stream) };
        let mut reader = ArrowArrayStreamReader::try_new(stream).expect("valid Arrow stream");
        let batch = reader.next().expect("one batch").expect("no stream error");
        assert_eq!(batch.num_rows(), 3);

        drop(reader);

        // Dropping an unread stream releases it and frees the connection.
        let unread = conn
            .arrow_query("SELECT 1", Parameters::None, None)?
            .to_arrow_stream()?;
        drop(unread);
        assert_eq!(conn.execute("SELECT 1", Parameters::None)?, 0);

        Ok(())
    }
}

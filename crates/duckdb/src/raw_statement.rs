use std::{cell::OnceCell, collections::HashMap, ffi::CStr, ops::Deref, ptr, rc::Rc, sync::Arc};

use arrow::{
    array::StructArray,
    datatypes::{DataType, Schema, SchemaRef},
    ffi::{FFI_ArrowArray, FFI_ArrowSchema, from_ffi},
};

use super::{Result, ffi};
use crate::{
    Error,
    core::{LogicalTypeHandle, LogicalTypeId},
    error::result_from_duckdb_arrow,
    types::Type,
};
#[cfg(feature = "polars")]
use polars_core::utils::arrow as polars_arrow;

/// Private newtype for DuckDB prepared statements that finalize themselves when dropped.
///
/// # Thread Safety
///
/// `RawStatement` is `Send` but not `Sync`:
/// - `Send` because it owns all its data and can be safely moved between threads
/// - Not `Sync` because DuckDB prepared statements don't support concurrent access
#[derive(Debug)]
pub struct RawStatement {
    ptr: ffi::duckdb_prepared_statement,
    result: Option<ffi::duckdb_arrow>,
    duckdb_result: Option<ffi::duckdb_result>,
    schema: Option<SchemaRef>,
    // Cached DuckDB logical ids for result columns. Arrow reports HUGEINT,
    // UHUGEINT, and scale-zero DECIMAL through the same decimal shape.
    result_column_logical_ids: Option<Box<[LogicalTypeId]>>,
    column_name_cache: OnceCell<HashMap<Box<str>, usize>>,
    // Tracks whether the duckdb_result is truly streaming or materialized.
    // This is needed because some statements (like CALL) return materialized
    // results even when execute_streaming is called.
    is_streaming: bool,
    // Cached SQL (trimmed) that we use as the key when we're in the statement
    // cache. This is None for statements which didn't come from the statement
    // cache.
    //
    // This is probably the same as `self.sql()` in most cases, but we don't
    // care either way -- It's a better cache key as it is anyway since it's the
    // actual source we got from rust.
    //
    // One example of a case where the result of `sqlite_sql` and the value in
    // `statement_cache_key` might differ is if the statement has a `tail`.
    statement_cache_key: Option<Arc<str>>,
}

impl RawStatement {
    #[inline]
    pub unsafe fn new(stmt: ffi::duckdb_prepared_statement) -> Self {
        Self {
            ptr: stmt,
            result: None,
            schema: None,
            result_column_logical_ids: None,
            column_name_cache: OnceCell::new(),
            duckdb_result: None,
            is_streaming: false,
            statement_cache_key: None,
        }
    }

    #[inline]
    pub fn is_null(&self) -> bool {
        self.ptr.is_null()
    }

    #[inline]
    pub unsafe fn ptr(&self) -> ffi::duckdb_prepared_statement {
        self.ptr
    }

    #[inline]
    pub(crate) fn set_statement_cache_key(&mut self, p: impl Into<Arc<str>>) {
        self.statement_cache_key = Some(p.into());
    }

    #[inline]
    pub(crate) fn statement_cache_key(&self) -> Option<Arc<str>> {
        self.statement_cache_key.clone()
    }

    #[inline]
    pub fn clear_bindings(&self) -> ffi::duckdb_state {
        unsafe { ffi::duckdb_clear_bindings(self.ptr) }
    }

    #[inline]
    pub fn result_unwrap(&self) -> ffi::duckdb_arrow {
        self.result.expect("The statement was not executed yet")
    }

    #[inline]
    pub fn row_count(&self) -> usize {
        unsafe { ffi::duckdb_arrow_row_count(self.result_unwrap()) as usize }
    }

    #[inline]
    pub fn step(&self) -> Option<StructArray> {
        let out = self.result?;
        unsafe {
            let mut arrays = FFI_ArrowArray::empty();
            if ffi::duckdb_query_arrow_array(
                out,
                &mut std::ptr::addr_of_mut!(arrays) as *mut _ as *mut ffi::duckdb_arrow_array,
            )
            .ne(&ffi::DuckDBSuccess)
            {
                return None;
            }

            if arrays.is_empty() {
                return None;
            }

            let mut schema = FFI_ArrowSchema::empty();
            if ffi::duckdb_query_arrow_schema(
                out,
                &mut std::ptr::addr_of_mut!(schema) as *mut _ as *mut ffi::duckdb_arrow_schema,
            ) != ffi::DuckDBSuccess
            {
                return None;
            }

            let array_data = from_ffi(arrays, &schema).expect("ok");
            let struct_array = StructArray::from(array_data);
            Some(struct_array)
        }
    }

    #[inline]
    pub fn streaming_step(&self, schema: SchemaRef) -> Option<StructArray> {
        if let Some(result) = self.duckdb_result {
            unsafe {
                // Use duckdb_stream_fetch_chunk for truly streaming results,
                // or duckdb_fetch_chunk for materialized results (e.g., from CALL statements)
                let mut out = if self.is_streaming {
                    ffi::duckdb_stream_fetch_chunk(result)
                } else {
                    ffi::duckdb_fetch_chunk(result)
                };

                if out.is_null() {
                    return None;
                }

                let mut arrays = FFI_ArrowArray::empty();
                ffi::duckdb_result_arrow_array(
                    result,
                    out,
                    &mut std::ptr::addr_of_mut!(arrays) as *mut _ as *mut ffi::duckdb_arrow_array,
                );

                ffi::duckdb_destroy_data_chunk(&mut out);

                if arrays.is_empty() {
                    return None;
                }

                let schema = FFI_ArrowSchema::try_from(schema.deref()).ok()?;
                let array_data = from_ffi(arrays, &schema).expect("ok");
                let struct_array = StructArray::from(array_data);
                return Some(struct_array);
            }
        }

        None
    }

    #[cfg(feature = "polars")]
    #[inline]
    pub(crate) fn step_polars(&self) -> Option<polars_arrow::array::StructArray> {
        let result = self.result?;

        unsafe {
            let mut ffi_arrow_array = polars_arrow::ffi::ArrowArray::empty();

            if ffi::duckdb_query_arrow_array(
                result,
                &mut std::ptr::addr_of_mut!(ffi_arrow_array) as *mut _ as *mut ffi::duckdb_arrow_array,
            )
            .ne(&ffi::DuckDBSuccess)
            {
                return None;
            }

            let mut ffi_arrow_schema = polars_arrow::ffi::ArrowSchema::empty();

            if ffi::duckdb_query_arrow_schema(
                result,
                &mut std::ptr::addr_of_mut!(ffi_arrow_schema) as *mut _ as *mut ffi::duckdb_arrow_schema,
            )
            .ne(&ffi::DuckDBSuccess)
            {
                return None;
            }

            let field =
                polars_arrow::ffi::import_field_from_c(&ffi_arrow_schema).expect("Failed to import Polars Arrow field");
            let import_array = polars_arrow::ffi::import_array_from_c(ffi_arrow_array, field.dtype);

            let array = match import_array {
                Ok(array) => array,
                // When array is empty, import_array_from_c returns ComputeError with message
                // "An ArrowArray of type X must have non-null children".
                Err(polars::error::PolarsError::ComputeError(msg)) if msg.to_string().contains("non-null children") => {
                    return None;
                }
                Err(err) => panic!("Failed to import Polars Arrow array from C: {err}"),
            };
            let struct_array = array
                .as_any()
                .downcast_ref::<polars_arrow::array::StructArray>()
                .expect("Failed to downcast Polars Arrow array to StructArray")
                .to_owned();

            Some(struct_array)
        }
    }

    // FIXME(mlafeldt): This currently panics if the query has not been executed yet.
    // The C API doesn't have a function to get the column count without executing the query first.
    #[inline]
    pub fn column_count(&self) -> usize {
        unsafe { ffi::duckdb_arrow_column_count(self.result_unwrap()) as usize }
    }

    #[inline]
    pub fn column_type(&self, idx: usize) -> DataType {
        self.schema().field(idx).data_type().to_owned()
    }

    #[inline]
    pub fn column_logical_type(&self, idx: usize) -> LogicalTypeHandle {
        self.try_column_logical_type(idx)
            .unwrap_or_else(|e| panic!("could not retrieve logical type for result column at index {idx}: {e}"))
    }

    #[inline]
    pub(crate) fn try_column_logical_type(&self, idx: usize) -> Result<LogicalTypeHandle> {
        unsafe {
            let ptr = ffi::duckdb_prepared_statement_column_logical_type(self.ptr, idx as u64);
            if ptr.is_null() {
                return Err(Error::DuckDBFailure(
                    ffi::Error::new(ffi::DuckDBError),
                    Some(format!(
                        "Could not retrieve logical type for result column at index {idx}"
                    )),
                ));
            }
            Ok(LogicalTypeHandle::new(ptr))
        }
    }

    #[inline]
    pub(crate) fn result_column_logical_id(&self, idx: usize) -> Option<LogicalTypeId> {
        let logical_ids = self.result_column_logical_ids.as_ref()?;
        debug_assert!(
            idx < logical_ids.len(),
            "result column logical-id cache is shorter than the result schema"
        );
        logical_ids.get(idx).copied()
    }

    #[inline]
    pub fn schema(&self) -> SchemaRef {
        self.schema.clone().unwrap()
    }

    #[inline]
    pub fn column_name(&self, idx: usize) -> Option<&String> {
        if idx >= self.column_count() {
            return None;
        }
        Some(self.schema.as_ref().unwrap().field(idx).name())
    }

    #[inline]
    pub fn column_index(&self, name: &str) -> Option<usize> {
        let cache = self.column_name_cache.get_or_init(|| self.build_column_name_cache());
        cache.get(&*name.to_ascii_lowercase()).copied()
    }

    fn build_column_name_cache(&self) -> HashMap<Box<str>, usize> {
        let schema = self.schema.as_ref().expect("The statement was not executed yet");
        let mut cache = HashMap::with_capacity(schema.fields().len());
        for (index, field) in schema.fields().iter().enumerate() {
            cache
                .entry(field.name().to_ascii_lowercase().into_boxed_str())
                .or_insert(index);
        }
        cache
    }

    /// NOTE: if execute failed, we shouldn't call any other methods which depends on result
    pub fn execute(&mut self) -> Result<usize> {
        self.reset_result();
        let result_column_logical_ids = self.validate_and_load_result_column_logical_ids()?;
        unsafe {
            let mut out: ffi::duckdb_arrow = ptr::null_mut();
            let rc = ffi::duckdb_execute_prepared_arrow(self.ptr, &mut out);
            result_from_duckdb_arrow(rc, out)?;

            let rows_changed = ffi::duckdb_arrow_rows_changed(out);
            let mut c_schema = Rc::into_raw(Rc::new(FFI_ArrowSchema::empty()));
            let rc = ffi::duckdb_query_arrow_schema(out, &mut c_schema as *mut _ as *mut ffi::duckdb_arrow_schema);
            if rc != ffi::DuckDBSuccess {
                Rc::from_raw(c_schema);
                result_from_duckdb_arrow(rc, out)?;
            }
            self.schema = Some(Arc::new(Schema::try_from(&*c_schema).unwrap()));
            Rc::from_raw(c_schema);

            self.result = Some(out);
            self.result_column_logical_ids = Some(result_column_logical_ids);
            Ok(rows_changed as usize)
        }
    }

    pub fn execute_streaming(&mut self) -> Result<()> {
        self.reset_result();
        let result_column_logical_ids = self.validate_and_load_result_column_logical_ids()?;
        unsafe {
            let mut out: ffi::duckdb_result = std::mem::zeroed();

            let rc = ffi::duckdb_execute_prepared_streaming(self.ptr, &mut out);
            if rc != ffi::DuckDBSuccess {
                let msg = {
                    let c_err = ffi::duckdb_result_error(&mut out);
                    if c_err.is_null() {
                        None
                    } else {
                        Some(CStr::from_ptr(c_err).to_string_lossy().to_string())
                    }
                };
                ffi::duckdb_destroy_result(&mut out);
                return Err(Error::DuckDBFailure(ffi::Error::new(rc), msg));
            }

            // Check if the result is truly streaming or materialized
            // Some statements (like CALL) return materialized results even when streaming is requested
            self.is_streaming = ffi::duckdb_result_is_streaming(out);

            self.duckdb_result = Some(out);
            // Row decoding consumes this cache only for the non-streaming path
            // today. Keep streaming execution state symmetrical for callers
            // that inspect statement metadata after execution.
            self.result_column_logical_ids = Some(result_column_logical_ids);

            Ok(())
        }
    }

    #[inline]
    pub fn reset_result(&mut self) {
        self.schema = None;
        self.result_column_logical_ids = None;
        self.column_name_cache = OnceCell::new();
        self.is_streaming = false;
        if self.result.is_some() {
            unsafe {
                ffi::duckdb_destroy_arrow(&mut self.result_unwrap());
            }
            self.result = None;
        }
        if let Some(mut result) = self.duckdb_result {
            unsafe {
                ffi::duckdb_destroy_result(&mut result);
            }
            self.duckdb_result = None;
        }
    }

    #[inline]
    pub fn bind_parameter_count(&self) -> usize {
        unsafe { ffi::duckdb_nparams(self.ptr) as usize }
    }

    pub fn parameter_name(&self, idx: usize) -> Result<String> {
        let count = self.bind_parameter_count();
        if idx == 0 || idx > count {
            return Err(Error::InvalidParameterIndex(idx));
        }

        unsafe {
            let name_ptr = ffi::duckdb_parameter_name(self.ptr, idx as u64);
            // Range check above ensures this shouldn't be null, but check defensively
            let name = match ffi::DuckDbString::from_nullable_ptr(name_ptr) {
                Some(name) => name.to_string_lossy().to_string(),
                None => {
                    return Err(Error::DuckDBFailure(
                        ffi::Error::new(ffi::DuckDBError),
                        Some(format!("Could not retrieve parameter name for index {idx}")),
                    ));
                }
            };

            Ok(name)
        }
    }

    fn validate_and_load_result_column_logical_ids(&self) -> Result<Box<[LogicalTypeId]>> {
        let column_count = unsafe { ffi::duckdb_prepared_statement_column_count(self.ptr) as usize };
        let mut logical_ids = Vec::with_capacity(column_count);
        for idx in 0..column_count {
            let logical_type = self.try_column_logical_type(idx)?;
            if logical_type.contains_type_id(LogicalTypeId::Variant) {
                return Err(Error::FromSqlConversionFailure(
                    idx,
                    Type::Variant,
                    "decoding Variant columns is not supported".into(),
                ));
            }
            logical_ids.push(logical_type.id());
        }

        Ok(logical_ids.into_boxed_slice())
    }
    #[inline]
    pub fn sql(&self) -> Option<&CStr> {
        panic!("not supported")
    }
}

impl Drop for RawStatement {
    fn drop(&mut self) {
        self.reset_result();
        if !self.ptr.is_null() {
            unsafe { ffi::duckdb_destroy_prepare(&mut self.ptr) };
        }
    }
}

unsafe impl Send for RawStatement {}

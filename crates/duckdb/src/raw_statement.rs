use std::{
    cell::{Cell, OnceCell},
    collections::HashMap,
    ffi::CStr,
    ops::Deref,
    sync::Arc,
};

use arrow::{
    array::StructArray,
    datatypes::{DataType, Schema, SchemaRef},
    ffi::{FFI_ArrowArray, FFI_ArrowSchema, from_ffi},
};

use super::{Result, ffi};
use crate::{
    Error,
    core::{LogicalTypeHandle, LogicalTypeId},
    error::result_from_duckdb_result,
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
    duckdb_result: Option<ffi::duckdb_result>,
    schema: Option<SchemaRef>,
    // Cached DuckDB logical ids for result columns. Arrow reports HUGEINT,
    // UHUGEINT, and DECIMAL(38,0) through the same decimal shape.
    result_column_logical_ids: Option<Box<[LogicalTypeId]>>,
    #[cfg(feature = "polars")]
    polars_arrow_field: OnceCell<polars_arrow::datatypes::Field>,
    result_chunk_idx: Cell<usize>,
    arrow_options: Cell<ffi::duckdb_arrow_options>,
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
            schema: None,
            result_column_logical_ids: None,
            #[cfg(feature = "polars")]
            polars_arrow_field: OnceCell::new(),
            result_chunk_idx: Cell::new(0),
            arrow_options: Cell::new(std::ptr::null_mut()),
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
    pub fn row_count(&self) -> usize {
        unsafe {
            let mut result = self.duckdb_result.expect("The statement was not executed yet");
            ffi::duckdb_row_count(&mut result) as usize
        }
    }

    #[inline]
    pub fn step(&self) -> Result<Option<StructArray>> {
        let Some(result) = self.duckdb_result else {
            return Ok(None);
        };
        let Some(schema) = self.schema.clone() else {
            return Ok(None);
        };
        unsafe {
            let Some(chunk) = self.next_result_chunk(result)? else {
                return Ok(None);
            };
            self.data_chunk_to_struct_array(result, chunk, schema)
        }
    }

    #[inline]
    pub fn streaming_step(&self, schema: SchemaRef) -> Result<Option<StructArray>> {
        if let Some(result) = self.duckdb_result {
            unsafe {
                // Use duckdb_stream_fetch_chunk for truly streaming results,
                // or duckdb_fetch_chunk for materialized results (e.g., from CALL statements)
                let out = if self.is_streaming {
                    ffi::duckdb_stream_fetch_chunk(result)
                } else {
                    ffi::duckdb_fetch_chunk(result)
                };

                if out.is_null() {
                    return Ok(None);
                }

                return self.data_chunk_to_struct_array(result, out, schema);
            }
        }

        Ok(None)
    }

    #[cfg(feature = "polars")]
    #[inline]
    pub(crate) fn step_polars(&self) -> Result<Option<polars_arrow::array::StructArray>> {
        let Some(result) = self.duckdb_result else {
            return Ok(None);
        };

        unsafe {
            let Some(chunk) = self.next_result_chunk(result)? else {
                return Ok(None);
            };

            let mut ffi_arrow_array = polars_arrow::ffi::ArrowArray::empty();
            self.chunk_to_arrow(result, chunk, &mut ffi_arrow_array as *mut _ as *mut ffi::ArrowArray)?;

            let field = self.polars_arrow_field(result)?;
            let import_array = polars_arrow::ffi::import_array_from_c(ffi_arrow_array, field.dtype.clone());

            let array = match import_array {
                Ok(array) => array,
                // When array is empty, import_array_from_c returns ComputeError with message
                // "An ArrowArray of type X must have non-null children".
                Err(polars::error::PolarsError::ComputeError(msg)) if msg.to_string().contains("non-null children") => {
                    return Ok(None);
                }
                Err(err) => {
                    return Err(Self::duckdb_failure(format!(
                        "Failed to import Polars Arrow array from C: {err}"
                    )));
                }
            };
            let struct_array = array
                .as_any()
                .downcast_ref::<polars_arrow::array::StructArray>()
                .ok_or_else(|| Self::duckdb_failure("Failed to downcast Polars Arrow array to StructArray"))?
                .to_owned();

            Ok(Some(struct_array))
        }
    }

    #[inline]
    pub fn column_count(&self) -> usize {
        self.schema
            .as_ref()
            .expect("The statement was not executed yet")
            .fields()
            .len()
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
        if let Some(mut result) = self.duckdb_result {
            return unsafe { Self::try_result_column_logical_type(&mut result, idx) };
        }

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
        self.reject_unsupported_prepared_result_columns_for_dml()?;
        unsafe {
            let mut out: ffi::duckdb_result = std::mem::zeroed();
            let rc = ffi::duckdb_execute_prepared(self.ptr, &mut out);
            result_from_duckdb_result(rc, &mut out)?;

            let rows_changed = ffi::duckdb_rows_changed(&mut out);
            let (schema, result_column_logical_ids) = match self.load_result_schema_and_logical_ids(&mut out) {
                Ok(result) => result,
                Err(err) => {
                    self.destroy_arrow_options();
                    ffi::duckdb_destroy_result(&mut out);
                    return Err(err);
                }
            };

            self.schema = Some(schema);
            self.duckdb_result = Some(out);
            self.result_column_logical_ids = Some(result_column_logical_ids);
            Ok(rows_changed as usize)
        }
    }

    pub fn execute_streaming(&mut self) -> Result<()> {
        self.reset_result();
        self.reject_unsupported_prepared_result_columns_for_dml()?;
        unsafe {
            let mut out: ffi::duckdb_result = std::mem::zeroed();

            let rc = ffi::duckdb_execute_prepared_streaming(self.ptr, &mut out);
            result_from_duckdb_result(rc, &mut out)?;
            let result_column_logical_ids = match Self::load_result_column_logical_ids(&mut out) {
                Ok(result) => result,
                Err(err) => {
                    self.destroy_arrow_options();
                    ffi::duckdb_destroy_result(&mut out);
                    return Err(err);
                }
            };

            // Check if the result is truly streaming or materialized
            // Some statements (like CALL) return materialized results even when streaming is requested
            self.is_streaming = ffi::duckdb_result_is_streaming(out);

            self.duckdb_result = Some(out);
            // Streaming does not use the ids directly, but loading them
            // rejects unsupported Variant columns before streaming begins.
            self.result_column_logical_ids = Some(result_column_logical_ids);

            Ok(())
        }
    }

    #[inline]
    pub fn reset_result(&mut self) {
        self.schema = None;
        self.result_column_logical_ids = None;
        #[cfg(feature = "polars")]
        {
            self.polars_arrow_field = OnceCell::new();
        }
        self.result_chunk_idx.set(0);
        self.destroy_arrow_options();
        self.column_name_cache = OnceCell::new();
        self.is_streaming = false;
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

    fn reject_unsupported_prepared_result_columns_for_dml(&self) -> Result<()> {
        if !self.is_dml_statement() {
            return Ok(());
        }

        // This uses prepared metadata only as a pre-execution side-effect
        // barrier for DML RETURNING. Executed result metadata remains the
        // authoritative source for decoding after execution.
        let column_count = unsafe { ffi::duckdb_prepared_statement_column_count(self.ptr) as usize };
        for idx in 0..column_count {
            let logical_type = self.try_column_logical_type(idx)?;
            Self::reject_unsupported_result_logical_type(idx, &logical_type)?;
        }

        Ok(())
    }

    fn is_dml_statement(&self) -> bool {
        matches!(
            unsafe { ffi::duckdb_prepared_statement_type(self.ptr) },
            ffi::duckdb_statement_type_DUCKDB_STATEMENT_TYPE_INSERT
                | ffi::duckdb_statement_type_DUCKDB_STATEMENT_TYPE_UPDATE
                | ffi::duckdb_statement_type_DUCKDB_STATEMENT_TYPE_DELETE
        )
    }

    unsafe fn load_result_schema_and_logical_ids(
        &self,
        result: *mut ffi::duckdb_result,
    ) -> Result<(SchemaRef, Box<[LogicalTypeId]>)> {
        unsafe {
            let mut c_schema = FFI_ArrowSchema::empty();
            let logical_ids = self.result_to_arrow_schema(result, &mut c_schema as *mut _ as *mut ffi::ArrowSchema)?;
            let schema = Arc::new(
                Schema::try_from(&c_schema)
                    .map_err(|err| Self::arrow_failure("Could not import DuckDB Arrow schema", err))?,
            );
            debug_assert_eq!(
                schema.fields().len(),
                logical_ids.len(),
                "result schema and logical-id cache are built from the same DuckDB column count"
            );

            Ok((schema, logical_ids))
        }
    }

    unsafe fn load_result_column_logical_ids(result: *mut ffi::duckdb_result) -> Result<Box<[LogicalTypeId]>> {
        unsafe {
            let logical_ids = Self::result_column_logical_types(result)?
                .iter()
                .map(LogicalTypeHandle::id)
                .collect::<Vec<_>>();
            Ok(logical_ids.into_boxed_slice())
        }
    }

    unsafe fn result_column_logical_types(result: *mut ffi::duckdb_result) -> Result<Vec<LogicalTypeHandle>> {
        unsafe {
            let column_count = ffi::duckdb_column_count(result) as usize;
            let mut logical_types = Vec::with_capacity(column_count);

            for idx in 0..column_count {
                let logical_type = Self::try_result_column_logical_type(result, idx)?;
                Self::reject_unsupported_result_logical_type(idx, &logical_type)?;
                logical_types.push(logical_type);
            }

            Ok(logical_types)
        }
    }

    fn reject_unsupported_result_logical_type(idx: usize, logical_type: &LogicalTypeHandle) -> Result<()> {
        if logical_type.contains_type_id(LogicalTypeId::Variant) {
            return Err(Error::FromSqlConversionFailure(
                idx,
                Type::Variant,
                "decoding Variant columns is not supported".into(),
            ));
        }

        Ok(())
    }

    unsafe fn try_result_column_logical_type(result: *mut ffi::duckdb_result, idx: usize) -> Result<LogicalTypeHandle> {
        unsafe {
            let ptr = ffi::duckdb_column_logical_type(result, idx as u64);
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

    unsafe fn result_to_arrow_schema(
        &self,
        result: *mut ffi::duckdb_result,
        dst: *mut ffi::ArrowSchema,
    ) -> Result<Box<[LogicalTypeId]>> {
        unsafe {
            let column_count = ffi::duckdb_column_count(result) as usize;
            let logical_types = Self::result_column_logical_types(result)?;
            let logical_ids = logical_types.iter().map(LogicalTypeHandle::id).collect::<Vec<_>>();
            let mut names = Vec::with_capacity(column_count);

            for idx in 0..column_count {
                let name = ffi::duckdb_column_name(result, idx as u64);
                if name.is_null() {
                    return Err(Error::DuckDBFailure(
                        ffi::Error::new(ffi::DuckDBError),
                        Some(format!("Could not retrieve name for result column at index {idx}")),
                    ));
                }
                names.push(name);
            }

            let mut type_ptrs = logical_types
                .iter()
                .map(|logical_type| logical_type.ptr)
                .collect::<Vec<_>>();
            let arrow_options = self.arrow_options_for_result(*result)?;
            let error_data = ffi::duckdb_to_arrow_schema(
                arrow_options,
                type_ptrs.as_mut_ptr(),
                names.as_mut_ptr(),
                column_count as u64,
                dst,
            );
            Self::result_from_duckdb_error_data(error_data)?;

            Ok(logical_ids.into_boxed_slice())
        }
    }

    unsafe fn next_result_chunk(&self, result: ffi::duckdb_result) -> Result<Option<ffi::duckdb_data_chunk>> {
        unsafe {
            let chunk_idx = self.result_chunk_idx.get();
            if chunk_idx >= ffi::duckdb_result_chunk_count(result) as usize {
                return Ok(None);
            }
            let chunk = ffi::duckdb_result_get_chunk(result, chunk_idx as u64);
            self.result_chunk_idx.set(chunk_idx + 1);
            if chunk.is_null() {
                Err(Self::duckdb_failure(format!(
                    "Could not fetch result chunk at index {chunk_idx}"
                )))
            } else {
                Ok(Some(chunk))
            }
        }
    }

    unsafe fn data_chunk_to_struct_array(
        &self,
        result: ffi::duckdb_result,
        chunk: ffi::duckdb_data_chunk,
        schema: SchemaRef,
    ) -> Result<Option<StructArray>> {
        unsafe {
            let mut arrays = FFI_ArrowArray::empty();
            self.chunk_to_arrow(result, chunk, &mut arrays as *mut _ as *mut ffi::ArrowArray)?;

            if arrays.is_empty() {
                return Ok(None);
            }

            let schema = FFI_ArrowSchema::try_from(schema.deref())
                .map_err(|err| Self::arrow_failure("Could not export Arrow schema", err))?;
            let array_data = from_ffi(arrays, &schema)
                .map_err(|err| Self::arrow_failure("Could not import DuckDB Arrow array", err))?;
            Ok(Some(StructArray::from(array_data)))
        }
    }

    unsafe fn chunk_to_arrow(
        &self,
        result: ffi::duckdb_result,
        mut chunk: ffi::duckdb_data_chunk,
        dst: *mut ffi::ArrowArray,
    ) -> Result<()> {
        unsafe {
            let arrow_options = match self.arrow_options_for_result(result) {
                Ok(arrow_options) => arrow_options,
                Err(err) => {
                    ffi::duckdb_destroy_data_chunk(&mut chunk);
                    return Err(err);
                }
            };
            let error_data = ffi::duckdb_data_chunk_to_arrow(arrow_options, chunk, dst);
            ffi::duckdb_destroy_data_chunk(&mut chunk);
            Self::result_from_duckdb_error_data(error_data)
        }
    }

    unsafe fn arrow_options_for_result(&self, mut result: ffi::duckdb_result) -> Result<ffi::duckdb_arrow_options> {
        unsafe {
            let arrow_options = self.arrow_options.get();
            if !arrow_options.is_null() {
                return Ok(arrow_options);
            }

            let arrow_options = ffi::duckdb_result_get_arrow_options(&mut result);
            if arrow_options.is_null() {
                return Err(Self::duckdb_failure("Could not retrieve Arrow options for result"));
            }
            self.arrow_options.set(arrow_options);
            Ok(arrow_options)
        }
    }

    fn destroy_arrow_options(&self) {
        let mut arrow_options = self.arrow_options.replace(std::ptr::null_mut());
        if !arrow_options.is_null() {
            unsafe {
                ffi::duckdb_destroy_arrow_options(&mut arrow_options);
            }
        }
    }

    unsafe fn result_from_duckdb_error_data(mut error_data: ffi::duckdb_error_data) -> Result<()> {
        unsafe {
            if error_data.is_null() {
                return Ok(());
            }
            if !ffi::duckdb_error_data_has_error(error_data) {
                ffi::duckdb_destroy_error_data(&mut error_data);
                return Ok(());
            }

            let msg = {
                let c_err = ffi::duckdb_error_data_message(error_data);
                if c_err.is_null() {
                    None
                } else {
                    Some(CStr::from_ptr(c_err).to_string_lossy().to_string())
                }
            };
            ffi::duckdb_destroy_error_data(&mut error_data);
            Err(Error::DuckDBFailure(ffi::Error::new(ffi::DuckDBError), msg))
        }
    }

    #[cfg(feature = "polars")]
    fn polars_arrow_field(&self, result: ffi::duckdb_result) -> Result<&polars_arrow::datatypes::Field> {
        if self.polars_arrow_field.get().is_none() {
            let field = unsafe { self.load_polars_arrow_field(result)? };
            let _ = self.polars_arrow_field.set(field);
        }
        Ok(self
            .polars_arrow_field
            .get()
            .expect("polars Arrow field cache was just initialized"))
    }

    #[cfg(feature = "polars")]
    unsafe fn load_polars_arrow_field(&self, result: ffi::duckdb_result) -> Result<polars_arrow::datatypes::Field> {
        unsafe {
            let mut result = result;
            let mut schema = polars_arrow::ffi::ArrowSchema::empty();
            let _ = self.result_to_arrow_schema(&mut result, &mut schema as *mut _ as *mut ffi::ArrowSchema)?;

            polars_arrow::ffi::import_field_from_c(&schema)
                .map_err(|err| Self::duckdb_failure(format!("Failed to import Polars Arrow field: {err}")))
        }
    }

    fn arrow_failure(context: &str, err: impl std::fmt::Display) -> Error {
        Self::duckdb_failure(format!("{context}: {err}"))
    }

    fn duckdb_failure(message: impl Into<String>) -> Error {
        Error::DuckDBFailure(ffi::Error::new(ffi::DuckDBError), Some(message.into()))
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

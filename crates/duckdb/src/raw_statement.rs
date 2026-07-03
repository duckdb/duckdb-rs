use std::{ffi::CStr, sync::Arc};

use arrow::{
    array::StructArray,
    datatypes::{DataType, SchemaRef},
};

use super::{Result, ffi};
use crate::{
    Error,
    core::{LogicalTypeHandle, LogicalTypeId},
    error::{duckdb_failure_from_message, result_from_duckdb_result},
    executed_result::{ExecutedResult, reject_unsupported_result_logical_type},
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
    result: Option<ExecutedResult>,
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
        self.executed().row_count()
    }

    #[inline]
    pub fn step(&self) -> Result<Option<StructArray>> {
        self.result.as_ref().map_or(Ok(None), ExecutedResult::step)
    }

    #[cfg(feature = "polars")]
    #[inline]
    pub(crate) fn step_polars(&self) -> Result<Option<polars_arrow::array::StructArray>> {
        self.result.as_ref().map_or(Ok(None), ExecutedResult::step_polars)
    }

    #[inline]
    pub fn column_count(&self) -> usize {
        self.schema_ref().fields().len()
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
        if let Some(result) = &self.result {
            return result.try_column_logical_type(idx);
        }

        unsafe {
            let ptr = ffi::duckdb_prepared_statement_column_logical_type(self.ptr, idx as u64);
            if ptr.is_null() {
                return Err(duckdb_failure_from_message(format!(
                    "Could not retrieve logical type for result column at index {idx}"
                )));
            }
            Ok(LogicalTypeHandle::new(ptr))
        }
    }

    #[inline]
    pub(crate) fn result_column_logical_id(&self, idx: usize) -> Option<LogicalTypeId> {
        self.result.as_ref().map(|result| result.result_column_logical_id(idx))
    }

    #[inline]
    pub fn schema(&self) -> SchemaRef {
        self.schema_ref().clone()
    }

    #[inline]
    fn schema_ref(&self) -> &SchemaRef {
        self.executed().schema_ref()
    }

    #[inline]
    fn executed(&self) -> &ExecutedResult {
        self.result.as_ref().expect("The statement was not executed yet")
    }

    #[inline]
    pub fn column_name(&self, idx: usize) -> Option<&String> {
        self.executed().column_name(idx)
    }

    #[inline]
    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.executed().column_index(name)
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
            self.result = Some(ExecutedResult::new(out)?);
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
            self.result = Some(ExecutedResult::new(out)?);

            Ok(())
        }
    }

    #[inline]
    pub fn reset_result(&mut self) {
        self.result = None;
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
            reject_unsupported_result_logical_type(idx, &logical_type)?;
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

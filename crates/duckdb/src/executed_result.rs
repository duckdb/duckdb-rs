use std::{
    cell::{Cell, OnceCell, UnsafeCell},
    collections::HashMap,
    ffi::{CStr, CString},
    os::raw::c_char,
    ptr::NonNull,
    sync::Arc,
};

use arrow::{
    array::StructArray,
    datatypes::{Schema, SchemaRef},
    ffi::{FFI_ArrowArray, FFI_ArrowSchema, from_ffi},
};

use super::{Result, ffi};
use crate::{
    Error,
    core::{LogicalTypeHandle, LogicalTypeId},
    error::{
        arrow_conversion_failure, duckdb_failure_from_message, result_error_message, result_from_duckdb_error_data,
    },
    types::Type,
};
#[cfg(feature = "polars")]
use polars_core::utils::arrow as polars_arrow;

#[derive(Debug)]
pub(crate) struct ExecutedResult {
    arrow_options: ArrowOptionsHandle,
    result: DuckdbResultHandle,
    schema: SchemaRef,
    columns: Box<[ResultColumn]>,
    arrow_schema: FFI_ArrowSchema,
    exhausted: Cell<bool>,
    column_name_cache: OnceCell<HashMap<Box<str>, usize>>,
    #[cfg(feature = "polars")]
    polars_arrow_field: OnceCell<polars_arrow::datatypes::Field>,
}

// SAFETY: `ExecutedResult` is Send but not Sync. It owns its DuckDB result in an
// UnsafeCell because fetching mutates DuckDB's result cursor through `&self`.
// Cell and OnceCell keep cache/cursor mutation local to the statement. Moving
// the owner to another thread transfers the only access path to the result and
// Arrow options handles; concurrent access is not promised.
unsafe impl Send for ExecutedResult {}

#[derive(Debug)]
struct ResultColumn {
    name: CString,
    logical_type: LogicalTypeHandle,
}

impl ExecutedResult {
    /// Takes ownership of a DuckDB result.
    ///
    /// # Safety
    ///
    /// `result` must be initialized by DuckDB and not owned anywhere else.
    /// This function takes ownership unconditionally: after it is called, the
    /// result is destroyed by `ExecutedResult` even if metadata loading fails.
    pub(crate) unsafe fn new(result: ffi::duckdb_result) -> Result<Self> {
        let result = DuckdbResultHandle::new(result);
        let arrow_options = unsafe { ArrowOptionsHandle::new(&result)? };
        let (schema, columns, arrow_schema) = unsafe { Self::load_schema_and_columns(&result, &arrow_options)? };
        Ok(Self {
            arrow_options,
            result,
            schema,
            columns,
            arrow_schema,
            exhausted: Cell::new(false),
            column_name_cache: OnceCell::new(),
            #[cfg(feature = "polars")]
            polars_arrow_field: OnceCell::new(),
        })
    }

    pub(crate) fn row_count(&self) -> usize {
        unsafe { ffi::duckdb_row_count(self.result.as_mut_ptr()) as usize }
    }

    pub(crate) fn step(&self) -> Result<Option<StructArray>> {
        unsafe {
            let Some(chunk) = self.next_chunk()? else {
                return Ok(None);
            };
            self.data_chunk_to_struct_array(chunk).map(Some)
        }
    }

    #[cfg(feature = "polars")]
    pub(crate) fn step_polars(&self) -> Result<Option<polars_arrow::array::StructArray>> {
        unsafe {
            let Some(chunk) = self.next_chunk()? else {
                return Ok(None);
            };

            let mut ffi_arrow_array = polars_arrow::ffi::ArrowArray::empty();
            self.chunk_to_arrow(chunk, &mut ffi_arrow_array as *mut _ as *mut ffi::ArrowArray)?;

            let field = self.polars_arrow_field()?;
            let array = polars_arrow::ffi::import_array_from_c(ffi_arrow_array, field.dtype.clone())
                .map_err(|err| arrow_conversion_failure("Could not import Polars Arrow array from C", err))?;
            let struct_array = array
                .as_any()
                .downcast_ref::<polars_arrow::array::StructArray>()
                .ok_or_else(|| duckdb_failure_from_message("Failed to downcast Polars Arrow array to StructArray"))?
                .to_owned();

            Ok(Some(struct_array))
        }
    }

    pub(crate) fn schema_ref(&self) -> &SchemaRef {
        &self.schema
    }

    pub(crate) fn result_column_logical_id(&self, idx: usize) -> LogicalTypeId {
        self.columns
            .get(idx)
            .unwrap_or_else(|| panic!("result column logical-type index {idx} is out of bounds"))
            .logical_type
            .id()
    }

    pub(crate) fn try_column_logical_type(&self, idx: usize) -> Result<LogicalTypeHandle> {
        unsafe { Self::try_result_column_logical_type(self.result.as_mut_ptr(), idx) }
    }

    pub(crate) fn column_name(&self, idx: usize) -> Option<&String> {
        self.schema.fields().get(idx).map(|field| field.name())
    }

    pub(crate) fn column_index(&self, name: &str) -> Option<usize> {
        let cache = self.column_name_cache.get_or_init(|| self.build_column_name_cache());
        cache.get(&*name.to_ascii_lowercase()).copied()
    }

    fn build_column_name_cache(&self) -> HashMap<Box<str>, usize> {
        let schema = self.schema_ref();
        let mut cache = HashMap::with_capacity(schema.fields().len());
        for (index, field) in schema.fields().iter().enumerate() {
            cache
                .entry(field.name().to_ascii_lowercase().into_boxed_str())
                .or_insert(index);
        }
        cache
    }

    unsafe fn load_schema_and_columns(
        result: &DuckdbResultHandle,
        arrow_options: &ArrowOptionsHandle,
    ) -> Result<(SchemaRef, Box<[ResultColumn]>, FFI_ArrowSchema)> {
        unsafe {
            let mut arrow_schema = FFI_ArrowSchema::empty();
            let columns = Self::load_result_columns(result.as_mut_ptr())?;
            Self::export_arrow_schema(
                arrow_options,
                &columns,
                &mut arrow_schema as *mut _ as *mut ffi::ArrowSchema,
            )?;
            let schema = Arc::new(
                Schema::try_from(&arrow_schema)
                    .map_err(|err| arrow_conversion_failure("Could not import DuckDB Arrow schema", err))?,
            );
            debug_assert_eq!(
                schema.fields().len(),
                columns.len(),
                "result schema and column cache are built from the same DuckDB column count"
            );

            Ok((schema, columns, arrow_schema))
        }
    }

    unsafe fn load_result_columns(result: *mut ffi::duckdb_result) -> Result<Box<[ResultColumn]>> {
        unsafe {
            let column_count = ffi::duckdb_column_count(result) as usize;
            let mut columns = Vec::with_capacity(column_count);

            for idx in 0..column_count {
                let logical_type = Self::try_result_column_logical_type(result, idx)?;
                reject_unsupported_result_logical_type(idx, &logical_type)?;

                let name = ffi::duckdb_column_name(result, idx as u64);
                if name.is_null() {
                    return Err(duckdb_failure_from_message(format!(
                        "Could not retrieve name for result column at index {idx}"
                    )));
                }
                let name = CStr::from_ptr(name).to_owned();

                columns.push(ResultColumn { name, logical_type });
            }

            Ok(columns.into_boxed_slice())
        }
    }

    unsafe fn try_result_column_logical_type(result: *mut ffi::duckdb_result, idx: usize) -> Result<LogicalTypeHandle> {
        unsafe {
            let ptr = ffi::duckdb_column_logical_type(result, idx as u64);
            logical_type_from_duckdb_column(ptr, idx)
        }
    }

    unsafe fn export_arrow_schema(
        arrow_options: &ArrowOptionsHandle,
        columns: &[ResultColumn],
        dst: *mut ffi::ArrowSchema,
    ) -> Result<()> {
        unsafe {
            let mut type_ptrs = columns.iter().map(|column| column.logical_type.ptr).collect::<Vec<_>>();
            let mut names = columns
                .iter()
                .map(|column| column.name.as_ptr() as *const c_char)
                .collect::<Vec<_>>();
            let error_data = ffi::duckdb_to_arrow_schema(
                arrow_options.as_ptr(),
                type_ptrs.as_mut_ptr(),
                names.as_mut_ptr(),
                columns.len() as ffi::idx_t,
                dst,
            );
            result_from_duckdb_error_data(error_data)
        }
    }

    unsafe fn next_chunk(&self) -> Result<Option<ffi::duckdb_data_chunk>> {
        unsafe {
            if self.exhausted.get() {
                return Ok(None);
            }

            // duckdb_fetch_chunk is the non-deprecated fetcher for both
            // materialized and streaming results. That matters for CALL, which
            // can produce a materialized result even after execute_streaming.
            let chunk = ffi::duckdb_fetch_chunk(self.result.as_handle());
            if chunk.is_null() {
                self.check_result_error()?;
                self.exhausted.set(true);
                return Ok(None);
            }

            if ffi::duckdb_data_chunk_get_size(chunk) == 0 {
                let mut chunk = chunk;
                ffi::duckdb_destroy_data_chunk(&mut chunk);
                self.check_result_error()?;
                self.exhausted.set(true);
                return Ok(None);
            }

            Ok(Some(chunk))
        }
    }

    unsafe fn check_result_error(&self) -> Result<()> {
        unsafe {
            let Some(message) = result_error_message(self.result.as_mut_ptr()) else {
                return Ok(());
            };
            Err(duckdb_failure_from_message(message))
        }
    }

    unsafe fn data_chunk_to_struct_array(&self, chunk: ffi::duckdb_data_chunk) -> Result<StructArray> {
        unsafe {
            let mut arrays = FFI_ArrowArray::empty();
            self.chunk_to_arrow(chunk, &mut arrays as *mut _ as *mut ffi::ArrowArray)?;

            let array_data = from_ffi(arrays, &self.arrow_schema)
                .map_err(|err| arrow_conversion_failure("Could not import DuckDB Arrow array", err))?;
            Ok(StructArray::from(array_data))
        }
    }

    unsafe fn chunk_to_arrow(&self, mut chunk: ffi::duckdb_data_chunk, dst: *mut ffi::ArrowArray) -> Result<()> {
        unsafe {
            let error_data = ffi::duckdb_data_chunk_to_arrow(self.arrow_options.as_ptr(), chunk, dst);
            ffi::duckdb_destroy_data_chunk(&mut chunk);
            result_from_duckdb_error_data(error_data)
        }
    }

    #[cfg(feature = "polars")]
    fn polars_arrow_field(&self) -> Result<&polars_arrow::datatypes::Field> {
        if self.polars_arrow_field.get().is_none() {
            let field = unsafe { self.load_polars_arrow_field()? };
            let _ = self.polars_arrow_field.set(field);
        }
        Ok(self
            .polars_arrow_field
            .get()
            .expect("polars Arrow field cache was just initialized"))
    }

    #[cfg(feature = "polars")]
    unsafe fn load_polars_arrow_field(&self) -> Result<polars_arrow::datatypes::Field> {
        unsafe {
            let mut schema = polars_arrow::ffi::ArrowSchema::empty();
            Self::export_arrow_schema(
                &self.arrow_options,
                &self.columns,
                &mut schema as *mut _ as *mut ffi::ArrowSchema,
            )?;

            polars_arrow::ffi::import_field_from_c(&schema)
                .map_err(|err| arrow_conversion_failure("Could not import Polars Arrow field", err))
        }
    }
}

pub(crate) unsafe fn logical_type_from_duckdb_column(
    ptr: ffi::duckdb_logical_type,
    idx: usize,
) -> Result<LogicalTypeHandle> {
    if ptr.is_null() {
        return Err(duckdb_failure_from_message(format!(
            "Could not retrieve logical type for result column at index {idx}"
        )));
    }
    Ok(unsafe { LogicalTypeHandle::new(ptr) })
}

pub(crate) fn reject_unsupported_result_logical_type(idx: usize, logical_type: &LogicalTypeHandle) -> Result<()> {
    if logical_type.contains_type_id(LogicalTypeId::Variant) {
        return Err(Error::FromSqlConversionFailure(
            idx,
            Type::Variant,
            "decoding Variant columns is not supported".into(),
        ));
    }

    Ok(())
}

/// Owned DuckDB result handle.
///
/// DuckDB advances result state while fetching chunks, so the raw result lives
/// in an `UnsafeCell` and fetch methods take `&self`. The owner is `Send` but
/// not `Sync`, which keeps that interior mutation single-threaded.
#[derive(Debug)]
struct DuckdbResultHandle {
    result: UnsafeCell<ffi::duckdb_result>,
}

impl DuckdbResultHandle {
    fn new(result: ffi::duckdb_result) -> Self {
        Self {
            result: UnsafeCell::new(result),
        }
    }

    fn as_handle(&self) -> ffi::duckdb_result {
        unsafe { *self.result.get() }
    }

    fn as_mut_ptr(&self) -> *mut ffi::duckdb_result {
        self.result.get()
    }
}

impl Drop for DuckdbResultHandle {
    fn drop(&mut self) {
        unsafe {
            ffi::duckdb_destroy_result(self.result.get());
        }
    }
}

#[derive(Debug)]
struct ArrowOptionsHandle {
    ptr: NonNull<ffi::_duckdb_arrow_options>,
}

impl ArrowOptionsHandle {
    /// Takes ownership of a DuckDB Arrow options handle for `result`.
    ///
    /// # Safety
    ///
    /// `result` must be a live DuckDB result. The returned handle owns the
    /// options pointer and destroys it on drop.
    unsafe fn new(result: &DuckdbResultHandle) -> Result<Self> {
        unsafe {
            let arrow_options = ffi::duckdb_result_get_arrow_options(result.as_mut_ptr());
            let Some(arrow_options) = NonNull::new(arrow_options) else {
                return Err(duckdb_failure_from_message(
                    "Could not retrieve Arrow options for result",
                ));
            };
            Ok(Self { ptr: arrow_options })
        }
    }

    fn as_ptr(&self) -> ffi::duckdb_arrow_options {
        self.ptr.as_ptr()
    }
}

impl Drop for ArrowOptionsHandle {
    fn drop(&mut self) {
        let mut arrow_options = self.ptr.as_ptr();
        unsafe {
            ffi::duckdb_destroy_arrow_options(&mut arrow_options);
        }
    }
}

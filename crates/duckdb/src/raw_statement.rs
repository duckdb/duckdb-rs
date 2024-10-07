use std::{ffi::CStr, ptr, rc::Rc, sync::Arc};

use arrow::{
    array::StructArray,
    datatypes::{DataType, Schema, SchemaRef},
    ffi::{from_ffi, FFI_ArrowArray, FFI_ArrowSchema},
};

use super::{ffi, Result};
#[cfg(feature = "polars")]
use crate::arrow2;
use crate::error::result_from_duckdb_arrow;

// Private newtype for raw sqlite3_stmts that finalize themselves when dropped.
// TODO: destroy statement and result
#[derive(Debug)]
pub struct RawStatement {
    ptr: ffi::duckdb_prepared_statement,
    result: Option<ffi::duckdb_arrow>,
    schema: Option<SchemaRef>,
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
    pub unsafe fn new(stmt: ffi::duckdb_prepared_statement) -> RawStatement {
        RawStatement {
            ptr: stmt,
            result: None,
            schema: None,
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
        self.result.unwrap()
    }

    #[inline]
    pub fn row_count(&self) -> usize {
        unsafe { ffi::duckdb_arrow_row_count(self.result_unwrap()) as usize }
    }

    #[inline]
    pub fn step(&self) -> Option<StructArray> {
        self.result?;
        unsafe {
            let mut arrays = FFI_ArrowArray::empty();
            if ffi::duckdb_query_arrow_array(
                self.result_unwrap(),
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
                self.result_unwrap(),
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

    #[cfg(feature = "polars")]
    #[inline]
    pub fn step2(&self) -> Option<arrow2::array::StructArray> {
        self.result?;

        unsafe {
            let mut ffi_arrow2_array = arrow2::ffi::ArrowArray::empty();

            if ffi::duckdb_query_arrow_array(
                self.result_unwrap(),
                &mut std::ptr::addr_of_mut!(ffi_arrow2_array) as *mut _ as *mut ffi::duckdb_arrow_array,
            )
            .ne(&ffi::DuckDBSuccess)
            {
                return None;
            }

            let mut ffi_arrow2_schema = arrow2::ffi::ArrowSchema::empty();

            if ffi::duckdb_query_arrow_schema(
                self.result_unwrap(),
                &mut std::ptr::addr_of_mut!(ffi_arrow2_schema) as *mut _ as *mut ffi::duckdb_arrow_schema,
            )
            .ne(&ffi::DuckDBSuccess)
            {
                return None;
            }

            let arrow2_field =
                arrow2::ffi::import_field_from_c(&ffi_arrow2_schema).expect("Failed to import arrow2 Field from C");
            let import_arrow2_array = arrow2::ffi::import_array_from_c(ffi_arrow2_array, arrow2_field.data_type);

            if let Err(err) = import_arrow2_array {
                // When array is empty, import_array_from_c returns error with message
                // "ComputeError("An ArrowArray of type X must have non-null children")
                // Therefore, we return None when encountering this error.
                match err {
                    polars::error::PolarsError::ComputeError(_) => return None,
                    _ => panic!("Failed to import arrow2 Array from C: {}", err),
                }
            }

            let arrow2_array = import_arrow2_array.unwrap();
            let arrow2_struct_array = arrow2_array
                .as_any()
                .downcast_ref::<arrow2::array::StructArray>()
                .expect("Failed to downcast arrow2 Array to arrow2 StructArray")
                .to_owned();

            Some(arrow2_struct_array)
        }
    }

    #[inline]
    pub fn column_count(&self) -> usize {
        unsafe { ffi::duckdb_arrow_column_count(self.result_unwrap()) as usize }
    }

    #[inline]
    pub fn column_type(&self, idx: usize) -> DataType {
        self.schema().field(idx).data_type().to_owned()
    }

    #[inline]
    pub fn schema(&self) -> SchemaRef {
        self.schema.clone().unwrap()
    }

    #[inline]
    #[allow(dead_code)]
    pub fn column_decltype(&self, _idx: usize) -> Option<&CStr> {
        panic!("not implemented")
    }

    #[inline]
    pub fn column_name(&self, idx: usize) -> Option<&String> {
        if idx >= self.column_count() {
            return None;
        }
        Some(self.schema.as_ref().unwrap().field(idx).name())
    }

    #[allow(dead_code)]
    unsafe fn print_result(&self, mut result: ffi::duckdb_result) {
        use ffi::{duckdb_column_count, duckdb_column_name, duckdb_row_count};

        println!(
            "row-count: {}, column-count: {}",
            duckdb_row_count(&mut result),
            duckdb_column_count(&mut result)
        );
        for i in 0..duckdb_column_count(&mut result) {
            print!(
                "column-name:{} ",
                CStr::from_ptr(duckdb_column_name(&mut result, i)).to_string_lossy()
            );
        }
        println!();
        // print the data of the result
        for row_idx in 0..duckdb_row_count(&mut result) {
            print!("row-value:");
            for col_idx in 0..duckdb_column_count(&mut result) {
                let val = ffi::duckdb_value_varchar(&mut result, col_idx, row_idx);
                print!("{} ", CStr::from_ptr(val).to_string_lossy());
            }
            println!();
        }
    }

    /// NOTE: if execute failed, we shouldn't call any other methods which depends on result
    pub fn execute(&mut self) -> Result<usize> {
        self.reset_result();
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
            Ok(rows_changed as usize)
        }
    }

    #[inline]
    pub fn reset_result(&mut self) {
        self.schema = None;
        if self.result.is_some() {
            unsafe {
                ffi::duckdb_destroy_arrow(&mut self.result_unwrap());
            }
            self.result = None;
        }
    }

    #[inline]
    pub fn bind_parameter_count(&self) -> usize {
        unsafe { ffi::duckdb_nparams(self.ptr) as usize }
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

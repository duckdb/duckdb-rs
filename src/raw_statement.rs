use std::convert::TryFrom;
use std::ffi::CStr;
use std::os::raw::c_void;
use std::ptr;
use std::sync::Arc;

use super::ffi;
use super::Result;
use crate::error::result_from_duckdb_arrow;

use arrow::array::{ArrayData, StructArray};
use arrow::datatypes::{DataType, Schema, SchemaRef};
use arrow::ffi::{ArrowArray, FFI_ArrowArray, FFI_ArrowSchema};

// Private newtype for raw sqlite3_stmts that finalize themselves when dropped.
// TODO: destroy statement and result
#[derive(Debug)]
pub struct RawStatement {
    ptr: ffi::duckdb_prepared_statement,
    result: Option<ffi::duckdb_arrow>,
    c_schema: Option<*const FFI_ArrowSchema>,
    schema: Option<SchemaRef>,
}

impl RawStatement {
    #[inline]
    pub unsafe fn new(stmt: ffi::duckdb_prepared_statement) -> RawStatement {
        RawStatement {
            ptr: stmt,
            result: None,
            c_schema: None,
            schema: None,
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
            let (mut arrays, mut schema) = ArrowArray::into_raw(ArrowArray::empty());
            let schema = &mut schema;
            let arrays = &mut arrays;
            // TODO: Can we reuse schema?
            // destroy schema as we don't need it...
            // Arc::from_raw(schema);
            // TODO: use this after https://github.com/apache/arrow-rs/pull/612
            // let mut arrays = Arc::into_raw(Arc::new(FFI_ArrowArray::empty()));
            if ffi::duckdb_query_arrow_array(self.result_unwrap(), arrays as *mut _ as *mut *mut c_void)
                != ffi::DuckDBSuccess
            {
                let _ = ArrowArray::try_from_raw(*arrays as *mut FFI_ArrowArray, *schema as *mut FFI_ArrowSchema);
                return None;
            }
            if (**arrays).is_empty() {
                let _ = ArrowArray::try_from_raw(*arrays as *mut FFI_ArrowArray, *schema as *mut FFI_ArrowSchema);
                return None;
            }

            if ffi::duckdb_query_arrow_schema(self.result_unwrap(), schema as *mut _ as *mut *mut c_void)
                != ffi::DuckDBSuccess
            {
                // clean raw data
                let _ = ArrowArray::try_from_raw(*arrays as *mut FFI_ArrowArray, *schema as *mut FFI_ArrowSchema);
                return None;
            }

            let arrow_array =
                ArrowArray::try_from_raw(*arrays as *const FFI_ArrowArray, *schema as *const FFI_ArrowSchema)
                    .expect("ok");
            let array_data = ArrayData::try_from(arrow_array).expect("ok");
            let struct_array = StructArray::from(array_data);
            Some(struct_array)
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
    pub fn schema(&self) -> &SchemaRef {
        self.schema.as_ref().unwrap()
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
        use ffi::duckdb_column_count;
        use ffi::duckdb_column_name;
        use ffi::duckdb_row_count;

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
            let mut c_schema = Arc::into_raw(Arc::new(FFI_ArrowSchema::empty()));
            let schema = &mut c_schema;
            let rc = ffi::duckdb_query_arrow_schema(out, schema as *mut _ as *mut *mut c_void);
            if rc != ffi::DuckDBSuccess {
                Arc::from_raw(c_schema);
                result_from_duckdb_arrow(rc, out)?;
            }
            self.schema = Some(Arc::new(Schema::try_from(&*c_schema).unwrap()));
            self.c_schema = Some(c_schema);

            self.result = Some(out);
            Ok(rows_changed as usize)
        }
    }

    #[inline]
    pub fn reset_result(&mut self) {
        self.schema = None;
        if self.c_schema.is_some() {
            unsafe {
                Arc::from_raw(self.c_schema.unwrap());
            }
            self.c_schema = None;
        }
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

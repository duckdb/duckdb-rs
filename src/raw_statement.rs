use std::convert::TryFrom;
use std::ffi::CStr;
use std::os::raw::c_void;
use std::ptr;
use std::slice;
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
    pub result: Option<ffi::duckdb_arrow>,
    pub c_schema: Option<*const FFI_ArrowSchema>,
    pub schema: Option<SchemaRef>,
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
        if self.result.is_none() {
            return None;
        }
        unsafe {
            let (mut arrays, mut _schema) = ArrowArray::into_raw(ArrowArray::empty());
            let arrays = &mut arrays;
            if ffi::duckdb_query_arrow_array(self.result_unwrap(), arrays as *mut _ as *mut *mut c_void)
                != ffi::DuckDBSuccess
            {
                return None;
            }
            let arrow_array =
                ArrowArray::try_from_raw(*arrays as *const FFI_ArrowArray, self.c_schema.unwrap()).expect("ok");
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
        let columns = slice::from_raw_parts(result.columns, result.column_count as usize);
        println!("row-count: {}, column-count: {}", result.row_count, result.column_count);
        for i in 0..result.column_count {
            print!(
                "column-name:{} ",
                CStr::from_ptr(columns[i as usize].name).to_string_lossy()
            );
        }
        println!();
        // print the data of the result
        for row_idx in 0..result.row_count {
            print!("row-value:");
            for col_idx in 0..result.column_count {
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
            let (mut _arrays, mut c_schema) = ArrowArray::into_raw(ArrowArray::empty());
            let schema = &mut c_schema;
            let rc = ffi::duckdb_query_arrow_schema(out, schema as *mut _ as *mut *mut c_void);
            result_from_duckdb_arrow(rc, out)?;
            self.schema = Some(Arc::new(Schema::try_from(&*c_schema).unwrap()));
            self.c_schema = Some(c_schema);

            self.result = Some(out);
            Ok(rows_changed as usize)
        }
    }

    #[inline]
    pub fn reset_result(&mut self) {
        self.c_schema = None;
        self.schema = None;
        if !self.result.is_none() {
            unsafe {
                ffi::duckdb_destroy_arrow(&mut self.result_unwrap());
                self.result = None;
            }
        }
    }

    #[inline]
    pub fn bind_parameter_count(&self) -> usize {
        unsafe {
            let mut nparams: u64 = 0;
            // TODO: why if failed?
            ffi::duckdb_nparams(self.ptr, &mut nparams);
            nparams as usize
        }
    }

    #[inline]
    pub fn sql(&self) -> Option<&CStr> {
        panic!("not supported")
    }
}

impl Drop for RawStatement {
    fn drop(&mut self) {
        self.reset_result();
        unsafe { ffi::duckdb_destroy_prepare(&mut self.ptr) };
    }
}

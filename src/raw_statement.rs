use super::ffi;
use crate::error::{error_from_duckdb_code};
use super::{StatementStatus, Result};
use std::ffi::CStr;
use std::os::raw::{c_int, c_uint};
use std::mem;
use std::slice;

// Private newtype for raw sqlite3_stmts that finalize themselves when dropped.
// TODO: destroy statement and result
#[derive(Debug)]
pub struct RawStatement {
    ptr: ffi::duckdb_prepared_statement,
    pub result: ffi::duckdb_result,
}

impl RawStatement {
    #[inline]
    pub unsafe fn new(stmt: ffi::duckdb_prepared_statement) -> RawStatement {
        RawStatement {
            ptr: stmt,
            result: mem::zeroed(),
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
    pub unsafe fn result(&self) -> ffi::duckdb_result {
        self.result
    }

    #[inline]
    pub fn column_count(&self) -> usize {
        // TODO: change return type?
        let count = unsafe {self.result().column_count as usize};
        count
    }

    #[inline]
    pub unsafe fn column_type(&self, idx: usize) -> c_uint {
        let columns = slice::from_raw_parts(self.result().columns, self.result().column_count as usize);
        columns[idx].type_
    }

    #[inline]
    #[allow(dead_code)]
    pub fn column_decltype(&self, idx: usize) -> Option<&CStr> {
        unsafe {
            let columns = slice::from_raw_parts(self.result().columns, self.result().column_count as usize);
            Some(CStr::from_ptr(columns[idx].name))
        }
    }

    #[inline]
    pub fn column_name(&self, idx: usize) -> Option<&CStr> {
        if idx >= self.column_count() {
            return None;
        }
        unsafe {
            let columns = slice::from_raw_parts(self.result().columns, self.result().column_count as usize);
            Some(CStr::from_ptr(columns[idx].name))
        }
    }

    #[allow(dead_code)]
    unsafe fn print_result(&self, mut result: ffi::duckdb_result) {
        let columns = slice::from_raw_parts(result.columns, result.column_count as usize);
        println!("row-count: {}, column-count: {}", result.row_count, result.column_count);
        for i in 0..result.column_count {
            print!("column-name:{} ", CStr::from_ptr(columns[i as usize].name).to_string_lossy());
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

    pub fn execute(&mut self) -> Result<usize> {
        // TODO: change return type
        self.reset_result();
        unsafe {
            let rc = ffi::duckdb_execute_prepared(self.ptr, &mut self.result);
            if rc != ffi::DuckDBSuccess {
                let message = Some(CStr::from_ptr(self.result.error_message).to_string_lossy().to_string());
                return Err(error_from_duckdb_code(rc as c_int, message));
            }
            // self.print_result(self.result);
            Ok(self.result.row_changed as usize)
        }
    }

    #[inline]
    pub fn reset_result(&mut self) -> c_int {
        unsafe {
            ffi::duckdb_destroy_result(&mut self.result);
            self.result = mem::zeroed();
        }
        0
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
        panic!("sql: not supported")
    }

    #[inline]
    pub fn finalize(mut self) -> c_int {
        self.finalize_()
    }

    #[inline]
    fn finalize_(&mut self) -> c_int {
        self.reset_result();
        unsafe { ffi::duckdb_destroy_prepare(&mut self.ptr) };
        0
    }

    #[inline]
    pub fn get_status(&self, _: StatementStatus, _: bool) -> i32 {
        panic!("get_status: not supported")
    }
}

impl Drop for RawStatement {
    fn drop(&mut self) {
        self.finalize_();
    }
}

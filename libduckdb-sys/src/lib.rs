#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(deref_nullptr)]
#![allow(improper_ctypes)]

#[allow(clippy::all)]
mod bindings {
    include!(concat!(env!("OUT_DIR"), "/bindgen.rs"));
}
#[allow(clippy::all)]
pub use bindings::*;

pub const DuckDBError: duckdb_state = duckdb_state_DuckDBError;
pub const DuckDBSuccess: duckdb_state = duckdb_state_DuckDBSuccess;

pub use self::error::*;
mod error;

#[cfg(test)]
mod tests {
    use super::*;
    use std::convert::TryFrom;
    use std::ffi::{CStr, CString};
    use std::mem;
    use std::os::raw::{c_char, c_void};
    use std::ptr;

    use arrow::array::{Array, ArrayData, Int32Array, StructArray};
    use arrow::datatypes::DataType;
    use arrow::ffi::{ArrowArray, FFI_ArrowArray, FFI_ArrowSchema};

    unsafe fn print_int_result(mut result: duckdb_result) {
        for i in 0..duckdb_column_count(&mut result) {
            print!(
                "{} ",
                CStr::from_ptr(duckdb_column_name(&mut result, i)).to_string_lossy()
            );
        }
        println!();
        // print the data of the result
        for row_idx in 0..duckdb_row_count(&mut result) {
            for col_idx in 0..duckdb_column_count(&mut result) {
                let val = duckdb_value_int32(&mut result, col_idx, row_idx);
                print!("{} ", val);
            }
            println!();
        }
    }

    #[test]
    fn test_query_arrow() {
        unsafe {
            // open db
            let mut db: duckdb_database = ptr::null_mut();
            let mut con: duckdb_connection = ptr::null_mut();
            if duckdb_open(ptr::null_mut(), &mut db) != duckdb_state_DuckDBSuccess {
                panic!("duckdb_open error")
            }
            if duckdb_connect(db, &mut con) != duckdb_state_DuckDBSuccess {
                panic!("duckdb_connect error")
            }
            // create a table
            let sql = CString::new("CREATE TABLE integers(i INTEGER, j INTEGER);").unwrap();
            if duckdb_query(con, sql.as_ptr() as *const c_char, ptr::null_mut()) != duckdb_state_DuckDBSuccess {
                panic!("CREATE TABLE error")
            }

            // insert three rows into the table
            let sql = CString::new("INSERT INTO integers VALUES (3, 4), (5, 6), (7, NULL);").unwrap();
            let mut result: duckdb_arrow = ptr::null_mut();
            if duckdb_query_arrow(con, sql.as_ptr() as *const c_char, &mut result) != duckdb_state_DuckDBSuccess {
                panic!("INSERT error")
            }
            assert_eq!(duckdb_arrow_rows_changed(result), 3);
            duckdb_destroy_arrow(&mut result);

            // query rows again
            let mut result: duckdb_arrow = ptr::null_mut();
            let sql = CString::new("select i, j from integers order by i desc").unwrap();
            if duckdb_query_arrow(con, sql.as_ptr() as *const c_char, &mut result) != duckdb_state_DuckDBSuccess {
                panic!("SELECT error")
            }
            assert_eq!(duckdb_arrow_row_count(result), 3);
            assert_eq!(duckdb_arrow_column_count(result), 2);

            let mut arrays = &FFI_ArrowArray::empty();
            let mut schema = &FFI_ArrowSchema::empty();
            let schema = &mut schema;
            if duckdb_query_arrow_schema(result, schema as *mut _ as *mut *mut c_void) != duckdb_state_DuckDBSuccess {
                panic!("SELECT error")
            }
            let arrays = &mut arrays;
            if duckdb_query_arrow_array(result, arrays as *mut _ as *mut *mut c_void) != duckdb_state_DuckDBSuccess {
                panic!("SELECT error")
            }
            let arrow_array =
                ArrowArray::try_from_raw(*arrays as *const FFI_ArrowArray, *schema as *const FFI_ArrowSchema)
                    .expect("ok");
            let array_data = ArrayData::try_from(arrow_array).expect("ok");
            let struct_array = StructArray::from(array_data);
            assert_eq!(struct_array.len(), 3);
            assert_eq!(struct_array.columns().len(), 2);
            assert_eq!(struct_array.column(0).data_type(), &DataType::Int32);
            assert_eq!(struct_array.column(1).data_type(), &DataType::Int32);
            let arr_i = struct_array.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
            assert_eq!(arr_i.value(0), 7);
            assert_eq!(arr_i.value(1), 5);
            assert_eq!(arr_i.value(2), 3);
            let arr_j = struct_array.column(1).as_any().downcast_ref::<Int32Array>().unwrap();
            assert!(arr_j.is_null(0));
            assert_eq!(arr_j.value(1), 6);
            assert_eq!(arr_j.value(2), 4);

            let mut arrays: duckdb_arrow_array = ptr::null_mut();
            if duckdb_query_arrow_array(result, &mut arrays) != duckdb_state_DuckDBSuccess {
                panic!("SELECT error")
            }
            assert!(arrays.is_null());
            duckdb_destroy_arrow(&mut result);
            duckdb_disconnect(&mut con);
            duckdb_close(&mut db);
        }
    }

    #[test]
    fn basic_api_usage() {
        unsafe {
            // open db
            let mut db: duckdb_database = ptr::null_mut();
            let mut con: duckdb_connection = ptr::null_mut();
            if duckdb_open(ptr::null_mut(), &mut db) != duckdb_state_DuckDBSuccess {
                panic!("duckdb_open error")
            }
            if duckdb_connect(db, &mut con) != duckdb_state_DuckDBSuccess {
                panic!("duckdb_connect error")
            }
            // create a table
            let sql = CString::new("CREATE TABLE integers(i INTEGER, j INTEGER);").unwrap();
            if duckdb_query(con, sql.as_ptr() as *const c_char, ptr::null_mut()) != duckdb_state_DuckDBSuccess {
                panic!("CREATE TABLE error")
            }
            // insert three rows into the table
            let sql = CString::new("INSERT INTO integers VALUES (3, 4), (5, 6), (7, NULL);").unwrap();
            if duckdb_query(con, sql.as_ptr() as *const c_char, ptr::null_mut()) != duckdb_state_DuckDBSuccess {
                panic!("INSERT error")
            }
            // query rows again
            let mut result: duckdb_result = mem::zeroed();
            let sql = CString::new("select * from integers").unwrap();
            if duckdb_query(con, sql.as_ptr() as *const c_char, &mut result) != duckdb_state_DuckDBSuccess {
                panic!(
                    "SELECT error: {}",
                    CStr::from_ptr(duckdb_result_error(&mut result)).to_string_lossy()
                )
            }
            assert_eq!(duckdb_row_count(&mut result), 3);
            assert_eq!(duckdb_column_count(&mut result), 2);
            print_int_result(result);
            duckdb_destroy_result(&mut result);

            // test prepare
            let mut stmt: duckdb_prepared_statement = ptr::null_mut();
            let sql = CString::new("select * from integers where i>?").unwrap();
            if duckdb_prepare(con, sql.as_ptr() as *const c_char, &mut stmt) != duckdb_state_DuckDBSuccess {
                panic!("Prepare error");
            }
            if duckdb_bind_int32(stmt, 1, 4) != duckdb_state_DuckDBSuccess {
                panic!("Bind params error");
            }
            if duckdb_execute_prepared(stmt, &mut result) != duckdb_state_DuckDBSuccess {
                panic!("Execute prepared error");
            }
            assert_eq!(duckdb_row_count(&mut result), 2);
            assert_eq!(duckdb_column_count(&mut result), 2);
            print_int_result(result);
            duckdb_destroy_result(&mut result);

            // test bind params again
            if duckdb_bind_int32(stmt, 1, 5) != duckdb_state_DuckDBSuccess {
                panic!("Bind params error");
            }
            if duckdb_execute_prepared(stmt, &mut result) != duckdb_state_DuckDBSuccess {
                panic!("Execute prepared error");
            }
            assert_eq!(duckdb_row_count(&mut result), 1);
            assert_eq!(duckdb_column_count(&mut result), 2);
            print_int_result(result);
            duckdb_destroy_result(&mut result);
            duckdb_destroy_prepare(&mut stmt);

            // clean up
            duckdb_disconnect(&mut con);
            duckdb_close(&mut db);
        }
    }
}

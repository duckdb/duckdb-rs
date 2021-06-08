#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(improper_ctypes)]

mod bindings {
    include!(concat!(env!("OUT_DIR"), "/bindgen.rs"));
}
pub use bindings::*;

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::{CStr, CString};
    use std::mem;
    use std::os::raw::c_char;
    use std::ptr;
    use std::slice;

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
                    CStr::from_ptr(result.error_message).to_string_lossy()
                )
            }
            assert_eq!(result.row_count, 3);
            assert_eq!(result.column_count, 2);
            // print the names of the result
            let columns = slice::from_raw_parts(result.columns, result.column_count as usize);
            for i in 0..result.column_count {
                print!("{} ", CStr::from_ptr(columns[i as usize].name).to_string_lossy());
            }
            println!();
            // print the data of the result
            for row_idx in 0..result.row_count {
                for col_idx in 0..result.column_count {
                    let val = duckdb_value_varchar(&mut result, col_idx, row_idx);
                    print!("{} ", CStr::from_ptr(val).to_string_lossy());
                }
                println!();
            }
            // clean up
            duckdb_destroy_result(&mut result);
            duckdb_disconnect(&mut con);
            duckdb_close(&mut db);
        }
    }
}

#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(deref_nullptr)]
#![allow(improper_ctypes)]

/// Dependency-free Arrow C Data Interface structs used by DuckDB Arrow APIs.
pub mod arrow_c_data;
pub use arrow_c_data::{ArrowArray, ArrowSchema};

#[allow(clippy::all, unsafe_op_in_unsafe_fn)]
mod bindings {
    use crate::arrow_c_data::{ArrowArray, ArrowSchema};

    include!(concat!(env!("OUT_DIR"), "/bindgen.rs"));
}
#[allow(clippy::all)]
pub use bindings::*;

mod string;
pub use string::*;

pub const DuckDBError: duckdb_state = duckdb_state_DuckDBError;
pub const DuckDBSuccess: duckdb_state = duckdb_state_DuckDBSuccess;

pub use self::error::*;
mod error;

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        ffi::{CStr, CString},
        mem::{self, align_of, offset_of, size_of},
        os::raw::c_char,
        ptr,
    };

    use arrow::{
        array::{Array, Int32Array, StructArray},
        datatypes::DataType,
        ffi::{FFI_ArrowArray, FFI_ArrowSchema, from_ffi},
    };

    macro_rules! assert_field_offset {
        ($left:ty, $right:ty, $field:ident) => {
            assert_eq!(
                offset_of!($left, $field),
                offset_of!($right, $field),
                concat!("field offset differs for ", stringify!($field))
            );
        };
    }

    #[test]
    fn arrow_rs_ffi_layouts_match() {
        assert_eq!(size_of::<ArrowArray>(), size_of::<FFI_ArrowArray>());
        assert_eq!(align_of::<ArrowArray>(), align_of::<FFI_ArrowArray>());
        assert_field_offset!(ArrowArray, FFI_ArrowArray, length);
        assert_field_offset!(ArrowArray, FFI_ArrowArray, null_count);
        assert_field_offset!(ArrowArray, FFI_ArrowArray, offset);
        assert_field_offset!(ArrowArray, FFI_ArrowArray, n_buffers);
        assert_field_offset!(ArrowArray, FFI_ArrowArray, n_children);
        assert_field_offset!(ArrowArray, FFI_ArrowArray, buffers);
        assert_field_offset!(ArrowArray, FFI_ArrowArray, children);
        assert_field_offset!(ArrowArray, FFI_ArrowArray, dictionary);
        assert_field_offset!(ArrowArray, FFI_ArrowArray, release);
        assert_field_offset!(ArrowArray, FFI_ArrowArray, private_data);

        assert_eq!(size_of::<ArrowSchema>(), size_of::<FFI_ArrowSchema>());
        assert_eq!(align_of::<ArrowSchema>(), align_of::<FFI_ArrowSchema>());
        assert_field_offset!(ArrowSchema, FFI_ArrowSchema, format);
        assert_field_offset!(ArrowSchema, FFI_ArrowSchema, name);
        assert_field_offset!(ArrowSchema, FFI_ArrowSchema, metadata);
        assert_field_offset!(ArrowSchema, FFI_ArrowSchema, flags);
        assert_field_offset!(ArrowSchema, FFI_ArrowSchema, n_children);
        assert_field_offset!(ArrowSchema, FFI_ArrowSchema, children);
        assert_field_offset!(ArrowSchema, FFI_ArrowSchema, dictionary);
        assert_field_offset!(ArrowSchema, FFI_ArrowSchema, release);
        assert_field_offset!(ArrowSchema, FFI_ArrowSchema, private_data);
    }

    unsafe fn assert_no_error(mut error: duckdb_error_data) {
        if unsafe { !duckdb_error_data_has_error(error) } {
            unsafe {
                duckdb_destroy_error_data(&mut error);
            }
            return;
        }

        let message = {
            let message = unsafe { duckdb_error_data_message(error) };
            if message.is_null() {
                "<no error message>".into()
            } else {
                unsafe { CStr::from_ptr(message) }.to_string_lossy().into_owned()
            }
        };
        unsafe {
            duckdb_destroy_error_data(&mut error);
        }
        panic!("DuckDB error: {message}");
    }

    unsafe fn print_int_result(result: &mut duckdb_result) {
        unsafe {
            for i in 0..duckdb_column_count(result) {
                print!("{} ", CStr::from_ptr(duckdb_column_name(result, i)).to_string_lossy());
            }
            println!();
            // print the data of the result
            for row_idx in 0..duckdb_row_count(result) {
                for col_idx in 0..duckdb_column_count(result) {
                    let val = duckdb_value_int32(result, col_idx, row_idx);
                    print!("{val} ");
                }
                println!();
            }
        }
    }

    #[test]
    fn test_new_arrow_c_data_api() {
        unsafe {
            let mut db: duckdb_database = ptr::null_mut();
            let mut con: duckdb_connection = ptr::null_mut();
            if duckdb_open(ptr::null_mut(), &mut db) != duckdb_state_DuckDBSuccess {
                panic!("duckdb_open error")
            }
            if duckdb_connect(db, &mut con) != duckdb_state_DuckDBSuccess {
                panic!("duckdb_connect error")
            }

            let sql = CString::new(
                "SELECT 7::INTEGER AS i, 11::INTEGER AS j UNION ALL SELECT 5::INTEGER, NULL::INTEGER ORDER BY i DESC",
            )
            .unwrap();
            let mut result: duckdb_result = mem::zeroed();
            if duckdb_query(con, sql.as_ptr() as *const c_char, &mut result) != duckdb_state_DuckDBSuccess {
                let message = {
                    let message = duckdb_result_error(&mut result);
                    if message.is_null() {
                        "<no error message>".into()
                    } else {
                        CStr::from_ptr(message).to_string_lossy().into_owned()
                    }
                };
                panic!("SELECT error: {message}");
            }

            let mut arrow_options = duckdb_result_get_arrow_options(&mut result);
            assert!(!arrow_options.is_null());

            let column_count = duckdb_column_count(&mut result) as usize;
            let mut types = Vec::with_capacity(column_count);
            let mut names = Vec::with_capacity(column_count);
            for column in 0..column_count {
                let column_type = duckdb_column_logical_type(&mut result, column as idx_t);
                assert!(!column_type.is_null());
                let column_name = duckdb_column_name(&mut result, column as idx_t);
                assert!(!column_name.is_null());
                types.push(column_type);
                names.push(column_name);
            }

            let mut schema = ArrowSchema::empty();
            assert_no_error(duckdb_to_arrow_schema(
                arrow_options,
                types.as_mut_ptr(),
                names.as_mut_ptr(),
                column_count as idx_t,
                &mut schema,
            ));
            assert!(schema.release.is_some());
            assert_eq!(schema.n_children, 2);

            let mut chunk = duckdb_fetch_chunk(result);
            assert!(!chunk.is_null());

            let mut array = ArrowArray::empty();
            assert_no_error(duckdb_data_chunk_to_arrow(arrow_options, chunk, &mut array));
            assert!(array.release.is_some());
            assert_eq!(array.length, 2);
            assert_eq!(array.n_children, 2);

            let ffi_array = ptr::read((&array as *const ArrowArray).cast::<FFI_ArrowArray>());
            assert!(array.release.take().is_some());
            let ffi_schema = ptr::read((&schema as *const ArrowSchema).cast::<FFI_ArrowSchema>());
            assert!(schema.release.take().is_some());
            let array_data = from_ffi(ffi_array, &ffi_schema).unwrap_or_else(|error| panic!("from_ffi: {error}"));
            let struct_array = StructArray::from(array_data);
            assert_eq!(struct_array.len(), 2);
            assert_eq!(struct_array.columns().len(), 2);
            assert_eq!(struct_array.column(0).data_type(), &DataType::Int32);
            assert_eq!(struct_array.column(1).data_type(), &DataType::Int32);

            let arr_i = struct_array.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
            assert_eq!(arr_i.value(0), 7);
            assert_eq!(arr_i.value(1), 5);
            let arr_j = struct_array.column(1).as_any().downcast_ref::<Int32Array>().unwrap();
            assert_eq!(arr_j.value(0), 11);
            assert!(arr_j.is_null(1));

            drop(struct_array);
            drop(ffi_schema);

            duckdb_destroy_data_chunk(&mut chunk);
            for column_type in &mut types {
                duckdb_destroy_logical_type(column_type);
            }
            duckdb_destroy_arrow_options(&mut arrow_options);
            duckdb_destroy_result(&mut result);
            duckdb_disconnect(&mut con);
            duckdb_close(&mut db);
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

            let mut arrays = FFI_ArrowArray::empty();
            let mut schema = FFI_ArrowSchema::empty();
            if duckdb_query_arrow_schema(
                result,
                &mut std::ptr::addr_of_mut!(schema) as *mut _ as *mut duckdb_arrow_schema,
            ) != duckdb_state_DuckDBSuccess
            {
                panic!("SELECT error")
            }
            if duckdb_query_arrow_array(
                result,
                &mut std::ptr::addr_of_mut!(arrays) as *mut _ as *mut duckdb_arrow_array,
            ) != duckdb_state_DuckDBSuccess
            {
                panic!("SELECT error")
            }
            let array_data = from_ffi(arrays, &schema).expect("ok");
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
            print_int_result(&mut result);
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
            print_int_result(&mut result);
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
            print_int_result(&mut result);
            duckdb_destroy_result(&mut result);
            duckdb_destroy_prepare(&mut stmt);

            // clean up
            duckdb_disconnect(&mut con);
            duckdb_close(&mut db);
        }
    }
}

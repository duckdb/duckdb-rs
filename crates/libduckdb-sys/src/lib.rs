#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(deref_nullptr)]
#![allow(improper_ctypes)]

/// Dependency-free Arrow C Data Interface structs used by DuckDB Arrow APIs.
mod arrow_c_data;
pub use arrow_c_data::{ArrowArray, ArrowSchema};

// Build-script helper mounted here so cargo test runs its unit tests.
#[cfg(test)]
mod build_paths;

#[allow(clippy::all, unsafe_op_in_unsafe_fn)]
mod bindings {
    // Bindgen preserves DuckDB's C API comments verbatim. Some of those comments
    // contain C snippets and prose that rustdoc parses as Rust links or HTML.
    #![allow(rustdoc::broken_intra_doc_links, rustdoc::invalid_html_tags)]

    // Bindgen references these blocklisted forward declarations unqualified.
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
        mem,
        os::raw::c_char,
        ptr,
        sync::Arc,
    };

    use arrow::{
        array::{Array, ArrayRef, Int32Array, StructArray},
        datatypes::{DataType, Field},
        ffi::{FFI_ArrowArray, FFI_ArrowSchema, from_ffi, to_ffi},
    };

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

    struct TestDb {
        db: duckdb_database,
        con: duckdb_connection,
    }

    impl TestDb {
        unsafe fn open() -> Self {
            let mut db: duckdb_database = ptr::null_mut();
            let mut con: duckdb_connection = ptr::null_mut();
            unsafe {
                if duckdb_open(ptr::null_mut(), &mut db) != duckdb_state_DuckDBSuccess {
                    panic!("duckdb_open error")
                }
                if duckdb_connect(db, &mut con) != duckdb_state_DuckDBSuccess {
                    duckdb_close(&mut db);
                    panic!("duckdb_connect error")
                }
            }
            Self { db, con }
        }

        fn connection(&self) -> duckdb_connection {
            self.con
        }

        unsafe fn query_result(&self, sql: &str) -> duckdb_result {
            unsafe {
                let sql = CString::new(sql).unwrap();
                let mut result: duckdb_result = mem::zeroed();
                if duckdb_query(self.con, sql.as_ptr() as *const c_char, &mut result) != duckdb_state_DuckDBSuccess {
                    let message = result_error_message(&mut result).unwrap_or_else(|| "unknown error".to_string());
                    duckdb_destroy_result(&mut result);
                    panic!("DuckDB query failed: {message}");
                }
                result
            }
        }

        unsafe fn create_integers_table(&self) {
            unsafe {
                let mut create_result = self.query_result("CREATE TABLE integers(i INTEGER, j INTEGER);");
                duckdb_destroy_result(&mut create_result);

                let mut insert_result = self.query_result("INSERT INTO integers VALUES (3, 4), (5, 6), (7, NULL);");
                assert_eq!(duckdb_rows_changed(&mut insert_result), 3);
                duckdb_destroy_result(&mut insert_result);
            }
        }
    }

    impl Drop for TestDb {
        fn drop(&mut self) {
            unsafe {
                duckdb_disconnect(&mut self.con);
                duckdb_close(&mut self.db);
            }
        }
    }

    unsafe fn result_error_message(result: &mut duckdb_result) -> Option<String> {
        unsafe {
            let message = duckdb_result_error(result);
            if message.is_null() {
                None
            } else {
                Some(CStr::from_ptr(message).to_string_lossy().into_owned())
            }
        }
    }

    unsafe fn assert_no_arrow_error(error_data: duckdb_error_data) {
        unsafe {
            if let Some((error_type, message)) = take_arrow_error(error_data) {
                panic!("DuckDB Arrow C Data Interface conversion failed with type {error_type}: {message}");
            }
        }
    }

    unsafe fn take_arrow_error(mut error_data: duckdb_error_data) -> Option<(duckdb_error_type, String)> {
        unsafe {
            if error_data.is_null() {
                return None;
            }

            if !duckdb_error_data_has_error(error_data) {
                duckdb_destroy_error_data(&mut error_data);
                return None;
            }

            let error_type = duckdb_error_data_error_type(error_data);
            let message = duckdb_error_data_message(error_data);
            let message = if message.is_null() {
                "<no error message>".to_string()
            } else {
                CStr::from_ptr(message).to_string_lossy().into_owned()
            };
            duckdb_destroy_error_data(&mut error_data);
            Some((error_type, message))
        }
    }

    #[test]
    fn test_arrow_c_data_interface_to_data_chunk() {
        unsafe {
            let db = TestDb::open();
            let con = db.connection();
            let vector_size = duckdb_vector_size() as usize;
            let row_count = vector_size + 3;
            let ids: Vec<i32> = (0..row_count as i32).collect();
            let scores: Vec<Option<i32>> = (0..row_count)
                .map(|i| if i == vector_size { None } else { Some(i as i32 * 2) })
                .collect();

            let struct_array = StructArray::from(vec![
                (
                    Arc::new(Field::new("id", DataType::Int32, false)),
                    Arc::new(Int32Array::from(ids)) as ArrayRef,
                ),
                (
                    Arc::new(Field::new("score", DataType::Int32, true)),
                    Arc::new(Int32Array::from(scores)) as ArrayRef,
                ),
            ]);
            let (mut arrow_array, mut arrow_schema) = to_ffi(&struct_array.to_data()).expect("export Arrow array");

            let mut converted_schema: duckdb_arrow_converted_schema = ptr::null_mut();
            assert_no_arrow_error(duckdb_schema_from_arrow(
                con,
                &mut arrow_schema as *mut FFI_ArrowSchema as *mut ArrowSchema,
                &mut converted_schema,
            ));
            assert!(!converted_schema.is_null());

            let mut chunk: duckdb_data_chunk = ptr::null_mut();
            assert_no_arrow_error(duckdb_data_chunk_from_arrow(
                con,
                &mut arrow_array as *mut FFI_ArrowArray as *mut ArrowArray,
                converted_schema,
                &mut chunk,
            ));
            assert!(!chunk.is_null());

            assert_eq!(duckdb_data_chunk_get_column_count(chunk), 2);
            assert_eq!(duckdb_data_chunk_get_size(chunk), row_count as idx_t);

            let id_vector = duckdb_data_chunk_get_vector(chunk, 0);
            let id_data = duckdb_vector_get_data(id_vector) as *const i32;
            assert_eq!(*id_data, 0);
            assert_eq!(*id_data.add(vector_size), vector_size as i32);
            assert_eq!(*id_data.add(row_count - 1), row_count as i32 - 1);

            let score_vector = duckdb_data_chunk_get_vector(chunk, 1);
            let score_data = duckdb_vector_get_data(score_vector) as *const i32;
            assert_eq!(*score_data, 0);
            assert_eq!(*score_data.add(row_count - 1), (row_count as i32 - 1) * 2);

            let score_validity = duckdb_vector_get_validity(score_vector);
            assert!(!score_validity.is_null());
            assert!(duckdb_validity_row_is_valid(score_validity, 0));
            assert!(!duckdb_validity_row_is_valid(score_validity, vector_size as idx_t));
            assert!(duckdb_validity_row_is_valid(score_validity, vector_size as idx_t + 1));

            duckdb_destroy_data_chunk(&mut chunk);
            duckdb_destroy_arrow_converted_schema(&mut converted_schema);
        }
    }

    unsafe extern "C" fn noop_arrow_schema_release(_schema: *mut ArrowSchema) {}

    #[test]
    fn test_arrow_c_data_interface_reports_schema_errors() {
        unsafe {
            let db = TestDb::open();
            let con = db.connection();

            let root_format = CString::new("+s").unwrap();
            let child_format = CString::new("?").unwrap();
            let child_name = CString::new("unsupported").unwrap();
            let mut child_schema = ArrowSchema::empty();
            child_schema.format = child_format.as_ptr();
            child_schema.name = child_name.as_ptr();
            child_schema.release = Some(noop_arrow_schema_release);

            let mut child_schema_ptr = &mut child_schema as *mut ArrowSchema;
            let mut root_schema = ArrowSchema::empty();
            root_schema.format = root_format.as_ptr();
            root_schema.n_children = 1;
            root_schema.children = &mut child_schema_ptr;
            root_schema.release = Some(noop_arrow_schema_release);

            let mut converted_schema: duckdb_arrow_converted_schema = ptr::null_mut();
            let (error_type, message) =
                take_arrow_error(duckdb_schema_from_arrow(con, &mut root_schema, &mut converted_schema))
                    .expect("expected DuckDB Arrow C Data Interface conversion to fail");
            assert_eq!(error_type, duckdb_error_type_DUCKDB_ERROR_INVALID_INPUT);
            assert!(message.contains("Unsupported Internal Arrow Type"));
            assert!(converted_schema.is_null());
        }
    }

    #[test]
    fn test_result_data_chunk_to_arrow() {
        unsafe {
            let db = TestDb::open();
            db.create_integers_table();

            // Query rows and export the resulting data chunks to Arrow.
            let mut result = db.query_result("select i, j from integers order by i desc");
            assert_eq!(duckdb_column_count(&mut result), 2);

            let mut schema = FFI_ArrowSchema::empty();
            let mut arrow_options = duckdb_result_get_arrow_options(&mut result);
            assert!(!arrow_options.is_null());

            let mut column_types = Vec::new();
            let mut column_names = Vec::new();
            for column in 0..duckdb_column_count(&mut result) {
                let logical_type = duckdb_column_logical_type(&mut result, column);
                assert!(!logical_type.is_null());
                column_types.push(logical_type);

                let name = duckdb_column_name(&mut result, column);
                assert!(!name.is_null());
                column_names.push(name);
            }

            assert_no_arrow_error(duckdb_to_arrow_schema(
                arrow_options,
                column_types.as_mut_ptr(),
                column_names.as_mut_ptr(),
                column_types.len() as idx_t,
                &mut schema as *mut FFI_ArrowSchema as *mut ArrowSchema,
            ));

            let mut chunks = Vec::new();
            loop {
                let mut chunk = duckdb_fetch_chunk(result);
                if chunk.is_null() {
                    break;
                }

                let mut arrays = FFI_ArrowArray::empty();
                assert_no_arrow_error(duckdb_data_chunk_to_arrow(
                    arrow_options,
                    chunk,
                    &mut arrays as *mut FFI_ArrowArray as *mut ArrowArray,
                ));
                duckdb_destroy_data_chunk(&mut chunk);

                let array_data = from_ffi(arrays, &schema).expect("ok");
                chunks.push(StructArray::from(array_data));
            }
            if let Some(message) = result_error_message(&mut result) {
                panic!("DuckDB result error while fetching chunks: {message}");
            }

            assert_eq!(chunks.len(), 1);
            let struct_array = &chunks[0];
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

            for logical_type in &mut column_types {
                duckdb_destroy_logical_type(logical_type);
            }
            duckdb_destroy_arrow_options(&mut arrow_options);
            duckdb_destroy_result(&mut result);
        }
    }

    #[test]
    fn basic_api_usage() {
        unsafe {
            let db = TestDb::open();
            let con = db.connection();
            db.create_integers_table();

            // query rows
            let mut result = db.query_result("select * from integers");
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
        }
    }
}

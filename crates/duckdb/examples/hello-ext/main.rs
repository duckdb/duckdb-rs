#![warn(unsafe_op_in_unsafe_fn)]

extern crate duckdb;
extern crate duckdb_loadable_macros;
extern crate libduckdb_sys;

use duckdb::{
    core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId},
    vtab::{BindInfo, Free, FunctionInfo, InitInfo, VTab},
    Connection, Result,
};
use duckdb_loadable_macros::duckdb_entrypoint;
use libduckdb_sys as ffi;
use std::{
    error::Error,
    ffi::{c_char, c_void, CString},
    ptr,
};

struct HelloBindData {
    name: String,
}

impl Free for HelloBindData {}

struct HelloInitData {
    done: bool,
}

struct HelloVTab;

impl Free for HelloInitData {}

impl VTab for HelloVTab {
    type InitData = HelloInitData;
    type BindData = HelloBindData;

    unsafe fn bind(bind: &BindInfo, data: *mut HelloBindData) -> Result<(), Box<dyn std::error::Error>> {
        bind.add_result_column("column0", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        let name = bind.get_parameter(0).to_string();
        unsafe {
            ptr::write(data, HelloBindData { name });
        }
        Ok(())
    }

    unsafe fn init(_: &InitInfo, data: *mut HelloInitData) -> Result<(), Box<dyn std::error::Error>> {
        unsafe {
            ptr::write(data, HelloInitData { done: false });
        }
        Ok(())
    }

    unsafe fn func(func: &FunctionInfo, output: &mut DataChunkHandle) -> Result<(), Box<dyn std::error::Error>> {
        let init_info = unsafe { func.get_init_data::<HelloInitData>().as_mut().unwrap() };
        let bind_info = unsafe { func.get_bind_data::<HelloBindData>().as_mut().unwrap() };

        if init_info.done {
            output.set_len(0);
        } else {
            init_info.done = true;
            let vector = output.flat_vector(0);
            let result = CString::new(format!("Hello {}", bind_info.name))?;
            vector.insert(0, result);
            output.set_len(1);
        }
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![LogicalTypeHandle::from(LogicalTypeId::Varchar)])
    }
}

// Exposes a extern C function named "libhello_ext_init" in the compiled dynamic library,
// the "entrypoint" that duckdb will use to load the extension.
#[duckdb_entrypoint]
pub fn libhello_ext_init(conn: Connection) -> Result<(), Box<dyn Error>> {
    conn.register_table_function::<HelloVTab>("hello")?;
    Ok(())
}

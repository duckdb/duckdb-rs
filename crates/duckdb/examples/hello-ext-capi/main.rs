extern crate duckdb;
extern crate duckdb_loadable_macros;
extern crate libduckdb_sys;

use duckdb::{
    core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId},
    vtab::{BindInfo, Free, FunctionInfo, InitInfo, VTab},
    Connection, Result,
};
use duckdb_loadable_macros::duckdb_entrypoint_c_api;
use libduckdb_sys as ffi;
use std::{
    error::Error,
    ffi::{c_char, CString},
};

#[repr(C)]
struct HelloBindData {
    name: *mut c_char,
}

impl Free for HelloBindData {
    fn free(&mut self) {
        unsafe {
            if self.name.is_null() {
                return;
            }
            drop(CString::from_raw(self.name));
        }
    }
}

#[repr(C)]
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
        let param = bind.get_parameter(0).to_string();
        unsafe {
            (*data).name = CString::new(param).unwrap().into_raw();
        }
        Ok(())
    }

    unsafe fn init(_: &InitInfo, data: *mut HelloInitData) -> Result<(), Box<dyn std::error::Error>> {
        unsafe {
            (*data).done = false;
        }
        Ok(())
    }

    unsafe fn func(func: &FunctionInfo, output: &mut DataChunkHandle) -> Result<(), Box<dyn std::error::Error>> {
        let init_info = func.get_init_data::<HelloInitData>();
        let bind_info = func.get_bind_data::<HelloBindData>();

        unsafe {
            if (*init_info).done {
                output.set_len(0);
            } else {
                (*init_info).done = true;
                let vector = output.flat_vector(0);
                let name = CString::from_raw((*bind_info).name);
                let result = CString::new(format!("Hello {}", name.to_str()?))?;
                // Can't consume the CString
                (*bind_info).name = CString::into_raw(name);
                vector.insert(0, result);
                output.set_len(1);
            }
        }
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![LogicalTypeHandle::from(LogicalTypeId::Varchar)])
    }
}

#[duckdb_entrypoint_c_api(ext_name = "rusty_quack", min_duckdb_version = "v0.0.1")]
pub fn extension_entrypoint(con: Connection) -> Result<(), Box<dyn Error>> {
    con.register_table_function::<HelloVTab>("hello")
        .expect("Failed to register hello table function");
    Ok(())
}

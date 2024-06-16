extern crate duckdb;
extern crate duckdb_loadable_macros;
extern crate libduckdb_sys;

use crate::duckdb::core::Inserter;
use duckdb::{
    core::{DataChunk, DataChunkHandle, FlatVector, LogicalTypeHandle, LogicalTypeId},
    vtab::{BindInfo, Free, FunctionInfo, InitInfo, VTab},
    Connection, Result,
};
use duckdb_loadable_macros::duckdb_entrypoint;
use libduckdb_sys as ffi;
use std::{
    error::Error,
    ffi::{c_char, c_void, CString},
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

    fn bind(bind: &BindInfo, data: *mut HelloBindData) -> Result<(), Box<dyn std::error::Error>> {
        bind.add_result_column("column0", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        let param = bind.get_parameter(0).to_string();
        unsafe {
            (*data).name = CString::new(param).unwrap().into_raw();
        }
        Ok(())
    }

    fn init(_: &InitInfo, data: *mut HelloInitData) -> Result<(), Box<dyn std::error::Error>> {
        unsafe {
            (*data).done = false;
        }
        Ok(())
    }

    fn func(func: &FunctionInfo, output: DataChunkHandle) -> Result<(), Box<dyn std::error::Error>> {
        let init_info = func.get_init_data::<HelloInitData>();
        let bind_info = func.get_bind_data::<HelloBindData>();
        let mut data_chunk = DataChunk::from_existing_handle(output);

        unsafe {
            if (*init_info).done {
                data_chunk.set_len(0);
            } else {
                (*init_info).done = true;
                data_chunk.reserve(1);
                let vector = data_chunk.columns[0].try_as_flat_mut().unwrap();
                let name = CString::from_raw((*bind_info).name);
                let result = CString::new(format!("Hello {}", name.to_str()?))?;
                // Can't consume the CString
                (*bind_info).name = CString::into_raw(name);
                vector.insert(0, result);
                data_chunk.set_len(1);
            }
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

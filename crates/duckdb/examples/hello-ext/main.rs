#![warn(unsafe_op_in_unsafe_fn)]

extern crate duckdb;
extern crate duckdb_loadable_macros;
extern crate libduckdb_sys;

use duckdb::{
    core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId},
    vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab},
    Connection, Result,
};
use duckdb_loadable_macros::duckdb_entrypoint;
use libduckdb_sys as ffi;
use std::{
    error::Error,
    ffi::{c_char, c_void, CString},
    sync::atomic::{AtomicBool, Ordering},
};

struct HelloBindData {
    name: String,
}

struct HelloInitData {
    done: AtomicBool,
}

struct HelloVTab;

impl VTab for HelloVTab {
    type InitData = HelloInitData;
    type BindData = HelloBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        bind.add_result_column("column0", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        let name = bind.get_parameter(0).to_string();
        Ok(HelloBindData { name })
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        Ok(HelloInitData {
            done: AtomicBool::new(false),
        })
    }

    fn func(func: &TableFunctionInfo<Self>, output: &mut DataChunkHandle) -> Result<(), Box<dyn std::error::Error>> {
        let init_data = func.get_init_data();
        let bind_data = func.get_bind_data();
        if init_data.done.swap(true, Ordering::Relaxed) {
            output.set_len(0);
        } else {
            let vector = output.flat_vector(0);
            let result = CString::new(format!("Hello {}", bind_data.name))?;
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

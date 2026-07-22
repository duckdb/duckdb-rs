use super::*;
use crate::core::{Inserter, LogicalTypeId};
use std::{
    error::Error,
    ffi::CString,
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

    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn std::error::Error>> {
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

struct HelloWithNamedVTab {}
impl VTab for HelloWithNamedVTab {
    type InitData = HelloInitData;
    type BindData = HelloBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        bind.add_result_column("column0", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        let name = bind.get_named_parameter("name").unwrap().to_string();
        assert!(bind.get_named_parameter("unknown_name").is_none());
        Ok(HelloBindData { name })
    }

    fn init(init_info: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
        HelloVTab::init(init_info)
    }

    fn func(func: &TableFunctionInfo<Self>, output: &mut DataChunkHandle) -> Result<(), Box<dyn Error>> {
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

    fn named_parameters() -> Option<Vec<(String, LogicalTypeHandle)>> {
        Some(vec![(
            "name".to_string(),
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        )])
    }
}

#[test]
fn test_table_function() -> Result<(), Box<dyn Error>> {
    let conn = Connection::open_in_memory()?;
    conn.register_table_function::<HelloVTab>("hello")?;

    let val = conn.query_row("select * from hello('duckdb')", [], |row| <(String,)>::try_from(row))?;
    assert_eq!(val, ("Hello duckdb".to_string(),));

    Ok(())
}

#[test]
fn test_named_table_function() -> Result<(), Box<dyn Error>> {
    let conn = Connection::open_in_memory()?;
    conn.register_table_function::<HelloWithNamedVTab>("hello_named")?;

    let val = conn.query_row("select * from hello_named(name = 'duckdb')", [], |row| {
        <(String,)>::try_from(row)
    })?;
    assert_eq!(val, ("Hello duckdb".to_string(),));

    Ok(())
}

// Test table function with extra info
struct PrefixVTab;

impl VTab for PrefixVTab {
    type InitData = HelloInitData;
    type BindData = HelloBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        bind.add_result_column("column0", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        let name = bind.get_parameter(0).to_string();
        Ok(HelloBindData { name })
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
        Ok(HelloInitData {
            done: AtomicBool::new(false),
        })
    }

    fn func(func: &TableFunctionInfo<Self>, output: &mut DataChunkHandle) -> Result<(), Box<dyn Error>> {
        let init_data = func.get_init_data();
        let bind_data = func.get_bind_data();
        let prefix = unsafe { &*func.get_extra_info::<String>() };

        if init_data.done.swap(true, Ordering::Relaxed) {
            output.set_len(0);
        } else {
            let vector = output.flat_vector(0);
            let result = CString::new(format!("{prefix} {}", bind_data.name))?;
            vector.insert(0, result);
            output.set_len(1);
        }
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![LogicalTypeHandle::from(LogicalTypeId::Varchar)])
    }
}

#[test]
fn test_table_function_with_extra_info() -> Result<(), Box<dyn Error>> {
    let conn = Connection::open_in_memory()?;
    conn.register_table_function_with_extra_info::<PrefixVTab, _>("greet", &"Howdy".to_string())?;

    let val = conn.query_row("select * from greet('partner')", [], |row| <(String,)>::try_from(row))?;
    assert_eq!(val, ("Howdy partner".to_string(),));

    Ok(())
}

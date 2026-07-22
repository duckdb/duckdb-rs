use super::*;
use crate::{
    callback::test_support::{HazardousPanicPayload, PanickingDisplayError},
    core::{Inserter, LogicalTypeId},
};
use std::{
    error::Error,
    ffi::CString,
    sync::atomic::{AtomicBool, AtomicU64, Ordering},
};

fn write_single_bigint_row(output: &mut DataChunkHandle) {
    let mut vector = output.flat_vector(0);
    unsafe { vector.as_mut_slice_with_len::<i64>(1)[0] = 42 };
    output.set_len(1);
}

fn write_single_bigint_row_once(done: &AtomicBool, output: &mut DataChunkHandle) {
    if done.swap(true, Ordering::SeqCst) {
        output.set_len(0);
    } else {
        write_single_bigint_row(output);
    }
}

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

struct PanickingVTab<const STAGE: u8>;

struct PanickingVTabBindData {
    _allocation: Box<[u8]>,
}

struct PanickingVTabInitData {
    calls: AtomicU64,
    _allocation: Box<[u8]>,
}

impl<const STAGE: u8> VTab for PanickingVTab<STAGE> {
    type InitData = PanickingVTabInitData;
    type BindData = PanickingVTabBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        if STAGE == 0 {
            panic!("table bind callback panic")
        }
        bind.add_result_column("value", LogicalTypeId::Bigint.into());
        Ok(PanickingVTabBindData {
            _allocation: vec![0; 32].into_boxed_slice(),
        })
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
        if STAGE == 1 {
            panic!("table init callback panic")
        }
        Ok(PanickingVTabInitData {
            calls: AtomicU64::new(0),
            _allocation: vec![0; 32].into_boxed_slice(),
        })
    }

    fn func(func: &TableFunctionInfo<Self>, output: &mut DataChunkHandle) -> Result<(), Box<dyn Error>> {
        let call = func.get_init_data().calls.fetch_add(1, Ordering::SeqCst);
        if STAGE == 2 && call > 0 {
            panic!("table execution callback panic")
        }
        if STAGE == 2 {
            write_single_bigint_row(output);
        } else {
            output.set_len(0);
        }
        Ok(())
    }
}

static TABLE_RECOVERY_PANICKED: AtomicBool = AtomicBool::new(false);

struct RecoveringVTab;

struct RecoveringVTabInitData {
    done: AtomicBool,
}

impl VTab for RecoveringVTab {
    type InitData = RecoveringVTabInitData;
    type BindData = ();

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        bind.add_result_column("value", LogicalTypeId::Bigint.into());
        Ok(())
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
        Ok(RecoveringVTabInitData {
            done: AtomicBool::new(false),
        })
    }

    fn func(func: &TableFunctionInfo<Self>, output: &mut DataChunkHandle) -> Result<(), Box<dyn Error>> {
        if !TABLE_RECOVERY_PANICKED.swap(true, Ordering::SeqCst) {
            panic!("recoverable table execution callback panic")
        }

        write_single_bigint_row_once(&func.get_init_data().done, output);
        Ok(())
    }
}

struct NulErrorVTab<const STAGE: u8>;

impl<const STAGE: u8> VTab for NulErrorVTab<STAGE> {
    type InitData = ();
    type BindData = ();

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        if STAGE == 0 {
            return Err("table bind before\0after".into());
        }
        bind.add_result_column("value", LogicalTypeId::Bigint.into());
        Ok(())
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
        if STAGE == 1 {
            Err("table init before\0after".into())
        } else {
            Ok(())
        }
    }

    fn func(_: &TableFunctionInfo<Self>, output: &mut DataChunkHandle) -> Result<(), Box<dyn Error>> {
        if STAGE == 2 {
            Err("table execution before\0after".into())
        } else {
            output.set_len(0);
            Ok(())
        }
    }
}

struct PanickingDisplayVTab;

impl VTab for PanickingDisplayVTab {
    type InitData = ();
    type BindData = ();

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        bind.add_result_column("value", LogicalTypeId::Bigint.into());
        Ok(())
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
        Ok(())
    }

    fn func(_: &TableFunctionInfo<Self>, _: &mut DataChunkHandle) -> Result<(), Box<dyn Error>> {
        Err(Box::new(PanickingDisplayError))
    }
}

static TABLE_PANIC_PAYLOADS: AtomicU64 = AtomicU64::new(0);
static TABLE_PANIC_PAYLOAD_DROPS: AtomicU64 = AtomicU64::new(0);

struct HazardousPanicPayloadVTab;

impl VTab for HazardousPanicPayloadVTab {
    type InitData = ();
    type BindData = ();

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        bind.add_result_column("value", LogicalTypeId::Bigint.into());
        Ok(())
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
        Ok(())
    }

    fn func(_: &TableFunctionInfo<Self>, _: &mut DataChunkHandle) -> Result<(), Box<dyn Error>> {
        TABLE_PANIC_PAYLOADS.fetch_add(1, Ordering::SeqCst);
        HazardousPanicPayload::panic(&TABLE_PANIC_PAYLOAD_DROPS)
    }
}

static TABLE_BIND_DATA_DROPS: AtomicU64 = AtomicU64::new(0);
static TABLE_INIT_DATA_DROPS: AtomicU64 = AtomicU64::new(0);
static TABLE_EXTRA_INFO_DROPS: AtomicU64 = AtomicU64::new(0);
static TABLE_BIND_DATA_CREATED: AtomicU64 = AtomicU64::new(0);
static TABLE_INIT_DATA_CREATED: AtomicU64 = AtomicU64::new(0);

struct PanickingBindDataDrop {
    _allocation: Box<[u8]>,
}

impl Drop for PanickingBindDataDrop {
    fn drop(&mut self) {
        TABLE_BIND_DATA_DROPS.fetch_add(1, Ordering::SeqCst);
        panic!("table bind-data destructor panic")
    }
}

struct PanickingInitDataDrop {
    done: AtomicBool,
}

impl Drop for PanickingInitDataDrop {
    fn drop(&mut self) {
        TABLE_INIT_DATA_DROPS.fetch_add(1, Ordering::SeqCst);
        panic!("table init-data destructor panic")
    }
}

struct PanickingExtraInfoDrop {
    panic_on_drop: bool,
}

impl Clone for PanickingExtraInfoDrop {
    fn clone(&self) -> Self {
        // The stack original drops quietly; only DuckDB's registered clone panics.
        Self { panic_on_drop: true }
    }
}

impl Drop for PanickingExtraInfoDrop {
    fn drop(&mut self) {
        if self.panic_on_drop {
            TABLE_EXTRA_INFO_DROPS.fetch_add(1, Ordering::SeqCst);
            panic!("table extra-info destructor panic")
        }
    }
}

struct PanickingDropVTab;

impl VTab for PanickingDropVTab {
    type InitData = PanickingInitDataDrop;
    type BindData = PanickingBindDataDrop;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        bind.add_result_column("value", LogicalTypeId::Bigint.into());
        TABLE_BIND_DATA_CREATED.fetch_add(1, Ordering::SeqCst);
        Ok(PanickingBindDataDrop {
            _allocation: vec![0; 32].into_boxed_slice(),
        })
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
        TABLE_INIT_DATA_CREATED.fetch_add(1, Ordering::SeqCst);
        Ok(PanickingInitDataDrop {
            done: AtomicBool::new(false),
        })
    }

    fn func(func: &TableFunctionInfo<Self>, output: &mut DataChunkHandle) -> Result<(), Box<dyn Error>> {
        write_single_bigint_row_once(&func.get_init_data().done, output);
        Ok(())
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

#[test]
fn table_panics_become_query_errors_at_each_callback_stage() -> Result<(), Box<dyn Error>> {
    let conn = Connection::open_in_memory()?;
    conn.register_table_function::<PanickingVTab<0>>("panicking_bind")?;
    conn.register_table_function::<PanickingVTab<1>>("panicking_init")?;
    conn.register_table_function::<PanickingVTab<2>>("panicking_func")?;

    let bind_error = conn.prepare("SELECT * FROM panicking_bind()").err().unwrap();
    assert!(bind_error.to_string().contains("table bind callback panic"));

    let init_error = conn.prepare("SELECT * FROM panicking_init()")?.query([]).err().unwrap();
    assert!(init_error.to_string().contains("table init callback panic"));

    let func_error = conn.prepare("SELECT * FROM panicking_func()")?.query([]).err().unwrap();
    assert!(func_error.to_string().contains("table execution callback panic"));

    let value: i64 = conn.query_row("SELECT 1 + 1", [], |row| row.get(0))?;
    assert_eq!(value, 2);
    Ok(())
}

#[test]
fn table_function_remains_usable_after_execution_panic() -> Result<(), Box<dyn Error>> {
    TABLE_RECOVERY_PANICKED.store(false, Ordering::SeqCst);

    let conn = Connection::open_in_memory()?;
    conn.register_table_function::<RecoveringVTab>("recovering_vtab")?;

    let error = conn
        .prepare("SELECT * FROM recovering_vtab()")?
        .query([])
        .err()
        .unwrap();
    assert!(error.to_string().contains("recoverable table execution callback panic"));

    let value: i64 = conn.query_row("SELECT * FROM recovering_vtab()", [], |row| row.get(0))?;
    assert_eq!(value, 42);
    Ok(())
}

#[test]
fn table_errors_escape_interior_nuls_at_every_callback_stage() -> Result<(), Box<dyn Error>> {
    let conn = Connection::open_in_memory()?;
    conn.register_table_function::<NulErrorVTab<0>>("nul_error_bind")?;
    conn.register_table_function::<NulErrorVTab<1>>("nul_error_init")?;
    conn.register_table_function::<NulErrorVTab<2>>("nul_error_func")?;

    let bind_error = conn.prepare("SELECT * FROM nul_error_bind()").err().unwrap();
    let init_error = conn.prepare("SELECT * FROM nul_error_init()")?.query([]).err().unwrap();
    let func_error = conn.prepare("SELECT * FROM nul_error_func()")?.query([]).err().unwrap();

    for (error, expected) in [
        (bind_error, "table bind before\\0after"),
        (init_error, "table init before\\0after"),
        (func_error, "table execution before\\0after"),
    ] {
        let message = error.to_string();
        assert!(message.contains(expected), "unexpected callback error: {message}");
    }
    Ok(())
}

#[test]
fn table_display_failures_become_query_errors() -> Result<(), Box<dyn Error>> {
    let conn = Connection::open_in_memory()?;
    conn.register_table_function::<PanickingDisplayVTab>("panicking_display")?;

    let error = conn
        .prepare("SELECT * FROM panicking_display()")?
        .query([])
        .err()
        .unwrap();

    assert!(error.to_string().contains("callback error display panic"));
    Ok(())
}

#[test]
fn table_hazardous_panic_payloads_are_contained() -> Result<(), Box<dyn Error>> {
    TABLE_PANIC_PAYLOADS.store(0, Ordering::SeqCst);
    TABLE_PANIC_PAYLOAD_DROPS.store(0, Ordering::SeqCst);

    let conn = Connection::open_in_memory()?;
    conn.register_table_function::<HazardousPanicPayloadVTab>("hazardous_panic_payload")?;

    let error = conn
        .prepare("SELECT * FROM hazardous_panic_payload()")?
        .query([])
        .err()
        .unwrap();

    assert!(error.to_string().contains("non-string panic payload"));
    let payloads = TABLE_PANIC_PAYLOADS.load(Ordering::SeqCst);
    assert!(payloads > 0);
    assert_eq!(TABLE_PANIC_PAYLOAD_DROPS.load(Ordering::SeqCst), payloads);
    Ok(())
}

#[test]
fn table_state_destructor_panics_are_contained() -> Result<(), Box<dyn Error>> {
    TABLE_BIND_DATA_DROPS.store(0, Ordering::SeqCst);
    TABLE_INIT_DATA_DROPS.store(0, Ordering::SeqCst);
    TABLE_EXTRA_INFO_DROPS.store(0, Ordering::SeqCst);
    TABLE_BIND_DATA_CREATED.store(0, Ordering::SeqCst);
    TABLE_INIT_DATA_CREATED.store(0, Ordering::SeqCst);
    {
        let conn = Connection::open_in_memory()?;
        let extra_info = PanickingExtraInfoDrop { panic_on_drop: false };
        conn.register_table_function_with_extra_info::<PanickingDropVTab, _>("panicking_drop", &extra_info)?;
        let value: i64 = conn.query_row("SELECT * FROM panicking_drop()", [], |row| row.get(0))?;
        assert_eq!(value, 42);
    }

    let bind_data_created = TABLE_BIND_DATA_CREATED.load(Ordering::SeqCst);
    let init_data_created = TABLE_INIT_DATA_CREATED.load(Ordering::SeqCst);
    assert!(bind_data_created > 0);
    assert!(init_data_created > 0);
    assert_eq!(TABLE_BIND_DATA_DROPS.load(Ordering::SeqCst), bind_data_created);
    assert_eq!(TABLE_INIT_DATA_DROPS.load(Ordering::SeqCst), init_data_created);
    assert_eq!(TABLE_EXTRA_INFO_DROPS.load(Ordering::SeqCst), 1);
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

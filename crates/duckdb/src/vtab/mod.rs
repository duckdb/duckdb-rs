// #![warn(unsafe_op_in_unsafe_fn)]

use std::ffi::c_void;

use crate::{
    Connection, Result,
    callback::{catch_boxed_callback, catch_drop},
    error::Error,
    inner_connection::InnerConnection,
};

use super::ffi;

mod function;
mod value;

/// The duckdb Arrow table function interface
#[cfg(feature = "vtab-arrow")]
pub mod arrow;
#[cfg(feature = "vtab-arrow")]
pub use self::arrow::{
    arrow_arraydata_to_query_params, arrow_ffi_to_query_params, arrow_recordbatch_to_query_params,
    record_batch_to_duckdb_data_chunk, to_duckdb_logical_type, to_duckdb_logical_type_for_field, to_duckdb_type_id,
};
pub use function::{BindInfo, InitInfo, TableFunction, TableFunctionInfo};
pub use value::Value;

use crate::core::{DataChunkHandle, LogicalTypeHandle};
use ffi::{duckdb_bind_info, duckdb_data_chunk, duckdb_function_info, duckdb_init_info};

/// Given a raw pointer to a box, free the box and the data contained within it.
///
/// # Safety
/// The pointer must be a valid pointer to a `Box<T>` created by `Box::into_raw`.
unsafe extern "C" fn drop_boxed<T>(v: *mut c_void) {
    catch_drop(|| drop(unsafe { Box::from_raw(v.cast::<T>()) }));
}

/// Duckdb table function trait
///
/// See to the HelloVTab example for more details
/// <https://duckdb.org/docs/api/c/table_functions>
pub trait VTab: Sized {
    /// The data type of the init data.
    ///
    /// The init data tracks the state of the table function and is global across threads.
    ///
    /// The init data is shared across threads so must be `Send + Sync`.
    type InitData: Sized + Send + Sync;

    /// The data type of the bind data.
    ///
    /// The bind data is shared across threads so must be `Send + Sync`.
    type BindData: Sized + Send + Sync;

    /// Bind data to the table function
    ///
    /// This function is used for determining the return type of a table producing function and returning bind data
    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>>;

    /// Initialize the table function
    fn init(init: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>>;

    /// Generate rows from the table function.
    ///
    /// The implementation should populate the `output` parameter with the rows to be returned.
    ///
    /// When the table function is done, the implementation should set the length of the output to 0.
    fn func(func: &TableFunctionInfo<Self>, output: &mut DataChunkHandle) -> Result<(), Box<dyn std::error::Error>>;

    /// Does the table function support pushdown
    /// default is false
    fn supports_pushdown() -> bool {
        false
    }
    /// The parameters of the table function
    /// default is None
    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        None
    }
    /// The named parameters of the table function
    /// default is None
    fn named_parameters() -> Option<Vec<(String, LogicalTypeHandle)>> {
        None
    }
}

unsafe extern "C" fn func<T>(info: duckdb_function_info, output: duckdb_data_chunk)
where
    T: VTab,
{
    let info = TableFunctionInfo::<T>::from(info);
    let result = catch_boxed_callback(|| unsafe {
        let mut data_chunk_handle = DataChunkHandle::new_unowned_output(output);
        T::func(&info, &mut data_chunk_handle)
    });
    if let Err(error) = result {
        info.set_error(&error);
    }
}

unsafe extern "C" fn init<T>(info: duckdb_init_info)
where
    T: VTab,
{
    let info = InitInfo::from(info);
    let result = catch_boxed_callback(|| T::init(&info));
    match result {
        Ok(init_data) => unsafe {
            info.set_init_data(
                Box::into_raw(Box::new(init_data)) as *mut c_void,
                Some(drop_boxed::<T::InitData>),
            );
        },
        Err(error) => info.set_error(&error),
    }
}

unsafe extern "C" fn bind<T>(info: duckdb_bind_info)
where
    T: VTab,
{
    let info = BindInfo::from(info);
    let result = catch_boxed_callback(|| T::bind(&info));
    match result {
        Ok(bind_data) => unsafe {
            info.set_bind_data(
                Box::into_raw(Box::new(bind_data)) as *mut c_void,
                Some(drop_boxed::<T::BindData>),
            );
        },
        Err(error) => info.set_error(&error),
    }
}

impl Connection {
    /// Register the given TableFunction with the current db
    #[inline]
    pub fn register_table_function<T: VTab>(&self, name: &str) -> Result<()> {
        let table_function = TableFunction::default();
        table_function
            .set_name(name)
            .supports_pushdown(T::supports_pushdown())
            .set_bind(Some(bind::<T>))
            .set_init(Some(init::<T>))
            .set_function(Some(func::<T>));
        for ty in T::parameters().unwrap_or_default() {
            table_function.add_parameter(&ty);
        }
        for (name, ty) in T::named_parameters().unwrap_or_default() {
            table_function.add_named_parameter(&name, &ty);
        }
        self.db.borrow_mut().register_table_function(table_function)
    }

    /// Register the given TableFunction with custom extra info.
    ///
    /// This allows you to pass extra info that can be accessed during bind, init, and execution
    /// via `BindInfo::get_extra_info`, `InitInfo::get_extra_info`, or `TableFunctionInfo::get_extra_info`.
    ///
    /// The extra info is cloned once during registration and stored in DuckDB's catalog.
    #[inline]
    pub fn register_table_function_with_extra_info<T: VTab, E>(&self, name: &str, extra_info: &E) -> Result<()>
    where
        E: Clone + Send + Sync + 'static,
    {
        let table_function = TableFunction::default();
        table_function
            .set_name(name)
            .supports_pushdown(T::supports_pushdown())
            .set_bind(Some(bind::<T>))
            .set_init(Some(init::<T>))
            .set_function(Some(func::<T>))
            .set_extra_info(extra_info.clone());
        for ty in T::parameters().unwrap_or_default() {
            table_function.add_parameter(&ty);
        }
        for (name, ty) in T::named_parameters().unwrap_or_default() {
            table_function.add_named_parameter(&name, &ty);
        }
        self.db.borrow_mut().register_table_function(table_function)
    }
}

impl InnerConnection {
    /// Register the given TableFunction with the current db
    pub fn register_table_function(&mut self, table_function: TableFunction) -> Result<()> {
        unsafe {
            let rc = ffi::duckdb_register_table_function(self.con, table_function.ptr);
            if rc != ffi::DuckDBSuccess {
                return Err(Error::DuckDBFailure(ffi::Error::new(rc), None));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::core::{Inserter, LogicalTypeId};
    use std::{
        error::Error,
        ffi::CString,
        sync::atomic::{AtomicBool, AtomicU64, Ordering},
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
                let mut vector = output.flat_vector(0);
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
                let mut vector = output.flat_vector(0);
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

    struct PanickingBindVTab;

    impl VTab for PanickingBindVTab {
        type InitData = ();
        type BindData = ();

        fn bind(_: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
            panic!("table bind callback panic")
        }

        fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
            Ok(())
        }

        fn func(_: &TableFunctionInfo<Self>, _: &mut DataChunkHandle) -> Result<(), Box<dyn Error>> {
            Ok(())
        }
    }

    struct PanickingInitVTab;

    impl VTab for PanickingInitVTab {
        type InitData = ();
        type BindData = ();

        fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
            bind.add_result_column("value", LogicalTypeId::Bigint.into());
            Ok(())
        }

        fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
            panic!("table init callback panic")
        }

        fn func(_: &TableFunctionInfo<Self>, _: &mut DataChunkHandle) -> Result<(), Box<dyn Error>> {
            Ok(())
        }
    }

    struct PanickingFuncVTab;

    impl VTab for PanickingFuncVTab {
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
            panic!("table execution callback panic")
        }
    }

    struct ErrorBindVTab;

    impl VTab for ErrorBindVTab {
        type InitData = ();
        type BindData = ();

        fn bind(_: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
            Err("table bind callback error".into())
        }

        fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
            Ok(())
        }

        fn func(_: &TableFunctionInfo<Self>, _: &mut DataChunkHandle) -> Result<(), Box<dyn Error>> {
            Ok(())
        }
    }

    struct ErrorInitVTab;

    impl VTab for ErrorInitVTab {
        type InitData = ();
        type BindData = ();

        fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
            bind.add_result_column("value", LogicalTypeId::Bigint.into());
            Ok(())
        }

        fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
            Err("table init callback error".into())
        }

        fn func(_: &TableFunctionInfo<Self>, _: &mut DataChunkHandle) -> Result<(), Box<dyn Error>> {
            Ok(())
        }
    }

    struct ErrorFuncVTab;

    impl VTab for ErrorFuncVTab {
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
            Err("table execution callback error".into())
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

    static TABLE_BIND_DATA_DROPS: AtomicU64 = AtomicU64::new(0);
    static TABLE_INIT_DATA_DROPS: AtomicU64 = AtomicU64::new(0);

    struct PanickingBindDataDrop;

    impl Drop for PanickingBindDataDrop {
        fn drop(&mut self) {
            TABLE_BIND_DATA_DROPS.fetch_add(1, Ordering::SeqCst);
            panic!("table bind-data destructor panic");
        }
    }

    struct PanickingInitDataDrop {
        done: AtomicBool,
    }

    impl Drop for PanickingInitDataDrop {
        fn drop(&mut self) {
            TABLE_INIT_DATA_DROPS.fetch_add(1, Ordering::SeqCst);
            panic!("table init-data destructor panic");
        }
    }

    struct PanickingDropVTab;

    impl VTab for PanickingDropVTab {
        type InitData = PanickingInitDataDrop;
        type BindData = PanickingBindDataDrop;

        fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
            bind.add_result_column("value", LogicalTypeId::Bigint.into());
            Ok(PanickingBindDataDrop)
        }

        fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
            Ok(PanickingInitDataDrop {
                done: AtomicBool::new(false),
            })
        }

        fn func(func: &TableFunctionInfo<Self>, output: &mut DataChunkHandle) -> Result<(), Box<dyn Error>> {
            if func.get_init_data().done.swap(true, Ordering::SeqCst) {
                output.set_len(0);
            } else {
                let mut vector = output.flat_vector(0);
                unsafe { vector.write(0, 42_i64) };
                output.set_len(1);
            }
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
    fn table_panics_become_query_errors() -> Result<(), Box<dyn Error>> {
        let conn = Connection::open_in_memory()?;
        conn.register_table_function::<PanickingBindVTab>("panicking_bind")?;
        conn.register_table_function::<PanickingInitVTab>("panicking_init")?;
        conn.register_table_function::<PanickingFuncVTab>("panicking_func")?;

        let bind_error = conn.prepare("SELECT * FROM panicking_bind()").err().unwrap();
        assert!(bind_error.to_string().contains("table bind callback panic"));

        let init_error = conn.prepare("SELECT * FROM panicking_init()")?.query([]).err().unwrap();
        assert!(init_error.to_string().contains("table init callback panic"));

        let func_error = conn.prepare("SELECT * FROM panicking_func()")?.query([]).err().unwrap();
        assert!(func_error.to_string().contains("table execution callback panic"));
        Ok(())
    }

    #[test]
    fn table_errors_become_query_errors() -> Result<(), Box<dyn Error>> {
        let conn = Connection::open_in_memory()?;
        conn.register_table_function::<ErrorBindVTab>("error_bind")?;
        conn.register_table_function::<ErrorInitVTab>("error_init")?;
        conn.register_table_function::<ErrorFuncVTab>("error_func")?;

        let bind_error = conn.prepare("SELECT * FROM error_bind()").err().unwrap();
        assert!(bind_error.to_string().contains("table bind callback error"));

        let init_error = conn.prepare("SELECT * FROM error_init()")?.query([]).err().unwrap();
        assert!(init_error.to_string().contains("table init callback error"));

        let func_error = conn.prepare("SELECT * FROM error_func()")?.query([]).err().unwrap();
        assert!(func_error.to_string().contains("table execution callback error"));
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
            assert!(!message.contains('\0'));
        }
        Ok(())
    }

    #[test]
    fn table_state_destructor_panics_are_contained() -> Result<(), Box<dyn Error>> {
        TABLE_BIND_DATA_DROPS.store(0, Ordering::SeqCst);
        TABLE_INIT_DATA_DROPS.store(0, Ordering::SeqCst);
        {
            let conn = Connection::open_in_memory()?;
            conn.register_table_function::<PanickingDropVTab>("panicking_drop")?;
            let value: i64 = conn.query_row("SELECT * FROM panicking_drop()", [], |row| row.get(0))?;
            assert_eq!(value, 42);
        }

        assert!(TABLE_BIND_DATA_DROPS.load(Ordering::SeqCst) > 0);
        assert!(TABLE_INIT_DATA_DROPS.load(Ordering::SeqCst) > 0);
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
                let mut vector = output.flat_vector(0);
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
}

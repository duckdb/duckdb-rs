// #![warn(unsafe_op_in_unsafe_fn)]

use std::ffi::c_void;

use crate::{error::Error, inner_connection::InnerConnection, Connection, Result};

use super::ffi;

mod function;

/// The duckdb Arrow table function interface
#[cfg(feature = "vtab-arrow")]
pub mod arrow;
#[cfg(feature = "vtab-arrow")]
pub use self::arrow::{
    arrow_arraydata_to_query_params, arrow_ffi_to_query_params, arrow_recordbatch_to_query_params,
    record_batch_to_duckdb_data_chunk, to_duckdb_logical_type, to_duckdb_type_id,
};
#[cfg(feature = "vtab-excel")]
mod excel;

pub use function::{BindInfo, InitInfo, TableFunction, TableFunctionInfo};

use crate::core::{DataChunkHandle, LogicalTypeHandle, LogicalTypeId};
use ffi::{duckdb_bind_info, duckdb_data_chunk, duckdb_function_info, duckdb_init_info};

/// Given a raw pointer to a box, free the box and the data contained within it.
///
/// # Safety
/// The pointer must be a valid pointer to a `Box<T>` created by `Box::into_raw`.
unsafe extern "C" fn drop_boxed<T>(v: *mut c_void) {
    drop(unsafe { Box::from_raw(v.cast::<T>()) });
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
    let mut data_chunk_handle = DataChunkHandle::new_unowned(output);
    let result = T::func(&info, &mut data_chunk_handle);
    if result.is_err() {
        info.set_error(&result.err().unwrap().to_string());
    }
}

unsafe extern "C" fn init<T>(info: duckdb_init_info)
where
    T: VTab,
{
    let info = InitInfo::from(info);
    match T::init(&info) {
        Ok(init_data) => {
            info.set_init_data(
                Box::into_raw(Box::new(init_data)) as *mut c_void,
                Some(drop_boxed::<T::InitData>),
            );
        }
        Err(e) => {
            info.set_error(&e.to_string());
        }
    }
}

unsafe extern "C" fn bind<T>(info: duckdb_bind_info)
where
    T: VTab,
{
    let info = BindInfo::from(info);
    match T::bind(&info) {
        Ok(bind_data) => {
            info.set_bind_data(
                Box::into_raw(Box::new(bind_data)) as *mut c_void,
                Some(drop_boxed::<T::BindData>),
            );
        }
        Err(e) => {
            info.set_error(&e.to_string());
        }
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
        ffi::{c_char, CString},
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

    #[cfg(feature = "vtab-loadable")]
    use duckdb_loadable_macros::duckdb_entrypoint;

    // this function is never called, but is still type checked
    // Exposes a extern C function named "libhello_ext_init" in the compiled dynamic library,
    // the "entrypoint" that duckdb will use to load the extension.
    #[cfg(feature = "vtab-loadable")]
    #[duckdb_entrypoint]
    fn libhello_ext_init(conn: Connection) -> Result<(), Box<dyn Error>> {
        conn.register_table_function::<HelloVTab>("hello")?;
        Ok(())
    }
}

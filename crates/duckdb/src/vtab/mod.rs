use crate::{error::Error, inner_connection::InnerConnection, Connection, Result};

use super::{ffi, ffi::duckdb_free};
use std::ffi::c_void;

mod function;
mod value;

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

pub use function::{BindInfo, FunctionInfo, InitInfo, TableFunction};
pub use value::Value;

use crate::core::{DataChunkHandle, LogicalTypeHandle, LogicalTypeId};
use ffi::{duckdb_bind_info, duckdb_data_chunk, duckdb_function_info, duckdb_init_info};

use ffi::duckdb_malloc;
use std::mem::size_of;

/// duckdb_malloc a struct of type T
/// used for the bind_info and init_info
/// # Safety
/// This function is obviously unsafe
unsafe fn malloc_data_c<T>() -> *mut T {
    duckdb_malloc(size_of::<T>()).cast()
}

/// free bind or info data
///
/// # Safety
///   This function is obviously unsafe
/// TODO: maybe we should use a Free trait here
unsafe extern "C" fn drop_data_c<T: Free>(v: *mut c_void) {
    let actual = v.cast::<T>();
    (*actual).free();
    duckdb_free(v);
}

/// Free trait for the bind and init data
pub trait Free {
    /// Free the data
    fn free(&mut self) {}
}

/// Duckdb table function trait
///
/// See to the HelloVTab example for more details
/// <https://duckdb.org/docs/api/c/table_functions>
pub trait VTab: Sized {
    /// The data type of the bind data
    type InitData: Sized + Free;
    /// The data type of the init data
    type BindData: Sized + Free;

    /// Bind data to the table function
    ///
    /// # Safety
    ///
    /// This function is unsafe because it dereferences raw pointers (`data`) and manipulates the memory directly.
    /// The caller must ensure that:
    ///
    /// - The `data` pointer is valid and points to a properly initialized `BindData` instance.
    /// - The lifetime of `data` must outlive the execution of `bind` to avoid dangling pointers, especially since
    ///   `bind` does not take ownership of `data`.
    /// - Concurrent access to `data` (if applicable) must be properly synchronized.
    /// - The `bind` object must be valid and correctly initialized.
    unsafe fn bind(bind: &BindInfo, data: *mut Self::BindData) -> Result<(), Box<dyn std::error::Error>>;
    /// Initialize the table function
    ///
    /// # Safety
    ///
    /// This function is unsafe because it performs raw pointer dereferencing on the `data` argument.
    /// The caller is responsible for ensuring that:
    ///
    /// - The `data` pointer is non-null and points to a valid `InitData` instance.
    /// - There is no data race when accessing `data`, meaning if `data` is accessed from multiple threads,
    ///   proper synchronization is required.
    /// - The lifetime of `data` extends beyond the scope of this call to avoid use-after-free errors.
    unsafe fn init(init: &InitInfo, data: *mut Self::InitData) -> Result<(), Box<dyn std::error::Error>>;
    /// The actual function
    ///
    /// # Safety
    ///
    /// This function is unsafe because it:
    ///
    /// - Dereferences multiple raw pointers (`func` to access `init_info` and `bind_info`).
    ///
    /// The caller must ensure that:
    ///
    /// - All pointers (`func`, `output`, internal `init_info`, and `bind_info`) are valid and point to the expected types of data structures.
    /// - The `init_info` and `bind_info` data pointed to remains valid and is not freed until after this function completes.
    /// - No other threads are concurrently mutating the data pointed to by `init_info` and `bind_info` without proper synchronization.
    /// - The `output` parameter is correctly initialized and can safely be written to.
    unsafe fn func(func: &FunctionInfo, output: &mut DataChunkHandle) -> Result<(), Box<dyn std::error::Error>>;
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
    let info = FunctionInfo::from(info);
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
    let data = malloc_data_c::<T::InitData>();
    let result = T::init(&info, data);
    info.set_init_data(data.cast(), Some(drop_data_c::<T::InitData>));
    if result.is_err() {
        info.set_error(&result.err().unwrap().to_string());
    }
}

unsafe extern "C" fn bind<T>(info: duckdb_bind_info)
where
    T: VTab,
{
    let info = BindInfo::from(info);
    let data = malloc_data_c::<T::BindData>();
    let result = T::bind(&info, data);
    info.set_bind_data(data.cast(), Some(drop_data_c::<T::BindData>));
    if result.is_err() {
        info.set_error(&result.err().unwrap().to_string());
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
    use crate::core::Inserter;
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

    struct HelloWithNamedVTab {}
    impl VTab for HelloWithNamedVTab {
        type InitData = HelloInitData;
        type BindData = HelloBindData;

        unsafe fn bind(bind: &BindInfo, data: *mut HelloBindData) -> Result<(), Box<dyn Error>> {
            bind.add_result_column("column0", LogicalTypeHandle::from(LogicalTypeId::Varchar));
            let param = bind.get_named_parameter("name").unwrap().to_string();
            assert!(bind.get_named_parameter("unknown_name").is_none());
            unsafe {
                (*data).name = CString::new(param).unwrap().into_raw();
            }
            Ok(())
        }

        unsafe fn init(init_info: &InitInfo, data: *mut HelloInitData) -> Result<(), Box<dyn Error>> {
            HelloVTab::init(init_info, data)
        }

        unsafe fn func(func: &FunctionInfo, output: &mut DataChunkHandle) -> Result<(), Box<dyn Error>> {
            HelloVTab::func(func, output)
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

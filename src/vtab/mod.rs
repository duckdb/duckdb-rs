use crate::error::Error;
use crate::inner_connection::InnerConnection;
use crate::{Connection, Result};

use super::ffi;
use super::ffi::duckdb_free;
use std::ffi::c_void;

mod data_chunk;
#[cfg(feature = "excel")]
mod excel;
mod function;
mod logical_type;
mod value;
mod vector;

pub use data_chunk::DataChunk;
pub use function::{BindInfo, FunctionInfo, InitInfo, TableFunction};
pub use logical_type::{LogicalType, LogicalTypeId};
pub use value::Value;
pub use vector::{FlatVector, Inserter, ListVector, StructVector, Vector};

use ffi::{duckdb_bind_info, duckdb_data_chunk, duckdb_function_info, duckdb_init_info};

/// Returns a `*const c_char` pointer to the given string
#[macro_export]
macro_rules! as_string {
    ($x:expr) => {
        std::ffi::CString::new($x).expect("c string").as_ptr().cast::<c_char>()
    };
}
pub(crate) use as_string;

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
/// https://duckdb.org/docs/api/c/table_functions
pub trait VTab: Sized {
    /// The data type of the bind data
    type InitData: Sized + Free;
    /// The data type of the init data
    type BindData: Sized + Free;

    /// Bind data to the table function
    fn bind(bind: &BindInfo, data: *mut Self::BindData) -> Result<(), Box<dyn std::error::Error>>;
    /// Initialize the table function
    fn init(init: &InitInfo, data: *mut Self::InitData) -> Result<(), Box<dyn std::error::Error>>;
    /// The actual function
    fn func(func: &FunctionInfo, output: &DataChunk) -> Result<(), Box<dyn std::error::Error>>;
    /// Does the table function support pushdown
    /// default is false
    fn supports_pushdown() -> bool {
        false
    }
    /// The parameters of the table function
    /// default is None
    fn parameters() -> Option<Vec<LogicalType>> {
        None
    }
}

unsafe extern "C" fn func<T>(info: duckdb_function_info, output: duckdb_data_chunk)
where
    T: VTab,
{
    let info = FunctionInfo::from(info);
    let output = DataChunk::from(output);
    let result = T::func(&info, &output);
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
        self.db.borrow_mut().register_table_funcion(table_function)
    }
}

impl InnerConnection {
    /// Register the given TableFunction with the current db
    pub fn register_table_funcion(&mut self, table_function: TableFunction) -> Result<()> {
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
    use crate::{Connection, Result};
    use std::error::Error;
    use std::ffi::{c_char, CString};

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
            bind.add_result_column("column0", LogicalType::new(LogicalTypeId::Varchar));
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

        fn func(func: &FunctionInfo, output: &DataChunk) -> Result<(), Box<dyn std::error::Error>> {
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

        fn parameters() -> Option<Vec<LogicalType>> {
            Some(vec![LogicalType::new(LogicalTypeId::Varchar)])
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
}

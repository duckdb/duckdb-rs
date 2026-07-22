// #![warn(unsafe_op_in_unsafe_fn)]

use std::ffi::c_void;

use crate::{Connection, Result, error::Error, inner_connection::InnerConnection};

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
    unsafe {
        let info = TableFunctionInfo::<T>::from(info);
        let mut data_chunk_handle = DataChunkHandle::new_unowned(output);
        let result = T::func(&info, &mut data_chunk_handle);
        if let Err(e) = result {
            info.set_error(&e.to_string());
        }
    }
}

unsafe extern "C" fn init<T>(info: duckdb_init_info)
where
    T: VTab,
{
    unsafe {
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
}

unsafe extern "C" fn bind<T>(info: duckdb_bind_info)
where
    T: VTab,
{
    unsafe {
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
mod tests;

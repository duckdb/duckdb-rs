use super::ffi::{
    duckdb_bind_add_result_column, duckdb_bind_get_extra_info, duckdb_bind_get_parameter,
    duckdb_bind_get_parameter_count, duckdb_bind_info, duckdb_bind_set_bind_data, duckdb_bind_set_cardinality,
    duckdb_bind_set_error, duckdb_create_table_function, duckdb_data_chunk, duckdb_delete_callback_t,
    duckdb_destroy_table_function, duckdb_table_function, duckdb_table_function_add_parameter,
    duckdb_table_function_init_t, duckdb_table_function_set_bind, duckdb_table_function_set_extra_info,
    duckdb_table_function_set_function, duckdb_table_function_set_init, duckdb_table_function_set_local_init,
    duckdb_table_function_set_name, duckdb_table_function_supports_projection_pushdown, idx_t,
};
use super::LogicalType;
use super::{as_string, Value};
use std::ffi::{c_void, CString};
use std::os::raw::c_char;

/// An interface to store and retrieve data during the function bind stage
#[derive(Debug)]
pub struct BindInfo {
    ptr: *mut c_void,
}

impl BindInfo {
    /// Adds a result column to the output of the table function.
    ///
    /// # Arguments
    ///  * `name`: The name of the column
    ///  * `type`: The logical type of the column
    pub fn add_result_column(&self, column_name: &str, column_type: LogicalType) {
        unsafe {
            duckdb_bind_add_result_column(self.ptr, as_string!(column_name), column_type.ptr);
        }
    }
    /// Report that an error has occurred while calling bind.
    ///
    /// # Arguments
    ///  * `error`: The error message
    pub fn set_error(&self, error: &str) {
        unsafe {
            duckdb_bind_set_error(self.ptr, as_string!(error));
        }
    }
    /// Sets the user-provided bind data in the bind object. This object can be retrieved again during execution.
    ///
    /// # Arguments
    ///  * `extra_data`: The bind data object.
    ///  * `destroy`: The callback that will be called to destroy the bind data (if any)
    ///
    /// # Safety
    ///
    pub unsafe fn set_bind_data(&self, data: *mut c_void, free_function: Option<unsafe extern "C" fn(*mut c_void)>) {
        duckdb_bind_set_bind_data(self.ptr, data, free_function);
    }
    /// Retrieves the number of regular (non-named) parameters to the function.
    pub fn get_parameter_count(&self) -> u64 {
        unsafe { duckdb_bind_get_parameter_count(self.ptr) }
    }
    /// Retrieves the parameter at the given index.
    ///
    /// # Arguments
    ///  * `index`: The index of the parameter to get
    ///
    /// returns: The value of the parameter
    pub fn get_parameter(&self, param_index: u64) -> Value {
        unsafe { Value::from(duckdb_bind_get_parameter(self.ptr, param_index)) }
    }

    /// Sets the cardinality estimate for the table function, used for optimization.
    ///
    /// # Arguments
    /// * `cardinality`: The cardinality estimate
    /// * `is_exact`: Whether or not the cardinality estimate is exact, or an approximation
    pub fn set_cardinality(&self, cardinality: idx_t, is_exact: bool) {
        unsafe { duckdb_bind_set_cardinality(self.ptr, cardinality, is_exact) }
    }
    /// Retrieves the extra info of the function as set in [`TableFunction::set_extra_info`]
    ///
    /// # Arguments
    /// * `returns`: The extra info
    pub fn get_extra_info<T>(&self) -> *const T {
        unsafe { duckdb_bind_get_extra_info(self.ptr).cast() }
    }
}

impl From<duckdb_bind_info> for BindInfo {
    fn from(ptr: duckdb_bind_info) -> Self {
        Self { ptr }
    }
}

use super::ffi::{
    duckdb_init_get_bind_data, duckdb_init_get_column_count, duckdb_init_get_column_index, duckdb_init_get_extra_info,
    duckdb_init_info, duckdb_init_set_error, duckdb_init_set_init_data, duckdb_init_set_max_threads,
};

/// An interface to store and retrieve data during the function init stage
#[derive(Debug)]
pub struct InitInfo(duckdb_init_info);

impl From<duckdb_init_info> for InitInfo {
    fn from(ptr: duckdb_init_info) -> Self {
        Self(ptr)
    }
}

impl InitInfo {
    /// # Safety
    pub unsafe fn set_init_data(&self, data: *mut c_void, freeer: Option<unsafe extern "C" fn(*mut c_void)>) {
        duckdb_init_set_init_data(self.0, data, freeer);
    }

    /// Returns the column indices of the projected columns at the specified positions.
    ///
    /// This function must be used if projection pushdown is enabled to figure out which columns to emit.
    ///
    /// returns: The column indices at which to get the projected column index
    pub fn get_column_indices(&self) -> Vec<idx_t> {
        let mut indices;
        unsafe {
            let column_count = duckdb_init_get_column_count(self.0);
            indices = Vec::with_capacity(column_count as usize);
            for i in 0..column_count {
                indices.push(duckdb_init_get_column_index(self.0, i))
            }
        }
        indices
    }

    /// Retrieves the extra info of the function as set in [`TableFunction::set_extra_info`]
    ///
    /// # Arguments
    /// * `returns`: The extra info
    pub fn get_extra_info<T>(&self) -> *const T {
        unsafe { duckdb_init_get_extra_info(self.0).cast() }
    }
    /// Gets the bind data set by [`BindInfo::set_bind_data`] during the bind.
    ///
    /// Note that the bind data should be considered as read-only.
    /// For tracking state, use the init data instead.
    ///
    /// # Arguments
    /// * `returns`: The bind data object
    pub fn get_bind_data<T>(&self) -> *const T {
        unsafe { duckdb_init_get_bind_data(self.0).cast() }
    }
    /// Sets how many threads can process this table function in parallel (default: 1)
    ///
    /// # Arguments
    /// * `max_threads`: The maximum amount of threads that can process this table function
    pub fn set_max_threads(&self, max_threads: idx_t) {
        unsafe { duckdb_init_set_max_threads(self.0, max_threads) }
    }
    /// Report that an error has occurred while calling init.
    ///
    /// # Arguments
    /// * `error`: The error message
    pub fn set_error(&self, error: &str) {
        unsafe { duckdb_init_set_error(self.0, as_string!(error)) }
    }
}

/// A function that returns a queryable table
#[derive(Debug)]
pub struct TableFunction {
    pub(crate) ptr: duckdb_table_function,
}

impl Drop for TableFunction {
    fn drop(&mut self) {
        unsafe {
            duckdb_destroy_table_function(&mut self.ptr);
        }
    }
}

impl TableFunction {
    /// Sets whether or not the given table function supports projection pushdown.
    ///
    /// If this is set to true, the system will provide a list of all required columns in the `init` stage through
    /// the [`InitInfo::get_column_indices`] method.
    /// If this is set to false (the default), the system will expect all columns to be projected.
    ///
    /// # Arguments
    ///  * `pushdown`: True if the table function supports projection pushdown, false otherwise.
    pub fn supports_pushdown(&self, supports: bool) -> &Self {
        unsafe {
            duckdb_table_function_supports_projection_pushdown(self.ptr, supports);
        }
        self
    }

    /// Adds a parameter to the table function.
    ///
    /// # Arguments
    ///  * `logical_type`: The type of the parameter to add.
    pub fn add_parameter(&self, logical_type: &LogicalType) -> &Self {
        unsafe {
            duckdb_table_function_add_parameter(self.ptr, logical_type.ptr);
        }
        self
    }

    /// Sets the main function of the table function
    ///
    /// # Arguments
    ///  * `function`: The function
    pub fn set_function(
        &self,
        func: Option<unsafe extern "C" fn(info: duckdb_function_info, output: duckdb_data_chunk)>,
    ) -> &Self {
        unsafe {
            duckdb_table_function_set_function(self.ptr, func);
        }
        self
    }

    /// Sets the init function of the table function
    ///
    /// # Arguments
    ///  * `function`: The init function
    pub fn set_init(&self, init_func: Option<unsafe extern "C" fn(*mut c_void)>) -> &Self {
        unsafe {
            duckdb_table_function_set_init(self.ptr, init_func);
        }
        self
    }

    /// Sets the bind function of the table function
    ///
    /// # Arguments
    ///  * `function`: The bind function
    pub fn set_bind(&self, bind_func: Option<unsafe extern "C" fn(*mut c_void)>) -> &Self {
        unsafe {
            duckdb_table_function_set_bind(self.ptr, bind_func);
        }
        self
    }

    /// Creates a new empty table function.
    pub fn new() -> Self {
        Self {
            ptr: unsafe { duckdb_create_table_function() },
        }
    }

    /// Sets the name of the given table function.
    ///
    /// # Arguments
    ///  * `name`: The name of the table function
    pub fn set_name(&self, name: &str) -> &TableFunction {
        unsafe {
            let string = CString::from_vec_unchecked(name.as_bytes().into());
            duckdb_table_function_set_name(self.ptr, string.as_ptr());
        }
        self
    }

    /// Assigns extra information to the table function that can be fetched during binding, etc.
    ///
    /// # Arguments
    /// * `extra_info`: The extra information
    /// * `destroy`: The callback that will be called to destroy the bind data (if any)
    ///
    /// # Safety
    pub unsafe fn set_extra_info(&self, extra_info: *mut c_void, destroy: duckdb_delete_callback_t) {
        duckdb_table_function_set_extra_info(self.ptr, extra_info, destroy);
    }

    /// Sets the thread-local init function of the table function
    ///
    /// # Arguments
    /// * `init`: The init function
    pub fn set_local_init(&self, init: duckdb_table_function_init_t) {
        unsafe { duckdb_table_function_set_local_init(self.ptr, init) };
    }
}

impl Default for TableFunction {
    fn default() -> Self {
        Self::new()
    }
}

use super::ffi::{
    duckdb_function_get_bind_data, duckdb_function_get_extra_info, duckdb_function_get_init_data,
    duckdb_function_get_local_init_data, duckdb_function_info, duckdb_function_set_error,
};

/// An interface to store and retrieve data during the function execution stage
#[derive(Debug)]
pub struct FunctionInfo(duckdb_function_info);

impl FunctionInfo {
    /// Report that an error has occurred while executing the function.
    ///
    /// # Arguments
    ///  * `error`: The error message
    pub fn set_error(&self, error: &str) {
        unsafe {
            duckdb_function_set_error(self.0, as_string!(error));
        }
    }
    /// Gets the bind data set by [`BindInfo::set_bind_data`] during the bind.
    ///
    /// Note that the bind data should be considered as read-only.
    /// For tracking state, use the init data instead.
    ///
    /// # Arguments
    /// * `returns`: The bind data object
    pub fn get_bind_data<T>(&self) -> *mut T {
        unsafe { duckdb_function_get_bind_data(self.0).cast() }
    }
    /// Gets the init data set by [`InitInfo::set_init_data`] during the init.
    ///
    /// # Arguments
    /// * `returns`: The init data object
    pub fn get_init_data<T>(&self) -> *mut T {
        unsafe { duckdb_function_get_init_data(self.0).cast() }
    }
    /// Retrieves the extra info of the function as set in [`TableFunction::set_extra_info`]
    ///
    /// # Arguments
    /// * `returns`: The extra info
    pub fn get_extra_info<T>(&self) -> *mut T {
        unsafe { duckdb_function_get_extra_info(self.0).cast() }
    }
    /// Gets the thread-local init data set by [`InitInfo::set_init_data`] during the local_init.
    ///
    /// # Arguments
    /// * `returns`: The init data object
    pub fn get_local_init_data<T>(&self) -> *mut T {
        unsafe { duckdb_function_get_local_init_data(self.0).cast() }
    }
}

impl From<duckdb_function_info> for FunctionInfo {
    fn from(ptr: duckdb_function_info) -> Self {
        Self(ptr)
    }
}

pub struct ScalarFunctionSet {
    ptr: duckdb_scalar_function_set,
}

impl ScalarFunctionSet {
    pub fn new(name: &str) -> Self {
        let c_name = CString::new(name).expect("name should contain valid utf-8");
        Self {
            ptr: unsafe { duckdb_create_scalar_function_set(c_name.as_ptr()) },
        }
    }

    pub fn add_function(&self, func: ScalarFunction) -> crate::Result<()> {
        unsafe {
            let rc = duckdb_add_scalar_function_to_set(self.ptr, func.ptr);
            if rc != DuckDBSuccess {
                return Err(Error::DuckDBFailure(ffi::Error::new(rc), None));
            }
        }

        Ok(())
    }

    pub(crate) fn register_with_connection(&self, con: duckdb_connection) -> crate::Result<()> {
        unsafe {
            let rc = ffi::duckdb_register_scalar_function_set(con, self.ptr);
            if rc != ffi::DuckDBSuccess {
                return Err(Error::DuckDBFailure(ffi::Error::new(rc), None));
            }
        }
        Ok(())
    }
}

/// A function that returns a queryable scalar function
#[derive(Debug)]
pub struct ScalarFunction {
    ptr: duckdb_scalar_function,
}

impl Drop for ScalarFunction {
    fn drop(&mut self) {
        unsafe {
            duckdb_destroy_scalar_function(&mut self.ptr);
        }
    }
}

use std::ffi::{c_void, CString};

use libduckdb_sys::{
    self as ffi, duckdb_add_scalar_function_to_set, duckdb_connection, duckdb_create_scalar_function,
    duckdb_create_scalar_function_set, duckdb_data_chunk, duckdb_delete_callback_t, duckdb_destroy_scalar_function,
    duckdb_function_info, duckdb_scalar_function, duckdb_scalar_function_add_parameter, duckdb_scalar_function_set,
    duckdb_scalar_function_set_extra_info, duckdb_scalar_function_set_function, duckdb_scalar_function_set_name,
    duckdb_scalar_function_set_return_type, duckdb_scalar_function_set_varargs, duckdb_vector, DuckDBSuccess,
};

use crate::{core::LogicalTypeHandle, Error};

impl ScalarFunction {
    /// Creates a new empty scalar function.
    pub fn new(name: impl Into<String>) -> Result<Self, Error> {
        let name: String = name.into();
        let f_ptr = unsafe { duckdb_create_scalar_function() };
        let c_name = CString::new(name).expect("name should contain valid utf-8");
        unsafe { duckdb_scalar_function_set_name(f_ptr, c_name.as_ptr()) };

        Ok(Self { ptr: f_ptr })
    }

    /// Adds a parameter to the scalar function.
    ///
    /// # Arguments
    ///  * `logical_type`: The type of the parameter to add.
    pub fn add_parameter(&self, logical_type: &LogicalTypeHandle) -> &Self {
        unsafe {
            duckdb_scalar_function_add_parameter(self.ptr, logical_type.ptr);
        }
        self
    }

    pub fn add_variadic_parameter(&self, logical_type: &LogicalTypeHandle) -> &Self {
        unsafe {
            duckdb_scalar_function_set_varargs(self.ptr, logical_type.ptr);
        }
        self
    }

    /// Sets the return type of the scalar function.
    ///
    /// # Arguments
    ///  * `logical_type`: The return type of the scalar function.
    pub fn set_return_type(&self, logical_type: &LogicalTypeHandle) -> &Self {
        unsafe {
            duckdb_scalar_function_set_return_type(self.ptr, logical_type.ptr);
        }
        self
    }

    /// Sets the main function of the scalar function
    ///
    /// # Arguments
    ///  * `function`: The function
    pub fn set_function(
        &self,
        func: Option<unsafe extern "C" fn(info: duckdb_function_info, input: duckdb_data_chunk, output: duckdb_vector)>,
    ) -> &Self {
        unsafe {
            duckdb_scalar_function_set_function(self.ptr, func);
        }
        self
    }

    /// Assigns extra information to the scalar function that can be fetched during binding, etc.
    ///
    /// # Arguments
    /// * `extra_info`: The extra information
    /// * `destroy`: The callback that will be called to destroy the bind data (if any)
    ///
    /// # Safety
    unsafe fn set_extra_info_impl(&self, extra_info: *mut c_void, destroy: duckdb_delete_callback_t) {
        duckdb_scalar_function_set_extra_info(self.ptr, extra_info, destroy);
    }

    pub fn set_extra_info<T: Default>(&self) -> &ScalarFunction {
        unsafe {
            let t = Box::new(T::default());
            let c_void = Box::into_raw(t) as *mut c_void;
            self.set_extra_info_impl(c_void, Some(drop_ptr::<T>));
        }
        self
    }
}

unsafe extern "C" fn drop_ptr<T>(ptr: *mut c_void) {
    let _ = Box::from_raw(ptr as *mut T);
}

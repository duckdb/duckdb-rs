use crate::ffi::{duckdb_destroy_value, duckdb_free, duckdb_get_int64, duckdb_get_varchar, duckdb_value};
use std::{ffi::CStr, fmt, os::raw::c_void};

/// The Value object holds a single arbitrary value of any type that can be
/// stored in the database.
#[derive(Debug)]
pub struct Value {
    pub(crate) ptr: duckdb_value,
}

impl From<duckdb_value> for Value {
    fn from(ptr: duckdb_value) -> Self {
        Self { ptr }
    }
}

impl Drop for Value {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            unsafe {
                duckdb_destroy_value(&mut self.ptr);
            }
        }
        self.ptr = std::ptr::null_mut();
    }
}

impl Value {
    /// Returns the value as a int64
    pub fn to_int64(&self) -> i64 {
        unsafe { duckdb_get_int64(self.ptr) }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        unsafe {
            let varchar = duckdb_get_varchar(self.ptr);
            let c_str = CStr::from_ptr(varchar);
            let res = write!(f, "{}", c_str.to_string_lossy());
            duckdb_free(varchar as *mut c_void);
            res
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ffi::duckdb_create_varchar;
    use std::ffi::CString;

    #[test]
    fn test_value_to_string() {
        let c_str = CString::new("some value").unwrap();
        let duckdb_val = unsafe { duckdb_create_varchar(c_str.as_ptr()) };
        let val = Value::from(duckdb_val);
        assert_eq!(val.to_string(), "some value");
    }
}

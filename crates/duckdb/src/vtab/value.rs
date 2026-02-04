use crate::ffi::{duckdb_destroy_value, duckdb_free, duckdb_get_int64, duckdb_get_list_child, duckdb_get_list_size, duckdb_get_varchar, duckdb_value};
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

    /// Returns the value as a Vec<Value>
    pub fn to_vec(&self) -> Vec<Value> {
        let size = unsafe { duckdb_get_list_size(self.ptr) };
        let mut out = Vec::with_capacity(size.try_into().unwrap());
        for i in 0..size {
            let child = unsafe { duckdb_get_list_child(self.ptr, i as u64) };
            out.push(Value::from(child));
        }
        out
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
    use crate::ffi::{duckdb_value, duckdb_create_int64, duckdb_create_list_value, duckdb_create_logical_type, duckdb_create_varchar, duckdb_destroy_logical_type, duckdb_destroy_value, DUCKDB_TYPE_DUCKDB_TYPE_BIGINT};
    use std::ffi::CString;

    #[test]
    fn test_value_to_string() {
        let c_str = CString::new("some value").unwrap();
        let duckdb_val = unsafe { duckdb_create_varchar(c_str.as_ptr()) };
        let val = Value::from(duckdb_val);
        assert_eq!(val.to_string(), "some value");
    }

    #[test]
    fn test_value_to_vec() {
        let list_items: Vec<i64> = vec![1, -200,2381292];
        let val = unsafe {
            // Create a duckdb list value
            let mut logical_type = duckdb_create_logical_type(DUCKDB_TYPE_DUCKDB_TYPE_BIGINT);
            let values: Vec<duckdb_value> = list_items.iter().map(|v| duckdb_create_int64(*v)).collect();
            let duckdb_val = duckdb_create_list_value(
                logical_type,
                values.as_ptr().cast_mut(),
                values.len() as u64
            );

            // Clean up temporary resources
            duckdb_destroy_logical_type(&mut logical_type);
            for mut v in values {
                duckdb_destroy_value(&mut v);
            }

            Value::from(duckdb_val)
        };

        let list = val.to_vec();
        assert_eq!(list.len(), list_items.len());
        assert_eq!(list.iter().map(|v| v.to_int64()).collect::<Vec<i64>>(), list_items);

    }
}

use crate::ffi::{duckdb_destroy_value, duckdb_get_int64, duckdb_get_varchar, duckdb_value};
use libduckdb_sys::{
    duckdb_create_bool, duckdb_create_int32, duckdb_create_int64, duckdb_create_null_value, duckdb_create_uint64,
};
use std::{ffi::CString, fmt};

/// The Value object holds a single arbitrary value of any type that can be
/// stored in the database.
#[derive(Debug)]
pub struct Value {
    pub(crate) ptr: duckdb_value,
}

impl<T> From<Option<T>> for Value
where
    T: Into<Value>,
{
    fn from(t: Option<T>) -> Self {
        match t {
            Some(t) => t.into(),
            None => Value::null(),
        }
    }
}

impl From<bool> for Value {
    fn from(value: bool) -> Self {
        Value {
            ptr: unsafe { duckdb_create_bool(value) },
        }
    }
}

impl Value {
    pub fn null() -> Value {
        Value {
            ptr: unsafe { duckdb_create_null_value() },
        }
    }
}

impl From<duckdb_value> for Value {
    fn from(ptr: duckdb_value) -> Self {
        Self { ptr }
    }
}

impl From<i32> for Value {
    fn from(t: i32) -> Self {
        Self {
            ptr: unsafe { duckdb_create_int32(t) },
        }
    }
}

impl From<i64> for Value {
    fn from(t: i64) -> Self {
        Self {
            ptr: unsafe { duckdb_create_int64(t) },
        }
    }
}

impl From<u64> for Value {
    fn from(t: u64) -> Self {
        Self {
            ptr: unsafe { duckdb_create_uint64(t) },
        }
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
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let c_string = unsafe { CString::from_raw(duckdb_get_varchar(self.ptr)) };
        write!(f, "{}", c_string.to_str().unwrap())
    }
}

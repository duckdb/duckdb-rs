use crate::ffi::{duckdb_destroy_value, duckdb_get_int64, duckdb_get_varchar, duckdb_value};
use libduckdb_sys::{
    duckdb_create_bool, duckdb_create_double, duckdb_create_float, duckdb_create_int16, duckdb_create_int32,
    duckdb_create_int64, duckdb_create_int8, duckdb_create_null_value, duckdb_create_uint16, duckdb_create_uint32,
    duckdb_create_uint64, duckdb_create_uint8,
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

macro_rules! impl_duckdb_create_value {
    ($ty:ty, $ddb_fn:ident) => {
        impl From<$ty> for Value {
            fn from(value: $ty) -> Self {
                Value {
                    ptr: unsafe { $ddb_fn(value) },
                }
            }
        }
    };
}

impl_duckdb_create_value!(bool, duckdb_create_bool);
impl_duckdb_create_value!(i8, duckdb_create_int8);
impl_duckdb_create_value!(i16, duckdb_create_int16);
impl_duckdb_create_value!(i32, duckdb_create_int32);
impl_duckdb_create_value!(i64, duckdb_create_int64);
impl_duckdb_create_value!(u8, duckdb_create_uint8);
impl_duckdb_create_value!(u16, duckdb_create_uint16);
impl_duckdb_create_value!(u32, duckdb_create_uint32);
impl_duckdb_create_value!(u64, duckdb_create_uint64);
impl_duckdb_create_value!(f32, duckdb_create_float);
impl_duckdb_create_value!(f64, duckdb_create_double);

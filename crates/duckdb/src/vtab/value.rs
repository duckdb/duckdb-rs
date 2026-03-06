use crate::core::LogicalTypeId;
use crate::ffi::{
    duckdb_destroy_value, duckdb_free, duckdb_get_bool, duckdb_get_double, duckdb_get_float, duckdb_get_int16,
    duckdb_get_int32, duckdb_get_int64, duckdb_get_int8, duckdb_get_list_child, duckdb_get_list_size,
    duckdb_get_type_id, duckdb_get_uint16, duckdb_get_uint32, duckdb_get_uint64, duckdb_get_uint8,
    duckdb_get_value_type, duckdb_get_varchar, duckdb_is_null_value, duckdb_value,
};
use std::{ffi::CStr, fmt, os::raw::c_void};

/// The Value object holds a single arbitrary value of any type that can be
/// stored in the database.
#[derive(Debug)]
pub struct Value {
    pub(crate) ptr: duckdb_value,
}

macro_rules! primitive_getters {
    ($($name:ident: $rust_type:ty => $ffi_func:ident),* $(,)?) => {
        $(
            pub fn $name(&self) -> $rust_type {
                unsafe { $ffi_func(self.ptr) }
            }
        )*
    };
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
    // Returns the value as a Rust type
    primitive_getters!(
        to_bool: bool => duckdb_get_bool,
        to_int8: i8 => duckdb_get_int8,
        to_uint8: u8 => duckdb_get_uint8,
        to_int16: i16 => duckdb_get_int16,
        to_uint16: u16 => duckdb_get_uint16,
        to_int32: i32 => duckdb_get_int32,
        to_uint32: u32 => duckdb_get_uint32,
        to_int64: i64 => duckdb_get_int64,
        to_uint64: u64 => duckdb_get_uint64,
        to_float: f32 => duckdb_get_float,
        to_double: f64 => duckdb_get_double,
    );

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

    pub fn is_null(&self) -> bool {
        unsafe { duckdb_is_null_value(self.ptr) }
    }

    pub fn logical_type_id(&self) -> LogicalTypeId {
        unsafe {
            let logical_type = duckdb_get_value_type(self.ptr);
            duckdb_get_type_id(logical_type).into()
        }
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
    use crate::core::{LogicalTypeHandle, LogicalTypeId};
    use crate::ffi::{
        duckdb_create_int64, duckdb_create_list_value, duckdb_create_varchar, duckdb_destroy_value, duckdb_value,
    };
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
        let list_items: Vec<i64> = vec![1, -200, 2381292];
        let val = unsafe {
            let logical_type = LogicalTypeHandle::from(LogicalTypeId::Bigint);
            let values: Vec<duckdb_value> = list_items.iter().map(|v| duckdb_create_int64(*v)).collect();
            let duckdb_val =
                duckdb_create_list_value(logical_type.ptr, values.as_ptr().cast_mut(), values.len() as u64);

            for mut v in values {
                duckdb_destroy_value(&mut v);
            }

            Value::from(duckdb_val)
        };

        let list = val.to_vec();
        assert_eq!(list.len(), list_items.len());
        assert_eq!(list.iter().map(|v| v.to_int64()).collect::<Vec<i64>>(), list_items);
    }

    #[test]
    fn test_value_primitive_getters() {
        use crate::ffi::{
            duckdb_create_bool, duckdb_create_date, duckdb_create_double, duckdb_create_float, duckdb_create_int16,
            duckdb_create_int32, duckdb_create_int64, duckdb_create_int8, duckdb_create_uint16, duckdb_create_uint32,
            duckdb_create_uint64, duckdb_create_uint8,
        };

        unsafe {
            // Test bool
            let bool_val = Value::from(duckdb_create_bool(true));
            assert!(bool_val.to_bool());

            // Test int8
            let i8_val = Value::from(duckdb_create_int8(-42));
            assert_eq!(i8_val.to_int8(), -42);

            // Test uint8
            let u8_val = Value::from(duckdb_create_uint8(255));
            assert_eq!(u8_val.to_uint8(), 255);

            // Test int16
            let i16_val = Value::from(duckdb_create_int16(-1000));
            assert_eq!(i16_val.to_int16(), -1000);

            // Test uint16
            let u16_val = Value::from(duckdb_create_uint16(50000));
            assert_eq!(u16_val.to_uint16(), 50000);

            // Test int32
            let i32_val = Value::from(duckdb_create_int32(-200000));
            assert_eq!(i32_val.to_int32(), -200000);

            // Test uint32
            let u32_val = Value::from(duckdb_create_uint32(4000000000));
            assert_eq!(u32_val.to_uint32(), 4000000000);

            // Test int64
            let i64_val = Value::from(duckdb_create_int64(-9000000000000000000));
            assert_eq!(i64_val.to_int64(), -9000000000000000000);

            // Test uint64
            let u64_val = Value::from(duckdb_create_uint64(18000000000000000000));
            assert_eq!(u64_val.to_uint64(), 18000000000000000000);

            // Test float
            let float_val = Value::from(duckdb_create_float(3.14f32));
            assert_eq!(float_val.to_float(), 3.14);

            // Test double
            let double_val = Value::from(duckdb_create_double(2.71828));
            assert_eq!(double_val.to_double(), 2.71828);
        }
    }

    #[test]
    fn test_value_is_null() {
        use crate::ffi::duckdb_create_null_value;

        unsafe {
            let null_val = Value::from(duckdb_create_null_value());
            assert!(null_val.is_null());
        }
    }
}

use crate::core::LogicalTypeId;
use crate::ffi::{
    DuckDbString, duckdb_destroy_value, duckdb_get_bool, duckdb_get_double, duckdb_get_float, duckdb_get_int8,
    duckdb_get_int16, duckdb_get_int32, duckdb_get_int64, duckdb_get_list_child, duckdb_get_list_size,
    duckdb_get_type_id, duckdb_get_uint8, duckdb_get_uint16, duckdb_get_uint32, duckdb_get_uint64,
    duckdb_get_value_type, duckdb_get_varchar, duckdb_is_null_value, duckdb_value,
};
use std::fmt;

/// The Value object holds a single arbitrary value of any type that can be
/// stored in the database.
#[derive(Debug)]
pub struct Value {
    pub(crate) ptr: duckdb_value,
}

macro_rules! primitive_getters {
    ($($name:ident: $rust_type:ty => $ffi_func:ident),* $(,)?) => {
        $(
            #[doc = concat!("Returns the value converted to `", stringify!($rust_type), "` using DuckDB's C API.")]
            ///
            /// This method does not check the value's logical type before converting.
            /// On conversion failure, DuckDB returns fallback values: `false` for `bool`,
            /// `T::MIN` for signed integers, `0` for unsigned integers, and `NaN` for
            /// floating-point values. These are indistinguishable from successful
            /// conversions to the same Rust value.
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

    /// Returns the value as a list of [`Value`]s.
    ///
    /// Returns `None` when the value is not a DuckDB `LIST`, or when the value is SQL `NULL`.
    pub fn to_list(&self) -> Option<Vec<Value>> {
        if self.is_null() || self.logical_type_id() != LogicalTypeId::List {
            return None;
        }

        let size = unsafe { duckdb_get_list_size(self.ptr) };
        let mut out = Vec::with_capacity(usize::try_from(size).ok()?);
        for i in 0..size {
            let child = unsafe { duckdb_get_list_child(self.ptr, i) };
            if child.is_null() {
                return None;
            }
            out.push(Value::from(child));
        }
        Some(out)
    }

    /// Returns whether the value is SQL `NULL`.
    pub fn is_null(&self) -> bool {
        unsafe { duckdb_is_null_value(self.ptr) }
    }

    /// Returns the DuckDB logical type id for this value.
    pub fn logical_type_id(&self) -> LogicalTypeId {
        unsafe {
            // Borrowed from DuckDB; this type must not be destroyed.
            let logical_type = duckdb_get_value_type(self.ptr);
            duckdb_get_type_id(logical_type).into()
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        unsafe {
            let varchar = DuckDbString::from_ptr(duckdb_get_varchar(self.ptr));
            write!(f, "{}", varchar.to_string_lossy())
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
    fn test_value_to_list() {
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

        let list = val.to_list().unwrap();
        assert_eq!(list.len(), list_items.len());
        assert_eq!(list.iter().map(|v| v.to_int64()).collect::<Vec<i64>>(), list_items);
    }

    #[test]
    fn test_value_to_list_returns_empty_vec_for_empty_list() {
        let val = unsafe {
            let logical_type = LogicalTypeHandle::from(LogicalTypeId::Bigint);
            let values: Vec<duckdb_value> = Vec::new();
            let duckdb_val = duckdb_create_list_value(logical_type.ptr, values.as_ptr().cast_mut(), 0);
            assert!(!duckdb_val.is_null());

            Value::from(duckdb_val)
        };

        let list = val.to_list().unwrap();
        assert!(list.is_empty());
    }

    #[test]
    fn test_value_to_list_returns_none_for_non_list() {
        let val = unsafe { Value::from(duckdb_create_int64(42)) };
        assert!(val.to_list().is_none());
    }

    #[test]
    fn test_value_primitive_getters() {
        use crate::ffi::{
            duckdb_create_bool, duckdb_create_double, duckdb_create_float, duckdb_create_int8, duckdb_create_int16,
            duckdb_create_int32, duckdb_create_int64, duckdb_create_uint8, duckdb_create_uint16, duckdb_create_uint32,
            duckdb_create_uint64,
        };

        unsafe {
            let bool_val = Value::from(duckdb_create_bool(true));
            assert!(bool_val.to_bool());

            let i8_val = Value::from(duckdb_create_int8(-42));
            assert_eq!(i8_val.to_int8(), -42);

            let u8_val = Value::from(duckdb_create_uint8(255));
            assert_eq!(u8_val.to_uint8(), 255);

            let i16_val = Value::from(duckdb_create_int16(-1000));
            assert_eq!(i16_val.to_int16(), -1000);

            let u16_val = Value::from(duckdb_create_uint16(50000));
            assert_eq!(u16_val.to_uint16(), 50000);

            let i32_val = Value::from(duckdb_create_int32(-200000));
            assert_eq!(i32_val.to_int32(), -200000);

            let u32_val = Value::from(duckdb_create_uint32(4000000000));
            assert_eq!(u32_val.to_uint32(), 4000000000);

            let i64_val = Value::from(duckdb_create_int64(-9000000000000000000));
            assert_eq!(i64_val.to_int64(), -9000000000000000000);

            let u64_val = Value::from(duckdb_create_uint64(18000000000000000000));
            assert_eq!(u64_val.to_uint64(), 18000000000000000000);

            let float_val = Value::from(duckdb_create_float(1.25f32));
            assert_eq!(float_val.to_float(), 1.25);

            let double_val = Value::from(duckdb_create_double(-4.5));
            assert_eq!(double_val.to_double(), -4.5);
        }
    }

    #[test]
    fn test_value_is_null() {
        use crate::ffi::duckdb_create_null_value;

        unsafe {
            let null_val = Value::from(duckdb_create_null_value());
            assert!(null_val.is_null());
            assert!(null_val.to_list().is_none());

            let non_null_val = Value::from(duckdb_create_int64(42));
            assert!(!non_null_val.is_null());
        }
    }

    #[test]
    fn test_value_logical_type_id() {
        let val = unsafe { Value::from(duckdb_create_int64(42)) };
        assert_eq!(val.logical_type_id(), LogicalTypeId::Bigint);
    }
}

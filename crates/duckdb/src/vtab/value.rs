use libduckdb_sys::{
    duckdb_bit, duckdb_blob, duckdb_date, duckdb_decimal, duckdb_destroy_value, duckdb_free, duckdb_get_bit,
    duckdb_get_blob, duckdb_get_bool, duckdb_get_date, duckdb_get_decimal, duckdb_get_double, duckdb_get_float,
    duckdb_get_hugeint, duckdb_get_int16, duckdb_get_int32, duckdb_get_int64, duckdb_get_int8, duckdb_get_interval,
    duckdb_get_list_child, duckdb_get_list_size, duckdb_get_map_key, duckdb_get_map_size, duckdb_get_map_value,
    duckdb_get_struct_child, duckdb_get_time, duckdb_get_time_tz, duckdb_get_timestamp, duckdb_get_timestamp_ms,
    duckdb_get_timestamp_ns, duckdb_get_timestamp_s, duckdb_get_timestamp_tz, duckdb_get_uhugeint, duckdb_get_uint16,
    duckdb_get_uint32, duckdb_get_uint64, duckdb_get_uint8, duckdb_get_uuid, duckdb_get_value_type, duckdb_get_varchar,
    duckdb_get_varint, duckdb_hugeint, duckdb_interval, duckdb_logical_type, duckdb_struct_type_child_count,
    duckdb_struct_type_child_name, duckdb_time, duckdb_time_tz, duckdb_timestamp, duckdb_timestamp_ms,
    duckdb_timestamp_ns, duckdb_timestamp_s, duckdb_uhugeint, duckdb_value, duckdb_varint,
};
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
    /// Returns the value as a boolean
    pub fn to_bool(&self) -> bool {
        unsafe { duckdb_get_bool(self.ptr) }
    }

    /// Returns the value as an i8
    pub fn to_int8(&self) -> i8 {
        unsafe { duckdb_get_int8(self.ptr) }
    }

    /// Returns the value as an u8
    pub fn to_uint8(&self) -> u8 {
        unsafe { duckdb_get_uint8(self.ptr) }
    }

    /// Returns the value as an i16
    pub fn to_int16(&self) -> i16 {
        unsafe { duckdb_get_int16(self.ptr) }
    }

    /// Returns the value as an u16
    pub fn to_uint16(&self) -> u16 {
        unsafe { duckdb_get_uint16(self.ptr) }
    }

    /// Returns the value as an i32
    pub fn to_int32(&self) -> i32 {
        unsafe { duckdb_get_int32(self.ptr) }
    }

    /// Returns the value as an u32
    pub fn to_uint32(&self) -> u32 {
        unsafe { duckdb_get_uint32(self.ptr) }
    }

    /// Returns the value as a int64
    pub fn to_int64(&self) -> i64 {
        unsafe { duckdb_get_int64(self.ptr) }
    }

    /// Returns the value as a int64
    pub fn to_uint64(&self) -> u64 {
        unsafe { duckdb_get_uint64(self.ptr) }
    }
    /// Returns the value as a hugeint
    pub fn to_hugeint(&self) -> duckdb_hugeint {
        unsafe { duckdb_get_hugeint(self.ptr) }
    }

    /// Returns the value as a uhugeint
    pub fn to_uhugeint(&self) -> duckdb_uhugeint {
        unsafe { duckdb_get_uhugeint(self.ptr) }
    }

    /// Returns the value as a varint
    pub fn to_varint(&self) -> duckdb_varint {
        unsafe { duckdb_get_varint(self.ptr) }
    }

    /// Returns the value as a decimal
    pub fn to_decimal(&self) -> duckdb_decimal {
        unsafe { duckdb_get_decimal(self.ptr) }
    }

    /// Returns the value as a float
    pub fn to_float(&self) -> f32 {
        unsafe { duckdb_get_float(self.ptr) }
    }

    /// Returns the value as a double
    pub fn to_double(&self) -> f64 {
        unsafe { duckdb_get_double(self.ptr) }
    }

    /// Returns the value as a date
    pub fn to_date(&self) -> duckdb_date {
        unsafe { duckdb_get_date(self.ptr) }
    }

    /// Returns the value as a time
    pub fn to_time(&self) -> duckdb_time {
        unsafe { duckdb_get_time(self.ptr) }
    }

    /// Returns the value as a time_tz
    pub fn to_time_tz(&self) -> duckdb_time_tz {
        unsafe { duckdb_get_time_tz(self.ptr) }
    }

    /// Returns the value as a timestamp
    pub fn to_timestamp(&self) -> duckdb_timestamp {
        unsafe { duckdb_get_timestamp(self.ptr) }
    }

    /// Returns the value as a timestamp_tz
    pub fn to_timestamp_tz(&self) -> duckdb_timestamp {
        unsafe { duckdb_get_timestamp_tz(self.ptr) }
    }

    /// Returns the value as a timestamp_s
    pub fn to_timestamp_s(&self) -> duckdb_timestamp_s {
        unsafe { duckdb_get_timestamp_s(self.ptr) }
    }

    /// Returns the value as a timestamp_ms
    pub fn to_timestamp_ms(&self) -> duckdb_timestamp_ms {
        unsafe { duckdb_get_timestamp_ms(self.ptr) }
    }

    /// Returns the value as a timestamp_ns
    pub fn to_timestamp_ns(&self) -> duckdb_timestamp_ns {
        unsafe { duckdb_get_timestamp_ns(self.ptr) }
    }

    /// Returns the value as a interval
    pub fn to_interval(&self) -> duckdb_interval {
        unsafe { duckdb_get_interval(self.ptr) }
    }

    /// Returns the value as a blob
    pub fn to_blob(&self) -> duckdb_blob {
        unsafe { duckdb_get_blob(self.ptr) }
    }

    /// Returns the value as a bit
    pub fn to_bit(&self) -> duckdb_bit {
        unsafe { duckdb_get_bit(self.ptr) }
    }

    /// Returns the value as a uuid
    pub fn to_uuid(&self) -> duckdb_uhugeint {
        unsafe { duckdb_get_uuid(self.ptr) }
    }

    /// Returns the value as a String
    pub fn to_varchar(&self) -> String {
        unsafe {
            let varchar = duckdb_get_varchar(self.ptr);
            let c_str = CStr::from_ptr(varchar);
            c_str.to_string_lossy().into_owned()
        }
    }

    /// Returns the value as a list
    pub fn to_list(&self) -> Vec<Self> {
        unsafe {
            let size = duckdb_get_list_size(self.ptr);
            (0..size)
                .map(|index| Value::from(duckdb_get_list_child(self.ptr, index)))
                .collect()
        }
    }

    /// Returns the value as a map key & value entries
    pub fn to_map_entries(&self) -> Vec<(Self, Self)> {
        unsafe {
            let size = duckdb_get_map_size(self.ptr);
            (0..size)
                .map(|index| (
                    Value::from(duckdb_get_map_key(self.ptr, index)),
                    Value::from(duckdb_get_map_value(self.ptr, index)),
                ))
                .collect()
        }
    }

    /// Returns the value as a struct type child names and values
    pub fn to_struct_properties(&self) -> Vec<(String, Self)> {
        unsafe {
            let value_type = self.value_type();
            let size = duckdb_struct_type_child_count(value_type);
            (0..size)
                .map(|index| (
                    CStr::from_ptr(duckdb_struct_type_child_name(value_type, index)).to_string_lossy().to_string(),
                    Value::from(duckdb_get_struct_child(self.ptr, index)),
                ))
                .collect()
        }
    }

    /// Returns the value logical type
    pub fn value_type(&self) -> duckdb_logical_type {
        unsafe { duckdb_get_value_type(self.ptr) }
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

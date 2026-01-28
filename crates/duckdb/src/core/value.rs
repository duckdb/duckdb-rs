use crate::{
    ffi::*,
    types::{ListType, ValueRef},
};

/// A wrapper around a DuckDB value handle.
#[derive(Debug)]
#[repr(C)]
pub struct ValueHandle {
    pub(crate) ptr: duckdb_value,
    // Do not add members so that array of this type can be used in FFI.
}

impl Drop for ValueHandle {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            unsafe {
                duckdb_destroy_value(&mut self.ptr);
            }
        }
        self.ptr = std::ptr::null_mut();
    }
}

impl<'a> From<ValueRef<'a>> for ValueHandle {
    fn from(value_ref: ValueRef<'a>) -> Self {
        let ptr = match value_ref {
            ValueRef::Null => unsafe { duckdb_create_null_value() },
            ValueRef::Boolean(v) => unsafe { duckdb_create_bool(v) },
            ValueRef::TinyInt(v) => unsafe { duckdb_create_int8(v) },
            ValueRef::SmallInt(v) => unsafe { duckdb_create_int16(v) },
            ValueRef::Int(v) => unsafe { duckdb_create_int32(v) },
            ValueRef::BigInt(v) => unsafe { duckdb_create_int64(v) },
            ValueRef::HugeInt(v) => {
                let lower = v.cast_unsigned() as u64;
                let upper = (v >> 64) as i64;
                unsafe { duckdb_create_hugeint(duckdb_hugeint { lower, upper }) }
            }
            ValueRef::UTinyInt(v) => unsafe { duckdb_create_uint8(v) },
            ValueRef::USmallInt(v) => unsafe { duckdb_create_uint16(v) },
            ValueRef::UInt(v) => unsafe { duckdb_create_uint32(v) },
            ValueRef::UBigInt(v) => unsafe { duckdb_create_uint64(v) },
            ValueRef::Float(v) => unsafe { duckdb_create_float(v) },
            ValueRef::Double(v) => unsafe { duckdb_create_double(v) },
            ValueRef::Text(v) => unsafe { duckdb_create_varchar_length(v.as_ptr() as *const i8, v.len() as u64) },
            ValueRef::Blob(v) => unsafe { duckdb_create_blob(v.as_ptr() as *const u8, v.len() as u64) },
            ValueRef::List(ListType::Native(arr)) => {
                let logical_type = value_ref
                    .data_type()
                    .inner_type()
                    .expect("List type doesn't have an inner type")
                    .logical_type_handle();
                // Underlying DuckDB API isn't marked const unfortunately, so we have to use a mutable pointer.
                let mut values = arr
                    .iter()
                    .map(ValueRef::from)
                    .map(ValueHandle::from)
                    .collect::<Vec<_>>();
                let value_count = arr.len() as u64;
                unsafe {
                    duckdb_create_list_value(
                        logical_type.ptr,
                        values[..].as_mut_ptr() as *mut duckdb_value,
                        value_count,
                    )
                }
            }
            ValueRef::Timestamp(..)
            | ValueRef::List(..)
            | ValueRef::Date32(..)
            | ValueRef::Time64(..)
            | ValueRef::Interval { .. }
            | ValueRef::Decimal(..)
            | ValueRef::Enum(..)
            | ValueRef::Struct(..)
            | ValueRef::Array(..)
            | ValueRef::Map(..)
            | ValueRef::Union(..) => unimplemented!("Not implemented for {:?}", value_ref),
        };
        assert!(!ptr.is_null(), "Failed to create DuckDB value for {:?}", value_ref);
        Self { ptr }
    }
}

//! Recursive conversion of [`Value`] into a `duckdb_value` handle.
//!
//! Container variants ([`Value::List`], [`Value::Array`], [`Value::Struct`],
//! [`Value::Map`]) cannot be represented as a [`ValueRef`](crate::types::ValueRef)
//! built from an owned [`Value`] because that path has no Arrow backing array.
//! Instead the bind/append sites route those values through [`FfiValue`], which
//! builds a `duckdb_value` recursively via DuckDB's C value API
//! (`duckdb_create_list_value`, `duckdb_create_array_value`,
//! `duckdb_create_struct_value`, `duckdb_create_map_value`).
//!
//! Ownership: the `duckdb_create_*_value` functions *copy* the child values
//! (verified in DuckDB's `duckdb_value-c.cpp`: `unwrapped_values.push_back(value)`
//! invokes the copy constructor). [`FfiValueArray`] therefore owns and destroys
//! every child it builds, even after a successful `create_*_value` call.

use std::ffi::CString;

use crate::{
    Error, Result,
    core::{LogicalTypeHandle, LogicalTypeId},
    ffi,
    types::{OrderedMap, TimeUnit, Value, to_duckdb_decimal, to_duckdb_hugeint, to_duckdb_uhugeint},
};

/// RAII guard owning a `duckdb_value`. Calls `duckdb_destroy_value` on drop.
#[derive(Debug)]
pub(crate) struct FfiValue {
    ptr: ffi::duckdb_value,
}

impl FfiValue {
    /// Recursively builds a `duckdb_value` from a Rust [`Value`], including
    /// nested containers (`List`, `Array`, `Struct`, `Map`).
    pub(crate) fn from_value(value: &Value) -> Result<Self> {
        let ptr = build(value)?;
        if ptr.is_null() {
            return Err(Error::ToSqlConversionFailure(
                format!("duckdb value construction failed for {}", value.data_type()).into(),
            ));
        }
        Ok(Self { ptr })
    }

    /// Returns the raw `duckdb_value` pointer. The guard still owns it and
    /// destroys it on drop, so callers must not free it themselves.
    pub(crate) fn ptr(&self) -> ffi::duckdb_value {
        self.ptr
    }
}

impl Drop for FfiValue {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            unsafe { ffi::duckdb_destroy_value(&mut self.ptr) };
        }
    }
}

/// Returns `true` when `value` is a container that must be routed through
/// [`FfiValue::from_value`] instead of the [`ValueRef`](crate::types::ValueRef)
/// bind/append path.
pub(crate) fn is_bindable_container(value: &Value) -> bool {
    matches!(
        value,
        Value::List(_) | Value::Array(_) | Value::Struct(_) | Value::Map(_)
    )
}

/// Owns a flat array of `duckdb_value` children and destroys them on drop.
struct FfiValueArray {
    values: Vec<ffi::duckdb_value>,
}

impl FfiValueArray {
    /// Builds a flat child array from `items`, recursing via [`build`]. On
    /// error, any children already constructed are destroyed.
    fn build<'a>(items: impl IntoIterator<Item = &'a Value>) -> Result<Self> {
        let mut values = Vec::new();
        for item in items {
            let ptr = build(item)?;
            if ptr.is_null() {
                for built in &mut values {
                    unsafe { ffi::duckdb_destroy_value(built) };
                }
                return Err(Error::ToSqlConversionFailure(
                    format!("failed to build child value of type {}", item.data_type()).into(),
                ));
            }
            values.push(ptr);
        }
        Ok(Self { values })
    }

    fn as_mut_ptr(&mut self) -> *mut ffi::duckdb_value {
        self.values.as_mut_ptr()
    }

    fn len(&self) -> ffi::idx_t {
        self.values.len() as ffi::idx_t
    }
}

impl Drop for FfiValueArray {
    fn drop(&mut self) {
        for value in &mut self.values {
            if !value.is_null() {
                unsafe { ffi::duckdb_destroy_value(value) };
            }
        }
    }
}

fn build(value: &Value) -> Result<ffi::duckdb_value> {
    let ptr = match value {
        Value::Null => unsafe { ffi::duckdb_create_null_value() },
        Value::Boolean(b) => unsafe { ffi::duckdb_create_bool(*b) },
        Value::TinyInt(i) => unsafe { ffi::duckdb_create_int8(*i) },
        Value::SmallInt(i) => unsafe { ffi::duckdb_create_int16(*i) },
        Value::Int(i) => unsafe { ffi::duckdb_create_int32(*i) },
        Value::BigInt(i) => unsafe { ffi::duckdb_create_int64(*i) },
        Value::HugeInt(i) => unsafe { ffi::duckdb_create_hugeint(to_duckdb_hugeint(*i)) },
        Value::UHugeInt(i) => unsafe { ffi::duckdb_create_uhugeint(to_duckdb_uhugeint(*i)) },
        Value::UTinyInt(i) => unsafe { ffi::duckdb_create_uint8(*i) },
        Value::USmallInt(i) => unsafe { ffi::duckdb_create_uint16(*i) },
        Value::UInt(i) => unsafe { ffi::duckdb_create_uint32(*i) },
        Value::UBigInt(i) => unsafe { ffi::duckdb_create_uint64(*i) },
        Value::Float(f) => unsafe { ffi::duckdb_create_float(*f) },
        Value::Double(d) => unsafe { ffi::duckdb_create_double(*d) },
        Value::Decimal(d) => unsafe { ffi::duckdb_create_decimal(to_duckdb_decimal(*d)) },
        Value::Text(s) => {
            let c = CString::new(s.as_str()).map_err(|e| Error::ToSqlConversionFailure(e.into()))?;
            unsafe { ffi::duckdb_create_varchar_length(c.as_ptr(), s.len() as ffi::idx_t) }
        }
        Value::Blob(b) | Value::Geometry(b) => unsafe { ffi::duckdb_create_blob(b.as_ptr(), b.len() as ffi::idx_t) },
        Value::Timestamp(unit, i) => unsafe {
            ffi::duckdb_create_timestamp(ffi::duckdb_timestamp {
                micros: unit.to_micros(*i),
            })
        },
        Value::Date32(days) => unsafe { ffi::duckdb_create_date(ffi::duckdb_date { days: *days }) },
        Value::Time64(unit, i) => unsafe {
            ffi::duckdb_create_time(ffi::duckdb_time {
                micros: unit.to_micros(*i),
            })
        },
        Value::Interval { months, days, nanos } => unsafe {
            ffi::duckdb_create_interval(ffi::duckdb_interval {
                months: *months,
                days: *days,
                micros: *nanos / 1_000,
            })
        },
        Value::List(items) => return build_list(items),
        Value::Array(items) => return build_array(items),
        Value::Struct(fields) => return build_struct(fields),
        Value::Map(entries) => return build_map(entries),
        Value::Enum(_) | Value::Union(_) => {
            return Err(Error::ToSqlConversionFailure(
                format!(
                    "binding {} parameters is not supported via the duckdb_value API",
                    variant_name(value)
                )
                .into(),
            ));
        }
    };
    Ok(ptr)
}

fn build_list(items: &[Value]) -> Result<ffi::duckdb_value> {
    let child_type = homogeneous_child_logical_type(items, "list")?;
    let mut children = FfiValueArray::build(items.iter())?;
    let ptr = unsafe { ffi::duckdb_create_list_value(child_type.ptr, children.as_mut_ptr(), children.len()) };
    Ok(ptr)
}

fn build_array(items: &[Value]) -> Result<ffi::duckdb_value> {
    let child_type = homogeneous_child_logical_type(items, "array")?;
    let mut children = FfiValueArray::build(items.iter())?;
    let ptr = unsafe { ffi::duckdb_create_array_value(child_type.ptr, children.as_mut_ptr(), children.len()) };
    Ok(ptr)
}

fn build_struct(fields: &OrderedMap<String, Value>) -> Result<ffi::duckdb_value> {
    let field_types: Vec<(&str, LogicalTypeHandle)> = fields
        .iter()
        .map(|(k, v)| Ok((k.as_str(), value_logical_type(v)?)))
        .collect::<Result<_>>()?;
    let struct_type = LogicalTypeHandle::struct_type(&field_types);
    let mut children = FfiValueArray::build(fields.values())?;
    let ptr = unsafe { ffi::duckdb_create_struct_value(struct_type.ptr, children.as_mut_ptr()) };
    Ok(ptr)
}

fn build_map(entries: &OrderedMap<Value, Value>) -> Result<ffi::duckdb_value> {
    let (key_type, value_type) = map_key_value_types(entries)?;
    let map_type = LogicalTypeHandle::map(&key_type, &value_type);
    let mut keys = FfiValueArray::build(entries.keys())?;
    let mut values = FfiValueArray::build(entries.values())?;
    let ptr = unsafe {
        ffi::duckdb_create_map_value(
            map_type.ptr,
            keys.as_mut_ptr(),
            values.as_mut_ptr(),
            entries.len() as ffi::idx_t,
        )
    };
    Ok(ptr)
}

/// Derives the element logical type for a homogeneous container. Returns a
/// clear error for empty containers, where the element type cannot be inferred
/// from the value alone.
fn homogeneous_child_logical_type(items: &[Value], kind: &str) -> Result<LogicalTypeHandle> {
    let first = items.first().ok_or_else(|| {
        Error::ToSqlConversionFailure(
            format!(
                "cannot infer element type for an empty {kind}; bind at least one element or \
                 cast the parameter in SQL"
            )
            .into(),
        )
    })?;
    let child_type = value_logical_type(first)?;
    let child_category = first.data_type();
    for item in &items[1..] {
        if item.data_type() != child_category {
            return Err(Error::ToSqlConversionFailure(
                format!("heterogeneous {kind} elements are not supported").into(),
            ));
        }
    }
    Ok(child_type)
}

fn map_key_value_types(entries: &OrderedMap<Value, Value>) -> Result<(LogicalTypeHandle, LogicalTypeHandle)> {
    let (key, value) = entries.iter().next().ok_or_else(|| {
        Error::ToSqlConversionFailure(
            "cannot infer key/value types for an empty map; bind at least one entry or cast in SQL".into(),
        )
    })?;
    Ok((value_logical_type(key)?, value_logical_type(value)?))
}

/// Builds the [`LogicalTypeHandle`] that matches a [`Value`], preserving full
/// type fidelity (e.g. decimal width/scale and nested container shapes).
fn value_logical_type(value: &Value) -> Result<LogicalTypeHandle> {
    Ok(match value {
        Value::Null => LogicalTypeHandle::from(LogicalTypeId::SqlNull),
        Value::Boolean(_) => LogicalTypeHandle::from(LogicalTypeId::Boolean),
        Value::TinyInt(_) => LogicalTypeHandle::from(LogicalTypeId::Tinyint),
        Value::SmallInt(_) => LogicalTypeHandle::from(LogicalTypeId::Smallint),
        Value::Int(_) => LogicalTypeHandle::from(LogicalTypeId::Integer),
        Value::BigInt(_) => LogicalTypeHandle::from(LogicalTypeId::Bigint),
        Value::HugeInt(_) => LogicalTypeHandle::from(LogicalTypeId::Hugeint),
        Value::UHugeInt(_) => LogicalTypeHandle::from(LogicalTypeId::UHugeint),
        Value::UTinyInt(_) => LogicalTypeHandle::from(LogicalTypeId::UTinyint),
        Value::USmallInt(_) => LogicalTypeHandle::from(LogicalTypeId::USmallint),
        Value::UInt(_) => LogicalTypeHandle::from(LogicalTypeId::UInteger),
        Value::UBigInt(_) => LogicalTypeHandle::from(LogicalTypeId::UBigint),
        Value::Float(_) => LogicalTypeHandle::from(LogicalTypeId::Float),
        Value::Double(_) => LogicalTypeHandle::from(LogicalTypeId::Double),
        Value::Decimal(d) => LogicalTypeHandle::decimal(d.width(), d.scale()),
        Value::Text(_) => LogicalTypeHandle::from(LogicalTypeId::Varchar),
        Value::Blob(_) | Value::Geometry(_) => LogicalTypeHandle::from(LogicalTypeId::Blob),
        Value::Timestamp(unit, _) => LogicalTypeHandle::from(timestamp_logical_id(*unit)),
        Value::Date32(_) => LogicalTypeHandle::from(LogicalTypeId::Date),
        Value::Time64(unit, _) => LogicalTypeHandle::from(time_logical_id(*unit)),
        Value::Interval { .. } => LogicalTypeHandle::from(LogicalTypeId::Interval),
        Value::List(items) => {
            let child = homogeneous_child_logical_type(items, "list")?;
            LogicalTypeHandle::list(&child)
        }
        Value::Array(items) => {
            let child = homogeneous_child_logical_type(items, "array")?;
            LogicalTypeHandle::array(&child, items.len() as u64)
        }
        Value::Struct(fields) => {
            let field_types: Vec<(&str, LogicalTypeHandle)> = fields
                .iter()
                .map(|(k, v)| Ok((k.as_str(), value_logical_type(v)?)))
                .collect::<Result<_>>()?;
            LogicalTypeHandle::struct_type(&field_types)
        }
        Value::Map(entries) => {
            let (key, value) = map_key_value_types(entries)?;
            LogicalTypeHandle::map(&key, &value)
        }
        Value::Enum(_) | Value::Union(_) => {
            return Err(Error::ToSqlConversionFailure(
                format!(
                    "binding {} parameters is not supported via the duckdb_value API",
                    variant_name(value)
                )
                .into(),
            ));
        }
    })
}

fn timestamp_logical_id(unit: TimeUnit) -> LogicalTypeId {
    match unit {
        TimeUnit::Second => LogicalTypeId::TimestampS,
        TimeUnit::Millisecond => LogicalTypeId::TimestampMs,
        TimeUnit::Microsecond => LogicalTypeId::Timestamp,
        TimeUnit::Nanosecond => LogicalTypeId::TimestampNs,
    }
}

fn time_logical_id(unit: TimeUnit) -> LogicalTypeId {
    match unit {
        TimeUnit::Second | TimeUnit::Millisecond | TimeUnit::Microsecond => LogicalTypeId::Time,
        TimeUnit::Nanosecond => LogicalTypeId::TimeNs,
    }
}

fn variant_name(value: &Value) -> &'static str {
    match value {
        Value::Enum(_) => "Enum",
        Value::Union(_) => "Union",
        _ => "container",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{OrderedMap, Type};

    #[test]
    fn data_type_of_float_array() {
        let v = Value::Array(vec![Value::Float(1.0), Value::Float(2.0)]);
        assert_eq!(v.data_type(), Type::Array(Box::new(Type::Float), 2));
    }

    #[test]
    fn data_type_of_empty_list_falls_back_to_null_child() {
        let v = Value::List(vec![]);
        assert_eq!(v.data_type(), Type::List(Box::new(Type::Null)));
    }

    #[test]
    fn data_type_of_struct_preserves_fields() {
        let fields = OrderedMap::from(vec![
            ("a".to_string(), Value::Int(1)),
            ("b".to_string(), Value::Float(2.0)),
        ]);
        let v = Value::Struct(fields);
        assert_eq!(
            v.data_type(),
            Type::Struct(vec![("a".to_string(), Type::Int), ("b".to_string(), Type::Float)])
        );
    }

    #[test]
    fn data_type_of_map_uses_first_entry() {
        let entries = OrderedMap::from(vec![(Value::Text("k".to_string()), Value::Int(7))]);
        let v = Value::Map(entries);
        assert_eq!(v.data_type(), Type::Map(Box::new(Type::Text), Box::new(Type::Int)));
    }

    #[test]
    fn ffi_value_supports_float_array() {
        let v = Value::Array(vec![Value::Float(1.5), Value::Float(2.5), Value::Float(3.5)]);
        let ffi_value = FfiValue::from_value(&v).expect("array should convert");
        assert!(!ffi_value.ptr().is_null());
    }

    #[test]
    fn ffi_value_rejects_empty_list() {
        let v = Value::List(vec![]);
        let err = FfiValue::from_value(&v).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("empty list"), "unexpected message: {msg}");
    }

    #[test]
    fn ffi_value_supports_array_of_structs() {
        let row = OrderedMap::from(vec![
            ("x".to_string(), Value::Int(1)),
            ("y".to_string(), Value::Float(2.0)),
        ]);
        let v = Value::Array(vec![Value::Struct(row.clone()), Value::Struct(row)]);
        let ffi_value = FfiValue::from_value(&v).expect("array-of-struct should convert");
        assert!(!ffi_value.ptr().is_null());
    }

    #[test]
    fn ffi_value_supports_map() {
        let entries = OrderedMap::from(vec![
            (Value::Text("a".to_string()), Value::Int(1)),
            (Value::Text("b".to_string()), Value::Int(2)),
        ]);
        let v = Value::Map(entries);
        let ffi_value = FfiValue::from_value(&v).expect("map should convert");
        assert!(!ffi_value.ptr().is_null());
    }

    #[test]
    fn ffi_value_rejects_enum() {
        let v = Value::Enum("variant".to_string());
        assert!(FfiValue::from_value(&v).is_err());
    }

    #[test]
    fn ffi_value_supports_nested_list() {
        let v = Value::List(vec![
            Value::List(vec![Value::Int(1), Value::Int(2)]),
            Value::List(vec![Value::Int(3)]),
        ]);
        let ffi_value = FfiValue::from_value(&v).expect("nested list should convert");
        assert!(!ffi_value.ptr().is_null());
    }
}

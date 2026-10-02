//! Owned `List`, `Array`, `Struct`, and `Map` values for parameter binding.
//!
//! DuckDB 1.5.5 `duckdb_create_list_value` and `duckdb_create_array_value` take
//! the child logical type. Both return null when the child pointer is null,
//! including a count of zero, so an empty container passes a non-null dangling
//! pointer. `duckdb_bind_value` and `duckdb_append_value` copy the value. This
//! module destroys the children after create and the container after bind or
//! append.

use std::ffi::{CString, c_char};
use std::ptr::NonNull;

use crate::core::{LogicalTypeHandle, LogicalTypeId};
use crate::ffi;
use crate::{Error, Result};

use super::{
    OrderedMap, TimeUnit, Value, binding_unsupported_value, to_duckdb_decimal, to_duckdb_hugeint, to_duckdb_uhugeint,
};

/// A `duckdb_value` this module owns.
///
/// [`OwnedDuckValue::as_raw`] is valid until this value is dropped.
pub(crate) struct OwnedDuckValue {
    ptr: ffi::duckdb_value,
}

impl OwnedDuckValue {
    pub(crate) fn as_raw(&self) -> ffi::duckdb_value {
        self.ptr
    }

    fn from_raw(ptr: ffi::duckdb_value, kind: &str) -> Result<Self> {
        if ptr.is_null() {
            return Err(rejected(kind));
        }
        Ok(Self { ptr })
    }
}

impl Drop for OwnedDuckValue {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            // SAFETY: `ptr` is a `duckdb_value` this object owns, and this is
            // the only destroy. DuckDB frees the value and clears the pointer.
            unsafe { ffi::duckdb_destroy_value(&mut self.ptr) }
        }
    }
}

fn container_type_name(value: &Value) -> Option<&'static str> {
    match value {
        Value::List(_) => Some("List"),
        Value::Array(_) => Some("Array"),
        Value::Struct(_) => Some("Struct"),
        Value::Map(_) => Some("Map"),
        _ => None,
    }
}

pub(crate) fn is_owned_container(value: &Value) -> bool {
    container_type_name(value).is_some()
}

/// Reject a container that cannot be bound, without building a `duckdb_value`.
///
/// Callers that append a row use this before DuckDB starts the row, then build
/// the value once at bind time.
pub(crate) fn validate_owned_container(value: &Value) -> Result<()> {
    match value {
        Value::List(_) | Value::Array(_) | Value::Struct(_) | Value::Map(_) => {
            value_shape(value)?;
            Ok(())
        }
        _ => Err(conversion("internal error: value is not a bindable container")),
    }
}

pub(crate) fn create_owned_container(value: &Value) -> Result<OwnedDuckValue> {
    match value {
        Value::List(items) => create_list(items),
        Value::Array(items) => create_array(items),
        Value::Struct(fields) => create_struct(fields),
        Value::Map(entries) => create_map(entries),
        Value::Enum(_) => Err(unsupported_variant("Enum")),
        Value::Union(_) => Err(unsupported_variant("Union")),
        _ => Err(conversion("internal error: value is not a bindable container")),
    }
}

#[derive(Debug, PartialEq, Eq)]
enum Shape {
    Boolean,
    TinyInt,
    SmallInt,
    Int,
    BigInt,
    HugeInt,
    UHugeInt,
    UTinyInt,
    USmallInt,
    UInt,
    UBigInt,
    Float,
    Double,
    Decimal { width: u8, scale: u8 },
    Timestamp,
    Varchar,
    Blob,
    Date,
    Time,
    Interval,
    List(Box<Shape>),
    Array { child: Box<Shape>, len: usize },
    Struct(Vec<(String, Shape)>),
    Map { key: Box<Shape>, value: Box<Shape> },
}

fn create_list(items: &[Value]) -> Result<OwnedDuckValue> {
    let child_shape = sequence_shape(items, "List")?;
    let child_type = shape_to_logical_type(&child_shape)?;
    let children = create_children(items)?;
    let ptrs = raw_values(&children);
    // SAFETY: `child_type` is a live logical type. `ptrs` owns the child
    // values and outlives this call. `nonnull_mut` is only read by DuckDB
    // 1.5.5, which copies the children. An empty slice is a dangling
    // non-null pointer with a count of 0, and that call does not dereference it.
    let created = unsafe { ffi::duckdb_create_list_value(child_type.ptr, nonnull_mut(&ptrs), ptrs.len() as u64) };
    OwnedDuckValue::from_raw(created, "List")
}

fn create_array(items: &[Value]) -> Result<OwnedDuckValue> {
    let child_shape = sequence_shape(items, "Array")?;
    let child_type = shape_to_logical_type(&child_shape)?;
    let children = create_children(items)?;
    let ptrs = raw_values(&children);
    // SAFETY: same contract as `create_list`. DuckDB copies the children and
    // does not write through `nonnull_mut`.
    let created = unsafe { ffi::duckdb_create_array_value(child_type.ptr, nonnull_mut(&ptrs), ptrs.len() as u64) };
    OwnedDuckValue::from_raw(created, "Array")
}

fn create_struct(fields: &OrderedMap<String, Value>) -> Result<OwnedDuckValue> {
    let field_count = fields.iter().count();
    let mut names = Vec::with_capacity(field_count);
    let mut shapes = Vec::with_capacity(field_count);
    let mut children = Vec::with_capacity(field_count);
    for (name, field) in fields.iter() {
        names.push(name.as_str());
        shapes.push(field_shape(field)?);
        children.push(create_value(field)?);
    }
    let mut type_handles = Vec::with_capacity(shapes.len());
    for shape in &shapes {
        type_handles.push(shape_to_logical_type(shape)?);
    }
    let logical = struct_logical_type(&names, &type_handles)?;
    let ptrs = raw_values(&children);
    // SAFETY: `logical` is a struct type whose field count equals `ptrs`.
    // The child values outlive this call. DuckDB 1.5.5 copies them and does
    // not write through the pointer. An empty struct passes a dangling
    // non-null pointer, and the C loop does not run when the field count is 0.
    let created = unsafe { ffi::duckdb_create_struct_value(logical.ptr, nonnull_mut(&ptrs)) };
    OwnedDuckValue::from_raw(created, "Struct")
}

fn create_map(entries: &OrderedMap<Value, Value>) -> Result<OwnedDuckValue> {
    let (key_shape, value_shape) = map_shapes(entries)?;
    let key_type = shape_to_logical_type(&key_shape)?;
    let value_type = shape_to_logical_type(&value_shape)?;
    let map_type = LogicalTypeHandle::map(&key_type, &value_type);
    if map_type.ptr.is_null() {
        return Err(rejected("Map"));
    }
    let entry_count = entries.iter().count();
    let mut keys = Vec::with_capacity(entry_count);
    let mut values = Vec::with_capacity(entry_count);
    for (key, value) in entries.iter() {
        keys.push(create_value(key)?);
        values.push(create_value(value)?);
    }
    let key_ptrs = raw_values(&keys);
    let value_ptrs = raw_values(&values);
    // SAFETY: `map_type` is a live map type. Key and value slices have the
    // same length and outlive this call. DuckDB copies both and does not
    // write through the pointers. Empty slices use a dangling non-null pointer
    // with a count of 0, which the C loop does not dereference.
    let created = unsafe {
        ffi::duckdb_create_map_value(
            map_type.ptr,
            nonnull_mut(&key_ptrs),
            nonnull_mut(&value_ptrs),
            key_ptrs.len() as u64,
        )
    };
    OwnedDuckValue::from_raw(created, "Map")
}

fn create_children(items: &[Value]) -> Result<Vec<OwnedDuckValue>> {
    let mut children = Vec::with_capacity(items.len());
    for item in items {
        children.push(create_value(item)?);
    }
    Ok(children)
}

fn create_value(value: &Value) -> Result<OwnedDuckValue> {
    // SAFETY: each constructor copies its argument. Text and blob pointers
    // borrow `value` and stay live for the call. A null return is not owned
    // and is rejected by `OwnedDuckValue::from_raw`.
    let ptr = unsafe {
        match value {
            Value::Null => ffi::duckdb_create_null_value(),
            Value::Boolean(v) => ffi::duckdb_create_bool(*v),
            Value::TinyInt(v) => ffi::duckdb_create_int8(*v),
            Value::SmallInt(v) => ffi::duckdb_create_int16(*v),
            Value::Int(v) => ffi::duckdb_create_int32(*v),
            Value::BigInt(v) => ffi::duckdb_create_int64(*v),
            Value::HugeInt(v) => ffi::duckdb_create_hugeint(to_duckdb_hugeint(*v)),
            Value::UHugeInt(v) => ffi::duckdb_create_uhugeint(to_duckdb_uhugeint(*v)),
            Value::UTinyInt(v) => ffi::duckdb_create_uint8(*v),
            Value::USmallInt(v) => ffi::duckdb_create_uint16(*v),
            Value::UInt(v) => ffi::duckdb_create_uint32(*v),
            Value::UBigInt(v) => ffi::duckdb_create_uint64(*v),
            Value::Float(v) => ffi::duckdb_create_float(*v),
            Value::Double(v) => ffi::duckdb_create_double(*v),
            Value::Decimal(decimal) => ffi::duckdb_create_decimal(to_duckdb_decimal(*decimal)),
            Value::Timestamp(unit, v) => create_timestamp(*unit, *v),
            Value::Text(text) => {
                let bytes = text.as_bytes();
                ffi::duckdb_create_varchar_length(bytes.as_ptr().cast(), bytes.len() as u64)
            }
            // GEOMETRY has no C value constructor in DuckDB 1.5.5. Scalar binds
            // already send the WKB bytes as a blob.
            Value::Blob(bytes) | Value::Geometry(bytes) => ffi::duckdb_create_blob(bytes.as_ptr(), bytes.len() as u64),
            Value::Date32(days) => ffi::duckdb_create_date(ffi::duckdb_date { days: *days }),
            Value::Time64(unit, v) => create_time(*unit, *v),
            Value::Interval { months, days, nanos } => ffi::duckdb_create_interval(ffi::duckdb_interval {
                months: *months,
                days: *days,
                micros: nanos / 1000,
            }),
            Value::List(items) => return create_list(items),
            Value::Array(items) => return create_array(items),
            Value::Struct(fields) => return create_struct(fields),
            Value::Map(entries) => return create_map(entries),
            Value::Enum(_) => return Err(unsupported_variant("Enum")),
            Value::Union(_) => return Err(unsupported_variant("Union")),
        }
    };
    OwnedDuckValue::from_raw(ptr, "scalar")
}

fn create_timestamp(unit: TimeUnit, value: i64) -> ffi::duckdb_value {
    // Scalar binds collapse every unit to TIMESTAMP microseconds.
    // SAFETY: `micros` is a plain integer. DuckDB copies it into a new value.
    unsafe {
        ffi::duckdb_create_timestamp(ffi::duckdb_timestamp {
            micros: unit.to_micros(value),
        })
    }
}

fn create_time(unit: TimeUnit, value: i64) -> ffi::duckdb_value {
    // Scalar binds collapse every unit to TIME microseconds.
    // SAFETY: `micros` is a plain integer. DuckDB copies it into a new value.
    unsafe {
        ffi::duckdb_create_time(ffi::duckdb_time {
            micros: unit.to_micros(value),
        })
    }
}

/// Element type of a list or array.
///
/// Null children do not choose the type. An empty sequence, or a sequence of
/// only nulls, uses `VARCHAR`.
/// Children must share one shape. An empty nested list is `VARCHAR[]`, so it
/// does not match a list of integers.
fn sequence_shape(items: &[Value], kind: &str) -> Result<Shape> {
    let mut found = None;
    for item in items {
        if matches!(item, Value::Null) {
            continue;
        }
        let shape = value_shape(item)?;
        match &found {
            None => found = Some(shape),
            Some(existing) if existing == &shape => {}
            Some(_) => {
                return Err(conversion(format!("cannot bind {kind} with mixed element types")));
            }
        }
    }
    Ok(found.unwrap_or(Shape::Varchar))
}

fn field_shape(value: &Value) -> Result<Shape> {
    if matches!(value, Value::Null) {
        Ok(Shape::Varchar)
    } else {
        value_shape(value)
    }
}

fn value_shape(value: &Value) -> Result<Shape> {
    Ok(match value {
        Value::Null => return Err(conversion("internal error: NULL has no container child type")),
        Value::Boolean(_) => Shape::Boolean,
        Value::TinyInt(_) => Shape::TinyInt,
        Value::SmallInt(_) => Shape::SmallInt,
        Value::Int(_) => Shape::Int,
        Value::BigInt(_) => Shape::BigInt,
        Value::HugeInt(_) => Shape::HugeInt,
        Value::UHugeInt(_) => Shape::UHugeInt,
        Value::UTinyInt(_) => Shape::UTinyInt,
        Value::USmallInt(_) => Shape::USmallInt,
        Value::UInt(_) => Shape::UInt,
        Value::UBigInt(_) => Shape::UBigInt,
        Value::Float(_) => Shape::Float,
        Value::Double(_) => Shape::Double,
        Value::Decimal(decimal) => Shape::Decimal {
            width: decimal.width(),
            scale: decimal.scale(),
        },
        Value::Timestamp(_, _) => Shape::Timestamp,
        Value::Text(_) => Shape::Varchar,
        Value::Blob(_) | Value::Geometry(_) => Shape::Blob,
        Value::Date32(_) => Shape::Date,
        Value::Time64(_, _) => Shape::Time,
        Value::Interval { .. } => Shape::Interval,
        Value::List(items) => Shape::List(Box::new(sequence_shape(items, "List")?)),
        Value::Array(items) => Shape::Array {
            child: Box::new(sequence_shape(items, "Array")?),
            len: items.len(),
        },
        Value::Struct(fields) => {
            let mut shapes = Vec::new();
            for (name, field) in fields.iter() {
                if name.as_bytes().contains(&0) {
                    return Err(conversion("struct field name contains an interior NUL"));
                }
                shapes.push((name.clone(), field_shape(field)?));
            }
            Shape::Struct(shapes)
        }
        Value::Map(entries) => {
            let (key, value) = map_shapes(entries)?;
            Shape::Map {
                key: Box::new(key),
                value: Box::new(value),
            }
        }
        Value::Enum(_) => return Err(unsupported_variant("Enum")),
        Value::Union(_) => return Err(unsupported_variant("Union")),
    })
}

fn map_shapes(entries: &OrderedMap<Value, Value>) -> Result<(Shape, Shape)> {
    let mut key_shape = None;
    let mut found_value_shape = None;
    let mut seen_keys = Vec::new();
    for (key, value) in entries.iter() {
        if matches!(key, Value::Null) {
            return Err(conversion("cannot bind Map with a NULL key"));
        }
        if seen_keys.contains(&key) {
            return Err(conversion("cannot bind Map with duplicate keys"));
        }
        seen_keys.push(key);
        let next_key = value_shape(key)?;
        match &key_shape {
            None => key_shape = Some(next_key),
            Some(existing) if existing == &next_key => {}
            Some(_) => return Err(conversion("cannot bind Map with mixed key types")),
        }
        if matches!(value, Value::Null) {
            continue;
        }
        let next_value = value_shape(value)?;
        match &found_value_shape {
            None => found_value_shape = Some(next_value),
            Some(existing) if existing == &next_value => {}
            Some(_) => return Err(conversion("cannot bind Map with mixed value types")),
        }
    }
    Ok((
        key_shape.unwrap_or(Shape::Varchar),
        found_value_shape.unwrap_or(Shape::Varchar),
    ))
}

fn shape_to_logical_type(shape: &Shape) -> Result<LogicalTypeHandle> {
    let handle = match shape {
        Shape::Boolean => LogicalTypeHandle::from(LogicalTypeId::Boolean),
        Shape::TinyInt => LogicalTypeHandle::from(LogicalTypeId::Tinyint),
        Shape::SmallInt => LogicalTypeHandle::from(LogicalTypeId::Smallint),
        Shape::Int => LogicalTypeHandle::from(LogicalTypeId::Integer),
        Shape::BigInt => LogicalTypeHandle::from(LogicalTypeId::Bigint),
        Shape::HugeInt => LogicalTypeHandle::from(LogicalTypeId::Hugeint),
        Shape::UHugeInt => LogicalTypeHandle::from(LogicalTypeId::UHugeint),
        Shape::UTinyInt => LogicalTypeHandle::from(LogicalTypeId::UTinyint),
        Shape::USmallInt => LogicalTypeHandle::from(LogicalTypeId::USmallint),
        Shape::UInt => LogicalTypeHandle::from(LogicalTypeId::UInteger),
        Shape::UBigInt => LogicalTypeHandle::from(LogicalTypeId::UBigint),
        Shape::Float => LogicalTypeHandle::from(LogicalTypeId::Float),
        Shape::Double => LogicalTypeHandle::from(LogicalTypeId::Double),
        Shape::Decimal { width, scale } => LogicalTypeHandle::decimal(*width, *scale),
        Shape::Timestamp => LogicalTypeHandle::from(LogicalTypeId::Timestamp),
        Shape::Varchar => LogicalTypeHandle::from(LogicalTypeId::Varchar),
        Shape::Blob => LogicalTypeHandle::from(LogicalTypeId::Blob),
        Shape::Date => LogicalTypeHandle::from(LogicalTypeId::Date),
        Shape::Time => LogicalTypeHandle::from(LogicalTypeId::Time),
        Shape::Interval => LogicalTypeHandle::from(LogicalTypeId::Interval),
        Shape::List(child) => {
            let child = shape_to_logical_type(child)?;
            LogicalTypeHandle::list(&child)
        }
        Shape::Array { child, len } => {
            let child = shape_to_logical_type(child)?;
            LogicalTypeHandle::array(&child, *len as u64)
        }
        Shape::Struct(fields) => {
            let mut names = Vec::with_capacity(fields.len());
            let mut types = Vec::with_capacity(fields.len());
            for (name, field) in fields {
                names.push(name.as_str());
                types.push(shape_to_logical_type(field)?);
            }
            return struct_logical_type(&names, &types);
        }
        Shape::Map { key, value } => {
            let key = shape_to_logical_type(key)?;
            let value = shape_to_logical_type(value)?;
            LogicalTypeHandle::map(&key, &value)
        }
    };
    if handle.ptr.is_null() {
        return Err(rejected("logical type"));
    }
    Ok(handle)
}

fn struct_logical_type(names: &[&str], types: &[LogicalTypeHandle]) -> Result<LogicalTypeHandle> {
    let c_names = names
        .iter()
        .map(|name| CString::new(*name))
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|_| conversion("struct field name contains an interior NUL"))?;
    let name_ptrs: Vec<*const c_char> = c_names.iter().map(|name| name.as_ptr()).collect();
    let type_ptrs: Vec<ffi::duckdb_logical_type> = types.iter().map(|logical_type| logical_type.ptr).collect();
    // SAFETY: both slices have `names.len()` live entries and outlive this
    // call. `nonnull_mut` is non-null for an empty struct, and DuckDB does
    // not dereference it when the count is 0. DuckDB copies the names and types.
    let ptr =
        unsafe { ffi::duckdb_create_struct_type(nonnull_mut(&type_ptrs), nonnull_mut(&name_ptrs), names.len() as u64) };
    if ptr.is_null() {
        return Err(rejected("Struct"));
    }
    // SAFETY: `ptr` is a non-null logical type just created here and not yet
    // destroyed. `LogicalTypeHandle` frees it.
    Ok(unsafe { LogicalTypeHandle::new(ptr) })
}

fn raw_values(values: &[OwnedDuckValue]) -> Vec<ffi::duckdb_value> {
    values.iter().map(OwnedDuckValue::as_raw).collect()
}

/// Pointer DuckDB 1.5.5 can read.
///
/// An empty slice cannot be a null pointer. `duckdb_create_list_value`,
/// `duckdb_create_array_value`, and `duckdb_create_struct_type` return null
/// when the values pointer is null, even when the count is 0. The dangling
/// pointer is non-null, and those functions do not dereference it when the
/// count is 0.
///
/// The C signatures take `*mut` and are not const. Callers must not write
/// through the pointer. A non-empty return borrows `values` for the call.
fn nonnull_mut<T>(values: &[T]) -> *mut T {
    if values.is_empty() {
        NonNull::<T>::dangling().as_ptr()
    } else {
        values.as_ptr().cast_mut()
    }
}

fn conversion(message: impl Into<String>) -> Error {
    Error::ToSqlConversionFailure(message.into().into())
}

fn rejected(kind: &str) -> Error {
    conversion(format!("DuckDB rejected the {kind} value"))
}

fn unsupported_variant(variant: &'static str) -> Error {
    Error::ToSqlConversionFailure(binding_unsupported_value(variant).into())
}

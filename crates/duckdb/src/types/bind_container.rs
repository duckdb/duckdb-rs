//! Owned `List`, `Array`, `Struct`, and `Map` values for parameter binding.
//!
//! DuckDB `duckdb_create_list_value` and `duckdb_create_array_value` take
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

use super::{Decimal, OrderedMap, TimeUnit, Value, binding_unsupported_value, to_duckdb_hugeint, to_duckdb_uhugeint};

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
enum Shape<'a> {
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
    Decimal {
        width: u8,
        scale: u8,
    },
    Timestamp,
    Varchar,
    /// No concrete child yet. An empty container or a NULL field uses this
    /// until a sibling supplies a type. It becomes `VARCHAR` only when nothing
    /// concrete is merged in.
    Unknown,
    Blob,
    Date,
    Time,
    Interval,
    List(Box<Shape<'a>>),
    Array {
        child: Box<Shape<'a>>,
        len: usize,
    },
    Struct(Vec<(&'a str, Shape<'a>)>),
    Map {
        key: Box<Shape<'a>>,
        value: Box<Shape<'a>>,
    },
}

/// A logical type and the child types that nested values reuse.
///
/// DuckDB copies this type into each value it creates. Every sibling that
/// shares the shape uses this same handle.
struct BuiltType {
    handle: LogicalTypeHandle,
    nested: NestedType,
}

enum NestedType {
    Flat,
    Child(Box<BuiltType>),
    Fields(Vec<BuiltType>),
    Map { key: Box<BuiltType>, value: Box<BuiltType> },
}

fn create_list(items: &[Value]) -> Result<OwnedDuckValue> {
    let child_shape = sequence_shape(items, "List")?;
    let child_type = build_type(&child_shape)?;
    create_list_with_shape(items, &child_shape, &child_type)
}

fn create_list_with_shape(items: &[Value], child_shape: &Shape<'_>, child_type: &BuiltType) -> Result<OwnedDuckValue> {
    let mut children = Vec::with_capacity(items.len());
    for item in items {
        children.push(create_value_with_shape(item, child_shape, child_type)?);
    }
    let ptrs = raw_values(&children);
    // SAFETY: `child_type.handle` stays alive for this call. DuckDB copies
    // that logical type and the child values. `nonnull_mut` is only read.
    // An empty slice is a dangling non-null pointer with a count of 0, and
    // that call does not dereference it.
    let created =
        unsafe { ffi::duckdb_create_list_value(child_type.handle.ptr, nonnull_mut(&ptrs), ptrs.len() as u64) };
    OwnedDuckValue::from_raw(created, "List")
}

fn create_array(items: &[Value]) -> Result<OwnedDuckValue> {
    let child_shape = array_shape(items)?;
    let child_type = build_type(&child_shape)?;
    create_array_with_shape(items, &child_shape, &child_type)
}

fn create_array_with_shape(items: &[Value], child_shape: &Shape<'_>, child_type: &BuiltType) -> Result<OwnedDuckValue> {
    let mut children = Vec::with_capacity(items.len());
    for item in items {
        children.push(create_value_with_shape(item, child_shape, child_type)?);
    }
    let ptrs = raw_values(&children);
    // SAFETY: same contract as `create_list`. DuckDB copies the logical type
    // and the children, and it does not write through `nonnull_mut`.
    let created =
        unsafe { ffi::duckdb_create_array_value(child_type.handle.ptr, nonnull_mut(&ptrs), ptrs.len() as u64) };
    OwnedDuckValue::from_raw(created, "Array")
}

fn create_struct(fields: &OrderedMap<String, Value>) -> Result<OwnedDuckValue> {
    let expected = struct_fields(fields)?;
    let built = build_struct_type(&expected)?;
    let NestedType::Fields(field_types) = &built.nested else {
        return Err(conversion("internal error: struct shape has no field types"));
    };
    create_struct_with_shape(fields, &expected, &built.handle, field_types)
}

fn create_struct_with_shape(
    fields: &OrderedMap<String, Value>,
    expected: &[(&str, Shape<'_>)],
    struct_type: &LogicalTypeHandle,
    field_types: &[BuiltType],
) -> Result<OwnedDuckValue> {
    if fields.iter().count() != expected.len() {
        return Err(conversion("cannot bind Struct with mismatched fields"));
    }
    if field_types.len() != expected.len() {
        return Err(conversion("internal error: struct shape has no field types"));
    }
    let mut children = Vec::with_capacity(expected.len());
    for ((name, field), ((expected_name, expected_shape), field_type)) in
        fields.iter().zip(expected.iter().zip(field_types))
    {
        if name != expected_name {
            return Err(conversion("cannot bind Struct with mismatched fields"));
        }
        children.push(create_value_with_shape(field, expected_shape, field_type)?);
    }
    let ptrs = raw_values(&children);
    // SAFETY: `struct_type` stays alive for this call. DuckDB copies that
    // struct type and the child values. It does not write through the
    // pointer. An empty struct passes a dangling non-null pointer, and the
    // C loop does not run when the field count is 0.
    let created = unsafe { ffi::duckdb_create_struct_value(struct_type.ptr, nonnull_mut(&ptrs)) };
    OwnedDuckValue::from_raw(created, "Struct")
}

fn create_map(entries: &OrderedMap<Value, Value>) -> Result<OwnedDuckValue> {
    let (key_shape, value_shape) = map_shapes(entries)?;
    let shape = Shape::Map {
        key: Box::new(key_shape),
        value: Box::new(value_shape),
    };
    let built = build_type(&shape)?;
    let Shape::Map { key, value } = &shape else {
        return Err(conversion("internal error: map shape has no key type"));
    };
    let NestedType::Map {
        key: key_type,
        value: value_type,
    } = &built.nested
    else {
        return Err(conversion("internal error: map shape has no key type"));
    };
    create_map_with_shape(entries, key, value, &built.handle, key_type, value_type)
}

fn create_map_with_shape(
    entries: &OrderedMap<Value, Value>,
    key_shape: &Shape<'_>,
    value_shape: &Shape<'_>,
    map_type: &LogicalTypeHandle,
    key_type: &BuiltType,
    value_type: &BuiltType,
) -> Result<OwnedDuckValue> {
    let entry_count = entries.iter().count();
    let mut keys = Vec::with_capacity(entry_count);
    let mut values = Vec::with_capacity(entry_count);
    for (key, value) in entries.iter() {
        keys.push(create_value_with_shape(key, key_shape, key_type)?);
        values.push(create_value_with_shape(value, value_shape, value_type)?);
    }
    let key_ptrs = raw_values(&keys);
    let value_ptrs = raw_values(&values);
    // SAFETY: `map_type` stays alive for this call. DuckDB copies that map
    // type and both slices. It does not write through the pointers. Empty
    // slices use a dangling non-null pointer with a count of 0, which the C
    // loop does not dereference.
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

fn create_value_with_shape(value: &Value, shape: &Shape<'_>, ty: &BuiltType) -> Result<OwnedDuckValue> {
    if matches!(value, Value::Null) {
        return null_value();
    }
    match (value, shape) {
        (Value::List(items), Shape::List(child)) => match &ty.nested {
            NestedType::Child(child_type) => create_list_with_shape(items, child, child_type),
            _ => Err(conversion("internal error: list shape has no child type")),
        },
        (Value::Array(items), Shape::Array { child, .. }) => match &ty.nested {
            NestedType::Child(child_type) => create_array_with_shape(items, child, child_type),
            _ => Err(conversion("internal error: array shape has no child type")),
        },
        (Value::Struct(fields), Shape::Struct(expected)) => match &ty.nested {
            NestedType::Fields(field_types) => create_struct_with_shape(fields, expected, &ty.handle, field_types),
            _ => Err(conversion("internal error: struct shape has no field types")),
        },
        (Value::Map(entries), Shape::Map { key, value }) => match &ty.nested {
            NestedType::Map {
                key: key_type,
                value: value_type,
            } => create_map_with_shape(entries, key, value, &ty.handle, key_type, value_type),
            _ => Err(conversion("internal error: map shape has no key type")),
        },
        (Value::Decimal(decimal), Shape::Decimal { width, scale }) => create_decimal(*decimal, *width, *scale),
        _ => create_value(value),
    }
}

fn null_value() -> Result<OwnedDuckValue> {
    // SAFETY: `duckdb_create_null_value` returns a new value this module owns.
    // A null return is rejected by `OwnedDuckValue::from_raw`.
    let ptr = unsafe { ffi::duckdb_create_null_value() };
    OwnedDuckValue::from_raw(ptr, "scalar")
}

fn create_decimal(decimal: Decimal, width: u8, scale: u8) -> Result<OwnedDuckValue> {
    // SAFETY: `width`, `scale`, and the payload are plain integers. DuckDB
    // copies them into a new value. A null return is rejected by `from_raw`.
    let ptr = unsafe {
        ffi::duckdb_create_decimal(ffi::duckdb_decimal {
            width,
            scale,
            value: to_duckdb_hugeint(decimal.value()),
        })
    };
    OwnedDuckValue::from_raw(ptr, "scalar")
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
            Value::Decimal(decimal) => ffi::duckdb_create_decimal(ffi::duckdb_decimal {
                width: decimal.width(),
                scale: decimal.scale(),
                value: to_duckdb_hugeint(decimal.value()),
            }),
            Value::Timestamp(unit, v) => create_timestamp(*unit, *v),
            Value::Text(text) => {
                let bytes = text.as_bytes();
                ffi::duckdb_create_varchar_length(bytes.as_ptr().cast(), bytes.len() as u64)
            }
            // GEOMETRY has no C value constructor. Scalar binds
            // already send the WKB bytes as a blob.
            Value::Blob(bytes) | Value::Geometry(bytes) => ffi::duckdb_create_blob(bytes.as_ptr(), bytes.len() as u64),
            Value::Date32(days) => ffi::duckdb_create_date(ffi::duckdb_date { days: *days }),
            Value::Time64(unit, v) => create_time(*unit, *v),
            Value::Interval { months, days, nanos } => ffi::duckdb_create_interval(ffi::duckdb_interval {
                months: *months,
                days: *days,
                micros: nanos / 1000,
            }),
            Value::List(_) | Value::Array(_) | Value::Struct(_) | Value::Map(_) => {
                return Err(conversion(
                    "internal error: Containers must go through `create_value_with_shape` to share the sibling type.",
                ));
            }
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

/// Combines two child shapes into one shape.
///
/// A shape is the type of one container child.
/// `Unknown` means that the child has no concrete type yet.
///
/// The function returns a shape in these cases:
///
/// 1. `Unknown` takes the other shape.
/// 2. Two decimals with the same scale use the larger width.
/// 3. Two lists combine their child shapes.
/// 4. Two maps combine their key shapes and their value shapes.
/// 5. Two arrays with the same length combine their child shapes.
/// 6. Two structs with the same field names and the same field order combine each field.
/// 7. Two equal shapes return the left shape.
///
/// A nested container uses these same rules for each child.
/// When the shapes do not match, the function returns an error.
/// The error text is `cannot bind {kind} with mixed element types`.
fn merge_shape<'a>(left: Shape<'a>, right: Shape<'a>, kind: &str) -> Result<Shape<'a>> {
    match (left, right) {
        // 1. `Unknown` takes the other shape.
        (Shape::Unknown, other) | (other, Shape::Unknown) => Ok(other),
        // 2. Two decimals with the same scale use the larger width.
        (
            Shape::Decimal {
                width: left_width,
                scale: left_scale,
            },
            Shape::Decimal {
                width: right_width,
                scale: right_scale,
            },
        ) if left_scale == right_scale => Ok(Shape::Decimal {
            width: left_width.max(right_width),
            scale: left_scale,
        }),
        // 3. Two lists combine their child shapes.
        (Shape::List(left_child), Shape::List(right_child)) => {
            Ok(Shape::List(Box::new(merge_shape(*left_child, *right_child, kind)?)))
        }
        // 4. Two maps combine their key shapes and their value shapes.
        (
            Shape::Map {
                key: left_key,
                value: left_value,
            },
            Shape::Map {
                key: right_key,
                value: right_value,
            },
        ) => Ok(Shape::Map {
            key: Box::new(merge_shape(*left_key, *right_key, kind)?),
            value: Box::new(merge_shape(*left_value, *right_value, kind)?),
        }),
        // 5. Two arrays with the same length combine their child shapes.
        (
            Shape::Array {
                child: left_child,
                len: left_len,
            },
            Shape::Array {
                child: right_child,
                len: right_len,
            },
        ) if left_len == right_len => Ok(Shape::Array {
            child: Box::new(merge_shape(*left_child, *right_child, kind)?),
            len: left_len,
        }),
        // 6. Two structs with the same field names and the same field order combine each field.
        (Shape::Struct(left_fields), Shape::Struct(right_fields)) => {
            if left_fields.len() != right_fields.len() {
                return Err(mixed(kind));
            }
            let mut fields = Vec::with_capacity(left_fields.len());
            for ((left_name, left_shape), (right_name, right_shape)) in left_fields.into_iter().zip(right_fields) {
                if left_name != right_name {
                    return Err(mixed(kind));
                }
                fields.push((left_name, merge_shape(left_shape, right_shape, kind)?));
            }
            Ok(Shape::Struct(fields))
        }
        // 7. Two equal shapes return the left shape.
        (left, right) if left == right => Ok(left),
        _ => Err(mixed(kind)),
    }
}

fn absorb<'a>(found: &mut Option<Shape<'a>>, next: Shape<'a>, kind: &str) -> Result<()> {
    match found.take() {
        None => *found = Some(next),
        Some(existing) => *found = Some(merge_shape(existing, next, kind)?),
    }
    Ok(())
}

fn absorb_message<'a>(found: &mut Option<Shape<'a>>, next: Shape<'a>, message: &'static str) -> Result<()> {
    match found.take() {
        None => *found = Some(next),
        Some(existing) => {
            *found = Some(merge_shape(existing, next, "Map").map_err(|_| conversion(message))?);
        }
    }
    Ok(())
}

fn mixed(kind: &str) -> Error {
    conversion(format!("cannot bind {kind} with mixed element types"))
}

/// Element type of a list or array.
///
/// Null children do not choose the type. An empty sequence, or a sequence of
/// only nulls, is `Unknown`. `Unknown` takes a sibling's shape. A real
/// `VARCHAR` child still does not match an integer.
fn sequence_shape<'a>(items: &'a [Value], kind: &str) -> Result<Shape<'a>> {
    let mut found = None;
    for item in items {
        if matches!(item, Value::Null) {
            continue;
        }
        absorb(&mut found, value_shape(item)?, kind)?;
    }
    Ok(found.unwrap_or(Shape::Unknown))
}

/// Element type of an array.
///
/// DuckDB has no valid zero-size ARRAY type.
fn array_shape<'a>(items: &'a [Value]) -> Result<Shape<'a>> {
    if items.is_empty() {
        return Err(conversion("cannot bind an empty Array"));
    }
    sequence_shape(items, "Array")
}

fn field_shape<'a>(value: &'a Value) -> Result<Shape<'a>> {
    if matches!(value, Value::Null) {
        Ok(Shape::Unknown)
    } else {
        value_shape(value)
    }
}

fn struct_fields<'a>(fields: &'a OrderedMap<String, Value>) -> Result<Vec<(&'a str, Shape<'a>)>> {
    let mut shapes: Vec<(&'a str, Shape<'a>)> = Vec::with_capacity(fields.iter().count());
    for (name, field) in fields.iter() {
        if name.as_bytes().contains(&0) {
            return Err(conversion("struct field name contains an interior NUL"));
        }
        // DuckDB compares struct field names case-insensitively.
        if shapes.iter().any(|(seen, _)| seen.eq_ignore_ascii_case(name)) {
            return Err(conversion(format!(
                "cannot bind Struct with duplicate field name {name:?}"
            )));
        }
        shapes.push((name.as_str(), field_shape(field)?));
    }
    Ok(shapes)
}

fn value_shape<'a>(value: &'a Value) -> Result<Shape<'a>> {
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
            child: Box::new(array_shape(items)?),
            len: items.len(),
        },
        Value::Struct(fields) => Shape::Struct(struct_fields(fields)?),
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

fn map_shapes<'a>(entries: &'a OrderedMap<Value, Value>) -> Result<(Shape<'a>, Shape<'a>)> {
    let mut key_shape = None;
    let mut found_value_shape = None;
    for (key, value) in entries.iter() {
        if matches!(key, Value::Null) {
            return Err(conversion("cannot bind Map with a NULL key"));
        }
        absorb_message(
            &mut key_shape,
            value_shape(key)?,
            "cannot bind Map with mixed key types",
        )?;
        if matches!(value, Value::Null) {
            continue;
        }
        absorb_message(
            &mut found_value_shape,
            value_shape(value)?,
            "cannot bind Map with mixed value types",
        )?;
    }
    Ok((
        key_shape.unwrap_or(Shape::Unknown),
        found_value_shape.unwrap_or(Shape::Unknown),
    ))
}

fn build_type(shape: &Shape<'_>) -> Result<BuiltType> {
    match shape {
        Shape::List(child) => {
            let child = build_type(child)?;
            let handle = live_type(LogicalTypeHandle::list(&child.handle), "logical type")?;
            Ok(BuiltType {
                handle,
                nested: NestedType::Child(Box::new(child)),
            })
        }
        Shape::Array { child, len } => {
            let child = build_type(child)?;
            let handle = live_type(LogicalTypeHandle::array(&child.handle, *len as u64), "logical type")?;
            Ok(BuiltType {
                handle,
                nested: NestedType::Child(Box::new(child)),
            })
        }
        Shape::Struct(fields) => build_struct_type(fields),
        Shape::Map { key, value } => {
            let key = build_type(key)?;
            let value = build_type(value)?;
            let handle = live_type(LogicalTypeHandle::map(&key.handle, &value.handle), "Map")?;
            Ok(BuiltType {
                handle,
                nested: NestedType::Map {
                    key: Box::new(key),
                    value: Box::new(value),
                },
            })
        }
        flat => {
            let handle = live_type(scalar_logical_type(flat), "logical type")?;
            Ok(BuiltType {
                handle,
                nested: NestedType::Flat,
            })
        }
    }
}

fn build_struct_type(fields: &[(&str, Shape<'_>)]) -> Result<BuiltType> {
    let mut names = Vec::with_capacity(fields.len());
    let mut children = Vec::with_capacity(fields.len());
    for (name, field) in fields {
        names.push(*name);
        children.push(build_type(field)?);
    }
    let type_refs: Vec<&LogicalTypeHandle> = children.iter().map(|child| &child.handle).collect();
    let handle = struct_logical_type(&names, &type_refs)?;
    Ok(BuiltType {
        handle,
        nested: NestedType::Fields(children),
    })
}

fn scalar_logical_type(shape: &Shape<'_>) -> LogicalTypeHandle {
    match shape {
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
        Shape::Varchar | Shape::Unknown => LogicalTypeHandle::from(LogicalTypeId::Varchar),
        Shape::Blob => LogicalTypeHandle::from(LogicalTypeId::Blob),
        Shape::Date => LogicalTypeHandle::from(LogicalTypeId::Date),
        Shape::Time => LogicalTypeHandle::from(LogicalTypeId::Time),
        Shape::Interval => LogicalTypeHandle::from(LogicalTypeId::Interval),
        Shape::List(_) | Shape::Array { .. } | Shape::Struct(_) | Shape::Map { .. } => {
            LogicalTypeHandle::from(LogicalTypeId::Varchar)
        }
    }
}

fn live_type(handle: LogicalTypeHandle, kind: &str) -> Result<LogicalTypeHandle> {
    if handle.ptr.is_null() {
        return Err(rejected(kind));
    }
    Ok(handle)
}

fn struct_logical_type(names: &[&str], types: &[&LogicalTypeHandle]) -> Result<LogicalTypeHandle> {
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

/// Pointer DuckDB can read.
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

//! Round-trip conversion between `Serialize`/`Deserialize` Rust values and
//! DuckDB `STRUCT` columns, via the [`Struct<T>`] newtype.
//!
//! Enable the `serde` cargo feature to use these impls. A `#[derive(Serialize,
//! Deserialize)]` struct maps to a DuckDB `STRUCT` whose field names match the
//! Rust field names. Nested structs, `Vec`/`[T; N]` (-> `LIST`), and string-keyed
//! maps (`HashMap`/`BTreeMap` -> `STRUCT`) are handled recursively.
//!
//! A blanket `impl<T: Serialize> ToSql` is impossible here because the existing
//! scalar `ToSql` impls (`i32`, `String`, `Option<T>`, ...) are themselves
//! `Serialize`, which would create overlapping impls. Wrapping the value in
//! [`Struct`] gives an equivalent blanket impl with no coherence conflict.
//!
//! ```rust,no_run
//! # use duckdb::{Connection, Result, types::Struct};
//! # use serde::{Serialize, Deserialize};
//! #[derive(Serialize, Deserialize, PartialEq, Debug)]
//! struct Point { x: i32, y: f64 }
//!
//! # fn example(conn: &Connection) -> Result<()> {
//! let p = Point { x: 3, y: 4.5 };
//! conn.execute("CREATE TABLE t (p STRUCT(x INTEGER, y DOUBLE))", [])?;
//! conn.execute("INSERT INTO t VALUES (?)", [Struct(&p)])?;
//! let back: Struct<Point> = conn.query_row("SELECT p FROM t", [], |r| r.get(0))?;
//! assert_eq!(back.0, p);
//! # Ok(())
//! # }
//! ```

use std::fmt;

use serde::Serialize;
use serde::de::{
    self, DeserializeOwned, DeserializeSeed, Deserializer, EnumAccess, MapAccess, SeqAccess, VariantAccess, Visitor,
};
use serde::ser::{self, SerializeMap, SerializeSeq, SerializeStruct, SerializeTuple, Serializer};

use crate::{
    Error, Result,
    types::{FromSql, FromSqlError, FromSqlResult, OrderedMap, ToSql, ToSqlOutput, Value, ValueRef},
};

/// Newtype wrapping a `Serialize`/`Deserialize` value for binding to or reading
/// from a DuckDB `STRUCT` column.
///
/// See the [module docs](crate::types::serde_struct) for the mapping rules.
#[derive(Debug, Clone)]
pub struct Struct<T>(pub T);

impl<T: Serialize> ToSql for Struct<T> {
    fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
        let value = self
            .0
            .serialize(ValueSerializer)
            .map_err(|e: SerError| Error::ToSqlConversionFailure(e.into()))?;
        Ok(ToSqlOutput::Owned(value))
    }
}

impl<T: DeserializeOwned> FromSql for Struct<T> {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        let owned = Value::from(value);
        let inner =
            T::deserialize(ValueDeserializer::new(owned)).map_err(|e: SerError| FromSqlError::Other(e.into()))?;
        Ok(Struct(inner))
    }
}

/// Error surfaced by both the serializer and the deserializer.
#[derive(Debug, Clone)]
struct SerError(String);

impl fmt::Display for SerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for SerError {}

impl ser::Error for SerError {
    fn custom<T: fmt::Display>(msg: T) -> Self {
        SerError(msg.to_string())
    }
}

impl de::Error for SerError {
    fn custom<T: fmt::Display>(msg: T) -> Self {
        SerError(msg.to_string())
    }
}

// ---------------------------------------------------------------------------
// Serializer: Rust value -> DuckDB Value
// ---------------------------------------------------------------------------

struct ValueSerializer;

impl Serializer for ValueSerializer {
    type Ok = Value;
    type Error = SerError;

    type SerializeSeq = SerSeq;
    type SerializeTuple = SerSeq;
    type SerializeTupleStruct = SerSeq;
    type SerializeTupleVariant = SerSeq;
    type SerializeMap = SerMap;
    type SerializeStruct = SerStruct;
    type SerializeStructVariant = SerStructVariant;

    fn serialize_bool(self, v: bool) -> Result<Value, SerError> {
        Ok(Value::Boolean(v))
    }
    fn serialize_i8(self, v: i8) -> Result<Value, SerError> {
        Ok(Value::TinyInt(v))
    }
    fn serialize_i16(self, v: i16) -> Result<Value, SerError> {
        Ok(Value::SmallInt(v))
    }
    fn serialize_i32(self, v: i32) -> Result<Value, SerError> {
        Ok(Value::Int(v))
    }
    fn serialize_i64(self, v: i64) -> Result<Value, SerError> {
        Ok(Value::BigInt(v))
    }
    fn serialize_i128(self, v: i128) -> Result<Value, SerError> {
        Ok(Value::HugeInt(v))
    }
    fn serialize_u8(self, v: u8) -> Result<Value, SerError> {
        Ok(Value::UTinyInt(v))
    }
    fn serialize_u16(self, v: u16) -> Result<Value, SerError> {
        Ok(Value::USmallInt(v))
    }
    fn serialize_u32(self, v: u32) -> Result<Value, SerError> {
        Ok(Value::UInt(v))
    }
    fn serialize_u64(self, v: u64) -> Result<Value, SerError> {
        Ok(Value::UBigInt(v))
    }
    fn serialize_u128(self, v: u128) -> Result<Value, SerError> {
        Ok(Value::UHugeInt(v))
    }
    fn serialize_f32(self, v: f32) -> Result<Value, SerError> {
        Ok(Value::Float(v))
    }
    fn serialize_f64(self, v: f64) -> Result<Value, SerError> {
        Ok(Value::Double(v))
    }
    fn serialize_char(self, v: char) -> Result<Value, SerError> {
        Ok(Value::Text(v.to_string()))
    }
    fn serialize_str(self, v: &str) -> Result<Value, SerError> {
        Ok(Value::Text(v.to_string()))
    }
    fn serialize_bytes(self, v: &[u8]) -> Result<Value, SerError> {
        Ok(Value::Blob(v.to_vec()))
    }
    fn serialize_none(self) -> Result<Value, SerError> {
        Ok(Value::Null)
    }
    fn serialize_some<T: ?Sized + Serialize>(self, value: &T) -> Result<Value, SerError> {
        value.serialize(self)
    }
    fn serialize_unit(self) -> Result<Value, SerError> {
        Ok(Value::Null)
    }
    fn serialize_unit_struct(self, _name: &'static str) -> Result<Value, SerError> {
        Ok(Value::Null)
    }
    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
    ) -> Result<Value, SerError> {
        Ok(Value::Text(variant.to_string()))
    }
    fn serialize_newtype_struct<T: ?Sized + Serialize>(
        self,
        _name: &'static str,
        value: &T,
    ) -> Result<Value, SerError> {
        value.serialize(self)
    }
    fn serialize_newtype_variant<T: ?Sized + Serialize>(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<Value, SerError> {
        let inner = value.serialize(ValueSerializer)?;
        let mut fields = OrderedMap::new();
        fields.insert(variant.to_string(), inner);
        Ok(Value::Struct(fields))
    }
    fn serialize_seq(self, _len: Option<usize>) -> Result<SerSeq, SerError> {
        Ok(SerSeq {
            items: Vec::new(),
            tag: None,
        })
    }
    fn serialize_tuple(self, _len: usize) -> Result<SerSeq, SerError> {
        Ok(SerSeq {
            items: Vec::new(),
            tag: None,
        })
    }
    fn serialize_tuple_struct(self, _name: &'static str, _len: usize) -> Result<SerSeq, SerError> {
        Ok(SerSeq {
            items: Vec::new(),
            tag: None,
        })
    }
    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        _len: usize,
    ) -> Result<SerSeq, SerError> {
        Ok(SerSeq {
            items: Vec::new(),
            tag: Some(variant.to_string()),
        })
    }
    fn serialize_map(self, _len: Option<usize>) -> Result<SerMap, SerError> {
        Ok(SerMap {
            fields: OrderedMap::new(),
            next_key: None,
        })
    }
    fn serialize_struct(self, _name: &'static str, _len: usize) -> Result<SerStruct, SerError> {
        Ok(SerStruct {
            fields: OrderedMap::new(),
        })
    }
    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        _len: usize,
    ) -> Result<SerStructVariant, SerError> {
        Ok(SerStructVariant {
            tag: variant.to_string(),
            fields: OrderedMap::new(),
        })
    }
}

/// Collects a sequence (and tuple/tuple-struct/tuple-variant) into a `Value::List`.
struct SerSeq {
    items: Vec<Value>,
    /// Set for tuple variants so `end` wraps the list in a tagged struct.
    tag: Option<String>,
}

impl SerializeSeq for SerSeq {
    type Ok = Value;
    type Error = SerError;
    fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), SerError> {
        self.items.push(value.serialize(ValueSerializer)?);
        Ok(())
    }
    fn end(self) -> Result<Value, SerError> {
        Ok(wrap_variant(self.tag, Value::List(self.items)))
    }
}

impl SerializeTuple for SerSeq {
    type Ok = Value;
    type Error = SerError;
    fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), SerError> {
        self.items.push(value.serialize(ValueSerializer)?);
        Ok(())
    }
    fn end(self) -> Result<Value, SerError> {
        Ok(wrap_variant(self.tag, Value::List(self.items)))
    }
}

impl ser::SerializeTupleStruct for SerSeq {
    type Ok = Value;
    type Error = SerError;
    fn serialize_field<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), SerError> {
        self.items.push(value.serialize(ValueSerializer)?);
        Ok(())
    }
    fn end(self) -> Result<Value, SerError> {
        Ok(wrap_variant(self.tag, Value::List(self.items)))
    }
}

impl ser::SerializeTupleVariant for SerSeq {
    type Ok = Value;
    type Error = SerError;
    fn serialize_field<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), SerError> {
        self.items.push(value.serialize(ValueSerializer)?);
        Ok(())
    }
    fn end(self) -> Result<Value, SerError> {
        Ok(wrap_variant(self.tag, Value::List(self.items)))
    }
}

/// Collects a map into a `Value::Struct` (string field names only).
struct SerMap {
    fields: OrderedMap<String, Value>,
    next_key: Option<String>,
}

impl SerializeMap for SerMap {
    type Ok = Value;
    type Error = SerError;
    fn serialize_key<T: ?Sized + Serialize>(&mut self, key: &T) -> Result<(), SerError> {
        let key = key.serialize(MapKeySerializer)?;
        self.next_key = Some(key);
        Ok(())
    }
    fn serialize_value<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), SerError> {
        let key = self
            .next_key
            .take()
            .ok_or_else(|| SerError("map value serialized without a key".into()))?;
        let value = value.serialize(ValueSerializer)?;
        self.fields.insert(key, value);
        Ok(())
    }
    fn end(self) -> Result<Value, SerError> {
        Ok(Value::Struct(self.fields))
    }
}

/// Collects a struct into a `Value::Struct`.
struct SerStruct {
    fields: OrderedMap<String, Value>,
}

impl SerializeStruct for SerStruct {
    type Ok = Value;
    type Error = SerError;
    fn serialize_field<T: ?Sized + Serialize>(&mut self, key: &'static str, value: &T) -> Result<(), SerError> {
        let value = value.serialize(ValueSerializer)?;
        self.fields.insert(key.to_string(), value);
        Ok(())
    }
    fn end(self) -> Result<Value, SerError> {
        Ok(Value::Struct(self.fields))
    }
    fn skip_field(&mut self, _key: &'static str) -> Result<(), SerError> {
        Ok(())
    }
}

/// Collects a struct variant into a single-field tagged `Value::Struct`.
struct SerStructVariant {
    tag: String,
    fields: OrderedMap<String, Value>,
}

impl ser::SerializeStructVariant for SerStructVariant {
    type Ok = Value;
    type Error = SerError;
    fn serialize_field<T: ?Sized + Serialize>(&mut self, key: &'static str, value: &T) -> Result<(), SerError> {
        let value = value.serialize(ValueSerializer)?;
        self.fields.insert(key.to_string(), value);
        Ok(())
    }
    fn end(self) -> Result<Value, SerError> {
        Ok(wrap_variant(Some(self.tag), Value::Struct(self.fields)))
    }
}

fn wrap_variant(tag: Option<String>, value: Value) -> Value {
    match tag {
        Some(variant) => {
            let mut fields = OrderedMap::new();
            fields.insert(variant, value);
            Value::Struct(fields)
        }
        None => value,
    }
}

/// Serializer restricted to scalar string-like keys. DuckDB `STRUCT` field
/// names are strings, so non-string map keys are rejected with a clear error.
struct MapKeySerializer;

impl Serializer for MapKeySerializer {
    type Ok = String;
    type Error = SerError;
    type SerializeSeq = ser::Impossible<String, SerError>;
    type SerializeTuple = ser::Impossible<String, SerError>;
    type SerializeTupleStruct = ser::Impossible<String, SerError>;
    type SerializeTupleVariant = ser::Impossible<String, SerError>;
    type SerializeMap = ser::Impossible<String, SerError>;
    type SerializeStruct = ser::Impossible<String, SerError>;
    type SerializeStructVariant = ser::Impossible<String, SerError>;

    fn serialize_str(self, v: &str) -> Result<String, SerError> {
        Ok(v.to_string())
    }
    fn serialize_char(self, v: char) -> Result<String, SerError> {
        Ok(v.to_string())
    }
    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
    ) -> Result<String, SerError> {
        Ok(variant.to_string())
    }
    fn serialize_newtype_struct<T: ?Sized + Serialize>(
        self,
        _name: &'static str,
        value: &T,
    ) -> Result<String, SerError> {
        value.serialize(self)
    }
    fn serialize_newtype_variant<T: ?Sized + Serialize>(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        value: &T,
    ) -> Result<String, SerError> {
        value.serialize(self)
    }
    fn serialize_some<T: ?Sized + Serialize>(self, value: &T) -> Result<String, SerError> {
        value.serialize(self)
    }

    fn serialize_bool(self, v: bool) -> Result<String, SerError> {
        non_string_key(v)
    }
    fn serialize_i8(self, v: i8) -> Result<String, SerError> {
        non_string_key(v)
    }
    fn serialize_i16(self, v: i16) -> Result<String, SerError> {
        non_string_key(v)
    }
    fn serialize_i32(self, v: i32) -> Result<String, SerError> {
        non_string_key(v)
    }
    fn serialize_i64(self, v: i64) -> Result<String, SerError> {
        non_string_key(v)
    }
    fn serialize_i128(self, v: i128) -> Result<String, SerError> {
        non_string_key(v)
    }
    fn serialize_u8(self, v: u8) -> Result<String, SerError> {
        non_string_key(v)
    }
    fn serialize_u16(self, v: u16) -> Result<String, SerError> {
        non_string_key(v)
    }
    fn serialize_u32(self, v: u32) -> Result<String, SerError> {
        non_string_key(v)
    }
    fn serialize_u64(self, v: u64) -> Result<String, SerError> {
        non_string_key(v)
    }
    fn serialize_u128(self, v: u128) -> Result<String, SerError> {
        non_string_key(v)
    }
    fn serialize_f32(self, v: f32) -> Result<String, SerError> {
        non_string_key(v)
    }
    fn serialize_f64(self, v: f64) -> Result<String, SerError> {
        non_string_key(v)
    }
    fn serialize_bytes(self, v: &[u8]) -> Result<String, SerError> {
        non_string_key(format_args!("{v:?}"))
    }
    fn serialize_none(self) -> Result<String, SerError> {
        non_string_key("None")
    }
    fn serialize_unit(self) -> Result<String, SerError> {
        non_string_key("()")
    }
    fn serialize_unit_struct(self, name: &'static str) -> Result<String, SerError> {
        non_string_key(name)
    }
    fn serialize_seq(self, _len: Option<usize>) -> Result<ser::Impossible<String, SerError>, SerError> {
        Err(SerError("DuckDB STRUCT field names must be strings".into()))
    }
    fn serialize_tuple(self, _len: usize) -> Result<ser::Impossible<String, SerError>, SerError> {
        Err(SerError("DuckDB STRUCT field names must be strings".into()))
    }
    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<ser::Impossible<String, SerError>, SerError> {
        Err(SerError("DuckDB STRUCT field names must be strings".into()))
    }
    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<ser::Impossible<String, SerError>, SerError> {
        Err(SerError("DuckDB STRUCT field names must be strings".into()))
    }
    fn serialize_map(self, _len: Option<usize>) -> Result<ser::Impossible<String, SerError>, SerError> {
        Err(SerError("DuckDB STRUCT field names must be strings".into()))
    }
    fn serialize_struct(self, _name: &'static str, _len: usize) -> Result<ser::Impossible<String, SerError>, SerError> {
        Err(SerError("DuckDB STRUCT field names must be strings".into()))
    }
    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<ser::Impossible<String, SerError>, SerError> {
        Err(SerError("DuckDB STRUCT field names must be strings".into()))
    }
}

fn non_string_key<T: fmt::Display>(v: T) -> Result<String, SerError> {
    Err(SerError(format!("DuckDB STRUCT field names must be strings, got {v}")))
}

// ---------------------------------------------------------------------------
// Deserializer: DuckDB Value -> Rust value
// ---------------------------------------------------------------------------

struct ValueDeserializer {
    value: Value,
}

impl ValueDeserializer {
    fn new(value: Value) -> Self {
        Self { value }
    }

    fn as_i64(&self) -> Result<i64, SerError> {
        match &self.value {
            Value::TinyInt(i) => Ok(*i as i64),
            Value::SmallInt(i) => Ok(*i as i64),
            Value::Int(i) => Ok(*i as i64),
            Value::BigInt(i) => Ok(*i),
            Value::UTinyInt(i) => Ok(*i as i64),
            Value::USmallInt(i) => Ok(*i as i64),
            Value::UInt(i) => Ok(*i as i64),
            Value::UBigInt(i) => i64::try_from(*i).map_err(|_| SerError(format!("value {i} out of range for i64"))),
            other => Err(SerError(format!("expected an integer, got {}", other.type_label()))),
        }
    }

    fn as_u64(&self) -> Result<u64, SerError> {
        match &self.value {
            Value::UTinyInt(i) => Ok(*i as u64),
            Value::USmallInt(i) => Ok(*i as u64),
            Value::UInt(i) => Ok(*i as u64),
            Value::UBigInt(i) => Ok(*i),
            Value::TinyInt(i) if *i >= 0 => Ok(*i as u64),
            Value::SmallInt(i) if *i >= 0 => Ok(*i as u64),
            Value::Int(i) if *i >= 0 => Ok(*i as u64),
            Value::BigInt(i) if *i >= 0 => Ok(*i as u64),
            other => Err(SerError(format!(
                "expected an unsigned integer, got {}",
                other.type_label()
            ))),
        }
    }

    fn as_i128(&self) -> Result<i128, SerError> {
        match &self.value {
            Value::HugeInt(i) => Ok(*i),
            Value::UHugeInt(i) => i128::try_from(*i).map_err(|_| SerError(format!("value {i} out of range for i128"))),
            other => self
                .as_i64()
                .map(|v| v as i128)
                .map_err(|_| SerError(format!("expected an integer, got {}", other.type_label()))),
        }
    }

    fn as_f64(&self) -> Result<f64, SerError> {
        match &self.value {
            Value::Float(f) => Ok(*f as f64),
            Value::Double(d) => Ok(*d),
            other => self
                .as_i64()
                .map(|v| v as f64)
                .map_err(|_| SerError(format!("expected a number, got {}", other.type_label()))),
        }
    }
}

impl Value {
    fn type_label(&self) -> &'static str {
        match self {
            Value::Null => "Null",
            Value::Boolean(_) => "Boolean",
            Value::TinyInt(_) => "TinyInt",
            Value::SmallInt(_) => "SmallInt",
            Value::Int(_) => "Int",
            Value::BigInt(_) => "BigInt",
            Value::HugeInt(_) => "HugeInt",
            Value::UHugeInt(_) => "UHugeInt",
            Value::UTinyInt(_) => "UTinyInt",
            Value::USmallInt(_) => "USmallInt",
            Value::UInt(_) => "UInt",
            Value::UBigInt(_) => "UBigInt",
            Value::Float(_) => "Float",
            Value::Double(_) => "Double",
            Value::Decimal(_) => "Decimal",
            Value::Timestamp(_, _) => "Timestamp",
            Value::Text(_) => "Text",
            Value::Blob(_) => "Blob",
            Value::Geometry(_) => "Geometry",
            Value::Date32(_) => "Date32",
            Value::Time64(_, _) => "Time64",
            Value::Interval { .. } => "Interval",
            Value::List(_) => "List",
            Value::Enum(_) => "Enum",
            Value::Struct(_) => "Struct",
            Value::Array(_) => "Array",
            Value::Map(_) => "Map",
            Value::Union(_) => "Union",
        }
    }
}

impl<'de> de::Deserializer<'de> for ValueDeserializer {
    type Error = SerError;

    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, SerError> {
        match self.value {
            Value::Null => visitor.visit_unit(),
            Value::Boolean(b) => visitor.visit_bool(b),
            Value::TinyInt(i) => visitor.visit_i64(i as i64),
            Value::SmallInt(i) => visitor.visit_i64(i as i64),
            Value::Int(i) => visitor.visit_i64(i as i64),
            Value::BigInt(i) => visitor.visit_i64(i),
            Value::UTinyInt(i) => visitor.visit_u64(i as u64),
            Value::USmallInt(i) => visitor.visit_u64(i as u64),
            Value::UInt(i) => visitor.visit_u64(i as u64),
            Value::UBigInt(i) => visitor.visit_u64(i),
            Value::HugeInt(i) => visitor.visit_i128(i),
            Value::UHugeInt(i) => visitor.visit_u128(i),
            Value::Float(f) => visitor.visit_f64(f as f64),
            Value::Double(d) => visitor.visit_f64(d),
            Value::Text(s) => visitor.visit_string(s),
            Value::Blob(b) => visitor.visit_byte_buf(b),
            Value::Geometry(b) => visitor.visit_bytes(&b),
            Value::Decimal(d) => visitor.visit_string(d.to_string()),
            Value::Timestamp(_, i) => visitor.visit_i64(i),
            Value::Date32(d) => visitor.visit_i64(d as i64),
            Value::Time64(_, i) => visitor.visit_i64(i),
            Value::Interval { months, days, nanos } => {
                let mut fields = OrderedMap::new();
                fields.insert("months".to_string(), Value::Int(months));
                fields.insert("days".to_string(), Value::Int(days));
                fields.insert("nanos".to_string(), Value::BigInt(nanos));
                visitor.visit_map(MapDeserializer {
                    entries: struct_entries(fields),
                    next_value: None,
                })
            }
            Value::List(items) | Value::Array(items) => visitor.visit_seq(SeqDeserializer {
                items: items.into_iter(),
            }),
            Value::Struct(fields) => visitor.visit_map(MapDeserializer {
                entries: struct_entries(fields),
                next_value: None,
            }),
            Value::Map(entries) => visitor.visit_map(MapDeserializer {
                entries: entries.0.into_iter(),
                next_value: None,
            }),
            Value::Enum(s) => visitor.visit_string(s),
            Value::Union(_) => Err(SerError(
                "Union values cannot be deserialized into a typed struct".into(),
            )),
        }
    }

    fn deserialize_bool<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, SerError> {
        match self.value {
            Value::Boolean(b) => visitor.visit_bool(b),
            _ => visitor.visit_bool(
                self.as_i64()
                    .map(|i| i != 0)
                    .map_err(|_| SerError(format!("expected bool, got {}", self.value.type_label())))?,
            ),
        }
    }

    fn deserialize_i8<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, SerError> {
        visitor.visit_i64(self.as_i64()?)
    }
    fn deserialize_i16<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, SerError> {
        visitor.visit_i64(self.as_i64()?)
    }
    fn deserialize_i32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, SerError> {
        visitor.visit_i64(self.as_i64()?)
    }
    fn deserialize_i64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, SerError> {
        visitor.visit_i64(self.as_i64()?)
    }
    fn deserialize_i128<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, SerError> {
        visitor.visit_i128(self.as_i128()?)
    }
    fn deserialize_u8<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, SerError> {
        visitor.visit_u64(self.as_u64()?)
    }
    fn deserialize_u16<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, SerError> {
        visitor.visit_u64(self.as_u64()?)
    }
    fn deserialize_u32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, SerError> {
        visitor.visit_u64(self.as_u64()?)
    }
    fn deserialize_u64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, SerError> {
        visitor.visit_u64(self.as_u64()?)
    }
    fn deserialize_u128<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, SerError> {
        visitor.visit_u128(match self.value {
            Value::UHugeInt(i) => i,
            _ => self.as_u64()? as u128,
        })
    }
    fn deserialize_f32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, SerError> {
        visitor.visit_f64(self.as_f64()?)
    }
    fn deserialize_f64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, SerError> {
        visitor.visit_f64(self.as_f64()?)
    }
    fn deserialize_char<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, SerError> {
        self.deserialize_str(visitor)
    }
    fn deserialize_str<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, SerError> {
        match self.value {
            Value::Text(s) => visitor.visit_string(s),
            Value::Enum(s) => visitor.visit_string(s),
            other => Err(SerError(format!("expected a string, got {}", other.type_label()))),
        }
    }
    fn deserialize_string<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, SerError> {
        self.deserialize_str(visitor)
    }
    fn deserialize_bytes<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, SerError> {
        match self.value {
            Value::Blob(b) => visitor.visit_byte_buf(b),
            Value::Geometry(b) => visitor.visit_byte_buf(b),
            other => Err(SerError(format!("expected bytes, got {}", other.type_label()))),
        }
    }
    fn deserialize_byte_buf<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, SerError> {
        self.deserialize_bytes(visitor)
    }
    fn deserialize_option<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, SerError> {
        match self.value {
            Value::Null => visitor.visit_none(),
            _ => visitor.visit_some(self),
        }
    }
    fn deserialize_unit<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, SerError> {
        match self.value {
            Value::Null => visitor.visit_unit(),
            other => Err(SerError(format!("expected unit, got {}", other.type_label()))),
        }
    }
    fn deserialize_unit_struct<V: Visitor<'de>>(self, _name: &'static str, visitor: V) -> Result<V::Value, SerError> {
        visitor.visit_unit()
    }
    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, SerError> {
        visitor.visit_newtype_struct(self)
    }
    fn deserialize_seq<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, SerError> {
        match self.value {
            Value::List(items) | Value::Array(items) => visitor.visit_seq(SeqDeserializer {
                items: items.into_iter(),
            }),
            Value::Null => visitor.visit_seq(SeqDeserializer {
                items: Vec::new().into_iter(),
            }),
            other => Err(SerError(format!("expected a sequence, got {}", other.type_label()))),
        }
    }
    fn deserialize_tuple<V: Visitor<'de>>(self, _len: usize, visitor: V) -> Result<V::Value, SerError> {
        self.deserialize_seq(visitor)
    }
    fn deserialize_tuple_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _len: usize,
        visitor: V,
    ) -> Result<V::Value, SerError> {
        self.deserialize_seq(visitor)
    }
    fn deserialize_map<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, SerError> {
        match self.value {
            Value::Struct(fields) => visitor.visit_map(MapDeserializer {
                entries: struct_entries(fields),
                next_value: None,
            }),
            Value::Map(fields) => visitor.visit_map(MapDeserializer {
                entries: fields.0.into_iter(),
                next_value: None,
            }),
            other => Err(SerError(format!("expected a struct/map, got {}", other.type_label()))),
        }
    }
    fn deserialize_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, SerError> {
        self.deserialize_map(visitor)
    }
    fn deserialize_enum<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, SerError> {
        match self.value {
            Value::Text(s) => visitor.visit_enum(UnitEnumAccess { name: s }),
            Value::Enum(s) => visitor.visit_enum(UnitEnumAccess { name: s }),
            Value::Struct(mut fields) if fields.len() == 1 => {
                let (variant, inner) = fields.0.swap_remove(0);
                visitor.visit_enum(TaggedEnumAccess { variant, inner })
            }
            other => Err(SerError(format!("expected an enum, got {}", other.type_label()))),
        }
    }
    fn deserialize_identifier<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, SerError> {
        self.deserialize_str(visitor)
    }
    fn deserialize_ignored_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, SerError> {
        visitor.visit_unit()
    }
}

struct SeqDeserializer {
    items: std::vec::IntoIter<Value>,
}

impl<'de> SeqAccess<'de> for SeqDeserializer {
    type Error = SerError;
    fn next_element_seed<T: DeserializeSeed<'de>>(&mut self, seed: T) -> Result<Option<T::Value>, SerError> {
        match self.items.next() {
            Some(value) => Ok(Some(seed.deserialize(ValueDeserializer::new(value))?)),
            None => Ok(None),
        }
    }
    fn size_hint(&self) -> Option<usize> {
        Some(self.items.len())
    }
}

struct MapDeserializer {
    entries: std::vec::IntoIter<(Value, Value)>,
    next_value: Option<Value>,
}

/// Wraps `STRUCT` string field names as `Value::Text` so the same `(Value,
/// Value)` entry stream serves both STRUCT and MAP deserialization.
fn struct_entries(fields: OrderedMap<String, Value>) -> std::vec::IntoIter<(Value, Value)> {
    fields
        .0
        .into_iter()
        .map(|(k, v)| (Value::Text(k), v))
        .collect::<Vec<_>>()
        .into_iter()
}

impl<'de> MapAccess<'de> for MapDeserializer {
    type Error = SerError;
    fn next_key_seed<K: DeserializeSeed<'de>>(&mut self, seed: K) -> Result<Option<K::Value>, SerError> {
        match self.entries.next() {
            Some((key, value)) => {
                self.next_value = Some(value);
                Ok(Some(seed.deserialize(ValueDeserializer::new(key))?))
            }
            None => Ok(None),
        }
    }
    fn next_value_seed<V: DeserializeSeed<'de>>(&mut self, seed: V) -> Result<V::Value, SerError> {
        let value = self
            .next_value
            .take()
            .ok_or_else(|| SerError("struct field value missing its key".into()))?;
        seed.deserialize(ValueDeserializer::new(value))
    }
    fn size_hint(&self) -> Option<usize> {
        Some(self.entries.len())
    }
}

struct UnitEnumAccess {
    name: String,
}

impl<'de> EnumAccess<'de> for UnitEnumAccess {
    type Error = SerError;
    type Variant = UnitVariantAccess;
    fn variant_seed<V: DeserializeSeed<'de>>(self, seed: V) -> Result<(V::Value, Self::Variant), SerError> {
        let value = seed.deserialize(StrDeserializer(&self.name))?;
        Ok((value, UnitVariantAccess))
    }
}

struct TaggedEnumAccess {
    variant: String,
    inner: Value,
}

impl<'de> EnumAccess<'de> for TaggedEnumAccess {
    type Error = SerError;
    type Variant = ValueVariantAccess;
    fn variant_seed<V: DeserializeSeed<'de>>(self, seed: V) -> Result<(V::Value, Self::Variant), SerError> {
        let value = seed.deserialize(StrDeserializer(&self.variant))?;
        Ok((
            value,
            ValueVariantAccess {
                inner: Some(self.inner),
            },
        ))
    }
}

struct UnitVariantAccess;

impl<'de> VariantAccess<'de> for UnitVariantAccess {
    type Error = SerError;
    fn unit_variant(self) -> Result<(), SerError> {
        Ok(())
    }
    fn newtype_variant_seed<T: DeserializeSeed<'de>>(self, _seed: T) -> Result<T::Value, SerError> {
        Err(SerError(
            "expected a unit enum variant, got newtype variant data".into(),
        ))
    }
    fn tuple_variant<V: Visitor<'de>>(self, _len: usize, _visitor: V) -> Result<V::Value, SerError> {
        Err(SerError("expected a unit enum variant, got tuple variant data".into()))
    }
    fn struct_variant<V: Visitor<'de>>(
        self,
        _fields: &'static [&'static str],
        _visitor: V,
    ) -> Result<V::Value, SerError> {
        Err(SerError("expected a unit enum variant, got struct variant data".into()))
    }
}

struct ValueVariantAccess {
    inner: Option<Value>,
}

impl<'de> VariantAccess<'de> for ValueVariantAccess {
    type Error = SerError;
    fn unit_variant(self) -> Result<(), SerError> {
        Ok(())
    }
    fn newtype_variant_seed<T: DeserializeSeed<'de>>(mut self, seed: T) -> Result<T::Value, SerError> {
        let inner = self
            .inner
            .take()
            .ok_or_else(|| SerError("enum variant already consumed".into()))?;
        seed.deserialize(ValueDeserializer::new(inner))
    }
    fn tuple_variant<V: Visitor<'de>>(self, _len: usize, visitor: V) -> Result<V::Value, SerError> {
        let inner = self
            .inner
            .ok_or_else(|| SerError("enum variant already consumed".into()))?;
        ValueDeserializer::new(inner).deserialize_seq(visitor)
    }
    fn struct_variant<V: Visitor<'de>>(
        self,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, SerError> {
        let inner = self
            .inner
            .ok_or_else(|| SerError("enum variant already consumed".into()))?;
        ValueDeserializer::new(inner).deserialize_map(visitor)
    }
}

/// Deserializer that always yields a borrowed string. Used for struct/enum
/// identifiers read back from a DuckDB `STRUCT` field name.
struct StrDeserializer<'a>(&'a str);

impl<'de> de::Deserializer<'de> for StrDeserializer<'_> {
    type Error = SerError;

    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, SerError> {
        visitor.visit_str(self.0)
    }

    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
        bytes byte_buf option unit unit_struct newtype_struct seq tuple
        tuple_struct map struct enum identifier ignored_any
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Connection, Result, types::Value};
    use serde::{Deserialize, Serialize};
    use std::collections::HashMap;

    #[derive(Serialize, Deserialize, PartialEq, Debug)]
    struct Point {
        x: i32,
        y: f64,
    }

    #[derive(Serialize, Deserialize, PartialEq, Debug)]
    struct Nested {
        name: String,
        point: Point,
        tags: Vec<String>,
        scores: Vec<f64>,
        active: bool,
        maybe: Option<f32>,
        meta: HashMap<String, i64>,
        count: u32,
        big: i128,
    }

    #[test]
    fn serializer_maps_struct_to_value_struct() {
        let p = Point { x: 3, y: 4.5 };
        let wrapper = Struct(&p);
        let value = wrapper.to_sql().unwrap();
        let owned = match value {
            ToSqlOutput::Owned(v) => v,
            other => panic!("expected Owned, got {other:?}"),
        };
        match owned {
            Value::Struct(fields) => {
                assert_eq!(fields.get(&"x".to_string()), Some(&Value::Int(3)));
                match fields.get(&"y".to_string()) {
                    Some(Value::Double(d)) => assert!((d - 4.5).abs() < 1e-9),
                    other => panic!("unexpected y: {other:?}"),
                }
            }
            other => panic!("expected Value::Struct, got {other:?}"),
        }
    }

    #[test]
    fn serializer_rejects_non_string_map_key() {
        let mut bad: HashMap<i32, i32> = HashMap::new();
        bad.insert(1, 2);
        let err = Struct(&bad).to_sql().unwrap_err();
        assert!(err.to_string().contains("STRUCT field names must be strings"), "{err}");
    }

    #[test]
    fn round_trip_nested_struct() -> Result<()> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch(
            "CREATE TABLE t (row STRUCT(
                name VARCHAR,
                point STRUCT(x INTEGER, y DOUBLE),
                tags VARCHAR[],
                scores DOUBLE[],
                active BOOLEAN,
                maybe FLOAT,
                meta MAP(VARCHAR, BIGINT),
                count UINTEGER,
                big HUGEINT
            ));",
        )?;

        let mut meta = HashMap::new();
        meta.insert("a".to_string(), 1_i64);
        meta.insert("b".to_string(), 2_i64);
        let row = Nested {
            name: "hi".to_string(),
            point: Point { x: 3, y: 4.5 },
            tags: vec!["t1".to_string(), "t2".to_string()],
            scores: vec![1.0, 2.5],
            active: true,
            maybe: Some(9.5),
            meta,
            count: 42,
            big: 1_000_000_000_000,
        };

        conn.execute("INSERT INTO t VALUES (?)", [Struct(&row)])?;
        let back: Struct<Nested> = conn.query_row("SELECT row FROM t", [], |r| r.get(0))?;
        assert_eq!(back.0, row);
        Ok(())
    }

    #[test]
    fn round_trip_unit_enum() -> Result<()> {
        #[derive(Serialize, Deserialize, PartialEq, Debug)]
        struct Item {
            color: Color,
        }
        #[derive(Serialize, Deserialize, PartialEq, Debug)]
        #[allow(dead_code)]
        enum Color {
            Red,
            Green,
            Blue,
        }

        let conn = Connection::open_in_memory()?;
        conn.execute_batch("CREATE TABLE t (row STRUCT(color VARCHAR));")?;
        let item = Item { color: Color::Green };
        conn.execute("INSERT INTO t VALUES (?)", [Struct(&item)])?;
        let back: Struct<Item> = conn.query_row("SELECT row FROM t", [], |r| r.get(0))?;
        assert_eq!(back.0, item);
        Ok(())
    }

    #[test]
    fn round_trip_none_option() -> Result<()> {
        #[derive(Serialize, Deserialize, PartialEq, Debug)]
        struct Row {
            maybe: Option<f32>,
        }
        let conn = Connection::open_in_memory()?;
        conn.execute_batch("CREATE TABLE t (row STRUCT(maybe FLOAT));")?;
        let row = Row { maybe: None };
        conn.execute("INSERT INTO t VALUES (?)", [Struct(&row)])?;
        let back: Struct<Row> = conn.query_row("SELECT row FROM t", [], |r| r.get(0))?;
        assert_eq!(back.0, row);
        Ok(())
    }

    #[test]
    fn from_sql_rejects_type_mismatch() -> Result<()> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch("CREATE TABLE t (v INTEGER); INSERT INTO t VALUES (1);")?;
        let err = conn
            .query_row::<Struct<Point>, _, _>("SELECT v FROM t", [], |r| r.get(0))
            .unwrap_err();
        assert!(err.to_string().contains("expected a struct/map"), "{err}");
        Ok(())
    }
}

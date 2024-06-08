use super::{Null, OrderedMap, TimeUnit, Type};
use rust_decimal::prelude::*;

/// Owning [dynamic type value](http://sqlite.org/datatype3.html). Value's type is typically
/// dictated by DuckDB (not by the caller).
///
/// See [`ValueRef`](crate::types::ValueRef) for a non-owning dynamic type
/// value.
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    /// The value is a `NULL` value.
    Null,
    /// The value is a boolean.
    Boolean(bool),
    /// The value is a signed tiny integer.
    TinyInt(i8),
    /// The value is a signed small integer.
    SmallInt(i16),
    /// The value is a signed integer.
    Int(i32),
    /// The value is a signed big integer.
    BigInt(i64),
    /// The value is a signed huge integer.
    HugeInt(i128),
    /// The value is a unsigned tiny integer.
    UTinyInt(u8),
    /// The value is a unsigned small integer.
    USmallInt(u16),
    /// The value is a unsigned integer.
    UInt(u32),
    /// The value is a unsigned big integer.
    UBigInt(u64),
    /// The value is a f32.
    Float(f32),
    /// The value is a f64.
    Double(f64),
    /// The value is a Decimal.
    Decimal(Decimal),
    /// The value is a timestamp.
    Timestamp(TimeUnit, i64),
    /// The value is a text string.
    Text(String),
    /// The value is a blob of data
    Blob(Vec<u8>),
    /// The value is a date32
    Date32(i32),
    /// The value is a time64
    Time64(TimeUnit, i64),
    /// The value is an interval (month, day, nano)
    Interval {
        /// months
        months: i32,
        /// days
        days: i32,
        /// nanos
        nanos: i64,
    },
    /// The value is a list
    List(Vec<Value>),
    /// The value is an enum
    Enum(String),
    /// The value is a struct
    Struct(OrderedMap<String, Value>),
    /// The value is an array
    Array(Vec<Value>),
    /// The value is a map
    Map(OrderedMap<Value, Value>),
    /// The value is a union
    Union(Box<Value>),
}

impl From<Null> for Value {
    #[inline]
    fn from(_: Null) -> Value {
        Value::Null
    }
}

impl From<bool> for Value {
    #[inline]
    fn from(i: bool) -> Value {
        Value::Boolean(i)
    }
}

impl From<usize> for Value {
    #[inline]
    fn from(i: usize) -> Value {
        Value::UBigInt(i as u64)
    }
}

impl From<isize> for Value {
    #[inline]
    fn from(i: isize) -> Value {
        Value::BigInt(i as i64)
    }
}

#[cfg(feature = "uuid")]
impl From<uuid::Uuid> for Value {
    #[inline]
    fn from(id: uuid::Uuid) -> Value {
        Value::Text(id.to_string())
    }
}

impl From<i8> for Value {
    #[inline]
    fn from(i: i8) -> Value {
        Value::TinyInt(i)
    }
}

impl From<i16> for Value {
    #[inline]
    fn from(i: i16) -> Value {
        Value::SmallInt(i)
    }
}

impl From<i32> for Value {
    #[inline]
    fn from(i: i32) -> Value {
        Value::Int(i)
    }
}

impl From<i64> for Value {
    #[inline]
    fn from(i: i64) -> Value {
        Value::BigInt(i)
    }
}

impl From<u8> for Value {
    #[inline]
    fn from(i: u8) -> Value {
        Value::UTinyInt(i)
    }
}

impl From<u16> for Value {
    #[inline]
    fn from(i: u16) -> Value {
        Value::USmallInt(i)
    }
}

impl From<u32> for Value {
    #[inline]
    fn from(i: u32) -> Value {
        Value::UInt(i)
    }
}

impl From<u64> for Value {
    #[inline]
    fn from(i: u64) -> Value {
        Value::UBigInt(i)
    }
}

impl From<i128> for Value {
    #[inline]
    fn from(i: i128) -> Value {
        Value::HugeInt(i)
    }
}

impl From<f32> for Value {
    #[inline]
    fn from(f: f32) -> Value {
        Value::Float(f)
    }
}

impl From<f64> for Value {
    #[inline]
    fn from(f: f64) -> Value {
        Value::Double(f)
    }
}

impl From<String> for Value {
    #[inline]
    fn from(s: String) -> Value {
        Value::Text(s)
    }
}

impl From<Vec<u8>> for Value {
    #[inline]
    fn from(v: Vec<u8>) -> Value {
        Value::Blob(v)
    }
}

impl<T> From<Option<T>> for Value
where
    T: Into<Value>,
{
    #[inline]
    fn from(v: Option<T>) -> Value {
        match v {
            Some(x) => x.into(),
            None => Value::Null,
        }
    }
}

impl Value {
    /// Returns DuckDB fundamental datatype.
    #[inline]
    pub fn data_type(&self) -> Type {
        match *self {
            Value::Null => Type::Null,
            Value::Boolean(_) => Type::Boolean,
            Value::TinyInt(_) => Type::TinyInt,
            Value::SmallInt(_) => Type::SmallInt,
            Value::Int(_) => Type::Int,
            Value::BigInt(_) => Type::BigInt,
            Value::HugeInt(_) => Type::HugeInt,
            Value::UTinyInt(_) => Type::UTinyInt,
            Value::USmallInt(_) => Type::USmallInt,
            Value::UInt(_) => Type::UInt,
            Value::UBigInt(_) => Type::UBigInt,
            Value::Float(_) => Type::Float,
            Value::Double(_) => Type::Double,
            Value::Decimal(_) => Type::Decimal,
            Value::Timestamp(_, _) => Type::Timestamp,
            Value::Text(_) => Type::Text,
            Value::Blob(_) => Type::Blob,
            Value::Date32(_) => Type::Date32,
            Value::Time64(..) => Type::Time64,
            Value::Interval { .. } => Type::Interval,
            Value::Union(..) | Value::Struct(..) | Value::List(..) | Value::Array(..) | Value::Map(..) => todo!(),
            Value::Enum(..) => Type::Enum,
        }
    }
}

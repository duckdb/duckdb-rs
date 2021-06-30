use super::{Null, Type};

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
    /// The value is a f32.
    Float(f32),
    /// The value is a f64.
    Double(f64),
    /// The value is a timestap.
    Timestamp(String),
    /// The value is a text string.
    Text(String),
    /// The value is a blob of data
    Blob(Vec<u8>),
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
        Value::Blob(id.as_bytes().to_vec())
    }
}

macro_rules! from_i128(
    ($t:ty) => (
        impl From<$t> for Value {
            #[inline]
            fn from(i: $t) -> Value {
                Value::HugeInt(i128::from(i))
            }
        }
    )
);

from_i128!(i8);
from_i128!(i16);
from_i128!(i32);
from_i128!(i64);
from_i128!(u8);
from_i128!(u16);
from_i128!(u32);

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
            Value::Float(_) => Type::Float,
            Value::Double(_) => Type::Double,
            Value::Timestamp(_) => Type::Timestamp,
            Value::Text(_) => Type::Text,
            Value::Blob(_) => Type::Blob,
        }
    }
}

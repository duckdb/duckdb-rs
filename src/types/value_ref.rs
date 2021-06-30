use super::{Type, Value};
use crate::types::{FromSqlError, FromSqlResult};

/// A non-owning [static type value](https://duckdb.org/docs/sql/data_types/overview). Typically the
/// memory backing this value is owned by SQLite.
///
/// See [`Value`](Value) for an owning dynamic type value.
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum ValueRef<'a> {
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
    Timestamp(&'a [u8]),
    /// The value is a text string.
    Text(&'a [u8]),
    /// The value is a blob of data
    Blob(&'a [u8]),
}

impl ValueRef<'_> {
    /// Returns DuckDB fundamental datatype.
    #[inline]
    pub fn data_type(&self) -> Type {
        match *self {
            ValueRef::Null => Type::Null,
            ValueRef::Boolean(_) => Type::Boolean,
            ValueRef::TinyInt(_) => Type::TinyInt,
            ValueRef::SmallInt(_) => Type::SmallInt,
            ValueRef::Int(_) => Type::Int,
            ValueRef::BigInt(_) => Type::BigInt,
            ValueRef::HugeInt(_) => Type::HugeInt,
            ValueRef::Float(_) => Type::Float,
            ValueRef::Double(_) => Type::Double,
            ValueRef::Timestamp(_) => Type::Timestamp,
            ValueRef::Text(_) => Type::Text,
            ValueRef::Blob(_) => Type::Blob,
        }
    }
}

impl<'a> ValueRef<'a> {
    /// If `self` is case `Integer`, returns the integral value. Otherwise,
    /// returns [`Err(Error::InvalidColumnType)`](crate::Error::
    /// InvalidColumnType).
    #[inline]
    pub fn as_i128(&self) -> FromSqlResult<i128> {
        match *self {
            ValueRef::TinyInt(i) => Ok(i as i128),
            ValueRef::SmallInt(i) => Ok(i as i128),
            ValueRef::Int(i) => Ok(i as i128),
            ValueRef::BigInt(i) => Ok(i as i128),
            ValueRef::HugeInt(i) => Ok(i),
            ValueRef::Text(a) => {
                let s = std::str::from_utf8(a).expect("invalid UTF-8");
                match s.parse::<i128>() {
                    Ok(i) => Ok(i),
                    // TODO(wangfenjin): update error type as parse error
                    _ => Err(FromSqlError::InvalidType),
                }
            }
            _ => Err(FromSqlError::InvalidType),
        }
    }

    /// If `self` is case `Real`, returns the floating point value. Otherwise,
    /// returns [`Err(Error::InvalidColumnType)`](crate::Error::
    /// InvalidColumnType).
    #[inline]
    pub fn as_f64(&self) -> FromSqlResult<f64> {
        match *self {
            ValueRef::Float(f) => Ok(f as f64),
            ValueRef::Double(f) => Ok(f),
            _ => Err(FromSqlError::InvalidType),
        }
    }

    /// If `self` is case `Text`, returns the string value. Otherwise, returns
    /// [`Err(Error::InvalidColumnType)`](crate::Error::InvalidColumnType).
    #[inline]
    pub fn as_str(&self) -> FromSqlResult<&'a str> {
        match *self {
            ValueRef::Text(t) => std::str::from_utf8(t).map_err(|e| FromSqlError::Other(Box::new(e))),
            _ => Err(FromSqlError::InvalidType),
        }
    }

    /// If `self` is case `Blob`, returns the byte slice. Otherwise, returns
    /// [`Err(Error::InvalidColumnType)`](crate::Error::InvalidColumnType).
    #[inline]
    pub fn as_blob(&self) -> FromSqlResult<&'a [u8]> {
        match *self {
            ValueRef::Blob(b) => Ok(b),
            _ => Err(FromSqlError::InvalidType),
        }
    }
}

impl From<ValueRef<'_>> for Value {
    #[inline]
    fn from(borrowed: ValueRef<'_>) -> Value {
        match borrowed {
            ValueRef::Null => Value::Null,
            ValueRef::Boolean(i) => Value::Boolean(i),
            ValueRef::TinyInt(i) => Value::TinyInt(i),
            ValueRef::SmallInt(i) => Value::SmallInt(i),
            ValueRef::Int(i) => Value::Int(i),
            ValueRef::BigInt(i) => Value::BigInt(i),
            ValueRef::HugeInt(i) => Value::HugeInt(i),
            ValueRef::Float(i) => Value::Float(i),
            ValueRef::Double(i) => Value::Double(i),
            ValueRef::Timestamp(t) => {
                let s = std::str::from_utf8(t).expect("invalid UTF-8");
                Value::Timestamp(s.to_string())
            }
            ValueRef::Text(s) => {
                let s = std::str::from_utf8(s).expect("invalid UTF-8");
                Value::Text(s.to_string())
            }
            ValueRef::Blob(b) => Value::Blob(b.to_vec()),
        }
    }
}

impl<'a> From<&'a str> for ValueRef<'a> {
    #[inline]
    fn from(s: &str) -> ValueRef<'_> {
        ValueRef::Text(s.as_bytes())
    }
}

impl<'a> From<&'a [u8]> for ValueRef<'a> {
    #[inline]
    fn from(s: &[u8]) -> ValueRef<'_> {
        ValueRef::Blob(s)
    }
}

impl<'a> From<&'a Value> for ValueRef<'a> {
    #[inline]
    fn from(value: &'a Value) -> ValueRef<'a> {
        match *value {
            Value::Null => ValueRef::Null,
            Value::Boolean(i) => ValueRef::Boolean(i),
            Value::TinyInt(i) => ValueRef::TinyInt(i),
            Value::SmallInt(i) => ValueRef::SmallInt(i),
            Value::Int(i) => ValueRef::Int(i),
            Value::BigInt(i) => ValueRef::BigInt(i),
            Value::HugeInt(i) => ValueRef::HugeInt(i),
            Value::Float(i) => ValueRef::Float(i),
            Value::Double(i) => ValueRef::Double(i),
            Value::Timestamp(ref t) => ValueRef::Timestamp(t.as_bytes()),
            Value::Text(ref s) => ValueRef::Text(s.as_bytes()),
            Value::Blob(ref b) => ValueRef::Blob(b),
        }
    }
}

impl<'a, T> From<Option<T>> for ValueRef<'a>
where
    T: Into<ValueRef<'a>>,
{
    #[inline]
    fn from(s: Option<T>) -> ValueRef<'a> {
        match s {
            Some(x) => x.into(),
            None => ValueRef::Null,
        }
    }
}

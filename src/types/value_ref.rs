use super::{Type, Value};
use crate::types::{FromSqlError, FromSqlResult};

use rust_decimal::prelude::*;

/// An absolute length of time in seconds, milliseconds, microseconds or nanoseconds.
/// Copy from arrow::datatypes::TimeUnit
#[derive(Copy, Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum TimeUnit {
    /// Time in seconds.
    Second,
    /// Time in milliseconds.
    Millisecond,
    /// Time in microseconds.
    Microsecond,
    /// Time in nanoseconds.
    Nanosecond,
}

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
    /// The value is a unsigned tiny integer.
    UTinyInt(u8),
    /// The value is a usigned small integer.
    USmallInt(u16),
    /// The value is a usigned integer.
    UInt(u32),
    /// The value is a usigned big integer.
    UBigInt(u64),
    /// The value is a f32.
    Float(f32),
    /// The value is a f64.
    Double(f64),
    /// The value is a decimal
    Decimal(Decimal),
    /// The value is a timestap.
    Timestamp(TimeUnit, i64),
    /// The value is a text string.
    Text(&'a [u8]),
    /// The value is a blob of data
    Blob(&'a [u8]),
    /// The value is a date32
    Date32(i32),
    /// The value is a time64
    Time64(TimeUnit, i64),
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
            ValueRef::UTinyInt(_) => Type::UTinyInt,
            ValueRef::USmallInt(_) => Type::USmallInt,
            ValueRef::UInt(_) => Type::UInt,
            ValueRef::UBigInt(_) => Type::UBigInt,
            ValueRef::Float(_) => Type::Float,
            ValueRef::Double(_) => Type::Double,
            ValueRef::Decimal(_) => Type::Decimal,
            ValueRef::Timestamp(..) => Type::Timestamp,
            ValueRef::Text(_) => Type::Text,
            ValueRef::Blob(_) => Type::Blob,
            ValueRef::Date32(_) => Type::Date32,
            ValueRef::Time64(..) => Type::Time64,
        }
    }
}

impl<'a> ValueRef<'a> {
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
            ValueRef::Text(t) => Ok(t),
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
            ValueRef::UTinyInt(i) => Value::UTinyInt(i),
            ValueRef::USmallInt(i) => Value::USmallInt(i),
            ValueRef::UInt(i) => Value::UInt(i),
            ValueRef::UBigInt(i) => Value::UBigInt(i),
            ValueRef::Float(i) => Value::Float(i),
            ValueRef::Double(i) => Value::Double(i),
            ValueRef::Decimal(i) => Value::Decimal(i),
            ValueRef::Timestamp(tu, t) => Value::Timestamp(tu, t),
            ValueRef::Text(s) => {
                let s = std::str::from_utf8(s).expect("invalid UTF-8");
                Value::Text(s.to_string())
            }
            ValueRef::Blob(b) => Value::Blob(b.to_vec()),
            ValueRef::Date32(d) => Value::Date32(d),
            ValueRef::Time64(t, d) => Value::Time64(t, d),
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
            Value::UTinyInt(i) => ValueRef::UTinyInt(i),
            Value::USmallInt(i) => ValueRef::USmallInt(i),
            Value::UInt(i) => ValueRef::UInt(i),
            Value::UBigInt(i) => ValueRef::UBigInt(i),
            Value::Float(i) => ValueRef::Float(i),
            Value::Double(i) => ValueRef::Double(i),
            Value::Decimal(i) => ValueRef::Decimal(i),
            Value::Timestamp(tu, t) => ValueRef::Timestamp(tu, t),
            Value::Text(ref s) => ValueRef::Text(s.as_bytes()),
            Value::Blob(ref b) => ValueRef::Blob(b),
            Value::Date32(d) => ValueRef::Date32(d),
            Value::Time64(t, d) => ValueRef::Time64(t, d),
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

use super::{Null, OrderedMap, TimeUnit, Type};
use rust_decimal::prelude::*;

/// Owning [dynamic type value](https://duckdb.org/docs/stable/sql/data_types/overview.html).
/// Value's type is typically dictated by DuckDB (not by the caller).
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
    ///
    /// The `i32` represents the number of days since the Unix epoch (1970-01-01).
    ///
    /// Enable the `chrono` feature for easy conversion to proper date types like `NaiveDate`.
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
    fn from(_: Null) -> Self {
        Self::Null
    }
}

impl From<bool> for Value {
    #[inline]
    fn from(i: bool) -> Self {
        Self::Boolean(i)
    }
}

impl From<usize> for Value {
    #[inline]
    fn from(i: usize) -> Self {
        Self::UBigInt(i as u64)
    }
}

impl From<isize> for Value {
    #[inline]
    fn from(i: isize) -> Self {
        Self::BigInt(i as i64)
    }
}

#[cfg(feature = "uuid")]
impl From<uuid::Uuid> for Value {
    #[inline]
    fn from(id: uuid::Uuid) -> Self {
        Self::Text(id.to_string())
    }
}

impl From<i8> for Value {
    #[inline]
    fn from(i: i8) -> Self {
        Self::TinyInt(i)
    }
}

impl From<i16> for Value {
    #[inline]
    fn from(i: i16) -> Self {
        Self::SmallInt(i)
    }
}

impl From<i32> for Value {
    #[inline]
    fn from(i: i32) -> Self {
        Self::Int(i)
    }
}

impl From<i64> for Value {
    #[inline]
    fn from(i: i64) -> Self {
        Self::BigInt(i)
    }
}

impl From<u8> for Value {
    #[inline]
    fn from(i: u8) -> Self {
        Self::UTinyInt(i)
    }
}

impl From<u16> for Value {
    #[inline]
    fn from(i: u16) -> Self {
        Self::USmallInt(i)
    }
}

impl From<u32> for Value {
    #[inline]
    fn from(i: u32) -> Self {
        Self::UInt(i)
    }
}

impl From<u64> for Value {
    #[inline]
    fn from(i: u64) -> Self {
        Self::UBigInt(i)
    }
}

impl From<i128> for Value {
    #[inline]
    fn from(i: i128) -> Self {
        Self::HugeInt(i)
    }
}

impl From<f32> for Value {
    #[inline]
    fn from(f: f32) -> Self {
        Self::Float(f)
    }
}

impl From<f64> for Value {
    #[inline]
    fn from(f: f64) -> Self {
        Self::Double(f)
    }
}

impl From<String> for Value {
    #[inline]
    fn from(s: String) -> Self {
        Self::Text(s)
    }
}

impl From<Vec<u8>> for Value {
    #[inline]
    fn from(v: Vec<u8>) -> Self {
        Self::Blob(v)
    }
}

impl<T> From<Option<T>> for Value
where
    T: Into<Self>,
{
    #[inline]
    fn from(v: Option<T>) -> Self {
        match v {
            Some(x) => x.into(),
            None => Self::Null,
        }
    }
}

impl Value {
    /// Returns DuckDB fundamental datatype.
    #[inline]
    pub fn data_type(&self) -> Type {
        match *self {
            Self::Null => Type::Null,
            Self::Boolean(_) => Type::Boolean,
            Self::TinyInt(_) => Type::TinyInt,
            Self::SmallInt(_) => Type::SmallInt,
            Self::Int(_) => Type::Int,
            Self::BigInt(_) => Type::BigInt,
            Self::HugeInt(_) => Type::HugeInt,
            Self::UTinyInt(_) => Type::UTinyInt,
            Self::USmallInt(_) => Type::USmallInt,
            Self::UInt(_) => Type::UInt,
            Self::UBigInt(_) => Type::UBigInt,
            Self::Float(_) => Type::Float,
            Self::Double(_) => Type::Double,
            Self::Decimal(_) => Type::Decimal,
            Self::Timestamp(_, _) => Type::Timestamp,
            Self::Text(_) => Type::Text,
            Self::Blob(_) => Type::Blob,
            Self::Date32(_) => Type::Date32,
            Self::Time64(..) => Type::Time64,
            Self::Interval { .. } => Type::Interval,
            Self::Union(..) | Self::Struct(..) | Self::List(..) | Self::Array(..) | Self::Map(..) => todo!(),
            Self::Enum(..) => Type::Enum,
        }
    }
}

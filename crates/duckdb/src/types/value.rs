
use super::{Null, OrderedMap, TimeUnit, Type};
use rust_decimal::prelude::*;

use serde::{Serialize, Serializer};

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
    ///
    /// rust_decimal crate.
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

impl Serialize for Value {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match *self {
            Value::Null => serializer.serialize_none(),
            Value::Boolean(b) => serializer.serialize_bool(b),
            Value::TinyInt(i) => serializer.serialize_i8(i),
            Value::SmallInt(i) => serializer.serialize_i16(i),
            Value::Int(i) => serializer.serialize_i32(i),
            Value::BigInt(i) => serializer.serialize_i64(i),
            Value::HugeInt(i) => serializer.serialize_i128(i),
            Value::UTinyInt(i) => serializer.serialize_u8(i),
            Value::USmallInt(i) => serializer.serialize_u16(i),
            Value::UInt(i) => serializer.serialize_u32(i),
            Value::UBigInt(i) => serializer.serialize_u64(i),
            Value::Float(f) => serializer.serialize_f32(f),
            Value::Double(d) => serializer.serialize_f64(d),
            Value::Decimal(d) => serializer.serialize_str(&d.to_string()),
            Value::Timestamp(t, i) => {
                let s = t.to_micros(i);
                serializer.serialize_i64(s)
            }
            Value::Text(ref s) => serializer.serialize_str(s),
            Value::Blob(ref v) => serializer.serialize_bytes(v),
            Value::Date32(i) => serializer.serialize_i32(i),
            Value::Time64(t, i) => {
                let s = t.to_micros(i);
                serializer.serialize_i64(s)
            }
            Value::Interval { months, days, nanos } => (months, days, nanos).serialize(serializer),
            Value::List(ref v) => v.serialize(serializer),
            Value::Enum(ref s) => serializer.serialize_str(s),
            Value::Struct(ref m) => m.serialize(serializer),
            Value::Array(ref v) => v.serialize(serializer),
            Value::Map(ref m) => m.serialize(serializer),
            Value::Union(ref v) => v.serialize(serializer),
        }
    }
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

impl Value {
    /// Consumes the Value and attempts to convert its inner data into the specified type T.
    ///
    /// This is a convenience method that combines `into_inner()` with a type-specific downcast.
    ///
    /// # Type Parameters
    /// * `T` - The target type to convert the inner value to. Must implement 'static.
    ///
    /// # Returns
    /// * `Some(T)` - If the Value contains data that can be converted to type T
    /// * `None` - If the Value is Null or if the inner data cannot be converted to type T
    ///
    /// # Example
    /// ```
    /// use duckdb::types::Value;
    ///
    /// let value = Value::Int(42);
    /// let int_value = value.into_inner_as::<i32>();
    /// assert_eq!(int_value, Some(42));
    /// ```
    ///
    /// # Panics
    /// This method panics if the inner value cannot be downcast to the specified type.
    ///
    /// # Safety
    /// This method is unsafe because it can panic if the inner value cannot be downcast to the specified type.
    /// It is the caller's responsibility to ensure that the inner value is of the correct type.
    ///
    /// # Returns
    /// Some containing the boxed inner value if the Value is not Null, otherwise None.
    ///
    #[inline]
    pub fn into_inner_as<T: 'static>(self) -> Option<T> {
        match self {
            Value::Boolean(v) => check_type(v),
            Value::TinyInt(v) => check_type(v),
            Value::SmallInt(v) => check_type(v),
            Value::Int(v) => check_type(v),
            Value::BigInt(v) => check_type(v),
            Value::HugeInt(v) => check_type(v),
            Value::UTinyInt(v) => check_type(v),
            Value::USmallInt(v) => check_type(v),
            Value::UInt(v) => check_type(v),
            Value::UBigInt(v) => check_type(v),
            Value::Float(v) => check_type(v),
            Value::Double(v) => check_type(v),
            Value::Text(v) => check_type(v),
            Value::Blob(v) => check_type(v),
            Value::Date32(v) => check_type(v),
            Value::Enum(v) => check_type(v),
            Value::Interval { months, days, nanos } => check_type((months, days, nanos)),
            Value::Decimal(v) => check_type(v),
            Value::Timestamp(unit, v) => check_type((unit, v)),
            Value::Time64(unit, v) => check_type((unit, v)),
            Value::Struct(v) => check_type(v),
            Value::Map(v) => check_type(v),
            Value::List(v) => check_type(v),
            Value::Array(v) => check_type(v),
            Value::Union(v) => check_type(v),
            Value::Null => None,
        }
    }
}

// 类型检查辅助函数
#[inline]
fn check_type<T: 'static, U: 'static>(value: U) -> Option<T> {
    if std::any::TypeId::of::<T>() == std::any::TypeId::of::<U>() {
        let mut value = std::mem::ManuallyDrop::new(value);
        Some(unsafe {
            let t_ptr = &mut *value as *mut U as *mut T;
            let t = std::ptr::read(t_ptr);
            t
        })
    } else {
        None
    }
}

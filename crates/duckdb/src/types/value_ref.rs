use super::{Type, Value};
use crate::types::{FromSqlError, FromSqlResult, OrderedMap};

use crate::Row;
use rust_decimal::prelude::*;

use arrow::{
    array::{
        Array, ArrayRef, DictionaryArray, FixedSizeListArray, LargeListArray, ListArray, MapArray, StringArray,
        StructArray, UnionArray,
    },
    datatypes::{UInt16Type, UInt32Type, UInt8Type},
};

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

impl TimeUnit {
    /// Convert a number of `TimeUnit` to microseconds.
    pub fn to_micros(&self, value: i64) -> i64 {
        match self {
            Self::Second => value * 1_000_000,
            Self::Millisecond => value * 1000,
            Self::Microsecond => value,
            Self::Nanosecond => value / 1000,
        }
    }
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
    /// The value is a decimal
    Decimal(Decimal),
    /// The value is a timestamp.
    Timestamp(TimeUnit, i64),
    /// The value is a text string.
    Text(&'a [u8]),
    /// The value is a blob of data
    Blob(&'a [u8]),
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
    List(ListType<'a>, usize),
    /// The value is an enum
    Enum(EnumType<'a>, usize),
    /// The value is a struct
    Struct(&'a StructArray, usize),
    /// The value is an array
    Array(&'a FixedSizeListArray, usize),
    /// The value is a map
    Map(&'a MapArray, usize),
    /// The value is a union
    Union(&'a ArrayRef, usize),
}

/// Wrapper type for different list sizes
#[derive(Debug, Copy, Clone, PartialEq)]
pub enum ListType<'a> {
    /// The underlying list is a `ListArray`
    Regular(&'a ListArray),
    /// The underlying list is a `LargeListArray`
    Large(&'a LargeListArray),
}

/// Wrapper type for different enum sizes
#[derive(Debug, Copy, Clone, PartialEq)]
pub enum EnumType<'a> {
    /// The underlying enum type is u8
    UInt8(&'a DictionaryArray<UInt8Type>),
    /// The underlying enum type is u16
    UInt16(&'a DictionaryArray<UInt16Type>),
    /// The underlying enum type is u32
    UInt32(&'a DictionaryArray<UInt32Type>),
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
            ValueRef::Interval { .. } => Type::Interval,
            ValueRef::Struct(arr, _) => arr.data_type().into(),
            ValueRef::Map(arr, _) => arr.data_type().into(),
            ValueRef::Array(arr, _) => arr.data_type().into(),
            ValueRef::List(arr, _) => match arr {
                ListType::Large(arr) => arr.data_type().into(),
                ListType::Regular(arr) => arr.data_type().into(),
            },
            ValueRef::Enum(..) => Type::Enum,
            ValueRef::Union(arr, _) => arr.data_type().into(),
        }
    }

    /// Returns an owned version of this ValueRef
    pub fn to_owned(&self) -> Value {
        (*self).into()
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
    fn from(borrowed: ValueRef<'_>) -> Self {
        match borrowed {
            ValueRef::Null => Self::Null,
            ValueRef::Boolean(i) => Self::Boolean(i),
            ValueRef::TinyInt(i) => Self::TinyInt(i),
            ValueRef::SmallInt(i) => Self::SmallInt(i),
            ValueRef::Int(i) => Self::Int(i),
            ValueRef::BigInt(i) => Self::BigInt(i),
            ValueRef::HugeInt(i) => Self::HugeInt(i),
            ValueRef::UTinyInt(i) => Self::UTinyInt(i),
            ValueRef::USmallInt(i) => Self::USmallInt(i),
            ValueRef::UInt(i) => Self::UInt(i),
            ValueRef::UBigInt(i) => Self::UBigInt(i),
            ValueRef::Float(i) => Self::Float(i),
            ValueRef::Double(i) => Self::Double(i),
            ValueRef::Decimal(i) => Self::Decimal(i),
            ValueRef::Timestamp(tu, t) => Self::Timestamp(tu, t),
            ValueRef::Text(s) => {
                let s = std::str::from_utf8(s).expect("invalid UTF-8");
                Self::Text(s.to_string())
            }
            ValueRef::Blob(b) => Self::Blob(b.to_vec()),
            ValueRef::Date32(d) => Self::Date32(d),
            ValueRef::Time64(t, d) => Self::Time64(t, d),
            ValueRef::Interval { months, days, nanos } => Self::Interval { months, days, nanos },
            ValueRef::List(items, idx) => match items {
                ListType::Regular(items) => {
                    let offsets = items.offsets();
                    from_list(
                        offsets[idx].try_into().unwrap(),
                        offsets[idx + 1].try_into().unwrap(),
                        idx,
                        items.values(),
                    )
                }
                ListType::Large(items) => {
                    let offsets = items.offsets();
                    from_list(
                        offsets[idx].try_into().unwrap(),
                        offsets[idx + 1].try_into().unwrap(),
                        idx,
                        items.values(),
                    )
                }
            },
            ValueRef::Enum(items, idx) => {
                let dict_values = match items {
                    EnumType::UInt8(res) => res.values(),
                    EnumType::UInt16(res) => res.values(),
                    EnumType::UInt32(res) => res.values(),
                }
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("Enum value is not a string");
                let dict_key = match items {
                    EnumType::UInt8(res) => res.key(idx),
                    EnumType::UInt16(res) => res.key(idx),
                    EnumType::UInt32(res) => res.key(idx),
                }
                .unwrap();
                Self::Enum(dict_values.value(dict_key).to_string())
            }
            ValueRef::Struct(items, idx) => {
                let capacity = items.columns().len();
                let mut value = Vec::with_capacity(capacity);
                value.extend(
                    items
                        .columns()
                        .iter()
                        .zip(items.fields().iter().map(|f| f.name().to_owned()))
                        .map(|(column, name)| -> (String, Self) {
                            (name, Row::value_ref_internal(idx, 0, column).to_owned())
                        }),
                );
                Self::Struct(OrderedMap::from(value))
            }
            ValueRef::Map(arr, idx) => {
                let keys = arr.keys();
                let values = arr.values();
                let offsets = arr.offsets();
                let range = offsets[idx]..offsets[idx + 1];
                let capacity = range.len();
                let mut map_vec = Vec::with_capacity(capacity);
                map_vec.extend(range.map(|row| {
                    let row = row.try_into().unwrap();
                    let key = Row::value_ref_internal(row, idx, keys).to_owned();
                    let value = Row::value_ref_internal(row, idx, values).to_owned();
                    (key, value)
                }));
                Self::Map(OrderedMap::from(map_vec))
            }
            ValueRef::Array(items, idx) => {
                let value_length = usize::try_from(items.value_length()).unwrap();
                let range = (idx * value_length)..((idx + 1) * value_length);
                let capacity = value_length;
                let mut array_vec = Vec::with_capacity(capacity);
                array_vec.extend(range.map(|row| Row::value_ref_internal(row, idx, items.values()).to_owned()));
                Self::Array(array_vec)
            }
            ValueRef::Union(column, idx) => {
                let column = column.as_any().downcast_ref::<UnionArray>().unwrap();
                let type_id = column.type_id(idx);
                let value_offset = column.value_offset(idx);

                let tag = Row::value_ref_internal(idx, value_offset, column.child(type_id));
                Self::Union(Box::new(tag.to_owned()))
            }
        }
    }
}

fn from_list(start: usize, end: usize, idx: usize, values: &ArrayRef) -> Value {
    let capacity = end - start;
    let mut list_vec = Vec::with_capacity(capacity);
    list_vec.extend((start..end).map(|row| Row::value_ref_internal(row, idx, values).to_owned()));
    Value::List(list_vec)
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
    fn from(value: &'a Value) -> Self {
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
            Value::Interval { months, days, nanos } => ValueRef::Interval { months, days, nanos },
            Value::Enum(..) => todo!(),
            Value::List(..) | Value::Struct(..) | Value::Map(..) | Value::Array(..) | Value::Union(..) => {
                unimplemented!()
            }
        }
    }
}

impl<'a, T> From<Option<T>> for ValueRef<'a>
where
    T: Into<Self>,
{
    #[inline]
    fn from(s: Option<T>) -> Self {
        match s {
            Some(x) => x.into(),
            None => ValueRef::Null,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::types::Type;
    use crate::{Connection, Result};

    #[test]
    fn test_list_types() -> Result<()> {
        let conn = Connection::open_in_memory()?;
        conn.execute(
            "CREATE TABLE test_table (float_list FLOAT[], double_list DOUBLE[], int_list INT[])",
            [],
        )?;
        conn.execute("INSERT INTO test_table VALUES ([1.5, 2.5], [3.5, 4.5], [1, 2])", [])?;

        let mut stmt = conn.prepare("SELECT float_list, double_list, int_list FROM test_table")?;
        let mut rows = stmt.query([])?;
        let row = rows.next()?.unwrap();

        let float_list = row.get_ref_unwrap(0);
        assert!(
            matches!(float_list.data_type(), Type::List(ref inner_type) if **inner_type == Type::Float),
            "Expected Type::List(Type::Float), got {:?}",
            float_list.data_type()
        );

        let double_list = row.get_ref_unwrap(1);
        assert!(
            matches!(double_list.data_type(), Type::List(ref inner_type) if **inner_type == Type::Double),
            "Expected Type::List(Type::Double), got {:?}",
            double_list.data_type()
        );

        let int_list = row.get_ref_unwrap(2);
        assert!(
            matches!(int_list.data_type(), Type::List(ref inner_type) if **inner_type == Type::Int),
            "Expected Type::List(Type::Int), got {:?}",
            int_list.data_type()
        );

        Ok(())
    }
}

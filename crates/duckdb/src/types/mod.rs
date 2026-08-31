//! Rust representations of DuckDB logical and physical value types.
//!
//! [`DuckDBType`] maps a Rust type to its DuckDB
//! [`crate::logical_type::LogicalType`]. [`ToValue`] and [`FromValue`] convert
//! between Rust values and owned [`crate::value::Value`] handles for parameters,
//! defaults, and other scalar APIs.
//!
//! This module also defines marker and wrapper types for values whose DuckDB
//! representation has no direct Rust primitive equivalent, including decimals,
//! nested types, intervals, UUIDs, `BIGNUM`, and `VARIANT`. Typed vector access
//! uses these representations through [`crate::vector::VectorElement`].

use crate::{
    Parameters, Result, check_api_call,
    connection::FFILink,
    error::Error,
    logical_type::{LogicalType, LogicalTypeID},
    parameter::QueryParameter,
    value::{Value, ValueInput},
};
use libduckdb_sys as ffi;
use std::fmt::Display;
use std::marker::PhantomData;

/// Constructs the DuckDB logical type represented by a Rust type.
pub trait DuckDBType {
    /// Return the DuckDB logical type represented by this Rust type.
    fn logical_type<C: FFILink + ?Sized>(link: &C) -> Result<LogicalType>;
}

/// Converts a Rust value into its DuckDB representation.
pub trait ToValue {
    /// Create a DuckDB value from this value.
    fn value<C: FFILink + ?Sized>(&self, link: &C) -> Result<Value>;
}

/// Converts an owned DuckDB value into a Rust value.
pub trait FromValue: Sized {
    /// Read a Rust value from a DuckDB value.
    fn from_value(value: &Value) -> Result<Self>;
}

/// Reads the raw [`ffi::duckdb_v2_bytes`] representation of a `VARCHAR` value.
///
/// Use [`String`] instead when a borrowed Rust [`str`] is sufficient.
pub struct TString;

/// A borrowed encoded `BIGNUM` value from a vector.
///
/// Call [`BigNum::decode`] to access its sign and magnitude.
#[repr(transparent)]
pub struct BigNum(ffi::duckdb_v2_bignum_t);

/// The decoded sign and magnitude of a [`BigNum`].
pub struct BigNumDecoded {
    /// Whether the number is negative.
    pub is_negative: bool,
    /// The unsigned magnitude in big-endian byte order.
    pub magnitude: Vec<u8>,
}

impl Display for BigNumDecoded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.magnitude.iter().all(|&b| b == 0) {
            return write!(f, "0");
        }

        const CHUNK_DIVISOR: u128 = 1_000_000_000_000_000_000;
        let mut magnitude = self.magnitude.to_vec();
        let mut chunks: Vec<u64> = Vec::new();

        while !magnitude.iter().all(|&b| b == 0) {
            let mut remainder: u128 = 0;
            for byte in magnitude.iter_mut() {
                let cur = (remainder << 8) | (*byte as u128);
                *byte = (cur / CHUNK_DIVISOR) as u8;
                remainder = cur % CHUNK_DIVISOR;
            }
            chunks.push(remainder as u64);
        }

        if self.is_negative {
            write!(f, "-")?;
        }

        let mut iter = chunks.iter().rev();
        write!(f, "{}", iter.next().unwrap())?;
        for chunk in iter {
            write!(f, "{:018}", chunk)?;
        }

        Ok(())
    }
}

impl BigNum {
    /// Return a decoded value containing the sign and big-endian magnitude.
    pub fn decode(&self) -> Result<BigNumDecoded> {
        let length = unsafe { self.0.value.inlined.length };
        let bytes = if length <= ffi::DUCKDB_V2_BYTES_INLINE_LENGTH {
            unsafe { self.0.value.inlined.inlined.as_ptr() as *const u8 }
        } else {
            unsafe { self.0.value.pointer.ptr as *const u8 }
        };
        let encoded = unsafe { std::slice::from_raw_parts(bytes, length as usize) };
        let (is_negative, magnitude) = Value::decode_bignum(encoded)?;

        Ok(BigNumDecoded { is_negative, magnitude })
    }
}

/// Reads a `VARIANT` row as an owned [`Value`].
pub struct Variant;

/// A `DECIMAL` vector element stored as the integer type `T`.
#[derive(Debug)]
pub struct Decimal<T> {
    _marker: PhantomData<T>,
}

/// Marks integer types supported as the physical storage of [`Decimal`].
pub trait InternalDecimalType {
    /// Convert the physical decimal storage to its scaled integer.
    fn to_i128(&self) -> i128;
}

macro_rules! impl_internal_decimal_type {
    ($($type:ty),+ $(,)?) => {
        $(
            impl InternalDecimalType for $type {
                fn to_i128(&self) -> i128 {
                    *self as i128
                }
            }
        )+
    };
}

impl_internal_decimal_type!(i16, i32, i64, i128);

/// A physical list entry parameterized by its child element type.
#[repr(C)]
pub struct List<L> {
    pub(crate) offset: u64,
    pub(crate) length: u64,
    pub(crate) _marker: PhantomData<L>,
}

/// A fixed-length array element parameterized by its child element type.
#[repr(C)]
pub struct Array<T> {
    offset: u64,
    length: u64,
    _marker: PhantomData<T>,
}

/// Reads a `STRUCT` vector as named fields.
pub struct Struct;

/// Reads a `MAP` vector with key type `K` and value type `V`.
pub struct Map<K, V>(pub ffi::duckdb_v2_list_entry, pub PhantomData<(K, V)>);

/// Reads a `UNION` vector through its tag and member child vectors.
pub struct Union;

macro_rules! declare_primitive_from_value {
    ($type:ty, $getter:path) => {
        impl FromValue for $type {
            fn from_value(value: &Value) -> Result<Self> {
                check_api_call!($getter, **value, RET)
            }
        }
    };
}

declare_primitive_from_value!(bool, ffi::duckdb_v2_value_get_bool);
declare_primitive_from_value!(u8, ffi::duckdb_v2_value_get_utinyint);
declare_primitive_from_value!(i8, ffi::duckdb_v2_value_get_tinyint);
declare_primitive_from_value!(i16, ffi::duckdb_v2_value_get_smallint);
declare_primitive_from_value!(i32, ffi::duckdb_v2_value_get_int);
declare_primitive_from_value!(i64, ffi::duckdb_v2_value_get_bigint);
declare_primitive_from_value!(u16, ffi::duckdb_v2_value_get_usmallint);
declare_primitive_from_value!(u32, ffi::duckdb_v2_value_get_uint);
declare_primitive_from_value!(u64, ffi::duckdb_v2_value_get_ubigint);
declare_primitive_from_value!(f32, ffi::duckdb_v2_value_get_float);
declare_primitive_from_value!(f64, ffi::duckdb_v2_value_get_double);

impl FromValue for i128 {
    fn from_value(value: &Value) -> Result<Self> {
        let raw = check_api_call!(ffi::duckdb_v2_value_get_hugeint, **value, RET)?;
        Ok((i128::from(raw.upper) << 64) | i128::from(raw.lower))
    }
}

impl FromValue for u128 {
    fn from_value(value: &Value) -> Result<Self> {
        let raw = check_api_call!(ffi::duckdb_v2_value_get_uhugeint, **value, RET)?;
        Ok((u128::from(raw.upper) << 64) | u128::from(raw.lower))
    }
}

fn owned_bytes(raw: ffi::DuckDBStr<'_>) -> Result<Vec<u8>> {
    if raw.len == 0 {
        return Ok(Vec::new());
    }
    if raw.ptr.is_null() {
        return Err(Error::api_error(
            "DuckDB returned a null pointer for a non-empty value".to_string(),
        ));
    }
    Ok(unsafe { std::slice::from_raw_parts(raw.ptr.cast(), raw.len as usize) }.to_vec())
}

impl FromValue for String {
    fn from_value(value: &Value) -> Result<Self> {
        let raw = check_api_call!(ffi::duckdb_v2_value_get_varchar, **value, RET)?;
        String::from_utf8(owned_bytes(raw)?).map_err(|_| Error::api_error("DuckDB returned invalid UTF-8".to_string()))
    }
}

macro_rules! declare_primitive_to_value {
    ($type:ty, $type_id:ident, $input:ident) => {
        impl DuckDBType for $type {
            fn logical_type<C: FFILink + ?Sized>(link: &C) -> Result<LogicalType> {
                link.logical_type_create_from_id(LogicalTypeID::$type_id, Parameters::None)
            }
        }

        impl ToValue for $type {
            fn value<C: FFILink + ?Sized>(&self, link: &C) -> Result<Value> {
                link.create_value(ValueInput::$input(*self))
            }
        }
    };
}

declare_primitive_to_value!(bool, DUCKDB_V2_LOGICAL_TYPE_ID_BOOLEAN, Bool);
declare_primitive_to_value!(u8, DUCKDB_V2_LOGICAL_TYPE_ID_UTINYINT, UTinyInt);
declare_primitive_to_value!(i8, DUCKDB_V2_LOGICAL_TYPE_ID_TINYINT, TinyInt);
declare_primitive_to_value!(i16, DUCKDB_V2_LOGICAL_TYPE_ID_SMALLINT, SmallInt);
declare_primitive_to_value!(i32, DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER, Int);
declare_primitive_to_value!(i64, DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT, BigInt);
declare_primitive_to_value!(u16, DUCKDB_V2_LOGICAL_TYPE_ID_USMALLINT, USmallInt);
declare_primitive_to_value!(u32, DUCKDB_V2_LOGICAL_TYPE_ID_UINTEGER, UInt);
declare_primitive_to_value!(u64, DUCKDB_V2_LOGICAL_TYPE_ID_UBIGINT, UBigInt);
declare_primitive_to_value!(f32, DUCKDB_V2_LOGICAL_TYPE_ID_FLOAT, Float);
declare_primitive_to_value!(f64, DUCKDB_V2_LOGICAL_TYPE_ID_DOUBLE, Double);
declare_primitive_to_value!(i128, DUCKDB_V2_LOGICAL_TYPE_ID_HUGEINT, HugeInt);
declare_primitive_to_value!(u128, DUCKDB_V2_LOGICAL_TYPE_ID_UHUGEINT, UHugeInt);

impl DuckDBType for str {
    fn logical_type<C: FFILink + ?Sized>(link: &C) -> Result<LogicalType> {
        link.logical_type_create_from_id(LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR, Parameters::None)
    }
}

impl ToValue for str {
    fn value<C: FFILink + ?Sized>(&self, link: &C) -> Result<Value> {
        link.create_value(ValueInput::Varchar(self))
    }
}

impl DuckDBType for String {
    fn logical_type<C: FFILink + ?Sized>(link: &C) -> Result<LogicalType> {
        str::logical_type(link)
    }
}

impl ToValue for String {
    fn value<C: FFILink + ?Sized>(&self, link: &C) -> Result<Value> {
        self.as_str().value(link)
    }
}

/// A byte string represented as a DuckDB `BLOB`.
#[repr(transparent)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlobValue<T>(pub T);

impl<T> DuckDBType for BlobValue<T> {
    fn logical_type<C: FFILink + ?Sized>(link: &C) -> Result<LogicalType> {
        link.logical_type_create_from_id(LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_BLOB, Parameters::None)
    }
}

impl<T: AsRef<[u8]>> ToValue for BlobValue<T> {
    fn value<C: FFILink + ?Sized>(&self, link: &C) -> Result<Value> {
        link.create_value(ValueInput::Blob(self.0.as_ref()))
    }
}

impl FromValue for BlobValue<Vec<u8>> {
    fn from_value(value: &Value) -> Result<Self> {
        let raw = check_api_call!(ffi::duckdb_v2_value_get_blob, **value, RET)?;
        Ok(Self(owned_bytes(raw)?))
    }
}

/// A BIT value in DuckDB's padding-header wire representation.
#[repr(transparent)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BitValue<T>(pub T);

impl<T> DuckDBType for BitValue<T> {
    fn logical_type<C: FFILink + ?Sized>(link: &C) -> Result<LogicalType> {
        link.logical_type_create_from_id(LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_BIT, Parameters::None)
    }
}

impl<T: AsRef<[u8]>> ToValue for BitValue<T> {
    fn value<C: FFILink + ?Sized>(&self, link: &C) -> Result<Value> {
        link.create_value(ValueInput::Bit(self.0.as_ref()))
    }
}

impl FromValue for BitValue<Vec<u8>> {
    fn from_value(value: &Value) -> Result<Self> {
        let raw = check_api_call!(ffi::duckdb_v2_value_get_blob, **value, RET)?;
        Ok(Self(owned_bytes(raw)?))
    }
}

macro_rules! declare_storage_value {
    ($doc:literal, $name:ident, $storage:ty, $type_id:ident, $input:ident, $getter:path) => {
        #[doc = $doc]
        #[repr(transparent)]
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub struct $name(pub $storage);

        impl Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                self.0.fmt(f)
            }
        }

        impl DuckDBType for $name {
            fn logical_type<C: FFILink + ?Sized>(link: &C) -> Result<LogicalType> {
                link.logical_type_create_from_id(LogicalTypeID::$type_id, Parameters::None)
            }
        }

        impl ToValue for $name {
            fn value<C: FFILink + ?Sized>(&self, link: &C) -> Result<Value> {
                link.create_value(ValueInput::$input(self.0))
            }
        }

        impl FromValue for $name {
            fn from_value(value: &Value) -> Result<Self> {
                Ok(Self(check_api_call!($getter, **value, RET)?))
            }
        }
    };
}

declare_storage_value!(
    "Days since 1970-01-01 represented as a DuckDB `DATE`.",
    DateValue,
    i32,
    DUCKDB_V2_LOGICAL_TYPE_ID_DATE,
    Date,
    ffi::duckdb_v2_value_get_date
);
declare_storage_value!(
    "Microseconds since midnight represented as a DuckDB `TIME`.",
    TimeValue,
    i64,
    DUCKDB_V2_LOGICAL_TYPE_ID_TIME,
    Time,
    ffi::duckdb_v2_value_get_time
);
declare_storage_value!(
    "Nanoseconds since midnight represented as a DuckDB `TIME_NS`.",
    TimeNsValue,
    i64,
    DUCKDB_V2_LOGICAL_TYPE_ID_TIME_NS,
    TimeNs,
    ffi::duckdb_v2_value_get_time_ns
);
declare_storage_value!(
    "Packed time and UTC offset represented as a DuckDB `TIME_TZ`.",
    TimeTzValue,
    u64,
    DUCKDB_V2_LOGICAL_TYPE_ID_TIME_TZ,
    TimeTz,
    ffi::duckdb_v2_value_get_time_tz
);
declare_storage_value!(
    "Microseconds since 1970-01-01 represented as a DuckDB `TIMESTAMP`.",
    TimestampValue,
    i64,
    DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP,
    Timestamp,
    ffi::duckdb_v2_value_get_timestamp
);
declare_storage_value!(
    "Seconds since 1970-01-01 represented as a DuckDB `TIMESTAMP_SEC`.",
    TimestampSecValue,
    i64,
    DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_SEC,
    TimestampSec,
    ffi::duckdb_v2_value_get_timestamp_sec
);
declare_storage_value!(
    "Milliseconds since 1970-01-01 represented as a DuckDB `TIMESTAMP_MS`.",
    TimestampMsValue,
    i64,
    DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_MS,
    TimestampMs,
    ffi::duckdb_v2_value_get_timestamp_ms
);
declare_storage_value!(
    "Nanoseconds since 1970-01-01 represented as a DuckDB `TIMESTAMP_NS`.",
    TimestampNsValue,
    i64,
    DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_NS,
    TimestampNs,
    ffi::duckdb_v2_value_get_timestamp_ns
);
declare_storage_value!(
    "UTC microseconds since 1970-01-01 represented as a DuckDB `TIMESTAMP_TZ`.",
    TimestampTzValue,
    i64,
    DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_TZ,
    TimestampTz,
    ffi::duckdb_v2_value_get_timestamp_tz
);
declare_storage_value!(
    "UTC nanoseconds since 1970-01-01 represented as a DuckDB `TIMESTAMP_TZ_NS`.",
    TimestampTzNsValue,
    i64,
    DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_TZ_NS,
    TimestampTzNs,
    ffi::duckdb_v2_value_get_timestamp_tz_ns
);

/// DuckDB's internal signed 128-bit UUID representation.
///
/// Because this type is signed internally, we need to do an conversion into a real UUID when interfacing with external libraries.
/// Enable the feature `uuid` to use conversions between `UuidValueRaw` and `uuid::Uuid`.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UuidValueRaw(pub i128);

#[cfg(feature = "uuid")]
impl From<uuid::Uuid> for UuidValueRaw {
    fn from(value: uuid::Uuid) -> Self {
        let lower = value.as_u128() as u64;
        let upper = (value.as_u128() >> 64) as u64;

        if upper > i64::MAX as u64 {
            UuidValueRaw(((upper - i64::MAX as u64 - 1) as i128) << 64 | lower as i128)
        } else {
            UuidValueRaw(((upper as i128) - i64::MAX as i128 - 1) << 64 | lower as i128)
        }
    }
}

#[cfg(feature = "uuid")]
impl From<UuidValueRaw> for uuid::Uuid {
    fn from(value: UuidValueRaw) -> Self {
        let upper = (value.0 >> 64) as u64;
        let upper = upper ^ (1u64 << 63);
        let lower = value.0 as u64;

        let mut bytes = [0u8; 16];

        for i in 0..8 {
            bytes[i] = ((upper >> (56 - 8 * i)) & 0xFF) as u8;
            bytes[8 + i] = ((lower >> (56 - 8 * i)) & 0xFF) as u8;
        }
        uuid::Uuid::from_bytes(bytes)
    }
}

impl DuckDBType for UuidValueRaw {
    fn logical_type<C: FFILink + ?Sized>(link: &C) -> Result<LogicalType> {
        link.logical_type_create_from_id(LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_UUID, Parameters::None)
    }
}

impl ToValue for UuidValueRaw {
    fn value<C: FFILink + ?Sized>(&self, link: &C) -> Result<Value> {
        link.create_value(ValueInput::Uuid(self.0))
    }
}

impl FromValue for UuidValueRaw {
    fn from_value(value: &Value) -> Result<Self> {
        let raw = check_api_call!(ffi::duckdb_v2_value_get_uuid, **value, RET)?;
        Ok(Self((i128::from(raw.upper as i64) << 64) | i128::from(raw.lower)))
    }
}

/// A DuckDB `INTERVAL` split into month, day, and microsecond components.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IntervalValue {
    /// Whole months.
    pub months: i32,
    /// Whole days.
    pub days: i32,
    /// Remaining microseconds.
    pub micros: i64,
}

impl DuckDBType for IntervalValue {
    fn logical_type<C: FFILink + ?Sized>(link: &C) -> Result<LogicalType> {
        link.logical_type_create_from_id(LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_INTERVAL, Parameters::None)
    }
}

impl ToValue for IntervalValue {
    fn value<C: FFILink + ?Sized>(&self, link: &C) -> Result<Value> {
        link.create_value(ValueInput::Interval {
            months: self.months,
            days: self.days,
            micros: self.micros,
        })
    }
}

impl FromValue for IntervalValue {
    fn from_value(value: &Value) -> Result<Self> {
        let raw = check_api_call!(ffi::duckdb_v2_value_get_interval, **value, RET)?;
        Ok(Self {
            months: raw.months,
            days: raw.days,
            micros: raw.micros,
        })
    }
}

impl FromValue for LogicalType {
    fn from_value(value: &Value) -> Result<Self> {
        value.logical_type()
    }
}

impl<T: DuckDBType + ?Sized> DuckDBType for &T {
    fn logical_type<C: FFILink + ?Sized>(link: &C) -> Result<LogicalType> {
        T::logical_type(link)
    }
}

impl<T: ToValue + ?Sized> ToValue for &T {
    fn value<C: FFILink + ?Sized>(&self, link: &C) -> Result<Value> {
        (*self).value(link)
    }
}

impl<T: DuckDBType> DuckDBType for Option<T> {
    fn logical_type<C: FFILink + ?Sized>(link: &C) -> Result<LogicalType> {
        T::logical_type(link)
    }
}

impl<T: ToValue + DuckDBType> ToValue for Option<T> {
    fn value<C: FFILink + ?Sized>(&self, link: &C) -> Result<Value> {
        match self {
            Some(value) => value.value(link),
            None => {
                let logical_type = Self::logical_type(link)?;
                link.create_value(ValueInput::Null(&logical_type))
            }
        }
    }
}

impl<T: FromValue> FromValue for Option<T> {
    fn from_value(value: &Value) -> Result<Self> {
        if value.is_null()? {
            Ok(None)
        } else {
            T::from_value(value).map(Some)
        }
    }
}

/// A scaled integer represented as `DECIMAL(WIDTH, SCALE)`.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecimalValue<T, const WIDTH: u8, const SCALE: u8>(pub T);

impl<T: InternalDecimalType, const WIDTH: u8, const SCALE: u8> DuckDBType for DecimalValue<T, WIDTH, SCALE> {
    fn logical_type<C: FFILink + ?Sized>(link: &C) -> Result<LogicalType> {
        link.logical_type_create("DECIMAL", Parameters::positional(&[&WIDTH, &SCALE]))
    }
}

impl<T: InternalDecimalType, const WIDTH: u8, const SCALE: u8> ToValue for DecimalValue<T, WIDTH, SCALE> {
    fn value<C: FFILink + ?Sized>(&self, link: &C) -> Result<Value> {
        link.create_value(ValueInput::Decimal {
            value: self.0.to_i128(),
            width: WIDTH,
            scale: SCALE,
        })
    }
}

/// A DuckDB `DECIMAL` in its runtime width, scale, and scaled-integer form.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecimalValueRaw {
    /// The integer payload scaled by ten to [`Self::scale`].
    pub value: i128,
    /// The total number of decimal digits.
    pub width: u8,
    /// The number of digits after the decimal point.
    pub scale: u8,
}

impl FromValue for DecimalValueRaw {
    fn from_value(value: &Value) -> Result<Self> {
        let mut width = 0;
        let mut scale = 0;
        let raw = check_api_call!(ffi::duckdb_v2_value_get_decimal, **value, RET, &mut width, &mut scale)?;
        Ok(Self {
            value: (i128::from(raw.upper) << 64) | i128::from(raw.lower),
            width,
            scale,
        })
    }
}

macro_rules! declare_bignum_type {
    ($type:ty) => {
        impl DuckDBType for $type {
            fn logical_type<C: FFILink + ?Sized>(link: &C) -> Result<LogicalType> {
                link.logical_type_create_from_id(LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_BIGNUM, Parameters::None)
            }
        }
    };
}

/// The bind-time `ANY` logical type used in function signatures.
pub struct Any;

impl DuckDBType for Any {
    fn logical_type<C: FFILink + ?Sized>(link: &C) -> Result<LogicalType> {
        link.logical_type_create_from_id(LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_ANY, Parameters::None)
    }
}

/// An owned encoded `BIGNUM` value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BigNumValue {
    /// Whether the number is negative.
    pub is_negative: bool,
    /// The unsigned magnitude in big-endian byte order.
    pub magnitude: Vec<u8>,
}

declare_bignum_type!(BigNumValue);
declare_bignum_type!(BigNum);

impl ToValue for BigNumValue {
    fn value<C: FFILink + ?Sized>(&self, link: &C) -> Result<Value> {
        let encoded = Value::encode_bignum(&self.magnitude, self.is_negative)?;
        link.create_value(ValueInput::BigNum(&encoded))
    }
}

impl FromValue for BigNumValue {
    fn from_value(value: &Value) -> Result<Self> {
        let raw = check_api_call!(ffi::duckdb_v2_value_get_blob, **value, RET)?;
        let encoded = owned_bytes(raw)?;
        let (is_negative, magnitude) = Value::decode_bignum(&encoded)?;
        Ok(Self { is_negative, magnitude })
    }
}

impl ToValue for BigNum {
    fn value<C: FFILink + ?Sized>(&self, link: &C) -> Result<Value> {
        let decoded = self.decode()?;
        let encoded = Value::encode_bignum(&decoded.magnitude, decoded.is_negative)?;
        link.create_value(ValueInput::BigNum(&encoded))
    }
}

/// Key-value entries represented as a DuckDB `MAP`.
pub struct MapValue<K, V> {
    /// Entries in insertion order.
    pub entries: Vec<(K, V)>,
}

impl<K: DuckDBType, V: DuckDBType> DuckDBType for MapValue<K, V> {
    fn logical_type<C: FFILink + ?Sized>(link: &C) -> Result<LogicalType> {
        let key_type = Value::from_logical_type(link, &K::logical_type(link)?)?;
        let value_type = Value::from_logical_type(link, &V::logical_type(link)?)?;
        link.logical_type_create("MAP", Parameters::positional(&[&key_type, &value_type]))
    }
}

impl<K: ToValue + DuckDBType, V: ToValue + DuckDBType> ToValue for MapValue<K, V> {
    fn value<C: FFILink + ?Sized>(&self, link: &C) -> Result<Value> {
        let mut keys = Vec::with_capacity(self.entries.len());
        let mut values = Vec::with_capacity(self.entries.len());
        for (key, value) in &self.entries {
            keys.push(key.value(link)?);
            values.push(value.value(link)?);
        }

        let key_type = K::logical_type(link)?;
        let value_type = V::logical_type(link)?;
        link.create_value(ValueInput::Map {
            key_type: &key_type,
            value_type: &value_type,
            keys: &keys,
            values: &values,
        })
    }
}

/// Defines the named fields of a [`StructValue`].
pub trait StructSchema {
    /// Return field names and types in storage order.
    fn fields<C: FFILink + ?Sized>(link: &C) -> Result<Vec<(&'static str, LogicalType)>>;
}

trait StructFieldValue {
    fn create_value(&self, link: &dyn FFILink) -> Result<Value>;
}

impl<T: ToValue> StructFieldValue for T {
    fn create_value(&self, link: &dyn FFILink) -> Result<Value> {
        ToValue::value(self, link)
    }
}

/// A heterogeneous DuckDB `STRUCT` value with schema `S`.
pub struct StructValue<'a, S> {
    fields: Vec<Box<dyn StructFieldValue + 'a>>,
    _schema: PhantomData<S>,
}

impl<'a, S> StructValue<'a, S> {
    /// Create an empty struct builder.
    pub fn new() -> Self {
        Self {
            fields: Vec::new(),
            _schema: PhantomData,
        }
    }

    /// Append a field value in schema order.
    pub fn field<T: ToValue + 'a>(mut self, value: T) -> Self {
        self.fields.push(Box::new(value));
        self
    }
}

impl<S> Default for StructValue<'_, S> {
    fn default() -> Self {
        Self::new()
    }
}

impl<S: StructSchema> DuckDBType for StructValue<'_, S> {
    fn logical_type<C: FFILink + ?Sized>(link: &C) -> Result<LogicalType> {
        let fields = S::fields(link)?;
        let values = fields
            .into_iter()
            .map(|(name, logical_type)| Ok((name, Value::from_logical_type(link, &logical_type)?)))
            .collect::<Result<Vec<_>>>()?;
        let parameters = values
            .iter()
            .map(|(name, value)| (*name, value as &dyn QueryParameter))
            .collect::<Vec<_>>();
        link.logical_type_create("STRUCT", Parameters::named(&parameters))
    }
}

impl<S: StructSchema> ToValue for StructValue<'_, S> {
    fn value<C: FFILink + ?Sized>(&self, link: &C) -> Result<Value> {
        let children = self
            .fields
            .iter()
            .map(|field| field.create_value(&link))
            .collect::<Result<Vec<_>>>()?;
        let fields = S::fields(link)?;
        let names = fields.iter().map(|(name, _)| *name).collect::<Vec<_>>();
        link.create_value(ValueInput::Struct {
            names: &names,
            children: &children,
        })
    }
}

/// Defines the named members of a [`UnionValue`].
pub trait UnionSchema {
    /// Return member names and types in tag order.
    fn members<C: FFILink + ?Sized>(link: &C) -> Result<Vec<(&'static str, LogicalType)>>;
}

/// An active member represented as a DuckDB `UNION` with schema `S`.
pub struct UnionValue<S, T> {
    /// The active member value.
    pub value: T,
    _schema: PhantomData<S>,
}

impl<S, T> UnionValue<S, T> {
    /// Create a union value from its active member.
    pub fn new(value: T) -> Self {
        Self {
            value,
            _schema: PhantomData,
        }
    }
}

impl<S: UnionSchema, T> DuckDBType for UnionValue<S, T> {
    fn logical_type<C: FFILink + ?Sized>(link: &C) -> Result<LogicalType> {
        let members = S::members(link)?;
        let values = members
            .into_iter()
            .map(|(name, logical_type)| Ok((name, Value::from_logical_type(link, &logical_type)?)))
            .collect::<Result<Vec<_>>>()?;
        let parameters = values
            .iter()
            .map(|(name, value)| (*name, value as &dyn QueryParameter))
            .collect::<Vec<_>>();
        link.logical_type_create("UNION", Parameters::named(&parameters))
    }
}

impl<S: UnionSchema, T: ToValue> ToValue for UnionValue<S, T> {
    fn value<C: FFILink + ?Sized>(&self, link: &C) -> Result<Value> {
        let value = self.value.value(link)?;
        link.value_cast(&value, Self::logical_type(link)?)
    }
}

/// A value converted to DuckDB's self-describing `VARIANT` type.
pub struct VariantValue<T>(pub T);

impl<T> DuckDBType for VariantValue<T> {
    fn logical_type<C: FFILink + ?Sized>(link: &C) -> Result<LogicalType> {
        link.logical_type_create("VARIANT", Parameters::None)
    }
}

impl<T: ToValue> ToValue for VariantValue<T> {
    fn value<C: FFILink + ?Sized>(&self, link: &C) -> Result<Value> {
        let value = self.0.value(link)?;
        link.value_cast(&value, Self::logical_type(link)?)
    }
}

impl<T: DuckDBType> DuckDBType for Vec<T> {
    fn logical_type<C: FFILink + ?Sized>(link: &C) -> Result<LogicalType> {
        let child_type = Value::from_logical_type(link, &T::logical_type(link)?)?;
        link.logical_type_create("LIST", Parameters::positional(&[&child_type]))
    }
}

impl<T: ToValue + DuckDBType> ToValue for Vec<T> {
    fn value<C: FFILink + ?Sized>(&self, link: &C) -> Result<Value> {
        let children = self.iter().map(|value| value.value(link)).collect::<Result<Vec<_>>>()?;
        let child_type = T::logical_type(link)?;
        link.create_value(ValueInput::List {
            child_type: &child_type,
            children: &children,
        })
    }
}

impl<T: DuckDBType, const N: usize> DuckDBType for [T; N] {
    fn logical_type<C: FFILink + ?Sized>(link: &C) -> Result<LogicalType> {
        let child_type = Value::from_logical_type(link, &T::logical_type(link)?)?;
        let length = (N as u64).value(link)?;
        link.logical_type_create("ARRAY", Parameters::positional(&[&child_type, &length]))
    }
}

impl<T: ToValue + DuckDBType, const N: usize> ToValue for [T; N] {
    fn value<C: FFILink + ?Sized>(&self, link: &C) -> Result<Value> {
        let children = self.iter().map(|value| value.value(link)).collect::<Result<Vec<_>>>()?;
        let child_type = T::logical_type(link)?;
        link.create_value(ValueInput::Array {
            child_type: &child_type,
            children: &children,
        })
    }
}

impl DuckDBType for () {
    fn logical_type<C: FFILink + ?Sized>(link: &C) -> Result<LogicalType> {
        link.logical_type_create("TUPLE", Parameters::None)
    }
}

impl ToValue for () {
    fn value<C: FFILink + ?Sized>(&self, link: &C) -> Result<Value> {
        link.create_value(ValueInput::Tuple(&[]))
    }
}

macro_rules! impl_tuple_value {
    ($(($type:ident, $index:tt)),+ $(,)?) => {
        impl<$($type: DuckDBType),+> DuckDBType for ($($type,)+) {
            fn logical_type<L: FFILink + ?Sized>(link: &L) -> Result<LogicalType> {
                let types = vec![
                    $(Value::from_logical_type(link, &$type::logical_type(link)?)?),+
                ];
                let parameters = types
                    .iter()
                    .map(|value| value as &dyn crate::parameter::QueryParameter)
                    .collect::<Vec<_>>();
                link.logical_type_create("TUPLE", Parameters::positional(&parameters))
            }
        }

        impl<$($type: ToValue),+> ToValue for ($($type,)+) {
            fn value<L: FFILink + ?Sized>(&self, link: &L) -> Result<Value> {
                let children = vec![$(self.$index.value(link)?),+];
                link.create_value(ValueInput::Tuple(&children))
            }
        }
    };
}

impl_tuple_value!((A, 0));
impl_tuple_value!((A, 0), (B, 1));
impl_tuple_value!((A, 0), (B, 1), (C, 2));
impl_tuple_value!((A, 0), (B, 1), (C, 2), (D, 3));
impl_tuple_value!((A, 0), (B, 1), (C, 2), (D, 3), (E, 4));
impl_tuple_value!((A, 0), (B, 1), (C, 2), (D, 3), (E, 4), (F, 5));
impl_tuple_value!((A, 0), (B, 1), (C, 2), (D, 3), (E, 4), (F, 5), (G, 6));
impl_tuple_value!((A, 0), (B, 1), (C, 2), (D, 3), (E, 4), (F, 5), (G, 6), (H, 7));

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests;

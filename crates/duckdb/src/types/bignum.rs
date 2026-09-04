//! The `BIGNUM` logical type.
//!
//! [`BigNum`] borrows a vector row's encoded representation; [`BigNumValue`]
//! is its owned, decoded counterpart. Both encode/decode through
//! [`crate::value::Value::encode_bignum`]/[`crate::value::Value::decode_bignum`].

use super::string::owned_bytes;
use super::{DuckDBType, FromValue, ToValue};
use crate::{
    Parameters, Result, check_api_call,
    connection::FFILink,
    logical_type::{LogicalType, LogicalTypeID},
    value::{Value, ValueInput},
    vector::{Vector, VectorElement},
};
use libduckdb_sys as ffi;
use std::fmt::Display;

/// A borrowed encoded `BIGNUM` value from a vector.
///
/// Call [`BigNum::decode`] to access its sign and magnitude.
#[repr(transparent)]
pub struct BigNum(ffi::duckdb_v2_bignum_t);

impl BigNum {
    /// Return a decoded value containing the sign and big-endian magnitude.
    pub fn decode(&self) -> Result<BigNumValue> {
        let length = unsafe { self.0.value.inlined.length };
        let bytes = if length <= ffi::DUCKDB_V2_BYTES_INLINE_LENGTH {
            unsafe { self.0.value.inlined.inlined.as_ptr() as *const u8 }
        } else {
            unsafe { self.0.value.pointer.ptr as *const u8 }
        };
        let encoded = unsafe { std::slice::from_raw_parts(bytes, length as usize) };
        let (is_negative, magnitude) = Value::decode_bignum(encoded)?;

        Ok(BigNumValue { is_negative, magnitude })
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

impl ToValue for BigNum {
    fn value<C: FFILink + ?Sized>(&self, link: &C) -> Result<Value> {
        let decoded = self.decode()?;
        let encoded = Value::encode_bignum(&decoded.magnitude, decoded.is_negative)?;
        link.create_value(ValueInput::BigNum(&encoded))
    }
}

impl VectorElement for BigNum {
    const TYPE_ID: LogicalTypeID = LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_BIGNUM;

    type Internal = BigNum;

    type Ref<'a> = &'a BigNum;

    fn get<'a, U: VectorElement>(vector: &'a Vector<'_, U>, physical: usize, _logical: usize) -> Self::Ref<'a>
    where
        Self: Sized + 'a,
    {
        let data_ptr = vector.view.as_ref().unwrap().as_ptr() as *const BigNum;
        (unsafe { &*data_ptr.add(physical) }) as _
    }
}

/// The decoded sign and magnitude of a [`BigNum`].
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

// TODO: Review if needed..
impl VectorElement for BigNumValue {
    const TYPE_ID: LogicalTypeID = LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_BIGNUM;

    type Internal = BigNum;

    type Ref<'a> = &'a BigNum;

    fn get<'a, U: VectorElement>(vector: &'a Vector<'_, U>, physical: usize, logical: usize) -> Self::Ref<'a>
    where
        Self: 'a,
    {
        BigNum::get(vector, physical, logical).into()
    }
}

impl Display for BigNumValue {
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

//! The `BIGNUM` logical type.
//!
//! [`BigNum`] borrows a vector row's encoded representation; [`BigNumValue`]
//! is its owned, decoded counterpart. Both encode/decode through
//! [`crate::value::Value::encode_bignum`]/[`crate::value::Value::decode_bignum`].

use super::string::owned_bytes;
use super::{DuckDBType, FromValue, ToValue};
use crate::ffi;
use crate::{
    Error, Parameters, Result, check_api_call,
    connection::FFILink,
    logical_type::{LogicalType, LogicalTypeID},
    raw::RawExt,
    value::{Value, ValueInput},
    vector::{HasDuckDBBytes, VectorElement, WritableVectorElement},
};
use std::fmt::Display;

/// A borrowed encoded `BIGNUM` value from a vector.
///
/// Call [`BigNum::decode`] to access its sign and magnitude.
#[repr(transparent)]
pub struct BigNum(ffi::duckdb_v2_bignum_t);

impl std::fmt::Debug for BigNum {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.decode() {
            Ok(value) => f.debug_tuple("BigNum").field(&value).finish(),
            Err(_) => f
                .debug_struct("BigNum")
                .field("size", &self.encoded().len())
                .finish_non_exhaustive(),
        }
    }
}

impl BigNum {
    fn encoded(&self) -> &[u8] {
        let length = unsafe { self.0.value.inlined.length };
        let bytes = if length <= ffi::DUCKDB_V2_BYTES_INLINE_LENGTH {
            unsafe { self.0.value.inlined.inlined.as_ptr() as *const u8 }
        } else {
            unsafe { self.0.value.pointer.ptr as *const u8 }
        };
        unsafe { std::slice::from_raw_parts(bytes, length as usize) }
    }

    /// Return a decoded value containing the sign and big-endian magnitude.
    pub fn decode(&self) -> Result<BigNumValue> {
        let (is_negative, magnitude) = Value::decode_bignum(self.encoded())?;

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

impl HasDuckDBBytes for BigNum {}
impl HasDuckDBBytes for BigNumValue {}

// SAFETY: `BigNum` matches DuckDB's string storage for `BIGNUM`.
unsafe impl VectorElement for BigNum {
    const TYPE_ID: LogicalTypeID = LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_BIGNUM;

    type Internal = BigNum;

    type Ref<'a> = &'a BigNum;

    unsafe fn get<'a, U: VectorElement>(
        vector: &crate::vector::VectorReader<'a, U>,
        physical: usize,
        _logical: usize,
    ) -> Self::Ref<'a>
    where
        Self: Sized + 'a,
    {
        let data_ptr = vector.as_ptr() as *const BigNum;
        (unsafe { &*data_ptr.add(physical) }) as _
    }
}

// SAFETY: a `BigNum` only borrows DuckDB-encoded bytes, which `write_bytes` copies into the vector's arena.
unsafe impl WritableVectorElement for BigNum {
    type Write<'a> = &'a BigNum;

    unsafe fn write(
        vector: &mut crate::vector::VectorWriter<'_, '_, Self>,
        index: usize,
        value: Option<Self::Write<'_>>,
    ) -> Result<()> {
        // SAFETY: The caller guarantees a writable, flat, in-bounds row.
        unsafe { vector.write_bytes(index, value.map(BigNum::encoded)) }
    }
}

/// The decoded sign and magnitude of a [`BigNum`].
/// Non-canonical values are rejected when converted to a [`Value`] or written to a vector.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BigNumValue {
    /// Whether the number is negative; must be false for zero.
    pub is_negative: bool,
    /// Nonempty big-endian magnitude without leading zero bytes; zero is `[0x00]`.
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
    fn _get_inner(value: &Value) -> Result<Self> {
        let logical_type = value.logical_type()?;
        if logical_type.id()? != LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_BIGNUM {
            return Err(Error::invalid_input(format!(
                "Expected BIGNUM value, found {}",
                logical_type.to_text()?
            )));
        }

        let raw = check_api_call!(ffi::duckdb_v2_value_get_blob, value.raw(), RET)?;
        let encoded = owned_bytes(raw)?;
        let (is_negative, magnitude) = Value::decode_bignum(&encoded)?;
        Ok(Self { is_negative, magnitude })
    }
}

// TODO: Review if needed..
// SAFETY: same storage as `BigNum`.
unsafe impl VectorElement for BigNumValue {
    const TYPE_ID: LogicalTypeID = LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_BIGNUM;

    type Internal = BigNum;

    type Ref<'a> = &'a BigNum;

    unsafe fn get<'a, U: VectorElement>(
        vector: &crate::vector::VectorReader<'a, U>,
        physical: usize,
        logical: usize,
    ) -> Self::Ref<'a>
    where
        Self: 'a,
    {
        // SAFETY: same storage as `BigNum`; the caller upholds the contract.
        unsafe { BigNum::get(vector, physical, logical) }
    }
}

// SAFETY: `encode_bignum` produces valid bytes, which `write_bytes` copies into the vector's arena.
unsafe impl WritableVectorElement for BigNumValue {
    type Write<'a> = &'a BigNumValue;

    unsafe fn write(
        vector: &mut crate::vector::VectorWriter<'_, '_, Self>,
        index: usize,
        value: Option<Self::Write<'_>>,
    ) -> Result<()> {
        let encoded = value
            .map(|value| Value::encode_bignum(&value.magnitude, value.is_negative))
            .transpose()?;
        // SAFETY: The caller guarantees a writable, flat, in-bounds row.
        unsafe { vector.write_bytes(index, encoded.as_deref()) }
    }
}

impl Display for BigNumValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use std::fmt::Write;

        const CHUNK_DIVISOR: u64 = 1_000_000_000;
        let mut magnitude: Vec<u32> = self
            .magnitude
            .rchunks(4)
            .rev()
            .map(|bytes| bytes.iter().fold(0, |limb, &byte| (limb << 8) | u32::from(byte)))
            .collect();
        let mut start = magnitude.iter().position(|&limb| limb != 0).unwrap_or(magnitude.len());
        let mut chunks = Vec::new();

        while start < magnitude.len() {
            let mut remainder = 0;
            for limb in &mut magnitude[start..] {
                let current = (remainder << 32) | u64::from(*limb);
                *limb = (current / CHUNK_DIVISOR) as u32;
                remainder = current % CHUNK_DIVISOR;
            }
            chunks.push(remainder);
            while start < magnitude.len() && magnitude[start] == 0 {
                start += 1;
            }
        }

        let Some(last) = chunks.pop() else {
            return f.pad_integral(true, "", "0");
        };
        let mut digits = last.to_string();
        for chunk in chunks.iter().rev() {
            write!(digits, "{chunk:09}")?;
        }
        f.pad_integral(!self.is_negative, "", &digits)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bignum_debug_shows_decoded_value() -> Result<()> {
        use crate::environment::{Environment, StorageLocation};

        let env = Environment::new()?;
        let conn = env.open(StorageLocation::InMemory)?.connect()?;
        let mut result = conn.query("SELECT -16909060::BIGNUM", Parameters::None)?;
        let chunk = result.next_chunk()?.unwrap();
        let vector = chunk.get_vector_at::<BigNum>(0)?;
        let value = vector.reader()?.get(0)?.unwrap();
        assert_eq!(
            format!("{value:?}"),
            "BigNum(BigNumValue { is_negative: true, magnitude: [1, 2, 3, 4] })"
        );
        Ok(())
    }

    #[test]
    fn bignum_display_digits_and_padding() {
        for value in [0_u128, 1_000_000_000, 10_u128.pow(18), 10_u128.pow(36), u128::MAX] {
            let bignum = BigNumValue {
                is_negative: false,
                magnitude: value.to_be_bytes().to_vec(),
            };
            assert_eq!(bignum.to_string(), value.to_string());
            assert_eq!(format!("{bignum:+042}"), format!("{value:+042}"));
            assert_eq!(format!("{bignum:*>42}"), format!("{value:*>42}"));
        }
        let negative = BigNumValue {
            is_negative: true,
            magnitude: vec![1],
        };
        assert_eq!(format!("{negative:05}"), "-0001");
        assert_eq!(format!("{negative:+}"), "-1");
    }
}

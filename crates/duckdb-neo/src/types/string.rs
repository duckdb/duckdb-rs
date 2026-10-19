//! `VARCHAR`, `BLOB`, and `BIT` byte-oriented logical types.
//!
//! [`str`] and [`String`] represent `VARCHAR`; [`BlobValue`] and [`BitValue`] wrap `BLOB` and `BIT` payloads.
//! All four share DuckDB's [`crate::bytes::DuckDBBytes`] wire representation.

use super::{DuckDBType, FromValue, ToValue};
use crate::ffi_str::{DuckDBStr, bytes_view};
use crate::{
    Parameters, Result,
    bytes::DuckDBBytes,
    check_api_call,
    connection::FFILink,
    error::Error,
    ffi,
    logical_type::{LogicalType, LogicalTypeID},
    raw::RawExt,
    value::{Value, ValueInput},
    vector::{Vector, VectorElement, WritableVectorElement},
};
/// Copy bytes DuckDB just returned; `raw` must be null or valid for the duration of the call.
pub(crate) fn owned_bytes(raw: ffi::duckdb_v2_str) -> Result<Vec<u8>> {
    if raw.len == 0 {
        return Ok(Vec::new());
    }
    DuckDBStr::from_raw(raw)
        .as_bytes()
        .map(<[u8]>::to_vec)
        .ok_or_else(|| Error::api_error("DuckDB returned a null pointer for a non-empty value"))
}

impl FromValue for String {
    fn _get_inner(value: &Value) -> Result<Self> {
        let raw = check_api_call!(ffi::duckdb_v2_value_get_varchar, value.raw(), RET)?;
        String::from_utf8(owned_bytes(raw)?).map_err(|_| Error::api_error("DuckDB returned invalid UTF-8"))
    }
}

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
pub struct BlobValue(pub Vec<u8>);

impl DuckDBType for BlobValue {
    fn logical_type<C: FFILink + ?Sized>(link: &C) -> Result<LogicalType> {
        link.logical_type_create_from_id(LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_BLOB, Parameters::None)
    }
}

impl ToValue for BlobValue {
    fn value<C: FFILink + ?Sized>(&self, link: &C) -> Result<Value> {
        link.create_value(ValueInput::Blob(self.0.as_ref()))
    }
}

impl FromValue for BlobValue {
    fn _get_inner(value: &Value) -> Result<Self> {
        let logical_type = value.logical_type()?;
        if logical_type.id()? != LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_BLOB {
            return Err(Error::invalid_input(format!(
                "Expected BLOB value, found {}",
                logical_type.to_text()?
            )));
        }
        let raw = check_api_call!(ffi::duckdb_v2_value_get_blob, value.raw(), RET)?;
        Ok(Self(owned_bytes(raw)?))
    }
}

/// A BIT value in DuckDB's padding-header wire representation.
///
/// The first byte counts the padding bits, below 8, which lead the first data byte and must be set.
#[repr(transparent)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BitValue(pub Vec<u8>);

/// Reject bytes DuckDB would misread as BIT; a padding count of 8 or more makes it write out of bounds.
fn check_bit(bytes: &[u8]) -> Result<()> {
    let [padding, first, ..] = bytes else {
        return Err(Error::invalid_input(
            "BIT storage needs a padding byte and at least one data byte",
        ));
    };
    if *padding >= 8 {
        return Err(Error::invalid_input(format!("BIT padding {padding} must be below 8")));
    }
    let padding_bits = !(u8::MAX >> padding);
    if first & padding_bits != padding_bits {
        return Err(Error::invalid_input("BIT padding bits must be set"));
    }
    Ok(())
}

impl DuckDBType for BitValue {
    fn logical_type<C: FFILink + ?Sized>(link: &C) -> Result<LogicalType> {
        link.logical_type_create_from_id(LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_BIT, Parameters::None)
    }
}

impl ToValue for BitValue {
    fn value<C: FFILink + ?Sized>(&self, link: &C) -> Result<Value> {
        check_bit(&self.0)?;
        link.create_value(ValueInput::Bit(self.0.as_ref()))
    }
}

impl FromValue for BitValue {
    fn _get_inner(value: &Value) -> Result<Self> {
        let logical_type = value.logical_type()?;
        if logical_type.id()? != LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_BIT {
            return Err(Error::invalid_input(format!(
                "Expected BIT value, found {}",
                logical_type.to_text()?
            )));
        }
        let raw = check_api_call!(ffi::duckdb_v2_value_get_blob, value.raw(), RET)?;
        Ok(Self(owned_bytes(raw)?))
    }
}

// SAFETY: `DuckDBBytes` matches DuckDB's string storage.
unsafe impl VectorElement for BlobValue {
    const TYPE_ID: LogicalTypeID = LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_BLOB;

    type Internal = DuckDBBytes;

    type Ref<'a> = &'a [u8];

    unsafe fn get<'a, U: VectorElement>(vector: &'a Vector<'_, U>, physical: usize, _logical: usize) -> Self::Ref<'a>
    where
        Self: 'a,
    {
        let data_ptr = vector.view.as_ref().unwrap().as_ptr() as *const DuckDBBytes;
        unsafe { &*data_ptr.add(physical) }.get_data()
    }
}

// SAFETY: `DuckDBBytes` matches DuckDB's string storage.
unsafe impl VectorElement for BitValue {
    const TYPE_ID: LogicalTypeID = LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_BIT;

    type Internal = DuckDBBytes;

    type Ref<'a> = &'a [u8];

    unsafe fn get<'a, U: VectorElement>(vector: &'a Vector<'_, U>, physical: usize, _logical: usize) -> Self::Ref<'a>
    where
        Self: Sized + 'a,
    {
        let data_ptr = vector.view.as_ref().unwrap().as_ptr() as *const DuckDBBytes;
        unsafe { &*data_ptr.add(physical) }.get_data()
    }
}

/// # Panics
/// Panics when a VARCHAR row contains invalid UTF-8.
// SAFETY: `DuckDBBytes` matches DuckDB's string storage.
unsafe impl VectorElement for String {
    const TYPE_ID: LogicalTypeID = LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR;

    type Internal = DuckDBBytes;

    type Ref<'a> = &'a str;

    unsafe fn get<'a, U: VectorElement>(vector: &'a Vector<'_, U>, physical: usize, _logical: usize) -> Self::Ref<'a>
    where
        Self: Sized + 'a,
    {
        let data_ptr = vector.view.as_ref().unwrap().as_ptr() as *const ffi::duckdb_v2_bytes;

        let string_view = unsafe { &*data_ptr.add(physical) };

        std::str::from_utf8(bytes_view(string_view)).expect("DuckDB VARCHAR is not valid UTF-8")
    }
}

// SAFETY: `&str` is valid UTF-8, and `write_bytes` stores it in the vector's arena.
unsafe impl WritableVectorElement for String {
    type Write<'a> = &'a str;

    unsafe fn write(vector: &mut Vector<'_, Self>, index: usize, value: Option<Self::Write<'_>>) -> Result<()> {
        vector.write_bytes(index, value.map(|v| v.as_bytes()))
    }
}

// SAFETY: `write_bytes` stores the bytes in the vector's arena.
unsafe impl WritableVectorElement for BlobValue {
    type Write<'a> = &'a [u8];

    unsafe fn write(vector: &mut Vector<'_, Self>, index: usize, value: Option<Self::Write<'_>>) -> Result<()> {
        vector.write_bytes(index, value)
    }
}

// SAFETY: `check_bit` rejects invalid BIT bytes before `write_bytes` stores them.
unsafe impl WritableVectorElement for BitValue {
    type Write<'a> = &'a [u8];

    unsafe fn write(vector: &mut Vector<'_, Self>, index: usize, value: Option<&[u8]>) -> Result<()> {
        if let Some(value) = value {
            check_bit(value)?;
        }
        vector.write_bytes(index, value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        data_chunk::DataChunk,
        environment::{Environment, StorageLocation},
        types::BigNumValue,
    };

    #[test]
    #[should_panic(expected = "DuckDB VARCHAR is not valid UTF-8")]
    fn varchar_vector_reports_invalid_utf8() {
        let env = Environment::new().unwrap();
        let conn = env.open(StorageLocation::InMemory).unwrap().connect().unwrap();
        let mut chunk = DataChunk::create(&conn, &[String::logical_type(&conn).unwrap()], true).unwrap();
        let mut vector = chunk.get_vector_at_mut::<String>(0).unwrap();
        vector.set_size(1).unwrap();
        vector.write_bytes(0, Some(&[0xff])).unwrap();
        let _ = vector.get(0);
    }

    #[test]
    fn blob_and_bit_getters_require_matching_logical_types() -> Result<()> {
        let env = Environment::new()?;
        let conn = env.open(StorageLocation::InMemory)?.connect()?;
        let blob = BlobValue(vec![0, 255]).value(&conn)?;
        let bit = BitValue(vec![0, 255]).value(&conn)?;
        let bignum = BigNumValue {
            is_negative: false,
            magnitude: vec![1],
        }
        .value(&conn)?;

        assert_eq!(blob.get::<BlobValue>()?, Some(BlobValue(vec![0, 255])));
        assert_eq!(bit.get::<BitValue>()?, Some(BitValue(vec![0, 255])));
        assert!(blob.get::<BitValue>().is_err());
        assert!(bit.get::<BlobValue>().is_err());
        assert!(bignum.get::<BlobValue>().is_err());
        assert!(bignum.get::<BitValue>().is_err());
        Ok(())
    }
}

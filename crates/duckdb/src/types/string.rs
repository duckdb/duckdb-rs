//! `VARCHAR`, `BLOB`, and `BIT` byte-oriented logical types.
//!
//! [`String`] and [`TString`] represent `VARCHAR`; [`BlobValue`] and
//! [`BitValue`] wrap byte payloads for `BLOB` and `BIT` respectively. All four
//! share DuckDB's [`crate::bytes::DuckDBBytes`] wire representation.

use super::{DuckDBType, FromValue, ToValue};
use crate::{
    Parameters, Result,
    bytes::DuckDBBytes,
    check_api_call,
    connection::FFILink,
    error::Error,
    logical_type::{LogicalType, LogicalTypeID},
    value::{Value, ValueInput},
    vector::{Unknown, Vector, VectorElement, WritableVectorElement},
};
use libduckdb_sys as ffi;

/// Reads the raw [`ffi::duckdb_v2_bytes`] representation of a `VARCHAR` value.
///
/// Use [`String`] instead when a borrowed Rust [`str`] is sufficient.
pub struct TString;

pub(crate) fn owned_bytes(raw: ffi::DuckDBStr<'_>) -> Result<Vec<u8>> {
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

impl<T> VectorElement for BlobValue<T> {
    const TYPE_ID: LogicalTypeID = LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_BLOB;

    type Internal = DuckDBBytes;

    type Ref<'a>
        = &'a [u8]
    where
        T: 'a;

    fn get<'a, U: VectorElement>(vector: &'a Vector<'_, U>, physical: usize, _logical: usize) -> Self::Ref<'a>
    where
        T: 'a,
    {
        let data_ptr = vector.view.as_ref().unwrap().as_ptr() as *const DuckDBBytes;
        unsafe { &*data_ptr.add(physical) }.get_data()
    }
}

impl<T> VectorElement for BitValue<T> {
    const TYPE_ID: LogicalTypeID = LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_BIT;

    type Internal = DuckDBBytes;

    type Ref<'a>
        = &'a [u8]
    where
        T: 'a;

    fn get<'a, U: VectorElement>(vector: &'a Vector<'_, U>, physical: usize, _logical: usize) -> Self::Ref<'a>
    where
        T: 'a,
    {
        let data_ptr = vector.view.as_ref().unwrap().as_ptr() as *const DuckDBBytes;
        unsafe { &*data_ptr.add(physical) }.get_data()
    }
}

impl VectorElement for TString {
    const TYPE_ID: LogicalTypeID = LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR;

    type Internal = DuckDBBytes;

    type Ref<'a> = &'a DuckDBBytes;

    fn validate(_other: &LogicalType, _children: &[Vector<'_, Unknown>]) -> Result<bool> {
        Ok(true)
    }

    fn get<'a, U: VectorElement>(vector: &'a Vector<'_, U>, physical: usize, _logical: usize) -> Self::Ref<'a>
    where
        Self: Sized + 'a,
    {
        let data_ptr = vector.view.as_ref().unwrap().as_ptr() as *const DuckDBBytes;
        (unsafe { &*data_ptr.add(physical) }) as _
    }
}

impl VectorElement for String {
    const TYPE_ID: LogicalTypeID = LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR;

    type Internal = DuckDBBytes;

    type Ref<'a> = &'a str;

    fn get<'a, U: VectorElement>(vector: &'a Vector<'_, U>, physical: usize, _logical: usize) -> Self::Ref<'a>
    where
        Self: Sized + 'a,
    {
        let data_ptr = vector.view.as_ref().unwrap().as_ptr() as *const ffi::duckdb_v2_bytes;

        let string_view = unsafe { &*data_ptr.add(physical) };

        string_view.into()
    }
}

impl WritableVectorElement for String {
    type Write<'a> = &'a str;

    fn write(vector: &mut Vector<'_, Self>, index: usize, value: Option<Self::Write<'_>>) -> Result<()> {
        vector.write_bytes(index, value.map(|v| v.as_bytes()))
    }
}

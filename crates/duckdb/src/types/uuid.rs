//! The `UUID` logical type.
//!
//! [`UuidValue`] mirrors DuckDB's internal signed 128-bit UUID representation
//! exactly, so it is read and written through a direct pointer cast (see
//! [`super::primitive::DeclareVectorElement`]), the same pattern used by the
//! calendar/clock types in [`super::temporal`].

use super::primitive::DeclareVectorElement;
use super::{DuckDBType, FromValue, ToValue};
use crate::{
    Parameters, Result, check_api_call,
    connection::FFILink,
    logical_type::{LogicalType, LogicalTypeID},
    value::{Value, ValueInput},
};
use libduckdb_sys as ffi;

/// DuckDB's internal signed 128-bit UUID representation.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UuidValue(pub i128);

impl DuckDBType for UuidValue {
    fn logical_type<C: FFILink + ?Sized>(link: &C) -> Result<LogicalType> {
        link.logical_type_create_from_id(LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_UUID, Parameters::None)
    }
}

impl ToValue for UuidValue {
    fn value<C: FFILink + ?Sized>(&self, link: &C) -> Result<Value> {
        link.create_value(ValueInput::Uuid(self.0))
    }
}

impl FromValue for UuidValue {
    fn from_value(value: &Value) -> Result<Self> {
        let raw = check_api_call!(ffi::duckdb_v2_value_get_uuid, **value, RET)?;
        Ok(Self((i128::from(raw.upper) << 64) | i128::from(raw.lower)))
    }
}

DeclareVectorElement!(UuidValue, DUCKDB_V2_LOGICAL_TYPE_ID_UUID);

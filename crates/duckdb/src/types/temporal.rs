//! `DATE`, `TIME`, `TIMESTAMP`, and `INTERVAL` logical types.
//!
//! Each storage-wrapper type mirrors DuckDB's physical representation for its
//! logical type exactly, so it can be read and written through a direct
//! pointer cast (see [`super::primitive::DeclareVectorElement`]).
//!
//! `UUID` follows the same pattern but lives in [`super::uuid`], since it is
//! not itself a calendar/clock type.

use super::primitive::DeclareVectorElement;
use super::{DuckDBType, FromValue, ToValue};
use crate::{
    Parameters, Result, check_api_call,
    connection::FFILink,
    logical_type::{LogicalType, LogicalTypeID},
    value::{Value, ValueInput},
};
use libduckdb_sys as ffi;
use std::fmt::Display;

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

        DeclareVectorElement!($name, $type_id);
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

DeclareVectorElement!(IntervalValue, DUCKDB_V2_LOGICAL_TYPE_ID_INTERVAL);

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
use crate::ffi;
use crate::{
    Parameters, Result, check_api_call,
    connection::FFILink,
    logical_type::{LogicalType, LogicalTypeID},
    value::{Value, ValueInput},
    vector::{Vector, WritableVectorElement},
};
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
            fn _get_inner(value: &Value) -> Result<Self> {
                Ok(Self(check_api_call!($getter, **value, RET)?))
            }
        }

        DeclareVectorElement!($name, $type_id);

        impl WritableVectorElement for $name {
            type Write<'a> = Self;

            fn write(vector: &mut Vector<'_, Self>, index: usize, value: Option<Self::Write<'_>>) -> Result<()> {
                vector.write_raw(index, value)
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
    fn _get_inner(value: &Value) -> Result<Self> {
        let raw = check_api_call!(ffi::duckdb_v2_value_get_interval, **value, RET)?;
        Ok(Self {
            months: raw.months,
            days: raw.days,
            micros: raw.micros,
        })
    }
}

DeclareVectorElement!(IntervalValue, DUCKDB_V2_LOGICAL_TYPE_ID_INTERVAL);

impl WritableVectorElement for IntervalValue {
    type Write<'a> = Self;

    fn write(vector: &mut Vector<'_, Self>, index: usize, value: Option<Self::Write<'_>>) -> Result<()> {
        vector.write_raw(index, value)
    }
}

macro_rules! declare_to_chrono {
    (
        $name:ident, $chrono_type:ty, $operation:ident
    ) => {
        #[cfg(feature = "chrono")]
        impl From<$name> for $chrono_type {
            fn from(value: $name) -> Self {
                <$chrono_type>::$operation(value.0)
            }
        }
    };
}

macro_rules! declare_to_chrono_option {
    (
        $name:ident, $chrono_type:ty, $operation:ident
    ) => {
        #[cfg(feature = "chrono")]
        impl From<$name> for Option<$chrono_type> {
            fn from(value: $name) -> Self {
                <$chrono_type>::$operation(value.0)
            }
        }
    };
}

declare_to_chrono_option!(DateValue, chrono::NaiveDate, from_epoch_days);

#[cfg(feature = "chrono")]
impl From<TimeValue> for Option<chrono::NaiveTime> {
    fn from(value: TimeValue) -> Self {
        let seconds = value.0 / 1_000_000;
        let nano = (value.0 % 1_000_000) as u32 * 1_000;

        chrono::NaiveTime::from_num_seconds_from_midnight_opt(seconds as u32, nano)
    }
}

#[cfg(feature = "chrono")]
impl From<TimeNsValue> for Option<chrono::NaiveTime> {
    fn from(value: TimeNsValue) -> Self {
        let seconds = value.0 / 1_000_000_000;
        let nano = (value.0 % 1_000_000_000) as u32;
        chrono::NaiveTime::from_num_seconds_from_midnight_opt(seconds as u32, nano)
    }
}

// TimeTz has no chrono equivalent, so we do not provide a conversion macro for it.

declare_to_chrono_option!(TimestampValue, chrono::DateTime<chrono::Utc>, from_timestamp_micros);

#[cfg(feature = "chrono")]
impl From<TimestampSecValue> for Option<chrono::DateTime<chrono::Utc>> {
    fn from(value: TimestampSecValue) -> Self {
        chrono::DateTime::<chrono::Utc>::from_timestamp(value.0, 0)
    }
}

declare_to_chrono_option!(TimestampMsValue, chrono::DateTime<chrono::Utc>, from_timestamp_millis);
declare_to_chrono!(TimestampNsValue, chrono::DateTime<chrono::Utc>, from_timestamp_nanos);

declare_to_chrono_option!(TimestampTzValue, chrono::DateTime<chrono::Utc>, from_timestamp_micros);
declare_to_chrono!(TimestampTzNsValue, chrono::DateTime<chrono::Utc>, from_timestamp_nanos);

#[cfg(test)]
#[cfg(feature = "chrono")]
mod chrono_tests {
    use crate::{
        Parameters,
        environment::{Environment, StorageLocation},
        types::{
            DateValue, TimeNsValue, TimeValue, TimestampMsValue, TimestampNsValue, TimestampSecValue, TimestampTzValue,
            TimestampValue,
        },
    };

    #[test]
    fn test_date() -> crate::Result<()> {
        let env = Environment::new()?;
        let db = env.open(StorageLocation::InMemory)?;
        let conn = db.connect()?;

        let result = conn.query("SELECT DATE '1992-09-20';", Parameters::None)?;

        for chunk in result {
            let chunk = chunk?;
            let vec = chunk.get_vector_at::<DateValue>(0)?;

            assert_eq!(vec.len(), 1);
            let chrono = Option::<chrono::NaiveDate>::from(*vec.get(0)?.unwrap()).unwrap();

            assert_eq!(chrono, chrono::NaiveDate::from_ymd_opt(1992, 9, 20).unwrap());
        }

        Ok(())
    }

    #[test]
    fn test_timestamp_conversion() -> crate::Result<()> {
        let env = Environment::new()?;
        let db = env.open(StorageLocation::InMemory)?;
        let conn = db.connect()?;
        {
            let mut result = conn.query("SELECT TIMESTAMP_NS '1992-09-20 11:30:00.123456789';", Parameters::None)?;
            let chunk = result.next().unwrap()?;
            let vec = chunk.get_vector_at::<TimestampNsValue>(0)?;
            let timestamp = <chrono::DateTime<chrono::Utc>>::from(*vec.get(0)?.unwrap());
            assert_eq!(
                timestamp,
                chrono::DateTime::parse_from_rfc3339("1992-09-20T11:30:00.123456789Z")
                    .unwrap()
                    .with_timezone(&chrono::Utc)
            );
        }
        {
            let mut result = conn.query("SELECT TIMESTAMP '1992-09-20 11:30:00.123456789';", Parameters::None)?;
            let chunk = result.next().unwrap()?;
            let vec = chunk.get_vector_at::<TimestampValue>(0)?;
            let val = *(vec.get(0)?.unwrap());
            let timestamp = Option::<chrono::DateTime<chrono::Utc>>::from(val).unwrap();
            assert_eq!(
                timestamp,
                chrono::DateTime::parse_from_rfc3339("1992-09-20T11:30:00.123456Z")
                    .unwrap()
                    .with_timezone(&chrono::Utc)
            );
        }
        {
            let mut result = conn.query("SELECT TIMESTAMP_MS '1992-09-20 11:30:00.123456789';", Parameters::None)?;
            let chunk = result.next().unwrap()?;
            let vec = chunk.get_vector_at::<TimestampMsValue>(0)?;
            let val = *(vec.get(0)?.unwrap());
            let timestamp = Option::<chrono::DateTime<chrono::Utc>>::from(val).unwrap();
            assert_eq!(
                timestamp,
                chrono::DateTime::parse_from_rfc3339("1992-09-20T11:30:00.123Z")
                    .unwrap()
                    .with_timezone(&chrono::Utc)
            );
        }
        {
            let mut result = conn.query("SELECT TIMESTAMP_S '1992-09-20 11:30:00.123456789';", Parameters::None)?;
            let chunk = result.next().unwrap()?;
            let vec = chunk.get_vector_at::<TimestampSecValue>(0)?;
            let val = *(vec.get(0)?.unwrap());
            let timestamp = Option::<chrono::DateTime<chrono::Utc>>::from(val).unwrap();
            assert_eq!(
                timestamp,
                chrono::DateTime::parse_from_rfc3339("1992-09-20T11:30:00Z")
                    .unwrap()
                    .with_timezone(&chrono::Utc)
            );
        }
        {
            let mut result = conn.query(
                "SELECT TIMESTAMPTZ '1992-09-20 11:30:00.123456789+00:00';",
                Parameters::None,
            )?;
            let chunk = result.next().unwrap()?;
            let vec = chunk.get_vector_at::<TimestampTzValue>(0)?;
            let val = *(vec.get(0)?.unwrap());
            let timestamp = Option::<chrono::DateTime<chrono::Utc>>::from(val).unwrap();
            assert_eq!(
                timestamp,
                chrono::DateTime::parse_from_rfc3339("1992-09-20T11:30:00.123456Z")
                    .unwrap()
                    .with_timezone(&chrono::Utc)
            );
        }
        {
            let mut result = conn.query(
                "SELECT TIMESTAMPTZ '1992-09-20 12:30:00.123456789+01:00';",
                Parameters::None,
            )?;
            let chunk = result.next().unwrap()?;
            let vec = chunk.get_vector_at::<TimestampTzValue>(0)?;
            let val = *(vec.get(0)?.unwrap());
            let timestamp = Option::<chrono::DateTime<chrono::Utc>>::from(val).unwrap();
            assert_eq!(
                timestamp,
                chrono::DateTime::parse_from_rfc3339("1992-09-20T12:30:00.123456+01:00")
                    .unwrap()
                    .with_timezone(&chrono::Utc)
            );
        }
        Ok(())
    }

    #[test]
    fn test_time_conversion() -> crate::Result<()> {
        let env = Environment::new()?;
        let db = env.open(StorageLocation::InMemory)?;
        let conn = db.connect()?;

        {
            let mut result = conn.query("SELECT TIME '1992-09-20 11:30:00.123456';", Parameters::None)?;
            let chunk = result.next().unwrap()?;
            let vec = chunk.get_vector_at::<TimeValue>(0)?;
            let val = *(vec.get(0)?.unwrap());
            let time = Option::<chrono::NaiveTime>::from(val).unwrap();
            assert_eq!(
                time,
                chrono::NaiveTime::parse_from_str("11:30:00.123456", "%H:%M:%S%.f").unwrap()
            );
        }
        {
            let mut result = conn.query("SELECT '15:30:00.123456789'::TIME_NS;", Parameters::None)?;
            let chunk = result.next().unwrap()?;
            let vec = chunk.get_vector_at::<TimeNsValue>(0)?;
            let val = *(vec.get(0)?.unwrap());
            let time = Option::<chrono::NaiveTime>::from(val).unwrap();
            assert_eq!(
                time,
                chrono::NaiveTime::parse_from_str("15:30:00.123456789", "%H:%M:%S%.f").unwrap()
            );
        }

        Ok(())
    }
}

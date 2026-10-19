//! `DATE`, `TIME`, `TIMESTAMP`, and `INTERVAL` logical types.
//!
//! Each storage-wrapper type mirrors DuckDB's physical representation for its
//! logical type exactly, so it can be read and written through a direct
//! pointer cast (see [`super::primitive::declare_vector_element`]).
//!
//! `UUID` follows the same pattern but lives in [`super::uuid`], since it is
//! not itself a calendar/clock type.

use super::primitive::declare_vector_element;
use super::{DuckDBType, FromValue, ToValue};
use crate::ffi;
use crate::{
    Parameters, Result, check_api_call,
    connection::FFILink,
    logical_type::{LogicalType, LogicalTypeID},
    raw::RawExt,
    value::{Value, ValueInput},
    vector::{Vector, WritableVectorElement},
};

macro_rules! declare_storage_value {
    ($doc:literal, $name:ident, $storage:ty, $type_id:ident, $input:ident, $getter:path $(, $derive:ident)*) => {
        #[doc = $doc]
        #[repr(transparent)]
        #[derive(Debug, Clone, Copy, PartialEq, Eq $(, $derive)*)]
        pub struct $name(pub $storage);

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
                check_api_call!($getter, value.raw(), RET).map(Self)
            }
        }

        declare_vector_element!($name, $type_id);

        // SAFETY: every `$storage` value is valid for its logical type.
        unsafe impl WritableVectorElement for $name {
            type Write<'a> = Self;

            unsafe fn write(vector: &mut Vector<'_, Self>, index: usize, value: Option<Self::Write<'_>>) -> Result<()> {
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
    ffi::duckdb_v2_value_get_date,
    Hash,
    PartialOrd,
    Ord
);
declare_storage_value!(
    "Microseconds since midnight represented as a DuckDB `TIME`.",
    TimeValue,
    i64,
    DUCKDB_V2_LOGICAL_TYPE_ID_TIME,
    Time,
    ffi::duckdb_v2_value_get_time,
    Hash,
    PartialOrd,
    Ord
);
declare_storage_value!(
    "Nanoseconds since midnight represented as a DuckDB `TIME_NS`.",
    TimeNsValue,
    i64,
    DUCKDB_V2_LOGICAL_TYPE_ID_TIME_NS,
    TimeNs,
    ffi::duckdb_v2_value_get_time_ns,
    Hash,
    PartialOrd,
    Ord
);
declare_storage_value!(
    "Packed `TIME_TZ`: microseconds in the high 40 bits, reverse-ordered UTC offset in the low 24.",
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
    ffi::duckdb_v2_value_get_timestamp,
    Hash,
    PartialOrd,
    Ord
);
declare_storage_value!(
    "Seconds since 1970-01-01 represented as a DuckDB `TIMESTAMP_SEC`.",
    TimestampSecValue,
    i64,
    DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_SEC,
    TimestampSec,
    ffi::duckdb_v2_value_get_timestamp_sec,
    Hash,
    PartialOrd,
    Ord
);
declare_storage_value!(
    "Milliseconds since 1970-01-01 represented as a DuckDB `TIMESTAMP_MS`.",
    TimestampMsValue,
    i64,
    DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_MS,
    TimestampMs,
    ffi::duckdb_v2_value_get_timestamp_ms,
    Hash,
    PartialOrd,
    Ord
);
declare_storage_value!(
    "Nanoseconds since 1970-01-01 represented as a DuckDB `TIMESTAMP_NS`.",
    TimestampNsValue,
    i64,
    DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_NS,
    TimestampNs,
    ffi::duckdb_v2_value_get_timestamp_ns,
    Hash,
    PartialOrd,
    Ord
);
declare_storage_value!(
    "UTC microseconds since 1970-01-01 represented as a DuckDB `TIMESTAMP_TZ`.",
    TimestampTzValue,
    i64,
    DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_TZ,
    TimestampTz,
    ffi::duckdb_v2_value_get_timestamp_tz,
    Hash,
    PartialOrd,
    Ord
);
declare_storage_value!(
    "UTC nanoseconds since 1970-01-01 represented as a DuckDB `TIMESTAMP_TZ_NS`.",
    TimestampTzNsValue,
    i64,
    DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_TZ_NS,
    TimestampTzNs,
    ffi::duckdb_v2_value_get_timestamp_tz_ns,
    Hash,
    PartialOrd,
    Ord
);

impl TimeTzValue {
    /// Number of low bits reserved for the reversed UTC offset.
    pub const OFFSET_BITS: u32 = 24;
    /// Mask selecting the reversed UTC offset.
    pub const OFFSET_MASK: u64 = (1 << Self::OFFSET_BITS) - 1;
    /// Maximum UTC offset in seconds (15:59:59).
    pub const MAX_OFFSET: i32 = 16 * 60 * 60 - 1;

    /// Packs microseconds since midnight and a UTC offset in seconds, positive east, without validation.
    pub const fn new(micros: i64, offset_seconds: i32) -> Self {
        Self(((micros as u64) << Self::OFFSET_BITS) | ((Self::MAX_OFFSET - offset_seconds) as u64 & Self::OFFSET_MASK))
    }

    /// Microseconds since midnight.
    pub const fn micros(self) -> i64 {
        (self.0 >> Self::OFFSET_BITS) as i64
    }

    /// UTC offset in seconds, positive east.
    pub const fn offset_seconds(self) -> i32 {
        Self::MAX_OFFSET - (self.0 & Self::OFFSET_MASK) as i32
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
    fn _get_inner(value: &Value) -> Result<Self> {
        check_api_call!(ffi::duckdb_v2_value_get_interval, value.raw(), RET).map(|raw| Self {
            months: raw.months,
            days: raw.days,
            micros: raw.micros,
        })
    }
}

declare_vector_element!(IntervalValue, DUCKDB_V2_LOGICAL_TYPE_ID_INTERVAL);

// SAFETY: every field combination is a valid `INTERVAL`.
unsafe impl WritableVectorElement for IntervalValue {
    type Write<'a> = Self;

    unsafe fn write(vector: &mut Vector<'_, Self>, index: usize, value: Option<Self::Write<'_>>) -> Result<()> {
        vector.write_raw(index, value)
    }
}

#[cfg(feature = "chrono")]
mod external {
    use ::chrono::Timelike;

    use crate::types::{DateValue, TimestampMsValue, TimestampTzValue, TimestampValue};
    use crate::types::{TimeNsValue, TimeValue, TimestampNsValue, TimestampSecValue, TimestampTzNsValue};

    /// Returns `None` for infinity or dates outside chrono's range.
    impl From<DateValue> for Option<::chrono::NaiveDate> {
        fn from(value: DateValue) -> Self {
            ::chrono::NaiveDate::from_epoch_days(value.0)
        }
    }

    impl From<::chrono::NaiveDate> for DateValue {
        fn from(value: ::chrono::NaiveDate) -> Self {
            Self(value.to_epoch_days())
        }
    }

    /// Returns `None` for times outside midnight (inclusive) to 24:00 (exclusive).
    impl From<TimeValue> for Option<::chrono::NaiveTime> {
        fn from(value: TimeValue) -> Self {
            let seconds = u32::try_from(value.0 / 1_000_000).ok()?;
            let nano = u32::try_from((value.0 % 1_000_000) * 1_000).ok()?;

            ::chrono::NaiveTime::from_num_seconds_from_midnight_opt(seconds, nano)
        }
    }

    /// Returns `None` for times outside midnight (inclusive) to 24:00 (exclusive).
    impl From<TimeNsValue> for Option<::chrono::NaiveTime> {
        fn from(value: TimeNsValue) -> Self {
            let seconds = u32::try_from(value.0 / 1_000_000_000).ok()?;
            let nano = u32::try_from(value.0 % 1_000_000_000).ok()?;
            ::chrono::NaiveTime::from_num_seconds_from_midnight_opt(seconds, nano)
        }
    }

    fn checked_timestamp_units(seconds: i64, nanos: u32, units_per_second: i64) -> crate::Result<i64> {
        let nanos_per_unit = 1_000_000_000 / units_per_second;
        if nanos >= 1_000_000_000 || i64::from(nanos) % nanos_per_unit != 0 {
            return Err(crate::Error::invalid_input(
                "timestamp precision cannot be represented exactly",
            ));
        }
        let units = i128::from(seconds) * i128::from(units_per_second) + i128::from(nanos) / i128::from(nanos_per_unit);
        let units = i64::try_from(units).map_err(|_| crate::Error::invalid_input("timestamp is out of range"))?;
        if units == i64::MAX || units == -i64::MAX {
            return Err(crate::Error::invalid_input("timestamp is reserved for infinity"));
        }
        Ok(units)
    }

    impl TryFrom<::chrono::NaiveTime> for TimeValue {
        type Error = crate::Error;

        fn try_from(value: ::chrono::NaiveTime) -> crate::Result<Self> {
            checked_timestamp_units(
                i64::from(value.num_seconds_from_midnight()),
                value.nanosecond(),
                1_000_000,
            )
            .map(Self)
        }
    }

    impl TryFrom<::chrono::NaiveTime> for TimeNsValue {
        type Error = crate::Error;

        fn try_from(value: ::chrono::NaiveTime) -> crate::Result<Self> {
            checked_timestamp_units(
                i64::from(value.num_seconds_from_midnight()),
                value.nanosecond(),
                1_000_000_000,
            )
            .map(Self)
        }
    }

    impl TryFrom<::chrono::NaiveDateTime> for TimestampValue {
        type Error = crate::Error;

        fn try_from(value: ::chrono::NaiveDateTime) -> crate::Result<Self> {
            let value = value.and_utc();
            checked_timestamp_units(value.timestamp(), value.nanosecond(), 1_000_000).map(Self)
        }
    }

    impl TryFrom<::chrono::NaiveDateTime> for TimestampSecValue {
        type Error = crate::Error;

        fn try_from(value: ::chrono::NaiveDateTime) -> crate::Result<Self> {
            let value = value.and_utc();
            checked_timestamp_units(value.timestamp(), value.nanosecond(), 1).map(Self)
        }
    }

    impl TryFrom<::chrono::NaiveDateTime> for TimestampMsValue {
        type Error = crate::Error;

        fn try_from(value: ::chrono::NaiveDateTime) -> crate::Result<Self> {
            let value = value.and_utc();
            checked_timestamp_units(value.timestamp(), value.nanosecond(), 1_000).map(Self)
        }
    }

    impl TryFrom<::chrono::NaiveDateTime> for TimestampNsValue {
        type Error = crate::Error;

        fn try_from(value: ::chrono::NaiveDateTime) -> crate::Result<Self> {
            let value = value.and_utc();
            checked_timestamp_units(value.timestamp(), value.nanosecond(), 1_000_000_000).map(Self)
        }
    }

    impl TryFrom<::chrono::DateTime<::chrono::Utc>> for TimestampTzValue {
        type Error = crate::Error;

        fn try_from(value: ::chrono::DateTime<::chrono::Utc>) -> crate::Result<Self> {
            checked_timestamp_units(value.timestamp(), value.nanosecond(), 1_000_000).map(Self)
        }
    }

    impl TryFrom<::chrono::DateTime<::chrono::Utc>> for TimestampTzNsValue {
        type Error = crate::Error;

        fn try_from(value: ::chrono::DateTime<::chrono::Utc>) -> crate::Result<Self> {
            checked_timestamp_units(value.timestamp(), value.nanosecond(), 1_000_000_000).map(Self)
        }
    }

    // chrono has no time-with-offset type.

    // Timestamps without a time zone map to `NaiveDateTime`; only the TZ variants map to `DateTime<Utc>`.
    /// Returns `None` for infinity or timestamps outside chrono's range.
    impl From<TimestampValue> for Option<::chrono::NaiveDateTime> {
        fn from(value: TimestampValue) -> Self {
            ::chrono::DateTime::from_timestamp_micros(value.0).map(|dt| dt.naive_utc())
        }
    }

    /// Returns `None` for infinity or timestamps outside chrono's range.
    impl From<TimestampSecValue> for Option<::chrono::NaiveDateTime> {
        fn from(value: TimestampSecValue) -> Self {
            ::chrono::DateTime::from_timestamp(value.0, 0).map(|dt| dt.naive_utc())
        }
    }

    /// Returns `None` for infinity or timestamps outside chrono's range.
    impl From<TimestampMsValue> for Option<::chrono::NaiveDateTime> {
        fn from(value: TimestampMsValue) -> Self {
            ::chrono::DateTime::from_timestamp_millis(value.0).map(|dt| dt.naive_utc())
        }
    }

    /// Returns `None` for infinity or timestamps outside chrono's range.
    impl From<TimestampTzValue> for Option<::chrono::DateTime<::chrono::Utc>> {
        fn from(value: TimestampTzValue) -> Self {
            ::chrono::DateTime::from_timestamp_micros(value.0)
        }
    }

    fn timestamp_nanos_to_chrono(value: i64) -> Option<::chrono::DateTime<::chrono::Utc>> {
        if value == i64::MAX || value == -i64::MAX {
            return None;
        }
        Some(::chrono::DateTime::from_timestamp_nanos(value))
    }

    /// Returns `None` for infinity; every finite nanosecond timestamp fits chrono's range.
    impl From<TimestampNsValue> for Option<::chrono::NaiveDateTime> {
        fn from(value: TimestampNsValue) -> Self {
            timestamp_nanos_to_chrono(value.0).map(|dt| dt.naive_utc())
        }
    }

    /// Returns `None` for infinity; every finite nanosecond timestamp fits chrono's range.
    impl From<TimestampTzNsValue> for Option<::chrono::DateTime<::chrono::Utc>> {
        fn from(value: TimestampTzNsValue) -> Self {
            timestamp_nanos_to_chrono(value.0)
        }
    }

    #[cfg(test)]
    mod tests {
        use crate::{
            Parameters,
            environment::{Environment, StorageLocation},
            types::{
                DateValue, TimeNsValue, TimeValue, TimestampMsValue, TimestampNsValue, TimestampSecValue,
                TimestampTzNsValue, TimestampTzValue, TimestampValue,
            },
        };

        fn naive(s: &str) -> ::chrono::NaiveDateTime {
            ::chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f").unwrap()
        }

        #[test]
        fn test_checked_chrono_to_storage() -> crate::Result<()> {
            for text in ["1992-09-20 11:30:00", "1960-01-02 03:04:05"] {
                let value = naive(text);
                assert_eq!(
                    Option::<::chrono::NaiveDateTime>::from(TimestampValue::try_from(value)?),
                    Some(value)
                );
                assert_eq!(
                    Option::<::chrono::NaiveDateTime>::from(TimestampSecValue::try_from(value)?),
                    Some(value)
                );
                assert_eq!(
                    Option::<::chrono::NaiveDateTime>::from(TimestampMsValue::try_from(value)?),
                    Some(value)
                );
                assert_eq!(
                    Option::<::chrono::NaiveDateTime>::from(TimestampNsValue::try_from(value)?),
                    Some(value)
                );
                let utc = value.and_utc();
                assert_eq!(
                    Option::<::chrono::DateTime<::chrono::Utc>>::from(TimestampTzValue::try_from(utc)?),
                    Some(utc)
                );
                assert_eq!(
                    Option::<::chrono::DateTime<::chrono::Utc>>::from(TimestampTzNsValue::try_from(utc)?),
                    Some(utc)
                );
            }
            let micros = naive("1960-01-02 03:04:05.123456");
            assert_eq!(
                Option::<::chrono::NaiveDateTime>::from(TimestampValue::try_from(micros)?),
                Some(micros)
            );
            let millis = naive("1960-01-02 03:04:05.123");
            assert_eq!(
                Option::<::chrono::NaiveDateTime>::from(TimestampMsValue::try_from(millis)?),
                Some(millis)
            );
            let nanos = naive("1960-01-02 03:04:05.123456789");
            assert_eq!(
                Option::<::chrono::NaiveDateTime>::from(TimestampNsValue::try_from(nanos)?),
                Some(nanos)
            );
            assert_eq!(
                Option::<::chrono::NaiveTime>::from(TimeValue::try_from(micros.time())?),
                Some(micros.time())
            );
            assert_eq!(
                Option::<::chrono::NaiveTime>::from(TimeNsValue::try_from(nanos.time())?),
                Some(nanos.time())
            );
            Ok(())
        }

        #[test]
        fn test_checked_chrono_to_storage_rejects_loss_and_overflow() {
            let nanos = naive("1992-09-20 11:30:00.123456789");
            assert!(TimestampSecValue::try_from(nanos).is_err());
            assert!(TimestampMsValue::try_from(nanos).is_err());
            assert!(TimestampValue::try_from(nanos).is_err());
            assert!(TimestampTzValue::try_from(nanos.and_utc()).is_err());
            assert!(TimeValue::try_from(nanos.time()).is_err());

            let leap = ::chrono::NaiveDate::from_ymd_opt(2016, 12, 31)
                .unwrap()
                .and_hms_nano_opt(23, 59, 59, 1_000_000_000)
                .unwrap();
            assert!(TimeValue::try_from(leap.time()).is_err());
            assert!(TimeNsValue::try_from(leap.time()).is_err());
            assert!(TimestampValue::try_from(leap).is_err());
            assert!(TimestampSecValue::try_from(leap).is_err());
            assert!(TimestampMsValue::try_from(leap).is_err());
            assert!(TimestampNsValue::try_from(leap).is_err());
            assert!(TimestampTzValue::try_from(leap.and_utc()).is_err());
            assert!(TimestampTzNsValue::try_from(leap.and_utc()).is_err());

            for value in [::chrono::NaiveDateTime::MIN, ::chrono::NaiveDateTime::MAX] {
                assert!(TimestampNsValue::try_from(value).is_err());
                assert!(TimestampTzNsValue::try_from(value.and_utc()).is_err());
            }
            for raw in [i64::MAX, -i64::MAX] {
                let value = ::chrono::DateTime::from_timestamp_nanos(raw);
                assert!(TimestampNsValue::try_from(value.naive_utc()).is_err());
                assert!(TimestampTzNsValue::try_from(value).is_err());
            }
        }

        #[test]
        fn test_date_epoch_round_trip() {
            for (year, month, day, days) in [(1969, 12, 31, -1), (1970, 1, 1, 0), (1992, 9, 20, 8298)] {
                let date = ::chrono::NaiveDate::from_ymd_opt(year, month, day).unwrap();
                let raw = DateValue::from(date);
                assert_eq!(raw.0, days);
                assert_eq!(Option::<::chrono::NaiveDate>::from(raw), Some(date));
            }
        }

        #[test]
        fn test_date() -> crate::Result<()> {
            let env = Environment::new()?;
            let instance = env.open(StorageLocation::InMemory)?;
            let conn = instance.connect()?;

            let mut result = conn.query("SELECT DATE '1992-09-20';", Parameters::None)?;

            let chunk = result.next_chunk()?.unwrap();
            let vec = chunk.get_vector_at::<DateValue>(0)?;

            assert_eq!(vec.len(), 1);
            let chrono = Option::<::chrono::NaiveDate>::from(*vec.get(0)?.unwrap()).unwrap();

            assert_eq!(chrono, ::chrono::NaiveDate::from_ymd_opt(1992, 9, 20).unwrap());

            Ok(())
        }

        #[test]
        fn test_timestamp_conversion() -> crate::Result<()> {
            let env = Environment::new()?;
            let instance = env.open(StorageLocation::InMemory)?;
            let conn = instance.connect()?;
            {
                let mut result =
                    conn.query("SELECT TIMESTAMP_NS '1992-09-20 11:30:00.123456789';", Parameters::None)?;
                let chunk = result.next_chunk()?.unwrap();
                let vec = chunk.get_vector_at::<TimestampNsValue>(0)?;
                let timestamp = Option::<::chrono::NaiveDateTime>::from(*vec.get(0)?.unwrap()).unwrap();
                assert_eq!(timestamp, naive("1992-09-20 11:30:00.123456789"));
            }
            {
                let mut result = conn.query("SELECT TIMESTAMP '1992-09-20 11:30:00.123456789';", Parameters::None)?;
                let chunk = result.next_chunk()?.unwrap();
                let vec = chunk.get_vector_at::<TimestampValue>(0)?;
                let val = *(vec.get(0)?.unwrap());
                let timestamp = Option::<::chrono::NaiveDateTime>::from(val).unwrap();
                assert_eq!(timestamp, naive("1992-09-20 11:30:00.123456"));
            }
            {
                let mut result =
                    conn.query("SELECT TIMESTAMP_MS '1992-09-20 11:30:00.123456789';", Parameters::None)?;
                let chunk = result.next_chunk()?.unwrap();
                let vec = chunk.get_vector_at::<TimestampMsValue>(0)?;
                let val = *(vec.get(0)?.unwrap());
                let timestamp = Option::<::chrono::NaiveDateTime>::from(val).unwrap();
                assert_eq!(timestamp, naive("1992-09-20 11:30:00.123"));
            }
            {
                let mut result = conn.query("SELECT TIMESTAMP_S '1992-09-20 11:30:00.123456789';", Parameters::None)?;
                let chunk = result.next_chunk()?.unwrap();
                let vec = chunk.get_vector_at::<TimestampSecValue>(0)?;
                let val = *(vec.get(0)?.unwrap());
                let timestamp = Option::<::chrono::NaiveDateTime>::from(val).unwrap();
                assert_eq!(timestamp, naive("1992-09-20 11:30:00"));
            }
            {
                let mut result = conn.query(
                    "SELECT TIMESTAMPTZ '1992-09-20 11:30:00.123456789+00:00';",
                    Parameters::None,
                )?;
                let chunk = result.next_chunk()?.unwrap();
                let vec = chunk.get_vector_at::<TimestampTzValue>(0)?;
                let val = *(vec.get(0)?.unwrap());
                let timestamp = Option::<::chrono::DateTime<::chrono::Utc>>::from(val).unwrap();
                assert_eq!(
                    timestamp,
                    ::chrono::DateTime::parse_from_rfc3339("1992-09-20T11:30:00.123456Z")
                        .unwrap()
                        .with_timezone(&::chrono::Utc)
                );
            }
            {
                let mut result = conn.query(
                    "SELECT TIMESTAMPTZ '1992-09-20 12:30:00.123456789+01:00';",
                    Parameters::None,
                )?;
                let chunk = result.next_chunk()?.unwrap();
                let vec = chunk.get_vector_at::<TimestampTzValue>(0)?;
                let val = *(vec.get(0)?.unwrap());
                let timestamp = Option::<::chrono::DateTime<::chrono::Utc>>::from(val).unwrap();
                assert_eq!(
                    timestamp,
                    ::chrono::DateTime::parse_from_rfc3339("1992-09-20T12:30:00.123456+01:00")
                        .unwrap()
                        .with_timezone(&::chrono::Utc)
                );
            }
            {
                let mut result = conn.query(
                    "SELECT TIMESTAMPTZ_NS '1992-09-20 11:30:00.123456789+00:00';",
                    Parameters::None,
                )?;
                let chunk = result.next_chunk()?.unwrap();
                let vector = chunk.get_vector_at::<TimestampTzNsValue>(0)?;
                let timestamp = Option::<::chrono::DateTime<::chrono::Utc>>::from(*vector.get(0)?.unwrap()).unwrap();
                assert_eq!(timestamp, naive("1992-09-20 11:30:00.123456789").and_utc());
            }
            Ok(())
        }

        #[test]
        fn test_temporal_infinity_conversion() -> crate::Result<()> {
            let env = Environment::new()?;
            let instance = env.open(StorageLocation::InMemory)?;
            let conn = instance.connect()?;
            let mut result = conn.query(
                "SELECT
                'infinity'::TIMESTAMP_NS,
                '-infinity'::TIMESTAMP_NS,
                'infinity'::TIMESTAMPTZ_NS,
                '-infinity'::TIMESTAMPTZ_NS,
                'infinity'::DATE,
                '-infinity'::DATE,
                'infinity'::TIMESTAMP,
                '-infinity'::TIMESTAMP,
                'infinity'::TIMESTAMP_S,
                '-infinity'::TIMESTAMP_S,
                'infinity'::TIMESTAMP_MS,
                '-infinity'::TIMESTAMP_MS,
                'infinity'::TIMESTAMPTZ,
                '-infinity'::TIMESTAMPTZ;",
                Parameters::None,
            )?;
            let chunk = result.next_chunk()?.unwrap();

            for index in 0..2 {
                let vector = chunk.get_vector_at::<TimestampNsValue>(index)?;
                assert!(Option::<::chrono::NaiveDateTime>::from(*vector.get(0)?.unwrap()).is_none());
            }
            for index in 2..4 {
                let vector = chunk.get_vector_at::<TimestampTzNsValue>(index)?;
                assert!(Option::<::chrono::DateTime<::chrono::Utc>>::from(*vector.get(0)?.unwrap()).is_none());
            }
            for index in 4..6 {
                let vector = chunk.get_vector_at::<DateValue>(index)?;
                assert!(Option::<::chrono::NaiveDate>::from(*vector.get(0)?.unwrap()).is_none());
            }
            for index in 6..8 {
                let vector = chunk.get_vector_at::<TimestampValue>(index)?;
                assert!(Option::<::chrono::NaiveDateTime>::from(*vector.get(0)?.unwrap()).is_none());
            }
            for index in 8..10 {
                let vector = chunk.get_vector_at::<TimestampSecValue>(index)?;
                assert!(Option::<::chrono::NaiveDateTime>::from(*vector.get(0)?.unwrap()).is_none());
            }
            for index in 10..12 {
                let vector = chunk.get_vector_at::<TimestampMsValue>(index)?;
                assert!(Option::<::chrono::NaiveDateTime>::from(*vector.get(0)?.unwrap()).is_none());
            }
            for index in 12..14 {
                let vector = chunk.get_vector_at::<TimestampTzValue>(index)?;
                assert!(Option::<::chrono::DateTime<::chrono::Utc>>::from(*vector.get(0)?.unwrap()).is_none());
            }

            Ok(())
        }

        #[test]
        fn test_time_out_of_range_conversion() {
            let wrapping_micros = ((1_i64 << 32) + 5) * 1_000_000;
            for raw in [-1, i64::MIN, i64::MAX, wrapping_micros, 86_400_000_000] {
                assert!(Option::<::chrono::NaiveTime>::from(TimeValue(raw)).is_none());
            }
            for raw in [-1, i64::MIN, i64::MAX, wrapping_micros * 1_000, 86_400_000_000_000] {
                assert!(Option::<::chrono::NaiveTime>::from(TimeNsValue(raw)).is_none());
            }
        }

        #[test]
        fn test_time_conversion() -> crate::Result<()> {
            let env = Environment::new()?;
            let instance = env.open(StorageLocation::InMemory)?;
            let conn = instance.connect()?;

            {
                let mut result = conn.query("SELECT TIME '1992-09-20 11:30:00.123456';", Parameters::None)?;
                let chunk = result.next_chunk()?.unwrap();
                let vec = chunk.get_vector_at::<TimeValue>(0)?;
                let val = *(vec.get(0)?.unwrap());
                let time = Option::<::chrono::NaiveTime>::from(val).unwrap();
                assert_eq!(
                    time,
                    ::chrono::NaiveTime::parse_from_str("11:30:00.123456", "%H:%M:%S%.f").unwrap()
                );
            }
            {
                let mut result = conn.query("SELECT '15:30:00.123456789'::TIME_NS;", Parameters::None)?;
                let chunk = result.next_chunk()?.unwrap();
                let vec = chunk.get_vector_at::<TimeNsValue>(0)?;
                let val = *(vec.get(0)?.unwrap());
                let time = Option::<::chrono::NaiveTime>::from(val).unwrap();
                assert_eq!(
                    time,
                    ::chrono::NaiveTime::parse_from_str("15:30:00.123456789", "%H:%M:%S%.f").unwrap()
                );
            }

            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        Parameters,
        environment::{Environment, StorageLocation},
    };

    #[test]
    fn test_temporal_ordering_and_hashing() {
        fn check<T: Copy + std::fmt::Debug + Ord + std::hash::Hash>(negative: T, zero: T, positive: T) {
            let ordered = std::collections::BTreeSet::from([positive, negative, zero, zero]);
            assert_eq!(ordered.into_iter().collect::<Vec<_>>(), [negative, zero, positive]);
            let hashed = std::collections::HashSet::from([negative, zero, zero, positive]);
            assert_eq!(hashed.len(), 3);
            assert!(hashed.contains(&zero));
        }

        check(super::DateValue(-1), super::DateValue(0), super::DateValue(1));
        check(super::TimeValue(-1), super::TimeValue(0), super::TimeValue(1));
        check(super::TimeNsValue(-1), super::TimeNsValue(0), super::TimeNsValue(1));
        check(
            super::TimestampValue(-1),
            super::TimestampValue(0),
            super::TimestampValue(1),
        );
        check(
            super::TimestampSecValue(-1),
            super::TimestampSecValue(0),
            super::TimestampSecValue(1),
        );
        check(
            super::TimestampMsValue(-1),
            super::TimestampMsValue(0),
            super::TimestampMsValue(1),
        );
        check(
            super::TimestampNsValue(-1),
            super::TimestampNsValue(0),
            super::TimestampNsValue(1),
        );
        check(
            super::TimestampTzValue(-1),
            super::TimestampTzValue(0),
            super::TimestampTzValue(1),
        );
        check(
            super::TimestampTzNsValue(-1),
            super::TimestampTzNsValue(0),
            super::TimestampTzNsValue(1),
        );
    }

    #[test]
    fn test_time_tz_packing() -> crate::Result<()> {
        let env = Environment::new()?;
        let instance = env.open(StorageLocation::InMemory)?;
        let conn = instance.connect()?;
        let mut result = conn.query("SELECT TIMETZ '12:34:56+01:30';", Parameters::None)?;
        let chunk = result.next_chunk()?.unwrap();
        let vector = chunk.get_vector_at::<super::TimeTzValue>(0)?;
        let value = *vector.get(0)?.unwrap();
        let micros = (12 * 3600 + 34 * 60 + 56) * 1_000_000;
        assert_eq!(value.micros(), micros);
        assert_eq!(value.offset_seconds(), 5400);
        assert_eq!(value, super::TimeTzValue::new(micros, 5400));

        for micros in [0, 1, 86_400_000_000] {
            for offset in [
                -super::TimeTzValue::MAX_OFFSET,
                -5400,
                0,
                5400,
                super::TimeTzValue::MAX_OFFSET,
            ] {
                let value = super::TimeTzValue::new(micros, offset);
                assert_eq!(value.micros(), micros);
                assert_eq!(value.offset_seconds(), offset);
            }
        }
        Ok(())
    }

    #[test]
    fn test_interval_conversion() -> crate::Result<()> {
        let env = Environment::new()?;
        let instance = env.open(StorageLocation::InMemory)?;
        let conn = instance.connect()?;

        let mut result = conn.query(
            "SELECT INTERVAL '1 year 2 months 3 days 4 hours 5 minutes 6 seconds';",
            Parameters::None,
        )?;
        let chunk = result.next_chunk()?.unwrap();
        let vec = chunk.get_vector_at::<super::IntervalValue>(0)?;
        let val = *(vec.get(0)?.unwrap());

        assert_eq!(val.months, 14);
        assert_eq!(val.days, 3);
        assert_eq!(val.micros, 14706000000);

        Ok(())
    }
}

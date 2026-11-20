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
    vector::WritableNativeElement,
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
        unsafe impl WritableNativeElement for $name {
            type Write = Self;

            #[inline]
            unsafe fn write(vector: &mut crate::vector::VectorWriter<'_, Self>, index: usize, value: &Self::Write,) -> Result<()> {
                // SAFETY: The caller guarantees a writable, flat, in-bounds row.
                unsafe { vector.write_internal(index, Some(*value)) }
            }
        }
    };
}

declare_storage_value!(
    "Days since 1970-01-01 represented as a DuckDB `DATE`.",
    DateValue,
    i32,
    Date,
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
    Time,
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
    TimeNs,
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
    TimeTz,
    TimeTz,
    ffi::duckdb_v2_value_get_time_tz
);
declare_storage_value!(
    "Microseconds since 1970-01-01 represented as a DuckDB `TIMESTAMP`.",
    TimestampValue,
    i64,
    Timestamp,
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
    TimestampSec,
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
    TimestampMs,
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
    TimestampNs,
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
    TimestampTz,
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
    TimestampTzNs,
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
        link.logical_type_create_from_id(LogicalTypeID::Interval, Parameters::None)
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

declare_vector_element!(IntervalValue, Interval);

// SAFETY: every field combination is a valid `INTERVAL`.
unsafe impl WritableNativeElement for IntervalValue {
    type Write = Self;

    #[inline]
    unsafe fn write(
        vector: &mut crate::vector::VectorWriter<'_, Self>,
        index: usize,
        value: &Self::Write,
    ) -> Result<()> {
        // SAFETY: The caller guarantees a writable, flat, in-bounds row.
        unsafe { vector.write_internal(index, Some(*value)) }
    }
}

#[cfg(any(feature = "chrono", feature = "jiff"))]
mod helpers {
    use crate::Result;

    pub(super) fn precision_error() -> crate::Error {
        crate::Error::invalid_input("timestamp precision cannot be represented exactly")
    }

    pub(super) fn finite(units: i64) -> Option<i64> {
        (units != i64::MAX && units != -i64::MAX).then_some(units)
    }

    pub(super) fn checked_timestamp_units(nanos: i128, units_per_second: i64) -> Result<i64> {
        let nanos_per_unit = i128::from(1_000_000_000 / units_per_second);
        if nanos % nanos_per_unit != 0 {
            return Err(precision_error());
        }
        let units = i64::try_from(nanos / nanos_per_unit)
            .map_err(|_| crate::Error::invalid_input("timestamp is out of range"))?;
        if finite(units).is_none() {
            return Err(crate::Error::invalid_input("timestamp is reserved for infinity"));
        }
        Ok(units)
    }
}

#[cfg(feature = "chrono")]
mod chrono;
#[cfg(feature = "jiff")]
mod jiff;

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
        let value = *vector.reader()?.get(0)?.unwrap();
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
        let val = *(vec.reader()?.get(0)?.unwrap());

        assert_eq!(val.months, 14);
        assert_eq!(val.days, 3);
        assert_eq!(val.micros, 14706000000);

        Ok(())
    }
}

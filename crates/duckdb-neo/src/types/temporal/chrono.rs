use ::chrono::Timelike;

use super::helpers;
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
    if nanos >= 1_000_000_000 {
        return Err(helpers::precision_error());
    }
    helpers::checked_timestamp_units(
        i128::from(seconds) * 1_000_000_000 + i128::from(nanos),
        units_per_second,
    )
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
    Some(::chrono::DateTime::from_timestamp_nanos(helpers::finite(value)?))
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
        let chrono = Option::<::chrono::NaiveDate>::from(*vec.reader()?.get(0)?.unwrap()).unwrap();

        assert_eq!(chrono, ::chrono::NaiveDate::from_ymd_opt(1992, 9, 20).unwrap());

        Ok(())
    }

    #[test]
    fn test_timestamp_conversion() -> crate::Result<()> {
        let env = Environment::new()?;
        let instance = env.open(StorageLocation::InMemory)?;
        let conn = instance.connect()?;
        {
            let mut result = conn.query("SELECT TIMESTAMP_NS '1992-09-20 11:30:00.123456789';", Parameters::None)?;
            let chunk = result.next_chunk()?.unwrap();
            let vec = chunk.get_vector_at::<TimestampNsValue>(0)?;
            let timestamp = Option::<::chrono::NaiveDateTime>::from(*vec.reader()?.get(0)?.unwrap()).unwrap();
            assert_eq!(timestamp, naive("1992-09-20 11:30:00.123456789"));
        }
        {
            let mut result = conn.query("SELECT TIMESTAMP '1992-09-20 11:30:00.123456789';", Parameters::None)?;
            let chunk = result.next_chunk()?.unwrap();
            let vec = chunk.get_vector_at::<TimestampValue>(0)?;
            let val = *(vec.reader()?.get(0)?.unwrap());
            let timestamp = Option::<::chrono::NaiveDateTime>::from(val).unwrap();
            assert_eq!(timestamp, naive("1992-09-20 11:30:00.123456"));
        }
        {
            let mut result = conn.query("SELECT TIMESTAMP_MS '1992-09-20 11:30:00.123456789';", Parameters::None)?;
            let chunk = result.next_chunk()?.unwrap();
            let vec = chunk.get_vector_at::<TimestampMsValue>(0)?;
            let val = *(vec.reader()?.get(0)?.unwrap());
            let timestamp = Option::<::chrono::NaiveDateTime>::from(val).unwrap();
            assert_eq!(timestamp, naive("1992-09-20 11:30:00.123"));
        }
        {
            let mut result = conn.query("SELECT TIMESTAMP_S '1992-09-20 11:30:00.123456789';", Parameters::None)?;
            let chunk = result.next_chunk()?.unwrap();
            let vec = chunk.get_vector_at::<TimestampSecValue>(0)?;
            let val = *(vec.reader()?.get(0)?.unwrap());
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
            let val = *(vec.reader()?.get(0)?.unwrap());
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
            let val = *(vec.reader()?.get(0)?.unwrap());
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
            let timestamp =
                Option::<::chrono::DateTime<::chrono::Utc>>::from(*vector.reader()?.get(0)?.unwrap()).unwrap();
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
            assert!(Option::<::chrono::NaiveDateTime>::from(*vector.reader()?.get(0)?.unwrap()).is_none());
        }
        for index in 2..4 {
            let vector = chunk.get_vector_at::<TimestampTzNsValue>(index)?;
            assert!(Option::<::chrono::DateTime<::chrono::Utc>>::from(*vector.reader()?.get(0)?.unwrap()).is_none());
        }
        for index in 4..6 {
            let vector = chunk.get_vector_at::<DateValue>(index)?;
            assert!(Option::<::chrono::NaiveDate>::from(*vector.reader()?.get(0)?.unwrap()).is_none());
        }
        for index in 6..8 {
            let vector = chunk.get_vector_at::<TimestampValue>(index)?;
            assert!(Option::<::chrono::NaiveDateTime>::from(*vector.reader()?.get(0)?.unwrap()).is_none());
        }
        for index in 8..10 {
            let vector = chunk.get_vector_at::<TimestampSecValue>(index)?;
            assert!(Option::<::chrono::NaiveDateTime>::from(*vector.reader()?.get(0)?.unwrap()).is_none());
        }
        for index in 10..12 {
            let vector = chunk.get_vector_at::<TimestampMsValue>(index)?;
            assert!(Option::<::chrono::NaiveDateTime>::from(*vector.reader()?.get(0)?.unwrap()).is_none());
        }
        for index in 12..14 {
            let vector = chunk.get_vector_at::<TimestampTzValue>(index)?;
            assert!(Option::<::chrono::DateTime<::chrono::Utc>>::from(*vector.reader()?.get(0)?.unwrap()).is_none());
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
            let val = *(vec.reader()?.get(0)?.unwrap());
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
            let val = *(vec.reader()?.get(0)?.unwrap());
            let time = Option::<::chrono::NaiveTime>::from(val).unwrap();
            assert_eq!(
                time,
                ::chrono::NaiveTime::parse_from_str("15:30:00.123456789", "%H:%M:%S%.f").unwrap()
            );
        }

        Ok(())
    }
}

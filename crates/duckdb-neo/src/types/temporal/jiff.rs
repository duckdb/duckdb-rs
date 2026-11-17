use ::jiff::{SignedDuration, Span, Timestamp, civil};

use super::helpers::{checked_timestamp_units, finite};
use crate::types::{DateValue, IntervalValue, TimeNsValue, TimeValue};
use crate::types::{TimestampMsValue, TimestampNsValue, TimestampSecValue, TimestampValue};
use crate::types::{TimestampTzNsValue, TimestampTzValue};

const UNIX_EPOCH: civil::DateTime = civil::DateTime::constant(1970, 1, 1, 0, 0, 0, 0);

/// Returns `None` for infinity or dates outside jiff's range.
impl From<DateValue> for Option<civil::Date> {
    fn from(value: DateValue) -> Self {
        UNIX_EPOCH
            .date()
            .checked_add(SignedDuration::from_hours(i64::from(value.0) * 24))
            .ok()
    }
}

impl From<civil::Date> for DateValue {
    fn from(value: civil::Date) -> Self {
        Self((value.duration_since(UNIX_EPOCH.date()).as_hours() / 24) as i32)
    }
}

/// Returns `None` for times outside midnight (inclusive) to 24:00 (exclusive).
impl From<TimeValue> for Option<civil::Time> {
    fn from(value: TimeValue) -> Self {
        civil::Time::MIN.checked_add(SignedDuration::from_micros(value.0)).ok()
    }
}

/// Returns `None` for times outside midnight (inclusive) to 24:00 (exclusive).
impl From<TimeNsValue> for Option<civil::Time> {
    fn from(value: TimeNsValue) -> Self {
        civil::Time::MIN.checked_add(SignedDuration::from_nanos(value.0)).ok()
    }
}

impl TryFrom<civil::Time> for TimeValue {
    type Error = crate::Error;

    fn try_from(value: civil::Time) -> crate::Result<Self> {
        checked_timestamp_units(value.duration_since(civil::Time::MIN).as_nanos(), 1_000_000).map(Self)
    }
}

impl From<civil::Time> for TimeNsValue {
    fn from(value: civil::Time) -> Self {
        Self(value.duration_since(civil::Time::MIN).as_nanos() as i64)
    }
}

// jiff has no time-with-offset type.

trait EpochOffset: Sized {
    fn from_epoch(duration: SignedDuration) -> Option<Self>;
    fn epoch_nanos(self) -> i128;
}

impl EpochOffset for civil::DateTime {
    fn from_epoch(duration: SignedDuration) -> Option<Self> {
        UNIX_EPOCH.checked_add(duration).ok()
    }

    fn epoch_nanos(self) -> i128 {
        self.duration_since(UNIX_EPOCH).as_nanos()
    }
}

impl EpochOffset for Timestamp {
    fn from_epoch(duration: SignedDuration) -> Option<Self> {
        Timestamp::from_duration(duration).ok()
    }

    fn epoch_nanos(self) -> i128 {
        self.as_nanosecond()
    }
}

macro_rules! timestamp_conversions {
    ($name:ident, $target:ty, $units_per_second:literal, $duration:path) => {
        /// Returns `None` for infinity or timestamps outside jiff's range.
        impl From<$name> for Option<$target> {
            fn from(value: $name) -> Self {
                <$target>::from_epoch($duration(finite(value.0)?))
            }
        }

        impl TryFrom<$target> for $name {
            type Error = crate::Error;

            fn try_from(value: $target) -> crate::Result<Self> {
                checked_timestamp_units(value.epoch_nanos(), $units_per_second).map(Self)
            }
        }
    };
}

// Timestamps without a time zone map to `civil::DateTime`; only the TZ variants map to `Timestamp`.
timestamp_conversions!(TimestampValue, civil::DateTime, 1_000_000, SignedDuration::from_micros);
timestamp_conversions!(TimestampSecValue, civil::DateTime, 1, SignedDuration::from_secs);
timestamp_conversions!(TimestampMsValue, civil::DateTime, 1_000, SignedDuration::from_millis);
timestamp_conversions!(
    TimestampNsValue,
    civil::DateTime,
    1_000_000_000,
    SignedDuration::from_nanos
);
timestamp_conversions!(TimestampTzValue, Timestamp, 1_000_000, SignedDuration::from_micros);
timestamp_conversions!(TimestampTzNsValue, Timestamp, 1_000_000_000, SignedDuration::from_nanos);

/// Months and days stay calendar units, so adding the span to a `civil::DateTime` matches DuckDB `TIMESTAMP` math.
/// Returns `None` for mixed signs (such as `1 month -1 day`) or components beyond jiff's limits.
impl From<IntervalValue> for Option<Span> {
    fn from(value: IntervalValue) -> Self {
        let signs = [value.months.signum(), value.days.signum(), value.micros.signum() as i32];
        if signs.contains(&1) && signs.contains(&-1) {
            return None;
        }
        Span::new()
            .try_months(value.months)
            .ok()?
            .try_days(value.days)
            .ok()?
            .try_microseconds(value.micros)
            .ok()
    }
}

/// Folds years into months, weeks into days, and every time unit into microseconds.
impl TryFrom<Span> for IntervalValue {
    type Error = crate::Error;

    fn try_from(value: Span) -> crate::Result<Self> {
        let nanos = i128::from(value.get_hours()) * 3_600_000_000_000
            + i128::from(value.get_minutes()) * 60_000_000_000
            + i128::from(value.get_seconds()) * 1_000_000_000
            + i128::from(value.get_milliseconds()) * 1_000_000
            + i128::from(value.get_microseconds()) * 1_000
            + i128::from(value.get_nanoseconds());
        if nanos % 1_000 != 0 {
            return Err(crate::Error::invalid_input(
                "interval precision cannot be represented exactly",
            ));
        }
        Ok(Self {
            months: i32::from(value.get_years()) * 12 + value.get_months(),
            days: value.get_weeks() * 7 + value.get_days(),
            micros: i64::try_from(nanos / 1_000)
                .map_err(|_| crate::Error::invalid_input("interval is out of range"))?,
        })
    }
}

#[cfg(test)]
mod tests {
    use ::jiff::{Span, Timestamp, ToSpan, civil, tz::Offset};

    use crate::{
        Parameters,
        environment::{Environment, StorageLocation},
        types::{
            DateValue, IntervalValue, TimeNsValue, TimeValue, TimestampMsValue, TimestampNsValue, TimestampSecValue,
            TimestampTzNsValue, TimestampTzValue, TimestampValue,
        },
    };

    fn datetime(s: &str) -> civil::DateTime {
        s.parse().unwrap()
    }

    fn timestamp(s: &str) -> Timestamp {
        s.parse().unwrap()
    }

    #[test]
    fn test_checked_jiff_to_storage() -> crate::Result<()> {
        for text in ["1992-09-20 11:30:00", "1960-01-02 03:04:05"] {
            let value = datetime(text);
            assert_eq!(
                Option::<civil::DateTime>::from(TimestampValue::try_from(value)?),
                Some(value)
            );
            assert_eq!(
                Option::<civil::DateTime>::from(TimestampSecValue::try_from(value)?),
                Some(value)
            );
            assert_eq!(
                Option::<civil::DateTime>::from(TimestampMsValue::try_from(value)?),
                Some(value)
            );
            assert_eq!(
                Option::<civil::DateTime>::from(TimestampNsValue::try_from(value)?),
                Some(value)
            );
            let utc = Offset::UTC.to_timestamp(value).unwrap();
            assert_eq!(Option::<Timestamp>::from(TimestampTzValue::try_from(utc)?), Some(utc));
            assert_eq!(Option::<Timestamp>::from(TimestampTzNsValue::try_from(utc)?), Some(utc));
        }
        let micros = datetime("1960-01-02 03:04:05.123456");
        assert_eq!(
            Option::<civil::DateTime>::from(TimestampValue::try_from(micros)?),
            Some(micros)
        );
        let millis = datetime("1960-01-02 03:04:05.123");
        assert_eq!(
            Option::<civil::DateTime>::from(TimestampMsValue::try_from(millis)?),
            Some(millis)
        );
        let nanos = datetime("1960-01-02 03:04:05.123456789");
        assert_eq!(
            Option::<civil::DateTime>::from(TimestampNsValue::try_from(nanos)?),
            Some(nanos)
        );
        assert_eq!(
            Option::<civil::Time>::from(TimeValue::try_from(micros.time())?),
            Some(micros.time())
        );
        assert_eq!(
            Option::<civil::Time>::from(TimeNsValue::from(nanos.time())),
            Some(nanos.time())
        );
        let tz = timestamp("1960-01-02T03:04:05.123456789Z");
        assert_eq!(Option::<Timestamp>::from(TimestampTzNsValue::try_from(tz)?), Some(tz));

        assert_eq!(TimestampValue::try_from(micros)?, TimestampValue(-315_521_754_876_544));
        assert_eq!(TimeNsValue::from(civil::Time::MAX), TimeNsValue(86_399_999_999_999));
        Ok(())
    }

    #[test]
    fn test_checked_jiff_to_storage_rejects_loss_and_overflow() {
        let nanos = datetime("1992-09-20 11:30:00.123456789");
        assert!(TimestampSecValue::try_from(nanos).is_err());
        assert!(TimestampMsValue::try_from(nanos).is_err());
        assert!(TimestampValue::try_from(nanos).is_err());
        assert!(TimestampTzValue::try_from(timestamp("1992-09-20T11:30:00.123456789Z")).is_err());
        assert!(TimeValue::try_from(nanos.time()).is_err());

        for value in [civil::DateTime::MIN, civil::DateTime::MAX] {
            assert!(TimestampNsValue::try_from(value).is_err());
        }
        for value in [Timestamp::MIN, Timestamp::MAX] {
            assert!(TimestampTzNsValue::try_from(value).is_err());
        }
        for raw in [i64::MAX, -i64::MAX] {
            let value = Timestamp::from_nanosecond(i128::from(raw)).unwrap();
            assert!(TimestampTzNsValue::try_from(value).is_err());
            assert!(TimestampNsValue::try_from(Offset::UTC.to_datetime(value)).is_err());
        }
    }

    #[test]
    fn test_jiff_range_limits() -> crate::Result<()> {
        let max_second = civil::DateTime::constant(9999, 12, 31, 23, 59, 59, 0);
        for value in [civil::DateTime::MIN, max_second] {
            assert_eq!(
                Option::<civil::DateTime>::from(TimestampSecValue::try_from(value)?),
                Some(value)
            );
        }
        let max_micros = civil::DateTime::constant(9999, 12, 31, 23, 59, 59, 999_999_000);
        assert_eq!(
            Option::<civil::DateTime>::from(TimestampValue::try_from(max_micros)?),
            Some(max_micros)
        );
        let max_timestamp = Timestamp::constant(Timestamp::MAX.as_second(), 999_999_000);
        for value in [Timestamp::MIN, max_timestamp] {
            assert_eq!(
                Option::<Timestamp>::from(TimestampTzValue::try_from(value)?),
                Some(value)
            );
        }

        let below_min = TimestampSecValue::try_from(civil::DateTime::MIN)?.0 - 1;
        let above_max = TimestampSecValue::try_from(max_second)?.0 + 1;
        for raw in [below_min, above_max] {
            assert!(Option::<civil::DateTime>::from(TimestampSecValue(raw)).is_none());
        }
        let below_min = TimestampTzValue::try_from(Timestamp::MIN)?.0 - 1;
        let above_max = TimestampTzValue::try_from(max_timestamp)?.0 + 1;
        for raw in [below_min, above_max] {
            assert!(Option::<Timestamp>::from(TimestampTzValue(raw)).is_none());
        }
        Ok(())
    }

    #[test]
    fn test_date_epoch_round_trip() {
        for (year, month, day, days) in [(1969, 12, 31, -1), (1970, 1, 1, 0), (1992, 9, 20, 8298)] {
            let date = civil::Date::new(year, month, day).unwrap();
            let raw = DateValue::from(date);
            assert_eq!(raw.0, days);
            assert_eq!(Option::<civil::Date>::from(raw), Some(date));
        }
        for date in [civil::Date::MIN, civil::Date::MAX] {
            let raw = DateValue::from(date);
            assert_eq!(Option::<civil::Date>::from(raw), Some(date));
            assert!(Option::<civil::Date>::from(DateValue(raw.0 + raw.0.signum())).is_none());
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
        let date = Option::<civil::Date>::from(*vec.reader()?.get(0)?.unwrap()).unwrap();
        assert_eq!(date, civil::Date::new(1992, 9, 20).unwrap());

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
            let value = Option::<civil::DateTime>::from(*vec.reader()?.get(0)?.unwrap()).unwrap();
            assert_eq!(value, datetime("1992-09-20 11:30:00.123456789"));
        }
        {
            let mut result = conn.query("SELECT TIMESTAMP '1992-09-20 11:30:00.123456789';", Parameters::None)?;
            let chunk = result.next_chunk()?.unwrap();
            let vec = chunk.get_vector_at::<TimestampValue>(0)?;
            let value = Option::<civil::DateTime>::from(*vec.reader()?.get(0)?.unwrap()).unwrap();
            assert_eq!(value, datetime("1992-09-20 11:30:00.123456"));
        }
        {
            let mut result = conn.query("SELECT TIMESTAMP_MS '1992-09-20 11:30:00.123456789';", Parameters::None)?;
            let chunk = result.next_chunk()?.unwrap();
            let vec = chunk.get_vector_at::<TimestampMsValue>(0)?;
            let value = Option::<civil::DateTime>::from(*vec.reader()?.get(0)?.unwrap()).unwrap();
            assert_eq!(value, datetime("1992-09-20 11:30:00.123"));
        }
        {
            let mut result = conn.query("SELECT TIMESTAMP_S '1992-09-20 11:30:00.123456789';", Parameters::None)?;
            let chunk = result.next_chunk()?.unwrap();
            let vec = chunk.get_vector_at::<TimestampSecValue>(0)?;
            let value = Option::<civil::DateTime>::from(*vec.reader()?.get(0)?.unwrap()).unwrap();
            assert_eq!(value, datetime("1992-09-20 11:30:00"));
        }
        {
            let mut result = conn.query(
                "SELECT TIMESTAMPTZ '1992-09-20 12:30:00.123456789+01:00';",
                Parameters::None,
            )?;
            let chunk = result.next_chunk()?.unwrap();
            let vec = chunk.get_vector_at::<TimestampTzValue>(0)?;
            let value = Option::<Timestamp>::from(*vec.reader()?.get(0)?.unwrap()).unwrap();
            assert_eq!(value, timestamp("1992-09-20T11:30:00.123456Z"));
        }
        {
            let mut result = conn.query(
                "SELECT TIMESTAMPTZ_NS '1992-09-20 11:30:00.123456789+00:00';",
                Parameters::None,
            )?;
            let chunk = result.next_chunk()?.unwrap();
            let vec = chunk.get_vector_at::<TimestampTzNsValue>(0)?;
            let value = Option::<Timestamp>::from(*vec.reader()?.get(0)?.unwrap()).unwrap();
            assert_eq!(value, timestamp("1992-09-20T11:30:00.123456789Z"));
        }
        Ok(())
    }

    #[test]
    fn test_bind_jiff_parameters() -> crate::Result<()> {
        let env = Environment::new()?;
        let instance = env.open(StorageLocation::InMemory)?;
        let conn = instance.connect()?;

        let date = DateValue::from(civil::Date::new(1992, 9, 20).unwrap());
        let time = TimeValue::try_from(civil::Time::new(11, 30, 0, 123_456_000).unwrap())?;
        let ts = TimestampValue::try_from(datetime("1992-09-20 11:30:00.123456"))?;
        let tz = TimestampTzValue::try_from(timestamp("1992-09-20T11:30:00.123456Z"))?;
        let interval = IntervalValue::try_from(1.year().months(2).weeks(1).days(3).hours(4).milliseconds(5))?;
        let mut result = conn.query(
            "SELECT $1 = DATE '1992-09-20', $2 = TIME '11:30:00.123456', $3 = TIMESTAMP '1992-09-20 11:30:00.123456',
                $4 = TIMESTAMPTZ '1992-09-20 11:30:00.123456+00', $5 = INTERVAL '14 months 10 days 4 hours 5 milliseconds'",
            Parameters::positional(&[&date, &time, &ts, &tz, &interval]),
        )?;
        let chunk = result.next_chunk()?.unwrap();
        for index in 0..5 {
            assert_eq!(chunk.get_vector_at::<bool>(index)?.reader()?.get(0)?, Some(&true));
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
            assert!(Option::<civil::DateTime>::from(*vector.reader()?.get(0)?.unwrap()).is_none());
        }
        for index in 2..4 {
            let vector = chunk.get_vector_at::<TimestampTzNsValue>(index)?;
            assert!(Option::<Timestamp>::from(*vector.reader()?.get(0)?.unwrap()).is_none());
        }
        for index in 4..6 {
            let vector = chunk.get_vector_at::<DateValue>(index)?;
            assert!(Option::<civil::Date>::from(*vector.reader()?.get(0)?.unwrap()).is_none());
        }
        for index in 6..8 {
            let vector = chunk.get_vector_at::<TimestampValue>(index)?;
            assert!(Option::<civil::DateTime>::from(*vector.reader()?.get(0)?.unwrap()).is_none());
        }
        for index in 8..10 {
            let vector = chunk.get_vector_at::<TimestampSecValue>(index)?;
            assert!(Option::<civil::DateTime>::from(*vector.reader()?.get(0)?.unwrap()).is_none());
        }
        for index in 10..12 {
            let vector = chunk.get_vector_at::<TimestampMsValue>(index)?;
            assert!(Option::<civil::DateTime>::from(*vector.reader()?.get(0)?.unwrap()).is_none());
        }
        for index in 12..14 {
            let vector = chunk.get_vector_at::<TimestampTzValue>(index)?;
            assert!(Option::<Timestamp>::from(*vector.reader()?.get(0)?.unwrap()).is_none());
        }

        Ok(())
    }

    #[test]
    fn test_time_out_of_range_conversion() {
        let wrapping_micros = ((1_i64 << 32) + 5) * 1_000_000;
        for raw in [-1, i64::MIN, i64::MAX, wrapping_micros, 86_400_000_000] {
            assert!(Option::<civil::Time>::from(TimeValue(raw)).is_none());
        }
        for raw in [-1, i64::MIN, i64::MAX, wrapping_micros * 1_000, 86_400_000_000_000] {
            assert!(Option::<civil::Time>::from(TimeNsValue(raw)).is_none());
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
            let time = Option::<civil::Time>::from(*vec.reader()?.get(0)?.unwrap()).unwrap();
            assert_eq!(time, civil::Time::new(11, 30, 0, 123_456_000).unwrap());
        }
        {
            let mut result = conn.query("SELECT '15:30:00.123456789'::TIME_NS;", Parameters::None)?;
            let chunk = result.next_chunk()?.unwrap();
            let vec = chunk.get_vector_at::<TimeNsValue>(0)?;
            let time = Option::<civil::Time>::from(*vec.reader()?.get(0)?.unwrap()).unwrap();
            assert_eq!(time, civil::Time::new(15, 30, 0, 123_456_789).unwrap());
        }
        Ok(())
    }

    #[test]
    fn test_interval_conversion() -> crate::Result<()> {
        let env = Environment::new()?;
        let instance = env.open(StorageLocation::InMemory)?;
        let conn = instance.connect()?;

        let mut result = conn.query(
            "SELECT INTERVAL '1 year 2 months 3 days 4 hours 5 minutes 6 seconds',
                -INTERVAL '1 year 2 months 3 days 4 hours 5 minutes 6 seconds',
                INTERVAL '1 month -1 day';",
            Parameters::None,
        )?;
        let chunk = result.next_chunk()?.unwrap();
        let positive = Option::<Span>::from(*chunk.get_vector_at::<IntervalValue>(0)?.reader()?.get(0)?.unwrap());
        let expected = 14.months().days(3).microseconds(14_706_000_000_i64);
        assert_eq!(positive.unwrap().fieldwise(), expected);
        let negative = Option::<Span>::from(*chunk.get_vector_at::<IntervalValue>(1)?.reader()?.get(0)?.unwrap());
        assert_eq!(negative.unwrap().fieldwise(), expected.negate());
        let mixed = *chunk.get_vector_at::<IntervalValue>(2)?.reader()?.get(0)?.unwrap();
        assert!(Option::<Span>::from(mixed).is_none());
        Ok(())
    }

    #[test]
    fn test_span_matches_timestamp_arithmetic() -> crate::Result<()> {
        let env = Environment::new()?;
        let instance = env.open(StorageLocation::InMemory)?;
        let conn = instance.connect()?;

        for (start, interval) in [
            ("2024-01-31 12:00:00", "1 month 1 day 25 hours"),
            ("2024-03-31 12:00:00", "1 month 1 day"),
            ("2023-02-28 23:59:59.999999", "1 year 1 microsecond"),
        ] {
            let mut result = conn.query(
                format!(
                    "SELECT TIMESTAMP '{start}' + INTERVAL '{interval}', TIMESTAMP '{start}' - INTERVAL '{interval}',
                        INTERVAL '{interval}';"
                ),
                Parameters::None,
            )?;
            let chunk = result.next_chunk()?.unwrap();
            let read = |index| -> crate::Result<civil::DateTime> {
                Ok(Option::<civil::DateTime>::from(
                    *chunk.get_vector_at::<TimestampValue>(index)?.reader()?.get(0)?.unwrap(),
                )
                .unwrap())
            };
            let span =
                Option::<Span>::from(*chunk.get_vector_at::<IntervalValue>(2)?.reader()?.get(0)?.unwrap()).unwrap();
            let start = datetime(start);
            assert_eq!(start.checked_add(span).unwrap(), read(0)?);
            assert_eq!(start.checked_sub(span).unwrap(), read(1)?);
        }
        Ok(())
    }

    #[test]
    fn test_span_to_interval() -> crate::Result<()> {
        let span = 1
            .year()
            .months(2)
            .weeks(1)
            .days(3)
            .hours(4)
            .minutes(5)
            .seconds(6)
            .milliseconds(7)
            .microseconds(8);
        let expected = IntervalValue {
            months: 14,
            days: 10,
            micros: 14_706_007_008,
        };
        assert_eq!(IntervalValue::try_from(span)?, expected);
        assert_eq!(
            IntervalValue::try_from(span.negate())?,
            IntervalValue {
                months: -14,
                days: -10,
                micros: -14_706_007_008,
            }
        );
        assert_eq!(IntervalValue::try_from(1_000.nanoseconds())?.micros, 1);
        assert!(IntervalValue::try_from(1.nanosecond()).is_err());

        let zero = IntervalValue {
            months: 0,
            days: 0,
            micros: 0,
        };
        assert!(Option::<Span>::from(zero).unwrap().is_zero());
        for value in [
            IntervalValue {
                months: i32::MAX,
                ..zero
            },
            IntervalValue { days: i32::MIN, ..zero },
            IntervalValue {
                micros: i64::MAX,
                ..zero
            },
        ] {
            assert!(Option::<Span>::from(value).is_none());
        }
        Ok(())
    }
}

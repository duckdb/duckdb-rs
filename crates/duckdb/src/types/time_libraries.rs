//! Convert most of the [Time Strings](https://duckdb.org/docs/stable/sql/functions/date) to chrono types.

#[cfg(feature = "chrono")]
use chrono::{Local, TimeZone, Utc};
use num_integer::Integer;

use crate::{
    types::{FromSql, FromSqlError, FromSqlResult, ToSql, ToSqlOutput, Value, ValueRef},
    Result,
};

macro_rules! iso_8601_datetime_format {
    ($s:expr) => {{
        match $s.len() {
            //2016-02-23 23:56:04
            19 => ($s, "%F %T"),
            //2016-02-23 23:56:04.789
            23 => ($s, "%F %T%.f"),
            //2016-02-23 23:56:04.789+00:00
            _ => ($s, "%F %T%.f%:z"),
        }
    }};
}

macro_rules! iso_8601_date_format {
    ($s:expr) => {{
        match $s.len() {
            //2016-02-23
            10 => ($s, "%F"),
            _ => {
                //2016-02-23
                (&$s[..10], "%F")
            }
        }
    }};
}

macro_rules! iso_8601_time_format {
    ($s:expr) => {{
        match $s.len() {
            //23:56
            5 => ($s, "%H:%M"),
            //23:56:04
            8 => ($s, "%T"),
            //13:38:47.144
            _ => ($s, "%T%.f"),
        }
    }};
}

/// ISO 8601 calendar date without timezone => "YYYY-MM-DD"
#[cfg(feature = "chrono")]
impl ToSql for chrono::NaiveDate {
    #[inline]
    fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
        let date_str = self.format("%F").to_string();
        Ok(ToSqlOutput::from(date_str))
    }
}

/// ISO 8601 calendar date without timezone => "YYYY-MM-DD"
#[cfg(feature = "jiff")]
impl ToSql for jiff::civil::Date {
    #[inline]
    fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
        let date_str = self.strftime("%F").to_string();
        Ok(ToSqlOutput::from(date_str))
    }
}

/// "YYYY-MM-DD" => ISO 8601 calendar date without timezone.
#[cfg(feature = "chrono")]
impl FromSql for chrono::NaiveDate {
    #[inline]
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        if let ValueRef::Date32(days) = value {
            return Ok(Self::from_epoch_days(days).unwrap());
        }

        if value.as_timestamp().is_ok() {
            return chrono::NaiveDateTime::column_result(value).map(|dt| dt.date());
        }

        if let Ok(s) = value.as_str() {
            let (s, format) = iso_8601_date_format!(s);
            return chrono::NaiveDate::parse_from_str(s, format).map_err(|e| FromSqlError::Other(e.into()));
        }

        Err(FromSqlError::InvalidType)
    }
}

/// "YYYY-MM-DD" => ISO 8601 calendar date without timezone.
#[cfg(feature = "jiff")]
impl FromSql for jiff::civil::Date {
    #[inline]
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        if let ValueRef::Date32(days) = value {
            return jiff::civil::date(1970, 1, 1)
                .checked_add(jiff::Span::new().days(days))
                .map_err(|e| FromSqlError::Other(e.into()));
        }

        if value.as_timestamp().is_ok() {
            return Ok(jiff::Zoned::column_result(value)?.date());
        }

        if let Ok(s) = value.as_str() {
            let (s, format) = iso_8601_date_format!(s);
            return jiff::civil::Date::strptime(format, s).map_err(|e| FromSqlError::Other(e.into()));
        }

        Err(FromSqlError::InvalidType)
    }
}

/// ISO 8601 time without timezone => "HH:MM:SS.SSS"
#[cfg(feature = "chrono")]
impl ToSql for chrono::NaiveTime {
    #[inline]
    fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
        let time_str = self.format("%T%.f").to_string();
        Ok(ToSqlOutput::from(time_str))
    }
}

/// ISO 8601 time without timezone => "HH:MM:SS.SSS"
#[cfg(feature = "jiff")]
impl ToSql for jiff::civil::Time {
    #[inline]
    fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
        let time_str = self.strftime("%T%.f").to_string();
        Ok(ToSqlOutput::from(time_str))
    }
}

/// "HH:MM"/"HH:MM:SS"/"HH:MM:SS.SSS" => ISO 8601 time without timezone.
#[cfg(feature = "chrono")]
impl FromSql for chrono::NaiveTime {
    #[inline]
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        if value.as_timestamp().is_ok() {
            return Ok(chrono::NaiveDateTime::column_result(value)?.time());
        }

        if let Ok(s) = value.as_str() {
            let (s, format) = iso_8601_time_format!(s);
            return chrono::NaiveTime::parse_from_str(s, format).map_err(|e| FromSqlError::Other(e.into()));
        }

        Err(FromSqlError::InvalidType)
    }
}

/// "HH:MM"/"HH:MM:SS"/"HH:MM:SS.SSS" => ISO 8601 time without timezone.
#[cfg(feature = "jiff")]
impl FromSql for jiff::civil::Time {
    #[inline]
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        if value.as_timestamp().is_ok() {
            return Ok(jiff::Zoned::column_result(value)?.time());
        }

        if let Ok(s) = value.as_str() {
            let (s, format) = iso_8601_time_format!(s);
            return jiff::civil::Time::strptime(format, s).map_err(|e| FromSqlError::Other(e.into()));
        }

        Err(FromSqlError::InvalidType)
    }
}

/// ISO 8601 combined date and time without timezone =>
/// "YYYY-MM-DD HH:MM:SS.SSS"
#[cfg(feature = "chrono")]
impl ToSql for chrono::NaiveDateTime {
    #[inline]
    fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
        let date_time_str = self.format("%F %T%.f").to_string();
        Ok(ToSqlOutput::from(date_time_str))
    }
}

/// ISO 8601 combined date and time without timezone =>
/// "YYYY-MM-DD HH:MM:SS.SSS"
#[cfg(feature = "jiff")]
impl ToSql for jiff::civil::DateTime {
    #[inline]
    fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
        let date_time_str = self.strftime("%F %T%.f").to_string();
        Ok(ToSqlOutput::from(date_time_str))
    }
}

/// "YYYY-MM-DD HH:MM:SS"/"YYYY-MM-DD HH:MM:SS.SSS" => ISO 8601 combined date
/// and time without timezone. ("YYYY-MM-DDTHH:MM:SS"/"YYYY-MM-DDTHH:MM:SS.SSS"
/// also supported)
#[cfg(feature = "chrono")]
impl FromSql for chrono::NaiveDateTime {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        use chrono::DateTime;

        if let Ok((secs, nsecs)) = value.as_timestamp() {
            return Ok(DateTime::from_timestamp(secs, nsecs as u32).unwrap().naive_utc());
        }

        if let Ok(s) = value.as_str() {
            let (s, format) = iso_8601_datetime_format!(s);
            return Self::parse_from_str(s, format).map_err(|err| FromSqlError::Other(Box::new(err)));
        }

        return Err(FromSqlError::InvalidType);
    }
}

/// "YYYY-MM-DD HH:MM:SS"/"YYYY-MM-DD HH:MM:SS.SSS" => ISO 8601 combined date
/// and time with or without timezone.
#[cfg(feature = "jiff")]
impl FromSql for jiff::Zoned {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        use jiff::civil;

        if let Ok((secs, nsecs)) = value.as_timestamp() {
            return Ok(jiff::Timestamp::new(secs, nsecs as i32)
                .unwrap()
                .to_zoned(jiff::tz::TimeZone::UTC));
        }

        if let Ok(s) = value.as_str() {
            let (s, format) = iso_8601_datetime_format!(s);
            match s.len() {
                //2016-02-23 23:56:04
                //2016-02-23 23:56:04.789
                19 | 23 => {
                    return civil::DateTime::strptime(format, s)
                        .and_then(|dt| dt.to_zoned(jiff::tz::TimeZone::UTC))
                        .map_err(|err| FromSqlError::Other(Box::new(err)))
                }
                //2016-02-23 23:56:04.789+00:00
                _ => return Self::strptime(format, s).map_err(|err| FromSqlError::Other(Box::new(err))),
            }
        }

        return Err(FromSqlError::InvalidType);
    }
}

/// "YYYY-MM-DD HH:MM:SS"/"YYYY-MM-DD HH:MM:SS.SSS" => ISO 8601 combined date
/// and time without timezone.
#[cfg(feature = "jiff")]
impl FromSql for jiff::civil::DateTime {
    #[inline]
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        if value.as_timestamp().is_ok() {
            return Ok(jiff::Zoned::column_result(value)?.datetime());
        }

        if let Ok(s) = value.as_str() {
            let (s, format) = iso_8601_datetime_format!(s);
            match s.len() {
                //2016-02-23 23:56:04
                //2016-02-23 23:56:04.789
                19 | 23 => return Self::strptime(format, s).map_err(|err| FromSqlError::Other(Box::new(err))),
                //2016-02-23 23:56:04.789+00:00
                _ => return Ok(jiff::Zoned::column_result(value)?.datetime()),
            }
        }

        return Err(FromSqlError::InvalidType);
    }
}

/// Date and time with time zone => UTC ISO 8601 timestamp
/// ("YYYY-MM-DD HH:MM:SS.SSS+00:00").
#[cfg(feature = "chrono")]
impl<Tz: TimeZone> ToSql for chrono::DateTime<Tz> {
    #[inline]
    fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
        let date_time_str = self.with_timezone(&Utc).format("%F %T%.f%:z").to_string();
        Ok(ToSqlOutput::from(date_time_str))
    }
}

/// Date and time with time zone => UTC ISO 8601 timestamp
/// ("YYYY-MM-DD HH:MM:SS.SSS+00:00").
#[cfg(feature = "jiff")]
impl ToSql for jiff::Zoned {
    #[inline]
    fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
        let date_time_str = self.strftime("%F %T%.f%:z").to_string();
        Ok(ToSqlOutput::from(date_time_str))
    }
}

/// ISO 8601 ("YYYY-MM-DD HH:MM:SS.SSS[+-]HH:MM") into `DateTime<Utc>`.
#[cfg(feature = "chrono")]
impl FromSql for chrono::DateTime<Utc> {
    #[inline]
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        chrono::NaiveDateTime::column_result(value).map(|dt| Utc.from_utc_datetime(&dt))
    }
}

/// ISO 8601 ("YYYY-MM-DD HH:MM:SS.SSS[+-]HH:MM") into `Timestamp`.
#[cfg(feature = "jiff")]
impl FromSql for jiff::Timestamp {
    #[inline]
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        if let Ok((secs, nsecs)) = value.as_timestamp() {
            Ok(jiff::Timestamp::new(secs, nsecs as i32).unwrap())
        } else {
            Ok(jiff::Zoned::column_result(value)?.timestamp())
        }
    }
}

/// ISO 8601 ("YYYY-MM-DD HH:MM:SS.SSS[+-]HH:MM") into `DateTime<Local>`.
#[cfg(feature = "chrono")]
impl FromSql for chrono::DateTime<Local> {
    #[inline]
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        let utc_dt = chrono::DateTime::<Utc>::column_result(value)?;
        Ok(utc_dt.with_timezone(&Local))
    }
}

#[cfg(feature = "chrono")]
impl FromSql for chrono::TimeDelta {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        match value {
            ValueRef::Interval { months, days, nanos } => {
                let days = days + (months * 30);
                let (additional_seconds, nanos) = nanos.div_mod_floor(&NANOS_PER_SECOND);
                let seconds = additional_seconds + (i64::from(days) * 24 * 3600);

                match nanos.try_into() {
                    Ok(nanos) => {
                        if let Some(duration) = Self::new(seconds, nanos) {
                            Ok(duration)
                        } else {
                            Err(FromSqlError::Other("Invalid duration".into()))
                        }
                    }
                    Err(err) => Err(FromSqlError::Other(format!("Invalid duration: {err}").into())),
                }
            }
            _ => Err(FromSqlError::InvalidType),
        }
    }
}

/// Interval => Balanced `jiff::Span`
#[cfg(feature = "jiff")]
impl FromSql for jiff::Span {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        match value {
            ValueRef::Interval { months, days, nanos } => {
                let (years, months) = months.div_mod_floor(&12);
                let (weeks, days) = days.div_mod_floor(&7);
                let (hours, nanos) = nanos.div_mod_floor(&3_600_000_000_000);
                let (minutes, nanos) = nanos.div_mod_floor(&60_000_000_000);
                let (seconds, nanos) = nanos.div_mod_floor(&1_000_000_000);
                let (milliseconds, nanos) = nanos.div_mod_floor(&1_000_000);
                let (microseconds, nanos) = nanos.div_mod_floor(&1_000);
                Ok(jiff::Span::new()
                    .years(years)
                    .months(months)
                    .weeks(weeks)
                    .days(days)
                    .hours(hours)
                    .minutes(minutes)
                    .seconds(seconds)
                    .milliseconds(milliseconds)
                    .microseconds(microseconds)
                    .nanoseconds(nanos))
            }
            _ => Err(FromSqlError::InvalidType),
        }
    }
}

const DAYS_PER_MONTH: i64 = 30;
const SECONDS_PER_DAY: i64 = 24 * 3600;
const NANOS_PER_SECOND: i64 = 1_000_000_000;
const NANOS_PER_DAY: i64 = SECONDS_PER_DAY * NANOS_PER_SECOND;

/// Loads the interval and converts to a duration assuming
/// that there are 30 days in a month, and 24 hours in a day.
/// Use `jiff::Span` for more accurate conversions
#[cfg(feature = "jiff")]
impl FromSql for jiff::SignedDuration {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        match value {
            ValueRef::Interval { months, days, nanos } => {
                let days = months as i64 * DAYS_PER_MONTH + days as i64;
                Ok(jiff::SignedDuration::from_nanos(days * NANOS_PER_DAY + nanos))
            }
            _ => Err(FromSqlError::InvalidType),
        }
    }
}

#[cfg(feature = "chrono")]
impl ToSql for chrono::Duration {
    fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
        let nanos = self.num_nanoseconds().unwrap();
        let (days, nanos) = nanos.div_mod_floor(&NANOS_PER_DAY);
        let (months, days) = days.div_mod_floor(&DAYS_PER_MONTH);
        Ok(ToSqlOutput::Owned(Value::Interval {
            months: months.try_into().unwrap(),
            days: days.try_into().unwrap(),
            nanos,
        }))
    }
}

#[cfg(feature = "jiff")]
impl ToSql for jiff::Span {
    fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
        let months = self.get_years() as i32 * 12 + self.get_months();
        let days = self.get_weeks() * 7 + self.get_days();
        let nanos = self.get_hours() as i64 * 3_600_000_000_000
            + self.get_minutes() * 60_000_000_000
            + self.get_seconds() * 1_000_000_000
            + self.get_milliseconds() * 1_000_000
            + self.get_microseconds() * 1_000
            + self.get_nanoseconds();
        Ok(ToSqlOutput::Owned(Value::Interval { months, days, nanos }))
    }
}

/// Will store the duration in nanoseconds as an interval. Not using the
/// month or day units. This function doesn't work with durations longer
/// than i64::MAX nanoseconds (292 years). To store durations larger than
/// that, use a `jiff::Span`
#[cfg(feature = "jiff")]
impl ToSql for jiff::SignedDuration {
    fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::Owned(Value::Interval {
            months: 0,
            days: 0,
            nanos: self
                .as_nanos()
                .try_into()
                .map_err(|_| FromSqlError::OutOfRange(self.as_nanos()))?,
        }))
    }
}

#[cfg(test)]
mod test {
    use crate::{
        types::{FromSql, ToSql, ToSqlOutput, ValueRef},
        Connection, Result,
    };

    fn checked_memory_handle() -> Result<Connection> {
        let db = Connection::open_in_memory()?;
        db.execute_batch("CREATE TABLE foo (d DATE, t Text, i INTEGER, f FLOAT, b TIMESTAMP, tt time)")?;
        Ok(db)
    }

    #[test]
    #[cfg(feature = "chrono")]
    fn test_chrono_naive_time() -> Result<()> {
        use chrono::{Duration, NaiveTime};

        let db = checked_memory_handle()?;
        let time = NaiveTime::from_hms_micro_opt(23, 56, 4, 12_345).unwrap();
        db.execute("INSERT INTO foo (tt) VALUES (?)", [time])?;

        let s: String = db.query_row("SELECT tt FROM foo", [], |r| r.get(0))?;
        assert_eq!("23:56:04.012345", s);
        let t: NaiveTime = db.query_row("SELECT tt FROM foo", [], |r| r.get(0))?;
        assert_eq!(time, t);
        let t: NaiveTime = db.query_row("SELECT '23:56:04.012345'", [], |r| r.get(0))?;
        assert_eq!(time, t);
        let t: NaiveTime = db.query_row("SELECT '23:56:04'", [], |r| r.get(0))?;
        assert_eq!(time - Duration::microseconds(12345), t);
        Ok(())
    }

    #[test]
    #[cfg(feature = "jiff")]
    fn test_jiff_civil_time() -> Result<()> {
        use jiff::{civil::Time, ToSpan};

        let db = checked_memory_handle()?;
        let time = Time::new(23, 56, 4, 12_345_000).unwrap();
        db.execute("INSERT INTO foo (tt) VALUES (?)", [time])?;

        let s: String = db.query_row("SELECT tt FROM foo", [], |r| r.get(0))?;
        assert_eq!("23:56:04.012345", s);
        let t: Time = db.query_row("SELECT tt FROM foo", [], |r| r.get(0))?;
        assert_eq!(time, t);
        let t: Time = db.query_row("SELECT '23:56:04.012345'", [], |r| r.get(0))?;
        assert_eq!(time, t);
        let t: Time = db.query_row("SELECT '23:56:04'", [], |r| r.get(0))?;
        assert_eq!(time.saturating_sub(12345.microseconds()), t);
        Ok(())
    }

    #[test]
    #[cfg(feature = "chrono")]
    fn test_chrono_naive_date() -> Result<()> {
        use chrono::NaiveDate;

        let db = checked_memory_handle()?;
        let date = NaiveDate::from_ymd_opt(2016, 2, 23).unwrap();
        db.execute("INSERT INTO foo (d) VALUES (?)", [date])?;

        let s: String = db.query_row("SELECT d FROM foo", [], |r| r.get(0))?;
        assert_eq!("2016-02-23", s);
        let t: NaiveDate = db.query_row("SELECT d FROM foo", [], |r| r.get(0))?;
        assert_eq!(date, t);
        let t: NaiveDate = db.query_row("SELECT '2016-02-23'", [], |r| r.get(0))?;
        assert_eq!(date, t);
        Ok(())
    }

    #[test]
    #[cfg(feature = "jiff")]
    fn test_jiff_civil_date() -> Result<()> {
        use jiff::civil::{date, Date};

        let db = checked_memory_handle()?;
        let date = date(2016, 2, 23);
        db.execute("INSERT INTO foo (d) VALUES (?)", [date])?;

        let s: String = db.query_row("SELECT d FROM foo", [], |r| r.get(0))?;
        assert_eq!("2016-02-23", s);
        let t: Date = db.query_row("SELECT d FROM foo", [], |r| r.get(0))?;
        assert_eq!(date, t);
        let t: Date = db.query_row("SELECT '2016-02-23'", [], |r| r.get(0))?;
        assert_eq!(date, t);
        Ok(())
    }

    #[test]
    #[cfg(feature = "chrono")]
    fn test_chrono_naive_date_time() -> Result<()> {
        use chrono::{NaiveDate, NaiveDateTime, NaiveTime};

        let db = checked_memory_handle()?;
        let date = NaiveDate::from_ymd_opt(2016, 2, 23).unwrap();
        let time = NaiveTime::from_hms_opt(23, 56, 4).unwrap();
        let dt = NaiveDateTime::new(date, time);

        db.execute("INSERT INTO foo (b) VALUES (?)", [dt])?;

        let s: String = db.query_row("SELECT b FROM foo", [], |r| r.get(0))?;
        assert_eq!("2016-02-23 23:56:04", s);
        let v: NaiveDateTime = db.query_row("SELECT b FROM foo", [], |r| r.get(0))?;
        assert_eq!(dt, v);

        db.execute(
            "UPDATE foo set b = strftime(cast(b as datetime), '%Y-%m-%d %H:%M:%S')",
            [],
        )?; // "YYYY-MM-DD HH:MM:SS"
        let hms: NaiveDateTime = db.query_row("SELECT b FROM foo", [], |r| r.get(0))?;
        assert_eq!(dt, hms);
        Ok(())
    }

    #[test]
    #[cfg(feature = "jiff")]
    fn test_jiff_civil_date_time() -> Result<()> {
        use jiff::civil::DateTime;

        let db = checked_memory_handle()?;
        let dt = DateTime::new(2016, 2, 23, 23, 56, 4, 0).unwrap();

        db.execute("INSERT INTO foo (b) VALUES (?)", [dt])?;

        let s: String = db.query_row("SELECT b FROM foo", [], |r| r.get(0))?;
        assert_eq!("2016-02-23 23:56:04", s);
        let v: DateTime = db.query_row("SELECT b FROM foo", [], |r| r.get(0))?;
        assert_eq!(dt, v);

        db.execute(
            "UPDATE foo set b = strftime(cast(b as datetime), '%Y-%m-%d %H:%M:%S')",
            [],
        )?; // "YYYY-MM-DD HH:MM:SS"
        let hms: DateTime = db.query_row("SELECT b FROM foo", [], |r| r.get(0))?;
        assert_eq!(dt, hms);
        Ok(())
    }

    #[test]
    #[cfg(feature = "chrono")]
    fn test_chrono_date_time_utc() -> Result<()> {
        use chrono::{DateTime, Duration, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc};

        let db = checked_memory_handle()?;
        let date = NaiveDate::from_ymd_opt(2016, 2, 23).unwrap();
        let time = NaiveTime::from_hms_milli_opt(23, 56, 4, 789).unwrap();
        let dt = NaiveDateTime::new(date, time);
        let utc = Utc.from_utc_datetime(&dt);

        db.execute("INSERT INTO foo (b) VALUES (?)", [utc])?;

        let s: String = db.query_row("SELECT b FROM foo", [], |r| r.get(0))?;
        assert_eq!("2016-02-23 23:56:04.789", s);

        let v1: DateTime<Utc> = db.query_row("SELECT b FROM foo", [], |r| r.get(0))?;
        assert_eq!(utc, v1);

        let v2: DateTime<Utc> = db.query_row("SELECT '2016-02-23 23:56:04.789'", [], |r| r.get(0))?;
        assert_eq!(utc, v2);

        let v3: DateTime<Utc> = db.query_row("SELECT '2016-02-23 23:56:04'", [], |r| r.get(0))?;
        assert_eq!(utc - Duration::try_milliseconds(789).unwrap(), v3);

        let v4: DateTime<Utc> = db.query_row("SELECT '2016-02-23 23:56:04.789+00:00'", [], |r| r.get(0))?;
        assert_eq!(utc, v4);
        Ok(())
    }

    #[test]
    #[cfg(feature = "jiff")]
    fn test_jiff_zoned() -> Result<()> {
        use jiff::{civil::DateTime, tz::TimeZone, ToSpan, Zoned};

        let db = checked_memory_handle()?;
        let utc = DateTime::new(2016, 2, 23, 23, 56, 4, 789_000_000)
            .unwrap()
            .to_zoned(TimeZone::UTC)
            .unwrap();

        db.execute("INSERT INTO foo (b) VALUES (?)", [&utc])?;

        let s: String = db.query_row("SELECT b FROM foo", [], |r| r.get(0))?;
        assert_eq!("2016-02-23 23:56:04.789", s);

        let v1: Zoned = db.query_row("SELECT b FROM foo", [], |r| r.get(0))?;
        assert_eq!(utc, v1);

        let v2: Zoned = db.query_row("SELECT '2016-02-23 23:56:04.789'", [], |r| r.get(0))?;
        assert_eq!(utc, v2);

        let v3: Zoned = db.query_row("SELECT '2016-02-23 23:56:04'", [], |r| r.get(0))?;
        assert_eq!(utc.saturating_sub(789.milliseconds()), v3);

        let v4: Zoned = db.query_row("SELECT '2016-02-23 23:56:04.789+00:00'", [], |r| r.get(0))?;
        assert_eq!(utc, v4);
        Ok(())
    }

    #[test]
    #[cfg(feature = "chrono")]
    fn test_time_delta_roundtrip() {
        use chrono::TimeDelta;

        roundtrip_type(TimeDelta::new(3600, 0).unwrap());
        roundtrip_type(TimeDelta::new(3600, 1000).unwrap());
    }

    #[test]
    #[cfg(feature = "jiff")]
    fn test_signed_duration_roundtrip() {
        use jiff::SignedDuration;

        roundtrip_type(SignedDuration::new(3600, 0));
        roundtrip_type(SignedDuration::new(3600, 1000));
    }

    #[test]
    #[cfg(feature = "chrono")]
    fn test_time_delta() -> Result<()> {
        use chrono::TimeDelta;

        let db = checked_memory_handle()?;
        let td = TimeDelta::new(3600, 0).unwrap();

        let row: Result<TimeDelta> = db.query_row("SELECT ?", [td], |row| Ok(row.get(0)))?;

        assert_eq!(row.unwrap(), td);

        Ok(())
    }

    fn roundtrip_type<T: FromSql + ToSql + Eq + std::fmt::Debug>(td: T) {
        let sqled = td.to_sql().unwrap();
        let value = match sqled {
            ToSqlOutput::Borrowed(v) => v,
            ToSqlOutput::Owned(ref v) => ValueRef::from(v),
        };
        let reversed = FromSql::column_result(value).unwrap();

        assert_eq!(td, reversed);
    }

    #[test]
    #[cfg(feature = "chrono")]
    fn test_date_time_local() -> Result<()> {
        use chrono::{DateTime, Local, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc};

        let db = checked_memory_handle()?;
        let date = NaiveDate::from_ymd_opt(2016, 2, 23).unwrap();
        let time = NaiveTime::from_hms_milli_opt(23, 56, 4, 789).unwrap();
        let dt = NaiveDateTime::new(date, time);
        let local = Local.from_local_datetime(&dt).single().unwrap();

        db.execute("INSERT INTO foo (b) VALUES (?)", [local])?;

        let s: String = db.query_row("SELECT b FROM foo", [], |r| r.get(0))?;
        assert_eq!(DateTime::<Utc>::from(local).format("%F %T%.f").to_string(), s);

        let v: DateTime<Local> = db.query_row("SELECT b FROM foo", [], |r| r.get(0))?;
        assert_eq!(local, v);
        Ok(())
    }

    #[test]
    #[cfg(feature = "chrono")]
    fn test_duckdb_datetime_functions() -> Result<()> {
        use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};

        let db = checked_memory_handle()?;
        let result: Result<NaiveDate> = db.query_row("SELECT CURRENT_DATE", [], |r| r.get(0));
        assert!(result.is_ok());
        let result: Result<NaiveDateTime> = db.query_row("SELECT CURRENT_TIMESTAMP", [], |r| r.get(0));
        assert!(result.is_ok());
        let result: Result<DateTime<Utc>> = db.query_row("SELECT CURRENT_TIMESTAMP", [], |r| r.get(0));
        assert!(result.is_ok());
        let result: Result<NaiveTime> = db.query_row("SELECT CURRENT_TIME", [], |r| r.get(0));
        assert!(result.is_ok());
        Ok(())
    }

    #[test]
    #[cfg(feature = "chrono")]
    fn test_naive_date_time_param() -> Result<()> {
        use chrono::NaiveDateTime;

        let db = checked_memory_handle()?;
        let fixed_time = NaiveDateTime::parse_from_str("2023-01-01 12:00:00", "%Y-%m-%d %H:%M:%S").unwrap();
        let result: Result<bool> = db.query_row(
            "SELECT 1 WHERE ?::TIMESTAMP BETWEEN (TIMESTAMP '2023-01-01 11:59:00') AND (TIMESTAMP '2023-01-01 12:01:00')",
            [fixed_time],
            |r| r.get(0),
        );
        assert!(result.is_ok());
        Ok(())
    }

    #[test]
    #[cfg(feature = "chrono")]
    fn test_date_time_param() -> Result<()> {
        use chrono::{TimeZone, Utc};

        let db = checked_memory_handle()?;
        let fixed_time = Utc.with_ymd_and_hms(2023, 1, 1, 12, 0, 0).unwrap();
        let result: Result<bool> = db.query_row(
            "SELECT 1 WHERE ?::TIMESTAMPTZ BETWEEN (TIMESTAMPTZ '2023-01-01 11:59:00+00:00') AND (TIMESTAMPTZ '2023-01-01 12:01:00+00:00')",
            [fixed_time],
            |r| r.get(0),
        );
        assert!(result.is_ok());
        Ok(())
    }

    #[test]
    #[cfg(feature = "chrono")]
    fn test_lenient_parse_timezone() {
        use crate::types::FromSqlError;
        use chrono::{DateTime, Utc};

        // Not supported
        assert!(matches!(
            DateTime::<Utc>::column_result(ValueRef::Text(b"1970-01-01T00:00:00Z")),
            Err(FromSqlError::Other(_))
        ));
        assert!(matches!(
            DateTime::<Utc>::column_result(ValueRef::Text(b"1970-01-01T00:00:00+00")),
            Err(FromSqlError::Other(_))
        ));
    }
}

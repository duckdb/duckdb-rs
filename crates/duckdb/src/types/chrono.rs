//! Convert most of the [Time Strings](http://sqlite.org/lang_datefunc.html) to chrono types.

use chrono::{DateTime, Duration, Local, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc};
use num_integer::Integer;

use crate::{
    types::{FromSql, FromSqlError, FromSqlResult, TimeUnit, ToSql, ToSqlOutput, ValueRef},
    Result,
};

use super::Value;

/// ISO 8601 calendar date without timezone => "YYYY-MM-DD"
impl ToSql for NaiveDate {
    #[inline]
    fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
        let date_str = self.format("%F").to_string();
        Ok(ToSqlOutput::from(date_str))
    }
}

/// "YYYY-MM-DD" => ISO 8601 calendar date without timezone.
impl FromSql for NaiveDate {
    #[inline]
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        Ok(NaiveDateTime::column_result(value)?.date())
    }
}

/// ISO 8601 time without timezone => "HH:MM:SS.SSS"
impl ToSql for NaiveTime {
    #[inline]
    fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
        let date_str = self.format("%T%.f").to_string();
        Ok(ToSqlOutput::from(date_str))
    }
}

/// "HH:MM"/"HH:MM:SS"/"HH:MM:SS.SSS" => ISO 8601 time without timezone.
impl FromSql for NaiveTime {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        Ok(NaiveDateTime::column_result(value)?.time())
    }
}

/// ISO 8601 combined date and time without timezone =>
/// "YYYY-MM-DD HH:MM:SS.SSS"
impl ToSql for NaiveDateTime {
    #[inline]
    fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
        let date_str = self.format("%F %T%.f").to_string();
        Ok(ToSqlOutput::from(date_str))
    }
}

/// "YYYY-MM-DD HH:MM:SS"/"YYYY-MM-DD HH:MM:SS.SSS" => ISO 8601 combined date
/// and time without timezone. ("YYYY-MM-DDTHH:MM:SS"/"YYYY-MM-DDTHH:MM:SS.SSS"
/// also supported)
impl FromSql for NaiveDateTime {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        match value {
            ValueRef::Timestamp(tu, t) => {
                let (secs, nsecs) = match tu {
                    TimeUnit::Second => (t, 0),
                    TimeUnit::Millisecond => (t / 1000, (t % 1000) * 1_000_000),
                    TimeUnit::Microsecond => (t / 1_000_000, (t % 1_000_000) * 1000),
                    TimeUnit::Nanosecond => (t / 1_000_000_000, t % 1_000_000_000),
                };
                Ok(DateTime::from_timestamp(secs, nsecs as u32).unwrap().naive_utc())
            }
            ValueRef::Date32(d) => Ok(DateTime::from_timestamp(24 * 3600 * (d as i64), 0).unwrap().naive_utc()),
            ValueRef::Time64(TimeUnit::Microsecond, d) => Ok(DateTime::from_timestamp(
                d / 1_000_000,
                ((d % 1_000_000) * 1_000) as u32,
            )
            .unwrap()
            .naive_utc()),
            ValueRef::Text(s) => {
                let mut s = std::str::from_utf8(s).unwrap();
                let format = match s.len() {
                    //23:56:04
                    8 => "%T",
                    //2016-02-23
                    10 => "%F",
                    //13:38:47.144
                    12 => "%T%.f",
                    //2016-02-23 23:56:04
                    19 => "%F %T",
                    //2016-02-23 23:56:04.789
                    23 => "%F %T%.f",
                    //2016-02-23 23:56:04.789+00:00
                    29 => "%F %T%.f%:z",
                    _ => {
                        //2016-02-23
                        s = &s[..10];
                        "%F"
                    }
                };
                NaiveDateTime::parse_from_str(s, format).map_err(|err| FromSqlError::Other(Box::new(err)))
            }
            _ => Err(FromSqlError::InvalidType),
        }
    }
}

/// Date and time with time zone => UTC RFC3339 timestamp
/// ("YYYY-MM-DD HH:MM:SS.SSS+00:00").
impl<Tz: TimeZone> ToSql for DateTime<Tz> {
    #[inline]
    fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
        let date_str = self.with_timezone(&Utc).format("%F %T%.f%:z").to_string();
        Ok(ToSqlOutput::from(date_str))
    }
}

/// RFC3339 ("YYYY-MM-DD HH:MM:SS.SSS[+-]HH:MM") into `DateTime<Utc>`.
impl FromSql for DateTime<Utc> {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        NaiveDateTime::column_result(value).map(|dt| Utc.from_utc_datetime(&dt))
    }
}

/// RFC3339 ("YYYY-MM-DD HH:MM:SS.SSS[+-]HH:MM") into `DateTime<Local>`.
impl FromSql for DateTime<Local> {
    #[inline]
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        let utc_dt = DateTime::<Utc>::column_result(value)?;
        Ok(utc_dt.with_timezone(&Local))
    }
}

impl FromSql for Duration {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        match value {
            ValueRef::Interval { months, days, nanos } => {
                let days = days + (months * 30);
                let (additional_seconds, nanos) = nanos.div_mod_floor(&NANOS_PER_SECOND);
                let seconds = additional_seconds + (i64::from(days) * 24 * 3600);

                match nanos.try_into() {
                    Ok(nanos) => {
                        if let Some(duration) = Duration::new(seconds, nanos) {
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

const DAYS_PER_MONTH: i64 = 30;
const SECONDS_PER_DAY: i64 = 24 * 3600;
const NANOS_PER_SECOND: i64 = 1_000_000_000;
const NANOS_PER_DAY: i64 = SECONDS_PER_DAY * NANOS_PER_SECOND;

impl ToSql for Duration {
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

#[cfg(test)]
mod test {
    use crate::{
        types::{FromSql, ToSql, ToSqlOutput, ValueRef},
        Connection, Result,
    };
    use chrono::{DateTime, Duration, Local, NaiveDate, NaiveDateTime, NaiveTime, TimeDelta, TimeZone, Utc};

    fn checked_memory_handle() -> Result<Connection> {
        let db = Connection::open_in_memory()?;
        db.execute_batch("CREATE TABLE foo (d DATE, t Text, i INTEGER, f FLOAT, b TIMESTAMP, tt time)")?;
        Ok(db)
    }

    #[test]
    fn test_naive_time() -> Result<()> {
        let db = checked_memory_handle()?;
        let time = NaiveTime::from_hms_micro_opt(23, 56, 4, 12_345).unwrap();
        db.execute("INSERT INTO foo (tt) VALUES (?)", [time])?;

        let s: String = db.query_row("SELECT tt FROM foo", [], |r| r.get(0))?;
        assert_eq!("23:56:04.012345", s);
        let t: NaiveTime = db.query_row("SELECT tt FROM foo", [], |r| r.get(0))?;
        assert_eq!(time, t);
        Ok(())
    }

    #[test]
    fn test_naive_date() -> Result<()> {
        let db = checked_memory_handle()?;
        let date = NaiveDate::from_ymd_opt(2016, 2, 23).unwrap();
        db.execute("INSERT INTO foo (d) VALUES (?)", [date])?;

        let s: String = db.query_row("SELECT d FROM foo", [], |r| r.get(0))?;
        assert_eq!("2016-02-23", s);
        let t: NaiveDate = db.query_row("SELECT d FROM foo", [], |r| r.get(0))?;
        assert_eq!(date, t);
        Ok(())
    }

    #[test]
    fn test_naive_date_time() -> Result<()> {
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
    fn test_date_time_utc() -> Result<()> {
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
    fn test_time_delta_roundtrip() {
        roundtrip_type(TimeDelta::new(3600, 0).unwrap());
        roundtrip_type(TimeDelta::new(3600, 1000).unwrap());
    }

    #[test]
    fn test_time_delta() -> Result<()> {
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
    fn test_date_time_local() -> Result<()> {
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
    fn test_duckdb_datetime_functions() -> Result<()> {
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
    fn test_naive_date_time_param() -> Result<()> {
        let db = checked_memory_handle()?;
        let result: Result<bool> = db.query_row(
            "SELECT 1 WHERE ?::TIMESTAMP BETWEEN (now()::timestamp - INTERVAL '1 minute') AND (now()::timestamp + INTERVAL '1 minute')",
            [Utc::now().naive_utc()],
            |r| r.get(0),
        );
        assert!(result.is_ok());
        Ok(())
    }

    #[test]
    fn test_date_time_param() -> Result<()> {
        let db = checked_memory_handle()?;
        // TODO(wangfenjin): why need 2 params?
        let result: Result<bool> = db.query_row(
            "SELECT 1 WHERE ?::TIMESTAMP BETWEEN (now()::timestamp - INTERVAL '1 minute') AND (now()::timestamp + INTERVAL '1 minute')",
            [Utc::now()],
            |r| r.get(0),
        );
        println!("{result:?}");
        assert!(result.is_ok());
        Ok(())
    }

    #[test]
    #[ignore]
    fn test_lenient_parse_timezone() {
        assert!(DateTime::<Utc>::column_result(ValueRef::Text(b"1970-01-01T00:00:00Z")).is_ok());
        assert!(DateTime::<Utc>::column_result(ValueRef::Text(b"1970-01-01T00:00:00+00")).is_ok());
    }
}

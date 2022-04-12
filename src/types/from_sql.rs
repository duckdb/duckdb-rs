extern crate cast;

use super::{TimeUnit, Value, ValueRef};
use std::error::Error;
use std::fmt;

/// Enum listing possible errors from [`FromSql`] trait.
#[derive(Debug)]
#[non_exhaustive]
pub enum FromSqlError {
    /// Error when an DuckDB value is requested, but the type of the result
    /// cannot be converted to the requested Rust type.
    InvalidType,

    /// Error when the value returned by DuckDB cannot be stored into the
    /// requested type.
    OutOfRange(i128),

    /// `feature = "uuid"` Error returned when reading a `uuid` from a blob with
    /// a size other than 16. Only available when the `uuid` feature is enabled.
    #[cfg(feature = "uuid")]
    InvalidUuidSize(usize),

    /// An error case available for implementors of the [`FromSql`] trait.
    Other(Box<dyn Error + Send + Sync + 'static>),
}

impl PartialEq for FromSqlError {
    fn eq(&self, other: &FromSqlError) -> bool {
        match (self, other) {
            (FromSqlError::InvalidType, FromSqlError::InvalidType) => true,
            (FromSqlError::OutOfRange(n1), FromSqlError::OutOfRange(n2)) => n1 == n2,
            #[cfg(feature = "uuid")]
            (FromSqlError::InvalidUuidSize(s1), FromSqlError::InvalidUuidSize(s2)) => s1 == s2,
            (..) => false,
        }
    }
}

impl fmt::Display for FromSqlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            FromSqlError::InvalidType => write!(f, "Invalid type"),
            FromSqlError::OutOfRange(i) => write!(f, "Value {} out of range", i),
            #[cfg(feature = "uuid")]
            FromSqlError::InvalidUuidSize(s) => {
                write!(f, "Cannot read UUID value out of {} byte blob", s)
            }
            FromSqlError::Other(ref err) => err.fmt(f),
        }
    }
}

impl Error for FromSqlError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        if let FromSqlError::Other(ref err) = self {
            Some(&**err)
        } else {
            None
        }
    }
}

/// Result type for implementors of the [`FromSql`] trait.
pub type FromSqlResult<T> = Result<T, FromSqlError>;

/// A trait for types that can be created from a DuckDB value.
pub trait FromSql: Sized {
    /// Converts DuckDB value into Rust value.
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self>;
}

macro_rules! from_sql_integral(
    ($t:ident) => (
        impl FromSql for $t {
            #[inline]
            fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
                match value {
                    ValueRef::TinyInt(i) => Ok(<$t as cast::From<i8>>::cast(i).unwrap()),
                    ValueRef::SmallInt(i) => Ok(<$t as cast::From<i16>>::cast(i).unwrap()),
                    ValueRef::Int(i) => Ok(<$t as cast::From<i32>>::cast(i).unwrap()),
                    ValueRef::BigInt(i) => Ok(<$t as cast::From<i64>>::cast(i).unwrap()),
                    ValueRef::HugeInt(i) => Ok(<$t as cast::From<i128>>::cast(i).unwrap()),

                    ValueRef::UTinyInt(i) => Ok(<$t as cast::From<u8>>::cast(i).unwrap()),
                    ValueRef::USmallInt(i) => Ok(<$t as cast::From<u16>>::cast(i).unwrap()),
                    ValueRef::UInt(i) => Ok(<$t as cast::From<u32>>::cast(i).unwrap()),
                    ValueRef::UBigInt(i) => Ok(<$t as cast::From<u64>>::cast(i).unwrap()),

                    ValueRef::Float(i) => Ok(<$t as cast::From<f32>>::cast(i).unwrap()),
                    ValueRef::Double(i) => Ok(<$t as cast::From<f64>>::cast(i).unwrap()),

                    // TODO: more efficient way?
                    ValueRef::Decimal(i) => Ok(i.to_string().parse::<$t>().unwrap()),

                    ValueRef::Timestamp(_, i) => Ok(<$t as cast::From<i64>>::cast(i).unwrap()),
                    ValueRef::Date32(i) => Ok(<$t as cast::From<i32>>::cast(i).unwrap()),
                    ValueRef::Time64(TimeUnit::Microsecond, i) => Ok(<$t as cast::From<i64>>::cast(i).unwrap()),
                    ValueRef::Text(_) => {
                        let v = value.as_str()?.parse::<$t>();
                        match v {
                            Ok(i) => Ok(i),
                            Err(_) => {
                                let v = value.as_str()?.parse::<i128>();
                                match v {
                                    Ok(i) => Err(FromSqlError::OutOfRange(i)),
                                    _ => Err(FromSqlError::InvalidType),
                                }
                            },
                        }
                    }
                    _ => Err(FromSqlError::InvalidType),
                }
            }
        }
    )
);

/// A trait for to implement unwrap method for primitive types
/// cast::From trait returns Result or the primitive, and for
/// Result we need to unwrap() for the column_result function
/// We implement unwrap() for all the primitive types so
/// We can always call unwrap() for the cast() function.
trait Unwrap {
    fn unwrap(self) -> Self;
}

macro_rules! unwrap_integral(
    ($t:ident) => (
        impl Unwrap for $t {
            #[inline]
            fn unwrap(self) -> Self {
                self
            }
        }
    )
);

unwrap_integral!(i8);
unwrap_integral!(i16);
unwrap_integral!(i32);
unwrap_integral!(i64);
unwrap_integral!(i128);
unwrap_integral!(isize);
unwrap_integral!(u8);
unwrap_integral!(u16);
unwrap_integral!(u32);
unwrap_integral!(u64);
unwrap_integral!(usize);
unwrap_integral!(f32);
unwrap_integral!(f64);

from_sql_integral!(i8);
from_sql_integral!(i16);
from_sql_integral!(i32);
from_sql_integral!(i64);
from_sql_integral!(i128);
from_sql_integral!(isize);
from_sql_integral!(u8);
from_sql_integral!(u16);
from_sql_integral!(u32);
from_sql_integral!(u64);
from_sql_integral!(usize);
from_sql_integral!(f32);
from_sql_integral!(f64);

impl FromSql for bool {
    #[inline]
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        match value {
            ValueRef::Boolean(b) => Ok(b),
            _ => i8::column_result(value).map(|i| i != 0),
        }
    }
}

impl FromSql for String {
    #[inline]
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        match value {
            #[cfg(feature = "chrono")]
            ValueRef::Date32(_) => Ok(chrono::NaiveDate::column_result(value)?.format("%F").to_string()),
            #[cfg(feature = "chrono")]
            ValueRef::Time64(..) => Ok(chrono::NaiveTime::column_result(value)?.format("%T%.f").to_string()),
            #[cfg(feature = "chrono")]
            ValueRef::Timestamp(..) => Ok(chrono::NaiveDateTime::column_result(value)?
                .format("%F %T%.f")
                .to_string()),
            _ => value.as_str().map(ToString::to_string),
        }
    }
}

impl FromSql for Box<str> {
    #[inline]
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        value.as_str().map(Into::into)
    }
}

impl FromSql for std::rc::Rc<str> {
    #[inline]
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        value.as_str().map(Into::into)
    }
}

impl FromSql for std::sync::Arc<str> {
    #[inline]
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        value.as_str().map(Into::into)
    }
}

impl FromSql for Vec<u8> {
    #[inline]
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        value.as_blob().map(|b| b.to_vec())
    }
}

#[cfg(feature = "uuid")]
impl FromSql for uuid::Uuid {
    #[inline]
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        match value {
            ValueRef::Text(..) => value
                .as_str()
                .and_then(|s| uuid::Uuid::parse_str(s).map_err(|_| FromSqlError::InvalidUuidSize(s.len()))),
            ValueRef::Blob(..) => value
                .as_blob()
                .and_then(|bytes| {
                    uuid::Builder::from_slice(bytes).map_err(|_| FromSqlError::InvalidUuidSize(bytes.len()))
                })
                .map(|mut builder| builder.build()),
            _ => Err(FromSqlError::InvalidType),
        }
    }
}

impl<T: FromSql> FromSql for Option<T> {
    #[inline]
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        match value {
            ValueRef::Null => Ok(None),
            _ => FromSql::column_result(value).map(Some),
        }
    }
}

impl FromSql for Value {
    #[inline]
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        Ok(value.into())
    }
}

#[cfg(test)]
mod test {
    use super::FromSql;
    use crate::{Connection, Error, Result};
    use std::convert::TryFrom;

    #[test]
    fn test_timestamp_raw() -> Result<()> {
        let db = Connection::open_in_memory()?;
        let sql = "BEGIN;
                   CREATE TABLE timestamp (sec TIMESTAMP_S, milli TIMESTAMP_MS, micro TIMESTAMP_US, nano TIMESTAMP_NS );
                   INSERT INTO timestamp VALUES (NULL,NULL,NULL,NULL );
                   INSERT INTO timestamp VALUES ('2008-01-01 00:00:01','2008-01-01 00:00:01.594','2008-01-01 00:00:01.88926','2008-01-01 00:00:01.889268321' );
                   INSERT INTO timestamp VALUES (NULL,NULL,NULL,1199145601889268321 );
                   END;";
        db.execute_batch(sql)?;
        let v = db.query_row(
            "SELECT sec, milli, micro, nano FROM timestamp WHERE sec is not null",
            [],
            |row| <(i64, i64, i64, i64)>::try_from(row),
        )?;
        assert_eq!(v, (1199145601, 1199145601594, 1199145601889260, 1199145601889268000));
        Ok(())
    }

    #[test]
    fn test_time64_raw() -> Result<()> {
        let db = Connection::open_in_memory()?;
        let sql = "BEGIN;
                   CREATE TABLE time64 (t time);
                   INSERT INTO time64 VALUES ('20:08:10.998');
                   END;";
        db.execute_batch(sql)?;
        let v = db.query_row("SELECT * FROM time64", [], |row| <(i64,)>::try_from(row))?;
        assert_eq!(v, (72490998000,));
        Ok(())
    }

    #[test]
    fn test_date32_raw() -> Result<()> {
        let db = Connection::open_in_memory()?;
        let sql = "BEGIN;
                   CREATE TABLE date32 (d date);
                   INSERT INTO date32 VALUES ('2008-01-01');
                   END;";
        db.execute_batch(sql)?;
        let v = db.query_row("SELECT * FROM date32", [], |row| <(i32,)>::try_from(row))?;
        assert_eq!(v, (13879,));
        Ok(())
    }

    #[test]
    fn test_unsigned_integer() -> Result<()> {
        let db = Connection::open_in_memory()?;
        let sql = "BEGIN;
                   CREATE TABLE unsigned_int (u1 utinyint, u2 usmallint, u4 uinteger, u8 ubigint);
                   INSERT INTO unsigned_int VALUES (255, 65535, 4294967295, 18446744073709551615);
                   END;";
        db.execute_batch(sql)?;
        let v = db.query_row("SELECT * FROM unsigned_int", [], |row| {
            <(u8, u16, u32, u64)>::try_from(row)
        })?;
        assert_eq!(v, (255, 65535, 4294967295, 18446744073709551615));
        Ok(())
    }

    // This test asserts that i128s above/below the i64 max/min can written and retrieved properly.
    #[test]
    fn test_hugeint_max_min() -> Result<()> {
        let db = Connection::open_in_memory()?;
        db.execute("CREATE TABLE huge_int (u1 hugeint, u2 hugeint);", [])?;
        // Min/Max value defined in here: https://duckdb.org/docs/sql/data_types/numeric
        let i128max: i128 = i128::MAX;
        let i128min: i128 = i128::MIN + 1;
        db.execute("INSERT INTO huge_int VALUES (?, ?);", [&i128max, &i128min])?;
        let v = db.query_row("SELECT * FROM huge_int", [], |row| <(i128, i128)>::try_from(row))?;
        assert_eq!(v, (i128max, i128min));
        Ok(())
    }

    #[test]
    fn test_integral_ranges() -> Result<()> {
        let db = Connection::open_in_memory()?;

        fn check_ranges<T>(db: &Connection, out_of_range: &[i128], in_range: &[i128])
        where
            T: Into<i128> + FromSql + ::std::fmt::Debug,
        {
            for n in out_of_range {
                let err = db.query_row("SELECT ?", &[n], |r| r.get::<_, T>(0)).unwrap_err();
                match err {
                    Error::IntegralValueOutOfRange(_, value) => assert_eq!(*n, value),
                    _ => panic!("unexpected error: {}", err),
                }
            }
            for n in in_range {
                assert_eq!(*n, db.query_row("SELECT ?", &[n], |r| r.get::<_, T>(0)).unwrap().into());
            }
        }

        check_ranges::<i8>(&db, &[-129, 128], &[-128, 0, 1, 127]);
        check_ranges::<i16>(&db, &[-32769, 32768], &[-32768, -1, 0, 1, 32767]);
        check_ranges::<i32>(
            &db,
            &[-2_147_483_649, 2_147_483_648],
            &[-2_147_483_648, -1, 0, 1, 2_147_483_647],
        );
        check_ranges::<u8>(&db, &[-2, -1, 256], &[0, 1, 255]);
        check_ranges::<u16>(&db, &[-2, -1, 65536], &[0, 1, 65535]);
        check_ranges::<u32>(&db, &[-2, -1, 4_294_967_296], &[0, 1, 4_294_967_295]);
        Ok(())
    }

    // Don't need uuid crate if we only care about the string value of uuid
    #[test]
    fn test_uuid_string() -> Result<()> {
        let db = Connection::open_in_memory()?;
        let sql = "BEGIN;
                   CREATE TABLE uuid (u uuid);
                   INSERT INTO uuid VALUES ('10203040-5060-7080-0102-030405060708'),(NULL),('47183823-2574-4bfd-b411-99ed177d3e43');
                   END;";
        db.execute_batch(sql)?;
        let v = db.query_row("SELECT u FROM uuid order by u desc nulls last limit 1", [], |row| {
            <(String,)>::try_from(row)
        })?;
        assert_eq!(v, ("47183823-2574-4bfd-b411-99ed177d3e43".to_string(),));
        let v = db.query_row(
            "SELECT u FROM uuid where u>?",
            ["10203040-5060-7080-0102-030405060708"],
            |row| <(String,)>::try_from(row),
        )?;
        assert_eq!(v, ("47183823-2574-4bfd-b411-99ed177d3e43".to_string(),));
        Ok(())
    }

    #[cfg(feature = "uuid")]
    #[test]
    fn test_uuid_from_string() -> crate::Result<()> {
        let db = Connection::open_in_memory()?;
        let sql = "BEGIN;
                   CREATE TABLE uuid (u uuid);
                   INSERT INTO uuid VALUES ('10203040-5060-7080-0102-030405060708'),(NULL),('47183823-2574-4bfd-b411-99ed177d3e43');
                   END;";
        db.execute_batch(sql)?;
        let v = db.query_row("SELECT u FROM uuid order by u desc nulls last limit 1", [], |row| {
            <(uuid::Uuid,)>::try_from(row)
        })?;
        assert_eq!(v.0.to_string(), "47183823-2574-4bfd-b411-99ed177d3e43");
        Ok(())
    }
}

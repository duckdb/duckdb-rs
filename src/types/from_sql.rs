extern crate cast;

use super::{Value, ValueRef};
use std::error::Error;
use std::fmt;

/// Enum listing possible errors from [`FromSql`] trait.
#[derive(Debug)]
#[non_exhaustive]
pub enum FromSqlError {
    /// Error when an DuckDB value is requested, but the type of the result
    /// cannot be converted to the requested Rust type.
    InvalidType,

    /// Error when the i128 value returned by DuckDB cannot be stored into the
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
                    ValueRef::Float(i) => Ok(<$t as cast::From<f32>>::cast(i).unwrap()),
                    ValueRef::Double(i) => Ok(<$t as cast::From<f64>>::cast(i).unwrap()),
                    // TODO: more efficient way?
                    ValueRef::Decimal(i) => Ok(i.to_string().parse::<$t>().unwrap()),
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
        value.as_str().map(ToString::to_string)
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
        value
            .as_blob()
            .and_then(|bytes| uuid::Builder::from_slice(bytes).map_err(|_| FromSqlError::InvalidUuidSize(bytes.len())))
            .map(|mut builder| builder.build())
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

    #[test]
    #[ignore = "duckdb doesn't support this"]
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
}

//! [`ToSql`] and [`FromSql`] are also implemented for `Option<T>` where `T`
//! implements [`ToSql`] or [`FromSql`] for the cases where you want to know if
//! a value was NULL (which gets translated to `None`).

pub use self::{
    from_sql::{FromSql, FromSqlError, FromSqlResult},
    ordered_map::OrderedMap,
    string::DuckString,
    to_sql::{ToSql, ToSqlOutput},
    value::Value,
    value_ref::{EnumType, ListType, TimeUnit, ValueRef},
};

use arrow::datatypes::DataType;
use std::fmt;

#[cfg(feature = "chrono")]
mod chrono;
mod from_sql;
#[cfg(feature = "serde_json")]
mod serde_json;
mod to_sql;
#[cfg(feature = "url")]
mod url;
mod value;
mod value_ref;

mod ordered_map;
mod string;

/// Empty struct that can be used to fill in a query parameter as `NULL`.
///
/// ## Example
///
/// ```rust,no_run
/// # use duckdb::{Connection, Result};
/// # use duckdb::types::{Null};
///
/// fn insert_null(conn: &Connection) -> Result<usize> {
///     conn.execute("INSERT INTO people (name) VALUES (?)", [Null])
/// }
/// ```
#[derive(Copy, Clone)]
pub struct Null;

/// DuckDB data types.
/// See [Fundamental Datatypes](https://duckdb.org/docs/sql/data_types/overview).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Type {
    /// NULL
    Null,
    /// BOOLEAN
    Boolean,
    /// TINYINT
    TinyInt,
    /// SMALLINT
    SmallInt,
    /// INT
    Int,
    /// BIGINT
    BigInt,
    /// HUGEINT
    HugeInt,
    /// UTINYINT
    UTinyInt,
    /// USMALLINT
    USmallInt,
    /// UINT
    UInt,
    /// UBIGINT
    UBigInt,
    /// FLOAT
    Float,
    /// DOUBLE
    Double,
    /// DECIMAL
    Decimal,
    /// TIMESTAMP
    Timestamp,
    /// Text
    Text,
    /// BLOB
    Blob,
    /// DATE32
    Date32,
    /// TIME64
    Time64,
    /// INTERVAL
    Interval,
    /// LIST
    List(Box<Type>),
    /// ENUM
    Enum,
    /// STRUCT
    Struct(Vec<(String, Type)>),
    /// MAP
    Map(Box<Type>, Box<Type>),
    /// ARRAY
    Array(Box<Type>, u32),
    /// UNION
    Union,
    /// Any
    Any,
}

impl From<&DataType> for Type {
    fn from(value: &DataType) -> Self {
        match value {
            DataType::Null => Self::Null,
            DataType::Boolean => Self::Boolean,
            DataType::Int8 => Self::TinyInt,
            DataType::Int16 => Self::SmallInt,
            DataType::Int32 => Self::Int,
            DataType::Int64 => Self::BigInt,
            DataType::UInt8 => Self::UTinyInt,
            DataType::UInt16 => Self::USmallInt,
            DataType::UInt32 => Self::UInt,
            DataType::UInt64 => Self::UBigInt,
            // DataType::Float16 => Self::Float16,
            DataType::Float32 => Self::Float,  // Single precision (4 bytes)
            DataType::Float64 => Self::Double, // Double precision (8 bytes)
            DataType::Timestamp(_, _) => Self::Timestamp,
            DataType::Date32 => Self::Date32,
            // DataType::Date64 => Self::Date64,
            // DataType::Time32(_) => Self::Time32,
            DataType::Time64(_) => Self::Time64,
            // DataType::Duration(_) => Self::Duration,
            // DataType::Interval(_) => Self::Interval,
            DataType::Binary => Self::Blob,
            // DataType::FixedSizeBinary(_) => Self::FixedSizeBinary,
            // DataType::LargeBinary => Self::LargeBinary,
            DataType::LargeUtf8 | DataType::Utf8 => Self::Text,
            DataType::List(inner) => Self::List(Box::new(Self::from(inner.data_type()))),
            DataType::FixedSizeList(field, size) => {
                Self::Array(Box::new(Self::from(field.data_type())), (*size).try_into().unwrap())
            }
            // DataType::LargeList(_) => Self::LargeList,
            DataType::Struct(inner) => Self::Struct(
                inner
                    .iter()
                    .map(|f| (f.name().to_owned(), Self::from(f.data_type())))
                    .collect(),
            ),
            DataType::LargeList(inner) => Self::List(Box::new(Self::from(inner.data_type()))),
            DataType::Union(_, _) => Self::Union,
            DataType::Decimal128(..) => Self::Decimal,
            DataType::Decimal256(..) => Self::Decimal,
            DataType::Map(field, ..) => {
                let data_type = field.data_type();
                match data_type {
                    DataType::Struct(fields) => Self::Map(
                        Box::new(Self::from(fields[0].data_type())),
                        Box::new(Self::from(fields[1].data_type())),
                    ),
                    _ => unreachable!(),
                }
            }
            res => unimplemented!("{}", res),
        }
    }
}

impl fmt::Display for Type {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Self::Null => f.pad("Null"),
            Self::Boolean => f.pad("Boolean"),
            Self::TinyInt => f.pad("TinyInt"),
            Self::SmallInt => f.pad("SmallInt"),
            Self::Int => f.pad("Int"),
            Self::BigInt => f.pad("BigInt"),
            Self::HugeInt => f.pad("HugeInt"),
            Self::UTinyInt => f.pad("UTinyInt"),
            Self::USmallInt => f.pad("USmallInt"),
            Self::UInt => f.pad("UInt"),
            Self::UBigInt => f.pad("UBigInt"),
            Self::Float => f.pad("Float"),
            Self::Double => f.pad("Double"),
            Self::Decimal => f.pad("Decimal"),
            Self::Timestamp => f.pad("Timestamp"),
            Self::Text => f.pad("Text"),
            Self::Blob => f.pad("Blob"),
            Self::Date32 => f.pad("Date32"),
            Self::Time64 => f.pad("Time64"),
            Self::Interval => f.pad("Interval"),
            Self::Struct(..) => f.pad("Struct"),
            Self::List(..) => f.pad("List"),
            Self::Enum => f.pad("Enum"),
            Self::Map(..) => f.pad("Map"),
            Self::Array(..) => f.pad("Array"),
            Self::Union => f.pad("Union"),
            Self::Any => f.pad("Any"),
        }
    }
}

#[cfg(test)]
mod test {
    use super::Value;
    use crate::{params, Connection, Error, Result, Statement};
    use std::os::raw::{c_double, c_int};

    fn checked_memory_handle() -> Result<Connection> {
        let db = Connection::open_in_memory()?;
        db.execute_batch("CREATE TABLE foo (b BLOB, t TEXT, i INTEGER, f FLOAT, n BLOB)")?;
        Ok(db)
    }

    #[test]
    fn test_blob() -> Result<()> {
        let db = checked_memory_handle()?;

        let v1234 = vec![1u8, 2, 3, 4];
        db.execute("INSERT INTO foo(b) VALUES (?)", [&v1234])?;

        let v: Vec<u8> = db.query_row("SELECT b FROM foo", [], |r| r.get(0))?;
        assert_eq!(v, v1234);
        Ok(())
    }

    #[test]
    fn test_empty_blob() -> Result<()> {
        let db = checked_memory_handle()?;

        let empty = vec![];
        db.execute("INSERT INTO foo(b) VALUES (?)", [&empty])?;

        let v: Vec<u8> = db.query_row("SELECT b FROM foo", [], |r| r.get(0))?;
        assert_eq!(v, empty);
        Ok(())
    }

    #[test]
    fn test_str() -> Result<()> {
        let db = checked_memory_handle()?;

        let s = "hello, world!";
        db.execute("INSERT INTO foo(t) VALUES (?)", [&s])?;

        let from: String = db.query_row("SELECT t FROM foo", [], |r| r.get(0))?;
        assert_eq!(from, s);
        Ok(())
    }

    #[test]
    fn test_string() -> Result<()> {
        let db = checked_memory_handle()?;

        let s = "hello, world!";
        let result = db.execute("INSERT INTO foo(t) VALUES (?)", [s.to_owned()]);
        if result.is_err() {
            panic!("exe error: {}", result.unwrap_err())
        }

        let from: String = db.query_row("SELECT t FROM foo", [], |r| r.get(0))?;
        assert_eq!(from, s);
        Ok(())
    }

    #[test]
    fn test_value() -> Result<()> {
        let db = checked_memory_handle()?;

        db.execute("INSERT INTO foo(i) VALUES (?)", [Value::BigInt(10)])?;

        assert_eq!(10i64, db.query_row::<i64, _, _>("SELECT i FROM foo", [], |r| r.get(0))?);
        Ok(())
    }

    #[test]
    fn test_option() -> Result<()> {
        let db = checked_memory_handle()?;

        let s = "hello, world!";
        let b = Some(vec![1u8, 2, 3, 4]);

        db.execute("INSERT INTO foo(t) VALUES (?)", [&s])?;
        db.execute("INSERT INTO foo(b) VALUES (?)", [&b])?;

        let mut stmt = db.prepare("SELECT t, b FROM foo ORDER BY ROWID ASC")?;
        let mut rows = stmt.query([])?;

        {
            let row1 = rows.next()?.unwrap();
            let s1: Option<String> = row1.get_unwrap(0);
            let b1: Option<Vec<u8>> = row1.get_unwrap(1);
            assert_eq!(s, s1.unwrap());
            assert!(b1.is_none());
        }

        {
            let row2 = rows.next()?.unwrap();
            let s2: Option<String> = row2.get_unwrap(0);
            let b2: Option<Vec<u8>> = row2.get_unwrap(1);
            assert!(s2.is_none());
            assert_eq!(b, b2);
        }
        Ok(())
    }

    #[test]
    #[allow(clippy::cognitive_complexity)]
    #[ignore = "duckdb doesn't support this"]
    fn test_mismatched_types() -> Result<()> {
        fn is_invalid_column_type(err: Error) -> bool {
            matches!(err, Error::InvalidColumnType(..))
        }

        let db = checked_memory_handle()?;

        db.execute("INSERT INTO foo(b, t, i, f) VALUES (X'0102', 'text', 1, 1.5)", [])?;

        let mut stmt = db.prepare("SELECT b, t, i, f, n FROM foo")?;
        let mut rows = stmt.query([])?;
        let row = rows.next()?.unwrap();

        // check the correct types come back as expected
        assert_eq!(vec![1, 2], row.get::<_, Vec<u8>>(0)?);
        assert_eq!("text", row.get::<_, String>(1)?);
        assert_eq!(1, row.get::<_, c_int>(2)?);
        assert!((1.5 - row.get::<_, c_double>(3)?).abs() < f64::EPSILON);
        assert_eq!(row.get::<_, Option<c_int>>(4)?, None);
        assert_eq!(row.get::<_, Option<c_double>>(4)?, None);
        assert_eq!(row.get::<_, Option<String>>(4)?, None);

        // check some invalid types

        // 0 is actually a blob (Vec<u8>)
        assert!(is_invalid_column_type(row.get::<_, c_int>(0).err().unwrap()));
        assert!(is_invalid_column_type(row.get::<_, c_int>(0).err().unwrap()));
        assert!(is_invalid_column_type(row.get::<_, i64>(0).err().unwrap()));
        assert!(is_invalid_column_type(row.get::<_, c_double>(0).err().unwrap()));
        assert!(is_invalid_column_type(row.get::<_, String>(0).err().unwrap()));
        assert!(is_invalid_column_type(row.get::<_, Option<c_int>>(0).err().unwrap()));

        // 1 is actually a text (String)
        assert!(is_invalid_column_type(row.get::<_, c_int>(1).err().unwrap()));
        assert!(is_invalid_column_type(row.get::<_, i64>(1).err().unwrap()));
        assert!(is_invalid_column_type(row.get::<_, c_double>(1).err().unwrap()));
        assert!(is_invalid_column_type(row.get::<_, Vec<u8>>(1).err().unwrap()));
        assert!(is_invalid_column_type(row.get::<_, Option<c_int>>(1).err().unwrap()));

        // 2 is actually an integer
        assert!(is_invalid_column_type(row.get::<_, String>(2).err().unwrap()));
        assert!(is_invalid_column_type(row.get::<_, Vec<u8>>(2).err().unwrap()));
        assert!(is_invalid_column_type(row.get::<_, Option<String>>(2).err().unwrap()));

        // 3 is actually a float (c_double)
        assert!(is_invalid_column_type(row.get::<_, c_int>(3).err().unwrap()));
        assert!(is_invalid_column_type(row.get::<_, i64>(3).err().unwrap()));
        assert!(is_invalid_column_type(row.get::<_, String>(3).err().unwrap()));
        assert!(is_invalid_column_type(row.get::<_, Vec<u8>>(3).err().unwrap()));
        assert!(is_invalid_column_type(row.get::<_, Option<c_int>>(3).err().unwrap()));

        // 4 is actually NULL
        assert!(is_invalid_column_type(row.get::<_, c_int>(4).err().unwrap()));
        assert!(is_invalid_column_type(row.get::<_, i64>(4).err().unwrap()));
        assert!(is_invalid_column_type(row.get::<_, c_double>(4).err().unwrap()));
        assert!(is_invalid_column_type(row.get::<_, String>(4).err().unwrap()));
        assert!(is_invalid_column_type(row.get::<_, Vec<u8>>(4).err().unwrap()));
        Ok(())
    }

    #[test]
    fn test_dynamic_type() -> Result<()> {
        use super::Value;
        let db = checked_memory_handle()?;

        db.execute("INSERT INTO foo(b, t, i, f) VALUES (X'0102', 'text', 1, 1.5)", [])?;

        let mut stmt = db.prepare("SELECT b, t, i, f, n FROM foo")?;
        let mut rows = stmt.query([])?;
        let row = rows.next()?.unwrap();
        // NOTE: this is different from SQLite
        // assert_eq!(Value::Blob(vec![1, 2]), row.get::<_, Value>(0)?);
        assert_eq!(Value::Blob(vec![120, 48, 49, 48, 50]), row.get::<_, Value>(0)?);
        assert_eq!(Value::Text(String::from("text")), row.get::<_, Value>(1)?);
        assert_eq!(Value::Int(1), row.get::<_, Value>(2)?);
        match row.get::<_, Value>(3)? {
            Value::Float(val) => assert!((1.5 - val).abs() < f32::EPSILON),
            x => panic!("Invalid Value {x:?}"),
        }
        assert_eq!(Value::Null, row.get::<_, Value>(4)?);
        Ok(())
    }

    macro_rules! test_conversion {
        ($db_etc:ident, $insert_value:expr, $get_type:ty,expect $expected_value:expr) => {
            $db_etc.insert_statement.execute(params![$insert_value])?;
            let res = $db_etc
                .query_statement
                .query_row([], |row| row.get::<_, $get_type>(0));
            assert_eq!(res?, $expected_value);
            $db_etc.delete_statement.execute([])?;
        };
        ($db_etc:ident, $insert_value:expr, $get_type:ty,expect_from_sql_error) => {
            $db_etc.insert_statement.execute(params![$insert_value])?;
            let res = $db_etc
                .query_statement
                .query_row([], |row| row.get::<_, $get_type>(0));
            res.unwrap_err();
            $db_etc.delete_statement.execute([])?;
        };
        ($db_etc:ident, $insert_value:expr, $get_type:ty,expect_to_sql_error) => {
            $db_etc
                .insert_statement
                .execute(params![$insert_value])
                .unwrap_err();
        };
    }

    #[test]
    #[ignore = "duckdb doesn't support this"]
    fn test_numeric_conversions() -> Result<()> {
        #![allow(clippy::float_cmp)]

        // Test what happens when we store an f32 and retrieve an i32 etc.
        let db = Connection::open_in_memory()?;
        db.execute_batch("CREATE TABLE foo (x)")?;

        // DuckDB actually ignores the column types, so we just need to test
        // different numeric values.

        struct DbEtc<'conn> {
            insert_statement: Statement<'conn>,
            query_statement: Statement<'conn>,
            delete_statement: Statement<'conn>,
        }

        let mut db_etc = DbEtc {
            insert_statement: db.prepare("INSERT INTO foo VALUES (?1)")?,
            query_statement: db.prepare("SELECT x FROM foo")?,
            delete_statement: db.prepare("DELETE FROM foo")?,
        };

        // Basic non-converting test.
        test_conversion!(db_etc, 0u8, u8, expect 0u8);

        // In-range integral conversions.
        test_conversion!(db_etc, 100u8, i8, expect 100i8);
        test_conversion!(db_etc, 200u8, u8, expect 200u8);
        test_conversion!(db_etc, 100u16, i8, expect 100i8);
        test_conversion!(db_etc, 200u16, u8, expect 200u8);
        test_conversion!(db_etc, u32::MAX, u64, expect u32::MAX as u64);
        test_conversion!(db_etc, i64::MIN, i64, expect i64::MIN);
        test_conversion!(db_etc, i64::MAX, i64, expect i64::MAX);
        test_conversion!(db_etc, i64::MAX, u64, expect i64::MAX as u64);
        test_conversion!(db_etc, 100usize, usize, expect 100usize);
        test_conversion!(db_etc, 100u64, u64, expect 100u64);
        test_conversion!(db_etc, i64::MAX as u64, u64, expect i64::MAX as u64);

        // Out-of-range integral conversions.
        test_conversion!(db_etc, 200u8, i8, expect_from_sql_error);
        test_conversion!(db_etc, 400u16, i8, expect_from_sql_error);
        test_conversion!(db_etc, 400u16, u8, expect_from_sql_error);
        test_conversion!(db_etc, -1i8, u8, expect_from_sql_error);
        test_conversion!(db_etc, i64::MIN, u64, expect_from_sql_error);
        test_conversion!(db_etc, u64::MAX, i64, expect_to_sql_error);
        test_conversion!(db_etc, u64::MAX, u64, expect_to_sql_error);
        test_conversion!(db_etc, i64::MAX as u64 + 1, u64, expect_to_sql_error);

        // FromSql integer to float, always works.
        test_conversion!(db_etc, i64::MIN, f32, expect i64::MIN as f32);
        test_conversion!(db_etc, i64::MAX, f32, expect i64::MAX as f32);
        test_conversion!(db_etc, i64::MIN, f64, expect i64::MIN as f64);
        test_conversion!(db_etc, i64::MAX, f64, expect i64::MAX as f64);

        // FromSql float to int conversion, never works even if the actual value
        // is an integer.
        test_conversion!(db_etc, 0f64, i64, expect_from_sql_error);
        Ok(())
    }
}

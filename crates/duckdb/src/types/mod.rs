//! [`ToSql`] and [`FromSql`] are also implemented for `Option<T>` where `T`
//! implements [`ToSql`] or [`FromSql`] for the cases where you want to know if
//! a value was NULL (which gets translated to `None`).
//!
//! # Dynamic value compatibility notes
//!
//! [`Type`], [`Value`], and [`ValueRef`] are non-exhaustive. Downstream code
//! matching these enums should include a wildcard arm so new DuckDB types can
//! be added without another source break.
//!
//! Top-level `UHUGEINT` result values use [`Value::UHugeInt`] and
//! [`ValueRef::UHugeInt`] when DuckDB reports `UHUGEINT` logical metadata for
//! the result column. Nested `UHUGEINT` values inside lists, structs, maps,
//! arrays, and unions do not currently carry DuckDB child logical type metadata
//! through the borrowed container API, so values above `i128::MAX` cannot
//! recover their `u128` value from those metadata-less carriers.
//!
//! Parameter-derived scale-zero Arrow decimal result columns are intentionally
//! not guessed. DuckDB can report `Invalid` logical metadata for a bare bound
//! parameter (`SELECT ?`) even when the bound value is `u128`. In that case
//! `duckdb-rs` preserves the existing signed [`Value::HugeInt`] fallback instead
//! of inferring `UHUGEINT`, `HUGEINT`, or `DECIMAL` from parameter names,
//! aliases, or expression text. Cast the result expression so DuckDB reports
//! stable logical metadata when a dynamic `UHUGEINT` carrier is required.
//!
//! DuckDB `DECIMAL` values use [`Decimal`], a full-domain carrier storing
//! DuckDB's decimal width, scale, and scaled integer payload.
//! Reading a [`Decimal`] as `f32` or `f64` preserves its fractional component
//! but still inherits normal binary floating-point precision loss. Read
//! [`Decimal`] directly when exact decimal transport matters.
//! Enable the `rust_decimal` feature for compatibility impls that convert to
//! and from `rust_decimal::Decimal` when the value fits that crate's smaller
//! decimal domain, including compatibility reads from `FLOAT`, `DOUBLE`, and
//! text columns.
//!
//! Arrow decimal result values use [`Value::Decimal`] and [`ValueRef::Decimal`]
//! directly except for scale-zero `Decimal128(38, 0)`, which is also the Arrow
//! shape DuckDB uses for `HUGEINT` and `UHUGEINT`. Top-level result columns use
//! DuckDB logical metadata to distinguish those cases when DuckDB reports it.
//! Narrower scale-zero `DECIMAL` widths are unambiguous and materialize as
//! [`Decimal`] rather than the legacy signed [`Value::HugeInt`] fallback.
//! Metadata-less scale-zero `Decimal128(38, 0)` values preserve the existing
//! signed [`Value::HugeInt`] fallback. This affects parameter-derived
//! expressions where DuckDB reports `Invalid` metadata, and nested container
//! children where the borrowed container API does not carry DuckDB child
//! logical metadata.
//!
//! DuckDB `GEOMETRY` values use [`Value::Geometry`] and
//! [`ValueRef::Geometry`], backed by WKB bytes. CRS data is exposed as logical
//! type metadata through [`crate::core::LogicalTypeHandle::geometry_crs`]
//! rather than on each value.
//! Nested `GEOMETRY` values inside lists, structs, maps, arrays, and unions do
//! not currently carry DuckDB child logical type metadata through the borrowed
//! container API, so they materialize through their Arrow binary carrier as
//! [`Value::Blob`] values.

pub use self::{
    decimal::{Decimal, DecimalError},
    from_sql::{FromSql, FromSqlError, FromSqlResult},
    ordered_map::OrderedMap,
    string::DuckString,
    to_sql::{ToSql, ToSqlOutput},
    value::Value,
    value_ref::{EnumType, ListType, TimeUnit, ValueRef},
};
pub(crate) use decimal::to_duckdb_decimal;
pub(crate) use value_ref::{binding_unsupported_value, value_ref_from_value};

use arrow::datatypes::DataType;
use std::fmt;

use crate::ffi;

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

mod decimal;
mod ordered_map;
mod string;

pub(crate) fn to_duckdb_hugeint(i: i128) -> ffi::duckdb_hugeint {
    ffi::duckdb_hugeint {
        lower: i as u64,
        upper: (i >> 64) as i64,
    }
}

pub(crate) fn to_duckdb_uhugeint(i: u128) -> ffi::duckdb_uhugeint {
    ffi::duckdb_uhugeint {
        lower: i as u64,
        upper: (i >> 64) as u64,
    }
}

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
#[non_exhaustive]
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
    /// UHUGEINT
    UHugeInt,
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
    /// GEOMETRY
    Geometry,
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
    /// VARIANT. See [`LogicalTypeId::Variant`](crate::core::LogicalTypeId::Variant).
    Variant,
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
            DataType::FixedSizeBinary(_) => Self::Blob,
            // DataType::LargeBinary => Self::LargeBinary,
            DataType::LargeUtf8 | DataType::Utf8 => Self::Text,
            DataType::List(inner) => Self::List(Box::new(Self::from(inner.data_type()))),
            DataType::FixedSizeList(field, size) => Self::Array(
                Box::new(Self::from(field.data_type())),
                (*size)
                    .try_into()
                    .expect("Arrow FixedSizeList size must be non-negative"),
            ),
            // DataType::LargeList(_) => Self::LargeList,
            DataType::Struct(inner) => {
                let capacity = inner.len();
                let mut struct_vec = Vec::with_capacity(capacity);
                struct_vec.extend(inner.iter().map(|f| (f.name().to_owned(), Self::from(f.data_type()))));
                Self::Struct(struct_vec)
            }
            DataType::LargeList(inner) => Self::List(Box::new(Self::from(inner.data_type()))),
            DataType::Union(_, _) => Self::Union,
            // Type is a broad value category: Decimal256 is decimal-shaped
            // metadata, even though DuckDB value paths only carry widths <= 38.
            DataType::Decimal32(..) | DataType::Decimal64(..) | DataType::Decimal128(..) | DataType::Decimal256(..) => {
                Self::Decimal
            }
            DataType::Map(field, ..) => {
                let data_type = field.data_type();
                match data_type {
                    DataType::Struct(fields) if fields.len() == 2 => Self::Map(
                        Box::new(Self::from(fields[0].data_type())),
                        Box::new(Self::from(fields[1].data_type())),
                    ),
                    DataType::Struct(fields) => {
                        panic!(
                            "Arrow Map child struct must have exactly two fields, got {}",
                            fields.len()
                        )
                    }
                    _ => panic!("Arrow Map child type must be Struct(key, value), got {data_type}"),
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
            Self::UHugeInt => f.pad("UHugeInt"),
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
            Self::Geometry => f.pad("Geometry"),
            Self::Date32 => f.pad("Date32"),
            Self::Time64 => f.pad("Time64"),
            Self::Interval => f.pad("Interval"),
            Self::Struct(..) => f.pad("Struct"),
            Self::List(..) => f.pad("List"),
            Self::Enum => f.pad("Enum"),
            Self::Map(..) => f.pad("Map"),
            Self::Array(..) => f.pad("Array"),
            Self::Union => f.pad("Union"),
            Self::Variant => f.pad("Variant"),
            Self::Any => f.pad("Any"),
        }
    }
}

#[cfg(test)]
mod test {
    use super::{Type, Value};
    use crate::{Connection, Result};
    use arrow::datatypes::DataType;

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
    fn test_geometry_value_binds_as_wkb_blob() -> Result<()> {
        let db = checked_memory_handle()?;
        let wkb = vec![1_u8, 1, 0, 0, 0];

        let value = Value::Geometry(wkb.clone());
        let bound: Vec<u8> = db.query_row("SELECT ?::BLOB", [value], |r| r.get(0))?;

        assert_eq!(bound, wkb);
        Ok(())
    }

    #[test]
    fn test_geometry_value_binds_through_st_geomfromwkb() -> Result<()> {
        let db = checked_memory_handle()?;
        let wkb = vec![
            0x01, 0x01, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xF8, 0x7F, 0, 0, 0, 0, 0, 0, 0xF8, 0x7F,
        ];

        let value = Value::Geometry(wkb.clone());
        let bound: Value = db.query_row("SELECT ST_GeomFromWKB(?)", [value], |r| r.get(0))?;

        assert_eq!(bound, Value::Geometry(wkb));
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
        if let Err(e) = result {
            panic!("exe error: {e}")
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
    fn arrow_decimal_types_match_duckdb_decimal_classification() {
        assert_eq!(Type::from(&DataType::Decimal32(9, 2)), Type::Decimal);
        assert_eq!(Type::from(&DataType::Decimal64(18, 2)), Type::Decimal);
        assert_eq!(Type::from(&DataType::Decimal128(38, 2)), Type::Decimal);
        assert_eq!(Type::from(&DataType::Decimal256(76, 10)), Type::Decimal);
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
}

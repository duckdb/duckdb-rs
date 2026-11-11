//! Rust representations of DuckDB logical and physical value types.
//!
//! [`DuckDBType`] maps a Rust type to its DuckDB
//! [`crate::logical_type::LogicalType`]. [`ToValue`] and [`FromValue`] convert
//! between Rust values and owned [`crate::value::Value`] handles for parameters,
//! defaults, and other scalar APIs.
//!
//! This module also defines marker and wrapper types for values whose DuckDB
//! representation has no direct Rust primitive equivalent, including decimals,
//! nested types, intervals, UUIDs, `BIGNUM`, and `VARIANT`. Typed vector access
//! uses these representations through [`crate::vector::NativeElement`].
//!
//! Concrete representations and their [`ToValue`]/[`FromValue`]/
//! [`crate::vector::NativeElement`] implementations live together in family modules grouped by
//! logical type: `primitive`, `string` (`VARCHAR`/`BLOB`/`BIT`), `temporal`
//! (`DATE`/`TIME`/`TIMESTAMP`/`INTERVAL`), `uuid`, `decimal`, `bignum`, `rust_decimal` (feature-gated),
//! `list`, `array`, `map`, `structs`, `union`, `variant`, and `tuple`. Each family module is private;
//! its public types are re-exported below. The row readers and writers of nested types, such as
//! [`crate::vector::ListReader`], live in [`crate::vector`].
//!
//! Named Rust structs map to `STRUCT` through [`crate::AsDuckDB`], which derives the value
//! conversions and, with `#[duckdb(reader, writer)]`, a row reader and writer. [`Struct`]
//! accepts any `STRUCT` or `TUPLE` vector when the schema is only known at runtime.
//! Enums of single-field variants map to `UNION` the same way through [`crate::AsDuckDBUnion`],
//! with [`Union`] as the runtime fallback.
//!
//! ```
//! use duckdb_neo::{types::List, vector::{Elements, ListReader, RowReader, VectorReader}};
//!
//! fn list_items<'r>(row: Elements<'r, VectorReader<'_, i32>>) -> Vec<Option<&'r i32>> {
//!     row.iter().collect()
//! }
//! fn first_row<'a>(lists: &'a ListReader<'a, i32>) -> duckdb_neo::Result<Option<Vec<Option<&'a i32>>>> {
//!     Ok(lists.get(0)?.map(list_items))
//! }
//! # let _ = (first_row, std::marker::PhantomData::<List<i32>>);
//! ```

use crate::{Result, connection::FFILink, logical_type::LogicalType, value::Value};

mod bignum;
mod decimal;
mod enums;
mod primitive;
mod string;
mod temporal;
mod tuple;
mod uuid;
mod variant;

// `pub(crate)` so `crate::vector` can size array vectors via `array::array_size`.
pub(crate) mod array;
pub(crate) mod list;
mod map;
pub(crate) mod structs;
pub(crate) mod union;

pub use self::uuid::UuidValueRaw;
pub use array::Array;
pub use bignum::{BigNum, BigNumValue};
pub use decimal::{
    Decimal, DecimalContainer, DecimalSignature, DecimalValue, DecimalWidth, HugeDecimal, LongDecimal,
    MAX_DECIMAL_WIDTH, ShortDecimal, Width, WordDecimal,
};
pub use enums::{EnumIndex, EnumStorage};
pub use list::{List, ListIndex};
pub use map::Map;
pub use primitive::Any;
pub use primitive::{HugeInt, UHugeInt};
pub use string::{BitValue, BlobValue, UncheckedString};
pub use structs::Struct;
pub use temporal::{
    DateValue, IntervalValue, TimeNsValue, TimeTzValue, TimeValue, TimestampMsValue, TimestampNsValue,
    TimestampSecValue, TimestampTzNsValue, TimestampTzValue, TimestampValue,
};
pub use union::Union;
pub use variant::{Variant, VariantValue};

#[cfg(feature = "rust_decimal")]
mod rust_decimal;

/// Constructs the DuckDB logical type represented by a Rust type.
pub trait DuckDBType {
    /// Return the DuckDB logical type represented by this Rust type.
    fn logical_type<C: FFILink + ?Sized>(link: &C) -> Result<LogicalType>;
}

/// Converts a Rust value into its DuckDB representation.
pub trait ToValue {
    /// Create a DuckDB value from this value.
    fn value<C: FFILink + ?Sized>(&self, link: &C) -> Result<Value>;

    /// Create a typed DuckDB value representing SQL `NULL`.
    fn null_value<C: FFILink + ?Sized>(link: &C) -> Result<Value>
    where
        Self: DuckDBType,
    {
        Value::null(link, &Self::logical_type(link)?)
    }
}

/// Converts an owned DuckDB value into a Rust value.
pub trait FromValue: Sized {
    /// Decode a value that has already been checked as non-NULL.
    fn _get_inner(value: &Value) -> Result<Self>;

    /// Read a Rust value from a DuckDB value, returning `None` for SQL `NULL`.
    fn from_value(value: &Value) -> Result<Option<Self>> {
        if value.is_null()? {
            Ok(None)
        } else {
            Self::_get_inner(value).map(Some)
        }
    }
}

impl FromValue for LogicalType {
    fn _get_inner(value: &Value) -> Result<Self> {
        value.type_value_get_inner()
    }
}

impl<T: DuckDBType + ?Sized> DuckDBType for &T {
    fn logical_type<C: FFILink + ?Sized>(link: &C) -> Result<LogicalType> {
        T::logical_type(link)
    }
}

impl<T: ToValue + ?Sized> ToValue for &T {
    fn value<C: FFILink + ?Sized>(&self, link: &C) -> Result<Value> {
        (*self).value(link)
    }
}

impl<T: DuckDBType> DuckDBType for Option<T> {
    fn logical_type<C: FFILink + ?Sized>(link: &C) -> Result<LogicalType> {
        T::logical_type(link)
    }
}

impl<T: ToValue + DuckDBType> ToValue for Option<T> {
    fn value<C: FFILink + ?Sized>(&self, link: &C) -> Result<Value> {
        match self {
            Some(value) => value.value(link),
            None => T::null_value(link),
        }
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests;

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
//! uses these representations through [`crate::vector::VectorElement`].
//!
//! Concrete representations and their [`ToValue`]/[`FromValue`]/
//! [`crate::vector::VectorElement`]/[`crate::vector::WritableVectorElement`]
//! implementations live together in family modules grouped by logical type:
//! [`primitive`], `string` (`VARCHAR`/`BLOB`/`BIT`), `temporal`
//! (`DATE`/`TIME`/`TIMESTAMP`/`INTERVAL`), `uuid`, `decimal`, `bignum`,
//! `list`, `array`, `map`, `structs`, `union`, `variant`, and `tuple`. Each
//! family module is private; its public types are re-exported below (or from
//! [`crate::vector`] for vector-row helper types) so existing
//! `crate::types::*` and `crate::vector::*` paths keep working.

use crate::{Result, connection::FFILink, logical_type::LogicalType, value::Value};

mod bignum;
mod decimal;
mod primitive;
mod string;
mod temporal;
mod tuple;
mod uuid;
mod variant;

// Kept `pub(crate)` (rather than private) so `crate::vector` can re-export
// their borrowed row/iterator types at their existing `crate::vector::*`
// paths.
pub(crate) mod array;
pub(crate) mod list;
pub(crate) mod map;
pub(crate) mod structs;
pub(crate) mod union;

pub use array::Array;
pub use bignum::{BigNum, BigNumValue};
pub use decimal::{Decimal, DecimalValue, DecimalValueRaw, InternalDecimalType};
pub use list::List;
pub use map::{Map, MapValue};
pub use primitive::Any;
pub use string::{BitValue, BlobValue};
pub use structs::{Struct, StructSchema, StructValue};
pub use temporal::{
    DateValue, IntervalValue, TimeNsValue, TimeTzValue, TimeValue, TimestampMsValue, TimestampNsValue,
    TimestampSecValue, TimestampTzNsValue, TimestampTzValue, TimestampValue,
};
pub use union::{Union, UnionSchema, UnionValue};
pub use uuid::UuidValue;
pub use variant::{Variant, VariantValue};

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
        Value::null(&Self::logical_type(link)?)
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
        value.logical_type()
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

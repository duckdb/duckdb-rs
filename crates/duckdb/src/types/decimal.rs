//! The `DECIMAL` logical type.
//!
//! [`DecimalValue`] stores a scaled integer with a compile-time width and
//! scale; [`Decimal`] is its vector-element alias with width and scale left
//! for the runtime logical type. [`DecimalValueRaw`] reads a decimal's
//! runtime width, scale, and scaled integer generically.

use super::{DuckDBType, FromValue, ToValue};
use crate::{
    Parameters, Result, check_api_call,
    connection::FFILink,
    logical_type::{LogicalType, LogicalTypeID},
    value::{Value, ValueInput},
    vector::{Vector, VectorElement},
};
use libduckdb_sys as ffi;

/// Marks integer types supported as the physical storage of [`Decimal`].
pub trait InternalDecimalType {
    /// Convert the physical decimal storage to its scaled integer.
    fn to_i128(&self) -> i128;
}

macro_rules! impl_internal_decimal_type {
    ($($type:ty),+ $(,)?) => {
        $(
            impl InternalDecimalType for $type {
                fn to_i128(&self) -> i128 {
                    *self as i128
                }
            }
        )+
    };
}

impl_internal_decimal_type!(i16, i32, i64, i128);

/// A scaled integer represented as `DECIMAL(WIDTH, SCALE)`.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecimalValue<T, const WIDTH: u8, const SCALE: u8>(pub T);

/// A `DECIMAL` vector element stored as the integer type `T`.
/// Width and scale are unset; use DecimalValue when creating new decimals.
pub type Decimal<T> = DecimalValue<T, 0, 0>;

impl<T: InternalDecimalType, const WIDTH: u8, const SCALE: u8> DuckDBType for DecimalValue<T, WIDTH, SCALE> {
    fn logical_type<C: FFILink + ?Sized>(link: &C) -> Result<LogicalType> {
        link.logical_type_create("DECIMAL", Parameters::positional(&[&WIDTH, &SCALE]))
    }
}

impl<T: InternalDecimalType, const WIDTH: u8, const SCALE: u8> ToValue for DecimalValue<T, WIDTH, SCALE> {
    fn value<C: FFILink + ?Sized>(&self, link: &C) -> Result<Value> {
        link.create_value(ValueInput::Decimal {
            value: self.0.to_i128(),
            width: WIDTH,
            scale: SCALE,
        })
    }
}

impl<T: InternalDecimalType> VectorElement for Decimal<T> {
    const TYPE_ID: LogicalTypeID = LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_DECIMAL;

    type Internal = T;

    type Ref<'a>
        = &'a T
    where
        Self: 'a;

    fn get<'a, U: VectorElement>(vector: &'a Vector<'_, U>, physical: usize, _logical: usize) -> Self::Ref<'a>
    where
        Self: Sized + 'a,
    {
        let data_ptr = vector.view.as_ref().unwrap().as_ptr() as *const T;

        (unsafe { &*data_ptr.add(physical) }) as _
    }
}

/// A DuckDB `DECIMAL` in its runtime width, scale, and scaled-integer form.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecimalValueRaw {
    /// The integer payload scaled by ten to [`Self::scale`].
    pub value: i128,
    /// The total number of decimal digits.
    pub width: u8,
    /// The number of digits after the decimal point.
    pub scale: u8,
}

impl FromValue for DecimalValueRaw {
    fn from_value(value: &Value) -> Result<Self> {
        let mut width = 0;
        let mut scale = 0;
        let raw = check_api_call!(ffi::duckdb_v2_value_get_decimal, **value, RET, &mut width, &mut scale)?;
        Ok(Self {
            value: (i128::from(raw.upper) << 64) | i128::from(raw.lower),
            width,
            scale,
        })
    }
}

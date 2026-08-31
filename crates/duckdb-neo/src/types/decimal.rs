//! The `DECIMAL` logical type.
//!
//! [`DecimalValue`] stores a scaled integer with a compile-time width and
//! scale; [`Decimal`] is its vector-element alias with width and scale left
//! for the runtime logical type. [`DecimalValueRaw`] reads a decimal's
//! runtime width, scale, and scaled integer generically.

use super::{DuckDBType, FromValue, ToValue};
use crate::ffi;
use crate::{
    Parameters, Result, check_api_call,
    connection::FFILink,
    logical_type::{LogicalType, LogicalTypeID},
    value::{Value, ValueInput},
    vector::{Unknown, Vector, VectorElement, WritableVectorElement},
};
use std::ops::Deref;

/// Resolves the physical storage integer type for a `DECIMAL` of the given width.
#[macro_export]
macro_rules! get_decimal_size {
    (1) => {
        i16
    };
    (2) => {
        i16
    };
    (3) => {
        i16
    };
    (4) => {
        i16
    };
    (5) => {
        i32
    };
    (6) => {
        i32
    };
    (7) => {
        i32
    };
    (8) => {
        i32
    };
    (9) => {
        i32
    };
    (10) => {
        i64
    };
    (11) => {
        i64
    };
    (12) => {
        i64
    };
    (13) => {
        i64
    };
    (14) => {
        i64
    };
    (15) => {
        i64
    };
    (16) => {
        i64
    };
    (17) => {
        i64
    };
    (18) => {
        i64
    };
    (19) => {
        i128
    };
    (20) => {
        i128
    };
    (21) => {
        i128
    };
    (22) => {
        i128
    };
    (23) => {
        i128
    };
    (24) => {
        i128
    };
    (25) => {
        i128
    };
    (26) => {
        i128
    };
    (27) => {
        i128
    };
    (28) => {
        i128
    };
    (29) => {
        i128
    };
    (30) => {
        i128
    };
    (31) => {
        i128
    };
    (32) => {
        i128
    };
    (33) => {
        i128
    };
    (34) => {
        i128
    };
    (35) => {
        i128
    };
    (36) => {
        i128
    };
    (37) => {
        i128
    };
    (38) => {
        i128
    };
}

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

            impl From<$type> for Decimal<$type> {
                fn from(value: $type) -> Self {
                    DecimalValue(value)
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

impl<T, const WIDTH: u8, const SCALE: u8> Deref for DecimalValue<T, WIDTH, SCALE> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: InternalDecimalType, const WIDTH: u8, const SCALE: u8> DecimalValue<T, WIDTH, SCALE> {
    /// Convert to a [`rust_decimal::Decimal`] with the given scale.
    #[cfg(feature = "rust_decimal")]
    pub fn to_rust_decimal(&self, scale: u32) -> rust_decimal::Decimal {
        rust_decimal::Decimal::from_i128_with_scale(self.0.to_i128(), scale)
    }
}

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
        = &'a Decimal<T>
    where
        Self: 'a;

    fn validate(other: &LogicalType, _children: &[Vector<'_, Unknown>]) -> Result<bool> {
        if other.type_id() != Self::TYPE_ID {
            return Ok(false);
        }

        let (_, width) = other.get_param(0)?;
        let Some(width) = width.get::<u8>()? else {
            return Ok(false);
        };
        let storage_size = match width {
            1..=4 => size_of::<i16>(),
            5..=9 => size_of::<i32>(),
            10..=18 => size_of::<i64>(),
            19..=38 => size_of::<i128>(),
            _ => return Ok(false),
        };

        Ok(size_of::<T>() == storage_size)
    }

    fn get<'a, U: VectorElement>(vector: &'a Vector<'_, U>, physical: usize, _logical: usize) -> Self::Ref<'a>
    where
        Self: Sized + 'a,
    {
        let data_ptr = vector.view.as_ref().unwrap().as_ptr() as *const Decimal<T>;

        (unsafe { &*data_ptr.add(physical) }) as _
    }
}

impl<T: InternalDecimalType + 'static> WritableVectorElement for Decimal<T> {
    type Write<'a>
        = T
    where
        T: 'a;

    fn write(vector: &mut Vector<'_, Self>, index: usize, value: Option<Self::Write<'_>>) -> Result<()> {
        vector.write_raw(index, value)
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
    fn _get_inner(value: &Value) -> Result<Self> {
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

impl DecimalValueRaw {
    /// Convert a [`rust_decimal::Decimal`] to a decimal of the given width.
    #[cfg(feature = "rust_decimal")]
    pub fn from_rust_decimal(value: rust_decimal::Decimal, width: u8) -> Self {
        Self {
            value: value.mantissa(),
            width,
            scale: value.scale() as u8,
        }
    }
}

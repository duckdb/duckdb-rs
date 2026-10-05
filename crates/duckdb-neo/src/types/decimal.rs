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

impl<T: InternalDecimalType, const WIDTH: u8, const SCALE: u8> DecimalValue<T, WIDTH, SCALE> {
    /// Convert to a [`rust_decimal::Decimal`] with the given scale.
    ///
    /// Fails when the value or scale exceeds `rust_decimal`'s 96-bit mantissa or scale limit of 28.
    #[cfg(feature = "rust_decimal")]
    pub fn to_rust_decimal(&self, scale: u32) -> std::result::Result<rust_decimal::Decimal, rust_decimal::Error> {
        rust_decimal::Decimal::try_from_i128_with_scale(self.0.to_i128(), scale)
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

impl ToValue for DecimalValueRaw {
    fn value<C: FFILink + ?Sized>(&self, link: &C) -> Result<Value> {
        link.create_value(ValueInput::Decimal {
            value: self.value,
            width: self.width,
            scale: self.scale,
        })
    }
}

#[cfg(feature = "rust_decimal")]
mod external {
    use super::{Decimal, DecimalValueRaw};
    use crate::{
        Error, Result,
        logical_type::{LogicalType, LogicalTypeID},
        vector::{Unknown, Vector, VectorElement},
    };

    /// Widest `DECIMAL` whose every value fits `rust_decimal`'s 96-bit mantissa.
    const MAX_WIDTH: u8 = 28;

    impl DecimalValueRaw {
        /// Convert a [`rust_decimal::Decimal`] to a decimal of the given width.
        pub fn from_rust_decimal(value: rust_decimal::Decimal, width: u8) -> Self {
            Self {
                value: value.mantissa(),
                width,
                scale: value.scale() as u8,
            }
        }
    }

    impl TryFrom<DecimalValueRaw> for rust_decimal::Decimal {
        type Error = rust_decimal::Error;

        fn try_from(value: DecimalValueRaw) -> std::result::Result<Self, Self::Error> {
            Self::try_from_i128_with_scale(value.value, u32::from(value.scale))
        }
    }

    fn width_and_scale(logical_type: &LogicalType) -> Result<(u8, u8)> {
        let param = |index| -> Result<u8> {
            logical_type
                .get_param(index)?
                .1
                .get::<u8>()?
                .ok_or_else(|| Error::api_error("DECIMAL logical type is missing a parameter"))
        };
        Ok((param(0)?, param(1)?))
    }

    fn read<T: Copy + Into<i128>, U: VectorElement>(vector: &Vector<'_, U>, physical: usize) -> i128 {
        let data_ptr = vector.view.as_ref().unwrap().as_ptr() as *const Decimal<T>;
        (unsafe { *data_ptr.add(physical) }).0.into()
    }

    /// Reads `DECIMAL` columns up to width 28, so every value converts losslessly.
    impl VectorElement for rust_decimal::Decimal {
        const TYPE_ID: LogicalTypeID = LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_DECIMAL;

        type Internal = ();

        type Ref<'a> = rust_decimal::Decimal;

        fn validate(other: &LogicalType, _children: &[Vector<'_, Unknown>]) -> Result<bool> {
            if other.type_id() != Self::TYPE_ID {
                return Ok(false);
            }
            let (width, _) = width_and_scale(other)?;
            Ok((1..=MAX_WIDTH).contains(&width))
        }

        fn get<'a, U: VectorElement>(vector: &'a Vector<'_, U>, physical: usize, _logical: usize) -> Self::Ref<'a>
        where
            Self: 'a,
        {
            let (width, scale) = width_and_scale(&vector.logical_type).expect("validated DECIMAL logical type");
            let value = match width {
                1..=4 => read::<i16, U>(vector, physical),
                5..=9 => read::<i32, U>(vector, physical),
                10..=18 => read::<i64, U>(vector, physical),
                _ => read::<i128, U>(vector, physical),
            };
            // Width <= 28 and scale <= width keep this within rust_decimal's limits.
            rust_decimal::Decimal::from_i128_with_scale(value, u32::from(scale))
        }
    }

    #[cfg(test)]
    mod tests {
        use std::str::FromStr;

        use super::*;
        use crate::{
            Parameters,
            environment::{Environment, StorageLocation},
            types::{DecimalValue, ToValue},
        };

        fn dec(text: &str) -> rust_decimal::Decimal {
            rust_decimal::Decimal::from_str(text).unwrap()
        }

        #[test]
        fn rust_decimal_vector_reads_every_storage_width() -> Result<()> {
            let env = Environment::new()?;
            let db = env.open(StorageLocation::InMemory)?;
            let conn = db.connect()?;

            for (sql_type, values) in [
                ("DECIMAL(4, 1)", ["123.4", "-999.9", "0.0"]),
                ("DECIMAL(9, 3)", ["123456.789", "-0.001", "0.000"]),
                (
                    "DECIMAL(18, 6)",
                    ["123456789012.345678", "-999999999999.999999", "0.000001"],
                ),
                (
                    "DECIMAL(28, 10)",
                    [
                        "123456789012345678.0123456789",
                        "-999999999999999999.9999999999",
                        "0.0000000001",
                    ],
                ),
            ] {
                let sql = format!(
                    "SELECT v::{sql_type} FROM (VALUES ('{}'), ('{}'), ('{}'), (NULL)) t(v)",
                    values[0], values[1], values[2]
                );
                let mut result = conn.query(sql.as_str(), Parameters::None)?;
                let chunk = result.next().expect("query returned no rows")?;
                let vector = chunk.get_vector_at::<rust_decimal::Decimal>(0)?;

                let mut expected: Vec<_> = values.iter().map(|v| Some(dec(v))).collect();
                expected.push(None);
                assert_eq!(vector.iter()?.collect::<Vec<_>>(), expected, "{sql_type}");
            }

            Ok(())
        }

        #[test]
        fn rust_decimal_vector_rejects_wide_decimals() -> Result<()> {
            let env = Environment::new()?;
            let db = env.open(StorageLocation::InMemory)?;
            let conn = db.connect()?;

            let mut result = conn.query("SELECT 1::DECIMAL(29, 0)", Parameters::None)?;
            let chunk = result.next().expect("query returned no rows")?;
            assert!(chunk.get_vector_at::<rust_decimal::Decimal>(0).is_err());
            assert!(chunk.get_vector_at::<Decimal<i128>>(0).is_ok());

            Ok(())
        }

        #[test]
        fn to_rust_decimal_reports_overflow() {
            assert_eq!(DecimalValue::<i32, 9, 2>(-12345).to_rust_decimal(2), Ok(dec("-123.45")));
            assert!(Decimal::<i128>::from(10i128.pow(37)).to_rust_decimal(0).is_err());
            assert!(Decimal::<i16>::from(1).to_rust_decimal(29).is_err());
        }

        #[test]
        fn decimal_value_raw_round_trips_rust_decimal() -> Result<()> {
            let env = Environment::new()?;
            let db = env.open(StorageLocation::InMemory)?;
            let conn = db.connect()?;

            for (text, width) in [("123.45", 5), ("-0.001", 4), ("79228162514264337593543950335", 38)] {
                let decimal = dec(text);
                let raw = DecimalValueRaw::from_rust_decimal(decimal, width);
                assert_eq!(rust_decimal::Decimal::try_from(raw), Ok(decimal));

                let value = raw.value(&conn)?;
                assert_eq!(value.get::<DecimalValueRaw>()?, Some(raw));

                let mut result = conn.query("SELECT $1::VARCHAR", Parameters::positional(&[&raw]))?;
                let chunk = result.next().expect("query returned no rows")?;
                let text_vector = chunk.get_vector_at::<String>(0)?;
                assert_eq!(text_vector.get(0)?.map(|s| s.to_string()), Some(text.to_string()));
            }

            let too_wide = DecimalValueRaw {
                value: i128::MAX,
                width: 38,
                scale: 0,
            };
            assert!(rust_decimal::Decimal::try_from(too_wide).is_err());

            Ok(())
        }
    }
}

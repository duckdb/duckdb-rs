use crate::{
    Error, FromValue, Result, ToValue,
    connection::FFILink,
    error::check_api_call,
    ffi,
    logical_type::{LogicalType, LogicalTypeID},
    value::{Value, ValueInput},
    vector::{Unknown, Vector, VectorElement, WritableVectorElement},
};

/// Integer types DuckDB uses as the physical storage of a `DECIMAL`.
pub trait DecimalContainer {
    fn to_i128(&self) -> i128;
}

macro_rules! decimal_storage {
    ($($(#[$doc:meta])* $name:ident($type:ty) => $min:literal..=$max:literal;)+) => {
        $(
            impl DecimalContainer for $type {
                fn to_i128(&self) -> i128 {
                    i128::from(*self)
                }
            }

            $(#[$doc])*
            #[repr(transparent)]
            #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
            pub struct $name(pub $type);

            impl DecimalContainer for $name {
                fn to_i128(&self) -> i128 {
                    self.0.to_i128()
                }
            }

            impl From<$type> for $name {
                fn from(value: $type) -> Self {
                    Self(value)
                }
            }

            impl VectorElement for $name {
                const TYPE_ID: LogicalTypeID = LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_DECIMAL;

                type Internal = $type;

                type Ref<'a> = &'a $name;

                fn validate(other: &LogicalType, _children: &[Vector<'_, Unknown>]) -> Result<bool> {
                    if other.type_id() != Self::TYPE_ID {
                        return Ok(false);
                    }
                    Ok(matches!(DecimalSignature::from_logical_type(other)?.width, $min..=$max))
                }

                fn get<'a, U: VectorElement>(vector: &'a Vector<'_, U>, physical: usize, _logical: usize) -> Self::Ref<'a>
                where
                    Self: 'a,
                {
                    let data_ptr = vector.view.as_ref().unwrap().as_ptr() as *const $name;
                    unsafe { &*data_ptr.add(physical) }
                }
            }

            impl WritableVectorElement for $name {
                type Write<'a> = $name;

                fn write(vector: &mut Vector<'_, Self>, index: usize, value: Option<Self::Write<'_>>) -> Result<()> {
                    vector.write_raw(index, value)
                }
            }
        )+
    };
}

decimal_storage! {
    /// Storage of a `DECIMAL` with width 1 to 4.
    ShortDecimal(i16) => 1..=4;
    /// Storage of a `DECIMAL` with width 5 to 9.
    WordDecimal(i32) => 5..=9;
    /// Storage of a `DECIMAL` with width 10 to 18.
    LongDecimal(i64) => 10..=18;
    /// Storage of a `DECIMAL` with width 19 to 38.
    HugeDecimal(i128) => 19..=38;
}

/// The width and scale of a `DECIMAL` logical type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecimalSignature {
    pub width: u8,
    pub scale: u8,
}

/// A DuckDB `DECIMAL` in its runtime width, scale, and scaled-integer form.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecimalValue {
    /// The integer payload scaled by ten to [`Self::scale`].
    pub value: i128,
    /// The total number of decimal digits.
    pub width: u8,
    /// The number of digits after the decimal point.
    pub scale: u8,
}

impl DecimalValue {
    /// Returns `None` when the scaled integer does not fit `T`.
    pub fn to_primitive<T: DecimalContainer + TryFrom<i128>>(&self) -> Option<T> {
        T::try_from(self.value).ok()
    }

    pub fn from_primitive<T: DecimalContainer>(value: T, width: u8, scale: u8) -> Self {
        Self {
            value: value.to_i128(),
            width,
            scale,
        }
    }
}

impl From<DecimalValue> for DecimalSignature {
    fn from(value: DecimalValue) -> Self {
        Self {
            width: value.width,
            scale: value.scale,
        }
    }
}

impl DecimalSignature {
    pub fn logical_type<C: FFILink + ?Sized>(&self, link: &C) -> Result<LogicalType> {
        link.logical_type_create("DECIMAL", crate::Parameters::Positional(&[&self.width, &self.scale]))
    }

    pub fn from_logical_type(logical_type: &LogicalType) -> Result<Self> {
        if logical_type.type_id() != LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_DECIMAL {
            return Err(Error::api_error("logical type is not DECIMAL"));
        }
        let param = |index| -> Result<u8> {
            logical_type
                .get_param(index)?
                .1
                .get::<u8>()?
                .ok_or_else(|| Error::api_error("DECIMAL logical type is missing a parameter"))
        };
        Ok(Self {
            width: param(0)?,
            scale: param(1)?,
        })
    }

    pub fn to_value<T: DecimalContainer>(&self, value: T) -> DecimalValue {
        DecimalValue::from_primitive(value, self.width, self.scale)
    }
}

impl ToValue for DecimalValue {
    fn value<C: FFILink + ?Sized>(&self, link: &C) -> Result<Value> {
        link.create_value(ValueInput::Decimal {
            value: self.value,
            width: self.width,
            scale: self.scale,
        })
    }
}

impl FromValue for DecimalValue {
    fn _get_inner(value: &Value) -> Result<Self> {
        let mut width: u8 = 0;
        let mut scale: u8 = 0;

        let raw = check_api_call!(ffi::duckdb_v2_value_get_decimal, **value, RET, &mut width, &mut scale)?;

        Ok(Self {
            value: (i128::from(raw.upper) << 64) | i128::from(raw.lower),
            width,
            scale,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        Parameters,
        environment::{Environment, StorageLocation},
    };

    #[test]
    fn decimal_storage_matches_width() -> Result<()> {
        let env = Environment::new()?;
        let db = env.open(StorageLocation::InMemory)?;
        let conn = db.connect()?;

        let mut result = conn.query(
            "SELECT -1.5::DECIMAL(4, 1), 12.345::DECIMAL(9, 3), 1.5::DECIMAL(18, 1), 1.5::DECIMAL(38, 1)",
            Parameters::None,
        )?;
        let chunk = result.next().expect("query returned no rows")?;

        assert_eq!(
            chunk.get_vector_at::<ShortDecimal>(0)?.get(0)?,
            Some(&ShortDecimal(-15))
        );
        assert_eq!(
            chunk.get_vector_at::<WordDecimal>(1)?.get(0)?,
            Some(&WordDecimal(12_345))
        );
        assert_eq!(chunk.get_vector_at::<LongDecimal>(2)?.get(0)?, Some(&LongDecimal(15)));
        assert_eq!(chunk.get_vector_at::<HugeDecimal>(3)?.get(0)?, Some(&HugeDecimal(15)));

        assert!(chunk.get_vector_at::<WordDecimal>(0).is_err());
        assert!(chunk.get_vector_at::<HugeDecimal>(2).is_err());
        assert!(chunk.get_vector_at::<i16>(0).is_err());

        let signature = DecimalSignature::from_logical_type(chunk.get_vector_at::<WordDecimal>(1)?.logical_type())?;
        assert_eq!(signature, DecimalSignature { width: 9, scale: 3 });

        Ok(())
    }
}

#[cfg(feature = "rust_decimal")]
mod external {
    use super::DecimalValue;

    impl DecimalValue {
        /// Convert a [`rust_decimal::Decimal`] to a decimal of the given width.
        pub fn from_rust_decimal(value: rust_decimal::Decimal, width: u8) -> Self {
            Self {
                value: value.mantissa(),
                width,
                scale: value.scale() as u8,
            }
        }
    }

    /// Fails when the value or scale exceeds `rust_decimal`'s 96-bit mantissa or scale limit of 28.
    impl TryFrom<DecimalValue> for rust_decimal::Decimal {
        type Error = rust_decimal::Error;

        fn try_from(value: DecimalValue) -> std::result::Result<Self, Self::Error> {
            Self::try_from_i128_with_scale(value.value, u32::from(value.scale))
        }
    }

    #[cfg(test)]
    mod tests {
        use std::str::FromStr;

        use super::*;
        use crate::{
            Parameters, ToValue,
            environment::{Environment, StorageLocation},
        };

        fn dec(text: &str) -> rust_decimal::Decimal {
            rust_decimal::Decimal::from_str(text).unwrap()
        }

        #[test]
        fn decimal_value_round_trips_rust_decimal() -> crate::Result<()> {
            let env = Environment::new()?;
            let db = env.open(StorageLocation::InMemory)?;
            let conn = db.connect()?;

            for (text, width) in [("123.45", 5), ("-0.001", 4), ("79228162514264337593543950335", 38)] {
                let decimal = dec(text);
                let val = DecimalValue::from_rust_decimal(decimal, width);
                assert_eq!(rust_decimal::Decimal::try_from(val), Ok(decimal));

                let value = val.value(&conn)?;
                assert_eq!(value.get::<DecimalValue>()?, Some(val));

                let mut result = conn.query("SELECT $1::VARCHAR", Parameters::positional(&[&val]))?;
                let chunk = result.next().expect("query returned no rows")?;
                let text_vector = chunk.get_vector_at::<String>(0)?;
                assert_eq!(text_vector.get(0)?.map(|s| s.to_string()), Some(text.to_string()));
            }

            Ok(())
        }

        #[test]
        fn rust_decimal_conversion_reports_overflow() {
            let too_wide = DecimalValue::from_primitive(i128::MAX, 38, 0);
            assert!(rust_decimal::Decimal::try_from(too_wide).is_err());

            let too_precise = DecimalValue::from_primitive(1i16, 30, 29);
            assert!(rust_decimal::Decimal::try_from(too_precise).is_err());
        }
    }
}

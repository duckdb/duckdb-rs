//! The `DECIMAL` logical type.
//!
//! [`Decimal`] fixes width and scale at compile time and picks its storage
//! integer from the width, like `decimal_t<WIDTH, SCALE>` in the C++ API.
//! [`DecimalValue`] carries width and scale at runtime. [`ShortDecimal`],
//! [`WordDecimal`], [`LongDecimal`], and [`HugeDecimal`] read a vector's raw
//! storage for any scale within their width range.
//!
//! With the `rust_decimal` feature, `rust_decimal::Decimal` converts to and
//! from DuckDB values and vectors.

use std::fmt::Debug;
use std::hash::Hash;

use super::{DuckDBType, FromValue, ToValue};
use crate::{
    Error, Parameters, Result,
    connection::FFILink,
    error::check_api_call,
    ffi,
    logical_type::{LogicalType, LogicalTypeID},
    value::{Value, ValueInput},
    vector::{Unknown, Vector, VectorElement, WritableVectorElement},
};

/// The widest `DECIMAL` DuckDB supports.
pub const MAX_DECIMAL_WIDTH: u8 = 38;

/// Integer types DuckDB uses as the physical storage of a `DECIMAL`.
pub trait DecimalContainer {
    /// Widen the stored scaled integer.
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

/// Marks a `DECIMAL` width; [`DecimalWidth`] is implemented for 1 through 38.
pub struct Width<const WIDTH: u8>;

/// Maps a `DECIMAL` width to the integer DuckDB stores it in.
pub trait DecimalWidth {
    /// The storage integer for this width.
    type Storage: DecimalContainer + TryFrom<i128> + Copy + Debug + Eq + Ord + Hash + 'static;
}

macro_rules! decimal_widths {
    ($($type:ty => [$($width:literal),+];)+) => {
        $($(
            impl DecimalWidth for Width<$width> {
                type Storage = $type;
            }
        )+)+
    };
}

decimal_widths! {
    i16 => [1, 2, 3, 4];
    i32 => [5, 6, 7, 8, 9];
    i64 => [10, 11, 12, 13, 14, 15, 16, 17, 18];
    i128 => [19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38];
}

/// A `DECIMAL(WIDTH, SCALE)` stored as its scaled integer.
///
/// The width picks the storage integer, so `Decimal<9, 2>` and `Decimal<18, 2>`
/// are different types and their payloads are not interchangeable.
///
/// ```
/// use duckdb_neo::types::Decimal;
///
/// let price = Decimal::<5, 2>::new(12_345); // 123.45, stored as i32
/// assert_eq!(price.0, 12_345i32);
/// ```
///
/// A scale above the width does not compile:
///
/// ```compile_fail
/// let _ = duckdb_neo::types::Decimal::<2, 3>::new(1);
/// ```
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Decimal<const WIDTH: u8, const SCALE: u8>(pub <Width<WIDTH> as DecimalWidth>::Storage)
where
    Width<WIDTH>: DecimalWidth;

impl<const WIDTH: u8, const SCALE: u8> Decimal<WIDTH, SCALE>
where
    Width<WIDTH>: DecimalWidth,
{
    const VALID_SCALE: () = assert!(SCALE <= WIDTH, "DECIMAL scale must not exceed its width");

    /// Wrap a scaled integer, e.g. `Decimal::<5, 2>::new(12_345)` for `123.45`.
    pub const fn new(value: <Width<WIDTH> as DecimalWidth>::Storage) -> Self {
        let () = Self::VALID_SCALE;
        Self(value)
    }
}

impl<const WIDTH: u8, const SCALE: u8> From<Decimal<WIDTH, SCALE>> for DecimalValue
where
    Width<WIDTH>: DecimalWidth,
{
    fn from(value: Decimal<WIDTH, SCALE>) -> Self {
        DecimalValue::new(value.0, WIDTH, SCALE)
    }
}

impl<const WIDTH: u8, const SCALE: u8> TryFrom<DecimalValue> for Decimal<WIDTH, SCALE>
where
    Width<WIDTH>: DecimalWidth,
{
    type Error = Error;

    /// Fails unless `value` has exactly this width and scale.
    fn try_from(value: DecimalValue) -> Result<Self> {
        if value.width != WIDTH || value.scale != SCALE {
            return Err(Error::invalid_input(format!(
                "expected DECIMAL({WIDTH}, {SCALE}), got DECIMAL({}, {})",
                value.width, value.scale
            )));
        }
        value
            .to_primitive()
            .map(Self::new)
            .ok_or_else(|| Error::invalid_input(format!("{} does not fit DECIMAL({WIDTH}, {SCALE})", value.value)))
    }
}

impl<const WIDTH: u8, const SCALE: u8> DuckDBType for Decimal<WIDTH, SCALE>
where
    Width<WIDTH>: DecimalWidth,
{
    fn logical_type<C: FFILink + ?Sized>(link: &C) -> Result<LogicalType> {
        let () = Self::VALID_SCALE;
        DecimalSignature {
            width: WIDTH,
            scale: SCALE,
        }
        .logical_type(link)
    }
}

impl<const WIDTH: u8, const SCALE: u8> ToValue for Decimal<WIDTH, SCALE>
where
    Width<WIDTH>: DecimalWidth,
{
    fn value<C: FFILink + ?Sized>(&self, link: &C) -> Result<Value> {
        DecimalValue::from(*self).value(link)
    }
}

impl<const WIDTH: u8, const SCALE: u8> FromValue for Decimal<WIDTH, SCALE>
where
    Width<WIDTH>: DecimalWidth,
{
    fn _get_inner(value: &Value) -> Result<Self> {
        DecimalValue::_get_inner(value)?.try_into()
    }
}

impl<const WIDTH: u8, const SCALE: u8> VectorElement for Decimal<WIDTH, SCALE>
where
    Width<WIDTH>: DecimalWidth,
{
    const TYPE_ID: LogicalTypeID = LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_DECIMAL;

    type Internal = <Width<WIDTH> as DecimalWidth>::Storage;

    type Ref<'a> = &'a Self;

    fn validate(other: &LogicalType, _children: &[Vector<'_, Unknown>]) -> Result<bool> {
        if other.type_id() != Self::TYPE_ID {
            return Ok(false);
        }
        Ok(DecimalSignature::from_logical_type(other)?
            == DecimalSignature {
                width: WIDTH,
                scale: SCALE,
            })
    }

    fn get<'a, U: VectorElement>(vector: &'a Vector<'_, U>, physical: usize, _logical: usize) -> Self::Ref<'a>
    where
        Self: 'a,
    {
        let data_ptr = vector.view.as_ref().unwrap().as_ptr() as *const Self;
        unsafe { &*data_ptr.add(physical) }
    }
}

impl<const WIDTH: u8, const SCALE: u8> WritableVectorElement for Decimal<WIDTH, SCALE>
where
    Width<WIDTH>: DecimalWidth,
{
    type Write<'a> = Self;

    fn write(vector: &mut Vector<'_, Self>, index: usize, value: Option<Self::Write<'_>>) -> Result<()> {
        vector.write_raw(index, value)
    }
}

/// The width and scale of a `DECIMAL` logical type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecimalSignature {
    /// The total number of decimal digits.
    pub width: u8,
    /// The number of digits after the decimal point.
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
    /// Build a decimal from a scaled integer; checked when converted to a DuckDB value.
    pub fn new<T: DecimalContainer>(value: T, width: u8, scale: u8) -> Self {
        Self {
            value: value.to_i128(),
            width,
            scale,
        }
    }

    /// Returns `None` when the scaled integer does not fit `T`.
    pub fn to_primitive<T: DecimalContainer + TryFrom<i128>>(&self) -> Option<T> {
        T::try_from(self.value).ok()
    }

    /// Check the signature and that the value has at most `width` digits.
    pub fn check(&self) -> Result<()> {
        DecimalSignature::from(*self).check()?;
        if self.value.unsigned_abs() >= 10u128.pow(u32::from(self.width)) {
            return Err(Error::invalid_input(format!(
                "{} does not fit DECIMAL({}, {})",
                self.value, self.width, self.scale
            )));
        }
        Ok(())
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
    /// Check that `1 <= width <= 38` and `scale <= width`.
    pub fn check(&self) -> Result<()> {
        if !(1..=MAX_DECIMAL_WIDTH).contains(&self.width) || self.scale > self.width {
            return Err(Error::invalid_input(format!(
                "invalid DECIMAL({}, {}): width must be 1 to {MAX_DECIMAL_WIDTH} and scale at most the width",
                self.width, self.scale
            )));
        }
        Ok(())
    }

    /// Create the `DECIMAL(width, scale)` logical type.
    pub fn logical_type<C: FFILink + ?Sized>(&self, link: &C) -> Result<LogicalType> {
        link.logical_type_create("DECIMAL", Parameters::Positional(&[&self.width, &self.scale]))
    }

    /// Read the width and scale of a `DECIMAL` logical type.
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

    /// Pair a scaled integer with this width and scale.
    pub fn to_value<T: DecimalContainer>(&self, value: T) -> DecimalValue {
        DecimalValue::new(value, self.width, self.scale)
    }
}

impl ToValue for DecimalValue {
    fn value<C: FFILink + ?Sized>(&self, link: &C) -> Result<Value> {
        self.check()?;
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
        data_chunk::DataChunk,
        environment::{Environment, StorageLocation},
    };

    #[test]
    fn decimal_storage_matches_width() -> Result<()> {
        let env = Environment::new()?;
        let db = env.open(StorageLocation::InMemory)?;
        let conn = db.connect()?;

        let mut result = conn.query(
            "SELECT -1.5::DECIMAL(4, 1), 12.345::DECIMAL(9, 3), 1.5::DECIMAL(18, 1), 1.5::DECIMAL(38, 1), 1::INTEGER",
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

        let vector = chunk.get_vector_at::<WordDecimal>(1)?;
        assert_eq!(
            DecimalSignature::from_logical_type(vector.logical_type())?,
            DecimalSignature { width: 9, scale: 3 }
        );
        assert_eq!(
            vector.decimal_signature(),
            Some(DecimalSignature { width: 9, scale: 3 })
        );
        assert_eq!(
            chunk.get_vector_at::<i32>(4).ok().and_then(|v| v.decimal_signature()),
            None
        );

        Ok(())
    }

    #[test]
    fn typed_decimal_round_trips() -> Result<()> {
        let env = Environment::new()?;
        let db = env.open(StorageLocation::InMemory)?;
        let conn = db.connect()?;

        let decimal = Decimal::<18, 3>::new(-123_456);
        let value = decimal.value(&conn)?;
        assert_eq!(value.dbg_string()?, "-123.456");
        assert_eq!(value.get::<Decimal<18, 3>>()?, Some(decimal));
        assert!(value.get::<Decimal<18, 2>>().is_err());
        assert!(value.get::<Decimal<9, 3>>().is_err());

        let mut result = conn.query(
            "SELECT $1, $2, $3::VARCHAR",
            Parameters::positional(&[
                &Some(decimal),
                &None::<Decimal<4, 1>>,
                &vec![Decimal::<5, 2>::new(12_345)],
            ]),
        )?;
        let chunk = result.next().expect("query returned no rows")?;
        assert_eq!(chunk.get_vector_at::<Decimal<18, 3>>(0)?.get(0)?, Some(&decimal));
        assert_eq!(chunk.get_vector_at::<Decimal<4, 1>>(1)?.get(0)?, None);
        assert_eq!(
            chunk.get_vector_at::<String>(2)?.get(0)?.map(|s| s.to_string()),
            Some("[123.45]".to_string())
        );
        assert!(chunk.get_vector_at::<Decimal<18, 2>>(0).is_err());
        assert!(chunk.get_vector_at::<Decimal<17, 3>>(0).is_err());

        let chunk = DataChunk::create(&[Decimal::<9, 2>::logical_type(&conn)?], true)?;
        let mut vector = chunk.get_vector_at::<Decimal<9, 2>>(0)?;
        vector.set_size(2)?;
        vector.write(0, Some(Decimal::new(-1)))?;
        vector.write(1, None)?;
        assert_eq!(vector.get(0)?, Some(&Decimal::new(-1)));
        assert_eq!(vector.get(1)?, None);

        Ok(())
    }

    #[test]
    fn decimal_value_is_checked() -> Result<()> {
        let env = Environment::new()?;
        let db = env.open(StorageLocation::InMemory)?;
        let conn = db.connect()?;

        assert!(DecimalValue::new(9_999i16, 4, 0).value(&conn).is_ok());
        assert!(DecimalValue::new(-9_999i16, 4, 4).value(&conn).is_ok());
        assert!(DecimalValue::new(10_000i16, 4, 0).value(&conn).is_err());
        assert!(DecimalValue::new(i64::MAX, 4, 0).value(&conn).is_err());
        assert!(DecimalValue::new(i128::MAX, 38, 0).value(&conn).is_err());
        assert!(DecimalValue::new(1i16, 2, 3).value(&conn).is_err());
        assert!(DecimalValue::new(1i16, 0, 0).value(&conn).is_err());
        assert!(DecimalValue::new(1i16, 39, 0).value(&conn).is_err());

        Ok(())
    }
}

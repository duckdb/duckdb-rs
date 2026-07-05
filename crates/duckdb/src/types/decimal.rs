use std::{
    error, fmt,
    hash::{Hash, Hasher},
};

use crate::{
    Result, ToSql, ffi,
    types::{FromSql, FromSqlError, FromSqlResult, ToSqlOutput, Value, ValueRef},
};

use super::{TimeUnit, to_duckdb_hugeint};

#[cfg(feature = "rust_decimal")]
use rust_decimal::prelude::FromPrimitive as _;

/// A DuckDB `DECIMAL(width, scale)` value.
///
/// `value` stores the scaled integer payload. For example,
/// `123.45::DECIMAL(9, 2)` is represented as
/// `Decimal::new(9, 2, 12345)?`.
///
/// The declared width is preserved when reading and writing DuckDB `DECIMAL`
/// columns. Values converted from non-decimal sources, such as integers or
/// `rust_decimal` values, infer the smallest width that can represent the
/// scaled payload. Width is not part of equality or hashing: values with the
/// same `scale` and scaled integer payload compare equal even if they arrived
/// through paths with different declared widths. Use [`Decimal::width`] when
/// the declared width matters; `Eq` and `Hash` do not preserve it.
///
/// Scale is part of identity: `DECIMAL(2, 1)` with payload `12` and
/// `DECIMAL(3, 2)` with payload `120` compare different. `Decimal`
/// intentionally does not implement `Ord` or `PartialOrd`; sort in SQL or
/// compare an explicitly normalized representation when ordering is needed.
#[derive(Copy, Clone, Debug)]
pub struct Decimal {
    width: u8,
    scale: u8,
    value: i128,
}

impl PartialEq for Decimal {
    fn eq(&self, other: &Self) -> bool {
        self.scale == other.scale && self.value == other.value
    }
}

impl Eq for Decimal {}

impl Hash for Decimal {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.scale.hash(state);
        self.value.hash(state);
    }
}

impl fmt::Display for Decimal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let scale = usize::from(self.scale);
        let raw = self.value.unsigned_abs().to_string();

        if scale == 0 {
            return f.pad_integral(!self.value.is_negative(), "", &raw);
        }

        let mut magnitude = String::new();
        if raw.len() <= scale {
            magnitude.push_str("0.");
            for _ in 0..(scale - raw.len()) {
                magnitude.push('0');
            }
            magnitude.push_str(&raw);
        } else {
            let point = raw.len() - scale;
            magnitude.push_str(&raw[..point]);
            magnitude.push('.');
            magnitude.push_str(&raw[point..]);
        }

        f.pad_integral(!self.value.is_negative(), "", &magnitude)
    }
}

impl Decimal {
    /// DuckDB's maximum `DECIMAL` width.
    pub const MAX_WIDTH: u8 = 38;

    /// Builds a valid `DECIMAL(width, scale)` wrapper.
    ///
    /// Returns [`DecimalError`] when `width` is outside DuckDB's `1..=38`
    /// range, `scale` is larger than `width`, or `value` has more digits than
    /// `width` allows.
    pub fn new(width: u8, scale: u8, value: i128) -> std::result::Result<Self, DecimalError> {
        if width == 0 || width > Self::MAX_WIDTH {
            return Err(DecimalError::InvalidWidth {
                width,
                max: Self::MAX_WIDTH,
            });
        }
        if scale > width {
            return Err(DecimalError::ScaleExceedsWidth { scale, width });
        }
        let digits = decimal_digits(value);
        if digits > width {
            return Err(DecimalError::ValueExceedsWidth { digits, width });
        }
        Ok(Self { width, scale, value })
    }

    pub(crate) fn validate_signed_scale(width: u8, scale: i8) -> std::result::Result<u8, String> {
        let scale = u8::try_from(scale).map_err(|_| format!("negative decimal scale is not supported: {scale}"))?;
        Self::new(width, scale, 0).map_err(|err| err.to_string())?;
        Ok(scale)
    }

    /// Builds a decimal value from trusted DuckDB output.
    ///
    /// The caller must ensure `width`, `scale`, and `value` are a valid DuckDB
    /// decimal triple.
    pub(crate) fn from_chunk(width: u8, scale: u8, value: i128) -> Self {
        debug_assert!(
            Self::new(width, scale, value).is_ok(),
            "DuckDB produced an invalid decimal triple"
        );
        Self { width, scale, value }
    }

    /// Returns the declared precision for DuckDB `DECIMAL` values.
    ///
    /// Values converted from non-decimal sources infer the smallest width that
    /// can represent the scaled payload.
    #[inline]
    pub const fn width(self) -> u8 {
        self.width
    }

    /// Returns the number of fractional digits.
    #[inline]
    pub const fn scale(self) -> u8 {
        self.scale
    }

    /// Returns the scaled integer payload.
    #[inline]
    pub const fn value(self) -> i128 {
        self.value
    }
}

/// Error returned when constructing an invalid [`Decimal`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecimalError {
    /// The width is outside DuckDB's supported range.
    InvalidWidth {
        /// The requested width.
        width: u8,
        /// DuckDB's maximum decimal width.
        max: u8,
    },
    /// The scale is larger than the width.
    ScaleExceedsWidth {
        /// The requested scale.
        scale: u8,
        /// The requested width.
        width: u8,
    },
    /// The scaled integer payload has more digits than the width allows.
    ValueExceedsWidth {
        /// Number of digits in the payload.
        digits: u8,
        /// The requested width.
        width: u8,
    },
}

impl fmt::Display for DecimalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Self::InvalidWidth { width, max } => {
                write!(f, "decimal width {width} is outside DuckDB's 1..={max} range")
            }
            Self::ScaleExceedsWidth { scale, width } => {
                write!(f, "decimal scale {scale} exceeds width {width}")
            }
            Self::ValueExceedsWidth { digits, width } => {
                write!(f, "decimal value has {digits} digits, exceeding width {width}")
            }
        }
    }
}

impl error::Error for DecimalError {}

impl From<DecimalError> for crate::Error {
    fn from(err: DecimalError) -> Self {
        Self::ToSqlConversionFailure(err.into())
    }
}

pub(crate) fn to_duckdb_decimal(decimal: Decimal) -> ffi::duckdb_decimal {
    ffi::duckdb_decimal {
        width: decimal.width,
        scale: decimal.scale,
        value: to_duckdb_hugeint(decimal.value),
    }
}

fn decimal_digits_unsigned(value: u128) -> u8 {
    value.checked_ilog10().map_or(1, |digits| digits as u8 + 1)
}

fn decimal_digits(value: i128) -> u8 {
    decimal_digits_unsigned(value.unsigned_abs())
}

fn integer_decimal(value: i128) -> FromSqlResult<Decimal> {
    let digits = decimal_digits(value);
    if digits > Decimal::MAX_WIDTH {
        return Err(FromSqlError::OutOfRange(value));
    }
    Ok(Decimal::from_chunk(digits, 0, value))
}

fn unsigned_integer_decimal(value: u128) -> FromSqlResult<Decimal> {
    let digits = decimal_digits_unsigned(value);
    if digits > Decimal::MAX_WIDTH {
        return Err(FromSqlError::OutOfRangeUnsigned(value));
    }
    Ok(Decimal::from_chunk(digits, 0, value as i128))
}

impl ToSql for Decimal {
    fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::Owned(Value::Decimal(*self)))
    }
}

impl FromSql for Decimal {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        match value {
            ValueRef::Decimal(decimal) => Ok(decimal),
            ValueRef::TinyInt(i) => integer_decimal(i as i128),
            ValueRef::SmallInt(i) => integer_decimal(i as i128),
            ValueRef::Int(i) => integer_decimal(i as i128),
            ValueRef::BigInt(i) => integer_decimal(i as i128),
            ValueRef::HugeInt(i) => integer_decimal(i),
            ValueRef::UHugeInt(i) => unsigned_integer_decimal(i),
            ValueRef::UTinyInt(i) => integer_decimal(i as i128),
            ValueRef::USmallInt(i) => integer_decimal(i as i128),
            ValueRef::UInt(i) => integer_decimal(i as i128),
            ValueRef::UBigInt(i) => integer_decimal(i as i128),
            ValueRef::Timestamp(_, i) => integer_decimal(i as i128),
            ValueRef::Date32(i) => integer_decimal(i as i128),
            ValueRef::Time64(TimeUnit::Microsecond, i) => integer_decimal(i as i128),
            _ => Err(FromSqlError::InvalidType),
        }
    }
}

#[cfg(feature = "rust_decimal")]
impl From<rust_decimal::Decimal> for Decimal {
    fn from(decimal: rust_decimal::Decimal) -> Self {
        let scale = decimal.scale() as u8;
        let mantissa = decimal.mantissa();
        let width = decimal_digits(mantissa).max(scale);

        // rust_decimal's scale is <= 28 and its mantissa fits in 96 bits, so
        // every valid rust_decimal value fits DuckDB's DECIMAL(38, scale).
        Self::new(width, scale, mantissa).expect("rust_decimal values fit DuckDB DECIMAL")
    }
}

#[cfg(feature = "rust_decimal")]
impl TryFrom<Decimal> for rust_decimal::Decimal {
    type Error = rust_decimal::Error;

    fn try_from(decimal: Decimal) -> std::result::Result<Self, Self::Error> {
        Self::try_from_i128_with_scale(decimal.value(), u32::from(decimal.scale()))
    }
}

#[cfg(feature = "rust_decimal")]
impl ToSql for rust_decimal::Decimal {
    fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::Owned(Value::Decimal((*self).into())))
    }
}

#[cfg(feature = "rust_decimal")]
fn invalid_decimal_float(kind: &str, value: impl std::fmt::Debug) -> FromSqlError {
    FromSqlError::Other(Box::new(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        format!("{kind} value {value:?} cannot be represented as Decimal"),
    )))
}

#[cfg(feature = "rust_decimal")]
fn decimal_from_f32(value: f32) -> FromSqlResult<rust_decimal::Decimal> {
    rust_decimal::Decimal::from_f32(value).ok_or_else(|| invalid_decimal_float("FLOAT", value))
}

#[cfg(feature = "rust_decimal")]
fn decimal_from_f64(value: f64) -> FromSqlResult<rust_decimal::Decimal> {
    rust_decimal::Decimal::from_f64(value).ok_or_else(|| invalid_decimal_float("DOUBLE", value))
}

#[cfg(feature = "rust_decimal")]
impl FromSql for rust_decimal::Decimal {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        match value {
            ValueRef::Decimal(decimal) => decimal.try_into().map_err(|err| FromSqlError::Other(Box::new(err))),
            ValueRef::TinyInt(i) => rust_decimal::Decimal::from_i8(i).ok_or(FromSqlError::OutOfRange(i as i128)),
            ValueRef::SmallInt(i) => rust_decimal::Decimal::from_i16(i).ok_or(FromSqlError::OutOfRange(i as i128)),
            ValueRef::Int(i) => rust_decimal::Decimal::from_i32(i).ok_or(FromSqlError::OutOfRange(i as i128)),
            ValueRef::BigInt(i) => rust_decimal::Decimal::from_i64(i).ok_or(FromSqlError::OutOfRange(i as i128)),
            ValueRef::HugeInt(i) => rust_decimal::Decimal::from_i128(i).ok_or(FromSqlError::OutOfRange(i)),
            ValueRef::UHugeInt(i) => rust_decimal::Decimal::from_u128(i).ok_or(FromSqlError::OutOfRangeUnsigned(i)),
            ValueRef::UTinyInt(i) => rust_decimal::Decimal::from_u8(i).ok_or(FromSqlError::OutOfRange(i as i128)),
            ValueRef::USmallInt(i) => rust_decimal::Decimal::from_u16(i).ok_or(FromSqlError::OutOfRange(i as i128)),
            ValueRef::UInt(i) => rust_decimal::Decimal::from_u32(i).ok_or(FromSqlError::OutOfRange(i as i128)),
            ValueRef::UBigInt(i) => rust_decimal::Decimal::from_u64(i).ok_or(FromSqlError::OutOfRange(i as i128)),
            ValueRef::Float(f) => decimal_from_f32(f),
            ValueRef::Double(d) => decimal_from_f64(d),
            ValueRef::Timestamp(_, i) => rust_decimal::Decimal::from_i64(i).ok_or(FromSqlError::OutOfRange(i as i128)),
            ValueRef::Date32(i) => rust_decimal::Decimal::from_i32(i).ok_or(FromSqlError::OutOfRange(i as i128)),
            ValueRef::Time64(TimeUnit::Microsecond, i) => {
                rust_decimal::Decimal::from_i64(i).ok_or(FromSqlError::OutOfRange(i as i128))
            }
            ValueRef::Text(_) => {
                let s = value.as_str()?;
                match s.parse::<rust_decimal::Decimal>() {
                    Ok(decimal) => Ok(decimal),
                    Err(_) => match s.parse::<i128>() {
                        Ok(i) => Err(FromSqlError::OutOfRange(i)),
                        Err(_) => Err(FromSqlError::InvalidType),
                    },
                }
            }
            _ => Err(FromSqlError::InvalidType),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{Connection, Error, Result};

    #[test]
    fn decimal_new_accepts_valid_values() -> Result<()> {
        let decimal = Decimal::new(9, 2, 12345)?;

        assert_eq!(decimal.width(), 9);
        assert_eq!(decimal.scale(), 2);
        assert_eq!(decimal.value(), 12345);

        Ok(())
    }

    #[test]
    fn decimal_identity_ignores_width() {
        use std::collections::HashSet;

        let declared = Decimal::new(38, 0, 3).unwrap();
        let inferred = Decimal::new(1, 0, 3).unwrap();

        assert_eq!(declared.width(), 38);
        assert_eq!(inferred.width(), 1);
        assert_eq!(declared, inferred);
        assert_eq!(Value::Decimal(declared), Value::Decimal(inferred));

        let mut values = HashSet::new();
        values.insert(declared);
        values.insert(inferred);

        assert_eq!(values.len(), 1);
    }

    #[test]
    fn decimal_identity_keeps_scale() {
        use std::collections::HashSet;

        let integer = Decimal::new(2, 0, 10).unwrap();
        let fractional = Decimal::new(2, 1, 10).unwrap();

        assert_ne!(integer, fractional);

        let mut values = HashSet::new();
        values.insert(integer);
        values.insert(fractional);

        assert_eq!(values.len(), 2);
    }

    #[test]
    fn decimal_display_renders_scaled_value() {
        assert_eq!(Decimal::new(9, 2, 12345).unwrap().to_string(), "123.45");
        assert_eq!(Decimal::new(3, 3, 1).unwrap().to_string(), "0.001");
        assert_eq!(Decimal::new(4, 3, -1).unwrap().to_string(), "-0.001");
        assert_eq!(Decimal::new(3, 0, -123).unwrap().to_string(), "-123");
        assert_eq!(Decimal::new(5, 2, -12345).unwrap().to_string(), "-123.45");
        assert_eq!(Decimal::new(2, 2, 99).unwrap().to_string(), "0.99");
        assert_eq!(Decimal::new(1, 0, 0).unwrap().to_string(), "0");
        assert_eq!(format!("{:>10}", Decimal::new(9, 2, 12345).unwrap()), "    123.45");
        assert_eq!(format!("{:+}", Decimal::new(9, 2, 12345).unwrap()), "+123.45");
        assert_eq!(format!("{:08}", Decimal::new(9, 2, 12345).unwrap()), "00123.45");
    }

    #[test]
    fn decimal_new_rejects_invalid_values() {
        assert!(Decimal::new(3, 3, 1).is_ok());
        assert_eq!(
            Decimal::new(0, 0, 0).unwrap_err(),
            DecimalError::InvalidWidth {
                width: 0,
                max: Decimal::MAX_WIDTH,
            }
        );
        assert_eq!(
            Decimal::new(39, 0, 0).unwrap_err(),
            DecimalError::InvalidWidth {
                width: 39,
                max: Decimal::MAX_WIDTH,
            }
        );
        assert_eq!(
            Decimal::new(9, 10, 0).unwrap_err(),
            DecimalError::ScaleExceedsWidth { scale: 10, width: 9 }
        );
        assert_eq!(
            Decimal::new(2, 1, 100).unwrap_err(),
            DecimalError::ValueExceedsWidth { digits: 3, width: 2 }
        );
        assert_eq!(
            Decimal::new(38, 0, i128::MAX).unwrap_err(),
            DecimalError::ValueExceedsWidth { digits: 39, width: 38 }
        );
    }

    #[test]
    fn decimal_signed_scale_helper_validates_arrow_metadata() {
        assert_eq!(Decimal::validate_signed_scale(9, 2).unwrap(), 2);
        assert_eq!(
            Decimal::validate_signed_scale(9, -1).unwrap_err(),
            "negative decimal scale is not supported: -1"
        );
        assert_eq!(
            Decimal::validate_signed_scale(9, 10).unwrap_err(),
            "decimal scale 10 exceeds width 9"
        );
    }

    #[test]
    fn test_to_duckdb_decimal_large_negative_upper_bits() -> Result<()> {
        let decimal = Decimal::new(28, 10, -7922816251426433759354395033_i128)?;
        let duckdb_decimal = to_duckdb_decimal(decimal);

        assert_eq!(duckdb_decimal.width, 28);
        assert_eq!(duckdb_decimal.scale, 10);
        assert_eq!(duckdb_decimal.value.lower, 7_378_697_629_483_820_647);
        assert_eq!(duckdb_decimal.value.upper, -429_496_730);
        Ok(())
    }

    #[test]
    fn test_to_duckdb_decimal_zero_and_boundaries() -> Result<()> {
        let zero = to_duckdb_decimal(Decimal::new(1, 0, 0)?);
        assert_eq!(zero.width, 1);
        assert_eq!(zero.scale, 0);
        assert_eq!(zero.value.lower, 0);
        assert_eq!(zero.value.upper, 0);

        let max = to_duckdb_decimal(Decimal::new(
            38,
            0,
            99_999_999_999_999_999_999_999_999_999_999_999_999_i128,
        )?);
        assert_eq!(max.width, 38);
        assert_eq!(max.scale, 0);
        assert_eq!(max.value.lower, 687_399_551_400_673_279);
        assert_eq!(max.value.upper, 5_421_010_862_427_522_170);
        Ok(())
    }

    #[test]
    fn test_from_sql_uhugeint_overflow_is_out_of_range() {
        let err = Decimal::column_result(ValueRef::UHugeInt(u128::MAX)).unwrap_err();
        match err {
            FromSqlError::OutOfRangeUnsigned(value) => assert_eq!(value, u128::MAX),
            _ => panic!("expected OutOfRangeUnsigned, got {err}"),
        }
    }

    #[test]
    fn test_from_sql_integer_decimal_overflow_is_out_of_range() {
        let err = Decimal::column_result(ValueRef::HugeInt(i128::MAX)).unwrap_err();
        match err {
            FromSqlError::OutOfRange(value) => assert_eq!(value, i128::MAX),
            _ => panic!("expected OutOfRange, got {err}"),
        }

        let value = 10_u128.pow(38);
        let err = Decimal::column_result(ValueRef::UHugeInt(value)).unwrap_err();
        match err {
            FromSqlError::OutOfRangeUnsigned(err_value) => assert_eq!(err_value, value),
            _ => panic!("expected OutOfRangeUnsigned, got {err}"),
        }
    }

    #[test]
    fn test_from_sql_time_values_as_decimal() -> Result<()> {
        assert_eq!(
            Decimal::column_result(ValueRef::Timestamp(TimeUnit::Second, -123))?,
            Decimal::new(3, 0, -123)?
        );
        assert_eq!(Decimal::column_result(ValueRef::Date32(42))?, Decimal::new(2, 0, 42)?);
        assert_eq!(
            Decimal::column_result(ValueRef::Time64(TimeUnit::Microsecond, 1_234_567))?,
            Decimal::new(7, 0, 1_234_567)?
        );
        Ok(())
    }

    #[test]
    fn test_native_decimal_rejects_float_double_and_text() {
        assert_eq!(
            Decimal::column_result(ValueRef::Float(1.25)),
            Err(FromSqlError::InvalidType)
        );
        assert_eq!(
            Decimal::column_result(ValueRef::Double(1.25)),
            Err(FromSqlError::InvalidType)
        );
        assert_eq!(
            Decimal::column_result(ValueRef::Text(b"123.456")),
            Err(FromSqlError::InvalidType)
        );
    }

    #[test]
    fn test_read_uhugeint_as_decimal() -> Result<()> {
        const U128_MAX_SQL: &str = "340282366920938463463374607431768211455";

        let db = Connection::open_in_memory()?;

        let value: Decimal = db.query_row("SELECT (5)::UHUGEINT", [], |row| row.get(0))?;
        assert_eq!(value, Decimal::new(1, 0, 5)?);

        let err = db
            .query_row(&format!("SELECT ({U128_MAX_SQL})::UHUGEINT"), [], |row| {
                row.get::<_, Decimal>(0)
            })
            .unwrap_err();

        match err {
            Error::UnsignedIntegralValueOutOfRange(0, value) => assert_eq!(value, u128::MAX),
            other => panic!("expected unsigned out-of-range error, got {other:?}"),
        }

        Ok(())
    }

    #[cfg(feature = "rust_decimal")]
    #[test]
    fn test_rust_decimal_converts_to_native_decimal() {
        let decimal = rust_decimal::Decimal::from_i128_with_scale(12345, 2);
        let native = Decimal::from(decimal);

        assert_eq!(native, Decimal::new(5, 2, 12345).unwrap());

        let decimal = rust_decimal::Decimal::from_i128_with_scale(1, 3);
        let native = Decimal::from(decimal);

        assert_eq!(native, Decimal::new(3, 3, 1).unwrap());
    }

    #[cfg(feature = "rust_decimal")]
    #[test]
    fn test_rust_decimal_round_trips_through_decimal_column() -> Result<()> {
        let db = Connection::open_in_memory()?;
        let value = rust_decimal::Decimal::from_i128_with_scale(12345, 2);

        let read: rust_decimal::Decimal = db.query_row("SELECT ?::DECIMAL(5,2)", [value], |row| row.get(0))?;

        assert_eq!(read, value);
        Ok(())
    }

    #[cfg(feature = "rust_decimal")]
    #[test]
    fn test_rust_decimal_reads_representable_wide_duckdb_decimal() -> Result<()> {
        let db = Connection::open_in_memory()?;

        let read: rust_decimal::Decimal = db.query_row("SELECT 1.23::DECIMAL(38,2)", [], |row| row.get(0))?;

        assert_eq!(read, rust_decimal::Decimal::from_i128_with_scale(123, 2));
        Ok(())
    }

    #[cfg(feature = "rust_decimal")]
    #[test]
    fn test_rust_decimal_compat_reads_float_double_and_text() {
        let float = rust_decimal::Decimal::column_result(ValueRef::Float(1.25)).unwrap();
        assert_eq!(float, rust_decimal::Decimal::from_f32(1.25).unwrap());

        let double = rust_decimal::Decimal::column_result(ValueRef::Double(1.25)).unwrap();
        assert_eq!(double, rust_decimal::Decimal::from_f64(1.25).unwrap());

        let text = rust_decimal::Decimal::column_result(ValueRef::Text(b"123.456")).unwrap();
        assert_eq!(text, "123.456".parse::<rust_decimal::Decimal>().unwrap());
    }

    #[cfg(feature = "rust_decimal")]
    #[test]
    fn test_rust_decimal_errors_on_oversized_duckdb_decimal() -> Result<()> {
        let db = Connection::open_in_memory()?;

        let err = db
            .query_row("SELECT 123456789012345678901234567890.12::DECIMAL(38,2)", [], |row| {
                row.get::<_, rust_decimal::Decimal>(0)
            })
            .unwrap_err();

        match err {
            Error::FromSqlConversionFailure(0, crate::types::Type::Decimal, _) => {}
            other => panic!("expected Decimal conversion failure, got {other:?}"),
        }

        Ok(())
    }

    #[cfg(feature = "rust_decimal")]
    #[test]
    fn test_rust_decimal_unrepresentable_float_errors_are_descriptive() {
        let err = rust_decimal::Decimal::column_result(ValueRef::Float(f32::NAN)).unwrap_err();
        assert_eq!(err.to_string(), "FLOAT value NaN cannot be represented as Decimal");

        let err = rust_decimal::Decimal::column_result(ValueRef::Double(f64::INFINITY)).unwrap_err();
        assert_eq!(err.to_string(), "DOUBLE value inf cannot be represented as Decimal");
    }
}

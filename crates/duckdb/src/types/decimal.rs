use rust_decimal::prelude::FromPrimitive as _;

use super::TimeUnit;
use crate::types::{FromSql, FromSqlError, FromSqlResult, ToSqlOutput, Value, ValueRef};
use crate::{Result, ToSql, ffi};

/// Convert a rust_decimal::Decimal to a ffi::duckdb_decimal
pub(crate) fn to_duckdb_decimal(d: rust_decimal::Decimal) -> ffi::duckdb_decimal {
    // The max value of rust_decimal's scale is 28.
    let d_scale = d.scale() as u8;
    let d_width = decimal_width(d).max(d_scale);
    let mantissa = d.mantissa();
    let d_value = ffi::duckdb_hugeint {
        lower: mantissa as u64,
        upper: (mantissa >> 64) as i64,
    };

    ffi::duckdb_decimal {
        width: d_width,
        scale: d_scale,
        value: d_value,
    }
}

/// Get the length of the decimal significant digits of a rust_decimal::Decimal
fn decimal_width(d: rust_decimal::Decimal) -> u8 {
    let abs = d.mantissa().unsigned_abs();
    if abs == 0 { 1 } else { abs.ilog10() as u8 + 1 }
}

fn invalid_decimal_float(kind: &str, value: impl std::fmt::Debug) -> FromSqlError {
    FromSqlError::Other(Box::new(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        format!("{kind} value {value:?} cannot be represented as Decimal"),
    )))
}

fn decimal_from_f32(value: f32) -> FromSqlResult<rust_decimal::Decimal> {
    rust_decimal::Decimal::from_f32(value).ok_or_else(|| invalid_decimal_float("FLOAT", value))
}

fn decimal_from_f64(value: f64) -> FromSqlResult<rust_decimal::Decimal> {
    rust_decimal::Decimal::from_f64(value).ok_or_else(|| invalid_decimal_float("DOUBLE", value))
}

impl ToSql for rust_decimal::Decimal {
    fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::Owned(Value::Decimal(*self)))
    }
}

impl FromSql for rust_decimal::Decimal {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        match value {
            ValueRef::TinyInt(i) => rust_decimal::Decimal::from_i8(i).ok_or(FromSqlError::OutOfRange(i as i128)),
            ValueRef::SmallInt(i) => rust_decimal::Decimal::from_i16(i).ok_or(FromSqlError::OutOfRange(i as i128)),
            ValueRef::Int(i) => rust_decimal::Decimal::from_i32(i).ok_or(FromSqlError::OutOfRange(i as i128)),
            ValueRef::BigInt(i) => rust_decimal::Decimal::from_i64(i).ok_or(FromSqlError::OutOfRange(i as i128)),
            ValueRef::HugeInt(i) => rust_decimal::Decimal::from_i128(i).ok_or(FromSqlError::OutOfRange(i)),
            ValueRef::UTinyInt(i) => rust_decimal::Decimal::from_u8(i).ok_or(FromSqlError::OutOfRange(i as i128)),
            ValueRef::USmallInt(i) => rust_decimal::Decimal::from_u16(i).ok_or(FromSqlError::OutOfRange(i as i128)),
            ValueRef::UInt(i) => rust_decimal::Decimal::from_u32(i).ok_or(FromSqlError::OutOfRange(i as i128)),
            ValueRef::UBigInt(i) => rust_decimal::Decimal::from_u64(i).ok_or(FromSqlError::OutOfRange(i as i128)),
            ValueRef::Float(f) => decimal_from_f32(f),
            ValueRef::Double(d) => decimal_from_f64(d),
            ValueRef::Decimal(decimal) => Ok(decimal),
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
    use rust_decimal::Decimal;

    use super::*;

    #[test]
    fn test_to_duckdb_decimal_large_negative_upper_bits() {
        let decimal = Decimal::from_i128_with_scale(-7922816251426433759354395033_i128, 10);
        let duck_decimal = to_duckdb_decimal(decimal);

        assert_eq!(duck_decimal.width, 28);
        assert_eq!(duck_decimal.scale, 10);
        assert_eq!(duck_decimal.value.lower, 7_378_697_629_483_820_647);
        assert_eq!(duck_decimal.value.upper, -429_496_730);
    }

    #[test]
    fn test_to_duckdb_decimal_zero_and_max_boundaries() {
        let zero = to_duckdb_decimal(Decimal::ZERO);
        assert_eq!(zero.width, 1);
        assert_eq!(zero.scale, 0);
        assert_eq!(zero.value.lower, 0);
        assert_eq!(zero.value.upper, 0);

        let max = to_duckdb_decimal(Decimal::MAX);
        assert_eq!(max.width, 29);
        assert_eq!(max.scale, 0);
        assert_eq!(max.value.lower, u64::MAX);
        assert_eq!(max.value.upper, 4_294_967_295);
    }

    #[test]
    fn test_from_sql_hugeint_overflow_is_out_of_range() {
        let err = Decimal::column_result(ValueRef::HugeInt(i128::MAX)).unwrap_err();
        match err {
            FromSqlError::OutOfRange(value) => assert_eq!(value, i128::MAX),
            _ => panic!("expected OutOfRange, got {err}"),
        }
    }

    #[test]
    fn test_from_sql_unrepresentable_float_errors_are_descriptive() {
        let err = Decimal::column_result(ValueRef::Float(f32::INFINITY)).unwrap_err();
        match err {
            FromSqlError::Other(err) => {
                assert_eq!(err.to_string(), "FLOAT value inf cannot be represented as Decimal");
            }
            _ => panic!("expected Other, got {err}"),
        }

        let err = Decimal::column_result(ValueRef::Double(1.5e30)).unwrap_err();
        match err {
            FromSqlError::Other(err) => {
                assert_eq!(err.to_string(), "DOUBLE value 1.5e30 cannot be represented as Decimal");
            }
            _ => panic!("expected Other, got {err}"),
        }
    }
}

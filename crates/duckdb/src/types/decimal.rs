use rust_decimal::prelude::FromPrimitive as _;

use super::TimeUnit;
use crate::ffi;
use crate::types::{FromSql, FromSqlError, FromSqlResult, Value, ValueRef};
use crate::Result;
use crate::{types::ToSqlOutput, ToSql};

/// Convert a rust_decimal::Decimal to a ffi::duckdb_decimal
pub fn to_duckdb_decimal(d: rust_decimal::Decimal) -> ffi::duckdb_decimal {
    // The max size of rust_decimal's scale is 28.
    let d_scale = d.scale() as u8;
    let d_width = decimal_width(d);
    let d_value = {
        let mantissa = d.mantissa();
        let lo = mantissa as u64;
        let hi = (mantissa >> 64) as i64;
        ffi::duckdb_hugeint { lower: lo, upper: hi }
    };

    ffi::duckdb_decimal {
        width: d_width,
        scale: d_scale,
        value: d_value,
    }
}

/// Get the length of the decimal significant digits of a rust_decimal::Decimal
fn decimal_width(d: rust_decimal::Decimal) -> u8 {
    let mut num = d.mantissa();

    if num == 0 {
        return 1;
    }

    let mut len = 0;
    num = num.abs();

    while num > 0 {
        len += 1;
        num /= 10;
    }

    len
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
            ValueRef::Float(f) => rust_decimal::Decimal::from_f32(f).ok_or(FromSqlError::OutOfRange(f as i128)),
            ValueRef::Double(d) => rust_decimal::Decimal::from_f64(d).ok_or(FromSqlError::OutOfRange(d as i128)),
            ValueRef::Decimal(decimal) => Ok(decimal),
            ValueRef::Timestamp(_, i) => rust_decimal::Decimal::from_i64(i).ok_or(FromSqlError::OutOfRange(i as i128)),
            ValueRef::Date32(i) => rust_decimal::Decimal::from_i32(i).ok_or(FromSqlError::OutOfRange(i as i128)),
            ValueRef::Time64(TimeUnit::Microsecond, i) => {
                rust_decimal::Decimal::from_i64(i).ok_or(FromSqlError::OutOfRange(i as i128))
            }
            ValueRef::Text(_) => {
                let s = value.as_str()?;
                s.parse::<rust_decimal::Decimal>().or_else(|_| {
                    s.parse::<i128>()
                        .map_err(|_| FromSqlError::InvalidType)
                        .and_then(|i| Err(FromSqlError::OutOfRange(i as i128)))
                })
            }
            _ => Err(FromSqlError::InvalidType),
        }
    }
}

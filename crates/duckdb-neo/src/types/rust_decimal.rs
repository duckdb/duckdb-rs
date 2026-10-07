use super::{DecimalSignature, DecimalValue, MAX_DECIMAL_WIDTH};
use crate::{
    Result,
    connection::FFILink,
    error::Error,
    logical_type::{LogicalType, LogicalTypeID},
    types::{FromValue, ToValue},
    value::Value,
    vector::{Unknown, Vector, VectorElement, WritableVectorElement},
};

/// Widest `DECIMAL` whose values all fit `rust_decimal`'s 96-bit mantissa.
const MAX_RUST_DECIMAL_WIDTH: u8 = 28;

/// Uses the widest `DECIMAL`, which holds any `rust_decimal::Decimal`.
impl From<rust_decimal::Decimal> for DecimalValue {
    fn from(value: rust_decimal::Decimal) -> Self {
        Self {
            value: value.mantissa(),
            width: MAX_DECIMAL_WIDTH,
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

impl ToValue for rust_decimal::Decimal {
    fn value<C: FFILink + ?Sized>(&self, link: &C) -> Result<Value> {
        DecimalValue::from(*self).value(link)
    }
}

impl FromValue for rust_decimal::Decimal {
    fn _get_inner(value: &Value) -> Result<Self> {
        let decimal = DecimalValue::_get_inner(value)?;
        rust_decimal::Decimal::try_from(decimal).map_err(|err| Error::invalid_input(format!("{decimal:?}: {err}")))
    }
}

fn signature<U: VectorElement>(vector: &Vector<'_, U>) -> DecimalSignature {
    vector
        .decimal_signature()
        .expect("validated DECIMAL vector has a signature")
}

/// Reads and writes `DECIMAL` columns of width up to 28, which always fit.
impl VectorElement for rust_decimal::Decimal {
    const TYPE_ID: LogicalTypeID = LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_DECIMAL;

    // The storage integer depends on the column's width.
    type Internal = ();

    type Ref<'a> = rust_decimal::Decimal;

    fn validate(other: &LogicalType, _children: &[Vector<'_, Unknown>]) -> Result<bool> {
        if other.type_id() != Self::TYPE_ID {
            return Ok(false);
        }
        Ok(DecimalSignature::from_logical_type(other)?.width <= MAX_RUST_DECIMAL_WIDTH)
    }

    fn get<'a, U: VectorElement>(vector: &'a Vector<'_, U>, physical: usize, _logical: usize) -> Self::Ref<'a>
    where
        Self: 'a,
    {
        let DecimalSignature { width, scale } = signature(vector);
        let data = vector.view.as_ref().unwrap().as_ptr() as *const u8;
        let value = unsafe {
            match width {
                1..=4 => i128::from(*(data as *const i16).add(physical)),
                5..=9 => i128::from(*(data as *const i32).add(physical)),
                10..=18 => i128::from(*(data as *const i64).add(physical)),
                _ => *(data as *const i128).add(physical),
            }
        };
        rust_decimal::Decimal::from_i128_with_scale(value, u32::from(scale))
    }
}

/// Rescales to the column's scale; fails instead of rounding or overflowing.
impl WritableVectorElement for rust_decimal::Decimal {
    type Write<'a> = rust_decimal::Decimal;

    fn write(vector: &mut Vector<'_, Self>, index: usize, value: Option<Self::Write<'_>>) -> Result<()> {
        let Some(value) = value else {
            return vector.write_raw::<()>(index, None);
        };
        let DecimalSignature { width, scale } = signature(vector);
        let from_scale = value.scale();
        let to_scale = u32::from(scale);
        let scaled = if from_scale <= to_scale {
            10i128
                .checked_pow(to_scale - from_scale)
                .and_then(|factor| value.mantissa().checked_mul(factor))
        } else {
            let factor = 10i128.pow(from_scale - to_scale);
            (value.mantissa() % factor == 0).then(|| value.mantissa() / factor)
        };
        let decimal = DecimalValue {
            value: scaled
                .ok_or_else(|| Error::invalid_input(format!("{value} does not fit DECIMAL({width}, {scale})")))?,
            width,
            scale,
        };
        decimal.check()?;
        match width {
            1..=4 => vector.write_raw(index, decimal.to_primitive::<i16>()),
            5..=9 => vector.write_raw(index, decimal.to_primitive::<i32>()),
            10..=18 => vector.write_raw(index, decimal.to_primitive::<i64>()),
            _ => vector.write_raw(index, Some(decimal.value)),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;
    use crate::{
        Parameters,
        data_chunk::DataChunk,
        environment::{Environment, StorageLocation},
        types::DuckDBType,
    };

    fn dec(text: &str) -> rust_decimal::Decimal {
        rust_decimal::Decimal::from_str(text).unwrap()
    }

    #[test]
    fn rust_decimal_value_round_trips() -> Result<()> {
        let env = Environment::new()?;
        let db = env.open(StorageLocation::InMemory)?;
        let conn = db.connect()?;

        for text in [
            "123.45",
            "-0.001",
            "0",
            "79228162514264337593543950335",
            "0.0000000000000000000000000001",
        ] {
            let decimal = dec(text);
            let raw = DecimalValue::from(decimal);
            assert_eq!(raw.width, 38);
            assert_eq!(rust_decimal::Decimal::try_from(raw), Ok(decimal));

            let value = decimal.value(&conn)?;
            assert_eq!(value.get::<rust_decimal::Decimal>()?, Some(decimal));

            let mut result = conn.query("SELECT $1::VARCHAR", Parameters::positional(&[&decimal]))?;
            let chunk = result.next().expect("query returned no rows")?;
            let text_vector = chunk.get_vector_at::<String>(0)?;
            assert_eq!(text_vector.get(0)?.map(|s| s.to_string()), Some(text.to_string()));
        }

        Ok(())
    }

    #[test]
    fn rust_decimal_conversion_reports_overflow() -> Result<()> {
        let too_wide = DecimalValue::new(10i128.pow(37), 38, 0);
        assert!(rust_decimal::Decimal::try_from(too_wide).is_err());

        let too_precise = DecimalValue::new(1i16, 30, 29);
        assert!(rust_decimal::Decimal::try_from(too_precise).is_err());

        let env = Environment::new()?;
        let db = env.open(StorageLocation::InMemory)?;
        let conn = db.connect()?;
        let value = too_wide.value(&conn)?;
        assert!(value.get::<rust_decimal::Decimal>().is_err());

        Ok(())
    }

    #[test]
    fn rust_decimal_vector_reads_every_storage_width() -> Result<()> {
        let env = Environment::new()?;
        let db = env.open(StorageLocation::InMemory)?;
        let conn = db.connect()?;

        let mut result = conn.query(
            "SELECT -1.5::DECIMAL(4, 1), 123.45::DECIMAL(5, 2), 1.5::DECIMAL(18, 1), \
                 -9999999999999999999999999.999::DECIMAL(28, 3), NULL::DECIMAL(9, 2), 1::DECIMAL(29, 0), \
                 MAP {1: 1.21::DECIMAL(4, 2)}",
            Parameters::None,
        )?;
        let chunk = result.next().expect("query returned no rows")?;

        assert_eq!(
            chunk.get_vector_at::<rust_decimal::Decimal>(0)?.get(0)?,
            Some(dec("-1.5"))
        );
        assert_eq!(
            chunk.get_vector_at::<rust_decimal::Decimal>(1)?.get(0)?,
            Some(dec("123.45"))
        );
        assert_eq!(
            chunk.get_vector_at::<rust_decimal::Decimal>(2)?.get(0)?,
            Some(dec("1.5"))
        );
        assert_eq!(
            chunk.get_vector_at::<rust_decimal::Decimal>(3)?.get(0)?,
            Some(dec("-9999999999999999999999999.999"))
        );
        assert_eq!(chunk.get_vector_at::<rust_decimal::Decimal>(4)?.get(0)?, None);
        assert!(chunk.get_vector_at::<rust_decimal::Decimal>(5).is_err());

        let map = chunk.get_vector_at::<crate::types::Map<i32, rust_decimal::Decimal>>(6)?;
        assert_eq!(map.get(0)?.unwrap().get(&1)?, Some(dec("1.21")));

        Ok(())
    }

    #[test]
    fn rust_decimal_vector_writes_rescale() -> Result<()> {
        let env = Environment::new()?;
        let db = env.open(StorageLocation::InMemory)?;
        let conn = db.connect()?;

        for width in [4, 9, 18, 28] {
            let logical_type = DecimalSignature { width, scale: 2 }.logical_type(&conn)?;
            let chunk = DataChunk::create(&[logical_type], true)?;
            let mut vector = chunk.get_vector_at::<rust_decimal::Decimal>(0)?;
            vector.set_size(4)?;
            vector.write(0, Some(dec("1.5")))?;
            vector.write(1, Some(dec("-12.300")))?;
            vector.write(2, None)?;
            vector.write(3, Some(dec("99.99")))?;
            assert_eq!(
                vector.iter()?.collect::<Vec<_>>(),
                vec![Some(dec("1.50")), Some(dec("-12.30")), None, Some(dec("99.99"))]
            );

            assert!(vector.write(0, Some(dec("1.234"))).is_err());
            let too_big = 10i128.pow(u32::from(width) - 2);
            assert!(
                vector
                    .write(0, Some(rust_decimal::Decimal::from_i128_with_scale(too_big, 0)))
                    .is_err()
            );
        }

        let chunk = DataChunk::create(&[crate::types::Decimal::<38, 2>::logical_type(&conn)?], true)?;
        assert!(chunk.get_vector_at::<rust_decimal::Decimal>(0).is_err());

        Ok(())
    }
}

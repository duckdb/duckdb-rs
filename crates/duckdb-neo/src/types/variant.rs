//! The self-describing `VARIANT` logical type.

use super::{DuckDBType, ToValue};
use crate::{
    Parameters, Result,
    connection::FFILink,
    logical_type::{LogicalType, LogicalTypeID},
    value::Value,
    vector::{Vector, VectorElement, WritableVectorElement},
};

/// [`Vector::get`]/[`Vector::iter`] read each row with one FFI call and an owned [`Value`] allocation.
/// [`Vector::write`] accepts a [`Value`] that DuckDB casts to `VARIANT`.
#[derive(Debug)]
pub struct Variant;

// SAFETY: rows are read only through DuckDB's value API.
unsafe impl VectorElement for Variant {
    const TYPE_ID: LogicalTypeID = LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_VARIANT;

    /// VARIANT has no fixed physical layout; rows use the per-cell value API.
    type Internal = ();

    type Ref<'a> = Value;

    unsafe fn get<'a, U: VectorElement>(vector: &'a Vector<'_, U>, _physical: usize, logical: usize) -> Self::Ref<'a>
    where
        Self: Sized + 'a,
    {
        vector.get_value_slow(logical).unwrap()
    }
}

// SAFETY: rows are written only through DuckDB's value API.
unsafe impl WritableVectorElement for Variant {
    type Write<'a> = Value;

    unsafe fn write(vector: &mut Vector<'_, Self>, index: usize, value: Option<Self::Write<'_>>) -> Result<()> {
        match value {
            Some(value) => vector.write_value_slow(index, value),
            None => vector.set_null_slow(index),
        }
    }
}

/// A value converted to DuckDB's self-describing `VARIANT` type.
#[derive(Debug)]
pub struct VariantValue<T>(pub T);

impl DuckDBType for Variant {
    fn logical_type<C: FFILink + ?Sized>(link: &C) -> Result<LogicalType> {
        link.logical_type_create("VARIANT", Parameters::None)
    }
}

impl<T> DuckDBType for VariantValue<T> {
    fn logical_type<C: FFILink + ?Sized>(link: &C) -> Result<LogicalType> {
        Variant::logical_type(link)
    }
}

impl<T: ToValue> ToValue for VariantValue<T> {
    fn value<C: FFILink + ?Sized>(&self, link: &C) -> Result<Value> {
        let value = self.0.value(link)?;
        link.value_cast(&value, &Self::logical_type(link)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::environment::{Environment, StorageLocation};

    #[test]
    fn variant_marker_has_logical_type() -> Result<()> {
        let env = Environment::new()?;
        let conn = env.open(StorageLocation::InMemory)?.connect()?;
        let logical_type = Variant::logical_type(&conn)?;
        assert_eq!(logical_type.id()?, LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_VARIANT);
        assert_eq!(
            logical_type.to_text()?,
            VariantValue::<i32>::logical_type(&conn)?.to_text()?
        );
        Ok(())
    }
}

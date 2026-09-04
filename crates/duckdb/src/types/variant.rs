//! The self-describing `VARIANT` logical type.

use super::{DuckDBType, ToValue};
use crate::{
    Parameters, Result,
    connection::FFILink,
    logical_type::{LogicalType, LogicalTypeID},
    value::Value,
    vector::{Unknown, Vector, VectorElement, WritableVectorElement},
};

/// Reads a `VARIANT` row as an owned [`Value`].
pub struct Variant;

impl VectorElement for Variant {
    const TYPE_ID: LogicalTypeID = LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_VARIANT;

    /// Variant does not have an Raw representation in the V2 API. Use [`get`] instead.
    type Internal = ();

    type Ref<'a> = Value;

    fn get<'a, U: VectorElement>(vector: &'a Vector<'_, U>, physical: usize, _logical: usize) -> Self::Ref<'a>
    where
        Self: Sized + 'a,
    {
        vector.get_value_slow(physical).unwrap()
    }
}

impl WritableVectorElement for Variant {
    type Write<'a> = Value;

    fn write(vector: &mut Vector<'_, Self>, index: usize, value: Option<Self::Write<'_>>) -> Result<()> {
        match value {
            Some(value) => vector.write_value_slow(index, value),
            None => vector.write_raw::<Unknown>(index, None),
        }
    }
}

/// A value converted to DuckDB's self-describing `VARIANT` type.
pub struct VariantValue<T>(pub T);

impl<T> DuckDBType for VariantValue<T> {
    fn logical_type<C: FFILink + ?Sized>(link: &C) -> Result<LogicalType> {
        link.logical_type_create("VARIANT", Parameters::None)
    }
}

impl<T: ToValue> ToValue for VariantValue<T> {
    fn value<C: FFILink + ?Sized>(&self, link: &C) -> Result<Value> {
        let value = self.0.value(link)?;
        link.value_cast(&value, Self::logical_type(link)?)
    }
}

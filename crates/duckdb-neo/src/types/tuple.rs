//! Anonymous `TUPLE` logical types (heterogeneous fixed-arity sequences).

use super::{DuckDBType, FromValue, ToValue};
use crate::{
    Parameters, Result,
    connection::FFILink,
    error::{DuckDBError, Error},
    logical_type::{LogicalType, LogicalTypeID},
    value::Value,
    value::ValueInput,
};

fn validate_tuple(value: &Value, expected_len: usize) -> Result<()> {
    let logical_type = value.fetch_logical_type()?;
    if logical_type.type_id() != LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_TUPLE {
        return Err(Error {
            code: DuckDBError::DUCKDB_V2_ERROR_INPUT_INVALID,
            message: format!("Expected TUPLE value, found {}", logical_type.to_string()?),
        });
    }
    let actual_len = value.child_count()?;
    if actual_len != expected_len {
        return Err(Error {
            code: DuckDBError::DUCKDB_V2_ERROR_INPUT_INVALID,
            message: format!("TUPLE has {actual_len} fields, expected {expected_len}"),
        });
    }
    Ok(())
}

impl DuckDBType for () {
    fn logical_type<C: FFILink + ?Sized>(link: &C) -> Result<LogicalType> {
        link.logical_type_create("TUPLE", Parameters::None)
    }
}

impl ToValue for () {
    fn value<C: FFILink + ?Sized>(&self, link: &C) -> Result<Value> {
        link.create_value(ValueInput::Tuple(&[]))
    }
}

impl FromValue for () {
    fn _get_inner(value: &Value) -> Result<Self> {
        validate_tuple(value, 0)
    }
}

macro_rules! impl_tuple_value {
    ($(($type:ident, $index:tt)),+ $(,)?) => {
        impl<$($type: DuckDBType),+> DuckDBType for ($($type,)+) {
            fn logical_type<L: FFILink + ?Sized>(link: &L) -> Result<LogicalType> {
                let types = vec![
                    $(Value::from_logical_type(link, &$type::logical_type(link)?)?),+
                ];
                let parameters = types
                    .iter()
                    .map(|value| value as &dyn crate::parameter::QueryParameter)
                    .collect::<Vec<_>>();
                link.logical_type_create("TUPLE", Parameters::positional(&parameters))
            }
        }

        impl<$($type: ToValue),+> ToValue for ($($type,)+) {
            fn value<L: FFILink + ?Sized>(&self, link: &L) -> Result<Value> {
                let children = vec![$(self.$index.value(link)?),+];
                link.create_value(ValueInput::Tuple(&children))
            }
        }

        impl<$($type: FromValue),+> FromValue for ($(Option<$type>,)+) {
            fn _get_inner(value: &Value) -> Result<Self> {
                const FIELD_COUNT: usize = 0 $(+ { let _ = stringify!($type); 1 })+;
                validate_tuple(value, FIELD_COUNT)?;
                Ok(($($type::from_value(&value.get_child($index)?)?,)+))
            }
        }
    };
}

impl_tuple_value!((A, 0));
impl_tuple_value!((A, 0), (B, 1));
impl_tuple_value!((A, 0), (B, 1), (C, 2));
impl_tuple_value!((A, 0), (B, 1), (C, 2), (D, 3));
impl_tuple_value!((A, 0), (B, 1), (C, 2), (D, 3), (E, 4));
impl_tuple_value!((A, 0), (B, 1), (C, 2), (D, 3), (E, 4), (F, 5));
impl_tuple_value!((A, 0), (B, 1), (C, 2), (D, 3), (E, 4), (F, 5), (G, 6));
impl_tuple_value!((A, 0), (B, 1), (C, 2), (D, 3), (E, 4), (F, 5), (G, 6), (H, 7));

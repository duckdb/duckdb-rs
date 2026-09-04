//! Anonymous `TUPLE` logical types (heterogeneous fixed-arity sequences).

use super::{DuckDBType, ToValue};
use crate::{Parameters, Result, connection::FFILink, logical_type::LogicalType, value::Value, value::ValueInput};

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

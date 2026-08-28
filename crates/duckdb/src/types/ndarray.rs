//! Conversions between DuckDB vector (`ARRAY`) columns and [`ndarray`] 1-D
//! arrays.
//!
//! Enable the `ndarray` cargo feature to use these impls. A DuckDB
//! `FLOAT[N]` / `DOUBLE[N]` column maps to [`Array1<f32>`] / [`Array1<f64>`]
//! respectively. Empty arrays cannot be bound because the element type cannot
//! be inferred from the value alone.

use crate::{
    Error, Result,
    types::{FromSql, FromSqlResult, ToSql, ToSqlOutput, Value, ValueRef},
};
use ndarray::Array1;

macro_rules! impl_ndarray_array1 {
    ($t:ty, $variant:ident) => {
        impl ToSql for Array1<$t> {
            fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
                if self.is_empty() {
                    return Err(Error::ToSqlConversionFailure(
                        "cannot bind an empty array; the element type cannot be inferred".into(),
                    ));
                }
                let value = Value::Array(self.iter().map(|&x| Value::$variant(x)).collect());
                Ok(ToSqlOutput::Owned(value))
            }
        }

        impl FromSql for Array1<$t> {
            fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
                Ok(Array1::from_vec(Vec::<$t>::column_result(value)?))
            }
        }
    };
}

impl_ndarray_array1!(f32, Float);
impl_ndarray_array1!(f64, Double);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ToSql;

    #[test]
    fn array1_to_sql_is_owned_vector() {
        let arr = Array1::from_vec(vec![1.0_f32, 2.0, 3.0]);
        let out = arr.to_sql().expect("to_sql succeeds");
        assert!(out.to_sql().is_ok());
    }

    #[test]
    fn empty_array1_to_sql_errors() {
        let arr = Array1::<f32>::zeros(0);
        assert!(arr.to_sql().is_err());
    }
}

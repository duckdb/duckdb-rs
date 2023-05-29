use polars_core::{
    prelude::{DataFrame, Series},
    utils::{
        accumulate_dataframes_vertical_unchecked,
        rayon::prelude::{IndexedParallelIterator, IntoParallelIterator, ParallelIterator},
    },
};

use super::{arrow::datatypes::SchemaRef, Statement};

/// An handle for the resulting Polars DataFrame of a query.
#[must_use = "Polars is lazy and will do nothing unless consumed"]
pub struct Polars<'stmt> {
    pub(crate) stmt: Option<&'stmt Statement<'stmt>>,
}

impl<'stmt> Polars<'stmt> {
    #[inline]
    pub(crate) fn new(stmt: &'stmt Statement<'stmt>) -> Polars<'stmt> {
        Polars { stmt: Some(stmt) }
    }

    /// return arrow schema
    #[inline]
    pub fn get_schema(&self) -> SchemaRef {
        self.stmt.unwrap().stmt.schema()
    }
}

impl<'stmt> Iterator for Polars<'stmt> {
    type Item = DataFrame;

    fn next(&mut self) -> Option<Self::Item> {
        let column_names = self.stmt.unwrap().column_names();
        let arrow_struct_array = self.stmt?.step()?;
        let (_, arrow_arrays, _) = arrow_struct_array.into_parts();
        let series_vec = arrow_arrays
            .into_par_iter()
            .enumerate()
            .map(|(i, arrow_array)| {
                let arrow_array_data = arrow_array.to_data();
                let arrow2_array = arrow2::array::from_data(&arrow_array_data);
                let name = column_names[i].as_str();

                Series::try_from((name, arrow2_array)).unwrap()
            })
            .collect::<Vec<_>>();

        Some(DataFrame::new_no_checks(series_vec))
    }
}

/// Polars DataFrame wrapper
pub struct PolarsDataFrame {
    inner: DataFrame,
}

impl PolarsDataFrame {
    /// Take Polars DataFrame
    pub fn into_inner(self) -> DataFrame {
        self.inner
    }
}

impl FromIterator<DataFrame> for PolarsDataFrame {
    fn from_iter<T: IntoIterator<Item = DataFrame>>(dfs: T) -> Self {
        PolarsDataFrame {
            inner: accumulate_dataframes_vertical_unchecked(dfs),
        }
    }
}

use polars::{
    export::rayon::prelude::{IndexedParallelIterator, IntoParallelIterator, ParallelIterator},
    prelude::DataFrame,
    series::Series,
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

                Series::try_from((name, arrow2_array)).expect("Failed to construct Series from arrow2 array")
            })
            .collect::<Vec<_>>();

        Some(DataFrame::new_no_checks(series_vec))
    }
}

#[cfg(test)]
mod tests {
    use polars::prelude::*;
    use polars_core::utils::accumulate_dataframes_vertical_unchecked;

    use crate::{test::checked_memory_handle, Result};

    #[test]
    fn test_query_polars_small() -> Result<()> {
        let db = checked_memory_handle();
        let sql = "BEGIN TRANSACTION;
                   CREATE TABLE test(t INTEGER);
                   INSERT INTO test VALUES (1); INSERT INTO test VALUES (2); INSERT INTO test VALUES (3); INSERT INTO test VALUES (4); INSERT INTO test VALUES (5);
                   END TRANSACTION;";
        db.execute_batch(sql)?;
        let mut stmt = db.prepare("select t from test order by t desc")?;
        let mut polars = stmt.query_polars([])?;

        let df = polars.next().expect("Failed to get DataFrame");
        assert_eq!(
            df,
            df! (
                "t" => [5i32, 4, 3, 2, 1],
            )
            .expect("Failed to construct DataFrame")
        );
        assert!(polars.next().is_none());

        Ok(())
    }

    #[test]
    fn test_query_polars_large() -> Result<()> {
        let db = checked_memory_handle();
        db.execute_batch("BEGIN TRANSACTION")?;
        db.execute_batch("CREATE TABLE test(t INTEGER);")?;

        for _ in 0..600 {
            db.execute_batch("INSERT INTO test VALUES (1); INSERT INTO test VALUES (2); INSERT INTO test VALUES (3); INSERT INTO test VALUES (4); INSERT INTO test VALUES (5);")?;
        }

        db.execute_batch("END TRANSACTION")?;
        let mut stmt = db.prepare("select t from test order by t")?;
        let pl = stmt.query_polars([])?;

        let df = accumulate_dataframes_vertical_unchecked(pl);
        assert_eq!(df.height(), 3000);
        assert_eq!(df.column("t").unwrap().i32().unwrap().sum().unwrap(), 9000);

        Ok(())
    }
}

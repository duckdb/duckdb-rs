use polars::prelude::DataFrame;

use super::{Statement, arrow::datatypes::SchemaRef};

/// An handle for the resulting Polars DataFrame of a query.
///
/// The iterator panics if DuckDB fails to convert a result chunk to Arrow or
/// if the Arrow chunk cannot be converted into a Polars DataFrame.
#[must_use = "Polars is lazy and will do nothing unless consumed"]
pub struct Polars<'stmt> {
    pub(crate) stmt: Option<&'stmt Statement<'stmt>>,
}

impl<'stmt> Polars<'stmt> {
    #[inline]
    pub(crate) fn new(stmt: &'stmt Statement<'stmt>) -> Self {
        Polars { stmt: Some(stmt) }
    }

    /// return arrow schema
    #[inline]
    pub fn get_schema(&self) -> SchemaRef {
        self.stmt.unwrap().stmt.schema()
    }
}

#[allow(clippy::needless_lifetimes)]
impl<'stmt> Iterator for Polars<'stmt> {
    type Item = DataFrame;

    fn next(&mut self) -> Option<Self::Item> {
        let struct_array = match self.stmt?.step_polars() {
            Ok(Some(array)) => array,
            Ok(None) => return None,
            Err(err) => panic!("Failed to fetch Polars DataFrame batch: {err}"),
        };
        let df = DataFrame::try_from(struct_array)
            .unwrap_or_else(|e| panic!("Failed to construct DataFrame from StructArray: {e}"));

        Some(df)
    }
}

#[cfg(test)]
mod tests {
    use polars::prelude::*;
    use polars_core::utils::accumulate_dataframes_vertical_unchecked;

    use crate::{Result, test::checked_memory_handle};

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

    #[test]
    fn test_query_polars_cached_ddl_uses_executed_metadata() -> Result<()> {
        let db = checked_memory_handle();
        db.execute_batch(
            r#"
            CREATE TABLE foo (x INTEGER);
            INSERT INTO foo VALUES (0);
        "#,
        )?;

        let sql = "SELECT * FROM foo";

        {
            let mut stmt = db.prepare_cached(sql)?;
            let mut polars = stmt.query_polars([])?;
            let df = polars.next().expect("Failed to get DataFrame");
            let column = df.column("x").expect("missing x column");
            assert_eq!(column.dtype(), &DataType::Int32);
            assert_eq!(column.get(0).expect("missing x value"), AnyValue::Int32(0));
            assert!(polars.next().is_none());
        }

        db.execute_batch(
            r#"
            DROP TABLE foo;
            CREATE TABLE foo (x DECIMAL(38, 0));
            INSERT INTO foo VALUES (5::DECIMAL(38,0));
        "#,
        )?;

        {
            let mut stmt = db.prepare_cached(sql)?;
            let mut polars = stmt.query_polars([])?;
            let df = polars.next().expect("Failed to get DataFrame");
            let column = df.column("x").expect("missing x column");
            assert_eq!(column.dtype(), &DataType::Decimal(Some(38), Some(0)));
            assert_eq!(column.get(0).expect("missing x value"), AnyValue::Decimal(5, 0));
            assert!(polars.next().is_none());
        }

        Ok(())
    }
}

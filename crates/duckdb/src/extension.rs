#[cfg(test)]
mod test {
    use crate::{Connection, Result};

    // https://duckdb.org/docs/extensions/json
    #[cfg(feature = "json")]
    #[test]
    fn test_extension_json() -> Result<()> {
        let db = Connection::open_in_memory()?;
        assert_eq!(
            4,
            db.query_row::<i32, _, _>(
                r#"SELECT json_array_length('["duck","goose","swan",null]');"#,
                [],
                |r| r.get(0)
            )?
        );
        Ok(())
    }

    // https://duckdb.org/docs/data/parquet/overview.html
    #[cfg(feature = "parquet")]
    #[test]
    fn test_extension_parquet() -> Result<()> {
        let db = Connection::open_in_memory()?;
        assert_eq!(
            300f32,
            db.query_row::<f32, _, _>(
                r#"SELECT SUM(value) FROM read_parquet('./examples/int32_decimal.parquet');"#,
                [],
                |r| r.get(0)
            )?
        );
        Ok(())
    }

    // https://duckdb.org/docs/stable/core_extensions/icu.html
    #[cfg(feature = "icu")]
    #[test]
    fn test_extension_icu() -> Result<()> {
        let db = Connection::open_in_memory()?;
        assert_eq!(
            1i64,
            db.query_row::<i64, _, _>(
                "SELECT count(*) FROM icu_calendar_names() WHERE name = 'gregorian';",
                [],
                |r| r.get(0)
            )?
        );
        assert_eq!(
            true,
            db.query_row::<bool, _, _>("SELECT length(icu_sort_key('Ş', 'ro')) > 0;", [], |r| r.get(0))?
        );
        Ok(())
    }

    #[cfg(feature = "parquet")]
    #[test]
    fn test_extension_remote_parquet() -> Result<()> {
        let db = Connection::open_in_memory()?;
        assert_eq!(
            9i64,
            db.query_row::<i64, _, _>(
                r#"SELECT count(*) FROM read_parquet('https://duckdb.org/data/prices.parquet');"#,
                [],
                |r| r.get(0)
            )?
        );
        Ok(())
    }
}

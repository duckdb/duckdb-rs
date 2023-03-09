#[cfg(test)]
mod test {
    use crate::{Connection, Result};

    // https://duckdb.org/docs/extensions/json
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

    // https://duckdb.org/docs/extensions/httpfs
    #[test]
    fn test_extension_httpfs() -> Result<()> {
        let db = Connection::open_in_memory()?;
        assert_eq!(
            300f32,
            db.query_row::<f32, _, _>(r#"SELECT SUM(value) FROM read_parquet('https://github.com/wangfenjin/duckdb-rs/raw/main/examples/int32_decimal.parquet');"#, [], |r| r.get(0))?
        );
        Ok(())
    }
}

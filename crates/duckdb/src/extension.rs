#[cfg(test)]
mod test {
    use crate::{Connection, Result};

    // https://duckdb.org/docs/current/data/json/overview
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

    // https://duckdb.org/docs/current/data/parquet/overview
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

    // https://duckdb.org/docs/current/core_extensions/icu
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
        assert!(db.query_row::<bool, _, _>("SELECT length(icu_sort_key('Ş', 'ro')) > 0;", [], |r| r.get(0))?);
        Ok(())
    }

    #[cfg(feature = "httpfs")]
    #[test]
    fn test_extension_httpfs() -> Result<(), Box<dyn std::error::Error>> {
        use crate::{Config, OptionalExt};

        let extension_directory = tempfile::tempdir()?;
        let config = Config::default()
            .with("extension_directory", extension_directory.path().to_string_lossy())?
            .with("autoinstall_known_extensions", "false")?
            .with("autoload_known_extensions", "false")?;
        let connection = Connection::open_in_memory_with_flags(config)?;

        connection.execute_batch("LOAD httpfs;")?;

        let extension_state = connection
            .query_row(
                "SELECT installed, loaded, install_mode, install_path FROM duckdb_extensions() WHERE extension_name = 'httpfs'",
                [],
                |row| {
                    Ok((
                        row.get::<_, bool>(0)?,
                        row.get::<_, bool>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                    ))
                },
            )
            .optional()?
            .expect("statically linked httpfs should be listed");
        assert_eq!(
            extension_state,
            (
                true,
                true,
                "STATICALLY_LINKED".to_owned(),
                Some("(BUILT-IN)".to_owned())
            ),
            "statically linked httpfs should load without an installed artifact"
        );
        assert!(
            extension_directory.path().read_dir()?.next().is_none(),
            "loading statically linked httpfs must not write an extension artifact"
        );

        Ok(())
    }
}

//! Checks that extensions enabled through `bundled-cmake-*` features are statically linked.
#![cfg(feature = "bundled-cmake")]

use duckdb_neo::{
    Parameters,
    connection::Connection,
    environment::{Environment, StorageLocation},
};

/// Open a connection that can only use statically linked extensions.
fn connect(extension_directory: &std::path::Path) -> duckdb_neo::Result<Connection> {
    let instance = Environment::new()?.instance()?;
    instance.set_option("extension_directory", &extension_directory.to_string_lossy())?;
    instance.set_option("autoinstall_known_extensions", "false")?;
    instance.set_option("autoload_known_extensions", "false")?;
    instance.attach(&StorageLocation::InMemory)?;
    instance.connect()
}

/// Return the first column of the first row as a string.
fn query_string(connection: &Connection, sql: &str) -> duckdb_neo::Result<Option<String>> {
    let chunk = connection
        .query(sql, Parameters::None)?
        .next_chunk()
        .expect("query should return a chunk")
        .unwrap();
    let vector = chunk.get_vector_at::<String>(0)?;
    Ok(vector.get(0)?.map(str::to_owned))
}

/// Load `name` and assert it came from the binary rather than an installed artifact.
fn assert_statically_linked(name: &str) -> Result<Connection, Box<dyn std::error::Error>> {
    let extension_directory = tempfile::tempdir()?;
    let connection = connect(extension_directory.path())?;
    connection.execute(format!("LOAD {name}").as_str(), Parameters::None)?;

    let state = query_string(
        &connection,
        &format!(
            "SELECT concat_ws('|', installed, loaded, install_mode, install_path) \
             FROM duckdb_extensions() WHERE extension_name = '{name}'"
        ),
    )?;
    assert_eq!(
        state.as_deref(),
        Some("true|true|STATICALLY_LINKED|(BUILT-IN)"),
        "{name} should be statically linked and loaded"
    );
    assert!(
        extension_directory.path().read_dir()?.next().is_none(),
        "loading statically linked {name} must not write an extension artifact"
    );
    Ok(connection)
}

// `bundled-cmake` always links parquet.
#[test]
fn test_extension_parquet() -> Result<(), Box<dyn std::error::Error>> {
    let connection = assert_statically_linked("parquet")?;
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("values.parquet");
    let path = path.to_str().expect("temp path should be UTF-8").replace('\'', "''");

    connection.execute(
        format!("COPY (SELECT range AS value FROM range(1, 101)) TO '{path}' (FORMAT parquet)").as_str(),
        Parameters::None,
    )?;
    assert_eq!(
        query_string(
            &connection,
            &format!("SELECT SUM(value)::VARCHAR FROM read_parquet('{path}')")
        )?
        .as_deref(),
        Some("5050")
    );
    Ok(())
}

#[cfg(feature = "bundled-cmake-autocomplete")]
#[test]
fn test_extension_autocomplete() -> Result<(), Box<dyn std::error::Error>> {
    let connection = assert_statically_linked("autocomplete")?;
    assert_eq!(
        query_string(
            &connection,
            "SELECT trim(suggestion) FROM sql_auto_complete('SEL') WHERE trim(suggestion) = 'SELECT' LIMIT 1"
        )?
        .as_deref(),
        Some("SELECT")
    );
    Ok(())
}

#[cfg(feature = "bundled-cmake-httpfs")]
#[test]
fn test_extension_httpfs() -> Result<(), Box<dyn std::error::Error>> {
    assert_statically_linked("httpfs")?;
    Ok(())
}

#[cfg(feature = "bundled-cmake-icu")]
#[test]
fn test_extension_icu() -> Result<(), Box<dyn std::error::Error>> {
    let connection = assert_statically_linked("icu")?;
    assert_eq!(
        query_string(
            &connection,
            "SELECT count(*)::VARCHAR FROM icu_calendar_names() WHERE name = 'gregorian'"
        )?
        .as_deref(),
        Some("1")
    );
    Ok(())
}

#[cfg(feature = "bundled-cmake-tpcds")]
#[test]
fn test_extension_tpcds() -> Result<(), Box<dyn std::error::Error>> {
    let connection = assert_statically_linked("tpcds")?;
    assert_eq!(
        query_string(&connection, "SELECT count(*)::VARCHAR FROM tpcds_queries()")?.as_deref(),
        Some("99")
    );
    Ok(())
}

#[cfg(feature = "bundled-cmake-tpch")]
#[test]
fn test_extension_tpch() -> Result<(), Box<dyn std::error::Error>> {
    let connection = assert_statically_linked("tpch")?;
    assert_eq!(
        query_string(&connection, "SELECT count(*)::VARCHAR FROM tpch_queries()")?.as_deref(),
        Some("22")
    );
    Ok(())
}

use duckdb::{Config, Connection, OptionalExt};

const MIN_DUCKDB_SOURCE_ID_LEN: usize = 7;

fn source_id_matches(expected_sha: &str, source_id: &str) -> bool {
    let expected_sha = expected_sha.to_ascii_lowercase();
    let source_id = source_id.to_ascii_lowercase();

    expected_sha.len() == 40
        && expected_sha.chars().all(|character| character.is_ascii_hexdigit())
        && source_id.len() >= MIN_DUCKDB_SOURCE_ID_LEN
        && expected_sha.starts_with(&source_id)
}

#[test]
#[ignore = "requires network access and matching nightly DuckDB extension artifacts"]
fn can_install_and_load_httpfs() -> Result<(), Box<dyn std::error::Error>> {
    let extension_directory = tempfile::tempdir()?;
    let config = Config::default().with("extension_directory", extension_directory.path().to_string_lossy())?;
    let connection = Connection::open_in_memory_with_flags(config)?;

    connection.execute_batch("FORCE INSTALL httpfs; LOAD httpfs;")?;

    let extension_state = connection
        .query_row(
            "SELECT installed, loaded FROM duckdb_extensions() WHERE extension_name = 'httpfs'",
            [],
            |row| Ok((row.get::<_, bool>(0)?, row.get::<_, bool>(1)?)),
        )
        .optional()?
        .expect("httpfs should be listed after FORCE INSTALL and LOAD");
    assert_eq!(extension_state, (true, true), "httpfs should be installed and loaded");

    Ok(())
}

/// CI points the build at a libduckdb artifact built from `DUCKDB_SHA`.
/// Assert we actually linked it and not a stray system library.
#[test]
#[ignore = "requires the nightly libduckdb artifact selected by DUCKDB_SHA"]
fn links_against_requested_duckdb_commit() -> Result<(), Box<dyn std::error::Error>> {
    let expected_sha = std::env::var("DUCKDB_SHA")
        .expect("DUCKDB_SHA must be set; this test only runs in the nightly workflow")
        .to_ascii_lowercase();
    let source_id: String =
        Connection::open_in_memory()?.query_row("SELECT source_id FROM pragma_version()", [], |row| row.get(0))?;

    assert!(
        source_id_matches(&expected_sha, &source_id),
        "linked DuckDB reports source_id {source_id}, expected a hexadecimal prefix of at least 7 characters matching {expected_sha}"
    );
    Ok(())
}

mod tests {
    use super::source_id_matches;

    #[test]
    fn source_id_match_is_case_insensitive() {
        assert!(source_id_matches(
            "14da6e6a4622b67d72fdeb21ed786a4f3ad9b063",
            "14DA6E6A46"
        ));
    }

    #[test]
    fn source_id_match_accepts_longer_git_abbreviations() {
        assert!(source_id_matches(
            "14da6e6a4622b67d72fdeb21ed786a4f3ad9b063",
            "14da6e6a462"
        ));
    }

    #[test]
    fn source_id_match_rejects_short_prefixes() {
        assert!(!source_id_matches("14da6e6a4622b67d72fdeb21ed786a4f3ad9b063", "1"));
    }
}

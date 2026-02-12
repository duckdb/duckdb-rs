/// Regression test for incorrect D_ASSERT(unbound_count == 0) in TableIndexList::Bind().
///
/// When `Bind()` is called with an `index_type` filter (e.g., "HNSW"), it only binds
/// indexes matching that type. The assertion incorrectly required ALL unbound indexes
/// to be zero, but when filtering by type, indexes of other types may remain unbound.
///
/// This was fixed in duckdb/duckdb main by removing the assertion and updating the
/// comment to "We bound all indexes. (of this type)".
///
/// The crash manifests as a debug assertion failure (STATUS_STACK_BUFFER_OVERRUN on
/// Windows) when closing/reopening a database that has extension-provided index types
/// (e.g., HNSW from the VSS extension).
#[cfg(test)]
mod test {
    use crate::{Config, Connection, Result};

    /// Reproduces the D_ASSERT(unbound_count == 0) crash in table_index_list.cpp.
    ///
    /// Steps:
    /// 1. Create a persistent database
    /// 2. Install and load the VSS extension (provides HNSW index type)
    /// 3. Create a table with a FLOAT[3] column and insert data
    /// 4. Create an HNSW index on that column
    /// 5. Close the database
    /// 6. Reopen it — this triggers TableIndexList::Bind() with index_type="HNSW"
    ///    while ART indexes may also be unbound, causing the assertion to fire
    ///
    /// Without the fix, this crashes with D_ASSERT(unbound_count == 0) in debug builds.
    #[test]
    fn test_table_index_list_bind_with_type_filter() -> Result<()> {
        let tmp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let db_path = tmp_dir.path().join("test_bind_assertion.duckdb");
        let db_path_str = db_path.to_str().unwrap();

        // Phase 1: Create database with HNSW index
        {
            let config = Config::default()
                .enable_autoload_extension(true)?
                .allow_unsigned_extensions()?;
            let db = Connection::open_with_flags(db_path_str, config)?;

            db.execute_batch("INSTALL vss; LOAD vss;")?;
            db.execute_batch("SET hnsw_enable_experimental_persistence = true;")?;

            db.execute_batch(
                "CREATE TABLE test_vectors (
                    id INTEGER PRIMARY KEY,
                    embedding FLOAT[3]
                );",
            )?;

            db.execute_batch(
                "INSERT INTO test_vectors VALUES
                    (1, [1.0, 0.0, 0.0]),
                    (2, [0.0, 1.0, 0.0]),
                    (3, [0.0, 0.0, 1.0]);",
            )?;

            db.execute_batch(
                "CREATE INDEX hnsw_idx ON test_vectors USING HNSW (embedding) WITH (metric = 'cosine');",
            )?;

            // Verify the index exists
            let index_count: i64 = db.query_row(
                "SELECT count(*) FROM duckdb_indexes() WHERE index_name = 'hnsw_idx'",
                [],
                |r| r.get(0),
            )?;
            assert_eq!(index_count, 1, "HNSW index should exist");
        }
        // Connection dropped here — database closed

        // Phase 2: Reopen the database — this triggers Bind() with type filter
        // Without the fix, this crashes with D_ASSERT(unbound_count == 0)
        {
            let config = Config::default()
                .enable_autoload_extension(true)?
                .allow_unsigned_extensions()?;
            let db = Connection::open_with_flags(db_path_str, config)?;

            // Verify the HNSW index persisted and is usable
            let index_count: i64 = db.query_row(
                "SELECT count(*) FROM duckdb_indexes() WHERE index_name = 'hnsw_idx'",
                [],
                |r| r.get(0),
            )?;
            assert_eq!(index_count, 1, "HNSW index should persist after reopen");

            // Verify data is intact
            let row_count: i64 =
                db.query_row("SELECT count(*) FROM test_vectors", [], |r| r.get(0))?;
            assert_eq!(row_count, 3, "Data should persist after reopen");
        }

        Ok(())
    }
}

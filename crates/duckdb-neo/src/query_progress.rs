//! Point-in-time progress for an executing query.

use std::sync::Arc;

use crate::{
    Result, check_api_call,
    connection::{Connection, InnerConnection, SettingScope},
    ffi,
};

/// Enables and reads query progress for a connection.
///
/// Creating a tracker enables connection-local progress tracking and disables
/// terminal progress output. These settings remain active after the tracker is
/// dropped. The tracker can be moved to another thread to take snapshots while
/// the connection runs a query.
pub struct QueryProgressTracker {
    connection: Arc<InnerConnection>,
}

impl QueryProgressTracker {
    /// Enable progress tracking and retain the connection used for snapshots.
    pub fn new(connection: &mut Connection) -> Result<Self> {
        connection.set_option("enable_progress_bar_print", "false", Some(SettingScope::Local))?;
        connection.set_option("enable_progress_bar", "true", Some(SettingScope::Local))?;
        Ok(Self {
            connection: connection.inner.clone(),
        })
    }

    /// Capture the active query's progress, or return `None` when unavailable.
    pub fn snapshot(&self) -> Result<Option<QueryProgress>> {
        QueryProgress::from_handle(self.connection.handle)
    }
}

/// A point-in-time snapshot of a connection's active query progress.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct QueryProgress {
    /// The completion percentage in `[0, 100]`.
    pub percentage: f64,
    /// The number of rows processed at capture time.
    pub rows_processed: usize,
    /// The estimated total number of rows to process.
    pub total_rows: usize,
}

impl QueryProgress {
    /// Capture progress, or return `None` when DuckDB has not published it.
    pub fn new(conn: &Connection) -> Result<Option<Self>> {
        Self::from_handle(conn.inner.handle)
    }

    fn from_handle(handle: ffi::duckdb_v2_connection_handle) -> Result<Option<Self>> {
        let mut percentage = 0.0;
        let mut rows_processed = 0;
        let mut total_rows = 0;
        check_api_call!(
            ffi::duckdb_v2_connection_progress_get,
            handle,
            &mut percentage,
            &mut rows_processed,
            &mut total_rows
        )?;

        if percentage < 0.0 || (rows_processed == 0 && total_rows == 0) {
            return Ok(None);
        }

        Ok(Some(Self {
            percentage,
            rows_processed: rows_processed as usize,
            total_rows: total_rows as usize,
        }))
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use crate::{
        Parameters, environment::Environment, environment::StorageLocation, query_progress::QueryProgressTracker,
    };

    #[test]
    fn test_query_progress() -> crate::Result<()> {
        let env = Environment::new()?;
        let db = env.open(StorageLocation::InMemory)?;
        let mut conn = db.connect()?;

        conn.execute(
            "CREATE TABLE t1 as select * from range(0, 100_00) r(i)",
            Parameters::None,
        )?;

        let tracker = QueryProgressTracker::new(&mut conn)?;
        assert!(tracker.snapshot()?.is_none());

        let mut statements = conn.parse(" SELECT * FROM t1 AS l, t1 AS r;")?;

        let statement = statements.next().unwrap()?;

        let mut result = conn.query(statement, Parameters::None)?;

        let _chunk = result.next().unwrap()?;
        let progress = tracker.snapshot()?.expect("expected query progress");

        assert_eq!(progress.rows_processed, 20_000);
        assert!(progress.percentage > 99.0);
        assert_eq!(progress.total_rows, 20_001);

        Ok(())
    }

    #[test]
    fn test_query_without_progress() -> crate::Result<()> {
        let env = Environment::new()?;
        let db = env.open(StorageLocation::InMemory)?;
        let mut conn = db.connect()?;
        let tracker = QueryProgressTracker::new(&mut conn)?;

        let mut statements = conn.parse("SELECT sum(sin(i)) FROM range(100000) AS t(i);")?;
        let statement = statements.next().expect("expected a statement")?;
        let mut result = conn.query(statement, Parameters::None)?;

        assert!(result.next().transpose()?.is_some());
        assert!(tracker.snapshot()?.is_none());

        Ok(())
    }
}

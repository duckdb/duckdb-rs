//! Point-in-time progress for an executing query.

use std::sync::{Arc, Weak};

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
/// the connection runs a query. It does not keep the connection open.
#[derive(Debug, Clone)]
pub struct QueryProgressTracker {
    connection: Weak<InnerConnection>,
}

impl QueryProgressTracker {
    /// Enable session progress without terminal output, keeping only a weak connection reference.
    /// For example: `let tracker = QueryProgressTracker::new(&connection)?;`.
    pub fn new(connection: &Connection) -> Result<Self> {
        connection
            .config()?
            .set_text("enable_progress_bar_print", "false", SettingScope::Session)?;
        connection
            .config()?
            .set_text("enable_progress_bar", "true", SettingScope::Session)?;
        Ok(Self {
            connection: Arc::downgrade(&connection.inner),
        })
    }

    /// Return whether the connection or its dependent objects are still alive.
    pub fn is_alive(&self) -> bool {
        self.connection.strong_count() > 0
    }

    /// Return `None` if the connection is closed, progress is unpublished, or both row counts are zero.
    /// Progress requires tracking to be enabled and a query to be active.
    pub fn snapshot(&self) -> Result<Option<QueryProgress>> {
        match self.connection.upgrade() {
            Some(connection) => QueryProgress::from_handle(connection.handle),
            None => Ok(None),
        }
    }
}

/// A point-in-time snapshot of a connection's active query progress.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct QueryProgress {
    /// The completion percentage in `[0, 100]`.
    pub percentage: f64,
    /// The number of rows processed at capture time.
    pub rows_processed: u64,
    /// The estimated total number of rows to process.
    pub total_rows: u64,
}

impl QueryProgress {
    /// Capture published progress; requires `enable_progress_bar` to be enabled.
    /// Returns `None` if no query is active, progress is unpublished, or both row counts are zero.
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
            rows_processed,
            total_rows,
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
    fn test_tracker_clone_liveness() -> crate::Result<()> {
        let conn = Environment::new()?.open(StorageLocation::InMemory)?.connect()?;
        let tracker = QueryProgressTracker::new(&conn)?;
        let clone = tracker.clone();
        assert!(tracker.is_alive());
        assert!(clone.is_alive());
        drop(conn);
        assert!(!tracker.is_alive());
        assert!(!clone.is_alive());
        assert!(clone.snapshot()?.is_none());
        Ok(())
    }

    #[test]
    fn test_query_progress() -> crate::Result<()> {
        let env = Environment::new()?;
        let instance = env.open(StorageLocation::InMemory)?;
        let conn = instance.connect()?;

        conn.execute(
            "CREATE TABLE t1 as select * from range(0, 100_00) r(i)",
            Parameters::None,
        )?;

        let tracker = QueryProgressTracker::new(&conn)?;
        assert!(tracker.snapshot()?.is_none());

        let mut statements = conn.parse(" SELECT * FROM t1 AS l, t1 AS r;")?;

        let statement = statements.next().unwrap()?;

        let mut result = conn.query(statement, Parameters::None)?;

        let _chunk = result.next_chunk()?.unwrap();
        let progress = tracker.snapshot()?.expect("expected query progress");

        let rows_processed: u64 = progress.rows_processed;
        let total_rows: u64 = progress.total_rows;
        assert_eq!(rows_processed, 12_048);
        assert!((progress.percentage - 60.24).abs() < 0.01);
        assert_eq!(total_rows, 20_001);

        Ok(())
    }

    #[test]
    fn test_query_without_progress() -> crate::Result<()> {
        let env = Environment::new()?;
        let instance = env.open(StorageLocation::InMemory)?;
        let conn = instance.connect()?;
        let tracker = QueryProgressTracker::new(&conn)?;

        let mut statements = conn.parse("SELECT sum(sin(i)) FROM range(100000) AS t(i);")?;
        let statement = statements.next().expect("expected a statement")?;
        let mut result = conn.query(statement, Parameters::None)?;

        assert!(result.next_chunk()?.is_some());
        assert!(tracker.snapshot()?.is_none());

        Ok(())
    }
}

//! Query progress tracking for DuckDB.
//!
//! DuckDB can report how far the query currently running on a connection has
//! progressed. This is the estimate that drives the CLI's progress bar.
//!
//! # Usage
//!
//! Progress tracking must be enabled per connection; it is off by default and
//! does not carry to other connections to the same database.
//!
//! ```rust,no_run
//! # use duckdb::{Connection, Result};
//! # fn main() -> Result<()> {
//! let conn = Connection::open_in_memory()?;
//! conn.set_query_progress_tracking(true)?;
//!
//! // A handle can be polled from another thread while the query runs.
//! let progress = conn.progress_handle();
//! std::thread::spawn(move || {
//!     loop {
//!         if let Some(pct) = progress.query_progress().percentage() {
//!             println!("{pct:.1}%");
//!         }
//!         std::thread::sleep(std::time::Duration::from_millis(500));
//!     }
//! });
//!
//! conn.execute_batch("create table t as select * from range(100000000)")?;
//! # Ok(())
//! # }
//! ```
//!
//! # Caveats
//!
//! A snapshot describes whichever query the connection is running at the time,
//! and the estimate only advances while a thread is inside DuckDB's execution
//! loop — for a streaming result, as the consumer pulls batches.
//!
//! DuckDB cannot estimate every plan, and the quality varies by source: a
//! table scan reports an estimate that advances, while some plans report a row
//! total they never make progress against, or no estimate at all. When there is
//! none, [`QueryProgress::percentage`] is `None` even with tracking enabled;
//! that means "unknown", not "zero".

use std::sync::Arc;

use crate::{Connection, Result, ffi, inner_connection::ConnectionCell, inner_connection::InnerConnection};

/// A snapshot of the progress of the query running on a connection.
///
/// The three values are read independently, so a snapshot taken mid-query may
/// mix readings from adjacent moments.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct QueryProgress {
    percentage: f64,
    rows_processed: u64,
    total_rows_to_process: u64,
}

impl QueryProgress {
    #[inline]
    fn unavailable() -> Self {
        Self {
            percentage: -1.0,
            rows_processed: 0,
            total_rows_to_process: 0,
        }
    }

    #[inline]
    fn from_raw(raw: ffi::duckdb_query_progress_type) -> Self {
        Self {
            percentage: raw.percentage,
            rows_processed: raw.rows_processed,
            total_rows_to_process: raw.total_rows_to_process,
        }
    }

    /// How much of the query DuckDB estimates is done, as a percentage in
    /// `0.0..=100.0`.
    ///
    /// `None` when no estimate is available: tracking is off, no query is
    /// running, or the plan is one DuckDB cannot estimate.
    #[inline]
    pub fn percentage(&self) -> Option<f64> {
        self.is_available().then_some(self.percentage)
    }

    /// Rows processed so far, or `0` when no estimate is available.
    #[inline]
    pub fn rows_processed(&self) -> u64 {
        self.rows_processed
    }

    /// Rows the query expects to process, or `0` when no estimate is available.
    #[inline]
    pub fn total_rows_to_process(&self) -> u64 {
        self.total_rows_to_process
    }

    /// Whether DuckDB reported an estimate at all.
    #[inline]
    pub fn is_available(&self) -> bool {
        self.percentage >= 0.0
    }
}

fn read_progress(cell: &ConnectionCell) -> QueryProgress {
    cell.with(|conn| QueryProgress::from_raw(unsafe { ffi::duckdb_query_progress(conn) }))
        .unwrap_or_else(QueryProgress::unavailable)
}

/// A handle for polling the progress of a long-running query.
///
/// A [`Connection`] is not `Sync`, so the thread running the query owns it for
/// the duration. This handle is `Send + Sync`, cheap to clone, and does not
/// block the query.
///
/// Once the connection is closed or dropped, [`ProgressHandle::query_progress`]
/// reports an unavailable snapshot rather than failing, matching how
/// [`crate::InterruptHandle`] becomes a no-op. That makes a closed connection
/// indistinguishable from a query DuckDB cannot estimate, so a poller deciding
/// whether to keep polling should ask [`ProgressHandle::is_connected`].
///
/// See [the module docs](crate::progress) for an example.
#[derive(Clone)]
pub struct ProgressHandle {
    cell: Arc<ConnectionCell>,
}

impl ProgressHandle {
    #[inline]
    pub(crate) fn new(cell: Arc<ConnectionCell>) -> Self {
        Self { cell }
    }

    /// Snapshot the progress of the query running on the connection this
    /// handle came from.
    pub fn query_progress(&self) -> QueryProgress {
        read_progress(&self.cell)
    }

    /// Whether the connection this handle came from is still open.
    ///
    /// Distinguishes a closed connection from a query with no estimate, since
    /// [`ProgressHandle::query_progress`] reports both as unavailable. This is
    /// a hint, not a guarantee: the connection may close immediately after it
    /// returns `true`.
    pub fn is_connected(&self) -> bool {
        self.cell.with(|_| ()).is_some()
    }
}

impl std::fmt::Debug for ProgressHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProgressHandle").finish_non_exhaustive()
    }
}

impl InnerConnection {
    pub fn query_progress(&self) -> QueryProgress {
        read_progress(&self.cell)
    }

    pub fn get_progress_handle(&self) -> ProgressHandle {
        ProgressHandle::new(self.cell.clone())
    }
}

impl Connection {
    /// Snapshot the progress of the query running on this connection.
    ///
    /// Requires [`Connection::set_query_progress_tracking`]. Because executing
    /// a query borrows the connection, this is useful mainly while consuming a
    /// streaming result; to watch a query from outside the thread running it,
    /// use [`Connection::progress_handle`].
    #[inline]
    pub fn query_progress(&self) -> QueryProgress {
        self.db.borrow().query_progress()
    }

    /// Get a [`ProgressHandle`] for polling this connection's query progress
    /// from another thread.
    ///
    /// Requires [`Connection::set_query_progress_tracking`].
    #[inline]
    pub fn progress_handle(&self) -> ProgressHandle {
        self.db.borrow().get_progress_handle()
    }

    /// Turn query progress tracking on or off for this connection. Off by
    /// default; until it is on, [`Connection::query_progress`] and
    /// [`ProgressHandle::query_progress`] report nothing.
    ///
    /// Enabling also turns off DuckDB's terminal progress bar, which
    /// `enable_progress_bar` would otherwise switch on. To get the bar back,
    /// follow this with `SET enable_progress_bar_print = true`.
    ///
    /// This is a client setting rather than a database one, so it cannot be set
    /// through [`crate::Config`] at open time.
    ///
    /// # Failure
    ///
    /// Will return `Err` if the embedding environment has disabled the progress
    /// bar, which DuckDB reports as a failure to set the option.
    pub fn set_query_progress_tracking(&self, enabled: bool) -> Result<()> {
        self.execute_batch(&format!(
            "SET enable_progress_bar = {enabled}; SET enable_progress_bar_print = false;"
        ))
    }
}

#[cfg(test)]
mod test {
    use std::{
        sync::{Arc, mpsc},
        thread,
        time::Duration,
    };

    use super::ProgressHandle;
    use crate::{Connection, Result};

    /// A scan reports an advancing estimate, where a `range()` cross product
    /// reports a row total it never makes progress against.
    const SCAN_QUERY: &str = "select * from t";

    /// Enough of [`SCAN_QUERY`] to have made measurable progress, and few
    /// enough to leave the scan unfinished.
    const BATCHES: usize = 40;

    fn tracked_connection() -> Result<Connection> {
        let conn = Connection::open_in_memory()?;
        conn.set_query_progress_tracking(true)?;
        Ok(conn)
    }

    /// A tracked connection holding a table large enough that [`BATCHES`]
    /// batches of [`SCAN_QUERY`] leave the scan in flight.
    fn scan_connection() -> Result<Connection> {
        let conn = tracked_connection()?;
        conn.execute_batch("create table t as select i, i::varchar as s from range(4000000) t(i)")?;
        Ok(conn)
    }

    /// Consumes [`BATCHES`] batches of [`SCAN_QUERY`] on another thread, then
    /// holds the query open across `inspect` so it can be observed mid-flight
    /// from the calling thread.
    fn with_query_in_flight<T>(conn: Connection, inspect: impl FnOnce() -> T) -> T {
        let (ready_tx, ready_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();

        let worker = thread::spawn(move || {
            let mut stmt = conn.prepare(SCAN_QUERY).unwrap();
            let mut stream = stmt.stream_arrow([]).unwrap();
            for _ in 0..BATCHES {
                assert!(stream.next().is_some(), "the scan finished early");
            }
            ready_tx.send(()).unwrap();
            release_rx.recv().unwrap();
        });

        ready_rx
            .recv_timeout(Duration::from_secs(60))
            .expect("the query thread never reached its ready point");
        let observed = inspect();
        release_tx.send(()).unwrap();
        worker.join().unwrap();
        observed
    }

    /// `Arc<InterruptHandle>` is documented as sendable to another thread, and
    /// both handles are only useful off the executing thread, so these impls
    /// are part of the contract rather than an accident of the fields.
    #[test]
    fn handles_are_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<ProgressHandle>();
        assert_send_sync::<crate::InterruptHandle>();
        assert_send_sync::<Arc<crate::InterruptHandle>>();
    }

    #[test]
    fn tracking_does_not_carry_to_another_connection() -> Result<()> {
        let conn = tracked_connection()?;
        let cloned = conn.try_clone()?;

        let mut stmt = cloned.prepare("select current_setting('enable_progress_bar')")?;
        let enabled: bool = stmt.query_row([], |row| row.get(0))?;
        assert!(!enabled, "a second connection must opt in separately");
        Ok(())
    }

    #[test]
    fn is_connected_distinguishes_a_closed_connection() -> Result<()> {
        let conn = tracked_connection()?;
        let progress = conn.progress_handle();
        assert!(progress.is_connected());

        // Both states read as unavailable, which is what `is_connected` is for.
        assert_eq!(progress.query_progress().percentage(), None);
        drop(conn);

        assert!(!progress.is_connected());
        assert_eq!(progress.query_progress().percentage(), None);
        Ok(())
    }

    #[test]
    fn progress_is_unavailable_when_tracking_is_off() -> Result<()> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch("select 42")?;

        let progress = conn.query_progress();
        assert_eq!(progress.percentage(), None);
        assert!(!progress.is_available());
        assert_eq!(progress.rows_processed(), 0);
        assert_eq!(progress.total_rows_to_process(), 0);
        Ok(())
    }

    #[test]
    fn progress_is_unavailable_when_no_query_is_running() -> Result<()> {
        let conn = tracked_connection()?;
        assert_eq!(conn.query_progress().percentage(), None);
        Ok(())
    }

    #[test]
    fn progress_is_unavailable_once_the_query_is_done() -> Result<()> {
        let conn = tracked_connection()?;
        conn.execute_batch("select count(*) from range(1000)")?;
        assert_eq!(conn.query_progress().percentage(), None);
        Ok(())
    }

    /// Consuming the stream drives execution from this thread, so the estimate
    /// advances on demand rather than on a race with a background query.
    #[test]
    fn progress_advances_as_a_streaming_result_is_consumed() -> Result<()> {
        let conn = scan_connection()?;
        let mut stmt = conn.prepare(SCAN_QUERY)?;
        let mut stream = stmt.stream_arrow([])?;

        let mut samples = Vec::new();
        for _ in 0..6 {
            for _ in 0..BATCHES {
                assert!(stream.next().is_some(), "the scan finished early");
            }
            samples.push(conn.query_progress());
        }

        let seen: Vec<_> = samples.iter().map(|s| (s.percentage(), s.rows_processed())).collect();
        for sample in &samples {
            assert!(sample.is_available(), "progress went unavailable mid-stream: {seen:?}");
            assert!(sample.rows_processed() > 0, "no rows reported: {seen:?}");
            assert!(sample.rows_processed() <= sample.total_rows_to_process());
            let percentage = sample.percentage().unwrap();
            assert!((0.0..=100.0).contains(&percentage), "percentage out of range: {seen:?}");
        }
        for pair in samples.windows(2) {
            assert!(
                pair[1].rows_processed() >= pair[0].rows_processed(),
                "progress went backwards: {seen:?}"
            );
        }
        assert!(
            samples.last().unwrap().rows_processed() > samples.first().unwrap().rows_processed(),
            "progress never advanced: {seen:?}"
        );
        Ok(())
    }

    #[test]
    fn handle_reports_progress_while_a_query_runs_on_another_thread() -> Result<()> {
        let conn = scan_connection()?;
        let progress = conn.progress_handle();

        let snapshot = with_query_in_flight(conn, || progress.query_progress());

        assert!(
            snapshot.is_available(),
            "progress should be readable from another thread"
        );
        assert!(snapshot.rows_processed() > 0);
        assert!(snapshot.rows_processed() <= snapshot.total_rows_to_process());
        let percentage = snapshot.percentage().unwrap();
        assert!(
            (0.0..=100.0).contains(&percentage),
            "percentage out of range: {percentage}"
        );
        Ok(())
    }

    #[test]
    fn tracking_can_be_turned_back_off() -> Result<()> {
        let conn = scan_connection()?;
        conn.set_query_progress_tracking(false)?;
        let progress = conn.progress_handle();

        let snapshot = with_query_in_flight(conn, || progress.query_progress());

        assert_eq!(
            snapshot.percentage(),
            None,
            "tracking is off, so there is nothing to report"
        );
        assert_eq!(snapshot.rows_processed(), 0);
        Ok(())
    }

    #[test]
    fn handle_outliving_its_connection_reports_unavailable() -> Result<()> {
        let conn = tracked_connection()?;
        let progress = conn.progress_handle();

        drop(conn);

        assert_eq!(progress.query_progress().percentage(), None);
        assert_eq!(progress.query_progress().rows_processed(), 0);
        Ok(())
    }

    #[test]
    fn handle_is_cloneable_and_shared_across_threads() -> Result<()> {
        let conn = tracked_connection()?;
        let progress = conn.progress_handle();

        let clone = progress.clone();
        let joined = thread::spawn(move || clone.query_progress()).join().unwrap();

        assert_eq!(joined.percentage(), None);
        assert_eq!(progress.query_progress().percentage(), None);
        Ok(())
    }
}

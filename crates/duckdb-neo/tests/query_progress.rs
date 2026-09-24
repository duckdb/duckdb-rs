use duckdb_neo::{
    Parameters,
    environment::{Environment, StorageLocation},
    query_progress::{QueryProgress, QueryProgressTracker},
};

#[test]
fn test_owned_progress_snapshots() -> duckdb_neo::Result<()> {
    let mut connection = Environment::new()?.open(StorageLocation::InMemory)?.connect()?;
    assert!(QueryProgress::new(&connection)?.is_none());
    let tracker = QueryProgressTracker::new(&mut connection)?;
    assert!(tracker.snapshot()?.is_none());

    connection.execute("CREATE TABLE data AS SELECT * FROM range(10000)", Parameters::None)?;
    let mut result = connection.query("SELECT * FROM data AS a, data AS b", Parameters::None)?;
    assert!(result.next().transpose()?.is_some());
    let progress = tracker.snapshot()?.expect("query should publish progress");
    assert!((0.0..=100.0).contains(&progress.percentage));
    assert!(progress.rows_processed > 0);
    assert!(progress.total_rows >= progress.rows_processed);
    for _ in 0..32 {
        assert_eq!(tracker.snapshot()?, Some(progress));
    }
    Ok(())
}

#[test]
fn test_progress_snapshot_from_other_thread() -> duckdb_neo::Result<()> {
    let mut connection = Environment::new()?.open(StorageLocation::InMemory)?.connect()?;
    let tracker = QueryProgressTracker::new(&mut connection)?;

    connection.execute("CREATE TABLE data AS SELECT * FROM range(10000)", Parameters::None)?;
    let mut result = connection.query("SELECT * FROM data AS a, data AS b", Parameters::None)?;
    assert!(result.next().transpose()?.is_some());

    let progress = std::thread::spawn(move || tracker.snapshot()).join().unwrap()?;
    assert!(progress.is_some_and(|p| p.rows_processed > 0));
    Ok(())
}

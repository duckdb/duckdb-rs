use std::thread;

use duckdb_neo::{
    Parameters,
    environment::{Environment, StorageLocation},
};

#[test]
fn test_send_between_threads() -> duckdb_neo::Result<()> {
    let connection = Environment::new()?.open(StorageLocation::InMemory)?.connect()?;

    let connection = thread::spawn(move || {
        connection
            .execute("CREATE TABLE test(value INTEGER)", Parameters::None)
            .expect("connection should execute SQL on another thread");
        connection
            .execute("INSERT INTO test VALUES (42)", Parameters::None)
            .expect("connection should retain its database on another thread");

        connection
    })
    .join()
    .expect("connection thread should complete");

    let mut result = connection.query("SELECT value FROM test", Parameters::None)?;
    let chunk = result.next().expect("query should return a chunk")?;
    let values = chunk
        .get_vector_at::<i32>(0)?
        .iter()?
        .map(|value| value.copied())
        .collect::<Vec<_>>();
    assert_eq!(values, vec![Some(42)]);
    assert!(result.next().is_none());
    drop(result);

    assert_eq!(
        connection.execute("DELETE FROM test WHERE value = 42", Parameters::None)?,
        1
    );

    Ok(())
}

#[test]
fn test_handles_do_not_keep_connection_open() -> duckdb_neo::Result<()> {
    use duckdb_neo::query_progress::QueryProgressTracker;

    let db = Environment::new()?.open(StorageLocation::InMemory)?;
    let other = db.connect()?;
    other.execute("CREATE TABLE t(i INTEGER)", Parameters::None)?;
    other.execute("INSERT INTO t VALUES (1)", Parameters::None)?;

    let mut first = db.connect()?;
    first.execute("BEGIN", Parameters::None)?;
    first.execute("UPDATE t SET i = 2", Parameters::None)?;
    let interrupt = first.interrupt_handle();
    let tracker = QueryProgressTracker::new(&mut first)?;
    drop(first);

    // Closing the connection rolls back its transaction, so the row is no longer locked.
    assert_eq!(other.execute("UPDATE t SET i = 3", Parameters::None)?, 1);
    assert!(!interrupt.is_alive());
    interrupt.interrupt()?;
    assert!(tracker.snapshot()?.is_none());
    Ok(())
}

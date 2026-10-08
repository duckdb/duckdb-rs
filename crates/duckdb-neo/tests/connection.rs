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

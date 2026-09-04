use std::thread;

use duckdb_rs::{
    Parameters,
    environment::{Environment, StorageLocation},
};

#[test]
fn test_send_between_threads() -> duckdb_rs::Result<()> {
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
    thread::scope(|scope| {
        scope
            .spawn(move || {
                let chunk = result
                    .next()
                    .expect("query should return a chunk")
                    .expect("query should succeed on another thread");
                let vector = chunk
                    .get_vector_at::<i32>(0)
                    .expect("query should return an integer column");
                let values = vector
                    .iter()
                    .expect("integer column should be readable")
                    .map(|value| value.copied())
                    .collect::<Vec<_>>();

                assert_eq!(values, vec![Some(42)]);
                assert!(result.next().is_none());
            })
            .join()
            .expect("query result thread should complete");
    });

    assert_eq!(
        connection.execute("DELETE FROM test WHERE value = 42", Parameters::None)?,
        1
    );

    Ok(())
}

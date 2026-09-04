use std::thread;

use duckdb_rs::{
    Parameters,
    environment::{Environment, StorageLocation},
};

#[test]
fn connection_can_be_sent_between_threads() -> duckdb_rs::Result<()> {
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

    assert_eq!(
        connection.execute("DELETE FROM test WHERE value = 42", Parameters::None)?,
        1
    );

    Ok(())
}

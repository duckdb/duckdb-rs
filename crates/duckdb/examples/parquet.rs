use std::path::PathBuf;

use duckdb::{
    arrow::{record_batch::RecordBatch, util::pretty::print_batches},
    Connection, Result,
};

fn main() -> Result<()> {
    let db = Connection::open_in_memory()?;

    db.execute_batch("INSTALL parquet; LOAD parquet;")?;

    let parquet_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("examples/int32_decimal.parquet");

    let rbs: Vec<RecordBatch> = db
        .prepare("SELECT * FROM read_parquet(?)")?
        .query_arrow([parquet_path.to_string_lossy()])?
        .collect();

    assert!(print_batches(&rbs).is_ok());

    Ok(())
}

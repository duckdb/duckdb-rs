extern crate duckdb;
use duckdb::{
    arrow::{record_batch::RecordBatch, util::pretty::print_batches},
    Connection, Result,
};

fn main() -> Result<()> {
    let db = Connection::open_in_memory()?;
    db.execute_batch("INSTALL parquet; LOAD parquet;")?;
    let rbs: Vec<RecordBatch> = db
        .prepare("SELECT * FROM read_parquet('./examples/int32_decimal.parquet');")?
        .query_arrow([])?
        .collect();
    assert!(print_batches(&rbs).is_ok());
    Ok(())
}

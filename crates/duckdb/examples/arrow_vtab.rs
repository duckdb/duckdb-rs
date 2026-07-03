// Query an in-memory Arrow RecordBatch from DuckDB SQL.
//
// Run with: cargo run --example arrow_vtab --features bundled,vtab-arrow

use std::{error::Error, sync::Arc};

use duckdb::{
    Connection,
    arrow::{
        array::{ArrayRef, BooleanArray, Int32Array, StringArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
        util::pretty::print_batches,
    },
    vtab::arrow::{ArrowVTab, arrow_recordbatch_to_query_params},
};

fn main() -> Result<(), Box<dyn Error>> {
    let conn = Connection::open_in_memory()?;
    conn.register_table_function::<ArrowVTab>("arrow")?;

    let params = arrow_recordbatch_to_query_params(cities_batch()?);
    let batches: Vec<RecordBatch> = conn
        .prepare(
            "
            SELECT city, population
            FROM arrow(?, ?)
            WHERE coastal AND population >= 500000
            ORDER BY population DESC
            ",
        )?
        .query_arrow(params)?
        .collect();

    print_batches(&batches)?;
    Ok(())
}

fn cities_batch() -> Result<RecordBatch, Box<dyn Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("city", DataType::Utf8, false),
        Field::new("population", DataType::Int32, false),
        Field::new("coastal", DataType::Boolean, false),
    ]));

    Ok(RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4])) as ArrayRef,
            Arc::new(StringArray::from(vec!["Amsterdam", "Berlin", "Lisbon", "Madrid"])) as ArrayRef,
            Arc::new(Int32Array::from(vec![921_402, 3_755_251, 567_131, 3_332_035])) as ArrayRef,
            Arc::new(BooleanArray::from(vec![true, false, true, false])) as ArrayRef,
        ],
    )?)
}

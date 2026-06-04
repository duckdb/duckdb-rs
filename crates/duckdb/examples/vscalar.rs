// Custom scalar function example.
//
// Defines a scalar function `add_one(BIGINT) -> BIGINT` by implementing the
// `VScalar` trait, registers it on a connection, and calls it from SQL.
//
// Run with: cargo run --example vscalar --features vscalar

use duckdb::{
    Connection, Result,
    core::{DataChunkHandle, LogicalTypeHandle, LogicalTypeId},
    vscalar::{ScalarFunctionSignature, VScalar},
    vtab::arrow::WritableVector,
};

struct AddOne;

impl VScalar for AddOne {
    // No per-function state is needed.
    type State = ();

    /// Called once per data chunk. DuckDB passes a batch of input rows and
    /// expects the corresponding output values to be written back.
    unsafe fn invoke(
        _: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let len = input.len();

        // Read the single BIGINT argument as a slice of i64. This example
        // assumes non-null input; a real function should also check validity.
        let src = input.flat_vector(0);
        let src = unsafe { src.as_slice_with_len::<i64>(len) };

        // Write the results into the output vector.
        let mut out = output.flat_vector();
        let dst = unsafe { out.as_mut_slice_with_len::<i64>(len) };

        for (d, s) in dst.iter_mut().zip(src) {
            *d = s + 1;
        }

        Ok(())
    }

    /// One signature: takes a BIGINT, returns a BIGINT.
    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![LogicalTypeHandle::from(LogicalTypeId::Bigint)],
            LogicalTypeHandle::from(LogicalTypeId::Bigint),
        )]
    }
}

fn main() -> Result<()> {
    let conn = Connection::open_in_memory()?;
    conn.register_scalar_function::<AddOne>("add_one")?;

    // Apply the function across a batch of rows.
    let mut stmt = conn.prepare("SELECT add_one(i) FROM range(5) t(i)")?;
    let values: Vec<i64> = stmt.query_map([], |row| row.get(0))?.collect::<Result<_>>()?;

    println!("add_one(0..5) = {values:?}");
    assert_eq!(values, vec![1, 2, 3, 4, 5]);

    Ok(())
}

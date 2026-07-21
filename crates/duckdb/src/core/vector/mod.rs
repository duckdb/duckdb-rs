//! Borrowed vector wrappers over [`duckdb_vector`].
//!
//! Each wrapper type carries a lifetime `'a`. When obtained via a
//! [`DataChunkHandle`][crate::core::DataChunkHandle] accessor, `'a` is
//! bound to the chunk so the wrapper cannot outlive it. When built via
//! one of the `unsafe fn from_raw` constructors (including the raw
//! `duckdb_vector` path used by the Arrow interop layer), `'a` is
//! caller-chosen and must not exceed the DuckDB vector's actual validity —
//! that path does not track liveness in the type system.

use libduckdb_sys::duckdb_validity_row_is_valid;

use crate::{
    Result,
    error::duckdb_failure_from_message,
    ffi::{duckdb_vector, duckdb_vector_get_validity},
};

mod array;
mod flat;
mod list;
mod r#struct;

pub use array::ArrayVector;
pub use flat::{FlatVector, Inserter};
pub use list::ListVector;
pub use r#struct::StructVector;

fn try_vector_row_is_null(ptr: duckdb_vector, row: u64, capacity: usize) -> Result<bool> {
    let row_index = usize::try_from(row)
        .map_err(|_| duckdb_failure_from_message(format!("row index {row} exceeds usize range")))?;
    if row_index >= capacity {
        return Err(duckdb_failure_from_message(format!(
            "row index {row} exceeds vector capacity {capacity}"
        )));
    }
    let valid = unsafe {
        let validity = duckdb_vector_get_validity(ptr);

        // validity can return a NULL pointer if the entire vector is valid
        if validity.is_null() {
            return Ok(false);
        }

        duckdb_validity_row_is_valid(validity, row)
    };

    Ok(!valid)
}

fn vector_row_is_null(ptr: duckdb_vector, row: u64, capacity: usize) -> bool {
    try_vector_row_is_null(ptr, row, capacity).unwrap_or_else(|err| panic!("{err}"))
}

#[cfg(test)]
mod tests;

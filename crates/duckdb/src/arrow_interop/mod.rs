//! Arrow/DuckDB conversion layer.
//!
//! This module owns schema mapping and vector/record-batch conversion. Public
//! compatibility paths are re-exported through `vtab::arrow`, with selected
//! legacy paths also re-exported through `vtab`.

mod from_duckdb;
mod schema;
mod to_duckdb;

#[cfg(test)]
pub(crate) mod test_support;
#[cfg(test)]
mod tests;

pub(crate) const UUID_EXTENSION_NAME: &str = "arrow.uuid";
pub(crate) const UUID_BYTE_WIDTH: i32 = 16;

pub use crate::core::{WritableVector, WritableVectorRef};
pub use from_duckdb::{data_chunk_to_arrow, flat_vector_to_arrow_array};
pub use schema::{to_duckdb_logical_type, to_duckdb_logical_type_for_field, to_duckdb_type_id};
pub use to_duckdb::{record_batch_to_duckdb_data_chunk, write_arrow_array_to_vector};

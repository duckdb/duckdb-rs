//! Arrow extension types in DuckDB's Arrow representation.

/// Arrow field metadata key naming the field's extension type.
#[cfg(test)]
pub(crate) const ARROW_EXTENSION_NAME_KEY: &str = "ARROW:extension:name";

pub(crate) const UUID_EXTENSION_NAME: &str = "arrow.uuid";
pub(crate) const UUID_BYTE_WIDTH: i32 = 16;

pub(crate) const VARIANT_EXTENSION_NAME: &str = "arrow.parquet.variant";

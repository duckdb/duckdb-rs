//! Conversions between Rust strings/slices and the v2 C API string types.
//!
//! These live in the sys crate because the orphan rule prevents a downstream
//! wrapper from implementing `From`/`Default` for the bindgen-generated types.

use super::{duckdb_v2_str, idx_t};

// Only Rust-to-C conversions live here: building a raw string is safe, reading through one is not.
impl From<&str> for duckdb_v2_str {
    fn from(val: &str) -> Self {
        duckdb_v2_str {
            ptr: val.as_ptr() as *const _,
            len: val.len() as idx_t,
        }
    }
}

impl From<&String> for duckdb_v2_str {
    fn from(val: &String) -> Self {
        duckdb_v2_str {
            ptr: val.as_ptr() as *const _,
            len: val.len() as idx_t,
        }
    }
}

impl Default for duckdb_v2_str {
    fn default() -> Self {
        duckdb_v2_str {
            ptr: std::ptr::null(),
            len: 0,
        }
    }
}

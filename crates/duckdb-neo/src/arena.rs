use crate::{Result, bytes::DuckDBBytes, error::check_api_call, ffi, raw::RawExt};
use duckdb_loadable_macros::{AsRaw, FromRaw};

/// A borrowed byte arena, valid until its vector is resized, flattened, or destroyed.
#[derive(Debug, FromRaw, AsRaw)]
pub struct Arena {
    pub(crate) handle: ffi::duckdb_v2_arena_handle,
}

impl Arena {
    /// Reserve uninitialized bytes in the arena.
    #[inline]
    pub fn allocate(&self, len: usize) -> Result<*mut u8> {
        check_api_call!(ffi::duckdb_v2_arena_allocate, self.raw(), len as u64, RET)
    }

    /// Encode bytes, storing short values inline and copying longer values into this arena.
    pub fn add_bytes<'a>(&'a self, value: &[u8]) -> Result<DuckDBBytes<'a>> {
        DuckDBBytes::new(self, value)
    }

    /// Concatenate byte slices directly into inline storage or one arena allocation.
    ///
    /// The combined length must fit in a `u32`.
    /// This does not validate UTF-8 or other logical-type encodings.
    #[inline]
    pub fn add_parts<'a>(&'a self, parts: &[&[u8]]) -> Result<DuckDBBytes<'a>> {
        DuckDBBytes::from_parts(parts, || Ok(self))
    }

    /// Concatenate UTF-8 string slices into inline storage or one arena allocation.
    ///
    /// The combined byte length must fit in a `u32`.
    #[inline]
    pub fn add_strings<'a>(&'a self, parts: &[&str]) -> Result<DuckDBBytes<'a>> {
        DuckDBBytes::from_parts(parts, || Ok(self))
    }

    /// Encode UTF-8 text, storing short values inline and copying longer values into this arena.
    pub fn add_str<'a>(&'a self, value: &str) -> Result<DuckDBBytes<'a>> {
        self.add_bytes(value.as_bytes())
    }
}

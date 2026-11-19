use std::marker::PhantomData;

use crate::raw::RawExt;
use duckdb_loadable_macros::{AsRaw, FromRaw};

use crate::{Result, error::check_api_call, ffi};

/// Nullability statistics of a function argument, function result, or table column.
///
/// Borrowed from DuckDB for the duration of a stats callback. Argument statistics
/// are read-only, and their setters fail.
#[derive(Debug, AsRaw, FromRaw)]
pub struct Stats<'a> {
    pub(crate) handle: ffi::duckdb_v2_stats_handle,
    _marker: PhantomData<&'a ()>,
}

impl Stats<'_> {
    /// Whether a value can be NULL; `false` guarantees none is.
    pub fn can_have_null(&self) -> Result<bool> {
        check_api_call!(ffi::duckdb_v2_stats_can_have_null, self.raw(), RET)
    }

    /// Whether a value can be non-NULL; `false` guarantees every value is NULL.
    pub fn can_have_valid(&self) -> Result<bool> {
        check_api_call!(ffi::duckdb_v2_stats_can_have_valid, self.raw(), RET)
    }

    /// Set whether a value can be non-NULL; `false` is a guarantee, while `true` is always safe.
    pub fn set_have_valid(&self, value: bool) -> Result<()> {
        check_api_call!(ffi::duckdb_v2_stats_set_can_have_valid, self.raw(), value)
    }

    /// Set whether a value can be NULL; `false` is a guarantee, while `true` is always safe.
    pub fn set_have_null(&self, value: bool) -> Result<()> {
        check_api_call!(ffi::duckdb_v2_stats_set_can_have_null, self.raw(), value)
    }
}

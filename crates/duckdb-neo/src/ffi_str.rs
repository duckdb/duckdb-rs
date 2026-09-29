//! Reading strings and bytes that DuckDB hands back through the C API.

use std::marker::PhantomData;

use crate::ffi;

/// A string returned by DuckDB, borrowed from whatever owns its bytes for `'a`.
///
/// Callers pick `'a` in [`Self::from_raw`]: tie it to the owning handle, or `'static` for static strings.
#[derive(Clone, Copy)]
pub(crate) struct DuckDBStr<'a> {
    raw: ffi::duckdb_v2_str,
    _owner: PhantomData<&'a [u8]>,
}

impl<'a> DuckDBStr<'a> {
    /// `raw` must be null or point to `len` UTF-8 bytes that stay valid and unchanged for `'a`.
    pub(crate) fn from_raw(raw: ffi::duckdb_v2_str) -> Self {
        Self {
            raw,
            _owner: PhantomData,
        }
    }

    /// Return the string, or `None` when DuckDB returned a null pointer.
    pub(crate) fn as_str(&self) -> Option<&'a str> {
        let bytes = self.as_bytes()?;
        // SAFETY: the C API returns UTF-8 text.
        Some(unsafe { std::str::from_utf8_unchecked(bytes) })
    }

    /// Return the raw bytes, or `None` when DuckDB returned a null pointer.
    pub(crate) fn as_bytes(&self) -> Option<&'a [u8]> {
        if self.raw.ptr.is_null() {
            return None;
        }
        // SAFETY: `from_raw` requires `len` bytes valid for `'a`.
        Some(unsafe { std::slice::from_raw_parts(self.raw.ptr.cast(), self.raw.len as usize) })
    }

    pub(crate) fn as_raw_ptr(&self) -> *const ffi::duckdb_v2_str {
        &self.raw as *const ffi::duckdb_v2_str
    }
}

impl From<DuckDBStr<'_>> for Option<String> {
    fn from(value: DuckDBStr<'_>) -> Self {
        value.as_str().map(str::to_owned)
    }
}

impl From<ffi::duckdb_v2_str> for DuckDBStr<'_> {
    fn from(value: ffi::duckdb_v2_str) -> Self {
        Self::from_raw(value)
    }
}

impl<'a> From<&'a str> for DuckDBStr<'a> {
    fn from(value: &str) -> Self {
        DuckDBStr::from(ffi::duckdb_v2_str {
            ptr: value.as_ptr().cast(),
            len: value.len() as ffi::idx_t,
        })
    }
}

impl<'a> From<&'a String> for DuckDBStr<'a> {
    fn from(value: &String) -> Self {
        DuckDBStr::from(ffi::duckdb_v2_str {
            ptr: value.as_ptr().cast(),
            len: value.len() as ffi::idx_t,
        })
    }
}

/// Build a C string argument from bytes that need not be UTF-8, e.g. BLOB data.
pub(crate) fn bytes_arg(bytes: &[u8]) -> ffi::duckdb_v2_str {
    ffi::duckdb_v2_str {
        ptr: bytes.as_ptr().cast(),
        len: bytes.len() as ffi::idx_t,
    }
}

/// Return the bytes of a `duckdb_v2_bytes` read from a vector.
///
/// `value` must come from DuckDB, so its pointer form points to `length` bytes valid while it is borrowed.
pub(crate) fn bytes_view(value: &ffi::duckdb_v2_bytes) -> &[u8] {
    // SAFETY: the inline form holds up to 12 bytes in place; longer values use the pointer form.
    unsafe {
        if value.value.inlined.length <= 12 {
            let len = value.value.inlined.length as usize;
            std::slice::from_raw_parts(value.value.inlined.inlined.as_ptr().cast(), len)
        } else {
            let len = value.value.pointer.length as usize;
            std::slice::from_raw_parts(value.value.pointer.ptr.cast(), len)
        }
    }
}

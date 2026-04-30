use std::{
    ffi::{CStr, c_char},
    ops::Deref,
};

use crate::duckdb_free;

pub struct DuckDbString {
    // Invariant: ptr[0..len+1] is valid C string, i.e. ptr[len] is NUL byte.
    ptr: core::ptr::NonNull<c_char>,
    len: usize,
}

impl DuckDbString {
    /// Creates a `DuckDbString` from a raw pointer to a C string.
    ///
    /// # Safety
    ///
    /// The caller must ensure that:
    /// - `ptr` is non-null and points to a null-terminated C string.
    /// - `ptr` was allocated by DuckDB and must be released with `duckdb_free`.
    /// - Ownership of `ptr` transfers to the returned `DuckDbString`. The caller must not reuse or free it.
    pub unsafe fn from_ptr(ptr: *const c_char) -> Self {
        assert!(!ptr.is_null(), "DuckDbString::from_ptr requires a non-null pointer");
        let len = unsafe { CStr::from_ptr(ptr) }.to_bytes().len();
        unsafe { Self::from_raw_parts(ptr, len) }
    }

    /// Creates a `DuckDbString` from a nullable raw pointer to a C string.
    ///
    /// Returns `None` if `ptr` is null.
    ///
    /// # Safety
    ///
    /// If `ptr` is non-null, the caller must ensure that:
    /// - `ptr` points to a null-terminated C string.
    /// - `ptr` was allocated by DuckDB and must be released with `duckdb_free`.
    /// - Ownership of `ptr` transfers to the returned `DuckDbString`. The caller must not reuse or free it.
    pub unsafe fn from_nullable_ptr(ptr: *const c_char) -> Option<Self> {
        if ptr.is_null() {
            None
        } else {
            Some(unsafe { Self::from_ptr(ptr) })
        }
    }

    /// Creates a `DuckDbString` from raw parts.
    ///
    /// # Safety
    ///
    /// The caller must ensure that:
    /// - `ptr` is non-null and points to a null-terminated C string.
    /// - `len` accurately represents the length of the string (excluding the null terminator).
    /// - `ptr` was allocated by DuckDB and must be released with `duckdb_free`.
    /// - Ownership of `ptr` transfers to the returned `DuckDbString`. The caller must not reuse or free it.
    /// - The string data is not mutated for the lifetime of the returned `DuckDbString`.
    pub unsafe fn from_raw_parts(ptr: *const c_char, len: usize) -> Self {
        let ptr = core::ptr::NonNull::new(ptr as *mut c_char)
            .expect("DuckDbString::from_raw_parts requires a non-null pointer");
        Self { ptr, len }
    }

    fn to_bytes_with_nul(&self) -> &[u8] {
        let ptr = self.ptr.as_ptr() as *const u8;
        unsafe { core::slice::from_raw_parts(ptr, self.len + 1) }
    }
}

impl Deref for DuckDbString {
    type Target = std::ffi::CStr;

    fn deref(&self) -> &Self::Target {
        let bytes = self.to_bytes_with_nul();
        unsafe { CStr::from_bytes_with_nul_unchecked(bytes) }
    }
}

impl Drop for DuckDbString {
    fn drop(&mut self) {
        let ptr = self.ptr.as_ptr() as *mut core::ffi::c_void;
        unsafe { duckdb_free(ptr) };
    }
}

#[cfg(test)]
mod tests {
    use super::DuckDbString;

    #[test]
    fn from_nullable_ptr_returns_none_for_null() {
        assert!(unsafe { DuckDbString::from_nullable_ptr(std::ptr::null()) }.is_none());
    }
}

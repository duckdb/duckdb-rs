use std::{
    ffi::{c_char, CStr},
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
    /// The caller must ensure that the pointer is valid and points to a null-terminated C string.
    /// The memory must remain valid for the lifetime of the returned `DuckDbString`.
    pub unsafe fn from_ptr(ptr: *const c_char) -> Self {
        let len = unsafe { CStr::from_ptr(ptr) }.to_bytes().len();
        unsafe { Self::from_raw_parts(ptr, len) }
    }

    /// Creates a `DuckDbString` from raw parts.
    ///
    /// # Safety
    ///
    /// The caller must ensure that:
    /// - `ptr` is a valid pointer to a null-terminated C string.
    /// - `len` accurately represents the length of the string (excluding the null terminator).
    /// - The memory referenced by `ptr` remains valid for the lifetime of the returned `DuckDbString`.
    /// - The string data is not mutated for the lifetime of the returned `DuckDbString`.
    pub unsafe fn from_raw_parts(ptr: *const c_char, len: usize) -> Self {
        let ptr = unsafe { core::ptr::NonNull::new_unchecked(ptr as *mut c_char) };
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

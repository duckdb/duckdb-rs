use std::{
    ffi::{c_char, CStr},
    ops::Deref,
};

use crate::duckdb_free;

pub struct DuckDbStr {
    // Invariant: ptr[0..len+1] is valid C string, i.e. ptr[len] is NUL byte.
    ptr: core::ptr::NonNull<c_char>,
    len: usize,
}

impl DuckDbStr {
    pub unsafe fn from_ptr(ptr: *const c_char) -> Self {
        let len = unsafe { CStr::from_ptr(ptr) }.to_bytes().len();
        unsafe { Self::from_raw_parts(ptr, len) }
    }

    pub unsafe fn from_raw_parts(ptr: *const c_char, len: usize) -> Self {
        let ptr = unsafe { core::ptr::NonNull::new_unchecked(ptr as *mut c_char) };
        Self { ptr, len }
    }

    fn to_bytes_with_nul(&self) -> &[u8] {
        let ptr = self.ptr.as_ptr() as *const u8;
        unsafe { core::slice::from_raw_parts(ptr, self.len + 1) }
    }
}

impl Deref for DuckDbStr {
    type Target = std::ffi::CStr;

    fn deref(&self) -> &Self::Target {
        let bytes = self.to_bytes_with_nul();
        unsafe { CStr::from_bytes_with_nul_unchecked(bytes) }
    }
}

impl Drop for DuckDbStr {
    fn drop(&mut self) {
        let ptr = self.ptr.as_ptr() as *mut core::ffi::c_void;
        unsafe { duckdb_free(ptr) };
    }
}

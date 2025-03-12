use crate::ffi::{duckdb_create_selection_vector, duckdb_selection_vector, duckdb_selection_vector_get_data_ptr};
use libduckdb_sys::idx_t;
use std::ptr;

pub struct SelectionVector {
    ptr: duckdb_selection_vector,
    len: idx_t,
}

impl SelectionVector {
    pub fn new_copy(vec: &[u32]) -> Self {
        let ptr = unsafe { duckdb_create_selection_vector(vec.len() as idx_t) };

        let data = unsafe { duckdb_selection_vector_get_data_ptr(ptr) };
        unsafe {
            ptr::copy_nonoverlapping(vec.as_ptr(), data, vec.len());
        }
        Self {
            ptr,
            len: vec.len() as idx_t,
        }
    }

    pub(crate) fn as_ptr(&self) -> duckdb_selection_vector {
        self.ptr
    }

    pub fn len(&self) -> u64 {
        self.len
    }
}

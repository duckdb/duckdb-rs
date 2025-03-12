use crate::ffi::{duckdb_create_selection_vector, duckdb_selection_vector, duckdb_selection_vector_get_data_ptr};
use std::ptr;

pub struct SelectionVector {
    ptr: duckdb_selection_vector,
    // u64 is an idx_t from duckdb.
    len: u64,
}

impl SelectionVector {
    pub fn new_copy(vec: &[u32]) -> Self {
        let sel = unsafe { duckdb_create_selection_vector(vec.len() as u64) };

        let data = unsafe { duckdb_selection_vector_get_data_ptr(sel) };
        unsafe {
            ptr::copy_nonoverlapping(vec.as_ptr(), data, vec.len());
        }
        Self {
            ptr,
            len: vec.len() as u64,
        }
    }

    pub(crate) fn as_ptr(&self) -> duckdb_selection_vector {
        self.ptr
    }

    pub fn len(&self) -> u64 {
        self.len
    }
}

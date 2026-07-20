use std::marker::PhantomData;

use libduckdb_sys::DuckDbString;

use crate::{
    Result,
    core::LogicalTypeHandle,
    ffi::{
        duckdb_struct_type_child_count, duckdb_struct_type_child_name, duckdb_struct_vector_get_child,
        duckdb_validity_set_row_invalid, duckdb_vector, duckdb_vector_ensure_validity_writable,
        duckdb_vector_get_column_type, duckdb_vector_get_validity, duckdb_vector_size,
    },
};

use super::{ArrayVector, FlatVector, ListVector, try_vector_row_is_null, vector_row_is_null};

/// A struct vector borrowed from a [`DataChunkHandle`][crate::core::DataChunkHandle].
///
/// Groups one child vector per struct field, all sharing the same row count.
pub struct StructVector<'a> {
    ptr: duckdb_vector,
    capacity: usize,
    _phantom: PhantomData<&'a ()>,
}

impl<'a> StructVector<'a> {
    /// Wrap a raw `duckdb_vector` pointer.
    ///
    /// # Safety
    /// `ptr` must be a valid `duckdb_vector` that remains valid for all of `'a`.
    pub(crate) unsafe fn from_raw(ptr: duckdb_vector) -> Self {
        unsafe { Self::from_raw_with_capacity(ptr, duckdb_vector_size() as usize) }
    }

    /// Wrap a raw struct vector pointer with a caller-supplied row capacity.
    ///
    /// # Safety
    /// `ptr` must be a valid struct `duckdb_vector` that remains valid for all
    /// of `'a`, and `capacity` must not exceed the backing row allocation.
    pub(crate) unsafe fn from_raw_with_capacity(ptr: duckdb_vector, capacity: usize) -> Self {
        Self {
            ptr,
            capacity,
            _phantom: PhantomData,
        }
    }

    /// Returns the child by idx in the struct vector.
    ///
    /// # Panics
    /// Panics if `capacity` exceeds the parent vector capacity.
    pub fn child(&self, idx: usize, capacity: usize) -> FlatVector<'a> {
        assert!(
            capacity <= self.capacity,
            "struct child capacity {capacity} exceeds parent capacity {}",
            self.capacity
        );
        // SAFETY: struct children share the parent allocation, and the check
        // above bounds the exposed child view by the parent capacity.
        unsafe { FlatVector::with_capacity(self.child_ptr(idx), capacity) }
    }

    // Legacy typed child accessors remain public for callers that need nested
    // vector views.

    /// Take the child as [StructVector].
    pub fn struct_vector_child(&self, idx: usize) -> StructVector<'a> {
        unsafe { StructVector::from_raw_with_capacity(self.child_ptr(idx), self.capacity) }
    }

    /// Take the child as [ListVector].
    pub fn list_vector_child(&self, idx: usize) -> ListVector<'a> {
        unsafe { ListVector::from_raw_with_capacity(self.child_ptr(idx), self.capacity) }
    }

    /// Take the child as [ArrayVector].
    pub fn array_vector_child(&self, idx: usize) -> ArrayVector<'a> {
        unsafe { ArrayVector::from_raw_with_capacity(self.child_ptr(idx), self.capacity) }
    }

    pub(crate) fn child_ptr(&self, idx: usize) -> duckdb_vector {
        unsafe { duckdb_struct_vector_get_child(self.ptr, idx as u64) }
    }

    /// Get the logical type of this struct vector.
    pub fn logical_type(&self) -> LogicalTypeHandle {
        unsafe { LogicalTypeHandle::new(duckdb_vector_get_column_type(self.ptr)) }
    }

    /// Returns true if the row at the given index is null.
    ///
    /// # Panics
    /// Panics if `row` is outside the vector capacity.
    pub fn row_is_null(&self, row: u64) -> bool {
        vector_row_is_null(self.ptr, row, self.capacity)
    }

    /// Returns whether the row is null, or an error if it is out of range.
    pub fn try_row_is_null(&self, row: u64) -> Result<bool> {
        try_vector_row_is_null(self.ptr, row, self.capacity)
    }

    /// Get the name of the child by idx.
    ///
    /// Panics if `idx` is out of range.
    pub fn child_name(&self, idx: usize) -> DuckDbString {
        let logical_type = self.logical_type();
        unsafe {
            let child_name_ptr = duckdb_struct_type_child_name(logical_type.ptr, idx as u64);
            if child_name_ptr.is_null() {
                panic!("child index {idx} out of range");
            }
            DuckDbString::from_ptr(child_name_ptr)
        }
    }

    /// Get the number of children.
    pub fn num_children(&self) -> usize {
        let logical_type = self.logical_type();
        unsafe { duckdb_struct_type_child_count(logical_type.ptr) as usize }
    }

    /// Set row as null
    pub fn set_null(&mut self, row: usize) {
        unsafe {
            duckdb_vector_ensure_validity_writable(self.ptr);
            let idx = duckdb_vector_get_validity(self.ptr);
            duckdb_validity_set_row_invalid(idx, row as u64);
        }
    }
}

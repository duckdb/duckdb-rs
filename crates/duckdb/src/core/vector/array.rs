use std::marker::PhantomData;

use libduckdb_sys::{duckdb_array_type_array_size, duckdb_array_vector_get_child};

use crate::{
    Result,
    core::LogicalTypeHandle,
    ffi::{
        duckdb_validity_set_row_invalid, duckdb_vector, duckdb_vector_ensure_validity_writable,
        duckdb_vector_get_column_type, duckdb_vector_get_validity, duckdb_vector_size,
    },
};

use super::{FlatVector, try_vector_row_is_null, vector_row_is_null};

/// A fixed-size list vector borrowed from a
/// [`DataChunkHandle`][crate::core::DataChunkHandle].
///
/// Exposes a fixed-width list whose child storage is contiguous across all rows.
pub struct ArrayVector<'a> {
    ptr: duckdb_vector,
    capacity: usize,
    _phantom: PhantomData<&'a ()>,
}

impl<'a> ArrayVector<'a> {
    /// Wrap a raw `duckdb_vector` pointer.
    ///
    /// # Safety
    /// `ptr` must be a valid `duckdb_vector` that remains valid for all of `'a`.
    pub(crate) unsafe fn from_raw(ptr: duckdb_vector) -> Self {
        unsafe { Self::from_raw_with_capacity(ptr, duckdb_vector_size() as usize) }
    }

    /// Wrap a raw array vector pointer with a caller-supplied row capacity.
    ///
    /// # Safety
    /// `ptr` must be a valid array `duckdb_vector` that remains valid for all
    /// of `'a`, and `capacity` must not exceed the backing row allocation.
    pub(crate) unsafe fn from_raw_with_capacity(ptr: duckdb_vector, capacity: usize) -> Self {
        Self {
            ptr,
            capacity,
            _phantom: PhantomData,
        }
    }

    /// Get the logical type of this ArrayVector.
    pub fn logical_type(&self) -> LogicalTypeHandle {
        unsafe { LogicalTypeHandle::new(duckdb_vector_get_column_type(self.ptr)) }
    }

    /// Returns the size of the array type.
    pub fn get_array_size(&self) -> u64 {
        let ty = self.logical_type();
        unsafe { duckdb_array_type_array_size(ty.ptr) as u64 }
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

    /// Returns the child vector.
    /// capacity should be a multiple of the array size.
    ///
    /// # Panics
    /// Panics if `capacity` is not a multiple of the fixed array size or exceeds
    /// the child allocation derived from the parent capacity.
    // TODO: not ideal interface. Where should we keep count.
    pub fn child(&self, capacity: usize) -> FlatVector<'a> {
        let array_size = self.get_array_size() as usize;
        assert_eq!(
            capacity % array_size,
            0,
            "array child capacity must be a multiple of the fixed array size"
        );
        let child_capacity = self
            .capacity
            .checked_mul(array_size)
            .expect("array child capacity overflows usize");
        assert!(
            capacity <= child_capacity,
            "array child capacity {capacity} exceeds backing capacity {child_capacity}"
        );
        // SAFETY: fixed-array children contain `array_size` values per parent
        // row, and the checks above bound `capacity` by that allocation.
        unsafe { FlatVector::with_capacity(self.child_ptr(), capacity) }
    }

    pub(crate) fn child_ptr(&self) -> duckdb_vector {
        unsafe { duckdb_array_vector_get_child(self.ptr) }
    }

    /// Set primitive data to the child node.
    ///
    /// # Panics
    /// Panics if `data.len()` is not a multiple of the fixed array size.
    ///
    /// # Safety
    /// The caller must ensure `T` matches the child vector's physical storage
    /// and no other references to the same storage exist during the copy. The
    /// caller must also ensure the child backing allocation has space for all
    /// `data` elements; nested callers can arrange this by reserving capacity
    /// on the parent list vector before taking the array child.
    pub unsafe fn set_child<T: Copy>(&self, data: &[T]) {
        unsafe { self.child(data.len()).copy(data) };
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

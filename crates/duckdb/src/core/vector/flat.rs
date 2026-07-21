use std::{ffi::CString, marker::PhantomData, slice};

use crate::{
    Result,
    core::LogicalTypeHandle,
    ffi::{
        duckdb_validity_set_row_invalid, duckdb_vector, duckdb_vector_assign_string_element,
        duckdb_vector_assign_string_element_len, duckdb_vector_ensure_validity_writable, duckdb_vector_get_column_type,
        duckdb_vector_get_data, duckdb_vector_get_validity, duckdb_vector_size,
    },
};

use super::{try_vector_row_is_null, vector_row_is_null};

/// A flat (contiguous, scalar-row) vector borrowed from a
/// [`DataChunkHandle`][crate::core::DataChunkHandle].
pub struct FlatVector<'a> {
    pub(super) ptr: duckdb_vector,
    capacity: usize,
    _phantom: PhantomData<&'a ()>,
}

impl<'a> FlatVector<'a> {
    /// Wrap a raw `duckdb_vector` pointer.
    ///
    /// # Safety
    /// `ptr` must be a valid `duckdb_vector` that remains valid for all of `'a`.
    pub(crate) unsafe fn from_raw(ptr: duckdb_vector) -> Self {
        Self {
            ptr,
            capacity: unsafe { duckdb_vector_size() as usize },
            _phantom: PhantomData,
        }
    }

    /// Wrap a raw vector pointer with a caller-supplied element capacity.
    ///
    /// # Safety
    /// `ptr` must be a valid `duckdb_vector` that remains valid for all of `'a`.
    /// The caller must ensure `capacity` does not exceed the backing allocation
    /// for the logical view being exposed. Later typed slice and copy methods
    /// trust this value for bounds checks.
    pub(crate) unsafe fn with_capacity(ptr: duckdb_vector, capacity: usize) -> Self {
        Self {
            ptr,
            capacity,
            _phantom: PhantomData,
        }
    }

    /// Returns the capacity of the vector
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    #[cfg(feature = "vtab-arrow")]
    pub(crate) fn ptr(&self) -> duckdb_vector {
        self.ptr
    }

    /// Returns true if the row at the given index is null
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

    /// Returns a mutable pointer to the vector's backing data cast to `T`.
    ///
    /// # Safety
    /// The caller must ensure `T` matches the DuckDB vector's physical storage.
    /// Before dereferencing the pointer, the caller must also ensure the target
    /// element is initialized, in bounds, and not accessed through another
    /// incompatible or aliased reference.
    pub unsafe fn as_mut_ptr<T>(&self) -> *mut T {
        unsafe { duckdb_vector_get_data(self.ptr).cast() }
    }

    /// Returns a typed slice of the vector.
    ///
    /// # Safety
    /// The caller must ensure `T` matches the DuckDB vector's physical storage,
    /// the backing allocation is sized for `self.capacity()` elements of `T`,
    /// each returned element is initialized before it is read, and no `&mut`
    /// references to the same storage exist for the returned lifetime.
    pub unsafe fn as_slice<T>(&self) -> &[T] {
        unsafe { slice::from_raw_parts(self.as_mut_ptr(), self.capacity()) }
    }

    /// Returns a typed slice of the vector up to a certain length.
    ///
    /// # Safety
    /// The caller must ensure `T` matches the DuckDB vector's physical storage,
    /// the backing allocation is sized for at least `len` elements of `T`, each
    /// returned element is initialized before it is read, and no `&mut`
    /// references to the same storage exist for the returned lifetime.
    pub unsafe fn as_slice_with_len<T>(&self, len: usize) -> &[T] {
        unsafe { slice::from_raw_parts(self.as_mut_ptr(), len) }
    }

    /// Returns a typed mutable slice of the vector.
    ///
    /// # Safety
    /// The caller must ensure `T` matches the DuckDB vector's physical storage,
    /// the backing allocation is sized for `self.capacity()` elements of `T`,
    /// and no other references to the same storage exist for the returned
    /// lifetime.
    pub unsafe fn as_mut_slice<T>(&mut self) -> &mut [T] {
        unsafe { slice::from_raw_parts_mut(self.as_mut_ptr(), self.capacity()) }
    }

    /// Returns a typed mutable slice of the vector up to a certain length.
    ///
    /// # Safety
    /// The caller must ensure `T` matches the DuckDB vector's physical storage,
    /// the backing allocation is sized for at least `len` elements of `T`, and
    /// no other references to the same storage exist for the returned lifetime.
    pub unsafe fn as_mut_slice_with_len<T>(&mut self, len: usize) -> &mut [T] {
        unsafe { slice::from_raw_parts_mut(self.as_mut_ptr(), len) }
    }

    /// Returns the logical type of the vector
    pub fn logical_type(&self) -> LogicalTypeHandle {
        unsafe { LogicalTypeHandle::new(duckdb_vector_get_column_type(self.ptr)) }
    }

    /// Set row as null
    pub fn set_null(&mut self, row: usize) {
        unsafe {
            duckdb_vector_ensure_validity_writable(self.ptr);
            let idx = duckdb_vector_get_validity(self.ptr);
            duckdb_validity_set_row_invalid(idx, row as u64);
        }
    }

    /// Copy typed data to the vector.
    ///
    /// # Safety
    /// The caller must ensure `T` matches the DuckDB vector's physical storage
    /// and no other references to the same storage exist during the copy.
    ///
    /// # Panics
    /// Panics if `data.len()` exceeds the vector capacity.
    pub unsafe fn copy<T: Copy>(&mut self, data: &[T]) {
        assert!(data.len() <= self.capacity());
        (unsafe { self.as_mut_slice::<T>() })[0..data.len()].copy_from_slice(data);
    }
}

/// A trait for inserting data into a vector.
pub trait Inserter<T> {
    /// Insert a value into the vector.
    fn insert(&self, index: usize, value: T);
}

impl Inserter<CString> for FlatVector<'_> {
    fn insert(&self, index: usize, value: CString) {
        unsafe {
            duckdb_vector_assign_string_element(self.ptr, index as u64, value.as_ptr());
        }
    }
}

impl Inserter<&str> for FlatVector<'_> {
    fn insert(&self, index: usize, value: &str) {
        self.insert(index, value.as_bytes());
    }
}

impl Inserter<&String> for FlatVector<'_> {
    fn insert(&self, index: usize, value: &String) {
        self.insert(index, value.as_str());
    }
}

impl Inserter<&[u8]> for FlatVector<'_> {
    fn insert(&self, index: usize, value: &[u8]) {
        let value_size = value.len();
        unsafe {
            // This function also works for binary data. https://duckdb.org/docs/api/c/api#duckdb_vector_assign_string_element_len
            duckdb_vector_assign_string_element_len(
                self.ptr,
                index as u64,
                value.as_ptr() as *const ::std::os::raw::c_char,
                value_size as u64,
            );
        }
    }
}

impl Inserter<&Vec<u8>> for FlatVector<'_> {
    fn insert(&self, index: usize, value: &Vec<u8>) {
        self.insert(index, value.as_slice());
    }
}

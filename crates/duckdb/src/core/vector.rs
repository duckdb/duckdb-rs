//! Borrowed vector wrappers over [`duckdb_vector`].
//!
//! Each wrapper type carries a lifetime `'a`. When obtained via a
//! [`DataChunkHandle`][crate::core::DataChunkHandle] accessor, `'a` is
//! bound to the chunk so the wrapper cannot outlive it. When built via
//! one of the `unsafe fn from_raw` constructors (including the raw
//! `duckdb_vector` path used by `vtab::arrow`), `'a` is caller-chosen and
//! must not exceed the DuckDB vector's actual validity — that path does not
//! track liveness in the type system.

use std::{ffi::CString, marker::PhantomData, slice};

use libduckdb_sys::{
    DuckDbString, duckdb_array_type_array_size, duckdb_array_vector_get_child, duckdb_validity_row_is_valid,
};

use super::LogicalTypeHandle;
use crate::ffi::{
    duckdb_list_entry, duckdb_list_vector_get_child, duckdb_list_vector_get_size, duckdb_list_vector_reserve,
    duckdb_list_vector_set_size, duckdb_struct_type_child_count, duckdb_struct_type_child_name,
    duckdb_struct_vector_get_child, duckdb_validity_set_row_invalid, duckdb_vector,
    duckdb_vector_assign_string_element, duckdb_vector_assign_string_element_len,
    duckdb_vector_ensure_validity_writable, duckdb_vector_get_column_type, duckdb_vector_get_data,
    duckdb_vector_get_validity, duckdb_vector_size,
};

/// A flat (contiguous, scalar-row) vector borrowed from a
/// [`DataChunkHandle`][crate::core::DataChunkHandle].
pub struct FlatVector<'a> {
    ptr: duckdb_vector,
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

    fn with_capacity(ptr: duckdb_vector, capacity: usize) -> Self {
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

    /// Returns true if the row at the given index is null
    pub fn row_is_null(&self, row: u64) -> bool {
        // use idx_t entry_idx = row_idx / 64; idx_t idx_in_entry = row_idx % 64; bool is_valid = validity_mask[entry_idx] & (1 « idx_in_entry);
        // as the row is valid function is slower
        let valid = unsafe {
            let validity = duckdb_vector_get_validity(self.ptr);

            // validity can return a NULL pointer if the entire vector is valid
            if validity.is_null() {
                return false;
            }

            duckdb_validity_row_is_valid(validity, row)
        };

        !valid
    }

    /// Returns an unsafe mutable pointer to the vector’s
    pub fn as_mut_ptr<T>(&self) -> *mut T {
        unsafe { duckdb_vector_get_data(self.ptr).cast() }
    }

    /// Returns a slice of the vector
    pub fn as_slice<T>(&self) -> &[T] {
        unsafe { slice::from_raw_parts(self.as_mut_ptr(), self.capacity()) }
    }

    /// Returns a slice of the vector up to a certain length
    pub fn as_slice_with_len<T>(&self, len: usize) -> &[T] {
        unsafe { slice::from_raw_parts(self.as_mut_ptr(), len) }
    }

    /// Returns a mutable slice of the vector
    pub fn as_mut_slice<T>(&mut self) -> &mut [T] {
        unsafe { slice::from_raw_parts_mut(self.as_mut_ptr(), self.capacity()) }
    }

    /// Returns a mutable slice of the vector up to a certain length
    pub fn as_mut_slice_with_len<T>(&mut self, len: usize) -> &mut [T] {
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

    /// Copy data to the vector.
    pub fn copy<T: Copy>(&mut self, data: &[T]) {
        assert!(data.len() <= self.capacity());
        self.as_mut_slice::<T>()[0..data.len()].copy_from_slice(data);
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

/// A list vector borrowed from a [`DataChunkHandle`][crate::core::DataChunkHandle].
///
/// Stores list entry offsets and lengths; elements live in a separate child vector.
///
/// Regression guard for #673 — a `ListVector` must not outlive its parent chunk:
///
/// ```compile_fail
/// use duckdb::core::{DataChunkHandle, LogicalTypeHandle, LogicalTypeId};
///
/// let vec;
/// {
///     let list_type = LogicalTypeHandle::list(&LogicalTypeId::Integer.into());
///     let chunk = DataChunkHandle::new(&[list_type]);
///     chunk.set_len(1);
///     let v = chunk.list_vector(0);
///     vec = v;
/// }
/// // chunk goes out of scope here; borrow checking rejects the outer use.
/// let _ = vec.get_entry(0);
/// ```
pub struct ListVector<'a> {
    /// ListVector does not own the vector pointer.
    entries: FlatVector<'a>,
}

impl<'a> ListVector<'a> {
    /// Wrap a raw `duckdb_vector` pointer.
    ///
    /// # Safety
    /// `ptr` must be a valid `duckdb_vector` that remains valid for all of `'a`.
    pub(crate) unsafe fn from_raw(ptr: duckdb_vector) -> Self {
        Self {
            entries: unsafe { FlatVector::from_raw(ptr) },
        }
    }

    /// Returns the number of entries in the list vector.
    pub fn len(&self) -> usize {
        unsafe { duckdb_list_vector_get_size(self.entries.ptr) as usize }
    }

    /// Returns true if the list vector is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the child vector.
    // TODO: not ideal interface. Where should we keep capacity.
    pub fn child(&self, capacity: usize) -> FlatVector<'a> {
        self.reserve(capacity);
        FlatVector::with_capacity(unsafe { duckdb_list_vector_get_child(self.entries.ptr) }, capacity)
    }

    /// Take the child as [StructVector].
    pub fn struct_child(&self, capacity: usize) -> StructVector<'a> {
        self.reserve(capacity);
        unsafe { StructVector::from_raw(duckdb_list_vector_get_child(self.entries.ptr)) }
    }

    /// Take the child as [ArrayVector].
    pub fn array_child(&self) -> ArrayVector<'a> {
        unsafe { ArrayVector::from_raw(duckdb_list_vector_get_child(self.entries.ptr)) }
    }

    /// Take the child as [ListVector].
    pub fn list_child(&self) -> ListVector<'a> {
        unsafe { ListVector::from_raw(duckdb_list_vector_get_child(self.entries.ptr)) }
    }

    /// Set primitive data to the child node.
    pub fn set_child<T: Copy>(&self, data: &[T]) {
        self.child(data.len()).copy(data);
        self.set_len(data.len());
    }

    /// Set offset and length to the entry.
    pub fn set_entry(&mut self, idx: usize, offset: usize, length: usize) {
        self.entries.as_mut_slice::<duckdb_list_entry>()[idx].offset = offset as u64;
        self.entries.as_mut_slice::<duckdb_list_entry>()[idx].length = length as u64;
    }

    /// Get offset and length for the entry at index.
    pub fn get_entry(&self, idx: usize) -> (usize, usize) {
        let entry = self.entries.as_slice::<duckdb_list_entry>()[idx];
        (entry.offset as usize, entry.length as usize)
    }

    /// Set row as null
    pub fn set_null(&mut self, row: usize) {
        unsafe {
            duckdb_vector_ensure_validity_writable(self.entries.ptr);
            let idx = duckdb_vector_get_validity(self.entries.ptr);
            duckdb_validity_set_row_invalid(idx, row as u64);
        }
    }

    /// Reserve the capacity for its child node.
    fn reserve(&self, capacity: usize) {
        unsafe {
            duckdb_list_vector_reserve(self.entries.ptr, capacity as u64);
        }
    }

    /// Set the length of the list vector.
    pub fn set_len(&self, new_len: usize) {
        unsafe {
            duckdb_list_vector_set_size(self.entries.ptr, new_len as u64);
        }
    }
}

/// A fixed-size list vector borrowed from a
/// [`DataChunkHandle`][crate::core::DataChunkHandle].
///
/// Exposes a fixed-width list whose child storage is contiguous across all rows.
pub struct ArrayVector<'a> {
    ptr: duckdb_vector,
    _phantom: PhantomData<&'a ()>,
}

impl<'a> ArrayVector<'a> {
    /// Wrap a raw `duckdb_vector` pointer.
    ///
    /// # Safety
    /// `ptr` must be a valid `duckdb_vector` that remains valid for all of `'a`.
    pub(crate) unsafe fn from_raw(ptr: duckdb_vector) -> Self {
        Self {
            ptr,
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

    /// Returns the child vector.
    /// capacity should be a multiple of the array size.
    // TODO: not ideal interface. Where should we keep count.
    pub fn child(&self, capacity: usize) -> FlatVector<'a> {
        FlatVector::with_capacity(unsafe { duckdb_array_vector_get_child(self.ptr) }, capacity)
    }

    /// Set primitive data to the child node.
    pub fn set_child<T: Copy>(&self, data: &[T]) {
        self.child(data.len()).copy(data);
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

/// A struct vector borrowed from a [`DataChunkHandle`][crate::core::DataChunkHandle].
///
/// Groups one child vector per struct field, all sharing the same row count.
pub struct StructVector<'a> {
    ptr: duckdb_vector,
    _phantom: PhantomData<&'a ()>,
}

impl<'a> StructVector<'a> {
    /// Wrap a raw `duckdb_vector` pointer.
    ///
    /// # Safety
    /// `ptr` must be a valid `duckdb_vector` that remains valid for all of `'a`.
    pub(crate) unsafe fn from_raw(ptr: duckdb_vector) -> Self {
        Self {
            ptr,
            _phantom: PhantomData,
        }
    }

    /// Returns the child by idx in the list vector.
    pub fn child(&self, idx: usize, capacity: usize) -> FlatVector<'a> {
        FlatVector::with_capacity(
            unsafe { duckdb_struct_vector_get_child(self.ptr, idx as u64) },
            capacity,
        )
    }

    /// Take the child as [StructVector].
    pub fn struct_vector_child(&self, idx: usize) -> StructVector<'a> {
        unsafe { StructVector::from_raw(duckdb_struct_vector_get_child(self.ptr, idx as u64)) }
    }

    /// Take the child as [ListVector].
    pub fn list_vector_child(&self, idx: usize) -> ListVector<'a> {
        unsafe { ListVector::from_raw(duckdb_struct_vector_get_child(self.ptr, idx as u64)) }
    }

    /// Take the child as [ArrayVector].
    pub fn array_vector_child(&self, idx: usize) -> ArrayVector<'a> {
        unsafe { ArrayVector::from_raw(duckdb_struct_vector_get_child(self.ptr, idx as u64)) }
    }

    /// Get the logical type of this struct vector.
    pub fn logical_type(&self) -> LogicalTypeHandle {
        unsafe { LogicalTypeHandle::new(duckdb_vector_get_column_type(self.ptr)) }
    }

    /// Get the name of the child by idx.
    pub fn child_name(&self, idx: usize) -> DuckDbString {
        let logical_type = self.logical_type();
        unsafe {
            let child_name_ptr = duckdb_struct_type_child_name(logical_type.ptr, idx as u64);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{DataChunkHandle, LogicalTypeHandle, LogicalTypeId};
    use std::ffi::CString;

    #[test]
    fn test_insert_string_values() {
        let chunk = DataChunkHandle::new(&[LogicalTypeId::Varchar.into()]);
        let vector = chunk.flat_vector(0);
        chunk.set_len(3);

        vector.insert(0, "first");
        vector.insert(1, &String::from("second"));
        let cstring = CString::new("third").unwrap();
        vector.insert(2, cstring);
    }

    #[test]
    fn test_insert_byte_values() {
        let chunk = DataChunkHandle::new(&[LogicalTypeId::Blob.into()]);
        let vector = chunk.flat_vector(0);
        chunk.set_len(2);

        vector.insert(0, b"hello world".as_slice());
        vector.insert(
            1,
            &vec![0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x20, 0x77, 0x6f, 0x72, 0x6c, 0x64],
        );
    }

    #[test]
    fn test_list_vector_get_entry() {
        let list_type = LogicalTypeHandle::list(&LogicalTypeId::Integer.into());
        let chunk = DataChunkHandle::new(&[list_type]);
        chunk.set_len(3);

        let mut list_vector = chunk.list_vector(0);

        list_vector.set_entry(0, 0, 2);
        list_vector.set_entry(1, 2, 1);
        list_vector.set_entry(2, 3, 2);

        assert_eq!(list_vector.get_entry(0), (0, 2));
        assert_eq!(list_vector.get_entry(1), (2, 1));
        assert_eq!(list_vector.get_entry(2), (3, 2));
    }
}

//! Borrowed vector wrappers over [`duckdb_vector`].
//!
//! Each wrapper type carries a lifetime `'a`. When obtained via a
//! [`DataChunkHandle`][crate::core::DataChunkHandle] accessor, `'a` is
//! bound to the chunk so the wrapper cannot outlive it. When built via
//! one of the `unsafe fn from_raw` constructors (including the raw
//! `duckdb_vector` path used by the Arrow interop layer), `'a` is
//! caller-chosen and must not exceed the DuckDB vector's actual validity —
//! that path does not track liveness in the type system.

use std::{ffi::CString, marker::PhantomData, slice};

use libduckdb_sys::{
    DuckDbString, duckdb_array_type_array_size, duckdb_array_vector_get_child, duckdb_validity_row_is_valid,
};

use super::LogicalTypeHandle;
use crate::{
    Result,
    error::duckdb_failure_from_message,
    ffi::{
        DuckDBSuccess, duckdb_list_entry, duckdb_list_vector_get_child, duckdb_list_vector_get_size,
        duckdb_list_vector_reserve, duckdb_list_vector_set_size, duckdb_state, duckdb_struct_type_child_count,
        duckdb_struct_type_child_name, duckdb_struct_vector_get_child, duckdb_validity_set_row_invalid, duckdb_vector,
        duckdb_vector_assign_string_element, duckdb_vector_assign_string_element_len,
        duckdb_vector_ensure_validity_writable, duckdb_vector_get_column_type, duckdb_vector_get_data,
        duckdb_vector_get_validity, duckdb_vector_size,
    },
};

fn list_vector_state_result(state: duckdb_state, action: &str, size: usize) -> Result<()> {
    if state == DuckDBSuccess {
        Ok(())
    } else {
        Err(duckdb_failure_from_message(format!(
            "failed to {action} {size} elements in DuckDB list vector"
        )))
    }
}

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

    /// Wrap a raw vector pointer with a caller-supplied element capacity.
    ///
    /// # Safety
    /// `ptr` must be a valid `duckdb_vector` that remains valid for all of `'a`.
    /// The caller must ensure `capacity` matches the backing allocation for the
    /// logical view being exposed. Later typed slice and copy methods trust this
    /// value for bounds checks.
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

    /// Wrap a raw list vector pointer with a caller-supplied capacity.
    ///
    /// # Safety
    /// `ptr` must be a valid list `duckdb_vector` that remains valid for all of
    /// `'a`, and `capacity` must not exceed the backing allocation available for
    /// this vector's list entries.
    #[cfg(feature = "vtab-arrow")]
    pub(crate) unsafe fn from_raw_with_capacity(ptr: duckdb_vector, capacity: usize) -> Self {
        Self {
            entries: unsafe { FlatVector::with_capacity(ptr, capacity) },
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

    /// Returns the child vector, reserving space for `capacity` child values.
    ///
    /// # Panics
    /// Panics if DuckDB rejects the reserve.
    // TODO: not ideal interface. Where should we keep capacity.
    pub fn child(&self, capacity: usize) -> FlatVector<'a> {
        let ptr = self
            .try_child_ptr_with_capacity(capacity)
            .unwrap_or_else(|err| panic!("{err}"));
        // SAFETY: DuckDB accepted the reserve for `capacity` child values.
        unsafe { FlatVector::with_capacity(ptr, capacity) }
    }

    /// Take the child as [StructVector], reserving space for `capacity` child
    /// values.
    ///
    /// # Panics
    /// Panics if DuckDB rejects the reserve.
    pub fn struct_child(&self, capacity: usize) -> StructVector<'a> {
        let ptr = self
            .try_child_ptr_with_capacity(capacity)
            .unwrap_or_else(|err| panic!("{err}"));
        // SAFETY: DuckDB accepted the reserve for `capacity` child values.
        unsafe { StructVector::from_raw(ptr) }
    }

    /// Take the child as [ArrayVector].
    pub fn array_child(&self) -> ArrayVector<'a> {
        unsafe { ArrayVector::from_raw(duckdb_list_vector_get_child(self.entries.ptr)) }
    }

    /// Take the child as [ListVector].
    pub fn list_child(&self) -> ListVector<'a> {
        unsafe { ListVector::from_raw(duckdb_list_vector_get_child(self.entries.ptr)) }
    }

    pub(crate) fn try_child_ptr_with_capacity(&self, capacity: usize) -> Result<duckdb_vector> {
        self.try_reserve(capacity)?;
        Ok(unsafe { duckdb_list_vector_get_child(self.entries.ptr) })
    }

    /// Set primitive data to the child node.
    ///
    /// Reserves child storage and commits the list length to `data.len()`.
    ///
    /// # Panics
    /// Panics if DuckDB rejects the reserve or size update.
    ///
    /// # Safety
    /// The caller must ensure `T` matches the child vector's physical storage
    /// and no other references to the same storage exist during the copy.
    pub unsafe fn set_child<T: Copy>(&self, data: &[T]) {
        unsafe { self.child(data.len()).copy(data) };
        self.set_len(data.len());
    }

    /// Set offset and length to the entry.
    pub fn set_entry(&mut self, idx: usize, offset: usize, length: usize) {
        let entries = unsafe { self.entries.as_mut_slice::<duckdb_list_entry>() };
        entries[idx].offset = offset as u64;
        entries[idx].length = length as u64;
    }

    /// Get offset and length for the entry at index.
    pub fn get_entry(&self, idx: usize) -> (usize, usize) {
        let entry = (unsafe { self.entries.as_slice::<duckdb_list_entry>() })[idx];
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

    /// Reserve space for `capacity` child values.
    ///
    /// This is public so callers can surface DuckDB allocation errors without
    /// using the panic-based convenience methods.
    pub fn try_reserve(&self, capacity: usize) -> Result<()> {
        let state = unsafe { duckdb_list_vector_reserve(self.entries.ptr, capacity as u64) };
        list_vector_state_result(state, "reserve child storage for", capacity)
    }

    /// Set the length of the list vector.
    ///
    /// Panics if DuckDB rejects the size update. Use [`Self::try_set_len`] to
    /// handle that error explicitly.
    pub fn set_len(&self, new_len: usize) {
        self.try_set_len(new_len).unwrap_or_else(|err| panic!("{err}"));
    }

    /// Set the length of the list vector.
    pub fn try_set_len(&self, new_len: usize) -> Result<()> {
        let state = unsafe { duckdb_list_vector_set_size(self.entries.ptr, new_len as u64) };
        list_vector_state_result(state, "set size to", new_len)
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
    ///
    /// # Panics
    /// Panics if `capacity` is not a multiple of the fixed array size.
    // TODO: not ideal interface. Where should we keep count.
    pub fn child(&self, capacity: usize) -> FlatVector<'a> {
        let array_size = self.get_array_size() as usize;
        assert_eq!(
            capacity % array_size,
            0,
            "array child capacity must be a multiple of the fixed array size"
        );
        // SAFETY: callers must pass a capacity covered by the array child
        // backing allocation. The multiple-of-array-size assertion checks only
        // fixed-size-list shape, not allocation size.
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

    /// Returns the child by idx in the struct vector.
    pub fn child(&self, idx: usize, capacity: usize) -> FlatVector<'a> {
        // SAFETY: struct children share the parent allocation, and callers pass
        // the row capacity for the child view they are exposing.
        unsafe { FlatVector::with_capacity(self.child_ptr(idx), capacity) }
    }

    // Legacy typed child accessors remain public for callers that need nested
    // vector views.

    /// Take the child as [StructVector].
    pub fn struct_vector_child(&self, idx: usize) -> StructVector<'a> {
        unsafe { StructVector::from_raw(self.child_ptr(idx)) }
    }

    /// Take the child as [ListVector].
    pub fn list_vector_child(&self, idx: usize) -> ListVector<'a> {
        unsafe { ListVector::from_raw(self.child_ptr(idx)) }
    }

    /// Take the child as [ArrayVector].
    pub fn array_vector_child(&self, idx: usize) -> ArrayVector<'a> {
        unsafe { ArrayVector::from_raw(self.child_ptr(idx)) }
    }

    pub(crate) fn child_ptr(&self, idx: usize) -> duckdb_vector {
        unsafe { duckdb_struct_vector_get_child(self.ptr, idx as u64) }
    }

    /// Get the logical type of this struct vector.
    pub fn logical_type(&self) -> LogicalTypeHandle {
        unsafe { LogicalTypeHandle::new(duckdb_vector_get_column_type(self.ptr)) }
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

    #[test]
    fn test_struct_vector_typed_child_accessors_smoke() {
        let int_type = LogicalTypeHandle::from(LogicalTypeId::Integer);
        let list_type = LogicalTypeHandle::list(&int_type);
        let array_type = LogicalTypeHandle::array(&int_type, 2);
        let nested_struct_type =
            LogicalTypeHandle::struct_type(&[("value", LogicalTypeHandle::from(LogicalTypeId::Integer))]);
        let struct_type = LogicalTypeHandle::struct_type(&[
            ("items", list_type),
            ("pair", array_type),
            ("nested", nested_struct_type),
        ]);
        let chunk = DataChunkHandle::new(&[struct_type]);
        let vector = chunk.struct_vector(0);

        let list_child = vector.list_vector_child(0);
        assert!(list_child.is_empty());
        let array_child = vector.array_vector_child(1);
        assert_eq!(array_child.get_array_size(), 2);
        let struct_child = vector.struct_vector_child(2);
        assert_eq!(struct_child.num_children(), 1);
        assert_eq!(struct_child.child_name(0).to_str().unwrap(), "value");

        let flat_child = vector.child(1, 2);
        assert_eq!(flat_child.capacity(), 2);

        let child_count = unsafe { duckdb_vector_size() as usize } + 1;
        let struct_type = LogicalTypeHandle::struct_type(&[("value", LogicalTypeHandle::from(LogicalTypeId::Integer))]);
        let list_type = LogicalTypeHandle::list(&struct_type);
        let chunk = DataChunkHandle::new(&[list_type]);
        let list = chunk.list_vector(0);
        let child = list.struct_child(child_count);
        assert_eq!(child.num_children(), 1);
    }

    #[test]
    fn test_array_vector_set_child_uses_reserved_nested_capacity() -> Result<()> {
        let item_type = LogicalTypeHandle::array(&LogicalTypeId::Integer.into(), 2);
        let list_type = LogicalTypeHandle::list(&item_type);
        let chunk = DataChunkHandle::new(&[list_type]);
        let list = chunk.list_vector(0);
        let child_count = unsafe { duckdb_vector_size() as usize } + 1;
        list.try_reserve(child_count)?;

        let array = list.array_child();
        let data = (0..(child_count * 2) as i32).collect::<Vec<_>>();
        unsafe { array.set_child(&data) };
        let child = array.child(data.len());
        let output = unsafe { child.as_slice_with_len::<i32>(data.len()) };

        assert_eq!(output[data.len() - 1], data[data.len() - 1]);

        Ok(())
    }
}

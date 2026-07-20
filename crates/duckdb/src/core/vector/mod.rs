//! Borrowed vector wrappers over [`duckdb_vector`].
//!
//! Each wrapper type carries a lifetime `'a`. When obtained via a
//! [`DataChunkHandle`][crate::core::DataChunkHandle] accessor, `'a` is
//! bound to the chunk so the wrapper cannot outlive it. When built via
//! one of the `unsafe fn from_raw` constructors (including the raw
//! `duckdb_vector` path used by the Arrow interop layer), `'a` is
//! caller-chosen and must not exceed the DuckDB vector's actual validity —
//! that path does not track liveness in the type system.

use std::{cell::Cell, ffi::CString, marker::PhantomData, slice};

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

/// Mirrors DuckDB's `DConstants::MAX_VECTOR_SIZE`, the largest allocation a
/// single vector can hold (not the per-chunk row count reported by
/// `duckdb_vector_size()`). `ListVector::Reserve` throws a C++ exception for
/// larger requests, and `duckdb_list_vector_reserve` does not catch it, so the
/// exception would cross the non-unwinding C boundary and abort the process.
/// Rejecting such requests up front keeps them recoverable errors.
const MAX_VECTOR_SIZE: u64 = 1 << 37;

fn list_vector_state_result(state: duckdb_state, action: &str, size: usize) -> Result<()> {
    if state == DuckDBSuccess {
        Ok(())
    } else {
        Err(duckdb_failure_from_message(format!(
            "failed to {action} {size} elements in DuckDB list vector"
        )))
    }
}

fn try_vector_row_is_null(ptr: duckdb_vector, row: u64, capacity: usize) -> Result<bool> {
    let row_index = usize::try_from(row)
        .map_err(|_| duckdb_failure_from_message(format!("row index {row} exceeds usize range")))?;
    if row_index >= capacity {
        return Err(duckdb_failure_from_message(format!(
            "row index {row} exceeds vector capacity {capacity}"
        )));
    }
    let valid = unsafe {
        let validity = duckdb_vector_get_validity(ptr);

        // validity can return a NULL pointer if the entire vector is valid
        if validity.is_null() {
            return Ok(false);
        }

        duckdb_validity_row_is_valid(validity, row)
    };

    Ok(!valid)
}

fn vector_row_is_null(ptr: duckdb_vector, row: u64, capacity: usize) -> bool {
    try_vector_row_is_null(ptr, row, capacity).unwrap_or_else(|err| panic!("{err}"))
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
    reserved_child_capacity: Cell<usize>,
}

impl<'a> ListVector<'a> {
    /// Wrap a raw `duckdb_vector` pointer.
    ///
    /// # Safety
    /// `ptr` must be a valid `duckdb_vector` that remains valid for all of `'a`.
    pub(crate) unsafe fn from_raw(ptr: duckdb_vector) -> Self {
        Self {
            entries: unsafe { FlatVector::from_raw(ptr) },
            reserved_child_capacity: Cell::new(0),
        }
    }

    /// Wrap a raw list vector pointer with a caller-supplied capacity.
    ///
    /// # Safety
    /// `ptr` must be a valid list `duckdb_vector` that remains valid for all of
    /// `'a`, and `capacity` must not exceed the backing allocation available for
    /// this vector's list entries. Any committed child size reported by DuckDB
    /// must also be covered by the child allocation, as it is when the size was
    /// committed through this wrapper or produced by DuckDB.
    pub(crate) unsafe fn from_raw_with_capacity(ptr: duckdb_vector, capacity: usize) -> Self {
        Self {
            entries: unsafe { FlatVector::with_capacity(ptr, capacity) },
            reserved_child_capacity: Cell::new(0),
        }
    }

    /// Returns the number of child elements stored by the list vector.
    ///
    /// This is the upper bound for the end of non-empty list entries, not the
    /// number of parent list rows.
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
        unsafe { StructVector::from_raw_with_capacity(ptr, capacity) }
    }

    /// Capacity of the child vector's backing storage.
    ///
    /// DuckDB initializes list children with standard vector capacity. This
    /// wrapper tracks larger explicit reservations, and larger committed sizes
    /// must already have been reserved; the safe size setters and
    /// DuckDB-produced chunks preserve that invariant.
    fn child_capacity(&self) -> usize {
        self.len()
            .max(self.reserved_child_capacity.get())
            .max(unsafe { duckdb_vector_size() as usize })
    }

    /// Take the child as [ArrayVector].
    pub fn array_child(&self) -> ArrayVector<'a> {
        // SAFETY: `child_capacity` is covered by the child's backing storage.
        unsafe {
            ArrayVector::from_raw_with_capacity(duckdb_list_vector_get_child(self.entries.ptr), self.child_capacity())
        }
    }

    /// Take the child as [ListVector].
    pub fn list_child(&self) -> ListVector<'a> {
        // SAFETY: `child_capacity` is covered by the child's backing storage.
        unsafe {
            ListVector::from_raw_with_capacity(duckdb_list_vector_get_child(self.entries.ptr), self.child_capacity())
        }
    }

    pub(crate) fn try_child_ptr_with_capacity(&self, capacity: usize) -> Result<duckdb_vector> {
        self.try_reserve(capacity)?;
        Ok(unsafe { duckdb_list_vector_get_child(self.entries.ptr) })
    }

    #[cfg(feature = "vtab-arrow")]
    pub(crate) fn child_ptr(&self) -> duckdb_vector {
        unsafe { duckdb_list_vector_get_child(self.entries.ptr) }
    }

    /// Returns true if the row at the given index is null.
    ///
    /// # Panics
    /// Panics if `row` is outside the vector capacity.
    pub fn row_is_null(&self, row: u64) -> bool {
        self.entries.row_is_null(row)
    }

    /// Returns whether the row is null, or an error if it is out of range.
    pub fn try_row_is_null(&self, row: u64) -> Result<bool> {
        self.entries.try_row_is_null(row)
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
    ///
    /// # Panics
    /// Panics if `idx` is outside the vector capacity.
    pub fn set_entry(&mut self, idx: usize, offset: usize, length: usize) {
        assert!(
            idx < self.entries.capacity(),
            "list entry row {idx} exceeds vector capacity {}",
            self.entries.capacity()
        );
        // SAFETY: the bounds check above keeps the write inside the entry
        // allocation, and a single-slot write avoids materializing a slice
        // over neighboring entries DuckDB may have left uninitialized.
        unsafe {
            self.entries
                .as_mut_ptr::<duckdb_list_entry>()
                .add(idx)
                .write(duckdb_list_entry {
                    offset: offset as u64,
                    length: length as u64,
                });
        }
    }

    /// Get offset and length for the entry at index.
    ///
    /// # Panics
    /// Panics if the entry is out of range or its offset or length cannot fit
    /// in `usize` on the current target.
    pub fn get_entry(&self, idx: usize) -> (usize, usize) {
        self.try_get_entry(idx).unwrap_or_else(|err| panic!("{err}"))
    }

    /// Returns the entry offset and length, or an error if the entry is out of
    /// range or cannot be represented on the current target.
    pub fn try_get_entry(&self, idx: usize) -> Result<(usize, usize)> {
        if idx >= self.entries.capacity() {
            return Err(duckdb_failure_from_message(format!(
                "list entry row {idx} exceeds vector capacity {}",
                self.entries.capacity()
            )));
        }
        // SAFETY: the bounds check above keeps the read inside the entry
        // allocation, and a single-slot read avoids materializing a slice
        // over neighboring entries DuckDB may have left uninitialized.
        let entry = unsafe { self.entries.as_mut_ptr::<duckdb_list_entry>().add(idx).read() };
        let offset = usize::try_from(entry.offset).map_err(|_| {
            duckdb_failure_from_message(format!(
                "DuckDB list entry offset {} at row {idx} exceeds usize range",
                entry.offset
            ))
        })?;
        let length = usize::try_from(entry.length).map_err(|_| {
            duckdb_failure_from_message(format!(
                "DuckDB list entry length {} at row {idx} exceeds usize range",
                entry.length
            ))
        })?;
        Ok((offset, length))
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
    /// This is public so callers can handle reservation errors without the
    /// panic-based convenience methods. Requests beyond DuckDB's maximum
    /// vector size (`2^37` elements) are rejected here as `Err`, because
    /// DuckDB reports them with a C++ exception the C API does not catch.
    /// Allocation failure inside DuckDB escapes the same way and aborts the
    /// process; it cannot be returned as an error through the current C API.
    pub fn try_reserve(&self, capacity: usize) -> Result<()> {
        if capacity as u64 > MAX_VECTOR_SIZE {
            return Err(duckdb_failure_from_message(format!(
                "cannot reserve {capacity} elements in DuckDB list vector: exceeds maximum vector size {MAX_VECTOR_SIZE}"
            )));
        }
        let state = unsafe { duckdb_list_vector_reserve(self.entries.ptr, capacity as u64) };
        list_vector_state_result(state, "reserve child storage for", capacity)?;
        self.reserved_child_capacity
            .set(self.reserved_child_capacity.get().max(capacity));
        Ok(())
    }

    /// Reserve child storage and set the length of the list vector.
    ///
    /// Panics if DuckDB rejects the reserve or size update. Use
    /// [`Self::try_set_len`] to handle that error explicitly.
    pub fn set_len(&self, new_len: usize) {
        self.try_set_len(new_len).unwrap_or_else(|err| panic!("{err}"));
    }

    /// Reserve child storage and set the length of the list vector.
    pub fn try_set_len(&self, new_len: usize) -> Result<()> {
        self.try_reserve(new_len)?;
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
    #[should_panic(expected = "exceeds vector capacity")]
    fn test_flat_vector_row_is_null_checks_capacity() {
        let chunk = DataChunkHandle::new(&[LogicalTypeId::Integer.into()]);
        let vector = chunk.flat_vector(0);

        vector.row_is_null(unsafe { duckdb_vector_size() });
    }

    #[test]
    fn test_vector_try_row_is_null_rejects_row_beyond_capacity() {
        let row = unsafe { duckdb_vector_size() };
        let chunk = DataChunkHandle::new(&[
            LogicalTypeId::Integer.into(),
            LogicalTypeHandle::list(&LogicalTypeId::Integer.into()),
            LogicalTypeHandle::array(&LogicalTypeId::Integer.into(), 2),
            LogicalTypeHandle::struct_type(&[("value", LogicalTypeId::Integer.into())]),
        ]);

        assert!(chunk.flat_vector(0).try_row_is_null(row).is_err());
        assert!(chunk.list_vector(1).try_row_is_null(row).is_err());
        assert!(chunk.array_vector(2).try_row_is_null(row).is_err());
        assert!(chunk.struct_vector(3).try_row_is_null(row).is_err());
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
    #[should_panic(expected = "exceeds parent capacity")]
    fn test_struct_vector_child_rejects_capacity_beyond_parent() {
        let struct_type = LogicalTypeHandle::struct_type(&[("value", LogicalTypeHandle::from(LogicalTypeId::Integer))]);
        let chunk = DataChunkHandle::new(&[struct_type]);
        let vector = chunk.struct_vector(0);
        let capacity = unsafe { duckdb_vector_size() as usize } + 1;

        vector.child(0, capacity);
    }

    #[test]
    #[should_panic(expected = "exceeds backing capacity")]
    fn test_array_vector_child_rejects_capacity_beyond_parent() {
        let array_type = LogicalTypeHandle::array(&LogicalTypeId::Integer.into(), 2);
        let chunk = DataChunkHandle::new(&[array_type]);
        let vector = chunk.array_vector(0);
        let capacity = (unsafe { duckdb_vector_size() as usize } + 1) * 2;

        vector.child(capacity);
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

    #[test]
    fn test_list_vector_set_len_reserves_list_child_storage() {
        let child_type = LogicalTypeHandle::list(&LogicalTypeId::Integer.into());
        let list_type = LogicalTypeHandle::list(&child_type);
        let chunk = DataChunkHandle::new(&[list_type]);
        let list = chunk.list_vector(0);
        let child_count = unsafe { duckdb_vector_size() as usize } + 1;
        list.set_len(child_count);

        let mut child = list.list_child();
        child.set_entry(child_count - 1, 0, 0);

        assert_eq!(child.get_entry(child_count - 1), (0, 0));
        assert!(!child.row_is_null((child_count - 1) as u64));
    }

    #[test]
    fn test_list_vector_set_len_reserves_array_child_storage() {
        let child_type = LogicalTypeHandle::array(&LogicalTypeId::Integer.into(), 2);
        let list_type = LogicalTypeHandle::list(&child_type);
        let chunk = DataChunkHandle::new(&[list_type]);
        let list = chunk.list_vector(0);
        let child_count = unsafe { duckdb_vector_size() as usize } + 1;
        list.set_len(child_count);

        let child = list.array_child();

        assert!(!child.row_is_null((child_count - 1) as u64));
    }

    #[test]
    fn test_list_vector_try_get_entry_rejects_row_beyond_capacity() {
        let list_type = LogicalTypeHandle::list(&LogicalTypeId::Integer.into());
        let chunk = DataChunkHandle::new(&[list_type]);
        let list = chunk.list_vector(0);
        let row = unsafe { duckdb_vector_size() as usize };

        let err = list.try_get_entry(row).unwrap_err();

        assert_eq!(
            err.to_string(),
            format!("list entry row {row} exceeds vector capacity {row}")
        );
    }

    #[test]
    #[cfg(target_pointer_width = "64")]
    fn test_list_vector_try_set_len_rejects_oversized_capacity() {
        let list_type = LogicalTypeHandle::list(&LogicalTypeId::Integer.into());
        let chunk = DataChunkHandle::new(&[list_type]);
        let list = chunk.list_vector(0);
        let oversized = MAX_VECTOR_SIZE as usize + 1;

        let err = list.try_set_len(oversized).unwrap_err();

        assert_eq!(
            err.to_string(),
            format!(
                "cannot reserve {oversized} elements in DuckDB list vector: exceeds maximum vector size {MAX_VECTOR_SIZE}"
            )
        );
    }

    #[test]
    fn test_list_vector_list_child_retains_default_capacity_before_len() {
        let child_type = LogicalTypeHandle::list(&LogicalTypeId::Integer.into());
        let list_type = LogicalTypeHandle::list(&child_type);
        let chunk = DataChunkHandle::new(&[list_type]);
        let list = chunk.list_vector(0);

        let mut child = list.list_child();
        child.set_entry(0, 0, 0);

        assert_eq!(child.get_entry(0), (0, 0));
    }

    #[test]
    fn test_list_vector_array_child_retains_default_capacity_before_len() {
        let child_type = LogicalTypeHandle::array(&LogicalTypeId::Integer.into(), 2);
        let list_type = LogicalTypeHandle::list(&child_type);
        let chunk = DataChunkHandle::new(&[list_type]);
        let list = chunk.list_vector(0);

        let child = list.array_child();

        assert!(!child.row_is_null(0));
    }
}

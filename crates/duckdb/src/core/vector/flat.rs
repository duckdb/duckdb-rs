use std::ffi::CString;

use super::{ResultExt, VectorRef};
use crate::{Result, core::LogicalTypeHandle, ffi::duckdb_vector};

/// A flat vector borrowed from a [`DataChunkHandle`](crate::core::DataChunkHandle).
pub struct FlatVector<'a> {
    pub(super) vector: VectorRef<'a>,
}

impl<'a> FlatVector<'a> {
    pub(in crate::core) fn from_vector(vector: VectorRef<'a>) -> Result<Self> {
        vector.expect_flat()?;
        Ok(Self { vector })
    }

    /// Returns this vector's explicit effective capacity.
    pub fn capacity(&self) -> usize {
        self.vector.capacity()
    }

    pub(crate) fn ptr(&self) -> duckdb_vector {
        self.vector.ptr()
    }

    /// Returns true if the row is null.
    ///
    /// # Panics
    /// Panics if `row` is outside the vector capacity.
    pub fn row_is_null(&self, row: u64) -> bool {
        self.vector.row_is_null(row)
    }

    /// Returns whether the row is null, or an error if it is out of range.
    pub fn try_row_is_null(&self, row: u64) -> Result<bool> {
        self.vector.try_row_is_null(row)
    }

    /// Returns a mutable pointer to the vector's backing data cast to `T`.
    ///
    /// # Safety
    /// `T` must match DuckDB's physical storage and the caller must initialize
    /// every slot before it is read.
    pub unsafe fn as_mut_ptr<T>(&mut self) -> *mut T {
        self.vector.data_mut_ptr().or_panic()
    }

    /// Returns an initialized typed slice spanning the vector capacity.
    ///
    /// # Safety
    /// `T` must match DuckDB's physical storage, every returned slot (including
    /// null rows) must contain a valid initialized `T`, and no mutable or
    /// incompatible references to the storage may exist.
    ///
    /// # Panics
    ///
    /// Panics while the payload remains under construction. Chunk-backed
    /// writers can finish their views and call
    /// [`DataChunkHandle::assume_initialized`](crate::core::DataChunkHandle::assume_initialized);
    /// raw writable adapters must be dropped and read through an initialized
    /// owner instead.
    pub unsafe fn as_slice<T>(&self) -> &[T] {
        unsafe { self.vector.as_slice(self.capacity()) }.or_panic()
    }

    /// Returns an initialized typed slice of `len` elements.
    ///
    /// # Safety
    /// `T` must match DuckDB's physical storage, every returned slot (including
    /// null rows) must contain a valid initialized `T`, and no mutable or
    /// incompatible references to the storage may exist.
    ///
    /// # Panics
    ///
    /// Panics if `len` exceeds the vector capacity or while the payload remains
    /// under construction. Chunk-backed writers can finish their views and
    /// call [`DataChunkHandle::assume_initialized`](crate::core::DataChunkHandle::assume_initialized);
    /// raw writable adapters must be dropped and read through an initialized
    /// owner instead.
    pub unsafe fn as_slice_with_len<T>(&self, len: usize) -> &[T] {
        unsafe { self.vector.as_slice(len) }.or_panic()
    }

    /// Returns a typed mutable slice spanning the vector capacity.
    ///
    /// # Safety
    /// `T` must match DuckDB's physical storage, the returned span must already
    /// contain valid `T` values, and no other references to that storage may
    /// exist. Use a typed copy/write interface to initialize new storage.
    pub unsafe fn as_mut_slice<T>(&mut self) -> &mut [T] {
        let capacity = self.capacity();
        unsafe { self.vector.as_mut_slice(capacity) }.or_panic()
    }

    /// Returns a typed mutable slice of `len` elements.
    ///
    /// # Safety
    /// `T` must match DuckDB's physical storage, the returned span must already
    /// contain valid `T` values, and no other references to that storage may
    /// exist. Use a typed copy/write interface to initialize new storage.
    ///
    /// # Panics
    ///
    /// Panics if `len` exceeds the vector capacity.
    pub unsafe fn as_mut_slice_with_len<T>(&mut self, len: usize) -> &mut [T] {
        unsafe { self.vector.as_mut_slice(len) }.or_panic()
    }

    /// Returns the logical type of the vector.
    pub fn logical_type(&self) -> LogicalTypeHandle {
        self.vector.logical_type_owned().or_panic()
    }

    /// Marks one row null.
    ///
    /// # Panics
    /// Panics if `row` is outside the vector capacity.
    pub fn set_null(&mut self, row: usize) {
        self.vector.set_null(row);
    }

    /// Copies typed data into the vector.
    ///
    /// # Safety
    /// `T` must match DuckDB's physical storage.
    ///
    /// # Panics
    /// Panics if `data` exceeds the effective capacity.
    pub unsafe fn copy<T: Copy>(&mut self, data: &[T]) {
        self.vector.ensure_writable().or_panic();
        self.vector.check_slice_len(data.len()).or_panic();
        if data.is_empty() {
            return;
        }
        let out = self.vector.data_mut_ptr::<T>().or_panic();
        // SAFETY: the bounds check proves the destination span and `copy`'s
        // contract requires `T` to match the physical storage.
        unsafe { std::ptr::copy_nonoverlapping(data.as_ptr(), out, data.len()) };
    }

    /// Writes one physical value at `row` without allocating an intermediate
    /// collection.
    ///
    /// # Safety
    /// `T` must match DuckDB's physical storage for this vector.
    ///
    /// # Panics
    /// Panics if `row` is outside the vector capacity.
    pub unsafe fn write<T>(&mut self, row: usize, value: T) {
        unsafe { self.vector.write(row, value) }.or_panic();
    }
}

#[cfg_attr(not(feature = "vtab-arrow"), allow(dead_code))]
impl FlatVector<'_> {
    pub(crate) fn readable_len(&self) -> Result<usize> {
        self.vector.readable_len()
    }

    pub(crate) fn reborrow_initialized(&self) -> Result<VectorRef<'_>> {
        self.vector.reborrow_initialized()
    }
}

/// A trait for inserting variable-length data into a flat vector.
///
/// `BIT` and `BIGNUM` use `string_t`-backed storage with internal encodings, so
/// generic insertion now rejects them instead of accepting verbatim bytes that
/// can form malformed values.
///
/// Insertions require exclusive access, so an initialized slice cannot remain
/// live across a write:
///
/// ```compile_fail
/// use duckdb::core::{DataChunkHandle, Inserter, LogicalTypeId};
///
/// let chunk = DataChunkHandle::new(&[LogicalTypeId::Varchar.into()]);
/// let mut vector = chunk.flat_vector(0);
/// let values = unsafe { vector.as_slice_with_len::<u8>(0) };
/// vector.insert(0, "value");
/// dbg!(values);
/// ```
pub trait Inserter<T> {
    /// Inserts `value` at `index`.
    ///
    /// # Panics
    ///
    /// Panics if `index` exceeds the vector capacity or the vector is not
    /// `VARCHAR`, `BLOB`, or WKB-backed `GEOMETRY` storage.
    fn insert(&mut self, index: usize, value: T);
}

impl Inserter<CString> for FlatVector<'_> {
    fn insert(&mut self, index: usize, value: CString) {
        self.vector.check_variable_length_insert(index).or_panic();
        unsafe { crate::ffi::duckdb_vector_assign_string_element(self.ptr(), index as u64, value.as_ptr()) };
    }
}

impl Inserter<&str> for FlatVector<'_> {
    fn insert(&mut self, index: usize, value: &str) {
        self.insert(index, value.as_bytes());
    }
}

impl Inserter<&String> for FlatVector<'_> {
    fn insert(&mut self, index: usize, value: &String) {
        self.insert(index, value.as_str());
    }
}

impl Inserter<&[u8]> for FlatVector<'_> {
    fn insert(&mut self, index: usize, value: &[u8]) {
        self.vector.check_variable_length_insert(index).or_panic();
        unsafe {
            crate::ffi::duckdb_vector_assign_string_element_len(
                self.ptr(),
                index as u64,
                value.as_ptr().cast(),
                value.len() as u64,
            );
        }
    }
}

impl Inserter<&Vec<u8>> for FlatVector<'_> {
    fn insert(&mut self, index: usize, value: &Vec<u8>) {
        self.insert(index, value.as_slice());
    }
}

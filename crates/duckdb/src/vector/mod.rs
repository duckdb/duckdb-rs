//! A generic, type-inferred DuckDB vector API.
//!
//! # Motivation
//!
//! DuckDB does not have a single physical vector layout. A vector's storage
//! *kind* (`duckdb_v2_vector_type`) can be:
//!
//! * `FLAT`       — one physical slot per logical row (the "normal" layout).
//! * `CONSTANT`   — a single physical value that applies to every row.
//! * `DICTIONARY` — a child vector plus a selection vector mapping each logical row to a child index.
//! * `OTHER`      — FSST / SEQUENCE / SHREDDED; must be `flatten`-ed first.
//!
//! Reading correctly therefore means honouring the *selection vector* returned
//! by `duckdb_v2_vector_get_view`: logical row `i` reads physical slot `sel[i]`
//! (or `0` for `CONSTANT`), not `data[i]`.
//!
//! # Approach
//!
//! [`Vector`] carries only its chunk lifetime and logical element type. Storage
//! representation and writability remain runtime properties reported by DuckDB.
//!
//! A column returned by [`crate::data_chunk::DataChunk::get_vector_at`] is
//! narrowed to its logical type before reading:
//!
//! ```ignore
//! let vector = chunk.get_vector_at::<i32>(0)?;
//! for value in vector.iter()? {
//!     ...
//! }
//! ```
//!
//! Element types plug in through the [`VectorElement`] trait. Each type defines its
//! own borrowed row representation: scalars yield references, while nested
//! values yield zero-copy handles into their child vectors.

use std::{marker::PhantomData, os::raw::c_void};

use crate::{
    Result,
    bytes::DuckDBBytes,
    check_api_call,
    error::{DuckDBError, Error},
    ffi,
    logical_type::LogicalType,
    value::Value,
};

mod element;
pub use crate::types::{
    Array, BigNum, BigNumDecoded, Decimal, InternalDecimalType, List, Map, Struct, TString, Union, Variant,
};
pub use element::*;

/// Runtime view of a vector's storage kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageKind {
    /// One physical slot per row.
    Flat,
    /// One physical slot shared by every row.
    Constant,
    /// A selection vector maps rows into a child vector.
    Dictionary,
    /// FSST / SEQUENCE / SHREDDED — must be flattened before reading.
    Other,
}

impl StorageKind {
    fn from_ffi(kind: ffi::DUCKDB_V2_VECTOR_TYPE) -> Self {
        use ffi::DUCKDB_V2_VECTOR_TYPE::*;
        match kind {
            DUCKDB_V2_VECTOR_TYPE_FLAT => StorageKind::Flat,
            DUCKDB_V2_VECTOR_TYPE_CONSTANT => StorageKind::Constant,
            DUCKDB_V2_VECTOR_TYPE_DICTIONARY => StorageKind::Dictionary,
            DUCKDB_V2_VECTOR_TYPE_OTHER => StorageKind::Other,
            _ => StorageKind::Other,
        }
    }
}

pub struct VectorView<T> {
    view: ffi::duckdb_v2_vector_view,
    kind: StorageKind,
    _marker: PhantomData<T>,
}

impl<T: VectorElement> VectorView<T> {
    /// Map a logical row to its physical storage index.
    ///
    /// # Panics
    ///
    /// Panics if `logical` is outside the view.
    pub fn physical_index(&self, logical: usize) -> usize {
        assert!(
            logical < self.len(),
            "logical index {logical} out of bounds for vector view of length {}",
            self.len()
        );

        match self.kind {
            StorageKind::Constant => 0,
            StorageKind::Flat if self.view.sel.is_null() => logical,
            StorageKind::Flat | StorageKind::Dictionary => unsafe { *self.view.sel.add(logical) as usize },
            StorageKind::Other => unreachable!("OTHER vectors have no readable view"),
        }
    }

    /// Return the number of logical rows in the view.
    pub fn len(&self) -> usize {
        self.view.count as usize
    }

    fn physical_len(&self) -> usize {
        match self.kind {
            StorageKind::Flat => self.len(),
            StorageKind::Constant => 1,
            StorageKind::Dictionary => self
                .selection()
                .and_then(|selection| selection.iter().max())
                .map_or(0, |index| *index as usize + 1),
            StorageKind::Other => unreachable!("OTHER vectors have no readable view"),
        }
    }

    fn is_null_physical(&self, physical: usize) -> bool {
        if self.view.validity.is_null() {
            false
        } else {
            unsafe { (*self.view.validity.add(physical / 64) & (1 << (physical % 64))) == 0 }
        }
    }

    /// Return whether the logical row contains SQL `NULL`.
    ///
    /// # Panics
    ///
    /// Panics if `index` is outside the view.
    pub fn is_null(&self, index: usize) -> bool {
        let physical = self.physical_index(index);
        self.is_null_physical(physical)
    }

    /// Return the vector's physical storage.
    ///
    /// Flat vectors contain one entry per logical row, constant vectors
    /// contain one entry, and dictionary vectors use [`Self::selection`] to
    /// map logical rows into this slice.
    ///
    /// # Safety
    ///
    /// Slices can return uninitialized memory for unselected or null entries.
    /// The caller is responsible for ensuring that they read only initialized and valid entries using the [`Self::selection`] and [`Self::validity`] methods.
    pub unsafe fn as_slice(&self) -> Option<&[T::Internal]> {
        if self.view.data.is_null() {
            None
        } else {
            Some(unsafe { std::slice::from_raw_parts(self.view.data as *const T::Internal, self.physical_len()) })
        }
    }

    /// Return a pointer to the physical storage, or null when no data is exposed.
    pub fn as_ptr(&self) -> *const T::Internal {
        if self.view.data.is_null() {
            std::ptr::null()
        } else {
            self.view.data as *const T::Internal
        }
    }

    /// Return the logical-to-physical row mapping, if DuckDB supplied one.
    pub fn selection(&self) -> Option<&[u32]> {
        if self.view.sel.is_null() {
            None
        } else {
            Some(unsafe { std::slice::from_raw_parts(self.view.sel as *const u32, self.view.count as usize) })
        }
    }

    /// Return the physical validity bitmask, or `None` when every entry is valid.
    pub fn validity(&self) -> Option<&[u64]> {
        if self.view.validity.is_null() {
            None
        } else {
            Some(unsafe {
                std::slice::from_raw_parts(self.view.validity as *const u64, self.physical_len().div_ceil(64))
            })
        }
    }

    pub(crate) unsafe fn cast<U: VectorElement>(&self) -> &VectorView<U> {
        unsafe { &*(self as *const VectorView<T> as *const VectorView<U>) }
    }

    /// Reinterpret this view as a view over a different logical element type.
    ///
    /// This only changes the `PhantomData` marker; the underlying FFI view
    /// (and thus any previously-flattened/dictionary state) is left untouched,
    /// unlike re-acquiring the view via `duckdb_v2_vector_get_view`.
    pub(crate) unsafe fn cast_owned<U: VectorElement>(self) -> VectorView<U> {
        VectorView {
            view: self.view,
            kind: self.kind,
            _marker: PhantomData,
        }
    }
}

/// A typed view of a DuckDB vector borrowed from its owning data chunk.
///
/// Storage representation and writability are runtime properties. Casting only
/// changes the logical element type and preserves the chunk lifetime.
pub struct Vector<'chunk, T: VectorElement> {
    pub(crate) handle: ffi::duckdb_v2_vector_handle,
    pub(crate) logical_type: LogicalType,
    pub(crate) kind: StorageKind,
    pub(crate) len: usize,
    pub(crate) view: Option<VectorView<T>>,
    pub(crate) writable: bool,
    pub(crate) data_mut: Option<*mut c_void>,
    pub(crate) validity_mut: Option<*mut u64>,
    heap: Option<ffi::duckdb_v2_arena_handle>,
    pub(crate) children: Vec<Vector<'chunk, Unknown>>,
    pub(crate) child_write_offset: usize,
    _chunk: PhantomData<&'chunk ()>,
    _type: PhantomData<T>,
}

impl<'chunk> Vector<'chunk, Unknown> {
    /// # Safety
    /// The caller must ensure that the handle is valid and that the vector's lifetime is tied to the lifetime of the chunk.
    pub(crate) fn from_handle(handle: &ffi::duckdb_v2_vector_handle, writable: bool) -> Result<Self> {
        let logical_type_handle = check_api_call!(ffi::duckdb_v2_vector_get_logical_type, *handle, RET)?;

        let vector_type: ffi::DUCKDB_V2_VECTOR_TYPE =
            check_api_call!(ffi::duckdb_v2_vector_get_vector_type, *handle, RET)?;

        let len: ffi::idx_t = check_api_call!(ffi::duckdb_v2_vector_get_size, *handle, RET)?;

        let child_count: ffi::idx_t = check_api_call!(ffi::duckdb_v2_vector_get_child_count, *handle, RET)?;

        let kind = StorageKind::from_ffi(vector_type);
        let view = Self::acquire_view(*handle, kind)?;
        let (data_mut, validity_mut) = Self::acquire_mutable_buffers(*handle, kind, writable)?;

        let mut children = Vec::with_capacity(child_count as usize);
        for index in 0..child_count {
            let child_handle = check_api_call!(ffi::duckdb_v2_vector_get_child, *handle, index, RET)?;
            children.push(Self::from_handle(&child_handle, writable)?);
        }

        Ok(Vector {
            handle: *handle,
            logical_type: LogicalType {
                handle: logical_type_handle,
            },
            kind,
            len: len as usize,
            view,
            writable,
            data_mut,
            validity_mut,
            heap: None,
            children,
            child_write_offset: 0,
            _chunk: PhantomData,
            _type: PhantomData,
        })
    }

    /// Validate and attach a logical element type.
    pub fn cast<T: VectorElement>(self) -> Result<Vector<'chunk, T>> {
        self.validate_as::<T>()?;
        Ok(self.cast_unchecked())
    }
}

impl<'chunk, T: VectorElement> Vector<'chunk, T> {
    /// Borrow the readable storage view.
    ///
    /// Returns `None` for [`StorageKind::Other`] until the vector is
    /// [`flatten`](Self::flatten)ed.
    pub fn get_view(&self) -> Option<&VectorView<T>> {
        self.view.as_ref()
    }

    fn acquire_view<U: VectorElement>(
        handle: ffi::duckdb_v2_vector_handle,
        kind: StorageKind,
    ) -> Result<Option<VectorView<U>>> {
        if kind == StorageKind::Other {
            return Ok(None);
        }

        let view: ffi::duckdb_v2_vector_view = check_api_call!(ffi::duckdb_v2_vector_get_view, handle, RET)?;
        Ok(Some(VectorView {
            view,
            kind,
            _marker: PhantomData,
        }))
    }

    fn acquire_mutable_buffers(
        handle: ffi::duckdb_v2_vector_handle,
        kind: StorageKind,
        writable: bool,
    ) -> Result<(Option<*mut c_void>, Option<*mut u64>)> {
        if !writable || kind != StorageKind::Flat {
            return Ok((None, None));
        }

        let data = check_api_call!(ffi::duckdb_v2_vector_get_data_mutable, handle, RET)?;
        let validity = check_api_call!(ffi::duckdb_v2_vector_flat_get_validity_mutable, handle, RET)?;
        Ok((Some(data), Some(validity)))
    }

    /// Refresh the vector's view and mutable buffers after a state change, e.g a flatten.
    fn refresh_buffers(&mut self) -> Result<()> {
        self.view = Self::acquire_view(self.handle, self.kind)?;
        (self.data_mut, self.validity_mut) = Self::acquire_mutable_buffers(self.handle, self.kind, self.writable)?;
        self.heap = None;
        Ok(())
    }

    fn refresh_tree(&mut self) -> Result<()> {
        self.refresh_buffers()?;
        for child in &mut self.children {
            child.refresh_tree()?;
        }
        Ok(())
    }

    pub(crate) fn cast_unchecked<U: VectorElement>(self) -> Vector<'chunk, U> {
        Vector {
            handle: self.handle,
            logical_type: self.logical_type,
            kind: self.kind,
            len: self.len,
            view: self.view.map(|view| unsafe { view.cast_owned::<U>() }),
            writable: self.writable,
            data_mut: self.data_mut,
            validity_mut: self.validity_mut,
            heap: self.heap,
            children: self.children,
            child_write_offset: self.child_write_offset,
            _chunk: self._chunk,
            _type: PhantomData,
        }
    }

    pub(crate) fn into_unknown(self) -> Vector<'chunk, Unknown> {
        self.cast_unchecked()
    }

    pub(crate) fn validate_as<U: VectorElement>(&self) -> Result<bool> {
        match U::validate(self.logical_type(), &self.children) {
            Ok(true) => Ok(true),
            Ok(false) => Err(Error {
                code: DuckDBError::DUCKDB_V2_ERROR_INPUT_INVALID,
                message: format!(
                    "Vector logical type mismatch: expected {:?}, got {:?}",
                    U::TYPE_ID,
                    self.logical_type.type_id()
                ),
            }),
            Err(e) => Err(Error {
                code: DuckDBError::DUCKDB_V2_ERROR_INPUT_INVALID,
                message: format!(
                    "Vector logical type validation failed: expected {:?}, got {:?}: {}",
                    U::TYPE_ID,
                    self.logical_type.type_id(),
                    e.message
                ),
            }),
        }
    }

    /// Return whether a logical row is `NULL`.
    pub fn is_null(&self, index: usize) -> Result<bool> {
        if index >= self.len {
            return Err(out_of_bounds(index, self.len));
        }

        let view = self.view.as_ref().ok_or_else(|| Error {
            code: DuckDBError::DUCKDB_V2_ERROR_INPUT_INVALID,
            message: "vector has no readable view".to_string(),
        })?;

        Ok(view.is_null(index))
    }

    /// Make this vector reference another vector's storage without copying.
    ///
    /// # Safety
    ///
    /// The source's storage must remain valid and must not be mutated
    /// concurrently for as long as the destination vector may be read. This
    /// requirement applies to the destination's owning chunk, even after the
    /// returned vector is dropped.
    pub unsafe fn copy_from<T2: VectorElement>(self, source: &Vector<'_, T2>) -> Result<Vector<'chunk, T2>> {
        check_api_call!(ffi::duckdb_v2_vector_reference, self.handle, source.handle)?;

        Ok(self.cast_unchecked::<T2>())
    }

    pub(crate) fn get_as_unchecked<U: VectorElement>(&self, index: usize) -> Option<U::Ref<'_>> {
        if index >= self.len {
            return None;
        }

        let view = self.view.as_ref()?;

        let physical = view.physical_index(index);

        if view.is_null_physical(physical) {
            None
        } else {
            Some(U::get(self, physical, index))
        }
    }

    pub(crate) fn get_as_checked<U: VectorElement>(&self, index: usize) -> Result<Option<U::Ref<'_>>> {
        self.validate_as::<U>()?;
        Ok(self.get_as_unchecked::<U>(index))
    }

    pub(crate) fn write_raw<U>(&mut self, index: usize, value: Option<U>) -> Result<()> {
        if index >= self.len {
            return Err(out_of_bounds(index, self.len));
        }

        let data = self.data_mut.ok_or_else(not_writable)?;
        let validity = self.validity_mut.ok_or_else(not_writable)?;
        unsafe {
            let word = validity.add(index / 64);
            let mask = 1u64 << (index % 64);
            match value {
                Some(value) => {
                    data.cast::<U>().add(index).write(value);
                    *word |= mask;
                }
                None => *word &= !mask,
            }
        }
        Ok(())
    }

    pub(crate) fn set_row_validity(&mut self, index: usize, is_valid: bool) -> Result<()> {
        if index >= self.len {
            return Err(out_of_bounds(index, self.len));
        }
        let validity = self.validity_mut.ok_or_else(not_writable)?;
        unsafe {
            let word = validity.add(index / 64);
            let mask = 1u64 << (index % 64);
            if is_valid {
                *word |= mask;
            } else {
                *word &= !mask;
            }
        }

        Ok(())
    }

    pub(crate) fn write_string(&mut self, index: usize, value: Option<&str>) -> Result<()> {
        if index >= self.len {
            return Err(out_of_bounds(index, self.len));
        }
        let value = match value {
            None => None,
            Some(value) => Some(DuckDBBytes::new(value, || self.heap())?),
        };
        self.write_raw(index, value)
    }

    /// Materialize one row as an owned [`Value`].
    ///
    /// This is the slow path; prefer [`Self::get`] or [`Self::iter`] for typed
    /// vector access.
    pub fn get_value_slow(&self, index: usize) -> Result<Value> {
        let value_handle = check_api_call!(ffi::duckdb_v2_vector_get_value, self.handle, index as u64, RET)?;

        Ok(Value { handle: value_handle })
    }

    /// Write one owned [`Value`] through DuckDB's generic value API.
    ///
    /// This is the slow path; prefer [`Self::write`] for typed vector access.
    pub fn write_value_slow(&mut self, index: usize, value: Value) -> Result<()> {
        check_api_call!(ffi::duckdb_v2_vector_set_value, self.handle, index as u64, value.handle,)
    }

    /// Set one row to `NULL` through DuckDB's generic value API.
    ///
    /// This is the slow path; prefer [`Self::write`] with `None`.
    pub fn set_null_slow(&mut self, index: usize) -> Result<()> {
        check_api_call!(ffi::duckdb_v2_vector_set_null, self.handle, index as u64,)
    }

    fn heap(&mut self) -> Result<ffi::duckdb_v2_arena_handle> {
        if let Some(handle) = self.heap {
            return Ok(handle);
        }
        let handle = check_api_call!(ffi::duckdb_v2_vector_get_arena, self.handle, RET)?;
        self.heap = Some(handle);
        Ok(handle)
    }

    pub(crate) fn write_as<U: WritableVectorElement>(
        &mut self,
        index: usize,
        value: Option<U::Write<'_>>,
    ) -> Result<()> {
        self.validate_as::<U>()?;
        // The element type is represented only by PhantomData.
        let typed = unsafe { &mut *(self as *mut Vector<'_, T> as *mut Vector<'_, U>) };
        U::write(typed, index, value)
    }

    /// Return the number of logical rows.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Return whether the vector contains no rows.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Return the vector's runtime storage representation.
    pub fn storage_kind(&self) -> StorageKind {
        self.kind
    }

    /// Return the vector's logical type.
    pub fn logical_type(&self) -> &LogicalType {
        &self.logical_type
    }

    /// Return whether DuckDB supplied this vector as writable output.
    pub fn is_writable(&self) -> bool {
        self.writable
    }

    /// Return the vector's child vectors.
    pub fn children(&self) -> &[Vector<'chunk, Unknown>] {
        &self.children
    }

    /// Return mutable access to the vector's children.
    pub fn children_mut(&mut self) -> &mut [Vector<'chunk, Unknown>] {
        &mut self.children
    }

    /// Explicitly materialize the vector as flat storage.
    pub fn flatten(&mut self) -> Result<()> {
        if self.kind == StorageKind::Flat {
            return Ok(());
        }
        check_api_call!(ffi::duckdb_v2_vector_flatten, self.handle)?;
        self.kind = StorageKind::Flat;
        self.refresh_tree()
    }

    /// Set the number of logical rows on writable output.
    pub fn set_size(&mut self, len: usize) -> Result<()> {
        if !self.writable {
            return Err(not_writable());
        }
        check_api_call!(ffi::duckdb_v2_vector_set_size, self.handle, len as u64)?;
        self.len = len;

        self.refresh_buffers()
    }

    /// Turn writable output into a constant vector.
    ///
    /// `value` must have the vector's logical type. When `is_valid` is false,
    /// every logical row is `NULL`; otherwise each row contains `value`.
    pub fn make_constant(&mut self, value: Value, is_valid: bool, count: usize) -> Result<()> {
        if !self.writable {
            return Err(not_writable());
        }
        check_api_call!(
            ffi::duckdb_v2_vector_make_constant,
            self.handle,
            value.handle,
            count as u64
        )?;
        check_api_call!(ffi::duckdb_v2_vector_constant_set_valid, self.handle, is_valid)?;
        self.kind = StorageKind::Constant;
        self.len = count;
        self.refresh_buffers()
    }

    /// Turn writable output into an arithmetic sequence.
    ///
    /// Produces `count` values following `start + index * increment`. The
    /// sequence uses [`StorageKind::Other`] and must be flattened before typed
    /// reads.
    pub fn make_sequence(&mut self, start: i64, increment: i64, count: usize) -> Result<()> {
        if !self.writable {
            return Err(not_writable());
        }
        check_api_call!(
            ffi::duckdb_v2_vector_make_sequence,
            self.handle,
            start,
            increment,
            count as u64
        )?;
        self.kind = StorageKind::Other;
        self.len = count;
        self.refresh_buffers()
    }
}

impl<T: VectorElement> Vector<'_, T> {
    /// Return the value at a logical row.
    ///
    /// Returns `None` for a `NULL` row. Vectors with
    /// [`StorageKind::Other`] return an error until flattened.
    pub fn get(&self, index: usize) -> Result<Option<T::Ref<'_>>> {
        if self.view.is_none() {
            return Err(other_not_readable());
        }
        if index >= self.len {
            return Err(out_of_bounds(index, self.len));
        }
        Ok(self.get_as_unchecked::<T>(index))
    }

    /// Iterate over the vector after checking its readable state once.
    pub fn iter(&self) -> Result<VectorIter<'_, '_, T>> {
        if self.view.is_none() {
            return Err(other_not_readable());
        }

        Ok(VectorIter { vector: self, index: 0 })
    }
}

impl<T: WritableVectorElement> Vector<'_, T> {
    /// Write a value at a logical row.
    ///
    /// The vector must be writable with flat storage, and `index` must be in
    /// range. Pass `None` to write SQL `NULL`.
    pub fn write(&mut self, index: usize, value: Option<T::Write<'_>>) -> Result<()> {
        if !self.writable {
            return Err(not_writable());
        }
        T::write(self, index, value)
    }
}

/// Iterates over the logical rows of a vector.
pub struct VectorIter<'vector, 'chunk, T: VectorElement> {
    vector: &'vector Vector<'chunk, T>,
    index: usize,
}

impl<'vector, T: VectorElement + 'vector> Iterator for VectorIter<'vector, '_, T> {
    type Item = Option<T::Ref<'vector>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.vector.len {
            return None;
        }
        let value = self.vector.get_as_unchecked::<T>(self.index);
        self.index += 1;
        Some(value)
    }
}

fn not_writable() -> Error {
    Error {
        code: DuckDBError::DUCKDB_V2_ERROR_INPUT_INVALID,
        message: "vector was not supplied as writable output".to_string(),
    }
}

fn other_not_readable() -> Error {
    Error {
        code: DuckDBError::DUCKDB_V2_ERROR_INPUT_INVALID,
        message: "OTHER vectors must be flattened before reading".to_string(),
    }
}

fn out_of_bounds(index: usize, len: usize) -> Error {
    Error {
        code: DuckDBError::DUCKDB_V2_ERROR_INPUT_PARAMETER_INVALID,
        message: format!("Vector index {} is out of bounds for length {}", index, len),
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests;

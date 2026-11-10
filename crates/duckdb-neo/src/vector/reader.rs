use std::ops::Range;

use super::{StorageKind, Vector, VectorElement, other_not_readable, out_of_bounds};
use crate::Result;
use crate::ffi;

/// Maps logical row indices to physical data and validity positions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Selection<'vec> {
    /// Logical row `i` reads physical slot `i`.
    Identity,
    /// Every logical row reads physical slot zero, without a selection buffer.
    Constant,
    /// Each entry contains the physical index of one logical row.
    Dictionary(&'vec [u32]),
}

impl Selection<'_> {
    /// Map a logical row to its physical index without checking the reader's length.
    /// Panics if a dictionary selection is indexed out of bounds.
    #[inline]
    pub fn index(&self, logical: usize) -> usize {
        match self {
            Self::Identity => logical,
            Self::Constant => 0,
            Self::Dictionary(selection) => selection[logical] as usize,
        }
    }
}

/// Prepared, read-only access to logical rows, created by [`Vector::reader`].
/// Values preserve SQL NULLs and borrow from the vector, not the reader.
#[repr(C)]
pub struct VectorReader<'vec, T: VectorElement> {
    vector: &'vec Vector<'vec, T>,
}

impl<T: VectorElement> std::fmt::Debug for VectorReader<'_, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VectorReader")
            .field("len", &self.len())
            .field("data", &self.view().data.cast::<()>())
            .field("storage", &self.cached().storage)
            .finish()
    }
}

impl<T: VectorElement> Clone for VectorReader<'_, T> {
    #[inline]
    fn clone(&self) -> Self {
        Self { vector: self.vector }
    }
}

#[derive(Debug)]
pub(super) struct CachedReader {
    view: ffi::duckdb_v2_vector_view,
    storage: ReaderStorage,
}

impl CachedReader {
    pub(super) unsafe fn new(view: ffi::duckdb_v2_vector_view, kind: StorageKind, len: usize) -> Self {
        let storage = match kind {
            StorageKind::Flat => ReaderStorage::Flat,
            StorageKind::Dictionary => ReaderStorage::Dictionary(DictionaryReader),
            StorageKind::Constant => ReaderStorage::Constant(ConstantReader {
                is_null: len != 0 && !unsafe { FlatReader::is_valid_unchecked(view.validity, 0) },
            }),
            StorageKind::Other => unreachable!("prepared vector has a readable storage kind"),
        };
        Self { view, storage }
    }
}

#[derive(Debug, Clone, Copy)]
enum ReaderStorage {
    Flat,
    Dictionary(DictionaryReader),
    Constant(ConstantReader),
}

#[derive(Debug, Clone)]
struct FlatReader {}

impl FlatReader {
    #[inline]
    unsafe fn is_valid_unchecked(validity: *const u64, physical: usize) -> bool {
        validity.is_null() || unsafe { *validity.add(physical / 64) & (1u64 << (physical % 64)) != 0 }
    }
}

#[derive(Debug, Clone, Copy)]
struct ConstantReader {
    is_null: bool,
}

impl ConstantReader {
    #[inline]
    fn physical_index(&self) -> Option<usize> {
        (!self.is_null).then_some(0)
    }
}

#[derive(Debug, Clone, Copy)]
struct DictionaryReader;

impl DictionaryReader {
    #[inline]
    unsafe fn physical_index_unchecked(validity: *const u64, selection: *const u32, logical: usize) -> Option<usize> {
        let physical = unsafe { *selection.add(logical) } as usize;
        unsafe { FlatReader::is_valid_unchecked(validity, physical) }.then_some(physical)
    }
}

impl ReaderStorage {
    #[inline]
    unsafe fn physical_index_unchecked(&self, view: ffi::duckdb_v2_vector_view, index: usize) -> Option<usize> {
        match self {
            Self::Flat => unsafe { FlatReader::is_valid_unchecked(view.validity, index) }.then_some(index),
            Self::Dictionary(_) => unsafe {
                DictionaryReader::physical_index_unchecked(view.validity, view.sel, index)
            },
            Self::Constant(reader) => reader.physical_index(),
        }
    }

    /// Return the physical storage span, including unselected dictionary slots.
    /// Flat uses the row count, constant uses one slot, and dictionary ends at its highest selected index.
    pub fn physical_len(&self, view: ffi::duckdb_v2_vector_view) -> usize {
        match self {
            Self::Flat => view.count as usize,
            Self::Constant(_) => 1,
            Self::Dictionary(_) => unsafe { std::slice::from_raw_parts(view.sel, view.count as usize) }
                .iter()
                .max()
                .map_or(0, |index| *index as usize + 1),
        }
    }
}

impl<'vec, T: VectorElement> VectorReader<'vec, T> {
    pub(super) fn new(vector: &'vec Vector<'vec, T>) -> Result<Self> {
        vector.view()?.ok_or_else(other_not_readable)?;
        vector.cache_children()?;
        Ok(Self { vector })
    }

    #[inline(always)]
    fn cached(&self) -> &'vec CachedReader {
        // SAFETY: construction prepared this vector, and only exclusive access can clear its cache.
        unsafe { self.vector.view.get().unwrap_unchecked() }
    }

    /// The vector's cached view; construction cached it, and the shared borrow keeps it.
    #[inline(always)]
    pub(crate) fn view(&self) -> ffi::duckdb_v2_vector_view {
        self.cached().view
    }

    #[inline]
    pub(crate) fn vector(&self) -> &'vec Vector<'vec, T> {
        self.vector
    }

    /// Return the number of logical rows.
    pub fn len(&self) -> usize {
        self.vector.len()
    }

    /// Return whether the reader contains no rows.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Return the physical data pointer, or null; invalidated when the vector is resized, reshaped, or destroyed.
    /// Map rows through [`Self::selection`] and check [`Self::validity`] before dereferencing.
    #[inline(always)]
    pub fn as_ptr(&self) -> *const T::Internal<'vec> {
        self.view().data.cast()
    }

    /// # Safety
    /// Borrow physical storage only if all slots, including NULL/unselected ones, are initialized as `T::Internal<'vec>`.
    pub unsafe fn as_slice(&self) -> Option<&'vec [T::Internal<'vec>]> {
        // SAFETY: The caller guarantees all physical slots are initialized.
        if self.view().data.is_null() {
            None
        } else {
            Some(unsafe {
                std::slice::from_raw_parts(self.view().data.cast(), self.cached().storage.physical_len(self.view()))
            })
        }
    }

    /// Borrow the physical validity mask, or `None` if all slots are valid; map rows with [`Self::selection`].
    /// Bit `physical % 64` of word `physical / 64` marks a non-NULL slot.
    pub fn validity(&self) -> Option<&'vec [u64]> {
        if self.view().validity.is_null() {
            None
        } else {
            Some(unsafe {
                std::slice::from_raw_parts(
                    self.view().validity,
                    self.cached().storage.physical_len(self.view()).div_ceil(64),
                )
            })
        }
    }

    /// Return the logical-to-physical mapping without materializing a buffer.
    pub fn selection(&self) -> Selection<'vec> {
        match self.cached().storage {
            ReaderStorage::Flat => Selection::Identity,
            ReaderStorage::Constant(_) => Selection::Constant,
            ReaderStorage::Dictionary(_) => {
                let selection = if self.view().sel.is_null() {
                    None
                } else {
                    let len = self.view().count as usize;
                    Some(unsafe { std::slice::from_raw_parts(self.view().sel, len) })
                };

                Selection::Dictionary(selection.expect("prepared dictionary has a selection"))
            }
        }
    }

    /// Return a logical row, or `None` for SQL NULL. Out-of-range indices are errors.
    #[inline]
    pub fn get(&self, index: usize) -> Result<Option<T::Ref<'vec>>> {
        if index >= self.len() {
            return Err(out_of_bounds(index, self.len()));
        }
        // SAFETY: Construction prepared the descendants, and the index is in bounds.
        Ok(unsafe { self.get_unchecked(index) })
    }

    /// Read a validated child directly from its prepared view.
    ///
    /// # Safety
    ///
    /// `child_index` must name a child validated as `U`, its descendants must be
    /// prepared, and `logical` must be within the child's logical rows.
    #[doc(hidden)]
    #[inline(always)]
    pub unsafe fn get_child_value_unchecked<U: VectorElement>(
        &self,
        child_index: usize,
        logical: usize,
    ) -> Option<U::Ref<'vec>> {
        // SAFETY: the caller guarantees the child type and logical row bounds.
        unsafe { self.child_reader::<U>(child_index).get_unchecked(logical) }
    }

    /// # Safety
    /// `index` must name a child validated as `U`.
    #[inline(always)]
    pub(crate) unsafe fn child_reader<U: VectorElement>(&self, index: usize) -> VectorReader<'vec, U> {
        let child = self
            .vector
            .children()
            .expect("child vectors are prepared")
            .cached_unchecked(index);
        VectorReader {
            // SAFETY: the caller guarantees the type, and construction prepared every descendant.
            vector: unsafe { child.cast_ref_unchecked::<U>() },
        }
    }

    /// Return whether a logical row is SQL NULL. Out-of-range indices are errors.
    #[inline]
    pub fn is_null(&self, index: usize) -> Result<bool> {
        if index >= self.len() {
            return Err(out_of_bounds(index, self.len()));
        }
        // SAFETY: The index is within the prepared vector's logical rows.
        Ok(unsafe { self.cached().storage.physical_index_unchecked(self.view(), index) }.is_none())
    }

    /// Iterate over logical rows without repeating reader preparation.
    pub fn iter(&self) -> VectorReaderIter<'vec, T> {
        VectorReaderIter {
            reader: self.clone(),
            rows: 0..self.len(),
        }
    }

    /// Return a row's list entry, or `None` for SQL NULL, without building the row.
    ///
    /// # Safety
    /// The physical storage must hold DuckDB list entries, as for `LIST` and `MAP`.
    #[inline]
    pub(crate) unsafe fn list_entry(&self, index: usize) -> Result<Option<crate::types::ListIndex>> {
        if index >= self.len() {
            return Err(out_of_bounds(index, self.len()));
        }
        // SAFETY: The index is within the prepared vector's logical rows.
        let Some(physical) = (unsafe { self.cached().storage.physical_index_unchecked(self.view(), index) }) else {
            return Ok(None);
        };
        // SAFETY: The caller guarantees list entry storage, and the row is valid.
        Ok(Some(unsafe {
            *self.view().data.cast::<crate::types::ListIndex>().add(physical)
        }))
    }

    #[inline(always)]
    pub(crate) unsafe fn get_unchecked(&self, index: usize) -> Option<T::Ref<'vec>> {
        let physical = unsafe { self.cached().storage.physical_index_unchecked(self.view(), index) }?;
        // SAFETY: The reader is typed, its descendants are prepared, and the row is valid.
        Some(unsafe { T::get(self, physical, index) })
    }

    #[inline(always)]
    pub(crate) unsafe fn get_flat_unchecked(&self, index: usize) -> Option<T::Ref<'vec>> {
        let ReaderStorage::Flat = &self.cached().storage else {
            // SAFETY: The caller guarantees flat storage for this prepared reader.
            unsafe { std::hint::unreachable_unchecked() }
        };
        if !unsafe { FlatReader::is_valid_unchecked(self.view().validity, index) } {
            return None;
        }
        // SAFETY: Flat storage uses the logical index directly; validity was checked above.
        Some(unsafe { T::get(self, index, index) })
    }
}

/// Iterates over the logical rows of a prepared [`VectorReader`].
#[derive(Debug)]
pub struct VectorReaderIter<'vec, T: VectorElement> {
    reader: VectorReader<'vec, T>,
    rows: Range<usize>,
}

impl<'vec, T: VectorElement + 'vec> Iterator for VectorReaderIter<'vec, T> {
    type Item = Option<T::Ref<'vec>>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let index = self.rows.next()?;
        // SAFETY: The range only yields logical rows of the prepared reader.
        Some(unsafe { self.reader.get_unchecked(index) })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.rows.size_hint()
    }
}

impl<T: VectorElement> ExactSizeIterator for VectorReaderIter<'_, T> {}

impl<T: VectorElement> std::iter::FusedIterator for VectorReaderIter<'_, T> {}

#[cfg(test)]
mod tests;

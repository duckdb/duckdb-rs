use std::ops::Range;

use super::{NativeElement, ReadableNativeElement, StorageKind, Vector, other_not_readable, out_of_bounds};
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
pub struct VectorReader<'vec, T: NativeElement> {
    vector: &'vec Vector<'vec, T>,
    cached: CachedReader,
}

impl<T: NativeElement> std::fmt::Debug for VectorReader<'_, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VectorReader")
            .field("len", &self.len())
            .field("data", &self.view().data.cast::<()>())
            .field("storage", &self.cached().storage)
            .finish()
    }
}

impl<T: NativeElement> Clone for VectorReader<'_, T> {
    #[inline]
    fn clone(&self) -> Self {
        Self {
            vector: self.vector,
            cached: self.cached,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) struct CachedReader {
    view: ffi::duckdb_v2_vector_view,
    storage: ReaderStorage,
    len: usize,
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
        Self { view, storage, len }
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
    #[inline(always)]
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

impl<'vec, T: NativeElement> VectorReader<'vec, T> {
    pub(super) fn new(vector: &'vec Vector<'vec, T>) -> Result<Self> {
        let cached = *vector.view()?.ok_or_else(other_not_readable)?;
        Ok(Self { vector, cached })
    }

    #[inline(always)]
    fn cached(&self) -> &CachedReader {
        &self.cached
    }

    /// The vector's view, copied at construction; the shared borrow keeps it current.
    #[inline(always)]
    pub(crate) fn view(&self) -> ffi::duckdb_v2_vector_view {
        self.cached.view
    }

    #[inline]
    pub(crate) fn vector(&self) -> &'vec Vector<'vec, T> {
        self.vector
    }

    /// Return the number of logical rows.
    #[inline]
    pub fn len(&self) -> usize {
        self.cached.len
    }

    /// Return whether the reader contains no rows.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Return the physical data pointer, or null; invalidated when the vector is resized, reshaped, or destroyed.
    /// Map rows through [`Self::selection`] and check [`Self::validity`] before dereferencing.
    #[inline(always)]
    pub fn as_ptr(&self) -> *const T::Native<'vec> {
        self.view().data.cast()
    }

    /// # Safety
    /// Borrow physical storage only if all slots, including NULL/unselected ones, are initialized as `T::Native<'vec>`.
    pub unsafe fn as_slice(&self) -> Option<&'vec [T::Native<'vec>]> {
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

    /// Return whether a logical row is SQL NULL. Out-of-range indices are errors.
    #[inline]
    pub fn is_null(&self, index: usize) -> Result<bool> {
        if index >= self.len() {
            return Err(out_of_bounds(index, self.len()));
        }
        // SAFETY: The index is within the prepared vector's logical rows.
        Ok(unsafe { self.cached().storage.physical_index_unchecked(self.view(), index) }.is_none())
    }

    /// # Safety
    /// `index` must be a logical row of this reader.
    #[inline(always)]
    pub(crate) unsafe fn physical_index_unchecked(&self, index: usize) -> Option<usize> {
        // SAFETY: the caller guarantees bounds.
        unsafe { self.cached().storage.physical_index_unchecked(self.view(), index) }
    }

    /// # Safety
    /// `index` must be a logical row, and the physical storage must hold DuckDB list entries.
    #[inline(always)]
    pub(crate) unsafe fn list_entry_unchecked(&self, index: usize) -> Option<crate::types::ListIndex> {
        // SAFETY: the caller guarantees bounds and list entry storage.
        unsafe {
            self.physical_index_unchecked(index)
                .map(|physical| *self.view().data.cast::<crate::types::ListIndex>().add(physical))
        }
    }
}

impl<'vec, T: ReadableNativeElement> VectorReader<'vec, T> {
    /// Return a logical row, or `None` for SQL NULL. Out-of-range indices are errors.
    #[inline]
    pub fn get(&self, index: usize) -> Result<Option<T::Ref<'vec>>> {
        if index >= self.len() {
            return Err(out_of_bounds(index, self.len()));
        }
        // SAFETY: The index is in bounds.
        Ok(unsafe { self.get_unchecked(index) })
    }

    /// Iterate over logical rows without repeating reader preparation.
    pub fn iter(&self) -> VectorReaderIter<'vec, T> {
        VectorReaderIter {
            reader: self.clone(),
            rows: 0..self.len(),
        }
    }

    #[inline(always)]
    pub(crate) unsafe fn get_unchecked(&self, index: usize) -> Option<T::Ref<'vec>> {
        let physical = unsafe { self.cached().storage.physical_index_unchecked(self.view(), index) }?;
        // SAFETY: The reader is typed, and the row is valid.
        Some(unsafe { T::get(self, physical, index) })
    }

    /// Fold `rows` with the storage kind resolved once, outside the loop.
    ///
    /// # Safety
    /// Every row of `rows` must be a logical row of this reader.
    #[inline(always)]
    unsafe fn fold_rows<B>(
        &self,
        rows: Range<usize>,
        init: B,
        mut f: impl FnMut(B, usize, Option<T::Ref<'vec>>) -> B,
    ) -> B {
        let view = self.view();
        match self.cached().storage {
            // SAFETY: the caller guarantees every row is in bounds; flat rows are their own physical slots.
            ReaderStorage::Flat => rows.fold(init, |acc, row| {
                let value = unsafe { FlatReader::is_valid_unchecked(view.validity, row) }
                    .then(|| unsafe { T::get(self, row, row) });
                f(acc, row, value)
            }),
            // SAFETY: as above; the selection maps every logical row.
            ReaderStorage::Dictionary(_) => rows.fold(init, |acc, row| {
                let value = unsafe { DictionaryReader::physical_index_unchecked(view.validity, view.sel, row) }
                    .map(|physical| unsafe { T::get(self, physical, row) });
                f(acc, row, value)
            }),
            // SAFETY: as above; every row reads physical slot zero.
            ReaderStorage::Constant(reader) => {
                let physical = reader.physical_index();
                rows.fold(init, |acc, row| {
                    f(
                        acc,
                        row,
                        physical.map(|physical| unsafe { T::get(self, physical, row) }),
                    )
                })
            }
        }
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
pub struct VectorReaderIter<'vec, T: ReadableNativeElement> {
    reader: VectorReader<'vec, T>,
    rows: Range<usize>,
}

impl<'vec, T: ReadableNativeElement + 'vec> Iterator for VectorReaderIter<'vec, T> {
    type Item = Option<T::Ref<'vec>>;

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        let index = self.rows.next()?;
        // SAFETY: The range only yields logical rows of the prepared reader.
        Some(unsafe { self.reader.get_unchecked(index) })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.rows.size_hint()
    }

    #[inline(always)]
    fn fold<B, F: FnMut(B, Self::Item) -> B>(self, init: B, mut f: F) -> B {
        // SAFETY: the range only yields logical rows of the prepared reader.
        unsafe { self.reader.fold_rows(self.rows, init, |acc, _, value| f(acc, value)) }
    }
}

impl<T: ReadableNativeElement> ExactSizeIterator for VectorReaderIter<'_, T> {}

impl<T: ReadableNativeElement> std::iter::FusedIterator for VectorReaderIter<'_, T> {}

impl<'vec, T: ReadableNativeElement> VectorReaderIter<'vec, T> {
    /// Add row numbers like [`Iterator::enumerate`], numbering the remaining rows from zero.
    /// Unlike [`std::iter::Enumerate`], it always inlines into large loops.
    pub fn enumerate(self) -> EnumeratedVectorReaderIter<'vec, T> {
        let origin = self.rows.start;
        EnumeratedVectorReaderIter { inner: self, origin }
    }
}

/// Row-numbered adapter returned by [`VectorReaderIter::enumerate`].
#[derive(Debug)]
pub struct EnumeratedVectorReaderIter<'vec, T: ReadableNativeElement> {
    inner: VectorReaderIter<'vec, T>,
    origin: usize,
}

impl<'vec, T: ReadableNativeElement + 'vec> Iterator for EnumeratedVectorReaderIter<'vec, T> {
    type Item = (usize, Option<T::Ref<'vec>>);

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        let index = self.inner.rows.next()?;
        // SAFETY: The range only yields logical rows of the prepared reader.
        Some((index - self.origin, unsafe { self.inner.reader.get_unchecked(index) }))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }

    #[inline(always)]
    fn fold<B, F: FnMut(B, Self::Item) -> B>(self, init: B, mut f: F) -> B {
        let origin = self.origin;
        // SAFETY: the range only yields logical rows of the prepared reader.
        unsafe {
            self.inner
                .reader
                .fold_rows(self.inner.rows, init, |acc, row, value| f(acc, (row - origin, value)))
        }
    }
}

impl<T: ReadableNativeElement> ExactSizeIterator for EnumeratedVectorReaderIter<'_, T> {}

impl<T: ReadableNativeElement> std::iter::FusedIterator for EnumeratedVectorReaderIter<'_, T> {}

#[cfg(test)]
mod tests;

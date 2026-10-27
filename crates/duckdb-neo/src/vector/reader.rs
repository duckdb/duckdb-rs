use std::{ops::Range, rc::Rc};

use crate::Result;

use super::{StorageKind, Vector, VectorElement, VectorView, other_not_readable, out_of_bounds};

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
pub struct VectorReader<'vec, T: VectorElement> {
    vector: &'vec Vector<'vec, T>,
    data: *const std::ffi::c_void,
    storage: ReaderStorage,
    child_readers: Option<Rc<[VectorReader<'vec, super::Unknown>]>>,
}

impl<T: VectorElement> std::fmt::Debug for VectorReader<'_, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VectorReader")
            .field("len", &self.len())
            .field("data", &self.data.cast::<()>())
            .field("storage", &self.storage)
            .finish()
    }
}

impl<T: VectorElement> Clone for VectorReader<'_, T> {
    #[inline]
    fn clone(&self) -> Self {
        Self {
            vector: self.vector,
            data: self.data,
            storage: self.storage,
            child_readers: self.child_readers.clone(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum ReaderStorage {
    Flat(FlatReader),
    Constant(ConstantReader),
    Dictionary(DictionaryReader),
}

#[derive(Debug, Clone, Copy)]
struct FlatReader {
    validity: *const u64,
}

impl FlatReader {
    #[inline]
    unsafe fn is_valid_unchecked(&self, physical: usize) -> bool {
        self.validity.is_null() || unsafe { *self.validity.add(physical / 64) & (1u64 << (physical % 64)) != 0 }
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
struct DictionaryReader {
    values: FlatReader,
    selection: *const u32,
}

impl DictionaryReader {
    #[inline]
    unsafe fn physical_index_unchecked(&self, logical: usize) -> Option<usize> {
        let physical = unsafe { *self.selection.add(logical) } as usize;
        unsafe { self.values.is_valid_unchecked(physical) }.then_some(physical)
    }
}

impl ReaderStorage {
    #[inline]
    unsafe fn physical_index_unchecked(&self, logical: usize) -> Option<usize> {
        match self {
            Self::Flat(reader) => unsafe { reader.is_valid_unchecked(logical) }.then_some(logical),
            Self::Constant(reader) => reader.physical_index(),
            Self::Dictionary(reader) => unsafe { reader.physical_index_unchecked(logical) },
        }
    }
}

impl<'vec, T: VectorElement> VectorReader<'vec, T> {
    pub(super) fn new(vector: &'vec Vector<'vec, T>) -> Result<Self> {
        if vector.view.is_none() {
            return Err(other_not_readable());
        }
        vector.cache_children()?;
        // SAFETY: The vector and all descendants have readable, cached views.
        Ok(unsafe { Self::from_prepared(vector) })
    }

    pub(crate) unsafe fn from_prepared(vector: &'vec Vector<'vec, T>) -> Self {
        let view = vector.view.as_ref().expect("prepared vector has a readable view");
        let flat = FlatReader {
            validity: view.view.validity,
        };
        let storage = match vector.storage_kind() {
            StorageKind::Flat => ReaderStorage::Flat(flat),
            StorageKind::Constant => ReaderStorage::Constant(ConstantReader {
                is_null: !vector.is_empty() && !unsafe { flat.is_valid_unchecked(0) },
            }),
            StorageKind::Dictionary => ReaderStorage::Dictionary(DictionaryReader {
                values: flat,
                selection: view.selection().expect("prepared dictionary has a selection").as_ptr(),
            }),
            StorageKind::Other => unreachable!("prepared vector has a readable storage kind"),
        };
        Self {
            vector,
            data: view.view.data,
            storage,
            child_readers: vector.children().map(|children| {
                (0..children.col_count())
                    .map(|index| {
                        // SAFETY: construction prepared every descendant view.
                        unsafe { VectorReader::from_prepared(children.cached_unchecked(index)) }
                    })
                    .collect::<Vec<_>>()
                    .into()
            }),
        }
    }

    /// Return the number of logical rows.
    pub fn len(&self) -> usize {
        self.vector.len()
    }

    /// Return whether the reader contains no rows.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Return the storage representation selected when the reader was created.
    pub fn storage_kind(&self) -> StorageKind {
        match self.storage {
            ReaderStorage::Flat(_) => StorageKind::Flat,
            ReaderStorage::Constant(_) => StorageKind::Constant,
            ReaderStorage::Dictionary(_) => StorageKind::Dictionary,
        }
    }

    /// Return the physical data pointer, or null; invalidated when the vector is resized, reshaped, or destroyed.
    /// Map rows through [`Self::selection`] and check [`Self::validity`] before dereferencing.
    #[inline(always)]
    pub fn as_ptr(&self) -> *const T::Internal<'vec> {
        self.data.cast()
    }

    /// # Safety
    /// Borrow physical storage only if all slots, including NULL/unselected ones, are initialized as `T::Internal<'vec>`.
    pub unsafe fn as_slice(&self) -> Option<&'vec [T::Internal<'vec>]> {
        // SAFETY: The caller guarantees all physical slots are initialized.
        if self.view().data().is_null() {
            None
        } else {
            Some(unsafe { std::slice::from_raw_parts(self.view().data(), self.view().physical_len()) })
        }
    }

    /// Borrow the physical validity mask, or `None` if all slots are valid; map rows with [`Self::selection`].
    /// Bit `physical % 64` of word `physical / 64` marks a non-NULL slot.
    pub fn validity(&self) -> Option<&'vec [u64]> {
        self.view().validity()
    }

    /// Return the logical-to-physical mapping without materializing a buffer.
    pub fn selection(&self) -> Selection<'vec> {
        match self.storage {
            ReaderStorage::Flat(_) => Selection::Identity,
            ReaderStorage::Constant(_) => Selection::Constant,
            ReaderStorage::Dictionary(_) => {
                Selection::Dictionary(self.view().selection().expect("prepared dictionary has a selection"))
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
    pub unsafe fn get_child_unchecked<U: VectorElement>(
        &self,
        child_index: usize,
        logical: usize,
    ) -> Option<U::Ref<'vec>> {
        let child = self.cached_child_reader(child_index);
        // SAFETY: the caller guarantees the child type and logical row bounds.
        let physical = unsafe { child.storage.physical_index_unchecked(logical) }?;
        Some(unsafe { U::get(child, physical, logical) })
    }

    /// Borrow a prepared child's reader state after validating its type externally.
    ///
    /// # Safety
    /// `child_index` must name a child validated as `U`.
    #[doc(hidden)]
    #[inline(always)]
    pub unsafe fn child_reader<U: VectorElement>(&self, child_index: usize) -> VectorReader<'vec, U> {
        let reader = self.cached_child_reader(child_index);
        VectorReader {
            // SAFETY: the caller guarantees that the child has type U.
            vector: unsafe { reader.vector.cast_ref_unchecked::<U>() },
            data: reader.data,
            storage: reader.storage,
            child_readers: reader.child_readers.clone(),
        }
    }

    fn cached_child_reader(&self, index: usize) -> &VectorReader<'vec, super::Unknown> {
        &self.child_readers.as_ref().expect("child readers are prepared")[index]
    }

    /// Return whether a logical row is SQL NULL. Out-of-range indices are errors.
    #[inline]
    pub fn is_null(&self, index: usize) -> Result<bool> {
        if index >= self.len() {
            return Err(out_of_bounds(index, self.len()));
        }
        // SAFETY: The index is within the prepared vector's logical rows.
        Ok(unsafe { self.storage.physical_index_unchecked(index) }.is_none())
    }

    /// Iterate over logical rows without repeating reader preparation.
    pub fn iter(&self) -> VectorReaderIter<'vec, T> {
        VectorReaderIter {
            reader: self.clone(),
            rows: 0..self.len(),
        }
    }

    fn view(&self) -> &'vec VectorView<T> {
        self.vector.view.as_ref().expect("reader has a readable view")
    }

    pub(crate) fn metadata(&self) -> super::TypeMetadata {
        self.vector.metadata()
    }

    pub(crate) fn children(&self) -> Option<&'vec crate::data_chunk::VectorCollection> {
        self.vector.children()
    }

    pub(crate) fn logical_type(&self) -> &'vec crate::logical_type::LogicalType {
        self.vector.logical_type()
    }

    pub(crate) fn get_value_slow(&self, index: usize) -> Result<crate::value::Value> {
        self.vector.get_value_slow(index)
    }

    #[inline(always)]
    pub(crate) unsafe fn get_unchecked(&self, index: usize) -> Option<T::Ref<'vec>> {
        let physical = unsafe { self.storage.physical_index_unchecked(index) }?;
        // SAFETY: The reader is typed, its descendants are prepared, and the row is valid.
        Some(unsafe { T::get(self, physical, index) })
    }

    #[inline(always)]
    pub(crate) unsafe fn get_flat_unchecked(&self, index: usize) -> Option<T::Ref<'vec>> {
        let ReaderStorage::Flat(reader) = &self.storage else {
            // SAFETY: The caller guarantees flat storage for this prepared reader.
            unsafe { std::hint::unreachable_unchecked() }
        };
        if !unsafe { reader.is_valid_unchecked(index) } {
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

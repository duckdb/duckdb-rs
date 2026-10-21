use std::ops::Range;

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
    /// Return the physical index for a logical row.
    ///
    /// This does not check the originating reader's length. Pass a row in
    /// `0..reader.len()` before using the result to access its physical storage.
    ///
    /// # Panics
    /// Panics if a dictionary selection is indexed beyond its length.
    #[inline]
    pub fn index(&self, logical: usize) -> usize {
        match self {
            Self::Identity => logical,
            Self::Constant => 0,
            Self::Dictionary(selection) => selection[logical] as usize,
        }
    }
}

/// Prepared, read-only access to a vector's logical rows.
///
/// Created by [`Vector::reader`]. Storage selection and descendant preparation
/// happen once; [`Self::get`] and [`Self::iter`] preserve SQL NULLs and borrow
/// their values from the vector, not the reader.
///
/// The vector cannot be reshaped while its reader is in use:
///
/// ```compile_fail,E0502
/// use duckdb_neo::{Result, vector::Vector};
///
/// fn resize(vector: &mut Vector<'_, i32>) -> Result<()> {
///     let reader = vector.reader()?;
///     vector.set_size(10)?;
///     let _ = reader.get(0)?;
///     Ok(())
/// }
/// ```
///
/// Values borrowed through a temporary reader also prevent mutation:
///
/// ```compile_fail,E0502
/// use duckdb_neo::{Result, vector::Vector};
///
/// fn overwrite(vector: &mut Vector<'_, i32>) -> Result<()> {
///     let value = vector.reader()?.get(0)?;
///     vector.writer()?.set(0, Some(42))?;
///     assert_eq!(value, Some(&42));
///     Ok(())
/// }
/// ```
pub struct VectorReader<'vec, T: VectorElement> {
    vector: &'vec Vector<'vec, T>,
    data: *const T::Internal,
    storage: ReaderStorage<'vec>,
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

impl<T: VectorElement> Copy for VectorReader<'_, T> {}

impl<T: VectorElement> Clone for VectorReader<'_, T> {
    fn clone(&self) -> Self {
        *self
    }
}

#[derive(Debug, Clone, Copy)]
enum ReaderStorage<'vec> {
    Flat(FlatReader),
    Constant(ConstantReader),
    Dictionary(DictionaryReader<'vec>),
}

#[derive(Debug, Clone, Copy)]
struct FlatReader {
    validity: *const u64,
}

impl FlatReader {
    #[inline]
    unsafe fn physical_index_unchecked(&self, logical: usize) -> Option<usize> {
        let valid =
            self.validity.is_null() || unsafe { *self.validity.add(logical / 64) & (1u64 << (logical % 64)) != 0 };
        valid.then_some(logical)
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
struct DictionaryReader<'vec> {
    values: FlatReader,
    selection: &'vec [u32],
}

impl DictionaryReader<'_> {
    #[inline]
    unsafe fn physical_index_unchecked(&self, logical: usize) -> Option<usize> {
        let physical = unsafe { *self.selection.get_unchecked(logical) } as usize;
        unsafe { self.values.physical_index_unchecked(physical) }
    }
}

impl ReaderStorage<'_> {
    #[inline]
    unsafe fn physical_index_unchecked(&self, logical: usize) -> Option<usize> {
        match self {
            Self::Flat(reader) => unsafe { reader.physical_index_unchecked(logical) },
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
                is_null: !vector.is_empty() && view.is_null_physical(0),
            }),
            StorageKind::Dictionary => ReaderStorage::Dictionary(DictionaryReader {
                values: flat,
                selection: view.selection().expect("prepared dictionary has a selection"),
            }),
            StorageKind::Other => unreachable!("prepared vector has a readable storage kind"),
        };
        Self {
            vector,
            data: view.as_ptr(),
            storage,
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

    /// Return the physical data pointer, or null when no data is exposed.
    ///
    /// Use [`Self::selection`] to map logical rows to physical slots, and check
    /// [`Self::validity`] before reading a slot. The pointer is invalidated when
    /// the vector is resized, reshaped, or destroyed.
    #[inline(always)]
    pub fn as_ptr(&self) -> *const T::Internal {
        self.data
    }

    /// Borrow physical storage, or `None` when no data is exposed.
    ///
    /// Flat storage covers every row; constant storage contains one slot.
    /// Dictionary storage extends through the highest selected physical index,
    /// including any intervening unselected slots.
    ///
    /// # Safety
    /// Every exposed slot must be initialized as `T::Internal`, including NULL
    /// and unselected slots. Use [`Self::as_ptr`] to read only known-valid slots.
    ///
    /// ```compile_fail,E0133
    /// use duckdb_neo::vector::VectorReader;
    ///
    /// fn read(reader: &VectorReader<'_, i32>) {
    ///     let _ = reader.as_slice();
    /// }
    /// ```
    pub unsafe fn as_slice(&self) -> Option<&'vec [T::Internal]> {
        // SAFETY: The caller guarantees all physical slots are initialized.
        unsafe { self.view().as_slice() }
    }

    /// Borrow the physical validity mask, or `None` when every slot is valid.
    ///
    /// Bit `physical % 64` of word `physical / 64` marks a non-NULL slot.
    /// Use [`Self::selection`] to map logical rows before checking these bits.
    /// The mask covers the physical range described by [`Self::as_slice`].
    ///
    /// Borrowed masks keep the vector immutable, even after the reader is gone:
    ///
    /// ```compile_fail,E0502
    /// use duckdb_neo::{Result, vector::Vector};
    ///
    /// fn overwrite(vector: &mut Vector<'_, i32>) -> Result<()> {
    ///     let mask = vector.reader()?.validity();
    ///     vector.writer()?.set(0, None)?;
    ///     assert!(mask.is_none());
    ///     Ok(())
    /// }
    /// ```
    pub fn validity(&self) -> Option<&'vec [u64]> {
        self.view().validity()
    }

    /// Return the logical-to-physical mapping without materializing a buffer.
    pub fn selection(&self) -> Selection<'vec> {
        match self.storage {
            ReaderStorage::Flat(_) => Selection::Identity,
            ReaderStorage::Constant(_) => Selection::Constant,
            ReaderStorage::Dictionary(reader) => Selection::Dictionary(reader.selection),
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
            reader: *self,
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
        unsafe { reader.physical_index_unchecked(index) }?;
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

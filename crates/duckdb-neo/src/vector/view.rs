use std::marker::PhantomData;

use crate::{
    ffi,
    vector::{CONSTANT_SELECTION_LEN, StorageKind, VectorElement},
};

/// A typed, borrowed view over a vector's physical storage.
///
/// Maps logical row indices to physical positions, accounting for constant,
/// dictionary, and selection-vector layouts.
// `repr(C)` fixes the view's field layout; `T` only appears in its marker.
#[repr(C)]
#[derive(Debug)]
pub struct VectorView<T> {
    pub(crate) view: ffi::duckdb_v2_vector_view,
    pub(crate) kind: StorageKind,
    pub(crate) _marker: PhantomData<T>,
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

    /// Return whether the vector contains no logical rows.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
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

    pub(crate) fn is_null_physical(&self, physical: usize) -> bool {
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

    /// Return the vector's physical storage as mutable.
    ///
    /// Flat vectors contain one entry per logical row, constant vectors
    /// contain one entry, and dictionary vectors use [`Self::selection`] to
    /// map logical rows into this slice.
    ///
    /// # Safety
    ///
    /// As for [`Self::as_slice`], the caller must read only initialized and valid entries.
    /// Every entry written must be a valid value of the vector's type whose out-of-line data,
    /// such as a string's bytes, outlives the vector; do not copy such entries from another vector.
    pub unsafe fn as_slice_mut(&mut self) -> Option<&mut [T::Internal]> {
        if self.view.data.is_null() {
            None
        } else {
            Some(unsafe { std::slice::from_raw_parts_mut(self.view.data as *mut T::Internal, self.physical_len()) })
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

    /// Return a mutable pointer to the physical storage, or null when no data is exposed.
    pub fn as_mut_ptr(&mut self) -> *mut T::Internal {
        if self.view.data.is_null() {
            std::ptr::null_mut()
        } else {
            self.view.data as *mut T::Internal
        }
    }

    /// Return the logical-to-physical row mapping, if DuckDB supplied one.
    ///
    /// A constant vector maps every row to slot 0, but its selection is DuckDB's
    /// shared array of [`CONSTANT_SELECTION_LEN`] zeros, so it is cut to that length.
    pub fn selection(&self) -> Option<&[u32]> {
        if self.view.sel.is_null() {
            None
        } else {
            let mut len = self.view.count as usize;
            if self.kind == StorageKind::Constant {
                len = len.min(CONSTANT_SELECTION_LEN);
            }
            Some(unsafe { std::slice::from_raw_parts(self.view.sel, len) })
        }
    }

    /// Return the physical validity bitmask, or `None` when every entry is valid.
    pub fn validity(&self) -> Option<&[u64]> {
        if self.view.validity.is_null() {
            None
        } else {
            Some(unsafe { std::slice::from_raw_parts(self.view.validity, self.physical_len().div_ceil(64)) })
        }
    }

    /// Return the physical validity bitmask as a mutable slice, or `None` when every entry is valid.
    pub fn validity_mut(&mut self) -> Option<&mut [u64]> {
        if self.view.validity.is_null() {
            None
        } else {
            Some(unsafe {
                std::slice::from_raw_parts_mut(self.view.validity as *mut u64, self.physical_len().div_ceil(64))
            })
        }
    }

    /// Change only the element marker, preserving the existing storage view.
    pub(crate) fn cast_owned<U: VectorElement>(self) -> VectorView<U> {
        VectorView {
            view: self.view,
            kind: self.kind,
            _marker: PhantomData,
        }
    }
}

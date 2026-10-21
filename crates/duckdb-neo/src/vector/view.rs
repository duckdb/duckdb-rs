use std::{marker::PhantomData, ops::Deref};

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
    #[inline]
    pub unsafe fn physical_index_unchecked(&self, logical: usize) -> usize {
        match self.kind {
            StorageKind::Constant => 0,
            StorageKind::Flat if self.view.sel.is_null() => logical,
            StorageKind::Flat | StorageKind::Dictionary => unsafe { *self.view.sel.add(logical) as usize },
            StorageKind::Other => unreachable!("OTHER vectors have no readable view"),
        }
    }

    /// Map a logical row to its physical storage index.
    ///
    /// # Panics
    ///
    /// Panics if `logical` is outside the view.
    #[inline(always)]
    pub fn physical_index(&self, logical: usize) -> usize {
        assert!(
            logical < self.len(),
            "logical index {logical} out of bounds for vector view of length {}",
            self.len()
        );
        unsafe { self.physical_index_unchecked(logical) }
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
    /// Every entry in the returned slice must be initialized, including unselected and NULL slots.
    /// Read only entries that are valid for the vector's logical type. Use [`Self::as_ptr`]
    /// when the buffer contains uninitialized slots.
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

    /// Change only the element marker, preserving the existing storage view.
    pub(crate) fn cast_owned<U: VectorElement>(self) -> VectorView<U> {
        VectorView {
            view: self.view,
            kind: self.kind,
            _marker: PhantomData,
        }
    }
}

/// An exclusive borrow of a writable flat vector's storage.
///
/// Read access delegates to [`VectorView`], but the cached view cannot be replaced:
///
/// ```compile_fail,E0596
/// use duckdb_neo::{Result, vector::Vector};
///
/// fn swap_views(a: &mut Vector<'_, i32>, b: &mut Vector<'_, i32>) -> Result<()> {
///     let mut a = a.get_view_mut()?;
///     let mut b = b.get_view_mut()?;
///     std::mem::swap(&mut *a, &mut *b);
///     Ok(())
/// }
/// ```
///
/// The vector cannot be resized while its view is still in use:
///
/// ```compile_fail,E0499
/// use duckdb_neo::{Result, vector::Vector};
///
/// fn resize(vector: &mut Vector<'_, i32>) -> Result<()> {
///     let view = vector.get_view_mut()?;
///     vector.set_size(10)?;
///     assert_eq!(view.len(), 10);
///     Ok(())
/// }
/// ```
#[derive(Debug)]
pub struct VectorViewMut<'view, T> {
    pub(crate) view: &'view mut VectorView<T>,
}

impl<T> Deref for VectorViewMut<'_, T> {
    type Target = VectorView<T>;

    fn deref(&self) -> &Self::Target {
        self.view
    }
}

impl<T: VectorElement> VectorViewMut<'_, T> {
    /// Return mutable physical storage, or `None` when no data is exposed.
    ///
    /// # Safety
    /// Every entry must already be initialized, including NULL slots. Every value
    /// written must match the vector's logical type, and its out-of-line data must
    /// outlive all vector reads. Use [`Self::as_mut_ptr`] for uninitialized slots.
    pub unsafe fn as_slice_mut(&mut self) -> Option<&mut [T::Internal]> {
        if self.view.view.data.is_null() {
            None
        } else {
            Some(unsafe { std::slice::from_raw_parts_mut(self.view.view.data as *mut T::Internal, self.len()) })
        }
    }

    /// Return a mutable pointer to physical storage, or null when no data is exposed.
    pub fn as_mut_ptr(&mut self) -> *mut T::Internal {
        self.view.view.data as *mut T::Internal
    }

    pub fn write_at(&mut self, index: usize, value: T::Internal) {
        unsafe {
            std::ptr::write(self.as_mut_ptr().add(index), value);
        }
    }

    /// Return the writable validity mask, with one bit per row.
    ///
    /// # Safety
    /// Rows marked valid must contain initialized values of the vector's logical type.
    /// NULL STRUCT and ARRAY rows must also have NULL descendants.
    pub fn validity_mut(&mut self) -> Option<&mut [u64]> {
        if self.view.view.validity.is_null() {
            None
        } else {
            Some(unsafe {
                std::slice::from_raw_parts_mut(self.view.view.validity as *mut u64, self.len().div_ceil(64))
            })
        }
    }

    /// # Safety
    /// The view must have writable validity storage, and `index` must be in bounds.
    #[inline]
    pub unsafe fn set_row_validity_unchecked(&mut self, index: usize, is_valid: bool) {
        let word = unsafe { (self.view.view.validity as *mut u64).add(index / 64) };
        let mask = 1u64 << (index % 64);
        unsafe {
            if is_valid {
                *word |= mask;
            } else {
                *word &= !mask;
            }
        }
    }
}

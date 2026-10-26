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
pub(crate) struct VectorView<T> {
    pub(crate) view: ffi::duckdb_v2_vector_view,
    pub(crate) kind: StorageKind,
    pub(crate) _marker: PhantomData<T>,
}

impl<T: VectorElement> VectorView<T> {
    /// Return the number of logical rows in the view.
    pub fn len(&self) -> usize {
        self.view.count as usize
    }

    /// Return the physical storage span, including unselected dictionary slots.
    /// Flat uses the row count, constant uses one slot, and dictionary ends at its highest selected index.
    pub fn physical_len(&self) -> usize {
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

    /// Return a pointer to the physical storage, or null when no data is exposed.
    pub fn data<'data>(&self) -> *const T::Internal<'data> {
        self.view.data.cast()
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

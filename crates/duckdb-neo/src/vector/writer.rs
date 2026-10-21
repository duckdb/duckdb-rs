use crate::{Result, logical_type::LogicalTypeID};

use super::{
    Arena, HasDuckDBBytes, StorageKind, Vector, VectorElement, VectorView, WritableVectorElement, not_flat,
    not_writable, out_of_bounds,
};

/// Exclusive access to a writable flat vector.
///
/// [`Self::set`] encodes typed values and maintains their validity. Raw data
/// access leaves validity to the caller; raw slices and manual validity changes
/// are unsafe because DuckDB trusts initialized, correctly typed values.
///
/// Resize the vector before acquiring its writer:
///
/// ```
/// # use duckdb_neo::{DuckDBType, Result, data_chunk::DataChunk, environment::{Environment, StorageLocation}};
/// # fn main() -> Result<()> {
/// # let env = Environment::new()?;
/// # let conn = env.open(StorageLocation::InMemory)?.connect()?;
/// let mut chunk = DataChunk::create(&conn, &[i32::logical_type(&conn)?], true)?;
/// let mut vector = chunk.get_vector_at_mut::<i32>(0)?;
/// vector.set_size(3)?;
/// {
///     let mut writer = vector.writer()?;
///     let data = writer.as_mut_ptr();
///     // SAFETY: All three slots are in bounds and contain INTEGER values.
///     unsafe {
///         for index in 0..writer.len() {
///             data.add(index).write(index as i32);
///         }
///         writer.fill_validity(true)?;
///         writer.set_row_validity(1, false)?;
///     }
/// }
/// assert_eq!(vector.reader()?.iter().collect::<Vec<_>>(), [Some(&0), None, Some(&2)]);
/// # Ok(())
/// # }
/// ```
///
/// The vector cannot be resized while its writer is in use:
///
/// ```compile_fail,E0499
/// use duckdb_neo::{Result, vector::Vector};
///
/// fn resize(vector: &mut Vector<'_, i32>) -> Result<()> {
///     let mut writer = vector.writer()?;
///     vector.set_size(10)?;
///     writer.set(0, Some(42))?;
///     Ok(())
/// }
/// ```
#[derive(Debug)]
pub struct VectorWriter<'writer, 'chunk, T: VectorElement> {
    inner: FlatWriter<'writer, 'chunk, T>,
}

#[derive(Debug)]
struct FlatWriter<'writer, 'chunk, T: VectorElement> {
    vector: &'writer mut Vector<'chunk, T>,
}

impl<'writer, 'chunk, T: VectorElement> VectorWriter<'writer, 'chunk, T> {
    pub(super) fn new(vector: &'writer mut Vector<'chunk, T>) -> Result<Self> {
        if !vector.is_writable() {
            return Err(not_writable());
        }
        if vector.storage_kind() != StorageKind::Flat {
            return Err(not_flat());
        }
        vector.get_view()?;
        Ok(Self {
            inner: FlatWriter { vector },
        })
    }

    /// Return the number of writable rows.
    pub fn len(&self) -> usize {
        self.inner.vector.len()
    }

    /// Return whether the writer contains no rows.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Return the mutable data pointer, or null when the type exposes no data.
    ///
    /// Writing through this pointer does not change validity. Only access rows
    /// within [`Self::len`], using the physical representation of the logical
    /// type. Typed writes may replace storage; reacquire the pointer afterwards.
    pub fn as_mut_ptr(&mut self) -> *mut T::Internal {
        self.view().as_ptr() as *mut T::Internal
    }

    /// Borrow mutable physical storage, or `None` when no data is exposed.
    ///
    /// Changes to these values do not change validity.
    ///
    /// # Safety
    /// Every slot must already be initialized, including NULL slots. Written
    /// values must match the logical type, including UTF-8 and nested offsets,
    /// and any referenced storage must outlive all reads from the vector.
    ///
    /// ```compile_fail,E0499
    /// use duckdb_neo::{Result, vector::Vector};
    ///
    /// fn overwrite(vector: &mut Vector<'_, i32>) -> Result<()> {
    ///     let mut writer = vector.writer()?;
    ///     writer.set(0, Some(1))?;
    ///     let data = unsafe { writer.as_slice_mut() }.unwrap();
    ///     writer.set(0, Some(2))?;
    ///     data[0] = 3;
    ///     Ok(())
    /// }
    /// ```
    pub unsafe fn as_slice_mut(&mut self) -> Option<&mut [T::Internal]> {
        let len = self.len();
        let data = self.as_mut_ptr();
        if data.is_null() {
            None
        } else {
            // SAFETY: The writer owns flat storage, and the caller guarantees initialization.
            Some(unsafe { std::slice::from_raw_parts_mut(data, len) })
        }
    }

    /// Borrow the physical validity mask, or `None` when every row is valid.
    pub fn validity(&self) -> Option<&[u64]> {
        self.view().validity()
    }

    /// Borrow a writable validity mask, allocating it on first use.
    ///
    /// Bit `index % 64` of word `index / 64` marks a non-NULL row.
    ///
    /// # Safety
    /// Rows marked valid must contain initialized values of the logical type.
    /// NULL STRUCT, ARRAY, and UNION rows must have NULL descendants. This only
    /// changes the parent's mask; it does not update children.
    ///
    /// A borrowed mask cannot survive a typed write that might replace it:
    ///
    /// ```compile_fail,E0499
    /// use duckdb_neo::{Result, vector::Vector};
    ///
    /// fn overwrite(vector: &mut Vector<'_, i32>) -> Result<()> {
    ///     let mut writer = vector.writer()?;
    ///     writer.set(0, Some(1))?;
    ///     let mask = unsafe { writer.validity_mut()? };
    ///     writer.set(0, None)?;
    ///     mask[0] = 0;
    ///     Ok(())
    /// }
    /// ```
    ///
    /// ```compile_fail,E0133
    /// use duckdb_neo::{Result, vector::Vector};
    ///
    /// fn validity(vector: &mut Vector<'_, i32>) -> Result<()> {
    ///     vector.writer()?.validity_mut()?;
    ///     Ok(())
    /// }
    /// ```
    pub unsafe fn validity_mut(&mut self) -> Result<&mut [u64]> {
        self.inner.vector.get_view_mut()?;
        let len = self.len().div_ceil(64);
        if len == 0 {
            return Ok(&mut []);
        }
        let words = self.view().view.validity as *mut u64;
        assert!(!words.is_null(), "writable validity mask is missing");
        // SAFETY: DuckDB supplied a writable mask, exclusively borrowed through the writer.
        Ok(unsafe { std::slice::from_raw_parts_mut(words, len) })
    }

    /// Set a row's validity without changing its data or descendants.
    ///
    /// # Safety
    /// A row marked valid must contain an initialized value of the logical type.
    /// A NULL STRUCT, ARRAY, or UNION row must have NULL descendants.
    pub unsafe fn set_row_validity(&mut self, index: usize, is_valid: bool) -> Result<()> {
        if index >= self.len() {
            return Err(out_of_bounds(index, self.len()));
        }
        // SAFETY: The writer is flat and writable, and the row is in bounds.
        unsafe { self.inner.vector.set_row_validity_unchecked(index, is_valid) }
    }

    /// Set every row's validity, including padding bits in the last word.
    ///
    /// # Safety
    /// Rows marked valid must contain initialized values of the logical type.
    /// NULL STRUCT, ARRAY, and UNION rows must have NULL descendants.
    pub unsafe fn fill_validity(&mut self, is_valid: bool) -> Result<()> {
        // SAFETY: The caller upholds the validity and descendant invariants.
        unsafe { self.validity_mut()? }.fill(if is_valid { u64::MAX } else { 0 });
        Ok(())
    }

    fn view(&self) -> &VectorView<T> {
        self.inner.vector.view.as_ref().expect("writer has a flat view")
    }
}

impl<T: WritableVectorElement> VectorWriter<'_, '_, T> {
    /// Encode a value and update its validity. `None` writes SQL NULL.
    ///
    /// Nested NULLs propagate to descendants. Logical value checks, such as
    /// decimal range and array length, remain enforced.
    #[inline]
    pub fn set(&mut self, index: usize, value: Option<T::Write<'_>>) -> Result<()> {
        if index >= self.len() {
            return Err(out_of_bounds(index, self.len()));
        }
        // SAFETY: The writer is flat and writable, and the row is in bounds.
        unsafe { self.set_unchecked(index, value) }
    }

    /// Encode a value and update its validity without checking bounds.
    ///
    /// Logical value checks still apply.
    ///
    /// # Safety
    /// `index` must be less than [`Self::len`].
    #[inline]
    pub unsafe fn set_unchecked(&mut self, index: usize, value: Option<T::Write<'_>>) -> Result<()> {
        // SAFETY: The writer guarantees flat, writable storage; the caller guarantees bounds.
        unsafe { write_unchecked(self.inner.vector, index, value) }
    }
}

impl<T: HasDuckDBBytes> VectorWriter<'_, '_, T> {
    /// Borrow the vector's byte arena for raw string or binary encoding.
    ///
    /// Take [`Self::as_mut_ptr`] before borrowing the arena to fill data slots.
    ///
    /// ```compile_fail,E0599
    /// use duckdb_neo::{Result, vector::Vector};
    ///
    /// fn arena(vector: &mut Vector<'_, i32>) -> Result<()> {
    ///     let mut writer = vector.writer()?;
    ///     let _ = writer.arena()?;
    ///     Ok(())
    /// }
    /// ```
    pub fn arena(&mut self) -> Result<&Arena> {
        self.inner.vector.arena()
    }
}

#[inline]
pub(super) unsafe fn write_unchecked<T: WritableVectorElement>(
    vector: &mut Vector<'_, T>,
    index: usize,
    value: Option<T::Write<'_>>,
) -> Result<()> {
    let is_valid = value.is_some();
    // SAFETY: The caller guarantees flat, writable storage and an in-bounds row.
    unsafe { T::write(vector, index, value)? };
    // VARIANT's value API determines nullness after casting the supplied Value.
    if T::TYPE_ID != LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_VARIANT {
        // SAFETY: The caller guarantees flat, writable storage and an in-bounds row.
        unsafe { vector.set_row_validity_unchecked(index, is_valid)? };
    }
    Ok(())
}

#[cfg(test)]
mod tests;

use std::{marker::PhantomData, ptr::NonNull};

use crate::{Result, check_api_call, ffi, logical_type::LogicalTypeID};

use super::{
    Arena, DuckDBBytes, HasDuckDBBytes, OwnedVectorElement, StorageKind, TypeMetadata, Unknown, Vector, VectorElement,
    WritableVectorElement, not_flat, not_writable, out_of_bounds,
};

/// Exclusive access to a writable flat vector.
///
/// [`Self::set`] encodes typed values and maintains their validity. Raw data
/// access leaves validity to the caller; raw slices and manual validity changes
/// are unsafe because DuckDB trusts initialized, correctly typed values.
/// Child writer state is prepared on the first child write and invalidated by reshaping
/// or slow-path writes that can replace buffers.
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
///
/// The writer cannot outlive the vector wrapper it borrows:
///
/// ```compile_fail,E0515
/// use duckdb_neo::{Result, vector::{Vector, VectorWriter}};
///
/// fn escape<'a>(mut vector: Vector<'a, i32>) -> Result<VectorWriter<'a, i32>> {
///     vector.writer()
/// }
/// ```
#[repr(C)]
#[derive(Debug)]
pub struct VectorWriter<'writer, T: VectorElement> {
    vector: NonNull<Vector<'writer, T>>,
    validity: *mut u64,
    data: *mut std::ffi::c_void,
    child_writers: Option<Vec<VectorWriter<'writer, Unknown>>>,
    _borrow: PhantomData<&'writer mut T>,
}

impl<'writer, T: VectorElement> VectorWriter<'writer, T> {
    pub(super) fn new(vector: &'writer mut Vector<'_, T>) -> Result<Self> {
        // SAFETY: the exclusive borrow covers this writer and every cached descendant.
        unsafe { Self::from_raw(NonNull::from(vector).cast()) }
    }

    unsafe fn from_raw(mut vector: NonNull<Vector<'writer, T>>) -> Result<Self> {
        // SAFETY: the caller exclusively borrows this vector for the writer's lifetime.
        let borrowed = unsafe { vector.as_mut() };
        if !borrowed.is_writable() {
            return Err(not_writable());
        }
        if borrowed.storage_kind() != StorageKind::Flat {
            return Err(not_flat());
        }
        let validity = borrowed.writable_validity()?;
        let data = borrowed.view.as_ref().ok_or_else(not_flat)?.view.data.cast_mut();
        Ok(Self {
            vector,
            validity,
            data,
            child_writers: None,
            _borrow: PhantomData,
        })
    }

    fn prepare_children(&mut self) -> Result<()> {
        let len = self.len();
        let align_fields = self.metadata() == TypeMetadata::Struct;
        let prepared = match self.vector_mut().children_collection_mut() {
            Some(children) => children
                .cached_iter_mut()?
                .map(|child| {
                    if align_fields && child.len() != len {
                        child.set_size(len)?;
                    }
                    // SAFETY: the iterator yields disjoint children, invalidated before any reshaping.
                    unsafe { VectorWriter::from_raw(NonNull::from(child).cast()) }
                })
                .collect::<Result<Vec<_>>>()?,
            None => Vec::new(),
        };
        self.child_writers = Some(prepared);
        Ok(())
    }

    /// Write a previously validated child through its cached writer state.
    ///
    /// # Safety
    /// `child_index` must name a child validated as `U`, and `index` must be in bounds
    /// for that child. Child types must stay fixed for this writer's lifetime.
    #[doc(hidden)]
    #[inline]
    pub unsafe fn write_child_unchecked<U: WritableVectorElement + 'writer>(
        &mut self,
        child_index: usize,
        index: usize,
        value: Option<U::Write<'_>>,
    ) -> Result<()> {
        // SAFETY: the caller guarantees the child's type and row bounds.
        unsafe {
            self.child_writer_unchecked::<U>(child_index)?
                .set_unchecked(index, value)
        }
    }

    /// Write a borrowed field through its cached child writer.
    ///
    /// # Safety
    /// The child must be validated as `U`, with `index` in bounds and a fixed type.
    #[doc(hidden)]
    #[inline]
    pub unsafe fn write_child_ref_unchecked<U: OwnedVectorElement + 'writer>(
        &mut self,
        child_index: usize,
        index: usize,
        value: Option<&U>,
    ) -> Result<()> {
        // SAFETY: the caller guarantees the child's type and row bounds.
        unsafe { U::write_ref(self.child_writer_unchecked::<U>(child_index)?, index, value) }
    }

    #[inline]
    pub(crate) unsafe fn child_writer_unchecked<U: VectorElement + 'writer>(
        &mut self,
        child_index: usize,
    ) -> Result<&mut VectorWriter<'writer, U>> {
        if self.child_writers.is_none() {
            self.prepare_children()?;
        }
        let child = &mut self.child_writers.as_mut().expect("child writers are prepared")[child_index];
        // SAFETY: the caller guarantees that this child was validated as U.
        Ok(unsafe { child.cast_unchecked() })
    }

    #[inline]
    pub(crate) unsafe fn cast_unchecked<U: VectorElement + 'writer>(&mut self) -> &mut VectorWriter<'writer, U> {
        // SAFETY: repr(C) fixes the layout; U changes only the vector pointer's type and borrow marker.
        unsafe { &mut *(self as *mut Self).cast::<VectorWriter<'writer, U>>() }
    }

    /// Return the number of writable rows.
    pub fn len(&self) -> usize {
        self.vector().len()
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
    /// The caller chooses `'data` and must uphold it when dereferencing the pointer.
    pub fn as_mut_ptr<'data>(&mut self) -> *mut T::Internal<'data> {
        self.data.cast()
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
    pub unsafe fn as_slice_mut(&mut self) -> Option<&mut [T::Internal<'_>]> {
        let len = self.len();
        let data = self.as_mut_ptr();
        if data.is_null() {
            None
        } else {
            // SAFETY: The writer owns flat storage, and the caller guarantees initialization.
            Some(unsafe { std::slice::from_raw_parts_mut(data, len) })
        }
    }

    /// Borrow the writable validity mask.
    ///
    /// Bit `index % 64` of word `index / 64` marks a non-NULL row.
    ///
    /// # Safety
    /// Rows marked valid must contain initialized values of the logical type
    /// before any read. NULL STRUCT, ARRAY, and UNION rows must have NULL descendants. This only
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
        let len = self.len().div_ceil(64);
        // SAFETY: DuckDB supplied a writable mask, exclusively borrowed through the writer.
        Ok(unsafe { std::slice::from_raw_parts_mut(self.validity, len) })
    }

    /// Set a row's validity without changing its data or descendants.
    ///
    /// # Safety
    /// A row marked valid must contain an initialized value of the logical type
    /// before any read. A NULL STRUCT, ARRAY, or UNION row must have NULL descendants.
    pub unsafe fn set_row_validity(&mut self, index: usize, is_valid: bool) -> Result<()> {
        if index >= self.len() {
            return Err(out_of_bounds(index, self.len()));
        }
        // SAFETY: The writer is flat and writable, and the row is in bounds.
        unsafe { self.set_validity_unchecked(index, is_valid) };
        Ok(())
    }

    /// Set every row's validity, including padding bits in the last word.
    ///
    /// # Safety
    /// Rows marked valid must contain initialized values of the logical type
    /// before any read. NULL STRUCT, ARRAY, and UNION rows must have NULL descendants.
    pub unsafe fn fill_validity(&mut self, is_valid: bool) -> Result<()> {
        // SAFETY: The caller upholds the validity and descendant invariants.
        unsafe { self.validity_mut()? }.fill(if is_valid { u64::MAX } else { 0 });
        Ok(())
    }

    pub(crate) fn metadata(&self) -> TypeMetadata {
        self.vector().metadata()
    }

    pub(crate) fn children(&self) -> Option<&crate::data_chunk::VectorCollection> {
        self.vector().children()
    }

    pub(crate) fn child_offset_cell(&self) -> &std::cell::Cell<usize> {
        self.vector().child_offset_cell()
    }

    pub(crate) fn children_collection_mut(&mut self) -> Option<&mut crate::data_chunk::VectorCollection> {
        self.vector_mut().children_collection_mut()
    }

    pub(crate) fn set_null_slow(&mut self, index: usize) -> Result<()> {
        self.vector_mut().set_null_slow(index)?;
        self.refresh_buffers()
    }

    pub(crate) fn write_value_slow(&mut self, index: usize, value: crate::value::Value) -> Result<()> {
        self.vector_mut().write_value_slow(index, value)?;
        self.refresh_buffers()
    }

    #[inline(always)]
    pub(crate) unsafe fn write_internal<U>(&mut self, index: usize, value: Option<U>) -> Result<()> {
        if let Some(value) = value {
            let data = self.data.cast::<U>();
            // SAFETY: The caller guarantees the row's bounds and physical representation.
            unsafe { data.add(index).write(value) };
        }
        Ok(())
    }

    fn vector(&self) -> &Vector<'writer, T> {
        // SAFETY: construction retains the vector's borrow, and this reference cannot outlive self.
        unsafe { self.vector.as_ref() }
    }

    fn vector_mut(&mut self) -> &mut Vector<'writer, T> {
        self.child_writers = None;
        // SAFETY: cached descendant pointers are gone before borrowing the vector for mutation.
        unsafe { self.vector.as_mut() }
    }
}

impl<T: WritableVectorElement> VectorWriter<'_, T> {
    /// Encode a value and update its validity. `None` writes SQL NULL.
    ///
    /// Nested NULLs propagate to descendants. Logical value checks, such as
    /// decimal range and array length, remain enforced.
    #[inline(always)]
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
    #[inline(always)]
    pub unsafe fn set_unchecked(&mut self, index: usize, value: Option<T::Write<'_>>) -> Result<()> {
        let is_valid = value.is_some();
        // SAFETY: The writer guarantees flat, writable storage; the caller guarantees bounds.
        unsafe { T::write(self, index, value)? };
        // VARIANT's value API determines nullness after casting the supplied Value.
        if T::TYPE_ID != LogicalTypeID::Variant {
            unsafe { self.set_validity_unchecked(index, is_valid) };
        }
        Ok(())
    }
}

impl<T: OwnedVectorElement> VectorWriter<'_, T> {
    /// Write a borrowed Rust value and update its validity. `None` writes SQL NULL.
    ///
    /// Strings and nested data are copied into the vector, so `value` is not moved or cloned.
    #[inline]
    pub fn set_ref(&mut self, index: usize, value: Option<&T>) -> Result<()> {
        if index >= self.len() {
            return Err(out_of_bounds(index, self.len()));
        }
        // SAFETY: The writer is flat and writable, and the row is in bounds.
        unsafe { T::write_ref(self, index, value) }
    }
}

impl<T: VectorElement> VectorWriter<'_, T> {
    #[inline]
    fn refresh_buffers(&mut self) -> Result<()> {
        let vector = self.vector_mut();
        let validity = vector.writable_validity()?;
        let data = vector.view.as_ref().ok_or_else(not_flat)?.view.data.cast_mut();
        self.validity = validity;
        self.data = data;
        Ok(())
    }

    #[inline(always)]
    pub(crate) unsafe fn set_validity_unchecked(&mut self, index: usize, is_valid: bool) {
        // SAFETY: The owning writer guarantees flat, writable storage; the caller guarantees bounds.
        let word = unsafe { self.validity.add(index / 64) };
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

impl VectorWriter<'_, String> {
    /// Write concatenated UTF-8 fragments; `None` writes NULL and an empty slice writes an empty string.
    /// Fails if `index` is out of bounds or the combined byte length exceeds `u32::MAX`.
    #[inline(always)]
    pub fn set_parts(&mut self, index: usize, parts: Option<&[&str]>) -> Result<()> {
        if index >= self.len() {
            return Err(out_of_bounds(index, self.len()));
        }
        if let Some(parts) = parts {
            // SAFETY: The row is in bounds, and concatenated UTF-8 is valid VARCHAR storage.
            unsafe { self.write_byte_parts(index, parts)? };
        }
        // SAFETY: Non-NULL rows are initialized; VARCHAR has no descendants.
        unsafe { self.set_validity_unchecked(index, parts.is_some()) };
        Ok(())
    }
}

impl VectorWriter<'_, crate::types::UncheckedString> {
    /// Concatenate UTF-8 fragments, as with [`VectorWriter::<String>::set_parts`].
    #[inline(always)]
    pub fn set_parts(&mut self, index: usize, parts: Option<&[&str]>) -> Result<()> {
        if index >= self.len() {
            return Err(out_of_bounds(index, self.len()));
        }
        if let Some(parts) = parts {
            // SAFETY: The row is in bounds, and concatenated UTF-8 is valid VARCHAR storage.
            unsafe { self.write_byte_parts(index, parts)? };
        }
        // SAFETY: Non-NULL rows are initialized; VARCHAR has no descendants.
        unsafe { self.set_validity_unchecked(index, parts.is_some()) };
        Ok(())
    }
}

impl VectorWriter<'_, crate::types::BlobValue> {
    /// Write concatenated BLOB fragments; `None` writes NULL and an empty slice writes an empty blob.
    /// Fails if `index` is out of bounds or the combined byte length exceeds `u32::MAX`.
    #[inline(always)]
    pub fn set_parts(&mut self, index: usize, parts: Option<&[&[u8]]>) -> Result<()> {
        if index >= self.len() {
            return Err(out_of_bounds(index, self.len()));
        }
        if let Some(parts) = parts {
            // SAFETY: The row is in bounds, and arbitrary bytes are valid BLOB storage.
            unsafe { self.write_byte_parts(index, parts)? };
        }
        // SAFETY: Non-NULL rows are initialized; BLOB has no descendants.
        unsafe { self.set_validity_unchecked(index, parts.is_some()) };
        Ok(())
    }
}

impl<T: HasDuckDBBytes> VectorWriter<'_, T> {
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
        let vector = self.vector_mut();
        if vector.heap.is_none() {
            let handle = check_api_call!(ffi::duckdb_v2_vector_get_arena, vector.handle, RET)?;
            vector.heap = Some(Arena { handle });
        }
        Ok(vector.heap.as_ref().unwrap())
    }

    #[inline]
    pub(crate) unsafe fn write_bytes(&mut self, index: usize, value: Option<&[u8]>) -> Result<()> {
        if let Some(value) = value {
            // SAFETY: The caller guarantees the row's bounds and logical encoding.
            unsafe { self.write_byte_parts(index, &[value])? };
        }
        Ok(())
    }

    #[inline(always)]
    unsafe fn write_byte_parts<P: AsRef<[u8]> + ?Sized>(&mut self, index: usize, parts: &[&P]) -> Result<()> {
        let data = self.data.cast::<DuckDBBytes<'_>>();
        let encoded = DuckDBBytes::from_parts(parts, || self.arena())?;
        // SAFETY: The row is in bounds, and the bytes belong to this vector's arena.
        unsafe { data.add(index).write(encoded) };
        Ok(())
    }
}

#[cfg(test)]
mod tests;

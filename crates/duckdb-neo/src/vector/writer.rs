use std::{marker::PhantomData, ptr::NonNull};

use crate::raw::RawExt;
use crate::{Result, check_api_call, ffi, logical_type::LogicalTypeID};

use super::{
    Arena, DuckDBBytes, HasDuckDBBytes, NativeElement, PlainNative, Selection, StorageKind, Vector, VectorReader,
    WritableNativeElement, not_flat, not_writable, out_of_bounds,
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
///     writer.set(0, &42)?;
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
pub struct VectorWriter<'writer, T: NativeElement> {
    vector: NonNull<Vector<'writer, T>>,
    len: usize,
    validity: *mut u64,
    data: *mut std::ffi::c_void,
    _borrow: PhantomData<&'writer mut T>,
}

impl<'writer, T: NativeElement> VectorWriter<'writer, T> {
    pub(super) fn new(vector: &'writer mut Vector<'_, T>) -> Result<Self> {
        // SAFETY: the exclusive borrow covers this writer and every cached descendant.
        unsafe { Self::from_raw(NonNull::from(vector).cast()) }
    }

    /// # Safety
    /// The vector must stay exclusively borrowed, and not be rewrapped, for the writer's lifetime.
    pub(crate) unsafe fn from_raw(mut vector: NonNull<Vector<'writer, T>>) -> Result<Self> {
        // SAFETY: the caller exclusively borrows this vector for the writer's lifetime.
        let borrowed = unsafe { vector.as_mut() };
        if !borrowed.is_writable() {
            return Err(not_writable());
        }
        if borrowed.storage_kind() != StorageKind::Flat {
            return Err(not_flat());
        }

        // Allocating the writable mask can replace the validity pointer of the cached view.
        borrowed.view.take();
        let validity = check_api_call!(ffi::duckdb_v2_vector_flat_get_validity_mutable, borrowed.raw(), RET)?;
        let data = check_api_call!(ffi::duckdb_v2_vector_get_data_mutable, borrowed.raw(), RET)?;

        Ok(Self {
            vector,
            len: borrowed.len(),
            validity,
            data,
            _borrow: PhantomData,
        })
    }

    /// Return the number of writable rows.
    pub fn len(&self) -> usize {
        self.len
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
    pub fn as_mut_ptr<'data>(&mut self) -> *mut T::Native<'data> {
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
    ///     writer.set(0, &1)?;
    ///     let data = unsafe { writer.as_slice_mut() }.unwrap();
    ///     writer.set(0, &2)?;
    ///     data[0] = 3;
    ///     Ok(())
    /// }
    /// ```
    pub unsafe fn as_slice_mut(&mut self) -> Option<&mut [T::Native<'_>]> {
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
    ///     writer.set(0, &1)?;
    ///     let mask = unsafe { writer.validity_mut()? };
    ///     writer.set_null(0)?;
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

    pub(crate) fn set_null_slow(&mut self, index: usize) -> Result<()> {
        let result = self.vector_mut().set_null_slow(index);
        self.refresh_buffers()?;
        result
    }

    pub(crate) fn write_value_slow(&mut self, index: usize, value: &crate::value::Value) -> Result<()> {
        let result = self.vector_mut().write_value_slow(index, value);
        self.refresh_buffers()?;
        result
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

    /// Reacquire the buffers and size after an ancestor resized or rewrote this vector.
    pub(crate) fn refresh_all(&mut self) -> Result<()> {
        self.len = self.vector().len();
        self.refresh_buffers()
    }

    /// Resize the vector like [`Vector::set_size`] and reacquire its buffers.
    pub(crate) fn resize(&mut self, len: usize) -> Result<()> {
        let result = self.vector_mut().set_size(len);
        self.len = self.vector().len();
        self.refresh_buffers()?;
        result
    }

    pub(crate) fn vector(&self) -> &Vector<'writer, T> {
        // SAFETY: construction retains the vector's borrow, and this reference cannot outlive self.
        unsafe { self.vector.as_ref() }
    }

    pub(crate) fn vector_mut(&mut self) -> &mut Vector<'writer, T> {
        // SAFETY: construction retains the vector's exclusive borrow, and this reference cannot outlive self.
        unsafe { self.vector.as_mut() }
    }

    /// NULL rows `start..end` of child `child` and their descendants.
    pub(crate) fn null_child_rows(&mut self, child: usize, start: usize, end: usize) -> Result<()> {
        self.vector_mut()
            .children_collection_mut()
            .expect("nested vectors have children")
            .cached_iter_mut()?
            .nth(child)
            .expect("validated child exists")
            .null_rows(start, end)
    }

    /// NULL `row` of every child, as a NULL STRUCT or UNION row requires.
    pub(crate) fn null_children_row(&mut self, row: usize) -> Result<()> {
        if let Some(children) = self.vector_mut().children_collection_mut() {
            for child in children.cached_iter_mut()? {
                child.null_rows(row, row + 1)?;
            }
        }
        Ok(())
    }
}

impl<T: WritableNativeElement> VectorWriter<'_, T> {
    /// Copy a borrowed value into the vector and mark the row valid.
    /// Logical value checks remain enforced.
    #[inline(always)]
    pub fn set(&mut self, index: usize, value: &T::Write) -> Result<()> {
        if index >= self.len() {
            return Err(out_of_bounds(index, self.len()));
        }
        // SAFETY: The writer is flat and writable, and the row is in bounds.
        unsafe { self.set_unchecked(index, value) }
    }

    /// Make a row SQL NULL, along with the descendants DuckDB requires to be NULL.
    #[inline(always)]
    pub fn set_null(&mut self, index: usize) -> Result<()> {
        if index >= self.len() {
            return Err(out_of_bounds(index, self.len()));
        }
        // SAFETY: The writer is flat and writable, and the row is in bounds.
        unsafe { self.set_null_unchecked(index) }
    }

    /// Write a row; `None` writes SQL NULL.
    #[inline(always)]
    pub fn set_option(&mut self, index: usize, value: Option<&T::Write>) -> Result<()> {
        match value {
            Some(value) => self.set(index, value),
            None => self.set_null(index),
        }
    }

    /// Encode a value and mark the row valid without checking bounds.
    ///
    /// Logical value checks still apply.
    ///
    /// # Safety
    /// `index` must be less than [`Self::len`].
    #[inline(always)]
    pub unsafe fn set_unchecked(&mut self, index: usize, value: &T::Write) -> Result<()> {
        // SAFETY: The writer guarantees flat, writable storage; the caller guarantees bounds.
        unsafe { T::write(self, index, value)? };
        // VARIANT's value API determines nullness after casting the supplied Value.
        if T::TYPE_ID != LogicalTypeID::Variant {
            // SAFETY: as above; the row now holds a value.
            unsafe { self.set_validity_unchecked(index, true) };
        }
        Ok(())
    }

    /// Make a row SQL NULL without checking bounds.
    ///
    /// # Safety
    /// `index` must be less than [`Self::len`].
    #[inline(always)]
    pub unsafe fn set_null_unchecked(&mut self, index: usize) -> Result<()> {
        // SAFETY: The writer guarantees flat, writable storage; the caller guarantees bounds.
        unsafe { T::write_null(self, index)? };
        if T::TYPE_ID != LogicalTypeID::Variant {
            // SAFETY: as above.
            unsafe { self.set_validity_unchecked(index, false) };
        }
        Ok(())
    }

    /// Write a row without checking bounds; `None` writes SQL NULL.
    ///
    /// # Safety
    /// `index` must be less than [`Self::len`].
    #[inline(always)]
    pub unsafe fn set_option_unchecked(&mut self, index: usize, value: Option<&T::Write>) -> Result<()> {
        // SAFETY: the caller guarantees bounds.
        unsafe {
            match value {
                Some(value) => self.set_unchecked(index, value),
                None => self.set_null_unchecked(index),
            }
        }
    }
}

impl<T: NativeElement> VectorWriter<'_, T> {
    #[inline]
    fn refresh_buffers(&mut self) -> Result<()> {
        self.validity = check_api_call!(
            ffi::duckdb_v2_vector_flat_get_validity_mutable,
            self.vector().raw(),
            RET
        )?;
        self.data = check_api_call!(ffi::duckdb_v2_vector_get_data_mutable, self.vector().raw(), RET)?;
        Ok(())
    }

    /// Mark rows `start..end` valid, a whole word at a time where possible.
    ///
    /// # Safety
    /// `end` must not exceed [`Self::len`], and the rows must hold initialized values.
    pub(crate) unsafe fn mark_valid_range(&mut self, start: usize, end: usize) {
        if start >= end {
            return;
        }
        let (first, last) = (start / 64, (end - 1) / 64);
        let low = u64::MAX << (start % 64);
        let high = u64::MAX >> (63 - (end - 1) % 64);
        // SAFETY: the caller guarantees the rows are within the mask.
        unsafe {
            if first == last {
                *self.validity.add(first) |= low & high;
                return;
            }
            *self.validity.add(first) |= low;
            std::slice::from_raw_parts_mut(self.validity.add(first + 1), last - first - 1).fill(u64::MAX);
            *self.validity.add(last) |= high;
        }
    }

    #[inline(always)]
    pub(crate) unsafe fn set_validity_unchecked(&mut self, index: usize, is_valid: bool) {
        // SAFETY: the caller guarantees bounds.
        unsafe {
            if is_valid {
                self.mark_valid_unchecked(index)
            } else {
                self.mark_null_unchecked(index)
            }
        }
    }

    #[inline(always)]
    unsafe fn mark_valid_unchecked(&mut self, index: usize) {
        // SAFETY: The owning writer guarantees flat, writable storage; the caller guarantees bounds.
        let word = unsafe { &mut *self.validity.add(index / 64) };
        let mask = 1u64 << (index % 64);
        // Masks start valid, so storing only to revive a NULL row keeps this branch predictable.
        if *word & mask == 0 {
            *word |= mask;
        }
    }

    #[inline(always)]
    unsafe fn mark_null_unchecked(&mut self, index: usize) {
        // SAFETY: The owning writer guarantees flat, writable storage; the caller guarantees bounds.
        unsafe { *self.validity.add(index / 64) &= !(1u64 << (index % 64)) };
    }
}

impl<T: NativeElement> VectorWriter<'_, T> {
    /// Copy `values` into rows `offset..offset + values.len()` and mark those rows valid.
    ///
    /// # Safety
    /// `T` must be the physical slot type, and every value must be a valid row.
    pub(crate) unsafe fn write_slice_unchecked(&mut self, offset: usize, values: &[T]) -> Result<()> {
        let end = offset
            .checked_add(values.len())
            .filter(|end| *end <= self.len())
            .ok_or_else(|| out_of_bounds(offset.saturating_add(values.len()).saturating_sub(1), self.len()))?;
        // SAFETY: the rows are in bounds, and the caller guarantees `T` and the values are valid rows.
        unsafe {
            std::ptr::copy_nonoverlapping(values.as_ptr(), self.data.cast::<T>().add(offset), values.len());
            self.mark_valid_range(offset, end);
        }
        Ok(())
    }
}

impl<T: PlainNative> VectorWriter<'_, T> {
    /// Copy `values` into rows `offset..offset + values.len()` and mark those rows valid.
    ///
    /// ```
    /// # use duckdb_neo::{DuckDBType, Result, data_chunk::DataChunk, environment::{Environment, StorageLocation}};
    /// # fn main() -> Result<()> {
    /// # let conn = Environment::new()?.open(StorageLocation::InMemory)?.connect()?;
    /// let mut chunk = DataChunk::create(&conn, &[i32::logical_type(&conn)?], true)?;
    /// let mut vector = chunk.get_vector_at_mut::<i32>(0)?;
    /// vector.set_size(4)?;
    /// let mut writer = vector.writer()?;
    /// writer.write_slice(1, &[7, 8, 9])?;
    /// writer.set_null(2)?;
    /// # drop(writer);
    /// assert_eq!(vector.reader()?.iter().collect::<Vec<_>>(), [Some(&0), Some(&7), None, Some(&9)]);
    /// # Ok(())
    /// # }
    /// ```
    pub fn write_slice(&mut self, offset: usize, values: &[T]) -> Result<()> {
        // SAFETY: every value of a `PlainNative` type is a valid row.
        unsafe { self.write_slice_unchecked(offset, values) }
    }

    /// Copy every row of `source`, values and NULLs, into the first rows of this vector.
    ///
    /// Flat sources copy whole buffers; dictionary and constant sources copy row by row.
    pub fn copy_from(&mut self, source: &VectorReader<'_, T>) -> Result<()> {
        let len = source.len();
        if len > self.len() {
            return Err(out_of_bounds(len - 1, self.len()));
        }
        if len == 0 {
            return Ok(());
        }
        let data = self.data.cast::<T>();
        match source.selection() {
            Selection::Identity => {
                // SAFETY: both vectors hold at least `len` slots of `T`.
                unsafe { std::ptr::copy_nonoverlapping(source.as_ptr().cast::<T>(), data, len) };
                match source.validity() {
                    // SAFETY: the rows are in bounds and hold the values just copied.
                    None => unsafe { self.mark_valid_range(0, len) },
                    // SAFETY: as above; the source mask covers `len` rows.
                    Some(mask) => unsafe { self.copy_validity(mask, len) },
                }
            }
            selection => {
                let (slots, validity) = (source.as_ptr().cast::<T>(), source.validity());
                for row in 0..len {
                    let physical = selection.index(row);
                    let valid = validity.is_none_or(|mask| mask[physical / 64] & (1 << (physical % 64)) != 0);
                    // SAFETY: the row is in bounds, the selection maps it to a source slot, and `T` is
                    // the physical slot type.
                    unsafe {
                        if valid {
                            data.add(row).write(*slots.add(physical));
                            self.mark_valid_unchecked(row);
                        } else {
                            self.mark_null_unchecked(row);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// # Safety
    /// `mask` must cover `len` rows, and `len` must not exceed [`Self::len`].
    unsafe fn copy_validity(&mut self, mask: &[u64], len: usize) {
        let (words, tail) = (len / 64, len % 64);
        // SAFETY: the caller guarantees both masks cover `len` rows.
        unsafe {
            std::ptr::copy_nonoverlapping(mask.as_ptr(), self.validity, words);
            if tail != 0 {
                let keep = u64::MAX << tail;
                let word = self.validity.add(words);
                *word = (*word & keep) | (mask[words] & !keep);
            }
        }
    }
}

impl<T: WritableNativeElement> VectorWriter<'_, T> {
    /// Make every row SQL NULL, along with the descendants DuckDB requires to be NULL.
    pub fn fill_null(&mut self) -> Result<()> {
        if self.vector().children().is_none() && T::TYPE_ID != LogicalTypeID::Variant {
            // SAFETY: NULL rows need no data, and the vector has no descendants.
            return unsafe { self.fill_validity(false) };
        }
        for row in 0..self.len() {
            // SAFETY: the row is in bounds.
            unsafe { self.set_null_unchecked(row)? };
        }
        Ok(())
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

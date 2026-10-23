use crate::{Result, check_api_call, ffi, logical_type::LogicalTypeID};

use super::{
    Arena, DuckDBBytes, HasDuckDBBytes, StorageKind, TypeMetadata, Vector, VectorElement, VectorView,
    WritableVectorElement, not_flat, not_writable, out_of_bounds,
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
        vector.view.as_ref().ok_or_else(not_flat)?;
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
    /// The caller chooses `'data` and must uphold it when dereferencing the pointer.
    pub fn as_mut_ptr<'data>(&mut self) -> *mut T::Internal<'data> {
        self.view().as_ptr() as *mut T::Internal<'data>
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

    /// Borrow the physical validity mask, or `None` when every row is valid.
    pub fn validity(&self) -> Option<&[u64]> {
        self.view().validity()
    }

    /// Borrow a writable validity mask, allocating it on first use.
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
        self.inner.ensure_validity_writable()?;
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
    /// A row marked valid must contain an initialized value of the logical type
    /// before any read. A NULL STRUCT, ARRAY, or UNION row must have NULL descendants.
    pub unsafe fn set_row_validity(&mut self, index: usize, is_valid: bool) -> Result<()> {
        if index >= self.len() {
            return Err(out_of_bounds(index, self.len()));
        }
        // SAFETY: The writer is flat and writable, and the row is in bounds.
        unsafe { self.inner.set_validity_unchecked(index, is_valid) }
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
        self.inner.vector.metadata()
    }

    pub(crate) fn children(&self) -> Option<&crate::data_chunk::VectorCollection> {
        self.inner.vector.children()
    }

    pub(crate) fn children_collection_mut(&mut self) -> Option<&mut crate::data_chunk::VectorCollection> {
        self.inner.vector.children_collection_mut()
    }

    pub(crate) fn set_null_slow(&mut self, index: usize) -> Result<()> {
        self.inner.vector.set_null_slow(index)
    }

    pub(crate) fn write_value_slow(&mut self, index: usize, value: crate::value::Value) -> Result<()> {
        self.inner.vector.write_value_slow(index, value)
    }

    #[inline(always)]
    pub(crate) unsafe fn write_internal<U>(&mut self, index: usize, value: Option<U>) -> Result<()> {
        if let Some(value) = value {
            let data = self.view().as_ptr() as *mut U;
            // SAFETY: The caller guarantees the row's bounds and physical representation.
            unsafe { data.add(index).write(value) };
        }
        Ok(())
    }

    pub(crate) fn write_as<U: WritableVectorElement>(
        &mut self,
        index: usize,
        value: Option<U::Write<'_>>,
    ) -> Result<()> {
        if index >= self.len() {
            return Err(out_of_bounds(index, self.len()));
        }
        self.inner.vector.validate_as::<U>()?;
        // SAFETY: U was validated; the repr(C) wrappers differ only in element markers.
        let typed = unsafe { &mut *(self.inner.vector as *mut Vector<'_, T> as *mut Vector<'_, U>) };
        let mut writer = VectorWriter {
            inner: FlatWriter { vector: typed },
        };
        // SAFETY: This writer inherits flat, exclusive storage and checked bounds.
        unsafe { writer.set_unchecked(index, value) }
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
    #[inline(always)]
    pub unsafe fn set_unchecked(&mut self, index: usize, value: Option<T::Write<'_>>) -> Result<()> {
        let is_valid = value.is_some();
        // SAFETY: The writer guarantees flat, writable storage; the caller guarantees bounds.
        unsafe { T::write(self, index, value)? };
        // VARIANT's value API determines nullness after casting the supplied Value.
        if T::TYPE_ID != LogicalTypeID::Variant {
            unsafe { self.inner.set_validity_unchecked(index, is_valid)? };
        }
        Ok(())
    }
}

impl<T: VectorElement> FlatWriter<'_, '_, T> {
    fn ensure_validity_writable(&mut self) -> Result<()> {
        if !self.vector.validity_writable {
            let validity = check_api_call!(ffi::duckdb_v2_vector_flat_get_validity_mutable, self.vector.handle, RET)?;
            self.vector.view.as_mut().ok_or_else(not_flat)?.view.validity = validity;
            self.vector.validity_writable = true;
        }
        Ok(())
    }

    #[inline(always)]
    unsafe fn set_validity_unchecked(&mut self, index: usize, is_valid: bool) -> Result<()> {
        let view = self.vector.view.as_ref().ok_or_else(not_flat)?;
        if is_valid && view.view.validity.is_null() {
            return Ok(());
        }
        self.ensure_validity_writable()?;
        let validity = self.vector.view.as_ref().unwrap().view.validity as *mut u64;
        // SAFETY: The owning writer guarantees flat, writable storage; the caller guarantees bounds.
        let word = unsafe { validity.add(index / 64) };
        let mask = 1u64 << (index % 64);
        unsafe {
            if is_valid {
                *word |= mask;
            } else {
                *word &= !mask;
            }
        }
        Ok(())
    }
}

impl VectorWriter<'_, '_, String> {
    /// Concatenate UTF-8 fragments into one row and update its validity.
    ///
    /// `None` writes SQL NULL; `Some(&[])` writes an empty string. The fragments
    /// are copied directly into inline storage (up to 12 bytes) or one arena
    /// allocation, without an intermediate buffer or UTF-8 validation pass.
    /// Returns an error if `index` is out of bounds or the combined byte length
    /// exceeds `u32::MAX`.
    ///
    /// ```
    /// # use duckdb_neo::{DuckDBType, Result, data_chunk::DataChunk, environment::{Environment, StorageLocation}};
    /// # fn main() -> Result<()> {
    /// # let env = Environment::new()?;
    /// # let conn = env.open(StorageLocation::InMemory)?.connect()?;
    /// let mut chunk = DataChunk::create(&conn, &[String::logical_type(&conn)?], true)?;
    /// let mut vector = chunk.get_vector_at_mut::<String>(0)?;
    /// vector.set_size(3)?;
    /// {
    ///     let mut writer = vector.writer()?;
    ///     writer.set_parts(0, Some(&["hello", " ", "world"]))?;
    ///     writer.set_parts(1, Some(&[]))?;
    ///     writer.set_parts(2, None)?;
    /// }
    /// assert_eq!(vector.reader()?.iter().collect::<Vec<_>>(), [Some("hello world"), Some(""), None]);
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// String fragments cannot bypass other logical types' encoders:
    ///
    /// ```compile_fail,E0599
    /// use duckdb_neo::{Result, types::BitValue, vector::Vector};
    ///
    /// fn write(vector: &mut Vector<'_, BitValue>) -> Result<()> {
    ///     vector.writer()?.set_parts(0, Some(&["not a BIT encoding"]))
    /// }
    /// ```
    #[inline]
    pub fn set_parts(&mut self, index: usize, parts: Option<&[&str]>) -> Result<()> {
        if index >= self.len() {
            return Err(out_of_bounds(index, self.len()));
        }
        if let Some(parts) = parts {
            // SAFETY: The row is in bounds, and concatenated UTF-8 is valid VARCHAR storage.
            unsafe { self.write_byte_parts(index, parts)? };
        }
        // SAFETY: Non-NULL rows are initialized; VARCHAR has no descendants.
        unsafe { self.inner.set_validity_unchecked(index, parts.is_some()) }
    }
}

impl VectorWriter<'_, '_, crate::types::UnsafeString> {
    /// Concatenate UTF-8 fragments, as with [`VectorWriter::<String>::set_parts`].
    #[inline]
    pub fn set_parts(&mut self, index: usize, parts: Option<&[&str]>) -> Result<()> {
        if index >= self.len() {
            return Err(out_of_bounds(index, self.len()));
        }
        if let Some(parts) = parts {
            // SAFETY: The row is in bounds, and concatenated UTF-8 is valid VARCHAR storage.
            unsafe { self.write_byte_parts(index, parts)? };
        }
        // SAFETY: Non-NULL rows are initialized; VARCHAR has no descendants.
        unsafe { self.inner.set_validity_unchecked(index, parts.is_some()) }
    }
}

impl VectorWriter<'_, '_, crate::types::BlobValue> {
    /// Concatenate byte fragments into one BLOB row and update its validity.
    ///
    /// `None` writes SQL NULL; `Some(&[])` writes an empty blob. Arbitrary bytes,
    /// including embedded NULs and invalid UTF-8, are copied directly into inline
    /// storage (up to 12 bytes) or one arena allocation, without an intermediate
    /// buffer. Returns an error if `index` is out of bounds or the combined byte
    /// length exceeds `u32::MAX`.
    ///
    /// ```
    /// # use duckdb_neo::{DuckDBType, Result, data_chunk::DataChunk, environment::{Environment, StorageLocation}, types::BlobValue};
    /// # fn main() -> Result<()> {
    /// # let env = Environment::new()?;
    /// # let conn = env.open(StorageLocation::InMemory)?.connect()?;
    /// let mut chunk = DataChunk::create(&conn, &[BlobValue::logical_type(&conn)?], true)?;
    /// let mut vector = chunk.get_vector_at_mut::<BlobValue>(0)?;
    /// vector.set_size(3)?;
    /// {
    ///     let mut writer = vector.writer()?;
    ///     writer.set_parts(0, Some(&[b"header", b"\0\xff"]))?;
    ///     writer.set_parts(1, Some(&[]))?;
    ///     writer.set_parts(2, None)?;
    /// }
    /// assert_eq!(vector.reader()?.get(0)?, Some(b"header\0\xff".as_slice()));
    /// assert_eq!(vector.reader()?.get(1)?, Some(b"".as_slice()));
    /// assert_eq!(vector.reader()?.get(2)?, None);
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// Byte fragments cannot bypass BIT's encoding validation:
    ///
    /// ```compile_fail,E0599
    /// use duckdb_neo::{Result, types::BitValue, vector::Vector};
    ///
    /// fn write(vector: &mut Vector<'_, BitValue>) -> Result<()> {
    ///     vector.writer()?.set_parts(0, Some(&[b"\xff"]))
    /// }
    /// ```
    #[inline]
    pub fn set_parts(&mut self, index: usize, parts: Option<&[&[u8]]>) -> Result<()> {
        if index >= self.len() {
            return Err(out_of_bounds(index, self.len()));
        }
        if let Some(parts) = parts {
            // SAFETY: The row is in bounds, and arbitrary bytes are valid BLOB storage.
            unsafe { self.write_byte_parts(index, parts)? };
        }
        // SAFETY: Non-NULL rows are initialized; BLOB has no descendants.
        unsafe { self.inner.set_validity_unchecked(index, parts.is_some()) }
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
        if self.inner.vector.heap.is_none() {
            let handle = check_api_call!(ffi::duckdb_v2_vector_get_arena, self.inner.vector.handle, RET)?;
            self.inner.vector.heap = Some(Arena { handle });
        }
        Ok(self.inner.vector.heap.as_ref().unwrap())
    }

    pub(crate) unsafe fn write_bytes(&mut self, index: usize, value: Option<&[u8]>) -> Result<()> {
        if let Some(value) = value {
            // SAFETY: The caller guarantees the row's bounds and logical encoding.
            unsafe { self.write_byte_parts(index, &[value])? };
        }
        Ok(())
    }

    unsafe fn write_byte_parts<P: AsRef<[u8]> + ?Sized>(&mut self, index: usize, parts: &[&P]) -> Result<()> {
        let data = self.view().as_ptr().cast::<DuckDBBytes<'_>>().cast_mut();
        let encoded = DuckDBBytes::from_parts(parts, || self.arena())?;
        // SAFETY: The row is in bounds, and the bytes belong to this vector's arena.
        unsafe { data.add(index).write(encoded) };
        Ok(())
    }
}

#[cfg(test)]
mod tests;

use std::cell::Cell;

use crate::{
    Result,
    error::duckdb_failure_from_message,
    ffi::{
        DuckDBSuccess, duckdb_list_entry, duckdb_list_vector_get_child, duckdb_list_vector_get_size,
        duckdb_list_vector_reserve, duckdb_list_vector_set_size, duckdb_state, duckdb_validity_set_row_invalid,
        duckdb_vector, duckdb_vector_ensure_validity_writable, duckdb_vector_get_validity, duckdb_vector_size,
    },
};

use super::{ArrayVector, FlatVector, StructVector};

/// Mirrors DuckDB's `DConstants::MAX_VECTOR_SIZE`, the largest allocation a
/// single vector can hold (not the per-chunk row count reported by
/// `duckdb_vector_size()`). `ListVector::Reserve` throws a C++ exception for
/// larger requests, and `duckdb_list_vector_reserve` does not catch it, so the
/// exception would cross the non-unwinding C boundary and abort the process.
/// Rejecting such requests up front keeps them recoverable errors.
pub(super) const MAX_VECTOR_SIZE: u64 = 1 << 37;

fn list_vector_state_result(state: duckdb_state, action: &str, size: usize) -> Result<()> {
    if state == DuckDBSuccess {
        Ok(())
    } else {
        Err(duckdb_failure_from_message(format!(
            "failed to {action} {size} elements in DuckDB list vector"
        )))
    }
}

/// A list vector borrowed from a [`DataChunkHandle`][crate::core::DataChunkHandle].
///
/// Stores list entry offsets and lengths; elements live in a separate child vector.
///
/// Regression guard for #673 — a `ListVector` must not outlive its parent chunk:
///
/// ```compile_fail
/// use duckdb::core::{DataChunkHandle, LogicalTypeHandle, LogicalTypeId};
///
/// let vec;
/// {
///     let list_type = LogicalTypeHandle::list(&LogicalTypeId::Integer.into());
///     let chunk = DataChunkHandle::new(&[list_type]);
///     chunk.set_len(1);
///     let v = chunk.list_vector(0);
///     vec = v;
/// }
/// // chunk goes out of scope here; borrow checking rejects the outer use.
/// let _ = vec.get_entry(0);
/// ```
pub struct ListVector<'a> {
    /// ListVector does not own the vector pointer.
    entries: FlatVector<'a>,
    reserved_child_capacity: Cell<usize>,
}

impl<'a> ListVector<'a> {
    /// Wrap a raw `duckdb_vector` pointer.
    ///
    /// # Safety
    /// `ptr` must be a valid `duckdb_vector` that remains valid for all of `'a`.
    pub(crate) unsafe fn from_raw(ptr: duckdb_vector) -> Self {
        Self {
            entries: unsafe { FlatVector::from_raw(ptr) },
            reserved_child_capacity: Cell::new(0),
        }
    }

    /// Wrap a raw list vector pointer with a caller-supplied capacity.
    ///
    /// # Safety
    /// `ptr` must be a valid list `duckdb_vector` that remains valid for all of
    /// `'a`, and `capacity` must not exceed the backing allocation available for
    /// this vector's list entries. Any committed child size reported by DuckDB
    /// must also be covered by the child allocation, as it is when the size was
    /// committed through this wrapper or produced by DuckDB.
    pub(crate) unsafe fn from_raw_with_capacity(ptr: duckdb_vector, capacity: usize) -> Self {
        Self {
            entries: unsafe { FlatVector::with_capacity(ptr, capacity) },
            reserved_child_capacity: Cell::new(0),
        }
    }

    /// Returns the number of child elements stored by the list vector.
    ///
    /// This is the upper bound for the end of non-empty list entries, not the
    /// number of parent list rows.
    pub fn len(&self) -> usize {
        unsafe { duckdb_list_vector_get_size(self.entries.ptr) as usize }
    }

    /// Returns true if the list vector is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the child vector, reserving space for `capacity` child values.
    ///
    /// # Panics
    /// Panics if DuckDB rejects the reserve.
    // TODO: not ideal interface. Where should we keep capacity.
    pub fn child(&self, capacity: usize) -> FlatVector<'a> {
        let ptr = self
            .try_child_ptr_with_capacity(capacity)
            .unwrap_or_else(|err| panic!("{err}"));
        // SAFETY: DuckDB accepted the reserve for `capacity` child values.
        unsafe { FlatVector::with_capacity(ptr, capacity) }
    }

    /// Take the child as [StructVector], reserving space for `capacity` child
    /// values.
    ///
    /// # Panics
    /// Panics if DuckDB rejects the reserve.
    pub fn struct_child(&self, capacity: usize) -> StructVector<'a> {
        let ptr = self
            .try_child_ptr_with_capacity(capacity)
            .unwrap_or_else(|err| panic!("{err}"));
        // SAFETY: DuckDB accepted the reserve for `capacity` child values.
        unsafe { StructVector::from_raw_with_capacity(ptr, capacity) }
    }

    /// Capacity of the child vector's backing storage.
    ///
    /// DuckDB initializes list children with standard vector capacity. This
    /// wrapper tracks larger explicit reservations, and larger committed sizes
    /// must already have been reserved; the safe size setters and
    /// DuckDB-produced chunks preserve that invariant.
    fn child_capacity(&self) -> usize {
        self.len()
            .max(self.reserved_child_capacity.get())
            .max(unsafe { duckdb_vector_size() as usize })
    }

    /// Take the child as [ArrayVector].
    pub fn array_child(&self) -> ArrayVector<'a> {
        // SAFETY: `child_capacity` is covered by the child's backing storage.
        unsafe {
            ArrayVector::from_raw_with_capacity(duckdb_list_vector_get_child(self.entries.ptr), self.child_capacity())
        }
    }

    /// Take the child as [ListVector].
    pub fn list_child(&self) -> ListVector<'a> {
        // SAFETY: `child_capacity` is covered by the child's backing storage.
        unsafe {
            ListVector::from_raw_with_capacity(duckdb_list_vector_get_child(self.entries.ptr), self.child_capacity())
        }
    }

    pub(crate) fn try_child_ptr_with_capacity(&self, capacity: usize) -> Result<duckdb_vector> {
        self.try_reserve(capacity)?;
        Ok(unsafe { duckdb_list_vector_get_child(self.entries.ptr) })
    }

    #[cfg(feature = "vtab-arrow")]
    pub(crate) fn child_ptr(&self) -> duckdb_vector {
        unsafe { duckdb_list_vector_get_child(self.entries.ptr) }
    }

    /// Returns true if the row at the given index is null.
    ///
    /// # Panics
    /// Panics if `row` is outside the vector capacity.
    pub fn row_is_null(&self, row: u64) -> bool {
        self.entries.row_is_null(row)
    }

    /// Returns whether the row is null, or an error if it is out of range.
    pub fn try_row_is_null(&self, row: u64) -> Result<bool> {
        self.entries.try_row_is_null(row)
    }

    /// Set primitive data to the child node.
    ///
    /// Reserves child storage and commits the list length to `data.len()`.
    ///
    /// # Panics
    /// Panics if DuckDB rejects the reserve or size update.
    ///
    /// # Safety
    /// The caller must ensure `T` matches the child vector's physical storage
    /// and no other references to the same storage exist during the copy.
    pub unsafe fn set_child<T: Copy>(&self, data: &[T]) {
        unsafe { self.child(data.len()).copy(data) };
        self.set_len(data.len());
    }

    /// Set offset and length to the entry.
    ///
    /// # Panics
    /// Panics if `idx` is outside the vector capacity.
    pub fn set_entry(&mut self, idx: usize, offset: usize, length: usize) {
        assert!(
            idx < self.entries.capacity(),
            "list entry row {idx} exceeds vector capacity {}",
            self.entries.capacity()
        );
        // SAFETY: the bounds check above keeps the write inside the entry
        // allocation, and a single-slot write avoids materializing a slice
        // over neighboring entries DuckDB may have left uninitialized.
        unsafe {
            self.entries
                .as_mut_ptr::<duckdb_list_entry>()
                .add(idx)
                .write(duckdb_list_entry {
                    offset: offset as u64,
                    length: length as u64,
                });
        }
    }

    /// Get offset and length for the entry at index.
    ///
    /// # Panics
    /// Panics if the entry is out of range or its offset or length cannot fit
    /// in `usize` on the current target.
    pub fn get_entry(&self, idx: usize) -> (usize, usize) {
        self.try_get_entry(idx).unwrap_or_else(|err| panic!("{err}"))
    }

    /// Returns the entry offset and length, or an error if the entry is out of
    /// range or cannot be represented on the current target.
    pub fn try_get_entry(&self, idx: usize) -> Result<(usize, usize)> {
        if idx >= self.entries.capacity() {
            return Err(duckdb_failure_from_message(format!(
                "list entry row {idx} exceeds vector capacity {}",
                self.entries.capacity()
            )));
        }
        // SAFETY: the bounds check above keeps the read inside the entry
        // allocation, and a single-slot read avoids materializing a slice
        // over neighboring entries DuckDB may have left uninitialized.
        let entry = unsafe { self.entries.as_mut_ptr::<duckdb_list_entry>().add(idx).read() };
        let offset = usize::try_from(entry.offset).map_err(|_| {
            duckdb_failure_from_message(format!(
                "DuckDB list entry offset {} at row {idx} exceeds usize range",
                entry.offset
            ))
        })?;
        let length = usize::try_from(entry.length).map_err(|_| {
            duckdb_failure_from_message(format!(
                "DuckDB list entry length {} at row {idx} exceeds usize range",
                entry.length
            ))
        })?;
        Ok((offset, length))
    }

    /// Set row as null
    pub fn set_null(&mut self, row: usize) {
        unsafe {
            duckdb_vector_ensure_validity_writable(self.entries.ptr);
            let idx = duckdb_vector_get_validity(self.entries.ptr);
            duckdb_validity_set_row_invalid(idx, row as u64);
        }
    }

    /// Reserve space for `capacity` child values.
    ///
    /// This is public so callers can handle reservation errors without the
    /// panic-based convenience methods. Requests beyond DuckDB's maximum
    /// vector size (`2^37` elements) are rejected here as `Err`, because
    /// DuckDB reports them with a C++ exception the C API does not catch.
    /// Allocation failure inside DuckDB escapes the same way and aborts the
    /// process; it cannot be returned as an error through the current C API.
    pub fn try_reserve(&self, capacity: usize) -> Result<()> {
        if capacity as u64 > MAX_VECTOR_SIZE {
            return Err(duckdb_failure_from_message(format!(
                "cannot reserve {capacity} elements in DuckDB list vector: exceeds maximum vector size {MAX_VECTOR_SIZE}"
            )));
        }
        let state = unsafe { duckdb_list_vector_reserve(self.entries.ptr, capacity as u64) };
        list_vector_state_result(state, "reserve child storage for", capacity)?;
        self.reserved_child_capacity
            .set(self.reserved_child_capacity.get().max(capacity));
        Ok(())
    }

    /// Reserve child storage and set the length of the list vector.
    ///
    /// Panics if DuckDB rejects the reserve or size update. Use
    /// [`Self::try_set_len`] to handle that error explicitly.
    pub fn set_len(&self, new_len: usize) {
        self.try_set_len(new_len).unwrap_or_else(|err| panic!("{err}"));
    }

    /// Reserve child storage and set the length of the list vector.
    pub fn try_set_len(&self, new_len: usize) -> Result<()> {
        self.try_reserve(new_len)?;
        let state = unsafe { duckdb_list_vector_set_size(self.entries.ptr, new_len as u64) };
        list_vector_state_result(state, "set size to", new_len)
    }
}

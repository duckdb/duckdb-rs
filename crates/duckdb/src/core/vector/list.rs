use super::{
    FlatVector, ResultExt, VectorAccess, VectorRef, WritableVectorRef,
    array::ArrayVector,
    resize::{MAX_VECTOR_SIZE, max_resize_data_width},
    state::ReadableSpan,
    r#struct::StructVector,
};
use std::{fmt::Display, ops::Range};

use crate::{
    Result,
    core::{LogicalTypeHandle, LogicalTypeId},
    error::duckdb_failure_from_message,
    ffi::{
        DuckDBSuccess, duckdb_list_entry, duckdb_list_vector_get_child, duckdb_list_vector_get_size,
        duckdb_list_vector_reserve, duckdb_list_vector_set_size, duckdb_state, duckdb_vector_get_column_type,
    },
};

pub(super) fn list_vector_state_result(state: duckdb_state, action: &str, size: usize) -> Result<()> {
    if state == DuckDBSuccess {
        Ok(())
    } else {
        Err(duckdb_failure_from_message(format!(
            "failed to {action} {size} elements in DuckDB list vector"
        )))
    }
}

impl VectorRef<'_> {
    pub(super) fn list_len(&self) -> Result<usize> {
        self.expect_list()?;
        self.raw_list_len()
    }

    fn raw_list_len(&self) -> Result<usize> {
        let len = unsafe { duckdb_list_vector_get_size(self.ptr) };
        checked_usize(len, format_args!("DuckDB list child size {len}"))
    }

    fn read_list_entry(&self, row: usize) -> Result<Option<(usize, usize)>> {
        self.ensure_initialized()?;
        self.expect_list()?;
        self.check_read_index(row, "list entry row")?;
        if self.try_row_is_null_in_capacity(row)? {
            // DuckDB does not promise initialized entry payload beneath a null
            // parent, so do not inspect offset/length for this row.
            return Ok(None);
        }
        let ptr = self.data_ptr::<duckdb_list_entry>()?;
        // SAFETY: the row was checked against the explicit entry capacity and
        // initialized views guarantee non-null entry payloads are readable.
        let entry = unsafe { ptr.add(row).read() };
        let offset = checked_usize(
            entry.offset,
            format_args!("DuckDB list entry offset {} at row {row}", entry.offset),
        )?;
        let length = checked_usize(
            entry.length,
            format_args!("DuckDB list entry length {} at row {row}", entry.length),
        )?;
        Ok(Some((offset, length)))
    }

    pub(super) fn try_list_entry(&self, row: usize) -> Result<(usize, usize)> {
        self.read_list_entry(row)?
            .ok_or_else(|| duckdb_failure_from_message(format!("DuckDB list entry at row {row} is null")))
    }

    pub(super) fn set_list_entry(&mut self, row: usize, offset: usize, length: usize) -> Result<()> {
        self.expect_list()?;
        self.check_index(row, "list entry row")?;
        self.ensure_writable()?;
        if length != 0 && !self.try_row_is_null_in_capacity(row)? {
            let end = list_entry_end(offset, length, row)?;
            let child_len = self.raw_list_len()?;
            if end > child_len {
                return Err(duckdb_failure_from_message(format!(
                    "list entry at row {row} ends at {end}, beyond committed child size {child_len}"
                )));
            }
        }
        let offset = u64::try_from(offset)
            .map_err(|_| duckdb_failure_from_message(format!("list entry offset {offset} exceeds u64 range")))?;
        let length = u64::try_from(length)
            .map_err(|_| duckdb_failure_from_message(format!("list entry length {length} exceeds u64 range")))?;
        let ptr = self.data_mut_ptr::<duckdb_list_entry>()?;
        // SAFETY: the row was checked and the mutable view uniquely owns this
        // slot for the duration of the write.
        unsafe {
            ptr.add(row).write(duckdb_list_entry { offset, length });
        }
        Ok(())
    }

    pub(super) fn try_reserve_list_child(&mut self, capacity: usize) -> Result<()> {
        self.expect_list()?;
        self.ensure_writable()?;
        if capacity as u64 > MAX_VECTOR_SIZE {
            return Err(duckdb_failure_from_message(format!(
                "cannot reserve {capacity} elements in DuckDB list vector: exceeds maximum vector size {MAX_VECTOR_SIZE}"
            )));
        }
        self.check_list_child_resize_bytes(capacity)?;
        let state = unsafe { duckdb_list_vector_reserve(self.ptr, capacity as u64) };
        list_vector_state_result(state, "reserve child storage for", capacity)?;
        self.reserved_child_capacity = self.reserved_child_capacity.max(capacity);
        Ok(())
    }

    fn check_list_child_resize_bytes(&self, capacity: usize) -> Result<()> {
        if capacity == 0 {
            return Ok(());
        }

        let child_ptr = unsafe { duckdb_list_vector_get_child(self.ptr) };
        if child_ptr.is_null() {
            return Err(duckdb_failure_from_message("DuckDB returned a null list child vector"));
        }
        let child_type_ptr = unsafe { duckdb_vector_get_column_type(child_ptr) };
        if child_type_ptr.is_null() {
            return Err(duckdb_failure_from_message(
                "DuckDB returned a null logical type for a list child vector",
            ));
        }
        let child_type = unsafe { LogicalTypeHandle::new(child_type_ptr) };
        let bytes_per_row = max_resize_data_width(&child_type)?;
        let resized_capacity = capacity.checked_next_power_of_two().ok_or_else(|| {
            duckdb_failure_from_message(format!(
                "cannot reserve {capacity} elements in DuckDB list vector: rounded capacity overflows usize"
            ))
        })?;
        let target_bytes = (resized_capacity as u128)
            .checked_mul(bytes_per_row as u128)
            .ok_or_else(|| {
                duckdb_failure_from_message(format!(
                    "cannot reserve {capacity} elements in DuckDB list vector: physical buffer size overflows u128"
                ))
            })?;
        if target_bytes > MAX_VECTOR_SIZE as u128 {
            return Err(duckdb_failure_from_message(format!(
                "cannot reserve {capacity} elements in DuckDB list vector: rounded capacity {resized_capacity} requires a {target_bytes}-byte physical buffer, exceeding maximum vector size {MAX_VECTOR_SIZE}"
            )));
        }
        Ok(())
    }

    pub(super) fn try_set_list_len(&mut self, len: usize) -> Result<()> {
        self.try_set_list_len_with_reset(len, |vector, rows| {
            let mut child = vector.current_list_child_mut()?;
            child.clear_nested_list_entries(rows)
        })
    }

    fn try_set_list_len_with_reset(
        &mut self,
        len: usize,
        reset_new_rows: impl FnOnce(&mut Self, Range<usize>) -> Result<()>,
    ) -> Result<()> {
        let old_len = self.list_len()?;
        self.try_reserve_list_child(len)?;
        if len != old_len {
            // Every step below mutates the logical vector or compensating
            // entries. Revoke initialized reads before the first mutation so
            // any later FFI or nested-reset error leaves a conservative state.
            self.mark_under_construction();
        }
        if len < old_len {
            // `set_list_entry` only bounds an entry when it is written, so
            // shrinking would strand entries already committed against the
            // longer child. DuckDB walks a non-null entry's range without
            // consulting the committed child size, so a stale range is an
            // out-of-bounds read rather than a recoverable error. Resetting
            // every entry to the empty list keeps the shrink sound without
            // inspecting entry payloads that may never have been written.
            let capacity = self.capacity;
            self.clear_list_entries(0..capacity)?;
        }
        let state = unsafe { duckdb_list_vector_set_size(self.ptr, len as u64) };
        list_vector_state_result(state, "set size to", len)?;
        if len > old_len {
            // Committing further child rows re-exposes entries that an earlier
            // shrink left behind, so a grow can strand a range just as a shrink
            // can. Only the newly committed rows are reset; rows below
            // `old_len` keep the entries their owner already wrote.
            reset_new_rows(self, old_len..len)?;
        }
        Ok(())
    }

    /// Resets every list entry reachable at `rows`.
    ///
    /// This walks DuckDB's *physical* layout rather than the logical child
    /// list: a `MAP` reports its key type as logical child 0 while its physical
    /// child is a `STRUCT`, and a list nested under a struct field is reachable
    /// without either vector being list-typed one level down.
    fn clear_nested_list_entries(&mut self, rows: Range<usize>) -> Result<()> {
        if rows.start >= rows.end {
            return Ok(());
        }
        match self.logical_type.id() {
            // Zeroing these entries makes the rows empty lists, so nothing
            // beneath them stays reachable and the walk stops here.
            LogicalTypeId::List | LogicalTypeId::Map => self.clear_list_entries(rows),
            // Struct-physical children share their parent's row indexing.
            LogicalTypeId::Struct | LogicalTypeId::Union | LogicalTypeId::Variant => {
                let count = self.struct_child_count()?;
                let capacity = rows.end.min(self.capacity);
                for index in 0..count {
                    let mut child = self.struct_child_mut(index, capacity)?;
                    child.clear_nested_list_entries(rows.clone())?;
                }
                Ok(())
            }
            // A fixed-size array stores `array_size` child rows per parent row.
            LogicalTypeId::Array => {
                let width = self.array_size()?;
                let scale = |row: usize| {
                    row.checked_mul(width)
                        .ok_or_else(|| duckdb_failure_from_message("array child row span overflows usize"))
                };
                let scaled = scale(rows.start)?..scale(rows.end)?;
                let mut child = self.array_child_mut(scaled.end)?;
                child.clear_nested_list_entries(scaled)
            }
            _ => Ok(()),
        }
    }

    fn clear_list_entries(&mut self, rows: Range<usize>) -> Result<()> {
        let end = rows.end.min(self.capacity);
        if rows.start >= end {
            return Ok(());
        }
        let ptr = self.data_mut_ptr::<duckdb_list_entry>()?;
        // SAFETY: `end` is clamped to the vector's effective capacity, which
        // bounds every row `set_list_entry` accepts, and the mutable view
        // uniquely owns that span. An all-zero entry is the empty list, which
        // DuckDB never dereferences.
        unsafe { ptr.add(rows.start).write_bytes(0, end - rows.start) };
        Ok(())
    }

    pub(super) fn reserved_list_child_mut(&mut self, capacity: usize) -> Result<VectorRef<'_>> {
        let readable_len = self.list_len()?.min(capacity);
        self.try_reserve_list_child(capacity)?;
        // The view spans everything DuckDB has backed, not just the requested
        // span. A narrower view would still reach vector-wide operations such
        // as `try_set_list_len`, whose compensating entry reset can only cover
        // the rows the view admits.
        let capacity = self.raw_list_len()?.max(self.reserved_child_capacity).max(capacity);
        let ptr = unsafe { duckdb_list_vector_get_child(self.ptr) };
        // SAFETY: the reservation covers `capacity`; the mutable parent borrow
        // uniquely reaches the child. Only the previously committed prefix is
        // readable until the owner re-establishes initialization.
        unsafe {
            Self::new(
                ptr,
                capacity,
                self.state.reborrow(),
                ReadableSpan::Fixed(readable_len),
                self.access,
            )
        }
    }

    pub(super) fn current_list_child_mut(&mut self) -> Result<VectorRef<'_>> {
        self.ensure_writable()?;
        let readable_len = self.list_len()?;
        let capacity = readable_len.max(self.reserved_child_capacity);
        let ptr = unsafe { duckdb_list_vector_get_child(self.ptr) };
        // SAFETY: committed and explicitly reserved spans are both backed by
        // DuckDB storage. No standard-vector-size floor is assumed.
        unsafe {
            Self::new(
                ptr,
                capacity,
                self.state.reborrow(),
                ReadableSpan::Fixed(readable_len),
                self.access,
            )
        }
    }
}

#[cfg_attr(not(feature = "vtab-arrow"), allow(dead_code))]
impl VectorRef<'_> {
    pub(super) fn try_list_range(&self, row: usize) -> Result<Option<Range<usize>>> {
        let Some((offset, length)) = self.read_list_entry(row)? else {
            return Ok(None);
        };
        if length == 0 {
            // Empty lists do not traverse child storage. DuckDB may leave a
            // stale offset in an otherwise valid empty entry, so normalize it
            // instead of treating it as a child access.
            return Ok(Some(0..0));
        }
        let end = list_entry_end(offset, length, row)?;
        let child_len = self.raw_list_len()?;
        if end > child_len {
            return Err(duckdb_failure_from_message(format!(
                "DuckDB list entry at row {row} ends at {end}, beyond child length {child_len}"
            )));
        }
        Ok(Some(offset..end))
    }

    pub(super) fn list_child_read(&self) -> Result<VectorRef<'_>> {
        self.ensure_initialized()?;
        let capacity = self.list_len()?;
        let ptr = unsafe { duckdb_list_vector_get_child(self.ptr) };
        // SAFETY: the parent state dynamically revokes reads after growth, the
        // committed child size bounds this view, and the parent keeps it live.
        unsafe {
            Self::new(
                ptr,
                capacity,
                self.state.reborrow(),
                ReadableSpan::Fixed(capacity),
                VectorAccess::ReadOnly,
            )
        }
    }

    pub(super) fn committed_list_child_mut(&mut self, capacity: usize) -> Result<VectorRef<'_>> {
        self.try_set_list_len(capacity)?;
        let ptr = unsafe { duckdb_list_vector_get_child(self.ptr) };
        // SAFETY: setting the list size reserves `capacity`; the mutable parent
        // borrow uniquely reaches the child. Growth revoked initialized reads.
        unsafe {
            Self::new(
                ptr,
                capacity,
                self.state.reborrow(),
                ReadableSpan::Fixed(capacity),
                self.access,
            )
        }
    }
}

fn checked_usize(value: u64, context: impl Display) -> Result<usize> {
    usize::try_from(value).map_err(|_| duckdb_failure_from_message(format!("{context} exceeds usize range")))
}

fn list_entry_end(offset: usize, length: usize, row: usize) -> Result<usize> {
    offset
        .checked_add(length)
        .ok_or_else(|| duckdb_failure_from_message(format!("DuckDB list entry at row {row} overflows usize")))
}

/// A list or map vector borrowed from a data chunk.
///
/// Writable child access requires a mutable borrow of the parent. The returned
/// child remains borrow-scoped so safe code cannot hold overlapping mutable
/// sibling views.
///
/// Commit child storage with [`Self::set_child`] or [`Self::set_len`] before
/// writing entries. Calling [`Self::set_entry`] for an uncommitted child range
/// panics, and shrinking the committed child size resets every parent entry to
/// the empty list. Code written against earlier releases may need to move its
/// entry writes after the child-size commit. Child views now keep their parent
/// borrowed until they are dropped, so reusing the parent in the same scope may
/// require an explicit `drop(child)` or a nested scope.
pub struct ListVector<'a> {
    pub(super) vector: VectorRef<'a>,
}

impl<'a> ListVector<'a> {
    pub(crate) fn from_vector(vector: VectorRef<'a>) -> Result<Self> {
        vector.expect_list()?;
        Ok(Self { vector })
    }

    /// Returns the committed list-child size.
    pub fn len(&self) -> usize {
        self.vector.list_len().or_panic()
    }

    /// Returns true when the committed child size is zero.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Reserves at least `capacity` child rows and returns a flat child view.
    ///
    /// The returned view spans all storage already committed or reserved by
    /// this live list view, so its capacity may exceed `capacity`. The argument
    /// is a minimum reservation request, not a logical write limit; callers
    /// that need a smaller logical prefix must enforce that limit themselves.
    pub fn child(&mut self, capacity: usize) -> FlatVector<'_> {
        FlatVector::from_vector(self.vector.reserved_list_child_mut(capacity).or_panic()).or_panic()
    }

    /// Reserves at least `capacity` child rows and returns a struct child view.
    ///
    /// The returned view spans all storage already committed or reserved by
    /// this live list view, so its capacity may exceed `capacity`. The argument
    /// is a minimum reservation request, not a logical write limit.
    pub fn struct_child(&mut self, capacity: usize) -> StructVector<'_> {
        let vector = self.vector.reserved_list_child_mut(capacity).or_panic();
        StructVector::from_vector(vector).or_panic()
    }

    /// Returns the current fixed-size-array child span.
    ///
    /// A fresh list has a zero-length child span. Reserve or commit the list
    /// child before using a non-empty nested array child.
    ///
    /// # Panics
    ///
    /// Panics if the physical child is not a fixed-size array.
    pub fn array_child(&mut self) -> ArrayVector<'_> {
        let vector = self.vector.current_list_child_mut().or_panic();
        ArrayVector::from_vector(vector).or_panic()
    }

    /// Returns the current nested-list child span.
    ///
    /// A fresh list has a zero-length child span. Reserve or commit the parent
    /// list child before using a non-empty nested list child.
    ///
    /// # Panics
    ///
    /// Panics if the physical child is not a list or map.
    pub fn list_child(&mut self) -> ListVector<'_> {
        let vector = self.vector.current_list_child_mut().or_panic();
        ListVector::from_vector(vector).or_panic()
    }

    /// Returns true if the parent row is null.
    pub fn row_is_null(&self, row: u64) -> bool {
        self.vector.row_is_null(row)
    }

    /// Returns whether the parent row is null, or an out-of-range error.
    pub fn try_row_is_null(&self, row: u64) -> Result<bool> {
        self.vector.try_row_is_null(row)
    }

    /// Copies primitive child data and commits its child size.
    ///
    /// # Safety
    /// `T` must match the child vector's physical storage.
    ///
    /// # Panics
    ///
    /// Panics if the child is not flat storage or reservation or commit fails.
    pub unsafe fn set_child<T: Copy>(&mut self, data: &[T]) {
        {
            let mut child = self.child(data.len());
            unsafe { child.copy(data) };
        }
        self.set_len(data.len());
    }

    /// Writes one checked list entry.
    ///
    /// # Panics
    ///
    /// Panics if the row is outside the parent capacity, the range overflows,
    /// or a non-null entry exceeds committed child storage.
    pub fn set_entry(&mut self, row: usize, offset: usize, length: usize) {
        self.try_set_entry(row, offset, length).or_panic();
    }

    /// Tries to write one checked list entry.
    pub fn try_set_entry(&mut self, row: usize, offset: usize, length: usize) -> Result<()> {
        self.vector.set_list_entry(row, offset, length)
    }

    /// Returns one list entry.
    ///
    /// # Panics
    ///
    /// Panics if `row` is out of range or null, or while the payload remains
    /// under construction. Raw writable adapters do not become readable in
    /// place; finish them and read through an initialized owner.
    pub fn get_entry(&self, row: usize) -> (usize, usize) {
        self.try_get_entry(row).or_panic()
    }

    /// Returns one checked list entry.
    pub fn try_get_entry(&self, row: usize) -> Result<(usize, usize)> {
        self.vector.try_list_entry(row)
    }

    /// Marks one parent row null.
    ///
    /// # Panics
    ///
    /// Panics if `row` exceeds the parent capacity.
    pub fn set_null(&mut self, row: usize) {
        self.vector.set_null(row);
    }

    /// Reserves list-child storage for this view.
    ///
    /// `capacity` is a minimum backing-storage request, not a write limit, and
    /// reservation does not commit child rows. Call [`Self::set_len`] before
    /// writing entries that refer to the reserved rows.
    ///
    /// Tracking that exposes reserved but uncommitted rows through nested child
    /// views is view-local. After dropping and reacquiring the list vector,
    /// reserve again before accessing those rows through [`Self::array_child`]
    /// or [`Self::list_child`].
    pub fn try_reserve(&mut self, capacity: usize) -> Result<()> {
        self.vector.try_reserve_list_child(capacity)
    }

    /// Reserves and commits the list-child size.
    ///
    /// Shrinking below the committed child size resets every entry to the
    /// empty list, because a stale entry range would otherwise point past the
    /// child storage DuckDB is willing to read. Write the entries you need
    /// after committing the child size, not before. Growing preserves this
    /// vector's existing entries but resets list and map entries nested within
    /// newly committed child rows.
    ///
    /// # Panics
    ///
    /// Panics if the size cannot be reserved.
    pub fn set_len(&mut self, len: usize) {
        self.try_set_len(len).or_panic();
    }

    /// Reserves and commits the list-child size.
    ///
    /// Shrinking below the committed child size resets every entry to the
    /// empty list. Growing resets list and map entries nested within newly
    /// committed child rows. See [`Self::set_len`].
    pub fn try_set_len(&mut self, len: usize) -> Result<()> {
        self.vector.try_set_list_len(len)
    }
}

#[cfg_attr(not(feature = "vtab-arrow"), allow(dead_code))]
impl ListVector<'_> {
    pub(crate) fn try_get_range(&self, row: usize) -> Result<Option<Range<usize>>> {
        self.vector.try_list_range(row)
    }

    pub(crate) fn read_child(&self) -> Result<VectorRef<'_>> {
        self.vector.list_child_read()
    }

    pub(crate) fn writable_child(&mut self, capacity: usize) -> Result<WritableVectorRef<'_>> {
        Ok(WritableVectorRef::from_vector(
            self.vector.committed_list_child_mut(capacity)?,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::DataChunkHandle;

    #[test]
    fn failed_growth_reset_revokes_initialized_reads() {
        let child_type = LogicalTypeHandle::list(&LogicalTypeId::Integer.into());
        let list_type = LogicalTypeHandle::list(&child_type);
        let mut chunk = DataChunkHandle::new(&[list_type]);
        {
            let mut list = chunk.list_vector(0);
            list.set_entry(0, 0, 0);
        }
        chunk.set_len(1);
        unsafe { chunk.assume_initialized() };

        let mut list = chunk.list_vector(0);
        let error = list
            .vector
            .try_set_list_len_with_reset(1, |_, _| {
                Err(duckdb_failure_from_message("injected nested-entry reset failure"))
            })
            .unwrap_err();
        assert_eq!(error.to_string(), "injected nested-entry reset failure");
        let error = list
            .vector
            .list_child_read()
            .err()
            .expect("failed resize must revoke initialized reads");
        assert_eq!(error.to_string(), super::super::state::UNDER_CONSTRUCTION_MESSAGE);
    }
}

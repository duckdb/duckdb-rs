//! Borrowed DuckDB vector views.
//!
//! [`VectorRef`] is the Arrow-independent native read seam. It owns raw
//! pointer, logical family, effective capacity, readable-span, validity, and
//! child-traversal invariants. The family-specific public wrappers remain as
//! temporary write adapters until the stacked write-seam branch replaces them.

use std::{marker::PhantomData, ops::Range};

use libduckdb_sys::{
    duckdb_array_type_array_size, duckdb_array_vector_get_child, duckdb_list_entry, duckdb_list_vector_get_child,
    duckdb_list_vector_get_size, duckdb_struct_type_child_count, duckdb_struct_vector_get_child,
    duckdb_validity_row_is_valid,
};

use super::{LogicalTypeHandle, LogicalTypeId};
use crate::{
    Result,
    error::duckdb_failure_from_message,
    ffi::{duckdb_vector, duckdb_vector_get_column_type, duckdb_vector_get_data, duckdb_vector_get_validity},
};

mod array;
mod flat;
mod list;
mod state;
mod r#struct;

use state::{ReadableSpan, StateRef};
pub(in crate::core) use state::{VectorState, VectorStateCell};

pub use array::ArrayVector;
pub use flat::{FlatVector, Inserter};
pub use list::ListVector;
pub use r#struct::StructVector;

/// The single borrowed native vector representation.
///
/// This read-only shape deliberately contains no Arrow types. Callers receive
/// checked family traversal and initialized extents without reproducing the
/// corresponding FFI or layout rules.
pub(crate) struct VectorRef<'a> {
    ptr: duckdb_vector,
    capacity: usize,
    logical_type: LogicalTypeHandle,
    state: StateRef<'a>,
    readable_span: ReadableSpan,
    _owner: PhantomData<&'a ()>,
}

impl<'a> VectorRef<'a> {
    /// Wrap a vector whose initialization state is owned by a data chunk.
    ///
    /// # Safety
    ///
    /// `ptr` must remain live for `'a`, `capacity` must not exceed its
    /// backing allocation, and `state` must describe the owning data chunk.
    pub(in crate::core) unsafe fn initialized_from_chunk(
        ptr: duckdb_vector,
        capacity: usize,
        state: &'a VectorStateCell,
    ) -> Result<Self> {
        unsafe {
            Self::new(
                ptr,
                capacity,
                StateRef::Borrowed(state),
                ReadableSpan::Chunk { multiplier: 1 },
            )
        }
    }

    unsafe fn from_state<'b>(
        ptr: duckdb_vector,
        capacity: usize,
        state: StateRef<'b>,
        readable_span: ReadableSpan,
    ) -> Result<VectorRef<'b>> {
        unsafe { Self::new(ptr, capacity, state, readable_span) }
    }

    unsafe fn new<'b>(
        ptr: duckdb_vector,
        capacity: usize,
        state: StateRef<'b>,
        readable_span: ReadableSpan,
    ) -> Result<VectorRef<'b>> {
        if ptr.is_null() {
            return Err(duckdb_failure_from_message("DuckDB returned a null vector pointer"));
        }
        let logical_type = unsafe { duckdb_vector_get_column_type(ptr) };
        if logical_type.is_null() {
            return Err(duckdb_failure_from_message(
                "DuckDB returned a null logical type for a vector",
            ));
        }

        Ok(VectorRef {
            ptr,
            capacity,
            logical_type: unsafe { LogicalTypeHandle::new(logical_type) },
            state,
            readable_span,
            _owner: PhantomData,
        })
    }

    pub(crate) fn capacity(&self) -> usize {
        self.capacity
    }

    pub(crate) fn logical_type(&self) -> &LogicalTypeHandle {
        &self.logical_type
    }

    pub(crate) fn readable_len(&self) -> Result<usize> {
        let chunk_len = self.state.readable_len_or_err()?;
        let len = match self.readable_span {
            ReadableSpan::Chunk { multiplier } => chunk_len
                .checked_mul(multiplier)
                .ok_or_else(|| duckdb_failure_from_message("initialized vector span overflows usize"))?,
            ReadableSpan::Fixed(len) => len,
        };
        Ok(len.min(self.capacity))
    }

    pub(crate) fn try_row_is_null(&self, row: u64) -> Result<bool> {
        let row = usize::try_from(row)
            .map_err(|_| duckdb_failure_from_message(format!("row index {row} exceeds usize range")))?;
        self.check_index(row, "row index")?;

        let valid = unsafe {
            let validity = duckdb_vector_get_validity(self.ptr);
            if validity.is_null() {
                return Ok(false);
            }
            duckdb_validity_row_is_valid(validity, row as u64)
        };
        Ok(!valid)
    }

    /// Reads one initialized non-null physical value.
    ///
    /// # Safety
    ///
    /// `T` must match DuckDB's physical storage and `row` must be non-null.
    pub(crate) unsafe fn read<T: Copy>(&self, row: usize) -> Result<T> {
        self.check_read_index(row, "row")?;
        let ptr = unsafe { duckdb_vector_get_data(self.ptr).cast::<T>() };
        if ptr.is_null() {
            return Err(duckdb_failure_from_message(
                "DuckDB returned a null vector data pointer",
            ));
        }
        Ok(unsafe { ptr.add(row).read() })
    }

    pub(crate) fn list_range(&self, row: usize) -> Result<Option<Range<usize>>> {
        self.expect_list()?;
        self.check_read_index(row, "list entry row")?;
        if self.try_row_is_null(row as u64)? {
            return Ok(None);
        }

        let entry = unsafe { self.read::<duckdb_list_entry>(row) }?;
        let length = usize::try_from(entry.length).map_err(|_| {
            duckdb_failure_from_message(format!(
                "DuckDB list entry length {} at row {row} exceeds usize range",
                entry.length
            ))
        })?;
        if length == 0 {
            return Ok(Some(0..0));
        }
        let offset = usize::try_from(entry.offset).map_err(|_| {
            duckdb_failure_from_message(format!(
                "DuckDB list entry offset {} at row {row} exceeds usize range",
                entry.offset
            ))
        })?;
        let end = offset
            .checked_add(length)
            .ok_or_else(|| duckdb_failure_from_message(format!("DuckDB list entry at row {row} overflows usize")))?;
        let child_len = self.list_len()?;
        if end > child_len {
            return Err(duckdb_failure_from_message(format!(
                "DuckDB list entry at row {row} ends at {end}, beyond child length {child_len}"
            )));
        }
        Ok(Some(offset..end))
    }

    pub(crate) fn list_child(&self) -> Result<VectorRef<'_>> {
        self.ensure_initialized()?;
        let len = self.list_len()?;
        let ptr = unsafe { duckdb_list_vector_get_child(self.ptr) };
        unsafe { Self::from_state(ptr, len, self.state.reborrow(), ReadableSpan::Fixed(len)) }
    }

    pub(crate) fn array_size(&self) -> Result<usize> {
        self.expect_array()?;
        let size = unsafe { duckdb_array_type_array_size(self.logical_type.ptr) };
        let size =
            usize::try_from(size).map_err(|_| duckdb_failure_from_message("DuckDB array size exceeds usize range"))?;
        if size == 0 {
            Err(duckdb_failure_from_message(
                "DuckDB array size must be greater than zero",
            ))
        } else {
            Ok(size)
        }
    }

    pub(crate) fn array_child(&self) -> Result<VectorRef<'_>> {
        self.ensure_initialized()?;
        let width = self.array_size()?;
        let capacity = self
            .capacity
            .checked_mul(width)
            .ok_or_else(|| duckdb_failure_from_message("array child capacity overflows usize"))?;
        let ptr = unsafe { duckdb_array_vector_get_child(self.ptr) };
        unsafe { Self::from_state(ptr, capacity, self.state.reborrow(), self.readable_span.scaled(width)?) }
    }

    pub(crate) fn struct_child_count(&self) -> Result<usize> {
        self.expect_struct()?;
        let count = unsafe { duckdb_struct_type_child_count(self.logical_type.ptr) };
        usize::try_from(count).map_err(|_| duckdb_failure_from_message("DuckDB struct child count exceeds usize range"))
    }

    pub(crate) fn struct_child(&self, index: usize) -> Result<VectorRef<'_>> {
        self.ensure_initialized()?;
        let count = self.struct_child_count()?;
        if index >= count {
            return Err(duckdb_failure_from_message(format!(
                "struct child index {index} exceeds child count {count}"
            )));
        }
        let ptr = unsafe { duckdb_struct_vector_get_child(self.ptr, index as u64) };
        unsafe { Self::from_state(ptr, self.capacity, self.state.reborrow(), self.readable_span) }
    }

    fn list_len(&self) -> Result<usize> {
        self.expect_list()?;
        let len = unsafe { duckdb_list_vector_get_size(self.ptr) };
        usize::try_from(len)
            .map_err(|_| duckdb_failure_from_message(format!("DuckDB list child size {len} exceeds usize range")))
    }

    fn ensure_initialized(&self) -> Result<()> {
        self.state.readable_len_or_err().map(|_| ())
    }

    fn check_index(&self, index: usize, kind: &str) -> Result<()> {
        if index < self.capacity {
            Ok(())
        } else {
            Err(duckdb_failure_from_message(format!(
                "{kind} {index} exceeds vector capacity {}",
                self.capacity
            )))
        }
    }

    fn check_read_index(&self, index: usize, kind: &str) -> Result<()> {
        let readable_len = self.readable_len()?;
        if index < readable_len {
            Ok(())
        } else {
            Err(duckdb_failure_from_message(format!(
                "{kind} {index} exceeds initialized vector length {readable_len}"
            )))
        }
    }

    fn expect_type(&self, expected: &str, matches: impl FnOnce(LogicalTypeId) -> bool) -> Result<()> {
        let actual = self.logical_type.id();
        if matches(actual) {
            Ok(())
        } else {
            Err(duckdb_failure_from_message(format!(
                "expected {expected} vector, got {actual:?}"
            )))
        }
    }

    pub(super) fn expect_flat(&self) -> Result<()> {
        self.expect_type("scalar", |id| {
            !matches!(
                id,
                LogicalTypeId::List
                    | LogicalTypeId::Array
                    | LogicalTypeId::Struct
                    | LogicalTypeId::Map
                    | LogicalTypeId::Union
                    | LogicalTypeId::Variant
            )
        })
    }

    fn expect_list(&self) -> Result<()> {
        self.expect_type("list or map", |id| {
            matches!(id, LogicalTypeId::List | LogicalTypeId::Map)
        })
    }

    fn expect_array(&self) -> Result<()> {
        self.expect_type("array", |id| id == LogicalTypeId::Array)
    }

    fn expect_struct(&self) -> Result<()> {
        self.expect_type("struct", |id| {
            matches!(
                id,
                LogicalTypeId::Struct | LogicalTypeId::Union | LogicalTypeId::Variant
            )
        })
    }
}

// Temporary legacy validity helpers. The write-seam branch removes these once
// every family-specific compatibility wrapper delegates to VectorRef.
fn try_vector_row_is_null(ptr: duckdb_vector, row: u64, capacity: usize) -> Result<bool> {
    let row_index = usize::try_from(row)
        .map_err(|_| duckdb_failure_from_message(format!("row index {row} exceeds usize range")))?;
    if row_index >= capacity {
        return Err(duckdb_failure_from_message(format!(
            "row index {row} exceeds vector capacity {capacity}"
        )));
    }
    let valid = unsafe {
        let validity = duckdb_vector_get_validity(ptr);
        if validity.is_null() {
            return Ok(false);
        }
        duckdb_validity_row_is_valid(validity, row)
    };
    Ok(!valid)
}

fn vector_row_is_null(ptr: duckdb_vector, row: u64, capacity: usize) -> bool {
    try_vector_row_is_null(ptr, row, capacity).unwrap_or_else(|err| panic!("{err}"))
}

#[cfg(test)]
mod tests;

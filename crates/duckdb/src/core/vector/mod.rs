//! Borrowed DuckDB vector views.
//!
//! [`VectorRef`] is the Arrow-independent native seam. It owns all raw-pointer,
//! logical-type, effective-capacity, initialization, validity, and child
//! traversal invariants. The public vector-family types are compatibility
//! adapters over that representation.
use std::{marker::PhantomData, slice};

use libduckdb_sys::duckdb_validity_row_is_valid;

use super::{LogicalTypeHandle, LogicalTypeId};
use crate::{
    Result,
    error::duckdb_failure_from_message,
    ffi::{
        duckdb_validity_set_row_invalid, duckdb_vector, duckdb_vector_ensure_validity_writable,
        duckdb_vector_get_column_type, duckdb_vector_get_data, duckdb_vector_get_validity,
    },
};

mod array;
mod flat;
mod lease;
mod list;
mod resize;
mod result_ext;
mod state;
mod r#struct;
mod writable;

pub(in crate::core) use lease::BorrowGuard;
#[cfg(test)]
use resize::{MAX_VECTOR_SIZE, max_resize_data_width};
use state::{ReadableSpan, StateRef};
pub(super) use state::{VectorState, VectorStateCell};

pub use array::ArrayVector;
pub use flat::{FlatVector, Inserter};
pub use list::ListVector;
pub(crate) use result_ext::ResultExt;
pub use r#struct::StructVector;
pub use writable::{WritableVector, WritableVectorRef};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum VectorAccess {
    ReadOnly,
    Writable,
}

/// The single borrowed native vector representation.
pub(crate) struct VectorRef<'a> {
    ptr: duckdb_vector,
    capacity: usize,
    logical_type: LogicalTypeHandle,
    state: StateRef<'a>,
    readable_span: ReadableSpan,
    access: VectorAccess,
    reserved_child_capacity: usize,
    borrow_guard: Option<BorrowGuard>,
    _owner: PhantomData<&'a ()>,
}

impl<'a> VectorRef<'a> {
    /// Wrap a vector whose state is owned by a data chunk.
    ///
    /// # Safety
    ///
    /// `ptr` must remain live for `'a`, `capacity` must not exceed its backing
    /// allocation, and `state` must describe every vector in the owning chunk.
    unsafe fn from_chunk(
        ptr: duckdb_vector,
        capacity: usize,
        state: &'a VectorStateCell,
        access: VectorAccess,
        readable_span: ReadableSpan,
    ) -> Result<Self> {
        unsafe { Self::new(ptr, capacity, StateRef::Borrowed(state), readable_span, access) }
    }

    pub(super) unsafe fn compatibility(
        ptr: duckdb_vector,
        capacity: usize,
        state: &'a VectorStateCell,
    ) -> Result<Self> {
        let access = match state.get() {
            VectorState::CallbackInput { .. } => VectorAccess::ReadOnly,
            VectorState::Initialized { .. } | VectorState::UnderConstruction => VectorAccess::Writable,
        };
        unsafe { Self::from_chunk(ptr, capacity, state, access, ReadableSpan::Chunk { multiplier: 1 }) }
    }

    pub(super) unsafe fn initialized_from_chunk(
        ptr: duckdb_vector,
        capacity: usize,
        state: &'a VectorStateCell,
    ) -> Result<Self> {
        unsafe {
            Self::from_chunk(
                ptr,
                capacity,
                state,
                VectorAccess::ReadOnly,
                ReadableSpan::Chunk { multiplier: 1 },
            )
        }
    }

    pub(super) unsafe fn writable_from_chunk(
        ptr: duckdb_vector,
        capacity: usize,
        state: &'a VectorStateCell,
    ) -> Result<Self> {
        if matches!(state.get(), VectorState::CallbackInput { .. }) {
            return Err(duckdb_failure_from_message(
                "DuckDB callback input vectors are read-only",
            ));
        }
        unsafe { Self::from_chunk(ptr, capacity, state, VectorAccess::Writable, ReadableSpan::Fixed(0)) }
    }

    /// Wrap a raw vector under construction.
    ///
    /// # Safety
    ///
    /// `ptr` must remain live and uniquely writable for `'a`, and `capacity`
    /// must not exceed its backing allocation.
    pub(super) unsafe fn writable(ptr: duckdb_vector, capacity: usize) -> Result<Self> {
        unsafe {
            Self::new(
                ptr,
                capacity,
                StateRef::Owned(VectorStateCell::new(VectorState::UnderConstruction)),
                ReadableSpan::Fixed(0),
                VectorAccess::Writable,
            )
        }
    }

    unsafe fn new<'b>(
        ptr: duckdb_vector,
        capacity: usize,
        state: StateRef<'b>,
        readable_span: ReadableSpan,
        access: VectorAccess,
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
            access,
            reserved_child_capacity: 0,
            borrow_guard: None,
            _owner: PhantomData,
        })
    }

    pub(in crate::core) fn with_borrow_guard(mut self, guard: BorrowGuard) -> Self {
        self.borrow_guard = Some(guard);
        self
    }

    pub(crate) fn capacity(&self) -> usize {
        self.capacity
    }

    #[cfg(test)]
    pub(super) fn is_initialized(&self) -> bool {
        self.state.shared().is_initialized()
    }

    pub(crate) fn logical_type(&self) -> &LogicalTypeHandle {
        &self.logical_type
    }

    pub(crate) fn logical_type_owned(&self) -> LogicalTypeHandle {
        let logical_type = unsafe { duckdb_vector_get_column_type(self.ptr) };
        assert!(
            !logical_type.is_null(),
            "DuckDB returned a null logical type for a vector"
        );
        // SAFETY: `self.ptr` is live for this borrow and DuckDB returns a newly
        // owned logical-type handle.
        unsafe { LogicalTypeHandle::new(logical_type) }
    }

    pub(super) fn ptr(&self) -> duckdb_vector {
        self.ptr
    }

    pub(super) fn reborrow(&mut self) -> Result<VectorRef<'_>> {
        // SAFETY: the mutable borrow keeps the original view inaccessible for
        // the returned view's lifetime and preserves its capacity/state.
        let mut vector = unsafe {
            Self::new(
                self.ptr,
                self.capacity,
                self.state.reborrow(),
                self.readable_span,
                self.access,
            )
        }?;
        vector.reserved_child_capacity = self.reserved_child_capacity;
        Ok(vector)
    }

    fn ensure_initialized(&self) -> Result<()> {
        self.state.readable_len_or_err().map(|_| ())
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

    fn mark_under_construction(&self) {
        debug_assert!(
            !matches!(self.state.shared().get(), VectorState::CallbackInput { .. }),
            "writable vector cannot share callback input state"
        );
        self.state.shared().set(VectorState::UnderConstruction);
    }

    pub(super) fn ensure_writable(&self) -> Result<()> {
        if self.access == VectorAccess::Writable {
            Ok(())
        } else {
            Err(duckdb_failure_from_message(
                "DuckDB callback input vectors are read-only",
            ))
        }
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

    pub(crate) fn try_row_is_null(&self, row: u64) -> Result<bool> {
        let row = usize::try_from(row)
            .map_err(|_| duckdb_failure_from_message(format!("row index {row} exceeds usize range")))?;
        self.try_row_is_null_in_capacity(row)
    }

    fn try_row_is_null_in_capacity(&self, row: usize) -> Result<bool> {
        self.check_index(row, "row index")?;

        // SAFETY: the row is inside the explicit effective capacity. Validity
        // storage is initialized independently of payload storage.
        let valid = unsafe {
            let validity = duckdb_vector_get_validity(self.ptr);
            if validity.is_null() {
                return Ok(false);
            }
            duckdb_validity_row_is_valid(validity, row as u64)
        };
        Ok(!valid)
    }

    pub(super) fn row_is_null(&self, row: u64) -> bool {
        self.try_row_is_null(row).or_panic()
    }

    pub(super) fn try_set_null(&mut self, row: usize) -> Result<()> {
        self.ensure_writable()?;
        self.check_index(row, "row index")?;
        // This is the only safe validity mutation and it only transitions a
        // row from valid to invalid. In particular, a list entry whose child
        // bounds were skipped because its parent is null cannot later become
        // readable through safe code without rewriting the entry.
        // SAFETY: the row is bounded above and the mutable view is unique.
        unsafe {
            duckdb_vector_ensure_validity_writable(self.ptr);
            let validity = duckdb_vector_get_validity(self.ptr);
            if validity.is_null() {
                return Err(duckdb_failure_from_message(
                    "DuckDB did not return a writable validity mask",
                ));
            }
            duckdb_validity_set_row_invalid(validity, row as u64);
        }
        Ok(())
    }

    pub(super) fn set_null(&mut self, row: usize) {
        self.try_set_null(row).or_panic();
    }

    pub(super) fn data_mut_ptr<T>(&mut self) -> Result<*mut T> {
        self.ensure_writable()?;
        self.raw_data_ptr()
    }

    pub(super) fn data_ptr<T>(&self) -> Result<*const T> {
        self.ensure_initialized()?;
        self.raw_data_ptr::<T>().map(|ptr| ptr.cast_const())
    }

    fn raw_data_ptr<T>(&self) -> Result<*mut T> {
        let ptr = unsafe { duckdb_vector_get_data(self.ptr).cast::<T>() };
        if ptr.is_null() {
            Err(duckdb_failure_from_message(
                "DuckDB returned a null vector data pointer",
            ))
        } else {
            Ok(ptr)
        }
    }

    pub(super) unsafe fn write<T>(&mut self, row: usize, value: T) -> Result<()> {
        self.check_index(row, "row")?;
        let ptr = self.data_mut_ptr::<T>()?;
        // SAFETY: the row is bounded above and the mutable view uniquely owns
        // this slot for the duration of the write.
        unsafe { ptr.add(row).write(value) };
        Ok(())
    }

    pub(super) unsafe fn as_slice<T>(&self, len: usize) -> Result<&[T]> {
        self.check_slice_len(len)?;
        let ptr = self.data_ptr()?;
        Ok(unsafe { slice::from_raw_parts(ptr, len) })
    }

    pub(super) unsafe fn as_mut_slice<T>(&mut self, len: usize) -> Result<&mut [T]> {
        self.check_slice_len(len)?;
        let ptr = self.data_mut_ptr()?;
        Ok(unsafe { slice::from_raw_parts_mut(ptr, len) })
    }

    fn check_slice_len(&self, len: usize) -> Result<()> {
        if len > self.capacity {
            return Err(duckdb_failure_from_message(format!(
                "slice length {len} exceeds vector capacity {}",
                self.capacity
            )));
        }
        Ok(())
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
            )
        })
    }

    pub(super) fn expect_list(&self) -> Result<()> {
        self.expect_type("list or map", |id| {
            matches!(id, LogicalTypeId::List | LogicalTypeId::Map)
        })
    }

    pub(super) fn expect_array(&self) -> Result<()> {
        self.expect_type("array", |id| id == LogicalTypeId::Array)
    }

    pub(super) fn expect_struct(&self) -> Result<()> {
        self.expect_type("struct", |id| id == LogicalTypeId::Struct)
    }
}

#[cfg_attr(not(feature = "vtab-arrow"), allow(dead_code))]
impl VectorRef<'_> {
    pub(crate) fn reborrow_initialized(&self) -> Result<VectorRef<'_>> {
        self.ensure_initialized()?;
        // SAFETY: the original initialized view keeps the pointer live and
        // shared reads may coexist. The shared state revokes stale reborrows.
        let mut vector = unsafe {
            Self::new(
                self.ptr,
                self.capacity,
                self.state.reborrow(),
                self.readable_span,
                VectorAccess::ReadOnly,
            )
        }?;
        vector.reserved_child_capacity = self.reserved_child_capacity;
        Ok(vector)
    }

    /// Reads one initialized physical value.
    ///
    /// # Safety
    ///
    /// `T` must match DuckDB's physical storage for this vector, and `row` must
    /// be non-null. Initialization guarantees readable non-null payloads only;
    /// DuckDB may leave payload bytes beneath null rows uninitialized.
    pub(crate) unsafe fn read<T: Copy>(&self, row: usize) -> Result<T> {
        self.check_read_index(row, "row")?;
        let ptr = self.data_ptr::<T>()?;
        // SAFETY: the caller selects the physical type and a non-null row; the
        // row is bounded above and initialized views guarantee its payload.
        Ok(unsafe { ptr.add(row).read() })
    }
}

#[cfg(all(test, feature = "vtab-arrow"))]
pub(crate) use tests::set_list_entry_unchecked_for_test;

#[cfg(test)]
mod tests;

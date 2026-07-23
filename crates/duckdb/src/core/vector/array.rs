use super::{FlatVector, ResultExt, VectorAccess, VectorRef, WritableVectorRef};
use crate::{
    Result,
    core::LogicalTypeHandle,
    error::duckdb_failure_from_message,
    ffi::{duckdb_array_type_array_size, duckdb_array_vector_get_child},
};

impl VectorRef<'_> {
    pub(super) fn array_size(&self) -> Result<usize> {
        self.expect_array()?;
        let size = unsafe { duckdb_array_type_array_size(self.logical_type.ptr) };
        usize::try_from(size)
            .map_err(|_| duckdb_failure_from_message("DuckDB array size exceeds usize range"))
            .and_then(|size| {
                if size == 0 {
                    Err(duckdb_failure_from_message(
                        "DuckDB array size must be greater than zero",
                    ))
                } else {
                    Ok(size)
                }
            })
    }

    fn array_child_capacity(&self) -> Result<usize> {
        self.capacity
            .checked_mul(self.array_size()?)
            .ok_or_else(|| duckdb_failure_from_message("array child capacity overflows usize"))
    }

    pub(super) fn array_child_mut(&mut self, capacity: usize) -> Result<VectorRef<'_>> {
        self.ensure_writable()?;
        let array_size = self.array_size()?;
        if capacity % array_size != 0 {
            return Err(duckdb_failure_from_message(
                "array child capacity must be a multiple of the fixed array size",
            ));
        }
        let backing_capacity = self.array_child_capacity()?;
        if capacity > backing_capacity {
            let remedy = if backing_capacity == 0 {
                "; reserve or commit the containing list child before writing nested values"
            } else {
                ""
            };
            return Err(duckdb_failure_from_message(format!(
                "array child capacity {capacity} exceeds backing capacity {backing_capacity}{remedy}"
            )));
        }
        let ptr = unsafe { duckdb_array_vector_get_child(self.ptr) };
        // SAFETY: checked multiplication and bounds prove the child span.
        unsafe {
            Self::new(
                ptr,
                capacity,
                self.state.reborrow(),
                self.readable_span.scaled(array_size)?,
                self.access,
            )
        }
    }
}

#[cfg_attr(not(feature = "vtab-arrow"), allow(dead_code))]
impl VectorRef<'_> {
    pub(super) fn array_child_read(&self) -> Result<VectorRef<'_>> {
        self.ensure_initialized()?;
        let capacity = self.array_child_capacity()?;
        let readable_span = self.readable_span.scaled(self.array_size()?)?;
        let ptr = unsafe { duckdb_array_vector_get_child(self.ptr) };
        // SAFETY: fixed-array children contain `array_size` values per parent.
        unsafe {
            Self::new(
                ptr,
                capacity,
                self.state.reborrow(),
                readable_span,
                VectorAccess::ReadOnly,
            )
        }
    }
}

/// A fixed-size array vector borrowed from a data chunk.
///
/// Writable child access requires a mutable borrow of the parent, keeping the
/// returned child borrow-scoped and preventing overlapping mutable views.
pub struct ArrayVector<'a> {
    pub(super) vector: VectorRef<'a>,
}

impl<'a> ArrayVector<'a> {
    pub(crate) fn from_vector(vector: VectorRef<'a>) -> Result<Self> {
        vector.expect_array()?;
        Ok(Self { vector })
    }

    /// Returns the logical type.
    pub fn logical_type(&self) -> LogicalTypeHandle {
        self.vector.logical_type_owned().or_panic()
    }

    /// Returns the fixed array width.
    pub fn get_array_size(&self) -> u64 {
        self.try_get_array_size().or_panic() as u64
    }

    pub(crate) fn try_get_array_size(&self) -> Result<usize> {
        self.vector.array_size()
    }

    /// Returns true if one parent row is null.
    pub fn row_is_null(&self, row: u64) -> bool {
        self.vector.row_is_null(row)
    }

    /// Returns whether one parent row is null, or an out-of-range error.
    pub fn try_row_is_null(&self, row: u64) -> Result<bool> {
        self.vector.try_row_is_null(row)
    }

    /// Returns a flat child view with an explicit element capacity.
    ///
    /// # Panics
    ///
    /// Panics if `capacity` is not a multiple of the fixed array width or
    /// exceeds the array child's backing capacity.
    pub fn child(&mut self, capacity: usize) -> FlatVector<'_> {
        FlatVector::from_vector(self.vector.array_child_mut(capacity).or_panic()).or_panic()
    }

    /// Copies primitive data into the fixed-array child.
    ///
    /// # Safety
    /// `T` must match the child vector's physical storage.
    ///
    /// # Panics
    ///
    /// Panics if `data.len()` is not a multiple of the fixed array width or
    /// exceeds the child backing capacity.
    pub unsafe fn set_child<T: Copy>(&mut self, data: &[T]) {
        unsafe { self.child(data.len()).copy(data) };
    }

    /// Marks one parent row null.
    ///
    /// # Panics
    ///
    /// Panics if `row` exceeds the parent capacity.
    pub fn set_null(&mut self, row: usize) {
        self.vector.set_null(row);
    }
}

#[cfg_attr(not(feature = "vtab-arrow"), allow(dead_code))]
impl ArrayVector<'_> {
    pub(crate) fn read_child(&self) -> Result<VectorRef<'_>> {
        self.vector.array_child_read()
    }

    pub(crate) fn writable_child(&mut self, capacity: usize) -> Result<WritableVectorRef<'_>> {
        Ok(WritableVectorRef::from_vector(self.vector.array_child_mut(capacity)?))
    }
}

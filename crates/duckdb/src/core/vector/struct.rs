use libduckdb_sys::DuckDbString;

use super::{FlatVector, ResultExt, VectorAccess, VectorRef, WritableVectorRef, array::ArrayVector, list::ListVector};
use crate::{
    Result,
    core::LogicalTypeHandle,
    error::duckdb_failure_from_message,
    ffi::{duckdb_struct_type_child_count, duckdb_struct_vector_get_child},
};

impl VectorRef<'_> {
    pub(super) fn struct_child_count(&self) -> Result<usize> {
        self.expect_struct()?;
        let count = unsafe { duckdb_struct_type_child_count(self.logical_type.ptr) };
        usize::try_from(count).map_err(|_| duckdb_failure_from_message("DuckDB struct child count exceeds usize range"))
    }

    fn check_struct_child(&self, index: usize) -> Result<()> {
        let count = self.struct_child_count()?;
        if index < count {
            Ok(())
        } else {
            Err(duckdb_failure_from_message(format!(
                "struct child index {index} exceeds child count {count}"
            )))
        }
    }

    pub(super) fn struct_child_mut(&mut self, index: usize, capacity: usize) -> Result<VectorRef<'_>> {
        self.ensure_writable()?;
        self.check_struct_child(index)?;
        if capacity > self.capacity {
            return Err(duckdb_failure_from_message(format!(
                "struct child capacity {capacity} exceeds parent capacity {}",
                self.capacity
            )));
        }
        let ptr = unsafe { duckdb_struct_vector_get_child(self.ptr, index as u64) };
        // SAFETY: DuckDB keeps each child vector live with its parent, the
        // explicit capacity is bounded by the parent, and the mutable parent
        // borrow uniquely reaches this child.
        unsafe { Self::new(ptr, capacity, self.state.reborrow(), self.readable_span, self.access) }
    }
}

#[cfg_attr(not(feature = "vtab-arrow"), allow(dead_code))]
impl VectorRef<'_> {
    pub(super) fn struct_child_read(&self, index: usize) -> Result<VectorRef<'_>> {
        self.ensure_initialized()?;
        self.check_struct_child(index)?;
        let ptr = unsafe { duckdb_struct_vector_get_child(self.ptr, index as u64) };
        // SAFETY: struct children inherit the explicit parent row capacity.
        unsafe {
            Self::new(
                ptr,
                self.capacity,
                self.state.reborrow(),
                self.readable_span,
                VectorAccess::ReadOnly,
            )
        }
    }
}

/// A struct-physical vector borrowed from a data chunk.
///
/// Writable child access requires a mutable borrow of the parent. Each child
/// remains borrow-scoped so safe code cannot hold overlapping mutable sibling
/// views. DuckDB represents `STRUCT`, `UNION`, and `VARIANT` with this layout.
/// Child indices and counts describe that physical layout; they need not match
/// the logical children reported by [`LogicalTypeHandle`].
pub struct StructVector<'a> {
    pub(super) vector: VectorRef<'a>,
}

impl<'a> StructVector<'a> {
    pub(crate) fn from_vector(vector: VectorRef<'a>) -> Result<Self> {
        vector.expect_struct()?;
        Ok(Self { vector })
    }

    /// Returns a flat child with an explicit effective capacity.
    ///
    /// # Panics
    ///
    /// Panics if `index` or `capacity` is out of range.
    pub fn child(&mut self, index: usize, capacity: usize) -> FlatVector<'_> {
        FlatVector::from_vector(self.child_vector(index, capacity)).or_panic()
    }

    /// Returns a nested struct child.
    pub fn struct_vector_child(&mut self, index: usize) -> StructVector<'_> {
        let capacity = self.vector.capacity();
        StructVector::from_vector(self.child_vector(index, capacity)).or_panic()
    }

    /// Returns a nested list child.
    pub fn list_vector_child(&mut self, index: usize) -> ListVector<'_> {
        let capacity = self.vector.capacity();
        ListVector::from_vector(self.child_vector(index, capacity)).or_panic()
    }

    /// Returns a nested fixed-array child.
    pub fn array_vector_child(&mut self, index: usize) -> ArrayVector<'_> {
        let capacity = self.vector.capacity();
        ArrayVector::from_vector(self.child_vector(index, capacity)).or_panic()
    }

    /// Returns the logical type.
    pub fn logical_type(&self) -> LogicalTypeHandle {
        self.vector.logical_type_owned().or_panic()
    }

    /// Returns true if one parent row is null.
    pub fn row_is_null(&self, row: u64) -> bool {
        self.vector.row_is_null(row)
    }

    /// Returns whether one parent row is null, or an out-of-range error.
    pub fn try_row_is_null(&self, row: u64) -> Result<bool> {
        self.vector.try_row_is_null(row)
    }

    /// Returns a physical child field name.
    ///
    /// This uses the same physical indexing as [`Self::num_children`]. In
    /// particular, a `UNION` includes DuckDB's hidden tag child, whose name is
    /// empty. Do not use these indices to access logical child metadata.
    ///
    /// # Panics
    /// Panics if `index` is out of range.
    pub fn child_name(&self, index: usize) -> DuckDbString {
        // DuckDB only debug-asserts this bound, so a release build would read
        // past the child vector and duplicate whatever it found. The null
        // check below cannot stand in for the bound: DuckDB never returns null
        // for an out-of-range index.
        self.vector.check_struct_child(index).or_panic();
        let logical_type = self.logical_type();
        unsafe {
            let ptr = crate::ffi::duckdb_struct_type_child_name(logical_type.ptr, index as u64);
            if ptr.is_null() {
                panic!("child index {index} out of range");
            }
            DuckDbString::from_ptr(ptr)
        }
    }

    /// Returns the number of physical struct children.
    ///
    /// This may differ from [`LogicalTypeHandle::num_children`]. A `UNION`, for
    /// example, includes DuckDB's hidden tag child in this count, while its
    /// logical type reports only the user-declared members.
    pub fn num_children(&self) -> usize {
        self.vector.struct_child_count().or_panic()
    }

    /// Marks one parent row null.
    pub fn set_null(&mut self, row: usize) {
        self.vector.set_null(row);
    }

    fn child_vector(&mut self, index: usize, capacity: usize) -> VectorRef<'_> {
        self.vector.struct_child_mut(index, capacity).or_panic()
    }
}

#[cfg_attr(not(feature = "vtab-arrow"), allow(dead_code))]
impl StructVector<'_> {
    pub(crate) fn read_child(&self, index: usize) -> Result<VectorRef<'_>> {
        self.vector.struct_child_read(index)
    }

    pub(crate) fn writable_child(&mut self, index: usize, capacity: usize) -> Result<WritableVectorRef<'_>> {
        Ok(WritableVectorRef::from_vector(
            self.vector.struct_child_mut(index, capacity)?,
        ))
    }
}

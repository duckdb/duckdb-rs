use super::{ArrayVector, FlatVector, ListVector, ResultExt, StructVector, VectorRef};
use crate::{Result, ffi::duckdb_vector};

/// A writable DuckDB vector interface.
///
/// This trait lives in the Arrow-independent core. Its historical Arrow path
/// remains available as a compatibility re-export.
///
/// A bare [`duckdb_vector`] does not expose its backing capacity and therefore
/// does not implement this trait. Adapt it with [`WritableVectorRef::from_raw`]
/// and an explicit capacity instead.
///
/// ```compile_fail
/// use duckdb::{core::WritableVector, ffi::duckdb_vector};
///
/// fn capacityless_view(raw: &mut duckdb_vector) {
///     let _ = raw.flat_vector();
/// }
/// ```
pub trait WritableVector {
    /// Borrows the vector as flat storage.
    fn flat_vector(&mut self) -> FlatVector<'_>;
    /// Borrows the vector as list or map storage.
    fn list_vector(&mut self) -> ListVector<'_>;
    /// Borrows the vector as fixed-array storage.
    fn array_vector(&mut self) -> ArrayVector<'_>;
    /// Borrows the vector as struct-physical storage.
    fn struct_vector(&mut self) -> StructVector<'_>;
}

/// A capacity-bearing writable adapter for a borrowed DuckDB vector.
///
/// Chunks and nested vectors construct this adapter internally. Callers that
/// only have a raw [`duckdb_vector`] must use [`WritableVectorRef::from_raw`]
/// and state the vector's complete logical row extent explicitly.
pub struct WritableVectorRef<'a> {
    vector: VectorRef<'a>,
}

impl<'a> WritableVectorRef<'a> {
    pub(in crate::core) fn from_vector(vector: VectorRef<'a>) -> Self {
        Self { vector }
    }

    /// Adapts a raw vector into the native writable seam.
    ///
    /// This is a temporary compatibility constructor for the scalar and table
    /// callback transports. Owner-backed native code should obtain writable
    /// views from its chunk or parent vector instead.
    ///
    /// # Safety
    /// The vector referenced by `ptr` must stay live, be uniquely writable for
    /// `'a`, and have backing storage for `capacity` rows. No other access path
    /// to the underlying vector, including its owner, may be used while the
    /// adapter is live. A raw vector carries no owner identity, so this adapter
    /// cannot join a [`DataChunkHandle`](crate::core::DataChunkHandle)'s
    /// owner-scoped alias registry; the caller must enforce that exclusivity.
    ///
    /// `capacity` must be the vector's **complete logical row extent** for this
    /// adapter invocation, not a caller-selected prefix. It need not equal the
    /// whole backing allocation; for a scalar callback, the current callback
    /// cardinality is the logical extent. Vector-wide operations reached
    /// through this adapter, such as committing a list-child size, reset stale
    /// list entries only across the rows the adapter admits. Understating the
    /// logical extent leaves entries above `capacity` pointing into storage
    /// the new size no longer covers.
    ///
    /// # Errors
    ///
    /// Returns an error if the pointer or logical type is null.
    pub unsafe fn from_raw(ptr: &'a mut duckdb_vector, capacity: usize) -> Result<Self> {
        let vector = unsafe { VectorRef::writable(*ptr, capacity) }?;
        Ok(Self::from_vector(vector))
    }

    #[cfg(test)]
    pub(super) fn initialized_for_test(&self) -> bool {
        self.vector.is_initialized()
    }
}

impl WritableVector for WritableVectorRef<'_> {
    fn flat_vector(&mut self) -> FlatVector<'_> {
        FlatVector::from_vector(self.vector.reborrow().or_panic()).or_panic()
    }

    fn list_vector(&mut self) -> ListVector<'_> {
        ListVector::from_vector(self.vector.reborrow().or_panic()).or_panic()
    }

    fn array_vector(&mut self) -> ArrayVector<'_> {
        ArrayVector::from_vector(self.vector.reborrow().or_panic()).or_panic()
    }

    fn struct_vector(&mut self) -> StructVector<'_> {
        StructVector::from_vector(self.vector.reborrow().or_panic()).or_panic()
    }
}

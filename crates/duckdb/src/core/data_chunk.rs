#[cfg(test)]
use super::vector::UNDER_CONSTRUCTION_MESSAGE;
use super::{
    logical_type::LogicalTypeHandle,
    vector::{
        ArrayVector, BorrowRegistry, FlatVector, ListVector, ResultExt, StructVector, VectorRef, VectorState,
        VectorStateCell, WritableVectorRef,
    },
};
use crate::ffi::{
    duckdb_create_data_chunk, duckdb_data_chunk, duckdb_data_chunk_get_column_count, duckdb_data_chunk_get_size,
    duckdb_data_chunk_get_vector, duckdb_data_chunk_set_size, duckdb_destroy_data_chunk,
};
use crate::{Result, error::duckdb_failure_from_message};

/// Handle to the DataChunk in DuckDB.
pub struct DataChunkHandle {
    /// Pointer to the DataChunk in duckdb C API.
    ptr: duckdb_data_chunk,

    /// Whether this `DataChunkHandle` owns `ptr`.
    owned: bool,

    /// Effective capacity exposed for every top-level vector in this chunk.
    capacity: usize,

    /// Read/write state and committed initialized span shared by every view.
    state: VectorStateCell,

    /// Live vector views owned by this handle.
    borrows: BorrowRegistry,
}

// Shared mutations are either conservative state transitions or pointer-lease
// bookkeeping, and neither leaves the native interface unsound after an
// unwind. Keep the public owner's compatibility guarantee explicit instead of
// choosing synchronization primitives solely for their auto traits.
impl std::panic::RefUnwindSafe for DataChunkHandle {}

impl Drop for DataChunkHandle {
    fn drop(&mut self) {
        if self.owned && !self.ptr.is_null() {
            unsafe { duckdb_destroy_data_chunk(&mut self.ptr) }
            self.ptr = std::ptr::null_mut();
        }
    }
}

impl DataChunkHandle {
    /// Wrap a callback-owned chunk without taking ownership.
    ///
    /// This temporary callback transport does not distinguish read-only input
    /// from under-construction output.
    ///
    /// # Safety
    ///
    /// `ptr` must be a valid `duckdb_data_chunk` that stays allocated for
    /// the handle's lifetime. Its current cardinality must describe initialized
    /// input whenever this compatibility handle is read.
    #[cfg(feature = "vtab")]
    pub(crate) unsafe fn new_unowned(ptr: duckdb_data_chunk) -> Self {
        let readable_len = unsafe { duckdb_data_chunk_get_size(ptr) as usize };
        Self {
            ptr,
            owned: false,
            capacity: standard_vector_capacity(),
            state: VectorStateCell::new(VectorState::Initialized { readable_len }),
            borrows: BorrowRegistry::default(),
        }
    }

    /// Create a new [DataChunkHandle] with the given [LogicalTypeHandle]s.
    ///
    /// Its payload starts under construction. Native Arrow writers finalize it
    /// automatically; manual writers must use [`Self::assume_initialized`]
    /// before safe reads or Arrow conversion.
    ///
    /// # Panics
    ///
    /// Panics if DuckDB rejects one of the logical types or cannot initialize
    /// the data chunk.
    pub fn new(logical_types: &[LogicalTypeHandle]) -> Self {
        let num_columns = logical_types.len();
        let mut c_types = Vec::with_capacity(num_columns);
        c_types.extend(logical_types.iter().map(|t| t.ptr));
        let ptr = unsafe { duckdb_create_data_chunk(c_types.as_mut_ptr(), num_columns as u64) };
        assert!(
            !ptr.is_null(),
            "DuckDB could not create data chunk for the requested logical types"
        );
        Self {
            ptr,
            owned: true,
            capacity: standard_vector_capacity(),
            state: VectorStateCell::new(VectorState::UnderConstruction),
            borrows: BorrowRegistry::default(),
        }
    }

    /// Get the vector at the specific column index: `idx`.
    ///
    /// A second simultaneous view of the same column panics. Views of distinct
    /// columns may coexist, which preserves callback input patterns.
    ///
    /// # Panics
    ///
    /// Panics if `idx` is out of range, the column is not represented by flat
    /// storage (`LIST`, `MAP`, `ARRAY`, `STRUCT`, `UNION`, or `VARIANT`), or the column
    /// already has a live vector view.
    pub fn flat_vector(&self, idx: usize) -> FlatVector<'_> {
        FlatVector::from_vector(self.shared_vector_view(idx)).or_panic()
    }

    /// Get a list vector from the column index.
    ///
    /// # Panics
    ///
    /// Panics if `idx` is out of range, the column is not `LIST` or `MAP`, or
    /// the column already has a live vector view.
    pub fn list_vector(&self, idx: usize) -> ListVector<'_> {
        ListVector::from_vector(self.shared_vector_view(idx)).or_panic()
    }

    /// Get an array vector from the column index.
    ///
    /// # Panics
    ///
    /// Panics if `idx` is out of range, the column is not `ARRAY`, or the
    /// column already has a live vector view.
    pub fn array_vector(&self, idx: usize) -> ArrayVector<'_> {
        ArrayVector::from_vector(self.shared_vector_view(idx)).or_panic()
    }

    /// Get struct vector at the column index: `idx`.
    ///
    /// # Panics
    ///
    /// Panics if `idx` is out of range, the column is not represented by
    /// struct-physical storage (`STRUCT`, `UNION`, or `VARIANT`), or the column
    /// already has a live vector view.
    pub fn struct_vector(&self, idx: usize) -> StructVector<'_> {
        StructVector::from_vector(self.shared_vector_view(idx)).or_panic()
    }

    /// Builds a shared view for the family-specific vector interface.
    ///
    /// The pointer-keyed lease prevents two safe writable-capable views of the
    /// same vector. The shared state gates reads while a chunk is under
    /// construction. Arrow reads join this lease, while native writable access
    /// requires an exclusive borrow of the chunk.
    fn shared_vector_view(&self, idx: usize) -> VectorRef<'_> {
        self.check_column_index(idx).or_panic();
        let ptr = unsafe { duckdb_data_chunk_get_vector(self.ptr, idx as u64) };
        let capacity = self.capacity;
        // SAFETY: the shared chunk state dynamically gates reads and mutation.
        // The pointer-keyed guard closes the shared interface's mutable-aliasing
        // hole.
        unsafe { VectorRef::shared_view(ptr, capacity, &self.state) }
            .and_then(|vector| self.borrows.acquire(ptr).map(|guard| vector.with_borrow_guard(guard)))
            .map_err(|error| duckdb_failure_from_message(format!("column {idx}: {error}")))
            .or_panic()
    }

    fn check_column_index(&self, idx: usize) -> Result<()> {
        let count = self.num_columns();
        if idx < count {
            Ok(())
        } else {
            Err(duckdb_failure_from_message(format!(
                "column index {idx} exceeds data chunk column count {count}"
            )))
        }
    }

    fn check_vector_capacity(&self, capacity: usize) -> Result<()> {
        if capacity <= self.capacity {
            Ok(())
        } else {
            Err(duckdb_failure_from_message(format!(
                "top-level vector capacity {capacity} exceeds data chunk capacity {}",
                self.capacity
            )))
        }
    }

    fn check_readable_len(&self, len: usize) -> Result<()> {
        let readable_len = self.state.readable_len_or_err()?;
        if len <= readable_len {
            Ok(())
        } else {
            Err(duckdb_failure_from_message(format!(
                "requested initialized vector length {len} exceeds data chunk length {readable_len}"
            )))
        }
    }

    fn ensure_writable(&self) -> Result<()> {
        match self.state.get() {
            VectorState::CallbackInput { .. } => Err(duckdb_failure_from_message(
                "DuckDB callback input data chunks are read-only",
            )),
            VectorState::Initialized { .. } | VectorState::UnderConstruction => Ok(()),
        }
    }

    /// Set the size of the data chunk.
    ///
    /// This remains a shared-reference operation so changing cardinality can
    /// revoke reads through already-live vector views. The shared state
    /// coordinates that revocation and rejects callback input at runtime.
    ///
    /// # Panics
    ///
    /// Panics if `new_len` exceeds the backing capacity.
    pub fn set_len(&self, new_len: usize) {
        self.try_set_len(new_len).or_panic();
    }

    /// Set the size of the data chunk after checking its backing capacity.
    pub fn try_set_len(&self, new_len: usize) -> Result<()> {
        self.ensure_writable()?;
        if new_len > self.capacity {
            return Err(duckdb_failure_from_message(format!(
                "data chunk length {new_len} exceeds capacity {}",
                self.capacity
            )));
        }
        let old_len = self.len();
        unsafe { duckdb_data_chunk_set_size(self.ptr, new_len as u64) };
        match self.state.get() {
            VectorState::CallbackInput { .. } => unreachable!("read-only chunks were rejected above"),
            VectorState::Initialized { readable_len } if new_len <= old_len => {
                self.state.set(VectorState::Initialized {
                    readable_len: readable_len.min(new_len),
                });
            }
            VectorState::Initialized { .. } => self.state.set(VectorState::UnderConstruction),
            VectorState::UnderConstruction => {}
        }
        Ok(())
    }

    /// Get the length / the number of rows in this [DataChunkHandle].
    pub fn len(&self) -> usize {
        unsafe { duckdb_data_chunk_get_size(self.ptr) as usize }
    }

    /// Check whether this [DataChunkHandle] is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get the number of columns in this [DataChunkHandle].
    pub fn num_columns(&self) -> usize {
        unsafe { duckdb_data_chunk_get_column_count(self.ptr) as usize }
    }

    /// Return the effective capacity of each top-level vector in this chunk.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Declare that every non-null payload in `0..self.len()` is initialized.
    ///
    /// This is only needed after manually filling a chunk through raw or typed
    /// vector writes. Native Arrow writers and DuckDB callback input establish
    /// this state automatically.
    ///
    /// # Safety
    ///
    /// Every non-null top-level payload in `0..self.len()` and every non-null
    /// payload in the full committed spans of its nested child vectors must be
    /// initialized. No writable vector view of this chunk may remain live.
    ///
    pub unsafe fn assume_initialized(&mut self) {
        unsafe { self.mark_initialized() }.or_panic();
    }

    pub(crate) unsafe fn mark_initialized(&mut self) -> Result<()> {
        self.ensure_writable()?;
        self.state.set(VectorState::Initialized {
            readable_len: self.len(),
        });
        Ok(())
    }

    /// Get the ptr of duckdb_data_chunk in this [DataChunkHandle].
    pub fn get_ptr(&self) -> duckdb_data_chunk {
        self.ptr
    }
}

#[cfg_attr(not(feature = "vtab-arrow"), allow(dead_code))]
impl DataChunkHandle {
    pub(crate) fn initialized_vector(&self, idx: usize, len: usize) -> Result<VectorRef<'_>> {
        self.check_column_index(idx)?;
        self.check_vector_capacity(len)?;
        self.check_readable_len(len)?;
        let ptr = unsafe { duckdb_data_chunk_get_vector(self.ptr, idx as u64) };
        // SAFETY: chunk initialization gates this construction, and the
        // requested span was checked against committed initialized rows.
        let vector = unsafe { VectorRef::initialized_from_chunk(ptr, len, &self.state) }?;
        self.borrows.acquire(ptr).map(|guard| vector.with_borrow_guard(guard))
    }

    pub(crate) fn writable_vector(&mut self, idx: usize, capacity: usize) -> Result<WritableVectorRef<'_>> {
        self.check_column_index(idx)?;
        self.check_vector_capacity(capacity)?;
        self.begin_write()?;
        let ptr = unsafe { duckdb_data_chunk_get_vector(self.ptr, idx as u64) };
        // SAFETY: the mutable chunk borrow uniquely owns the column for the
        // returned view's lifetime and DuckDB allocated the requested span.
        Ok(WritableVectorRef::from_vector(unsafe {
            VectorRef::writable_from_chunk(ptr, capacity, &self.state)?
        }))
    }

    pub(crate) fn begin_write(&mut self) -> Result<()> {
        self.ensure_writable()?;
        self.state.set(VectorState::UnderConstruction);
        Ok(())
    }
}

fn standard_vector_capacity() -> usize {
    unsafe { crate::ffi::duckdb_vector_size() as usize }
}

#[cfg(test)]
mod test {
    use super::{super::logical_type::LogicalTypeId, *};
    use crate::panic_utils::panic_payload;

    #[test]
    fn test_data_chunk_construction() {
        let dc = DataChunkHandle::new(&[LogicalTypeHandle::from(LogicalTypeId::Integer)]);

        assert_eq!(dc.num_columns(), 1);

        drop(dc);
    }

    #[test]
    fn data_chunk_creation_rejects_null_ffi_result() {
        let result = std::panic::catch_unwind(|| {
            DataChunkHandle::new(&[LogicalTypeHandle::from(LogicalTypeId::Any)]);
        })
        .unwrap_err();

        assert!(panic_payload(result.as_ref()).contains("DuckDB could not create data chunk"));
    }

    #[test]
    fn test_vector() {
        let mut datachunk = DataChunkHandle::new(&[LogicalTypeHandle::from(LogicalTypeId::Bigint)]);
        {
            let mut vector = datachunk.flat_vector(0);
            unsafe { vector.write(0, 42_i64) };
        }
        datachunk.set_len(1);
        unsafe { datachunk.assume_initialized() };

        let vector = FlatVector::from_vector(datachunk.initialized_vector(0, 1).unwrap()).unwrap();
        assert_eq!(unsafe { vector.as_slice_with_len::<i64>(1) }, &[42]);
    }

    #[test]
    fn flat_vector_rejects_nested_columns() {
        let list_type = LogicalTypeHandle::list(&LogicalTypeId::Integer.into());
        let datachunk = DataChunkHandle::new(&[list_type]);

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            datachunk.flat_vector(0);
        }))
        .unwrap_err();

        assert!(panic_payload(result.as_ref()).contains("expected scalar vector, got List"));
    }

    #[test]
    fn unfinished_chunk_cannot_create_initialized_vector() {
        let datachunk = DataChunkHandle::new(&[LogicalTypeHandle::from(LogicalTypeId::Bigint)]);

        assert_eq!(
            datachunk.initialized_vector(0, 0).err().unwrap().to_string(),
            UNDER_CONSTRUCTION_MESSAGE
        );
    }

    #[test]
    fn test_duplicate_column_vector_view_is_rejected() {
        let datachunk = DataChunkHandle::new(&[LogicalTypeHandle::from(LogicalTypeId::Bigint)]);
        let first = datachunk.flat_vector(0);

        let duplicate = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            datachunk.flat_vector(0);
        }))
        .unwrap_err();
        assert!(
            panic_payload(duplicate.as_ref()).contains("already has a live view; drop it before requesting another")
        );

        drop(first);
        let _second = datachunk.flat_vector(0);
    }

    #[test]
    fn test_raw_vector_uses_explicit_capacity() {
        let logical_type = LogicalTypeHandle::from(LogicalTypeId::Bigint);
        let mut ptr = unsafe { crate::ffi::duckdb_create_vector(logical_type.ptr, 1) };

        {
            let mut raw = unsafe { crate::core::WritableVectorRef::from_raw(&mut ptr, 1) }.unwrap();
            let vector = crate::core::WritableVector::flat_vector(&mut raw);
            assert_eq!(vector.capacity(), 1);
        }

        unsafe { crate::ffi::duckdb_destroy_vector(&mut ptr) };
    }

    #[test]
    fn raw_writable_vectors_report_an_actionable_read_error() {
        let logical_type = LogicalTypeHandle::from(LogicalTypeId::Bigint);
        let mut ptr = unsafe { crate::ffi::duckdb_create_vector(logical_type.ptr, 1) };

        {
            let mut raw = unsafe { crate::core::WritableVectorRef::from_raw(&mut ptr, 1) }.unwrap();
            let vector = crate::core::WritableVector::flat_vector(&mut raw);
            let error = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                unsafe { vector.as_slice_with_len::<i64>(1) };
            }))
            .unwrap_err();
            assert!(panic_payload(error.as_ref()).contains(UNDER_CONSTRUCTION_MESSAGE));
        }

        unsafe { crate::ffi::duckdb_destroy_vector(&mut ptr) };
    }

    #[test]
    fn test_data_chunk_cardinality_is_checked() {
        let datachunk = DataChunkHandle::new(&[LogicalTypeHandle::from(LogicalTypeId::Bigint)]);
        let oversized = datachunk.capacity + 1;

        assert_eq!(
            datachunk.try_set_len(oversized).unwrap_err().to_string(),
            format!("data chunk length {oversized} exceeds capacity {}", datachunk.capacity)
        );
        assert_eq!(datachunk.len(), 0);
    }

    #[test]
    fn growing_initialized_chunk_revokes_read_access() {
        let mut datachunk = DataChunkHandle::new(&[LogicalTypeHandle::from(LogicalTypeId::Bigint)]);
        {
            let mut vector = datachunk.flat_vector(0);
            unsafe { vector.write(0, 42_i64) };
        }
        datachunk.set_len(1);
        unsafe { datachunk.assume_initialized() };
        assert!(datachunk.initialized_vector(0, 1).is_ok());
        let stale_view = datachunk.flat_vector(0);

        datachunk.set_len(2);

        assert_eq!(
            stale_view.reborrow_initialized().err().unwrap().to_string(),
            UNDER_CONSTRUCTION_MESSAGE
        );
        drop(stale_view);
        assert_eq!(
            datachunk.initialized_vector(0, 2).err().unwrap().to_string(),
            UNDER_CONSTRUCTION_MESSAGE
        );
    }

    #[test]
    fn initialized_vector_is_bounded_by_committed_length() {
        let mut datachunk = DataChunkHandle::new(&[LogicalTypeHandle::from(LogicalTypeId::Bigint)]);
        datachunk.flat_vector(0).set_null(0);
        datachunk.set_len(1);
        unsafe { datachunk.assume_initialized() };

        assert_eq!(
            datachunk.initialized_vector(0, 2).err().unwrap().to_string(),
            "requested initialized vector length 2 exceeds data chunk length 1"
        );
    }

    #[test]
    fn shrinking_chunk_reduces_existing_initialized_vector_span() {
        let mut datachunk = DataChunkHandle::new(&[LogicalTypeHandle::from(LogicalTypeId::Bigint)]);
        {
            let mut vector = datachunk.flat_vector(0);
            vector.set_null(0);
            vector.set_null(1);
        }
        datachunk.set_len(2);
        unsafe { datachunk.assume_initialized() };
        let vector = datachunk.initialized_vector(0, 2).unwrap();

        datachunk.set_len(1);

        assert_eq!(vector.readable_len().unwrap(), 1);
    }

    #[test]
    #[cfg(feature = "vtab")]
    fn temporary_callback_chunk_preserves_existing_transport() {
        let mut owner = DataChunkHandle::new(&[LogicalTypeHandle::from(LogicalTypeId::Bigint)]);
        let mut vector = owner.flat_vector(0);
        for row in 0..3 {
            vector.set_null(row);
        }
        drop(vector);
        owner.set_len(3);
        unsafe { owner.assume_initialized() };

        let input = unsafe { DataChunkHandle::new_unowned(owner.ptr) };

        assert!(input.capacity() >= 3);
        assert_eq!(input.flat_vector(0).capacity(), input.capacity());
        assert!(input.initialized_vector(0, 4).is_err());
    }

    #[test]
    fn test_logi() {
        let key = LogicalTypeHandle::from(LogicalTypeId::Varchar);

        let value = LogicalTypeHandle::from(LogicalTypeId::UTinyint);

        let map = LogicalTypeHandle::map(&key, &value);

        assert_eq!(map.id(), LogicalTypeId::Map);

        // let union_ = LogicalType::new_union_type(HashMap::from([
        //     ("number", LogicalType::new(LogicalTypeId::Bigint)),
        //     ("string", LogicalType::new(LogicalTypeId::Varchar)),
        // ]));
        // assert_eq!(union_.type_id(), LogicalTypeId::Union);

        // let struct_ = LogicalType::new_struct_type(HashMap::from([
        //     ("number", LogicalType::new(LogicalTypeId::Bigint)),
        //     ("string", LogicalType::new(LogicalTypeId::Varchar)),
        // ]));
        // assert_eq!(struct_.type_id(), LogicalTypeId::Struct);
    }
}

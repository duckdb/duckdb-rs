use super::{
    logical_type::LogicalTypeHandle,
    vector::{ArrayVector, FlatVector, ListVector, StructVector, VectorRef, VectorState, VectorStateCell},
};
use crate::{
    Result,
    error::duckdb_failure_from_message,
    ffi::{
        duckdb_create_data_chunk, duckdb_data_chunk, duckdb_data_chunk_get_column_count, duckdb_data_chunk_get_size,
        duckdb_data_chunk_get_vector, duckdb_data_chunk_set_size, duckdb_destroy_data_chunk,
    },
};

/// Handle to a DuckDB data chunk.
pub struct DataChunkHandle {
    ptr: duckdb_data_chunk,
    owned: bool,
    capacity: usize,
    state: VectorStateCell,
}

impl Drop for DataChunkHandle {
    fn drop(&mut self) {
        if self.owned && !self.ptr.is_null() {
            unsafe { duckdb_destroy_data_chunk(&mut self.ptr) }
            self.ptr = std::ptr::null_mut();
        }
    }
}

impl DataChunkHandle {
    /// Wrap a callback-owned chunk using the existing callback transport.
    ///
    /// This constructor is intentionally temporary. Scalar input and table
    /// output do not yet carry distinct native trust and construction roles;
    /// the stacked callback-adapter branch will replace it with role-specific
    /// constructors.
    ///
    /// # Safety
    ///
    /// `ptr` must remain live and unmutated by unrelated code for the handle's
    /// lifetime. Its current cardinality must describe initialized callback
    /// input when this handle is read.
    #[cfg(feature = "vtab")]
    pub(crate) unsafe fn new_unowned(ptr: duckdb_data_chunk) -> Self {
        let readable_len = unsafe { duckdb_data_chunk_get_size(ptr) as usize };
        Self {
            ptr,
            owned: false,
            capacity: standard_vector_capacity(),
            state: VectorStateCell::new(VectorState::Initialized { readable_len }),
        }
    }

    /// Create a data chunk whose payload starts under construction.
    ///
    /// Native Arrow writers finalize the chunk automatically. Manual writers
    /// must call [`Self::assume_initialized`] after initializing every
    /// non-null payload in all committed nested spans.
    pub fn new(logical_types: &[LogicalTypeHandle]) -> Self {
        let num_columns = logical_types.len();
        let mut c_types = Vec::with_capacity(num_columns);
        c_types.extend(logical_types.iter().map(|ty| ty.ptr));
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
        }
    }

    /// Return a scalar vector compatibility view.
    ///
    /// # Panics
    ///
    /// Panics if `idx` is out of range or the column is nested.
    pub fn flat_vector(&self, idx: usize) -> FlatVector<'_> {
        self.check_column_index(idx).unwrap_or_else(|err| panic!("{err}"));
        let ptr = unsafe { duckdb_data_chunk_get_vector(self.ptr, idx as u64) };
        unsafe { FlatVector::from_chunk(ptr, self.capacity, &self.state) }
            .unwrap_or_else(|err| panic!("column {idx}: {err}"))
    }

    /// Return a list or map vector compatibility view.
    pub fn list_vector(&self, idx: usize) -> ListVector<'_> {
        self.check_column_index(idx).unwrap_or_else(|err| panic!("{err}"));
        unsafe { ListVector::from_raw_with_capacity(duckdb_data_chunk_get_vector(self.ptr, idx as u64), self.capacity) }
    }

    /// Return an array vector compatibility view.
    pub fn array_vector(&self, idx: usize) -> ArrayVector<'_> {
        self.check_column_index(idx).unwrap_or_else(|err| panic!("{err}"));
        unsafe {
            ArrayVector::from_raw_with_capacity(duckdb_data_chunk_get_vector(self.ptr, idx as u64), self.capacity)
        }
    }

    /// Return a struct-physical vector compatibility view.
    pub fn struct_vector(&self, idx: usize) -> StructVector<'_> {
        self.check_column_index(idx).unwrap_or_else(|err| panic!("{err}"));
        unsafe {
            StructVector::from_raw_with_capacity(duckdb_data_chunk_get_vector(self.ptr, idx as u64), self.capacity)
        }
    }

    /// Set the number of committed top-level rows.
    ///
    /// Growing an initialized chunk revokes native reads until initialization
    /// is re-established. Shrinking preserves initialization for the retained
    /// prefix.
    pub fn set_len(&self, new_len: usize) {
        self.try_set_len(new_len).unwrap_or_else(|err| panic!("{err}"));
    }

    /// Try to set the number of committed top-level rows.
    pub fn try_set_len(&self, new_len: usize) -> Result<()> {
        if new_len > self.capacity {
            return Err(duckdb_failure_from_message(format!(
                "data chunk length {new_len} exceeds capacity {}",
                self.capacity
            )));
        }
        let old_len = self.len();
        unsafe { duckdb_data_chunk_set_size(self.ptr, new_len as u64) };
        match self.state.get() {
            VectorState::Initialized { readable_len } if new_len <= old_len => {
                self.state.set(VectorState::Initialized {
                    readable_len: readable_len.min(new_len),
                });
            }
            VectorState::Initialized { .. } => {
                self.state.set(VectorState::UnderConstruction);
            }
            VectorState::CallbackInput { .. } | VectorState::UnderConstruction => {}
        }
        Ok(())
    }

    /// Return the committed top-level row count.
    pub fn len(&self) -> usize {
        unsafe { duckdb_data_chunk_get_size(self.ptr) as usize }
    }

    /// Return whether the chunk contains no committed rows.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Return the number of top-level columns.
    pub fn num_columns(&self) -> usize {
        unsafe { duckdb_data_chunk_get_column_count(self.ptr) as usize }
    }

    /// Return the effective capacity of each top-level vector.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Declare the complete committed payload initialized.
    ///
    /// # Safety
    ///
    /// Every non-null payload in `0..self.len()` and every non-null payload in
    /// the full committed spans of nested child vectors must be initialized.
    /// No writable vector view of this chunk may remain live.
    pub unsafe fn assume_initialized(&mut self) {
        unsafe { self.mark_initialized() }.unwrap_or_else(|err| panic!("{err}"));
    }

    pub(crate) unsafe fn mark_initialized(&mut self) -> Result<()> {
        self.state.set(VectorState::Initialized {
            readable_len: self.len(),
        });
        Ok(())
    }

    /// Return the underlying DuckDB data-chunk pointer.
    pub fn get_ptr(&self) -> duckdb_data_chunk {
        self.ptr
    }

    pub(crate) fn initialized_vector(&self, idx: usize, len: usize) -> Result<VectorRef<'_>> {
        self.check_column_index(idx)?;
        let readable_len = self.state.readable_len_or_err()?;
        if len > readable_len {
            return Err(duckdb_failure_from_message(format!(
                "requested initialized vector length {len} exceeds data chunk length {readable_len}"
            )));
        }
        if len > self.capacity {
            return Err(duckdb_failure_from_message(format!(
                "top-level vector capacity {len} exceeds data chunk capacity {}",
                self.capacity
            )));
        }
        let ptr = unsafe { duckdb_data_chunk_get_vector(self.ptr, idx as u64) };
        unsafe { VectorRef::initialized_from_chunk(ptr, len, &self.state) }
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
}

fn standard_vector_capacity() -> usize {
    unsafe { crate::ffi::duckdb_vector_size() as usize }
}

#[cfg(test)]
mod test {
    use super::{super::logical_type::LogicalTypeId, *};

    #[test]
    fn test_data_chunk_construction() {
        let chunk = DataChunkHandle::new(&[LogicalTypeHandle::from(LogicalTypeId::Integer)]);
        assert_eq!(chunk.num_columns(), 1);
    }

    #[test]
    fn test_vector() {
        let datachunk = DataChunkHandle::new(&[LogicalTypeHandle::from(LogicalTypeId::Bigint)]);
        let mut vector = datachunk.flat_vector(0);
        let data = unsafe { vector.as_mut_slice::<i64>() };
        data[0] = 42;
    }

    #[test]
    fn test_logi() {
        let key = LogicalTypeHandle::from(LogicalTypeId::Varchar);
        let value = LogicalTypeHandle::from(LogicalTypeId::UTinyint);
        let map = LogicalTypeHandle::map(&key, &value);
        assert_eq!(map.id(), LogicalTypeId::Map);
    }
}

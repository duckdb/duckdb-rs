use std::sync::Arc;

use libduckdb_sys::{duckdb_vector_get_column_type, duckdb_vector_size};

use super::{column_info::ColumnInfo, LogicalTypeHandle};
use crate::{
    core::{LogicalType, Vector, VectorHandle},
    ffi::{
        duckdb_create_data_chunk, duckdb_data_chunk, duckdb_data_chunk_get_column_count, duckdb_data_chunk_get_size,
        duckdb_data_chunk_get_vector, duckdb_data_chunk_set_size, duckdb_destroy_data_chunk,
    },
};

/// Handle to the DataChunk in DuckDB.
pub struct DataChunkHandle {
    /// Pointer to the DataChunk in duckdb C API.
    pub(crate) ptr: duckdb_data_chunk,

    /// Whether this [DataChunkHandle] own the [DataChunk::ptr].
    owned: bool,
}

impl Drop for DataChunkHandle {
    fn drop(&mut self) {
        if self.owned && !self.ptr.is_null() {
            unsafe { duckdb_destroy_data_chunk(&mut self.ptr) }
        }
        self.ptr = std::ptr::null_mut();
    }
}

impl DataChunkHandle {
    pub(crate) unsafe fn new_unowned(ptr: duckdb_data_chunk) -> Self {
        Self { ptr, owned: false }
    }

    /// Create a new [DataChunkHandle] with the given [LogicalTypeHandle]s.
    pub fn new(logical_types: &[LogicalType]) -> Self {
        let num_columns = logical_types.len();
        let mut c_types = logical_types.iter().map(|t| unsafe { t.ptr() }).collect::<Vec<_>>();
        let ptr = unsafe { duckdb_create_data_chunk(c_types.as_mut_ptr(), num_columns as u64) };
        Self { ptr, owned: true }
    }

    /// Get the vector at the specific column index: `idx`.
    pub fn get_vector<T>(&self, idx: usize) -> VectorHandle<T> {
        unsafe { VectorHandle::new_unchecked(duckdb_data_chunk_get_vector(self.ptr, idx as u64)) }
    }

    /// Get all the [LogicalTypes] in this [DataChunkHandle].
    pub fn get_logical_types(&self) -> Vec<LogicalType> {
        let num_columns = self.num_columns();
        (0..num_columns)
            .map(|idx| unsafe {
                let vector = duckdb_data_chunk_get_vector(self.ptr, idx as u64);
                let handle = LogicalTypeHandle::new(duckdb_vector_get_column_type(vector));
                LogicalType::from(handle)
            })
            .collect()
    }

    /// Get the maximum capacity of the data chunk.
    /// Data should be capped at this size.
    /// If the data exceeds this size, it should be split into multiple data chunks.
    pub fn max_capacity() -> usize {
        unsafe { duckdb_vector_size() as usize }
    }

    /// Set the size of the data chunk
    pub fn set_len(&self, new_len: usize) {
        unsafe { duckdb_data_chunk_set_size(self.ptr, new_len as u64) };
    }

    pub fn reserve(&self, size: usize) {
        if size > self.len() {
            self.set_len(size.next_power_of_two());
        }
    }

    pub fn with_capacity(self, size: usize) -> Self {
        self.reserve(size);
        self
    }

    /// Get the length / the number of rows in this [DataChunkHandle].
    pub fn len(&self) -> usize {
        unsafe { duckdb_data_chunk_get_size(self.ptr) as usize }
    }

    pub fn num_columns(&self) -> usize {
        unsafe { duckdb_data_chunk_get_column_count(self.ptr) as usize }
    }

    /// Check whether this [DataChunkHandle] is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

pub struct DataChunk {
    handle: Arc<DataChunkHandle>,
    pub columns: Vec<Vector>,
}

impl DataChunk {
    // Create a new [DataChunk] construction from logical types and columns infos
    pub fn new(capacity: usize, logical_types: &[LogicalType], columns: &[ColumnInfo]) -> Self {
        let handle = Arc::new(DataChunkHandle::new(logical_types).with_capacity(capacity));
        let columns = Vector::new_from_column_infos(columns, handle.clone());
        Self { handle, columns }
    }

    /// Create a new [DataChunk] from an existing [DataChunkHandle].
    pub fn from_existing_handle(handle: DataChunkHandle) -> Self {
        let handle = Arc::new(handle);
        let logical_types = handle.get_logical_types();
        let infos: Vec<ColumnInfo> = logical_types.iter().map(ColumnInfo::new).collect();
        let columns = Vector::new_from_column_infos(&infos, handle.clone());
        Self { handle, columns }
    }

    /// Create a new [DataChunk] from an arrow::RecordBatch.
    pub fn from_arrow_schema(schema: &arrow::datatypes::SchemaRef, handle: DataChunkHandle) -> Self {
        let handle = Arc::new(handle);
        let columns = Vector::from_arrow_schema(schema, handle.clone());
        Self { handle, columns }
    }

    /// Get the ptr of duckdb_data_chunk in this [DataChunk].
    pub fn get_handle(&self) -> Arc<DataChunkHandle> {
        self.handle.clone()
    }

    /// Get the number of columns in this [DataChunk].
    pub fn num_columns(&self) -> usize {
        self.columns.len()
    }

    pub fn reserve(&mut self, capacity: usize) {
        self.handle.reserve(capacity);
    }

    pub fn set_len(&mut self, size: usize) {
        self.handle.set_len(size);
    }
}

#[cfg(test)]
mod test {
    use super::{super::logical_type::LogicalTypeId, *};
    use crate::core::{FlatVector, LogicalTypeHandle};

    #[test]
    fn test_data_chunk_construction() {
        let dc = DataChunkHandle::new(&[LogicalTypeHandle::from(LogicalTypeId::Integer).into()]);

        assert_eq!(dc.num_columns(), 1);

        drop(dc);
    }

    #[test]
    fn test_vector() {
        let datachunk = DataChunkHandle::new(&[LogicalTypeHandle::from(LogicalTypeId::Bigint).into()]);
        datachunk.reserve(1);
        let mut vector = FlatVector::new(datachunk.get_vector(0), Arc::new(move || datachunk.len()));
        let data = vector.as_mut_slice::<i64>();

        data[0] = 42;
    }
}

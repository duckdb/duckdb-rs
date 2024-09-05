use super::{
    logical_type::LogicalTypeHandle,
    vector::{ArrayVector, FlatVector, ListVector, StructVector},
};
use crate::ffi::{
    duckdb_create_data_chunk, duckdb_data_chunk, duckdb_data_chunk_get_column_count, duckdb_data_chunk_get_size,
    duckdb_data_chunk_get_vector, duckdb_data_chunk_set_size, duckdb_destroy_data_chunk,
};

/// Handle to the DataChunk in DuckDB.
pub struct DataChunkHandle {
    /// Pointer to the DataChunk in duckdb C API.
    ptr: duckdb_data_chunk,

    /// Whether this [DataChunkHandle] own the [DataChunk::ptr].
    owned: bool,
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
    #[allow(dead_code)]
    pub(crate) unsafe fn new_unowned(ptr: duckdb_data_chunk) -> Self {
        Self { ptr, owned: false }
    }

    /// Create a new [DataChunkHandle] with the given [LogicalTypeHandle]s.
    pub fn new(logical_types: &[LogicalTypeHandle]) -> Self {
        let num_columns = logical_types.len();
        let mut c_types = logical_types.iter().map(|t| t.ptr).collect::<Vec<_>>();
        let ptr = unsafe { duckdb_create_data_chunk(c_types.as_mut_ptr(), num_columns as u64) };
        DataChunkHandle { ptr, owned: true }
    }

    /// Get the vector at the specific column index: `idx`.
    pub fn flat_vector(&self, idx: usize) -> FlatVector {
        FlatVector::from(unsafe { duckdb_data_chunk_get_vector(self.ptr, idx as u64) })
    }

    /// Get a list vector from the column index.
    pub fn list_vector(&self, idx: usize) -> ListVector {
        ListVector::from(unsafe { duckdb_data_chunk_get_vector(self.ptr, idx as u64) })
    }

    /// Get a array vector from the column index.
    pub fn array_vector(&self, idx: usize) -> ArrayVector {
        ArrayVector::from(unsafe { duckdb_data_chunk_get_vector(self.ptr, idx as u64) })
    }

    /// Get struct vector at the column index: `idx`.
    pub fn struct_vector(&self, idx: usize) -> StructVector {
        StructVector::from(unsafe { duckdb_data_chunk_get_vector(self.ptr, idx as u64) })
    }

    /// Set the size of the data chunk
    pub fn set_len(&self, new_len: usize) {
        unsafe { duckdb_data_chunk_set_size(self.ptr, new_len as u64) };
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

    /// Get the ptr of duckdb_data_chunk in this [DataChunkHandle].
    pub fn get_ptr(&self) -> duckdb_data_chunk {
        self.ptr
    }
}

#[cfg(test)]
mod test {
    use super::{super::logical_type::LogicalTypeId, *};

    #[test]
    fn test_data_chunk_construction() {
        let dc = DataChunkHandle::new(&[LogicalTypeHandle::from(LogicalTypeId::Integer)]);

        assert_eq!(dc.num_columns(), 1);

        drop(dc);
    }

    #[test]
    fn test_vector() {
        let datachunk = DataChunkHandle::new(&[LogicalTypeHandle::from(LogicalTypeId::Bigint)]);
        let mut vector = datachunk.flat_vector(0);
        let data = vector.as_mut_slice::<i64>();

        data[0] = 42;
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

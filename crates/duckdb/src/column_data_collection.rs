//! Buffer-backed collections of columnar data chunks.
//!
//! A [`ColumnDataCollection`] has a fixed schema and can be moved into an
//! appending or scanning state. Appended chunks are copied into storage owned
//! by the collection.

use std::ops::Deref;

use libduckdb_sys as ffi;

use crate::{
    Result, builder_helpers::context_and_connection_fn, check_api_call, check_api_call_no_err,
    data_chunk::DataChunk, logical_type::LogicalType,
};

struct WorkerScanState(ffi::duckdb_v2_column_data_collection_worker_scan_state_handle);
struct SharedScanState(ffi::duckdb_v2_column_data_collection_shared_scan_state_handle);

struct AppenderHandle(ffi::duckdb_v2_column_data_collection_append_state_handle);

impl Deref for AppenderHandle {
    type Target = ffi::duckdb_v2_column_data_collection_append_state_handle;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Drop for AppenderHandle {
    fn drop(&mut self) {
        check_api_call_no_err!(
            ffi::duckdb_v2_column_data_collection_append_state_destroy,
            &mut self.0
        )
        .unwrap();
    }
}

impl Deref for WorkerScanState {
    type Target = ffi::duckdb_v2_column_data_collection_worker_scan_state_handle;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Drop for WorkerScanState {
    fn drop(&mut self) {
        check_api_call_no_err!(
            ffi::duckdb_v2_column_data_collection_worker_scan_state_destroy,
            &mut self.0
        )
        .unwrap();
    }
}

impl Deref for SharedScanState {
    type Target = ffi::duckdb_v2_column_data_collection_shared_scan_state_handle;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Drop for SharedScanState {
    fn drop(&mut self) {
        check_api_call_no_err!(
            ffi::duckdb_v2_column_data_collection_shared_scan_state_destroy,
            &mut self.0
        )
        .unwrap();
    }
}

/// An owned collection of data chunks with a fixed column schema.
///
/// Move the collection into [`ColumnDataCollectionAppender`] to add rows or
/// [`ColumnDataCollectionScan`] to iterate over its chunks. Each transition
/// consumes the previous state, and the collection can be recovered afterward.
///
/// # Example
/// ```
/// use duckdb_rs::{DuckDBType, Environment, StorageLocation};
/// use duckdb_rs::column_data_collection::ColumnDataCollection;
/// use duckdb_rs::data_chunk::DataChunk;
///
/// # fn main() -> duckdb_rs::Result<()> {
/// let env = Environment::new()?;
/// let db = env.open(StorageLocation::InMemory)?;
/// let conn = db.connect()?;
/// let types = [i32::logical_type(&conn)?];
///
/// let collection = ColumnDataCollection::from_connection(&conn, &types)?;
/// let chunk = DataChunk::create(&types, true)?;
/// let mut values = chunk.get_vector_at::<i32>(0)?;
/// values.set_size(2)?;
/// values.write(0, Some(10))?;
/// values.write(1, Some(20))?;
///
/// let appender = collection.to_append()?;
/// appender.append(&chunk)?;
/// assert_eq!(appender.len()?, 2);
///
/// let mut scan = appender.to_scan()?;
/// let chunk = scan.next().transpose()?.expect("expected a chunk");
/// let values = chunk.get_vector_at::<i32>(0)?;
/// assert_eq!(values.get(0)?, Some(&10));
/// assert_eq!(values.get(1)?, Some(&20));
///
/// let collection = scan.to_normal();
/// assert_eq!(collection.len()?, 2);
/// # Ok(())
/// # }
/// ```
pub struct ColumnDataCollection {
    /// The owned DuckDB collection handle.
    pub handle: ffi::duckdb_v2_column_data_collection_handle,
    /// The collection's column types, in storage order.
    pub logical_types: Vec<LogicalType>,
}

impl ColumnDataCollection {
    context_and_connection_fn! {
        /// Create an empty collection using a connection or callback context's allocator.
        pub fn from_[context, connection](
            logical_types: impl Into<Vec<LogicalType>>,
        ) -> Result<Self>
        {
            context_fn: ffi::duckdb_v2_column_data_collection_create_with_context,
            connection_fn: ffi::duckdb_v2_column_data_collection_create_with_connection,
        }
        let logical_types = logical_types.into();

        let handle: ffi::duckdb_v2_column_data_collection_handle = check_api_call!(
            api_fn!(),
            **api_arg!(),
            logical_types
                .iter()
                .map(|lt| lt.handle)
                .collect::<Vec<ffi::duckdb_v2_logical_type_handle>>()
                .as_ptr(),
            logical_types.len() as u64,
            RET
        )?;

        Ok(ColumnDataCollection {
            handle,
            logical_types,
        })
    }

    /// Return whether the collection contains no rows.
    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.len()? == 0)
    }

    /// Return the total number of rows across all stored chunks.
    pub fn len(&self) -> Result<usize> {
        let count: u64 = check_api_call!(
            ffi::duckdb_v2_column_data_collection_row_count,
            self.handle,
            RET
        )?;

        Ok(count as usize)
    }

    /// Consume the collection and initialize it for appending.
    pub fn to_append(self) -> Result<ColumnDataCollectionAppender> {
        ColumnDataCollectionAppender::new(self)
    }

    /// Consume the collection and initialize an iterator over its chunks.
    pub fn to_scan(self) -> Result<ColumnDataCollectionScan> {
        ColumnDataCollectionScan::new(self)
    }
}

impl Drop for ColumnDataCollection {
    fn drop(&mut self) {
        check_api_call_no_err!(
            ffi::duckdb_v2_column_data_collection_destroy,
            &mut self.handle
        )
        .unwrap();
    }
}

/// An iterator over the chunks stored in a [`ColumnDataCollection`].
///
/// The scan owns the collection and its progress state. It can be consumed to
/// recover the collection or switch directly to appending.
pub struct ColumnDataCollectionScan {
    /// The collection being scanned.
    pub collection: ColumnDataCollection,
    worker_scan_state: WorkerScanState,
    shared_scan_state: SharedScanState,
}

impl ColumnDataCollectionScan {
    fn new(collection: ColumnDataCollection) -> Result<Self> {
        let worker_scan_state = WorkerScanState(check_api_call!(
            ffi::duckdb_v2_column_data_collection_worker_scan_state_create,
            collection.handle,
            RET
        )?);
        let shared_scan_state = SharedScanState(check_api_call!(
            ffi::duckdb_v2_column_data_collection_shared_scan_state_create,
            collection.handle,
            RET
        )?);

        Ok(ColumnDataCollectionScan {
            collection,
            worker_scan_state,
            shared_scan_state,
        })
    }

    /// Stop scanning and return the underlying collection.
    pub fn to_normal(self) -> ColumnDataCollection {
        self.collection
    }

    /// Stop scanning and initialize the collection for appending.
    pub fn to_append(self) -> Result<ColumnDataCollectionAppender> {
        ColumnDataCollectionAppender::new(self.collection)
    }
}

impl Iterator for ColumnDataCollectionScan {
    type Item = Result<DataChunk>;

    fn next(&mut self) -> Option<Self::Item> {
        let data_chunk = DataChunk::create(&self.collection.logical_types, false).unwrap();

        let result: Result<bool> = check_api_call!(
            ffi::duckdb_v2_column_data_collection_parallel_scan,
            self.collection.handle,
            *self.shared_scan_state,
            *self.worker_scan_state,
            data_chunk.handle,
            RET
        );

        match result {
            Ok(did_produce_chunk) => {
                if !did_produce_chunk {
                    None
                } else {
                    Some(Ok(data_chunk))
                }
            }
            Err(e) => Some(Err(e)),
        }
    }
}

/// An owned append state for a [`ColumnDataCollection`].
///
/// Chunks appended through this state must exactly match the collection's
/// column count and types. The state can be consumed to recover, scan, or
/// reset the collection.
pub struct ColumnDataCollectionAppender {
    collection: ColumnDataCollection,
    appender: AppenderHandle,
}

impl ColumnDataCollectionAppender {
    fn new(collection: ColumnDataCollection) -> Result<Self> {
        let appender = AppenderHandle(check_api_call!(
            ffi::duckdb_v2_column_data_collection_append_state_create,
            collection.handle,
            RET
        )?);

        Ok(Self {
            collection,
            appender,
        })
    }

    /// Append a copy of `chunk` to the collection.
    ///
    /// A mismatched column count or type returns an error without copying data.
    pub fn append(&self, chunk: &DataChunk) -> Result<()> {
        check_api_call!(
            ffi::duckdb_v2_column_data_collection_append,
            self.collection.handle,
            *self.appender,
            chunk.handle
        )?;

        Ok(())
    }

    /// Move all chunks from `other` into this collection.
    ///
    /// The source collection is consumed.
    pub fn combine(&mut self, mut other: ColumnDataCollection) -> Result<()> {
        check_api_call!(
            ffi::duckdb_v2_column_data_collection_combine,
            self.collection.handle,
            &mut other.handle
        )?;

        self.appender = AppenderHandle(check_api_call!(
            ffi::duckdb_v2_column_data_collection_append_state_create,
            self.collection.handle,
            RET
        )?);

        Ok(())
    }

    /// Remove all rows and return the collection with its schema unchanged.
    pub fn reset(self) -> Result<ColumnDataCollection> {
        check_api_call!(
            ffi::duckdb_v2_column_data_collection_reset,
            self.collection.handle,
        )?;

        Ok(self.collection)
    }

    /// Finish appending and return the underlying collection.
    pub fn to_normal(self) -> ColumnDataCollection {
        self.collection
    }

    /// Finish appending and initialize an iterator over the stored chunks.
    pub fn to_scan(self) -> Result<ColumnDataCollectionScan> {
        ColumnDataCollectionScan::new(self.collection)
    }

    /// Return the total number of rows across all stored chunks.
    pub fn len(&self) -> Result<usize> {
        self.collection.len()
    }

    /// Return whether the collection contains no rows.
    pub fn is_empty(&self) -> Result<bool> {
        self.collection.is_empty()
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod test {
    use crate::{DuckDBType, Environment, Parameters, StorageLocation};

    use super::*;

    #[test]
    fn test_collection_add() -> crate::Result<()> {
        let env = Environment::new()?;
        let db = env.open(StorageLocation::InMemory)?;
        let conn = db.connect()?;

        let mut statements = conn.parse(
            r#"
         CREATE TABLE employees (
             id           INTEGER PRIMARY KEY,
            is_active    BOOLEAN
         );

         INSERT INTO employees FROM buf;

         SELECT * FROM employees;
        "#,
        )?;

        let statement = statements.next().unwrap()?;
        conn.execute(statement, Parameters::None)?;

        let logical_types = [i32::logical_type(&conn)?, bool::logical_type(&conn)?];

        let collection = ColumnDataCollection::from_connection(&conn, &logical_types)?;

        let chunk = DataChunk::create(&logical_types, true)?;

        let mut id = chunk.get_vector_at::<i32>(0)?;
        let mut is_active = chunk.get_vector_at::<bool>(1)?;

        id.set_size(2)?;
        is_active.set_size(2)?;

        id.write(0, Some(10))?;
        id.write(1, None)?;

        let collection = collection.to_append()?;

        collection.append(&chunk)?;

        assert_eq!(collection.len()?, 2);

        let mut collection = collection.reset()?.to_append()?;

        assert_eq!(collection.len()?, 0);

        id.write(0, Some(10))?;
        id.write(1, Some(12))?;
        is_active.write(0, Some(false))?;
        is_active.write(1, None)?;

        collection.append(&chunk)?;

        assert_eq!(collection.len()?, 2);

        let chunk_2 = DataChunk::create(&logical_types, true)?;
        let mut id = chunk_2.get_vector_at::<i32>(0)?;
        let mut is_active = chunk_2.get_vector_at::<bool>(1)?;

        id.set_size(1)?;
        is_active.set_size(1)?;

        id.write(0, Some(14))?;
        is_active.write(0, Some(true))?;

        let collection_2 =
            ColumnDataCollection::from_connection(&conn, &logical_types)?.to_append()?;
        collection_2.append(&chunk_2)?;

        collection.combine(collection_2.to_normal())?;

        let statement = statements.next().unwrap()?;
        let statement = statement.add_collection(
            "buf",
            &collection.collection,
            Some(&["id".into(), "is_active".into()]),
        )?;

        let rows_changed = conn.execute(statement, Parameters::None)?;
        assert_eq!(rows_changed, 3);

        let statement = statements.next().unwrap()?;
        let result = conn.query(statement, Parameters::None)?;

        if let Some(chunk) = result.into_iter().next() {
            let chunk = chunk?;

            let id = chunk.get_vector_at::<i32>(0)?;
            let is_active = chunk.get_vector_at::<bool>(1)?;

            assert_eq!(id.get(0)?, Some(&10));
            assert_eq!(is_active.get(0)?, Some(&false));

            assert_eq!(id.get(1)?, Some(&12));
            assert_eq!(is_active.get(1)?, None);

            assert_eq!(id.get(2)?, Some(&14));
            assert_eq!(is_active.get(2)?, Some(&true));
        } else {
            assert!(false, "Expected a result chunk, but got none");
        }

        Ok(())
    }

    #[test]
    fn test_collection_scan() -> crate::Result<()> {
        let env = Environment::new()?;
        let db = env.open(StorageLocation::InMemory)?;
        let conn = db.connect()?;

        let logical_types = [i32::logical_type(&conn)?, bool::logical_type(&conn)?];

        let collection = ColumnDataCollection::from_connection(&conn, &logical_types)?;

        let chunk = DataChunk::create(&logical_types, true)?;

        let mut id = chunk.get_vector_at::<i32>(0)?;
        let mut is_active = chunk.get_vector_at::<bool>(1)?;

        id.set_size(2)?;
        is_active.set_size(2)?;

        id.write(0, Some(10))?;
        id.write(1, None)?;

        is_active.write(0, Some(false))?;
        is_active.write(1, None)?;

        let collection = collection.to_append()?;

        collection.append(&chunk)?;

        let scan = collection.to_scan()?;

        for result in scan {
            let chunk = result?;

            let id = chunk.get_vector_at::<i32>(0)?;
            let is_active = chunk.get_vector_at::<bool>(1)?;

            assert_eq!(id.get(0)?, Some(&10));
            assert_eq!(is_active.get(0)?, Some(&false));

            assert_eq!(id.get(1)?, None);
            assert_eq!(is_active.get(1)?, None);
        }

        Ok(())
    }
}

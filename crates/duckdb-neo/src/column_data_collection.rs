//! Buffer-backed collections of columnar data chunks.
//!
//! A [`ColumnDataCollection`] has a fixed schema and can be moved into an
//! appending or scanning state. Appended chunks are copied into storage owned
//! by the collection.

use std::ops::Deref;

use crate::ffi;

use crate::{
    Result, check_api_call, check_api_call_no_err,
    data_chunk::{DataChunk, DataChunkRef},
    handles::{
        ColumnDataCollectionAppendLink, ColumnDataCollectionAppendStateHandle, ColumnDataCollectionSharedScanLink,
        ColumnDataCollectionSharedScanStateHandle, ColumnDataCollectionWorkerScanLink,
        ColumnDataCollectionWorkerScanStateHandle,
    },
    links::{ColumnDataCollectionLink, DatabaseKeepAlive, KeepAlive},
    logical_type::LogicalType,
};

/// An owned collection of data chunks with a fixed column schema.
///
/// Move the collection into [`ColumnDataCollectionAppender`] to add rows or
/// [`ColumnDataCollectionScan`] to iterate over its chunks. Each transition
/// consumes the previous state, and the collection can be recovered afterward.
///
/// # Example
/// ```
/// use duckdb_neo::{DuckDBType, environment::{Environment, StorageLocation}};
/// use duckdb_neo::column_data_collection::ColumnDataCollection;
/// use duckdb_neo::data_chunk::DataChunk;
///
/// # fn main() -> duckdb_neo::Result<()> {
/// let env = Environment::new()?;
/// let db = env.open(StorageLocation::InMemory)?;
/// let conn = db.connect()?;
/// let types = [i32::logical_type(&conn)?];
///
/// let collection = ColumnDataCollection::new(&conn, &types)?;
/// let chunk = DataChunk::create(&types, true)?;
/// let mut values = chunk.get_vector_at::<i32>(0)?;
/// values.set_size(2)?;
/// values.write(0, Some(10))?;
/// values.write(1, Some(20))?;
///
/// let mut appender = collection.to_append()?;
/// appender.append(&chunk)?;
/// assert_eq!(appender.len()?, 2);
///
/// let mut scan = appender.to_scan()?;
/// let chunk = scan.next_chunk()?.expect("expected a chunk");
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
    /// Dropped after the handle is destroyed.
    pub(crate) database: Option<KeepAlive>,
}

impl Deref for ColumnDataCollection {
    type Target = ffi::duckdb_v2_column_data_collection_handle;
    fn deref(&self) -> &Self::Target {
        &self.handle
    }
}

impl ColumnDataCollection {
    /// Create an empty collection using a [`Connection`](crate::connection::Connection)
    /// or callback [`Context`](crate::connection::Context)'s buffer manager.
    #[allow(private_bounds)]
    pub fn new<C: ColumnDataCollectionLink + DatabaseKeepAlive>(
        link: &C,
        logical_types: impl Into<Vec<LogicalType>>,
    ) -> Result<Self> {
        let logical_types = logical_types.into();
        let types = logical_types.iter().map(|lt| lt.handle).collect::<Vec<_>>();
        let handle = link.create_column_data_collection(&types)?;

        Ok(ColumnDataCollection {
            handle,
            logical_types,
            database: link.keep_alive(),
        })
    }

    /// Return whether the collection contains no rows.
    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.len()? == 0)
    }

    /// Return the total number of rows across all stored chunks.
    pub fn len(&self) -> Result<usize> {
        let count: u64 = check_api_call!(ffi::duckdb_v2_column_data_collection_row_count, self.handle, RET)?;

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
        check_api_call_no_err!(ffi::duckdb_v2_column_data_collection_destroy, &mut self.handle).unwrap();
    }
}

// SAFETY: `&self` methods only read the row count; appending and scanning consume the
// collection. The keep-alive is `Send + Sync`.
unsafe impl Send for ColumnDataCollection {}
unsafe impl Sync for ColumnDataCollection {}

/// A scan over the chunks stored in a [`ColumnDataCollection`].
///
/// The scan owns the collection and its progress state. It can be consumed to
/// recover the collection or switch directly to appending.
///
/// Chunks are read with [`next_chunk`](Self::next_chunk) rather than through
/// [`Iterator`]: DuckDB does not copy the scanned data, so each chunk points
/// into buffers that stay valid only until the next scan call.
pub struct ColumnDataCollectionScan {
    worker_scan_state: ColumnDataCollectionWorkerScanStateHandle,
    shared_scan_state: ColumnDataCollectionSharedScanStateHandle,
    /// Reused for every scan call; DuckDB resets it before filling it.
    chunk: DataChunk<'static>,
    /// Declared last: the states above unpin buffers through the connection it keeps alive.
    collection: ColumnDataCollection,
}

impl ColumnDataCollectionScan {
    fn new(collection: ColumnDataCollection) -> Result<Self> {
        let worker_scan_state = collection.create_worker_scan_state()?;
        let shared_scan_state = collection.create_shared_scan_state()?;
        let chunk = DataChunk::create(&collection.logical_types, false)?;

        Ok(ColumnDataCollectionScan {
            collection,
            worker_scan_state,
            shared_scan_state,
            chunk,
        })
    }

    /// Return the next chunk, or `None` once every chunk has been scanned.
    ///
    /// The chunk points into buffers the scan keeps pinned, so it
    /// is only valid until the next call. Copy it with
    /// [`DataChunkRef::copy`] to keep its data longer.
    ///
    pub fn next_chunk(&mut self) -> Result<Option<DataChunkRef<'_>>> {
        let did_produce_chunk: bool = check_api_call!(
            ffi::duckdb_v2_column_data_collection_scan,
            self.collection.handle,
            *self.shared_scan_state,
            *self.worker_scan_state,
            **self.chunk,
            RET
        )?;

        Ok(did_produce_chunk.then(|| DataChunkRef::new(**self.chunk, false)))
    }

    /// Return the collection being scanned.
    pub fn collection(&self) -> &ColumnDataCollection {
        &self.collection
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

/// An owned append state for a [`ColumnDataCollection`].
///
/// Chunks appended through this state must exactly match the collection's
/// column count and types. The state can be consumed to recover, scan, or
/// reset the collection.
pub struct ColumnDataCollectionAppender {
    appender: ColumnDataCollectionAppendStateHandle,
    collection: ColumnDataCollection,
}

impl ColumnDataCollectionAppender {
    fn new(collection: ColumnDataCollection) -> Result<Self> {
        let appender = collection.create_append_state()?;

        Ok(Self { collection, appender })
    }

    /// Append a copy of `chunk` to the collection.
    ///
    /// A mismatched column count or type returns an error without copying data.
    pub fn append(&mut self, chunk: &DataChunk<'_>) -> Result<()> {
        check_api_call!(
            ffi::duckdb_v2_column_data_collection_append,
            self.collection.handle,
            *self.appender,
            ***chunk
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
        if self.collection.database.is_none() {
            self.collection.database = other.database.take();
        }

        self.appender = self.collection.create_append_state()?;

        Ok(())
    }

    /// Consume the appender and return an empty collection with its schema unchanged.
    pub fn reset(self) -> Result<ColumnDataCollection> {
        check_api_call!(ffi::duckdb_v2_column_data_collection_reset, self.collection.handle)?;
        Ok(self.collection)
    }

    /// Return an empty collection, retaining its schema and buffers.
    pub fn clear(self) -> Result<ColumnDataCollection> {
        check_api_call!(ffi::duckdb_v2_column_data_collection_clear, self.collection.handle)?;
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
    use crate::{
        DuckDBType, Parameters,
        builder_helpers::scalar_callback,
        environment::{Environment, StorageLocation},
        replacement_scan::{ReplacementScanBuilder, ReplacementScanCallbacks, ReplacementType},
        scalar::ScalarFunctionBuilder,
        signature::SignatureBuilder,
    };

    use super::*;

    #[test]
    fn test_collection_from_context() -> crate::Result<()> {
        scalar_callback!(CollectionIsEmpty, bool, |_input, output, context, _user_data| {
            let collection = ColumnDataCollection::new(context, [i32::logical_type(context)?])?;
            let mut output = output;
            output.set_size(1)?;
            output.write(0, Some(collection.is_empty()?))
        });

        let env = Environment::new()?;
        let db = env.open(StorageLocation::InMemory)?;
        let conn = db.connect()?;

        ScalarFunctionBuilder::new(
            "collection_is_empty",
            SignatureBuilder::new([], bool::logical_type(&conn)?),
            CollectionIsEmpty,
        )
        .register(&conn)?;

        let mut result = conn.query("SELECT collection_is_empty()", Parameters::None)?;
        let chunk = result.next().expect("expected a result chunk")?;
        assert_eq!(chunk.get_vector_at::<bool>(0)?.get(0)?, Some(&true));
        Ok(())
    }

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

         INSERT INTO employees FROM A;

         SELECT * FROM employees;
        "#,
        )?;

        let statement = statements.next().unwrap()?;
        conn.execute(statement, Parameters::None)?;

        let logical_types = [i32::logical_type(&conn)?, bool::logical_type(&conn)?];

        let collection = ColumnDataCollection::new(&conn, &logical_types)?;

        let chunk = DataChunk::create(&logical_types, true)?;

        let mut id = chunk.get_vector_at::<i32>(0)?;
        let mut is_active = chunk.get_vector_at::<bool>(1)?;

        id.set_size(2)?;
        is_active.set_size(2)?;

        id.write(0, Some(10))?;
        id.write(1, None)?;

        let mut collection = collection.to_append()?;

        collection.append(&chunk)?;

        assert_eq!(collection.len()?, 2);

        let collection = collection.reset()?;

        assert_eq!(collection.len()?, 0);

        id.write(0, Some(10))?;
        id.write(1, Some(12))?;
        is_active.write(0, Some(false))?;
        is_active.write(1, None)?;

        let mut collection = collection.to_append()?;

        collection.append(&chunk)?;

        assert_eq!(collection.len()?, 2);

        let chunk_2 = DataChunk::create(&logical_types, true)?;
        let mut id = chunk_2.get_vector_at::<i32>(0)?;
        let mut is_active = chunk_2.get_vector_at::<bool>(1)?;

        id.set_size(1)?;
        is_active.set_size(1)?;

        id.write(0, Some(14))?;
        is_active.write(0, Some(true))?;

        let mut collection_2 = ColumnDataCollection::new(&conn, &logical_types)?.to_append()?;
        collection_2.append(&chunk_2)?;

        collection.combine(collection_2.to_normal())?;

        struct A {
            cdc: ColumnDataCollection,
        }

        impl ReplacementScanCallbacks for A {
            fn scan<'a>(
                &'a self,
                _context: &crate::connection::Context,
                name: &crate::qualified_name::QualifiedName,
                handle: crate::replacement_scan::ReplacementHandle<'a>,
            ) -> Result<()> {
                if name.get_view()?.table == Some("A".into()) {
                    handle.set_reference(ReplacementType::NamedColumnDataCollection((
                        &self.cdc,
                        ["id".to_string(), "is_active".to_string()].into(),
                    )))?;
                }

                Ok(())
            }
        }

        ReplacementScanBuilder::new(A {
            cdc: collection.to_normal(),
        })
        .register(&conn)?;

        let statement = statements.next().unwrap()?;

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
            panic!("Expected a result chunk, but got none");
        }

        Ok(())
    }

    #[test]
    fn test_collection_scan() -> crate::Result<()> {
        let env = Environment::new()?;
        let db = env.open(StorageLocation::InMemory)?;
        let conn = db.connect()?;

        let logical_types = [i32::logical_type(&conn)?, bool::logical_type(&conn)?];

        let collection = ColumnDataCollection::new(&conn, &logical_types)?;

        let chunk = DataChunk::create(&logical_types, true)?;

        let mut id = chunk.get_vector_at::<i32>(0)?;
        let mut is_active = chunk.get_vector_at::<bool>(1)?;

        id.set_size(2)?;
        is_active.set_size(2)?;

        id.write(0, Some(10))?;
        id.write(1, None)?;

        is_active.write(0, Some(false))?;
        is_active.write(1, None)?;

        let mut collection = collection.to_append()?;

        collection.append(&chunk)?;

        let mut scan = collection.to_scan()?;

        while let Some(chunk) = scan.next_chunk()? {
            let id = chunk.get_vector_at::<i32>(0)?;
            let is_active = chunk.get_vector_at::<bool>(1)?;

            assert_eq!(id.get(0)?, Some(&10));
            assert_eq!(is_active.get(0)?, Some(&false));

            assert_eq!(id.get(1)?, None);
            assert_eq!(is_active.get(1)?, None);
        }

        Ok(())
    }

    #[test]
    fn test_connection_collection_keeps_database_alive() -> crate::Result<()> {
        let env = Environment::new()?;
        let db = env.open(StorageLocation::InMemory)?;
        let conn = db.connect()?;
        let types = [i32::logical_type(&conn)?];

        let chunk = DataChunk::create(&types, true)?;
        let mut values = chunk.get_vector_at::<i32>(0)?;
        values.set_size(1)?;
        values.write(0, Some(42))?;
        drop(values);

        let mut appender = ColumnDataCollection::new(&conn, types)?.to_append()?;
        appender.append(&chunk)?;
        let mut scan = appender.to_scan()?;

        drop(conn);
        drop(db);
        assert_eq!(env.get_database_count()?, 1);

        let scanned = scan.next_chunk()?.expect("expected a chunk");
        assert_eq!(scanned.get_vector_at::<i32>(0)?.get(0)?, Some(&42));
        drop(scan);
        assert_eq!(env.get_database_count()?, 0);

        Ok(())
    }
}

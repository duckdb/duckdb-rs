//! Buffer-backed collections of columnar data chunks.
//!
//! A [`ColumnDataCollection`] has a fixed schema and can be moved into an
//! appending state or borrowed for scanning. Appended chunks are copied into
//! storage owned by the collection.

use std::marker::PhantomData;

use crate::column_data_collection::ColumnParameter::{Named, Positional};
use crate::connection::HasFactory;
use crate::ffi;

use crate::{
    AsRaw, DropHandle, Result, check_api_call,
    data_chunk::{DataChunk, DataChunkMut, DataChunkRef},
    handles::{
        ColumnDataCollectionAppendLink, ColumnDataCollectionAppendStateHandle, ColumnDataCollectionSharedScanLink,
        ColumnDataCollectionSharedScanStateHandle, ColumnDataCollectionWorkerScanLink,
        ColumnDataCollectionWorkerScanStateHandle,
    },
    links::ConnectionOrigin,
    logical_type::LogicalType,
    raw::RawExt,
};

/// The columns of a [`ColumnDataCollection`], with or without names.
///
/// Usually created implicitly: `ColumnDataCollection::new` accepts anything
/// that converts into it, such as `[LogicalType; N]`, `&[LogicalType]` or
/// `Vec<LogicalType>` for positional columns, and the same containers of
/// `(LogicalType, name)` pairs for named ones.
#[derive(Debug, Clone)]
pub enum ColumnParameter {
    /// Unnamed columns of the given types.
    Positional(Vec<LogicalType>),
    /// Columns of the given types and names.
    Named(Vec<(LogicalType, String)>),
}

impl From<Vec<LogicalType>> for ColumnParameter {
    fn from(types: Vec<LogicalType>) -> Self {
        Positional(types)
    }
}

impl From<&[LogicalType]> for ColumnParameter {
    fn from(types: &[LogicalType]) -> Self {
        Positional(types.to_vec())
    }
}

impl From<&Vec<LogicalType>> for ColumnParameter {
    fn from(types: &Vec<LogicalType>) -> Self {
        Positional(types.clone())
    }
}

impl<const N: usize> From<[LogicalType; N]> for ColumnParameter {
    fn from(types: [LogicalType; N]) -> Self {
        Positional(types.into())
    }
}

impl<const N: usize> From<&[LogicalType; N]> for ColumnParameter {
    fn from(types: &[LogicalType; N]) -> Self {
        Positional(types.to_vec())
    }
}

fn named<'a, S: Into<String> + Clone + 'a>(columns: impl IntoIterator<Item = &'a (LogicalType, S)>) -> ColumnParameter {
    Named(
        columns
            .into_iter()
            .map(|(logical_type, name)| (logical_type.clone(), name.clone().into()))
            .collect(),
    )
}

impl<S: Into<String>> From<Vec<(LogicalType, S)>> for ColumnParameter {
    fn from(columns: Vec<(LogicalType, S)>) -> Self {
        Named(
            columns
                .into_iter()
                .map(|(logical_type, name)| (logical_type, name.into()))
                .collect(),
        )
    }
}

impl<S: Into<String> + Clone> From<&[(LogicalType, S)]> for ColumnParameter {
    fn from(columns: &[(LogicalType, S)]) -> Self {
        named(columns)
    }
}

impl<S: Into<String> + Clone> From<&Vec<(LogicalType, S)>> for ColumnParameter {
    fn from(columns: &Vec<(LogicalType, S)>) -> Self {
        named(columns)
    }
}

impl<S: Into<String>, const N: usize> From<[(LogicalType, S); N]> for ColumnParameter {
    fn from(columns: [(LogicalType, S); N]) -> Self {
        Vec::from(columns).into()
    }
}

impl<S: Into<String> + Clone, const N: usize> From<&[(LogicalType, S); N]> for ColumnParameter {
    fn from(columns: &[(LogicalType, S); N]) -> Self {
        named(columns)
    }
}

/// An owned collection of data chunks with a fixed column schema.
///
/// Move the collection into [`ColumnDataCollectionAppender`] to add rows or
/// borrow it through [`ColumnDataCollectionScan`] to iterate over its chunks.
/// Scanning takes a separate connection or callback context to allocate its
/// output chunk; the collection does not retain that context's factory.
///
/// # Lifetime
///
/// A collection reads and writes its data through the buffer manager of the
/// connection it was created from, so it borrows that
/// [`Connection`](crate::connection::Connection) or callback
/// [`Context`](crate::connection::Context) for `'conn`.
///
/// To let queries read a collection after that borrow ends, hand it to a
/// replacement scan registered on the same connection with
/// [`ReplacementScanBuilder::collection`](crate::replacement_scan::ReplacementScanBuilder::collection).
///
/// ```compile_fail,E0597
/// # use duckdb_neo::{DuckDBType, environment::{Environment, StorageLocation}};
/// # use duckdb_neo::column_data_collection::ColumnDataCollection;
/// # fn main() -> duckdb_neo::Result<()> {
/// let instance = Environment::new()?.open(StorageLocation::InMemory)?;
/// let collection = {
///     let conn = instance.connect()?;
///     ColumnDataCollection::new(&conn, [i32::logical_type(&conn)?])?
/// }; // error: `conn` does not live long enough
/// # Ok(())
/// # }
/// ```
///
/// # Example
/// ```
/// use duckdb_neo::{DuckDBType, environment::{Environment, StorageLocation}};
/// use duckdb_neo::column_data_collection::ColumnDataCollection;
/// use duckdb_neo::data_chunk::DataChunk;
///
/// # fn main() -> duckdb_neo::Result<()> {
/// let env = Environment::new()?;
/// let instance = env.open(StorageLocation::InMemory)?;
/// let conn = instance.connect()?;
/// let types = [i32::logical_type(&conn)?];
///
/// let collection = ColumnDataCollection::new(&conn, &types)?;
/// let mut chunk = DataChunk::create(&conn, &types, true)?;
/// let mut values = chunk.get_vector_at_mut::<i32>(0)?;
/// values.set_size(2)?;
/// values.writer()?.set(0, Some(10))?;
/// values.writer()?.set(1, Some(20))?;
///
/// let mut appender = collection.to_append()?;
/// appender.append(&mut chunk)?;
/// assert_eq!(appender.len()?, 2);
///
/// let mut scan = appender.to_scan(&conn)?;
/// let chunk = scan.next_chunk()?.expect("expected a chunk");
/// let values = chunk.get_vector_at::<i32>(0)?.reader()?;
/// assert_eq!(values.get(0)?, Some(&10));
/// assert_eq!(values.get(1)?, Some(&20));
///
/// drop(scan);
/// let collection = appender.to_normal();
/// assert_eq!(collection.len()?, 2);
/// # Ok(())
/// # }
/// ```
#[derive(Debug, AsRaw, DropHandle)]
#[drop_handle(ffi::duckdb_v2_column_data_collection_destroy)]
pub struct ColumnDataCollection<'conn> {
    pub(crate) handle: ffi::duckdb_v2_column_data_collection_handle,
    pub(crate) logical_types: Vec<LogicalType>,
    pub(crate) column_names: Option<Vec<String>>,
    pub(crate) origin: Option<ffi::duckdb_v2_connection_handle>,
    pub(crate) _marker: PhantomData<&'conn ()>,
}

impl<'conn> ColumnDataCollection<'conn> {
    /// Create an empty collection using a [`Connection`](crate::connection::Connection),
    /// [`Context`](crate::connection::Context), or [`Instance`](crate::instance::Instance).
    ///
    /// DuckDB rejects an empty schema and types containing `ANY`.
    ///
    /// The collection borrows `link`; see the [lifetime notes](Self#lifetime).
    /// `columns` is anything that converts into a [`ColumnParameter`].
    #[allow(private_bounds)]
    pub fn new<C: HasFactory + ConnectionOrigin>(link: &'conn C, columns: impl Into<ColumnParameter>) -> Result<Self> {
        let factory = link.factory()?;
        let (logical_types, names) = match columns.into() {
            Positional(logical_types) => (logical_types, None),
            Named(columns) => {
                let (logical_types, names) = columns.into_iter().unzip();
                (logical_types, Some(names))
            }
        };

        let types = logical_types.iter().map(|lt| lt.handle).collect::<Vec<_>>();

        check_api_call!(
            ffi::duckdb_v2_column_data_collection_create,
            factory.raw(),
            types.as_ptr(),
            types.len() as u64,
            RET
        )
        .map(|handle| ColumnDataCollection {
            handle,
            logical_types,
            column_names: names,
            origin: link.connection_origin(),
            _marker: PhantomData,
        })
    }

    /// Return the column types in schema order.
    pub fn logical_types(&self) -> &[LogicalType] {
        &self.logical_types
    }

    /// Return explicit column names, or `None` for positional columns.
    pub fn column_names(&self) -> Option<&[String]> {
        self.column_names.as_deref()
    }

    /// Return the number of columns in the schema.
    pub fn column_count(&self) -> usize {
        self.logical_types.len()
    }

    /// Return whether the collection contains no rows.
    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.len()? == 0)
    }

    /// Return the total number of rows across all stored chunks.
    pub fn len(&self) -> Result<usize> {
        check_api_call!(ffi::duckdb_v2_column_data_collection_row_count, self.handle, RET).map(|count| count as usize)
    }

    /// Consume the collection and initialize it for appending.
    pub fn to_append(self) -> Result<ColumnDataCollectionAppender<'conn>> {
        ColumnDataCollectionAppender::new(self)
    }

    /// Borrow the collection to iterate over its chunks.
    ///
    /// `link` supplies the factory for the scan's output chunk. Both it and the
    /// collection must outlive the scan.
    ///
    /// ```compile_fail,E0597
    /// # use duckdb_neo::{DuckDBType, environment::{Environment, StorageLocation}};
    /// # use duckdb_neo::column_data_collection::ColumnDataCollection;
    /// # fn main() -> duckdb_neo::Result<()> {
    /// # let conn = Environment::new()?.open(StorageLocation::InMemory)?.connect()?;
    /// let scan = {
    ///     let collection = ColumnDataCollection::new(&conn, [i32::logical_type(&conn)?])?;
    ///     collection.to_scan(&conn)?
    /// };
    /// assert!(scan.is_empty()?);
    /// # Ok(())
    /// # }
    /// ```
    pub fn to_scan<'scan, C: HasFactory>(&'scan self, link: &'scan C) -> Result<ColumnDataCollectionScan<'scan>> {
        ColumnDataCollectionScan::new(self, link)
    }
}

// SAFETY: shared methods read or create independent scan states; mutation requires ownership.
// The collection's buffers have no thread affinity.
unsafe impl Send for ColumnDataCollection<'_> {}
unsafe impl Sync for ColumnDataCollection<'_> {}

/// A scan over the chunks stored in a [`ColumnDataCollection`].
///
/// The scan borrows the collection and owns its progress state. Dropping it
/// releases the borrow, allowing the collection to be appended to again.
///
/// Chunks are read with [`next_chunk`](Self::next_chunk) rather than through
/// [`Iterator`]: DuckDB does not copy the scanned data, so each chunk points
/// into buffers that stay valid only until the next scan call.
#[derive(Debug)]
pub struct ColumnDataCollectionScan<'conn> {
    worker_scan_state: ColumnDataCollectionWorkerScanStateHandle,
    shared_scan_state: ColumnDataCollectionSharedScanStateHandle,
    /// Reused for every scan call; DuckDB resets it before filling it.
    chunk: DataChunk<'conn>,
    collection: &'conn ColumnDataCollection<'conn>,
}

impl<'conn> ColumnDataCollectionScan<'conn> {
    fn new<C: HasFactory>(collection: &'conn ColumnDataCollection<'conn>, link: &'conn C) -> Result<Self> {
        let worker_scan_state = collection.create_worker_scan_state()?;
        let shared_scan_state = collection.create_shared_scan_state()?;
        let chunk = DataChunk::create(link, &collection.logical_types, false)?;

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
    /// The chunk borrows the scan, so it cannot outlive it:
    ///
    /// ```compile_fail
    /// # use duckdb_neo::{DuckDBType, environment::{Environment, StorageLocation}};
    /// # use duckdb_neo::column_data_collection::ColumnDataCollection;
    /// # fn main() -> duckdb_neo::Result<()> {
    /// # let instance = Environment::new()?.open(StorageLocation::InMemory)?;
    /// # let conn = instance.connect()?;
    /// # let collection = ColumnDataCollection::new(&conn, [i32::logical_type(&conn)?])?;
    /// let chunk = {
    ///     let mut scan = collection.to_scan(&conn)?;
    ///     scan.next_chunk()?
    /// }; // error: `scan` does not live long enough
    /// # drop(chunk);
    /// # Ok(())
    /// # }
    /// ```
    pub fn next_chunk(&mut self) -> Result<Option<DataChunkRef<'_>>> {
        loop {
            let did_produce_chunk: bool = check_api_call!(
                ffi::duckdb_v2_column_data_collection_scan,
                self.collection.handle,
                self.shared_scan_state.raw(),
                self.worker_scan_state.raw(),
                self.chunk.handle,
                RET
            )?;
            if !did_produce_chunk {
                return Ok(None);
            }
            let chunk = DataChunkRef::new(self.chunk.handle, false)?;
            if chunk.row_count()? > 0 {
                return Ok(Some(chunk));
            }
        }
    }

    /// Return the total number of rows across all stored chunks.
    pub fn len(&self) -> Result<usize> {
        self.collection.len()
    }

    /// Return whether the collection contains no rows.
    pub fn is_empty(&self) -> Result<bool> {
        self.collection.is_empty()
    }

    /// Return the collection being scanned.
    pub fn collection(&self) -> &ColumnDataCollection<'conn> {
        self.collection
    }
}

/// An owned append state for a [`ColumnDataCollection`].
///
/// Chunks appended through this state must exactly match the collection's
/// column count and types. The state can be consumed to recover, scan, or
/// reset the collection.
#[derive(Debug)]
pub struct ColumnDataCollectionAppender<'conn> {
    appender: ColumnDataCollectionAppendStateHandle,
    // The append state must precede the collection so it drops before its storage.
    collection: ColumnDataCollection<'conn>,
}

impl<'conn> ColumnDataCollectionAppender<'conn> {
    fn new(collection: ColumnDataCollection<'conn>) -> Result<Self> {
        let appender = collection.create_append_state()?;

        Ok(Self { collection, appender })
    }

    /// Return the collection being appended to.
    pub fn collection(&self) -> &ColumnDataCollection<'conn> {
        &self.collection
    }

    /// Append a copy of `chunk`, an owned [`DataChunk`] or a borrowed [`DataChunkRef`], to the collection.
    ///
    /// DuckDB may flatten nested columns of `chunk` in place, so it is borrowed
    /// mutably. A mismatched column count or type returns an error without
    /// copying data.
    #[allow(private_bounds)]
    pub fn append<D: DataChunkMut>(&mut self, chunk: &mut D) -> Result<()> {
        let result = check_api_call!(
            ffi::duckdb_v2_column_data_collection_append,
            self.collection.handle,
            self.appender.raw(),
            chunk.chunk_handle()
        );
        // DuckDB may flatten nested columns in place, even when the append fails.
        chunk.evict_cached();
        result
    }

    /// Move all chunks from `other` into this collection and return the appender.
    ///
    /// `other` must come from the same instance. Combining across connections prevents
    /// handing the combined collection to a replacement scan.
    ///
    /// The append state is re-created to append after the combined chunks. If
    /// that fails, the error is returned and the collection is dropped.
    pub fn combine(mut self, mut other: ColumnDataCollection<'conn>) -> Result<Self> {
        check_api_call!(
            ffi::duckdb_v2_column_data_collection_combine,
            self.collection.handle,
            &mut other.handle
        )?;

        if self.collection.origin != other.origin {
            self.collection.origin = None;
        }

        self.appender = self.collection.create_append_state()?;

        Ok(self)
    }

    /// Consume the appender and return an empty collection with its schema unchanged.
    pub fn reset(self) -> Result<ColumnDataCollection<'conn>> {
        check_api_call!(ffi::duckdb_v2_column_data_collection_reset, self.collection.handle)?;
        Ok(self.collection)
    }

    /// Return an empty collection, retaining its schema and buffers.
    pub fn clear(self) -> Result<ColumnDataCollection<'conn>> {
        check_api_call!(ffi::duckdb_v2_column_data_collection_clear, self.collection.handle)?;
        Ok(self.collection)
    }

    /// Finish appending and return the underlying collection.
    pub fn to_normal(self) -> ColumnDataCollection<'conn> {
        self.collection
    }

    /// Borrow the appended rows for scanning without ending the append state.
    ///
    /// Appending can resume after the scan is dropped.
    pub fn to_scan<'scan, C: HasFactory>(&'scan self, link: &'scan C) -> Result<ColumnDataCollectionScan<'scan>> {
        self.collection.to_scan(link)
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
    #[test]
    fn test_empty_appender_scan() -> crate::Result<()> {
        let env = Environment::new()?;
        let conn = env.open(StorageLocation::InMemory)?.connect()?;
        let collection = ColumnDataCollection::new(&conn, [i32::logical_type(&conn)?])?;
        let appender = collection.to_append()?;
        let mut scan = appender.to_scan(&conn)?;
        assert!(scan.next_chunk()?.is_none());
        Ok(())
    }

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
            output.writer()?.set(0, Some(collection.is_empty()?))
        });

        let env = Environment::new()?;
        let instance = env.open(StorageLocation::InMemory)?;
        let conn = instance.connect()?;

        ScalarFunctionBuilder::new(
            "collection_is_empty",
            SignatureBuilder::new().return_type(bool::logical_type(&conn)?),
            CollectionIsEmpty,
        )
        .register(&conn)?;

        let mut result = conn.query("SELECT collection_is_empty()", Parameters::None)?;
        let chunk = result.next_chunk()?.expect("expected a result chunk");
        assert_eq!(chunk.get_vector_at::<bool>(0)?.reader()?.get(0)?, Some(&true));
        Ok(())
    }

    #[test]
    fn test_collection_add() -> crate::Result<()> {
        let env = Environment::new()?;
        let instance = env.open(StorageLocation::InMemory)?;
        let conn = instance.connect()?;

        let mut statements = conn.parse(
            r#"
         CREATE TABLE employees (
             id           INTEGER PRIMARY KEY,
            is_active    BOOLEAN
         );

         -- BY NAME fails unless the replacement scan's custom names are applied.
         INSERT INTO employees BY NAME SELECT * FROM A;

         SELECT * FROM employees;
        "#,
        )?;

        let statement = statements.next().unwrap()?;
        conn.execute(statement, Parameters::None)?;

        let logical_types = [i32::logical_type(&conn)?, bool::logical_type(&conn)?];
        let names = ["id", "is_active"];

        let collection = ColumnDataCollection::new(
            &conn,
            &[
                (logical_types[0].clone(), names[0].to_string()),
                (logical_types[1].clone(), names[1].to_string()),
            ],
        )?;

        let mut chunk = DataChunk::create(&conn, &logical_types, true)?;

        let [id, is_active] = chunk.get_vector_array_mut([0, 1])?;
        let (mut id, mut is_active) = (id.cast::<i32>()?, is_active.cast::<bool>()?);

        id.set_size(2)?;
        is_active.set_size(2)?;

        id.writer()?.set(0, Some(10))?;
        id.writer()?.set(1, None)?;

        let mut collection = collection.to_append()?;

        collection.append(&mut chunk)?;

        assert_eq!(collection.len()?, 2);

        let collection = collection.reset()?;

        assert_eq!(collection.len()?, 0);

        let [id, is_active] = chunk.get_vector_array_mut([0, 1])?;
        let (mut id, mut is_active) = (id.cast::<i32>()?, is_active.cast::<bool>()?);
        id.writer()?.set(0, Some(10))?;
        id.writer()?.set(1, Some(12))?;
        is_active.writer()?.set(0, Some(false))?;
        is_active.writer()?.set(1, None)?;

        let mut collection = collection.to_append()?;

        collection.append(&mut chunk)?;

        assert_eq!(collection.len()?, 2);

        let mut chunk_2 = DataChunk::create(&conn, &logical_types, true)?;
        let [id, is_active] = chunk_2.get_vector_array_mut([0, 1])?;
        let (mut id, mut is_active) = (id.cast::<i32>()?, is_active.cast::<bool>()?);

        id.set_size(1)?;
        is_active.set_size(1)?;

        id.writer()?.set(0, Some(14))?;
        is_active.writer()?.set(0, Some(true))?;

        let mut collection_2 = ColumnDataCollection::new(&conn, &logical_types)?.to_append()?;
        collection_2.append(&mut chunk_2)?;

        let collection = collection.combine(collection_2.to_normal())?;

        struct A;

        impl ReplacementScanCallbacks for A {
            fn scan(
                &self,
                _context: &crate::connection::Context,
                name: &crate::qualified_name::QualifiedName,
                handle: crate::replacement_scan::ReplacementHandle<'_>,
            ) -> Result<()> {
                if name.get_view()?.table == "A" {
                    handle.set_reference(ReplacementType::ColumnDataCollection("A"))?;
                }

                Ok(())
            }
        }

        ReplacementScanBuilder::new(A)
            .collection("A", collection.to_normal())
            .register(&conn)?;

        let statement = statements.next().unwrap()?;

        let rows_changed = conn.execute(statement, Parameters::None)?;
        assert_eq!(rows_changed, 3);

        let statement = statements.next().unwrap()?;
        let mut result = conn.query(statement, Parameters::None)?;

        if let Some(chunk) = result.next_chunk()? {
            let id = chunk.get_vector_at::<i32>(0)?;
            let id_reader = id.reader()?;
            let is_active = chunk.get_vector_at::<bool>(1)?;
            let is_active_reader = is_active.reader()?;

            assert_eq!(id_reader.get(0)?, Some(&10));
            assert_eq!(is_active_reader.get(0)?, Some(&false));

            assert_eq!(id_reader.get(1)?, Some(&12));
            assert_eq!(is_active_reader.get(1)?, None);

            assert_eq!(id_reader.get(2)?, Some(&14));
            assert_eq!(is_active_reader.get(2)?, Some(&true));
        } else {
            panic!("Expected a result chunk, but got none");
        }

        Ok(())
    }

    #[test]
    fn test_collection_scan() -> crate::Result<()> {
        let env = Environment::new()?;
        let instance = env.open(StorageLocation::InMemory)?;
        let conn = instance.connect()?;

        let logical_types = [i32::logical_type(&conn)?, bool::logical_type(&conn)?];

        let collection = ColumnDataCollection::new(&conn, &logical_types)?;

        let mut chunk = DataChunk::create(&conn, &logical_types, true)?;

        let [id, is_active] = chunk.get_vector_array_mut([0, 1])?;
        let (mut id, mut is_active) = (id.cast::<i32>()?, is_active.cast::<bool>()?);

        id.set_size(2)?;
        is_active.set_size(2)?;

        id.writer()?.set(0, Some(10))?;
        id.writer()?.set(1, None)?;

        is_active.writer()?.set(0, Some(false))?;
        is_active.writer()?.set(1, None)?;

        let mut collection = collection.to_append()?;

        collection.append(&mut chunk)?;

        let mut scan = collection.to_scan(&conn)?;

        while let Some(chunk) = scan.next_chunk()? {
            let id = chunk.get_vector_at::<i32>(0)?;
            let id_reader = id.reader()?;
            let is_active = chunk.get_vector_at::<bool>(1)?;
            let is_active_reader = is_active.reader()?;

            assert_eq!(id_reader.get(0)?, Some(&10));
            assert_eq!(is_active_reader.get(0)?, Some(&false));

            assert_eq!(id_reader.get(1)?, None);
            assert_eq!(is_active_reader.get(1)?, None);
        }

        Ok(())
    }

    #[test]
    fn test_collection_scan_borrow() -> crate::Result<()> {
        let conn = Environment::new()?.open(StorageLocation::InMemory)?.connect()?;
        let types = [i32::logical_type(&conn)?];
        let mut chunk = DataChunk::create(&conn, &types, true)?;
        let mut values = chunk.get_vector_at_mut::<i32>(0)?;
        values.set_size(1)?;
        values.writer()?.set(0, Some(42))?;
        let mut appender = ColumnDataCollection::new(&conn, &types)?.to_append()?;
        appender.append(&mut chunk)?;

        let mut first = appender.to_scan(&conn)?;
        let mut second = appender.to_scan(&conn)?;
        for scan in [&mut first, &mut second] {
            let chunk = scan.next_chunk()?.unwrap();
            assert_eq!(chunk.get_vector_at::<i32>(0)?.reader()?.get(0)?, Some(&42));
            assert!(scan.next_chunk()?.is_none());
        }
        drop(first);
        drop(second);

        appender.append(&mut chunk)?;
        let collection = appender.to_normal();
        assert_eq!(collection.len()?, 2);
        let mut scan = collection.to_scan(&conn)?;
        let mut rows = Vec::new();
        while let Some(chunk) = scan.next_chunk()? {
            rows.extend(chunk.get_vector_at::<i32>(0)?.reader()?.iter().flatten().copied());
        }
        assert_eq!(rows, [42, 42]);
        Ok(())
    }

    #[test]
    fn test_collection_append_after_combine() -> crate::Result<()> {
        let env = Environment::new()?;
        let instance = env.open(StorageLocation::InMemory)?;
        let conn = instance.connect()?;

        let logical_types = [i32::logical_type(&conn)?];
        let chunk_of = |values: &[i32]| -> crate::Result<DataChunk<'_>> {
            let mut chunk = DataChunk::create(&conn, &logical_types, true)?;
            let mut vector = chunk.get_vector_at_mut::<i32>(0)?;
            vector.set_size(values.len())?;
            for (row, value) in values.iter().enumerate() {
                vector.writer()?.set(row, Some(*value))?;
            }
            Ok(chunk)
        };

        let mut appender = ColumnDataCollection::new(&conn, &logical_types)?.to_append()?;
        appender.append(&mut chunk_of(&[1, 2])?)?;

        let mut other = ColumnDataCollection::new(&conn, &logical_types)?.to_append()?;
        other.append(&mut chunk_of(&[3])?)?;

        let mut appender = appender.combine(other.to_normal())?;
        appender.append(&mut chunk_of(&[4, 5])?)?;
        assert_eq!(appender.len()?, 5);

        let mut scan = appender.to_scan(&conn)?;
        let mut values = Vec::new();
        while let Some(chunk) = scan.next_chunk()? {
            values.extend(
                chunk
                    .get_vector_at::<i32>(0)?
                    .reader()?
                    .iter()
                    .map(|value| *value.unwrap()),
            );
        }
        assert_eq!(values, [1, 2, 3, 4, 5]);

        Ok(())
    }
}

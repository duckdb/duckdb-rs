//! Columnar batches exchanged with DuckDB.

use std::ops::Deref;

use crate::connection::{Connection, Context};
use crate::error::check_api_call_no_err;
use crate::ffi;
use crate::links::{DatabaseKeepAlive, KeepAlive};
use crate::logical_type::LogicalType;
use crate::vector::{Access, VectorElement};
use crate::{
    Result, check_api_call,
    vector::{Unknown, Vector},
};

fn check_distinct(indices: &[usize]) -> Result<()> {
    for (i, index) in indices.iter().enumerate() {
        if indices[..i].contains(index) {
            return Err(crate::error::Error {
                code: crate::error::DuckDBError::DUCKDB_V2_ERROR_INPUT_PARAMETER_INVALID,
                message: format!("vector index {index} was requested more than once"),
            });
        }
    }
    Ok(())
}

fn collect_array<V, const N: usize>(results: [Result<V>; N]) -> Result<[V; N]> {
    let vectors = results.into_iter().collect::<Result<Vec<_>>>()?;
    match vectors.try_into() {
        Ok(array) => Ok(array),
        Err(_) => unreachable!("collected exactly N vectors"),
    }
}

trait DataChunkLink {
    fn copy_data_chunk(&self, chunk: &DataChunkRef<'_>) -> crate::Result<DataChunk<'static>>;
}

impl DataChunkLink for Connection {
    fn copy_data_chunk(&self, chunk: &DataChunkRef<'_>) -> crate::Result<DataChunk<'static>> {
        let handle = check_api_call!(ffi::duckdb_v2_data_chunk_copy_with_connection, **self, **chunk, RET)?;
        Ok(DataChunk::new(handle, true).keep_alive(self))
    }
}

impl DataChunkLink for Context {
    fn copy_data_chunk(&self, chunk: &DataChunkRef<'_>) -> crate::Result<DataChunk<'static>> {
        Ok(DataChunk::new(
            check_api_call!(ffi::duckdb_v2_data_chunk_copy_with_context, **self, **chunk, RET)?,
            true,
        ))
    }
}

/// Read-only input vectors and their row count for scalar and aggregate callbacks.
///
/// Vectors follow argument order and borrow DuckDB's data for the duration of
/// the callback.
pub struct VectorCollection {
    pub(crate) handles: Vec<ffi::duckdb_v2_vector_handle>,
    pub(crate) is_writable: bool,
    pub(crate) row_count: usize,
}

impl VectorCollection {
    fn mut_access(&self) -> Access {
        if self.is_writable {
            Access::Writable
        } else {
            Access::Exclusive
        }
    }

    pub fn get_unchecked_vector_at(&self, index: usize) -> Result<Vector<'_, Unknown>> {
        Vector::from_handle(&self.handles[index], Access::Shared)
    }

    /// Return a read-only view of the vector at `index`, narrowed to `T`.
    ///
    /// A logical type incompatible with `T` returns an error. Use
    /// [`Self::get_vector_at_mut`] to flatten the vector.
    ///
    /// # Panics
    ///
    /// Panics if `index` is out of range.
    pub fn get_vector_at<T: VectorElement>(&self, index: usize) -> Result<Vector<'_, T>> {
        Vector::from_handle(&self.handles[index], Access::Shared)?.cast::<T>()
    }

    /// Return the vector at `index`, narrowed to `T`, borrowed mutably.
    /// The exclusive borrow allows reshaping the vector, e.g. with
    /// [`Vector::flatten`]. Writing requires writable input.
    ///
    /// # Panics
    ///
    /// Panics if `index` is out of range.
    pub fn get_vector_at_mut<T: VectorElement>(&mut self, index: usize) -> Result<Vector<'_, T>> {
        Vector::from_handle(&self.handles[index], self.mut_access())?.cast::<T>()
    }

    /// Return the vectors at distinct `indices`, borrowed mutably at the same time.
    /// Duplicate indices return an error. Narrow each vector with [`Vector::cast`].
    ///
    /// # Panics
    ///
    /// Panics if an index is out of range.
    pub fn get_vector_array_mut<const N: usize>(&mut self, indices: [usize; N]) -> Result<[Vector<'_, Unknown>; N]> {
        check_distinct(&indices)?;
        let access = self.mut_access();
        collect_array(indices.map(|index| Vector::from_handle(&self.handles[index], access)))
    }

    /// Return the number of vectors, which is the column count.
    pub fn col_count(&self) -> usize {
        self.handles.len()
    }

    /// Return the number of input rows in this callback batch.
    pub fn row_count(&self) -> usize {
        self.row_count
    }

    /// Wrap the callback's input vectors in a [`DataChunk`] without copying.
    ///
    /// The chunk's vectors reference the input's storage, so the chunk borrows
    /// this collection and cannot outlive the callback. It is writable only when
    /// the input is. Use [`DataChunkRef::copy`] to keep the data longer.
    pub fn to_data_chunk(&self) -> crate::Result<DataChunk<'_>> {
        let mut logical_types = Vec::with_capacity(self.col_count());

        for i in 0..self.col_count() {
            let vector = Vector::from_handle(&self.handles[i], Access::Shared)?;
            logical_types.push(vector.logical_type().clone());
        }

        let chunk = DataChunk::create(&logical_types, self.is_writable)?;

        for (i, handle) in self.handles.iter().enumerate() {
            let chunk_handle = chunk.get_vector_handle_at(i)?;

            check_api_call!(ffi::duckdb_v2_vector_reference, chunk_handle, *handle)?;
        }

        Ok(chunk)
    }
}

/// A non-owning view of a DuckDB data chunk.
///
/// Provides vector access for both owned [`DataChunk`] values and borrowed
/// callback chunks. Vectors borrowed from this view cannot outlive it.
#[derive(Debug)]
pub struct DataChunkRef<'a> {
    pub(crate) handle: ffi::duckdb_v2_data_chunk_handle,
    is_writable: bool,
    _marker: std::marker::PhantomData<&'a ()>,
}

/// A columnar batch whose vectors share one row count.
///
/// Chunks may be returned by queries and callbacks or created with
/// [`DataChunk::create`]. Vectors borrow the chunk and cannot outlive it.
///
/// # Example
/// ```
/// use duckdb_neo::{
///     Parameters,
///     environment::Environment,
///     environment::StorageLocation,
/// };
///
/// # fn main() -> duckdb_neo::Result<()> {
/// let env = Environment::new()?;
/// let db = env.open(StorageLocation::InMemory)?;
/// let conn = db.connect()?;
/// let mut statements = conn.parse("SELECT * FROM (VALUES (10), (20))")?;
/// let statement = statements.next().expect("expected a statement")?;
/// let chunk = conn
///     .query(statement, Parameters::None)?
///     .next()
///     .transpose()?
///     .expect("expected rows");
///
/// let values = chunk.get_vector_at::<i32>(0)?;
/// assert_eq!(chunk.row_count()?, 2);
/// assert_eq!(values.get(0)?, Some(&10));
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
///
/// Chunks created or copied by the caller are `DataChunk<'static>`. A chunk that
/// references data owned elsewhere, such as one from
/// [`VectorCollection::to_data_chunk`], borrows that data for `'a`.
pub struct DataChunk<'a> {
    pub(crate) chunk: DataChunkRef<'a>,
    /// Keeps the database behind a connection allocator alive; dropped after the chunk is destroyed.
    database: Option<KeepAlive>,
}

impl Deref for DataChunkRef<'_> {
    type Target = ffi::duckdb_v2_data_chunk_handle;
    fn deref(&self) -> &Self::Target {
        &self.handle
    }
}

impl<'a> DataChunkRef<'a> {
    pub(crate) fn new(handle: ffi::duckdb_v2_data_chunk_handle, is_writable: bool) -> Self {
        Self {
            handle,
            is_writable,
            _marker: std::marker::PhantomData,
        }
    }

    /// Return the number of rows shared by the chunk's vectors.
    pub fn row_count(&self) -> Result<usize> {
        let row_count: ffi::idx_t = check_api_call!(ffi::duckdb_v2_data_chunk_get_size, self.handle, RET)?;
        Ok(row_count as usize)
    }

    /// Return all vectors as logically untyped borrowed views.
    ///
    /// Narrow a vector with [`Vector::cast`] before typed access.
    pub fn vectors(&self) -> Result<Vec<Vector<'_, Unknown>>> {
        let count = self.col_count()?;

        let mut vectors = Vec::with_capacity(count);
        for i in 0..count {
            let vector: ffi::duckdb_v2_vector_handle =
                check_api_call!(ffi::duckdb_v2_data_chunk_get_vector, self.handle, i as u64, RET)?;
            vectors.push(Vector::from_handle(&vector, Access::Shared)?);
        }
        Ok(vectors)
    }

    /// Return the number of vectors, which is the column count.
    pub fn col_count(&self) -> Result<usize> {
        let out_count: ffi::idx_t = check_api_call!(ffi::duckdb_v2_data_chunk_get_vector_count, self.handle, RET)?;
        Ok(out_count as usize)
    }

    /// Return a read-only view of the vector at `index`, narrowed to `T`.
    ///
    /// An out-of-range index or a logical type incompatible with `T` returns an
    /// error. Use [`Self::get_vector_at_mut`] to write or flatten the vector.
    pub fn get_vector_at<T: VectorElement>(&self, index: usize) -> Result<Vector<'_, T>> {
        Vector::from_handle(&self.get_vector_handle_at(index)?, Access::Shared)?.cast::<T>()
    }

    /// Return the vector at `index`, narrowed to `T`, borrowed mutably.
    ///
    /// The exclusive borrow allows reshaping the vector, e.g. with
    /// [`Vector::flatten`], and writing to it when the chunk is writable. An
    /// out-of-range index or a logical type incompatible with `T` returns an
    /// error.
    ///
    /// The borrow rules out other views of the chunk while the vector is alive:
    ///
    /// ```compile_fail,E0502
    /// # use duckdb_neo::{DuckDBType, data_chunk::DataChunk, environment::{Environment, StorageLocation}};
    /// # fn main() -> duckdb_neo::Result<()> {
    /// # let env = Environment::new()?;
    /// # let conn = env.open(StorageLocation::InMemory)?.connect()?;
    /// let mut chunk = DataChunk::create(&[i32::logical_type(&conn)?], true)?;
    /// let shared = chunk.get_vector_at::<i32>(0)?;
    /// let mut exclusive = chunk.get_vector_at_mut::<i32>(0)?;
    /// exclusive.write(0, Some(1))?;
    /// let _ = shared.get(0)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn get_vector_at_mut<T: VectorElement>(&mut self, index: usize) -> Result<Vector<'_, T>> {
        Vector::from_handle(&self.get_vector_handle_at(index)?, self.mut_access())?.cast::<T>()
    }

    /// Return the vectors at distinct `indices`, borrowed mutably at the same time.
    ///
    /// Duplicate or out-of-range indices return an error. Narrow each vector
    /// with [`Vector::cast`].
    pub fn get_vector_array_mut<const N: usize>(&mut self, indices: [usize; N]) -> Result<[Vector<'_, Unknown>; N]> {
        check_distinct(&indices)?;
        let access = self.mut_access();
        collect_array(indices.map(|index| Vector::from_handle(&self.get_vector_handle_at(index)?, access)))
    }

    fn mut_access(&self) -> Access {
        if self.is_writable {
            Access::Writable
        } else {
            Access::Exclusive
        }
    }

    pub(crate) fn get_vector_handle_at(&self, index: usize) -> Result<ffi::duckdb_v2_vector_handle> {
        let vector: ffi::duckdb_v2_vector_handle =
            check_api_call!(ffi::duckdb_v2_data_chunk_get_vector, self.handle, index as u64, RET)?;
        Ok(vector)
    }

    /// Deep-copy this chunk into a new, writable chunk owned by `link`'s connection or context.
    ///
    /// A copy made through a [`Connection`] keeps its database alive. A copy made
    /// through a [`Context`] must not outlive the database, for example by being
    /// stored in a `static` or sent to code outside DuckDB.
    #[allow(private_bounds)]
    pub fn copy<C: DataChunkLink>(&self, link: &C) -> Result<DataChunk<'static>> {
        link.copy_data_chunk(self)
    }
}

/// Selects where a chunk's vectors are allocated.
pub enum Allocator<'a> {
    /// Allocate from the DuckDB default allocator.
    Default,
    /// Allocate from the given connection. The chunk keeps the connection's database alive.
    Connection(&'a Connection),
    /// Allocate from the given callback context. The chunk must not outlive the
    /// database, for example by being stored in a `static` or sent to code outside DuckDB.
    Context(&'a Context),
}

impl<'a> DataChunk<'a> {
    pub(crate) fn new(handle: ffi::duckdb_v2_data_chunk_handle, is_writable: bool) -> Self {
        Self {
            chunk: DataChunkRef::new(handle, is_writable),
            database: None,
        }
    }

    fn keep_alive(mut self, link: &impl DatabaseKeepAlive) -> Self {
        self.database = link.keep_alive();
        self
    }

    /// Create an empty chunk with one vector per logical type.
    pub fn create(types: &[LogicalType], writable: bool) -> Result<Self> {
        Self::create_with_allocator(types, writable, Allocator::Default)
    }

    /// Create an empty chunk with one vector per logical type.
    ///
    /// Vectors start as flat storage with zero logical rows. Set `writable` to
    /// allow mutation, then call [`Vector::set_size`] after populating them.
    pub fn create_with_allocator(types: &[LogicalType], writable: bool, allocator: Allocator<'_>) -> Result<Self> {
        let len = types.len();
        let type_handles = types
            .as_ref()
            .iter()
            .map(|lt| lt.handle)
            .collect::<Vec<ffi::duckdb_v2_logical_type_handle>>();

        let handle = match allocator {
            Allocator::Default => {
                check_api_call!(ffi::duckdb_v2_data_chunk_create, type_handles.as_ptr(), len as u64, RET)?
            }
            Allocator::Connection(conn) => {
                let handle = check_api_call!(
                    ffi::duckdb_v2_data_chunk_create_with_connection,
                    **conn,
                    type_handles.as_ptr(),
                    len as u64,
                    RET
                )?;
                return Ok(DataChunk::new(handle, writable).keep_alive(conn));
            }
            Allocator::Context(context) => check_api_call!(
                ffi::duckdb_v2_data_chunk_create_with_context,
                **context,
                type_handles.as_ptr(),
                len as u64,
                RET
            )?,
        };
        Ok(DataChunk::new(handle, writable))
    }

    /// See [`DataChunkRef::get_vector_at_mut`].
    pub fn get_vector_at_mut<T: VectorElement>(&mut self, index: usize) -> Result<Vector<'_, T>> {
        self.chunk.get_vector_at_mut(index)
    }

    /// See [`DataChunkRef::get_vector_array_mut`].
    pub fn get_vector_array_mut<const N: usize>(&mut self, indices: [usize; N]) -> Result<[Vector<'_, Unknown>; N]> {
        self.chunk.get_vector_array_mut(indices)
    }
}

impl Drop for DataChunk<'_> {
    fn drop(&mut self) {
        check_api_call_no_err!(ffi::duckdb_v2_data_chunk_destroy, &mut self.chunk.handle).unwrap();
    }
}

impl<'a> Deref for DataChunk<'a> {
    type Target = DataChunkRef<'a>;

    fn deref(&self) -> &Self::Target {
        &self.chunk
    }
}

// SAFETY: an owned C++ `DataChunk` has no thread affinity, and its allocators free thread-safely.
unsafe impl Send for DataChunk<'_> {}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {

    use crate::builder_helpers::scalar_callback;
    use crate::data_chunk::Allocator;
    use crate::scalar::ScalarFunctionBuilder;
    use crate::signature::{Parameter, SignatureBuilder};
    use crate::types::DuckDBType;
    use crate::{
        data_chunk::DataChunk,
        environment::{Environment, StorageLocation},
    };

    #[test]
    fn test_data_chunk_create_with_connection() -> crate::Result<()> {
        let env = Environment::new()?;
        let db = env.open(StorageLocation::InMemory)?;
        let conn = db.connect()?;

        let data_chunk =
            DataChunk::create_with_allocator(&[i32::logical_type(&conn)?], true, super::Allocator::Connection(&conn))?;

        assert_eq!(data_chunk.row_count()?, 0);
        assert_eq!(data_chunk.col_count()?, 1);

        Ok(())
    }

    #[test]
    fn test_copy_data_chunk() -> crate::Result<()> {
        scalar_callback!(ChunkCopy, i32, |input, result, context, _ud| {
            let mut dc =
                DataChunk::create_with_allocator(&[i32::logical_type(&context)?], true, Allocator::Context(context))?;

            let vec = dc.get_vector_at_mut::<i32>(0)?;

            let input = input.get_vector_at::<i32>(0)?;

            unsafe {
                vec.copy_from(&input)?;
            };
            let copy = dc.copy(context)?;
            let vec = copy.get_vector_at::<i32>(0)?;

            unsafe {
                result.copy_from(&vec)?;
            }

            Ok(())
        });

        let env = Environment::new()?;
        let db = env.open(StorageLocation::InMemory)?;
        let conn = db.connect()?;

        ScalarFunctionBuilder::new(
            "chunk_copy",
            SignatureBuilder::new(
                [Parameter::normal("in", i32::logical_type(&conn)?)],
                i32::logical_type(&conn)?,
            ),
            ChunkCopy,
        )
        .register(&conn)?;

        let query = conn.query("SELECT chunk_copy(unnest([1,2,3,4]))", crate::Parameters::None)?;

        for chunk in query {
            let chunk = chunk?;

            let vec = chunk.get_vector_at::<i32>(0)?;

            assert_eq!(vec.get(0)?, Some(&1));
            assert_eq!(vec.get(1)?, Some(&2));
            assert_eq!(vec.get(2)?, Some(&3));
            assert_eq!(vec.get(3)?, Some(&4));
            assert!(vec.get(4).is_err());
        }

        Ok(())
    }

    #[test]
    fn test_shared_vectors_cannot_change_storage() -> crate::Result<()> {
        let env = Environment::new()?;
        let db = env.open(StorageLocation::InMemory)?;
        let conn = db.connect()?;
        let mut chunk = DataChunk::create(&[i64::logical_type(&conn)?, i64::logical_type(&conn)?], true)?;
        chunk.get_vector_at_mut::<i64>(1)?.make_sequence(0, 1, 4)?;

        let mut shared = chunk.get_vector_at::<i64>(0)?;
        assert!(!shared.is_writable());
        assert!(shared.set_size(1).is_err());
        assert!(shared.write(0, Some(1)).is_err());

        let mut sequence = chunk.get_vector_at::<i64>(1)?;
        assert!(sequence.flatten().is_err());
        drop((shared, sequence));

        let mut sequence = chunk.get_vector_at_mut::<i64>(1)?;
        sequence.flatten()?;
        assert_eq!(sequence.get(3)?, Some(&3));

        Ok(())
    }

    #[test]
    fn test_get_vector_array_mut() -> crate::Result<()> {
        let env = Environment::new()?;
        let db = env.open(StorageLocation::InMemory)?;
        let conn = db.connect()?;
        let mut chunk = DataChunk::create(&[i32::logical_type(&conn)?, bool::logical_type(&conn)?], true)?;

        assert!(chunk.get_vector_array_mut([0, 0]).is_err());
        assert!(chunk.get_vector_array_mut([0, 2]).is_err());

        let [ids, flags] = chunk.get_vector_array_mut([0, 1])?;
        let (mut ids, mut flags) = (ids.cast::<i32>()?, flags.cast::<bool>()?);
        ids.set_size(1)?;
        flags.set_size(1)?;
        ids.write(0, Some(7))?;
        flags.write(0, Some(true))?;
        drop((ids, flags));

        assert_eq!(chunk.get_vector_at::<i32>(0)?.get(0)?, Some(&7));
        assert_eq!(chunk.get_vector_at::<bool>(1)?.get(0)?, Some(&true));

        Ok(())
    }

    #[test]
    fn test_connection_chunk_keeps_database_alive() -> crate::Result<()> {
        let env = crate::environment::Environment::new()?;
        let db = env.open(crate::environment::StorageLocation::InMemory)?;
        let conn = db.connect()?;
        let types = [i32::logical_type(&conn)?];

        let mut chunk = DataChunk::create_with_allocator(&types, true, Allocator::Connection(&conn))?;
        let copy = chunk.copy(&conn)?;
        let mut values = chunk.get_vector_at_mut::<i32>(0)?;
        values.set_size(1)?;
        values.write(0, Some(42))?;
        drop(values);

        drop(conn);
        drop(db);
        assert_eq!(env.get_database_count()?, 1);

        assert_eq!(chunk.get_vector_at::<i32>(0)?.get(0)?, Some(&42));
        drop(chunk);
        assert_eq!(env.get_database_count()?, 1);
        drop(copy);
        assert_eq!(env.get_database_count()?, 0);

        Ok(())
    }
}

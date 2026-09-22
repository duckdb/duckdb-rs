//! Columnar batches exchanged with DuckDB.

use std::ops::Deref;

use crate::connection::{Connection, Context};
use crate::error::check_api_call_no_err;
use crate::ffi;
use crate::logical_type::LogicalType;
use crate::vector::VectorElement;
use crate::{
    Result, check_api_call,
    vector::{Unknown, Vector},
};

trait DataChunkLink {
    fn copy_data_chunk(&self, chunk: &DataChunkRef<'_>) -> crate::Result<DataChunk>;
}

impl DataChunkLink for Connection {
    fn copy_data_chunk(&self, chunk: &DataChunkRef<'_>) -> crate::Result<DataChunk> {
        Ok(DataChunk::new(
            check_api_call!(ffi::duckdb_v2_data_chunk_copy_with_connection, **self, **chunk, RET)?,
            true,
        ))
    }
}

impl DataChunkLink for Context {
    fn copy_data_chunk(&self, chunk: &DataChunkRef<'_>) -> crate::Result<DataChunk> {
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
    /// Return all vectors as logically untyped borrowed views.
    pub fn vectors(&self) -> Result<Vec<Vector<'_, Unknown>>> {
        let mut vectors = vec![];

        for handle in &self.handles {
            vectors.push(Vector::from_handle(handle, self.is_writable)?);
        }

        Ok(vectors)
    }

    /// Return the vector at `index`, narrowed to `T`.
    ///
    /// A logical type incompatible with `T` returns an error.
    ///
    /// # Panics
    ///
    /// Panics if `index` is out of range.
    pub fn get_vector_at<T: VectorElement>(&self, index: usize) -> Result<Vector<'_, T>> {
        let vec = Vector::from_handle(&self.handles[index], self.is_writable)?;

        vec.cast::<T>()
    }

    /// Return the number of vectors, which is the column count.
    pub fn vectors_count(&self) -> usize {
        self.handles.len()
    }

    /// Return the number of input rows in this callback batch.
    pub fn row_count(&self) -> usize {
        self.row_count
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
pub struct DataChunk {
    chunk: DataChunkRef<'static>,
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
        let count = self.vectors_count()?;

        let mut vectors = Vec::with_capacity(count);
        for i in 0..count {
            let vector: ffi::duckdb_v2_vector_handle =
                check_api_call!(ffi::duckdb_v2_data_chunk_get_vector, self.handle, i as u64, RET)?;
            vectors.push(Vector::from_handle(&vector, self.is_writable)?);
        }
        Ok(vectors)
    }

    /// Return the number of vectors, which is the column count.
    pub fn vectors_count(&self) -> Result<usize> {
        let out_count: ffi::idx_t = check_api_call!(ffi::duckdb_v2_data_chunk_get_vector_count, self.handle, RET)?;
        Ok(out_count as usize)
    }

    /// Return the vector at `index`, narrowed to `T`.
    ///
    /// An out-of-range index or a logical type incompatible with `T` returns an
    /// error.
    pub fn get_vector_at<T: VectorElement>(&self, index: usize) -> Result<Vector<'_, T>> {
        let vector: ffi::duckdb_v2_vector_handle =
            check_api_call!(ffi::duckdb_v2_data_chunk_get_vector, self.handle, index as u64, RET)?;
        let vec = Vector::from_handle(&vector, self.is_writable)?;

        vec.cast::<T>()
    }

    /// Deep-copy this chunk into a new, writable chunk owned by `link`'s connection or context.
    #[allow(private_bounds)]
    pub fn copy<C: DataChunkLink>(&self, link: &C) -> Result<DataChunk> {
        link.copy_data_chunk(self)
    }
}

/// Selects where a chunk's vectors are allocated.
pub enum Allocator<'a> {
    /// Allocate from the DuckDB default allocator.
    Default,
    /// Allocate from the given connection.
    Connection(&'a Connection),
    /// Allocate from the given callback context.
    Context(&'a Context),
}

impl DataChunk {
    pub(crate) fn new(handle: ffi::duckdb_v2_data_chunk_handle, is_writable: bool) -> Self {
        Self {
            chunk: DataChunkRef::new(handle, is_writable),
        }
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
            Allocator::Connection(conn) => check_api_call!(
                ffi::duckdb_v2_data_chunk_create_with_connection,
                **conn,
                type_handles.as_ptr(),
                len as u64,
                RET
            )?,
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
}

impl Drop for DataChunk {
    fn drop(&mut self) {
        check_api_call_no_err!(ffi::duckdb_v2_data_chunk_destroy, &mut self.chunk.handle).unwrap();
    }
}

impl Deref for DataChunk {
    type Target = DataChunkRef<'static>;

    fn deref(&self) -> &Self::Target {
        &self.chunk
    }
}

unsafe impl Send for DataChunk {}

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
        assert_eq!(data_chunk.vectors_count()?, 1);

        Ok(())
    }

    #[test]
    fn test_copy_data_chunk() -> crate::Result<()> {
        scalar_callback!(ChunkCopy, i32, |input, result, context, _ud| {
            let dc =
                DataChunk::create_with_allocator(&[i32::logical_type(&context)?], true, Allocator::Context(&context))?;

            let vec = dc.get_vector_at::<i32>(0)?;

            let input = input.get_vector_at::<i32>(0)?;

            unsafe {
                vec.copy_from(&input)?;
            };
            let copy = dc.copy(&context)?;
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
}

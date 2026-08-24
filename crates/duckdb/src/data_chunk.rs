//! Columnar batches exchanged with DuckDB.

use crate::error::check_api_call_no_err;
use crate::ffi;
use crate::logical_type::LogicalType;
use crate::vector::VectorElement;
use crate::{
    Result, check_api_call,
    vector::{Unknown, Vector},
};

/// A columnar batch whose vectors share one row count.
///
/// Chunks may be returned by queries and callbacks or created with
/// [`DataChunk::create`]. Vectors borrow the chunk and cannot outlive it.
///
/// # Example
/// ```
/// use duckdb_rs::{
///     Parameters,
///     environment::Environment,
///     environment::StorageLocation,
/// };
///
/// # fn main() -> duckdb_rs::Result<()> {
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
    /// The DuckDB data-chunk handle.
    pub handle: ffi::duckdb_v2_data_chunk_handle,
    /// Whether dropping this value destroys the handle.
    pub is_owned: bool,
    /// Whether vectors borrowed from this chunk may be modified.
    pub is_writable: bool,
}

impl DataChunk {
    /// Create an empty chunk with one vector per logical type.
    ///
    /// Vectors start as flat storage with zero logical rows. Set `writable` to
    /// allow mutation, then call [`Vector::set_size`] after populating them.
    pub fn create(types: &[LogicalType], writable: bool) -> Result<Self> {
        let handle = check_api_call!(
            ffi::duckdb_v2_data_chunk_create,
            types
                .iter()
                .map(|lt| lt.handle)
                .collect::<Vec<ffi::duckdb_v2_logical_type_handle>>()
                .as_ptr(),
            types.len() as u64,
            RET
        )?;
        Ok(DataChunk {
            handle,
            is_owned: true,
            is_writable: writable,
        })
    }

    /// Return the number of rows shared by the chunk's vectors.
    pub fn row_count(&self) -> Result<usize> {
        let row_count: ffi::idx_t =
            check_api_call!(ffi::duckdb_v2_data_chunk_get_size, self.handle, RET)?;
        Ok(row_count as usize)
    }

    /// Return all vectors as logically untyped borrowed views.
    ///
    /// Narrow a vector with [`Vector::cast`] before typed access.
    pub fn vectors(&self) -> Result<Vec<Vector<'_, Unknown>>> {
        let count = self.vectors_count()?;

        let mut vectors = Vec::with_capacity(count);
        for i in 0..count {
            let vector: ffi::duckdb_v2_vector_handle = check_api_call!(
                ffi::duckdb_v2_data_chunk_get_vector,
                self.handle,
                i as u64,
                RET
            )?;
            vectors.push(Vector::from_handle(&vector, self.is_writable)?);
        }
        Ok(vectors)
    }

    /// Return the number of vectors, which is the column count.
    pub fn vectors_count(&self) -> Result<usize> {
        let out_count: ffi::idx_t =
            check_api_call!(ffi::duckdb_v2_data_chunk_get_vector_count, self.handle, RET)?;
        Ok(out_count as usize)
    }

    /// Return the vector at `index`, narrowed to `T`.
    ///
    /// An out-of-range index or a logical type incompatible with `T` returns an
    /// error.
    pub fn get_vector_at<T: VectorElement>(&self, index: usize) -> Result<Vector<'_, T>> {
        let vector: ffi::duckdb_v2_vector_handle = check_api_call!(
            ffi::duckdb_v2_data_chunk_get_vector,
            self.handle,
            index as u64,
            RET
        )?;
        let vec = Vector::from_handle(&vector, self.is_writable)?;

        vec.cast::<T>()
    }

    #[cfg(feature = "capi-v2-p2")]
    /// Convert the chunk to an Arrow C Data Interface array.
    ///
    /// The caller must invoke the returned array's `release` callback.
    pub fn to_arrow_array(&self, context: &crate::Context) -> Result<ffi::ArrowArray> {
        check_api_call!(
            ffi::duckdb_v2_data_chunk_to_arrow_array,
            **context,
            self.handle,
            RET
        )
    }
}

impl Drop for DataChunk {
    fn drop(&mut self) {
        if self.is_owned {
            check_api_call_no_err!(ffi::duckdb_v2_data_chunk_destroy, &mut self.handle).unwrap();
        }
    }
}

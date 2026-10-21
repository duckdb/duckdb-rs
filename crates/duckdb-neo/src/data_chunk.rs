//! Columnar batches exchanged with DuckDB.

use std::cell::{Cell, OnceCell};
use std::ops::Deref;

use duckdb_loadable_macros::AsRaw;

use crate::connection::{Connection, Context, HasFactory};
use crate::error::check_api_call_no_err;
use crate::ffi;
use crate::links::{InstanceKeepAlive, KeepAlive};
use crate::logical_type::LogicalType;
use crate::vector::{Access, VectorElement};
use crate::{
    Result, check_api_call,
    raw::RawExt,
    vector::{Unknown, Vector},
};

fn check_distinct(indices: &[usize]) -> Result<()> {
    for (i, index) in indices.iter().enumerate() {
        if indices[..i].contains(index) {
            return Err(crate::error::Error::invalid_parameter(format!(
                "vector index {index} was requested more than once"
            )));
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

/// A chunk that DuckDB may change in place, e.g. by flattening nested columns.
///
/// Implemented for owned and borrowed chunks without handing out `&mut DataChunkRef`
/// for an owned chunk: swapping that out would separate the chunk from its keep-alive.
pub(crate) trait DataChunkMut {
    fn chunk_handle(&self) -> ffi::duckdb_v2_data_chunk_handle;

    /// Drop cached views after DuckDB changed the chunk's vectors.
    fn evict_cached(&mut self);
}

impl DataChunkMut for DataChunkRef<'_> {
    fn chunk_handle(&self) -> ffi::duckdb_v2_data_chunk_handle {
        self.handle
    }

    fn evict_cached(&mut self) {
        self.vectors.evict_all();
    }
}

impl DataChunkMut for DataChunk<'_> {
    fn chunk_handle(&self) -> ffi::duckdb_v2_data_chunk_handle {
        self.chunk.handle
    }

    fn evict_cached(&mut self) {
        self.chunk.evict_cached();
    }
}

trait DataChunkLink {
    fn copy_data_chunk(&self, chunk: &DataChunkRef<'_>) -> crate::Result<DataChunk<'static>>;
}

impl DataChunkLink for Connection {
    fn copy_data_chunk(&self, chunk: &DataChunkRef<'_>) -> crate::Result<DataChunk<'static>> {
        let handle = check_api_call!(ffi::duckdb_v2_data_chunk_copy, self.factory()?.raw(), chunk.handle, RET)?;
        Ok(DataChunk::new(handle, true)?.keep_alive(self))
    }
}

impl DataChunkLink for Context {
    fn copy_data_chunk(&self, chunk: &DataChunkRef<'_>) -> crate::Result<DataChunk<'static>> {
        DataChunk::new(
            check_api_call!(ffi::duckdb_v2_data_chunk_copy, self.factory()?.raw(), chunk.handle, RET)?,
            true,
        )
    }
}

/// Input vectors and their row count for scalar and aggregate callbacks, and
/// the child vectors of nested vectors.
///
/// Vectors follow argument (or child) order and borrow DuckDB's data for the
/// duration of the callback. Shared views are cached, so repeated
/// [`Self::get_vector_at`] calls are cheap; mutable access drops the cached view.
#[derive(Debug)]
pub struct VectorCollection {
    pub(crate) handles: Vec<ffi::duckdb_v2_vector_handle>,
    pub(crate) access: Access,
    rows: RowCount,
    /// `'static` because the collection has no lifetime; views are only handed out as `&Vector<'_, _>`.
    cache: Vec<OnceCell<Vector<'static, Unknown>>>,
    /// Whether every view, including nested children, is cached.
    complete: Cell<bool>,
}

/// Where a [`VectorCollection`] reads its row count from.
#[derive(Debug)]
pub(crate) enum RowCount {
    /// A callback batch size; inputs are not writable, so it cannot change.
    Fixed(usize),
    /// The current size of a data chunk.
    Chunk(ffi::duckdb_v2_data_chunk_handle),
    /// The current size of the first vector, or 0 without vectors.
    FirstVector,
}

impl VectorCollection {
    pub(crate) fn new(handles: Vec<ffi::duckdb_v2_vector_handle>, access: Access, rows: RowCount) -> Self {
        let cache = handles.iter().map(|_| OnceCell::new()).collect();
        Self {
            handles,
            access,
            rows,
            cache,
            complete: Cell::new(false),
        }
    }

    fn cached(&self, index: usize) -> Result<&Vector<'_, Unknown>> {
        let slot = &self.cache[index];
        if let Some(vector) = slot.get() {
            return Ok(vector);
        }
        let _ = slot.set(Vector::from_handle(&self.handles[index], Access::Shared)?);
        Ok(slot.get().expect("view was just cached"))
    }

    /// Cache every view, recursively, so nested row types can borrow them infallibly.
    pub(crate) fn cache_all(&self) -> Result<()> {
        if self.complete.get() {
            return Ok(());
        }
        for index in 0..self.handles.len() {
            let vector = self.cached(index)?;
            if vector.view.is_none() {
                return Err(crate::vector::other_not_readable());
            }
            if let Some(children) = vector.children() {
                children.cache_all()?;
            }
        }
        self.complete.set(true);
        Ok(())
    }

    /// Borrow a view cached by [`Self::cache_all`].
    ///
    /// # Panics
    ///
    /// Panics if the view is not cached.
    pub(crate) fn cached_unchecked(&self, index: usize) -> &Vector<'_, Unknown> {
        self.cache[index].get().expect("vector views are cached before reads")
    }

    /// Borrow the cached wrapper of `index` for writing, so nested writers reuse it across rows.
    ///
    /// The wrapper is only validated as `T` when built, so callers must use the
    /// same `T` for an index; nested writers do, since the parent's type fixes it.
    pub(crate) fn cached_mut<T: VectorElement>(&mut self, index: usize) -> Result<&mut Vector<'static, T>> {
        // Writes through the wrapper may evict its own children.
        self.complete.set(false);
        let slot = &mut self.cache[index];
        if slot.get().is_none_or(|vector| vector.access != self.access) {
            let vector = Vector::from_handle(&self.handles[index], self.access)?;
            vector.validate_as::<T>()?;
            *slot = OnceCell::from(vector);
        }
        let vector = slot.get_mut().expect("view was just cached");
        // SAFETY: The repr(C) wrappers differ only in direct and nested markers; the vector was validated as T.
        Ok(unsafe { &mut *(vector as *mut Vector<'static, Unknown> as *mut Vector<'static, T>) })
    }

    /// Drop the cached view of `index`, which a mutable wrapper may reshape.
    fn evict(&mut self, index: usize) {
        self.cache[index].take();
        self.complete.set(false);
    }

    /// Drop every cached view after an operation that may have changed the vectors.
    pub(crate) fn evict_all(&mut self) {
        self.cache.iter_mut().for_each(|slot| drop(slot.take()));
        self.complete.set(false);
    }

    fn check_index(&self, index: usize) {
        assert!(
            index < self.col_count(),
            "vector index {index} is out of range for {} vectors",
            self.col_count()
        );
    }

    /// Return a read-only view of the vector at `index` without a logical type.
    ///
    /// # Panics
    ///
    /// Panics if `index` is out of range.
    pub fn get_untyped_vector_at(&self, index: usize) -> Result<&Vector<'_, Unknown>> {
        self.check_index(index);
        self.cached(index)
    }

    /// Return the vector at `index` without a logical type, borrowed mutably.
    ///
    /// # Panics
    ///
    /// Panics if `index` is out of range.
    pub fn get_untyped_vector_at_mut(&mut self, index: usize) -> Result<Vector<'_, Unknown>> {
        self.check_index(index);
        self.evict(index);
        Vector::from_handle(&self.handles[index], self.access)
    }

    /// Return a read-only view of the vector at `index`, narrowed to `T`.
    ///
    /// A logical type incompatible with `T` returns an error. Use
    /// [`Self::get_vector_at_mut`] to flatten the vector.
    ///
    /// # Panics
    ///
    /// Panics if `index` is out of range.
    pub fn get_vector_at<T: VectorElement>(&self, index: usize) -> Result<&Vector<'_, T>> {
        self.get_untyped_vector_at(index)?.cast_ref::<T>()
    }

    /// Return the vector at `index`, narrowed to `T`, borrowed mutably.
    ///
    /// The exclusive borrow allows reshaping the vector, e.g. with
    /// [`Vector::flatten`]. Writing requires writable vectors.
    ///
    /// # Panics
    ///
    /// Panics if `index` is out of range.
    pub fn get_vector_at_mut<T: VectorElement>(&mut self, index: usize) -> Result<Vector<'_, T>> {
        self.get_untyped_vector_at_mut(index)?.cast::<T>()
    }

    /// Return the vectors at distinct `indices`, borrowed mutably at the same time.
    ///
    /// Duplicate indices return an error. Narrow each vector with [`Vector::cast`].
    ///
    /// # Panics
    ///
    /// Panics if an index is out of range.
    pub fn get_vector_array_mut<const N: usize>(&mut self, indices: [usize; N]) -> Result<[Vector<'_, Unknown>; N]> {
        for index in indices {
            self.check_index(index);
        }
        check_distinct(&indices)?;
        for index in indices {
            self.evict(index);
        }
        collect_array(indices.map(|index| Vector::from_handle(&self.handles[index], self.access)))
    }

    /// Return the number of vectors, which is the column count.
    pub fn col_count(&self) -> usize {
        self.handles.len()
    }

    /// Return the number of rows, read from DuckDB on every call.
    ///
    /// This is the batch size for callback inputs, the chunk size for a
    /// [`DataChunkRef`], and the first child's size for child vectors.
    pub fn row_count(&self) -> Result<usize> {
        let rows: ffi::idx_t = match self.rows {
            RowCount::Fixed(rows) => return Ok(rows),
            RowCount::Chunk(chunk) => check_api_call!(ffi::duckdb_v2_data_chunk_get_size, chunk, RET)?,
            RowCount::FirstVector => match self.handles.first() {
                Some(vector) => check_api_call!(ffi::duckdb_v2_vector_get_size, *vector, RET)?,
                None => 0,
            },
        };
        Ok(rows as usize)
    }

    /// Wrap the callback's input vectors in a [`DataChunk`] without copying.
    ///
    /// The chunk's vectors reference the input's storage, so the chunk borrows
    /// this collection and cannot outlive the callback. It is writable only when
    /// the input is. Use [`DataChunkRef::copy`] to keep the data longer.
    pub fn to_data_chunk<'a, C: HasFactory + 'a>(&'a mut self, link: &'a C) -> crate::Result<DataChunk<'a>> {
        let mut logical_types = Vec::with_capacity(self.col_count());
        for index in 0..self.col_count() {
            logical_types.push(self.cached(index)?.logical_type().clone());
        }
        self.evict_all();

        let type_handles = logical_types
            .iter()
            .map(|logical_type| logical_type.handle)
            .collect::<Vec<_>>();
        let handle = check_api_call!(
            ffi::duckdb_v2_data_chunk_create,
            link.factory()?.raw(),
            type_handles.as_ptr(),
            type_handles.len() as u64,
            RET
        )?;
        let chunk: DataChunk<'a> = DataChunk::new(handle, self.access == Access::Writable)?;

        for (i, handle) in self.handles.iter().enumerate() {
            let chunk_handle = chunk.get_vector_handle_at(i);

            check_api_call!(ffi::duckdb_v2_vector_reference, chunk_handle, *handle)?;
        }
        Ok(chunk)
    }
}

/// The child vectors of a [`Vector`], borrowed mutably from it.
///
/// Readers trust the children validated when the parent was cast, so this
/// never hands out `&mut VectorCollection`, which could be swapped with another
/// vector's children. Read-only access goes through [`Deref`].
#[derive(Debug)]
pub struct ChildrenMut<'a>(pub(crate) &'a mut VectorCollection);

impl Deref for ChildrenMut<'_> {
    type Target = VectorCollection;

    fn deref(&self) -> &VectorCollection {
        self.0
    }
}

impl ChildrenMut<'_> {
    /// See [`VectorCollection::get_untyped_vector_at_mut`].
    pub fn get_untyped_vector_at_mut(&mut self, index: usize) -> Result<Vector<'_, Unknown>> {
        self.0.get_untyped_vector_at_mut(index)
    }

    /// See [`VectorCollection::get_vector_at_mut`].
    pub fn get_vector_at_mut<T: VectorElement>(&mut self, index: usize) -> Result<Vector<'_, T>> {
        self.0.get_vector_at_mut(index)
    }

    /// See [`VectorCollection::get_vector_array_mut`].
    pub fn get_vector_array_mut<const N: usize>(&mut self, indices: [usize; N]) -> Result<[Vector<'_, Unknown>; N]> {
        self.0.get_vector_array_mut(indices)
    }

    /// See [`VectorCollection::to_data_chunk`].
    pub fn to_data_chunk<'a, C: HasFactory + 'a>(&'a mut self, link: &'a C) -> Result<DataChunk<'a>> {
        self.0.to_data_chunk(link)
    }
}

/// A non-owning view of a DuckDB data chunk.
///
/// Provides vector access for both owned [`DataChunk`] values and borrowed
/// callback chunks. Vectors borrowed from this view cannot outlive it.
#[derive(AsRaw)]
pub struct DataChunkRef<'a> {
    pub(crate) handle: ffi::duckdb_v2_data_chunk_handle,
    vectors: VectorCollection,
    _marker: std::marker::PhantomData<&'a ()>,
}

impl std::fmt::Debug for DataChunkRef<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataChunkRef")
            .field("handle", &self.handle)
            .finish_non_exhaustive()
    }
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
/// let instance = env.open(StorageLocation::InMemory)?;
/// let conn = instance.connect()?;
/// let mut statements = conn.parse("SELECT * FROM (VALUES (10), (20))")?;
/// let statement = statements.next().expect("expected a statement")?;
/// let mut result = conn.query(statement, Parameters::None)?;
/// let chunk = result.next_chunk()?.expect("expected rows");
///
/// let values = chunk.get_vector_at::<i32>(0)?;
/// assert_eq!(chunk.row_count()?, 2);
/// assert_eq!(values.get(0)?, Some(&10));
/// # Ok(())
/// # }
/// ```
///
/// Only copies return `DataChunk<'static>`; [`Self::create`] borrows its link,
/// and [`VectorCollection::to_data_chunk`] borrows its link and source for `'a`.
///
/// The inner [`DataChunkRef`] is only reachable by shared reference, so it
/// cannot be swapped away from the chunk that owns and keeps it alive:
///
/// ```compile_fail,E0596
/// # use duckdb_neo::{DuckDBType, data_chunk::DataChunk, environment::{Environment, StorageLocation}};
/// # fn main() -> duckdb_neo::Result<()> {
/// # let env = Environment::new()?;
/// # let conn = env.open(StorageLocation::InMemory)?.connect()?;
/// let mut a = DataChunk::create(&conn, &[i32::logical_type(&conn)?], true)?;
/// let mut b = DataChunk::create(&conn, &[i32::logical_type(&conn)?], true)?;
/// std::mem::swap(&mut *a, &mut *b);
/// # Ok(())
/// # }
/// ```
///
/// ```compile_fail,E0614
/// # use duckdb_neo::data_chunk::DataChunk;
/// fn raw_handle(chunk: DataChunk<'_>) {
///     let raw = **chunk;
/// }
/// ```
#[derive(Debug)]
pub struct DataChunk<'a> {
    pub(crate) chunk: DataChunkRef<'a>,
    /// Keeps the instance behind a connection allocator alive; dropped after the chunk is destroyed.
    instance: Option<KeepAlive>,
}

impl<'a> DataChunkRef<'a> {
    pub(crate) fn new(handle: ffi::duckdb_v2_data_chunk_handle, is_writable: bool) -> Result<Self> {
        let count: ffi::idx_t = check_api_call!(ffi::duckdb_v2_data_chunk_get_vector_count, handle, RET)?;
        let mut handles = Vec::with_capacity(count as usize);
        for index in 0..count {
            handles.push(check_api_call!(
                ffi::duckdb_v2_data_chunk_get_vector,
                handle,
                index,
                RET
            )?);
        }
        let access = if is_writable {
            Access::Writable
        } else {
            Access::Exclusive
        };

        Ok(Self {
            handle,
            vectors: VectorCollection::new(handles, access, RowCount::Chunk(handle)),
            _marker: std::marker::PhantomData,
        })
    }

    /// Return the maximum number of rows the chunk can hold.
    pub fn capacity(&self) -> Result<usize> {
        check_api_call!(ffi::duckdb_v2_data_chunk_get_capacity, self.raw(), RET).map(|x| x as usize)
    }

    /// Return the number of rows shared by the chunk's vectors.
    pub fn row_count(&self) -> Result<usize> {
        self.vectors.row_count()
    }

    /// Return the number of vectors, which is the column count.
    pub fn col_count(&self) -> usize {
        self.vectors.col_count()
    }

    /// Return a read-only view of the vector at `index`, narrowed to `T`.
    ///
    /// A logical type incompatible with `T` returns an error. The view is
    /// cached until the vector is borrowed mutably. Use
    /// [`Self::get_vector_at_mut`] to write or flatten the vector:
    ///
    /// ```compile_fail,E0596
    /// # use duckdb_neo::{DuckDBType, data_chunk::DataChunk, environment::{Environment, StorageLocation}};
    /// # fn main() -> duckdb_neo::Result<()> {
    /// # let env = Environment::new()?;
    /// # let conn = env.open(StorageLocation::InMemory)?.connect()?;
    /// let chunk = DataChunk::create(&conn, &[i32::logical_type(&conn)?], true)?;
    /// chunk.get_vector_at::<i32>(0)?.set_size(1)?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Panics
    ///
    /// Panics if `index` is out of range.
    pub fn get_vector_at<T: VectorElement>(&self, index: usize) -> Result<&Vector<'_, T>> {
        self.vectors.get_vector_at(index)
    }

    /// Return the vector at `index`, narrowed to `T`, borrowed mutably.
    ///
    /// The exclusive borrow allows reshaping the vector, e.g. with
    /// [`Vector::flatten`], and writing to it when the chunk is writable. A
    /// logical type incompatible with `T` returns an error.
    ///
    /// The borrow rules out other views of the chunk while the vector is alive:
    ///
    /// ```compile_fail,E0502
    /// # use duckdb_neo::{DuckDBType, data_chunk::DataChunk, environment::{Environment, StorageLocation}};
    /// # fn main() -> duckdb_neo::Result<()> {
    /// # let env = Environment::new()?;
    /// # let conn = env.open(StorageLocation::InMemory)?.connect()?;
    /// let mut chunk = DataChunk::create(&conn, &[i32::logical_type(&conn)?], true)?;
    /// let shared = chunk.get_vector_at::<i32>(0)?;
    /// let mut exclusive = chunk.get_vector_at_mut::<i32>(0)?;
    /// exclusive.write(0, Some(1))?;
    /// let _ = shared.get(0)?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Panics
    ///
    /// Panics if `index` is out of range.
    pub fn get_vector_at_mut<T: VectorElement>(&mut self, index: usize) -> Result<Vector<'_, T>> {
        self.vectors.get_vector_at_mut(index)
    }

    /// Return the vectors at distinct `indices`, borrowed mutably at the same time.
    ///
    /// Duplicate indices return an error. Narrow each vector with [`Vector::cast`].
    ///
    /// # Panics
    ///
    /// Panics if an index is out of range.
    pub fn get_vector_array_mut<const N: usize>(&mut self, indices: [usize; N]) -> Result<[Vector<'_, Unknown>; N]> {
        self.vectors.get_vector_array_mut(indices)
    }

    pub(crate) fn get_vector_handle_at(&self, index: usize) -> ffi::duckdb_v2_vector_handle {
        self.vectors.check_index(index);
        self.vectors.handles[index]
    }

    /// Deep-copy this chunk into a new, writable chunk owned by `link`'s connection or context.
    ///
    /// A copy made through a [`Connection`] keeps its instance alive. A copy made through a [`Context`]
    /// may be kept in callback state but must not outlive the instance, for example in a `static`.
    #[allow(private_bounds)]
    pub fn copy<C: DataChunkLink>(&self, link: &C) -> Result<DataChunk<'static>> {
        link.copy_data_chunk(self)
    }
}

impl<'a> DataChunk<'a> {
    pub(crate) fn new(handle: ffi::duckdb_v2_data_chunk_handle, is_writable: bool) -> Result<Self> {
        let chunk = match DataChunkRef::new(handle, is_writable) {
            Ok(chunk) => chunk,
            Err(err) => {
                let mut handle = handle;
                let _ = check_api_call_no_err!(ffi::duckdb_v2_data_chunk_destroy, &mut handle);
                return Err(err);
            }
        };
        Ok(Self { chunk, instance: None })
    }

    fn keep_alive(mut self, link: &impl InstanceKeepAlive) -> Self {
        self.instance = link.keep_alive();
        self
    }

    /// Create an empty chunk; with `writable`, call [`Vector::set_size`] before writing rows.
    /// Without it, vectors only accept storage references through unsafe [`Vector::reference`].
    pub fn create<C: HasFactory>(link: &'a C, types: impl AsRef<[LogicalType]>, writable: bool) -> Result<Self> {
        let types = types.as_ref();
        let len = types.len();
        let type_handles = types
            .iter()
            .map(|lt| lt.handle)
            .collect::<Vec<ffi::duckdb_v2_logical_type_handle>>();

        check_api_call!(
            ffi::duckdb_v2_data_chunk_create,
            link.factory()?.raw(),
            type_handles.as_ptr(),
            len as u64,
            RET
        )
        .map(|handle| DataChunk::new(handle, writable))?
    }

    /// See [`DataChunkRef::get_vector_at_mut`].
    pub fn get_vector_at_mut<T: VectorElement>(&mut self, index: usize) -> Result<Vector<'_, T>> {
        self.chunk.get_vector_at_mut(index)
    }

    /// See [`DataChunkRef::get_vector_array_mut`].
    pub fn get_vector_array_mut<const N: usize>(&mut self, indices: [usize; N]) -> Result<[Vector<'_, Unknown>; N]> {
        self.chunk.get_vector_array_mut(indices)
    }

    /// Reset the row count to zero and drop cached views, preserving writability.
    pub fn clear(&mut self) -> Result<()> {
        self.evict_cached();
        check_api_call!(ffi::duckdb_v2_data_chunk_clear, self.handle)
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

// SAFETY: owned chunks have no thread affinity and their allocators free thread-safely.
// Borrowed chunks keep their source collection exclusive through the mutable borrow.
unsafe impl Send for DataChunk<'_> {}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {

    use crate::AsRaw;
    use crate::builder_helpers::scalar_callback;
    use crate::scalar::ScalarFunctionBuilder;
    use crate::signature::{FunctionParameter, SignatureBuilder};
    use crate::types::DuckDBType;
    use crate::{
        data_chunk::DataChunk,
        environment::{Environment, StorageLocation},
    };

    #[test]
    fn test_data_chunk_create_with_connection() -> crate::Result<()> {
        let env = Environment::new()?;
        let instance = env.open(StorageLocation::InMemory)?;
        let conn = instance.connect()?;

        let data_chunk = DataChunk::create(&conn, &[i32::logical_type(&conn)?], true)?;

        assert_eq!(data_chunk.row_count()?, 0);
        assert_eq!(data_chunk.col_count(), 1);
        assert_eq!(data_chunk.capacity()?, 2048);
        // SAFETY: both handles are borrowed while the chunk is alive and are never destroyed.
        assert_eq!(unsafe { crate::AsRaw::get_raw(&*data_chunk) }, data_chunk.chunk.handle);
        // SAFETY: this only compares the borrowed handle and does not mutate the chunk.
        assert_eq!(unsafe { data_chunk.get_raw() }, data_chunk.chunk.handle);

        Ok(())
    }

    #[test]
    fn test_data_chunk_create_writability() -> crate::Result<()> {
        let env = Environment::new()?;
        let conn = env.open(StorageLocation::InMemory)?.connect()?;
        let types = [i32::logical_type(&conn)?];
        let mut writable = DataChunk::create(&conn, &types, true)?;
        let mut vector = writable.get_vector_at_mut::<i32>(0)?;
        assert!(vector.write(0, Some(42)).is_err());
        vector.set_size(1)?;
        vector.write(0, Some(42))?;
        assert_eq!(vector.get(0)?, Some(&42));

        let mut read_only = DataChunk::create(&conn, &types, false)?;
        let mut vector = read_only.get_vector_at_mut::<i32>(0)?;
        assert!(!vector.is_writable());
        assert!(vector.set_size(1).is_err());
        assert!(vector.write(0, Some(42)).is_err());
        Ok(())
    }

    #[test]
    fn test_copy_data_chunk() -> crate::Result<()> {
        scalar_callback!(ChunkCopy, i32, |input, result, context, _ud| {
            let mut dc = DataChunk::create(&context, &[i32::logical_type(&context)?], true)?;

            let vec = dc.get_vector_at_mut::<i32>(0)?;

            let input = input.get_vector_at::<i32>(0)?;

            // SAFETY: the callback input outlives dc and is not mutated while dc references it.
            unsafe {
                vec.reference(input)?;
            };
            let copy = dc.copy(context)?;
            let vec = copy.get_vector_at::<i32>(0)?;
            let mut result = result;
            result.set_size(vec.len())?;

            for (index, value) in vec.iter()?.enumerate() {
                result.write(index, value.copied())?;
            }

            Ok(())
        });

        let env = Environment::new()?;
        let instance = env.open(StorageLocation::InMemory)?;
        let conn = instance.connect()?;

        ScalarFunctionBuilder::new(
            "chunk_copy",
            SignatureBuilder::new()
                .param(FunctionParameter::new::<i32>("in"))
                .return_type(i32::logical_type(&conn)?),
            ChunkCopy,
        )
        .register(&conn)?;

        let mut query = conn.query("SELECT chunk_copy(unnest([1,2,3,4]))", crate::Parameters::None)?;

        let mut values = Vec::new();
        while let Some(chunk) = query.next_chunk()? {
            values.extend(chunk.get_vector_at::<i32>(0)?.iter()?.map(|value| value.copied()));
        }
        assert_eq!(values, vec![Some(1), Some(2), Some(3), Some(4)]);

        Ok(())
    }

    #[test]
    fn test_unreadable_nested_child() -> crate::Result<()> {
        use crate::types::List;

        let env = Environment::new()?;
        let conn = env.open(StorageLocation::InMemory)?.connect()?;
        let mut chunk = DataChunk::create(&conn, &[Vec::<i64>::logical_type(&conn)?], true)?;
        let mut list = chunk.get_vector_at_mut::<List<i64>>(0)?;
        list.set_size(1)?;
        list.write(0, Some(vec![Some(0), Some(0)]))?;
        list.children_mut()
            .unwrap()
            .get_vector_at_mut::<i64>(0)?
            .make_sequence(10, 1, 2)?;

        let error = list.get(0).expect_err("sequence child must not read as NULL");
        assert!(error.to_string().contains("OTHER vectors must be flattened"));
        assert!(list.iter().is_err());
        list.children_mut().unwrap().get_vector_at_mut::<i64>(0)?.flatten()?;
        let values = list
            .get(0)?
            .expect("non-NULL list")
            .iter()
            .map(|value| value.copied())
            .collect::<Vec<_>>();
        assert_eq!(values, vec![Some(10), Some(11)]);
        Ok(())
    }

    #[test]
    fn test_vector_access() -> crate::Result<()> {
        let env = Environment::new()?;
        let conn = env.open(StorageLocation::InMemory)?.connect()?;
        let mut chunk = DataChunk::create(&conn, &[i64::logical_type(&conn)?, i64::logical_type(&conn)?], true)?;
        assert!(chunk.get_vector_array_mut([0, 0]).is_err());
        let [_, sequence] = chunk.get_vector_array_mut([0, 1])?;
        sequence.cast::<i64>()?.make_sequence(0, 1, 4)?;

        // Shared views are cached until the vector is borrowed mutably.
        let shared = chunk.get_vector_at::<i64>(1)?;
        assert!(!shared.is_writable());
        assert!(std::ptr::eq(shared, chunk.get_vector_at::<i64>(1)?));
        chunk.get_vector_at_mut::<i64>(1)?.flatten()?;
        assert_eq!(chunk.get_vector_at::<i64>(1)?.get(3)?, Some(&3));
        Ok(())
    }

    #[test]
    fn test_vector_index_out_of_range_panics() -> crate::Result<()> {
        let env = Environment::new()?;
        let conn = env.open(StorageLocation::InMemory)?.connect()?;
        let mut chunk = DataChunk::create(&conn, &[i64::logical_type(&conn)?], true)?;
        let panics = |f: &mut dyn FnMut()| std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)).is_err();
        assert!(panics(&mut || drop(chunk.get_vector_at::<i64>(1))));
        assert!(panics(&mut || drop(chunk.get_vector_at_mut::<i64>(1))));
        assert!(panics(&mut || drop(chunk.get_vector_array_mut([0, 1]))));
        Ok(())
    }

    #[test]
    fn test_data_chunk_clear() -> crate::Result<()> {
        let env = Environment::new()?;
        let conn = env.open(StorageLocation::InMemory)?.connect()?;

        let mut chunk = DataChunk::create(&conn, &[String::logical_type(&conn)?], true)?;
        let mut vec = chunk.get_vector_at_mut::<String>(0)?;

        vec.set_size(10)?;

        drop(vec);

        chunk.clear()?;

        let vec = chunk.get_vector_at_mut::<String>(0)?;

        assert!(vec.is_empty());

        Ok(())
    }
}

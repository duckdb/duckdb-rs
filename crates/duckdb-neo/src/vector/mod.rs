//! A generic, type-inferred DuckDB vector API.
//!
//! # Motivation
//!
//! DuckDB does not have a single physical vector layout. A vector's storage
//! *kind* (`duckdb_v2_vector_type`) can be:
//!
//! * `FLAT`       — one physical slot per logical row (the "normal" layout).
//! * `CONSTANT`   — a single physical value that applies to every row.
//! * `DICTIONARY` — a child vector plus a selection vector mapping each logical row to a child index.
//! * `OTHER`      — FSST / SEQUENCE / SHREDDED; must be `flatten`-ed first.
//!
//! Reading correctly therefore means honouring the *selection vector* returned
//! by `duckdb_v2_vector_get_view`: logical row `i` reads physical slot `sel[i]`
//! (or `0` for `CONSTANT`), not `data[i]`.
//!
//! # Approach
//!
//! [`Vector`] carries only its chunk lifetime and logical element type. Storage
//! representation and writability remain runtime properties reported by DuckDB.
//!
//! A column returned by [`crate::data_chunk::DataChunkRef::get_vector_at`] is
//! narrowed to its logical type before reading:
//!
//! ```
//! # use duckdb_neo::{environment::{Environment, StorageLocation}, Parameters};
//! # fn main() -> duckdb_neo::Result<()> {
//! # let env = Environment::new()?;
//! # let conn = env.open(StorageLocation::InMemory)?.connect()?;
//! # let mut result = conn.query("SELECT 1::INTEGER", Parameters::None)?;
//! # let chunk = result.next_chunk()?.expect("one result chunk");
//! let vector = chunk.get_vector_at::<i32>(0)?;
//! let reader = vector.reader()?;
//! for value in reader.iter() {
//!     assert_eq!(value, Some(&1));
//! }
//! # Ok(())
//! # }
//! ```
//!
//! Element types plug in through the [`VectorElement`] trait. Each type defines its
//! own borrowed row representation: scalars yield references, while nested
//! values yield zero-copy handles into their child vectors.

use std::{
    cell::{Cell, RefCell},
    collections::HashMap,
    marker::PhantomData,
    rc::Rc,
};

use crate::builder_helpers::ffi_enum_redeclaration;
use crate::error::DuckDBError;
use crate::raw::RawExt;
use crate::{
    Result, check_api_call,
    connection::HasFactory,
    data_chunk::{ChildrenMut, RowCount, VectorCollection},
    error::Error,
    ffi,
    logical_type::{LogicalType, LogicalTypeID},
    types::DecimalSignature,
    value::Value,
};

mod element;
use duckdb_loadable_macros::{AsRaw, FromRaw};
pub use element::*;

/// The next unwritten child element of every `LIST` and `MAP` vector in one wrapper tree.
///
/// The root wrapper (a chunk, a callback's inputs, or a standalone vector) owns the store, and
/// every descendant shares it. Offsets are keyed by handle, which DuckDB keeps stable for a
/// vector until its parent's storage is replaced, so they survive evicted and rebuilt wrappers.
#[derive(Debug, Default)]
pub(crate) struct ChildOffsets(RefCell<HashMap<ffi::duckdb_v2_vector_handle, Rc<Cell<usize>>>>);

impl ChildOffsets {
    fn cell(&self, handle: ffi::duckdb_v2_vector_handle) -> Rc<Cell<usize>> {
        self.0.borrow_mut().entry(handle).or_default().clone()
    }

    /// Restart every offset, e.g. after the chunk was cleared.
    pub(crate) fn reset(&self) {
        self.0.borrow().values().for_each(|offset| offset.set(0));
    }
}

/// Logical-type parameters read once per vector, so row access skips the FFI lookup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TypeMetadata {
    None,
    Array {
        size: usize,
    },
    Decimal(DecimalSignature),
    /// `STRUCT` or `TUPLE`, whose fields share the parent's row count.
    Struct,
    /// `LIST` or `MAP`, whose rows take consecutive elements of the child.
    List,
}

impl TypeMetadata {
    fn read(logical_type: &LogicalType) -> Result<Self> {
        Ok(match logical_type.id()? {
            LogicalTypeID::Array => Self::Array {
                size: crate::types::array::array_size(logical_type)?,
            },
            LogicalTypeID::Decimal => Self::Decimal(DecimalSignature::from_logical_type(logical_type)?),
            LogicalTypeID::Struct | LogicalTypeID::Tuple => Self::Struct,
            LogicalTypeID::List | LogicalTypeID::Map => Self::List,
            _ => Self::None,
        })
    }

    #[cfg(feature = "rust_decimal")]
    /// The width and scale of a `DECIMAL` vector, read once when the vector was created.
    pub(crate) fn decimal_signature(&self) -> Option<DecimalSignature> {
        match self {
            TypeMetadata::Decimal(signature) => Some(*signature),
            _ => None,
        }
    }
}

ffi_enum_redeclaration! {
    pub enum StorageKind <- ffi::DUCKDB_V2_VECTOR_TYPE {
    /// One physical slot per row.
    Flat = DUCKDB_V2_VECTOR_TYPE_FLAT,
    /// One physical slot shared by every row.
    Constant = DUCKDB_V2_VECTOR_TYPE_CONSTANT,
    /// A selection vector maps rows into a child vector.
    Dictionary = DUCKDB_V2_VECTOR_TYPE_DICTIONARY,
    /// FSST / SEQUENCE / SHREDDED — must be flattened before reading.
    Other = DUCKDB_V2_VECTOR_TYPE_OTHER,
    }
}

pub use crate::bytes::DuckDBBytes;

/// A borrowed byte arena, valid until its vector is resized, flattened, or destroyed.
#[derive(Debug, FromRaw, AsRaw)]
pub struct Arena {
    pub(crate) handle: ffi::duckdb_v2_arena_handle,
}

impl Arena {
    /// Reserve uninitialized bytes in the arena.
    #[inline]
    pub fn allocate(&self, len: usize) -> Result<*mut u8> {
        check_api_call!(ffi::duckdb_v2_arena_allocate, self.raw(), len as u64, RET)
    }

    /// Encode bytes, storing short values inline and copying longer values into this arena.
    pub fn add_bytes<'a>(&'a self, value: &[u8]) -> Result<DuckDBBytes<'a>> {
        DuckDBBytes::new(self, value)
    }

    /// Concatenate byte slices directly into inline storage or one arena allocation.
    ///
    /// The combined length must fit in a `u32`.
    /// This does not validate UTF-8 or other logical-type encodings.
    #[inline]
    pub fn add_parts<'a>(&'a self, parts: &[&[u8]]) -> Result<DuckDBBytes<'a>> {
        DuckDBBytes::from_parts(parts, || Ok(self))
    }

    /// Concatenate UTF-8 string slices into inline storage or one arena allocation.
    ///
    /// The combined byte length must fit in a `u32`.
    #[inline]
    pub fn add_strings<'a>(&'a self, parts: &[&str]) -> Result<DuckDBBytes<'a>> {
        DuckDBBytes::from_parts(parts, || Ok(self))
    }

    /// Encode UTF-8 text, storing short values inline and copying longer values into this arena.
    pub fn add_str<'a>(&'a self, value: &str) -> Result<DuckDBBytes<'a>> {
        self.add_bytes(value.as_bytes())
    }
}

mod combined;
mod reader;
mod view;
mod writer;

#[doc(hidden)]
pub use combined::VectorReaderTuple;
pub use combined::{CombinedVectorIterator, EnumeratedCombinedVectorIterator};
pub use reader::{Selection, VectorReader, VectorReaderIter};
use view::VectorView;
pub use writer::VectorWriter;

/// The length of DuckDB's shared constant-vector selection, `STANDARD_VECTOR_SIZE`.
pub const CONSTANT_SELECTION_LEN: usize = 2048;

/// How a [`Vector`] wrapper may touch the underlying DuckDB vector.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Access {
    /// Other wrappers of the same vector may exist, so its state must not change.
    Shared,
    /// The only wrapper; may reshape the vector (e.g. flatten) but not write rows.
    Exclusive,
    /// The only wrapper of writable output.
    Writable,
}

/// A typed view of a DuckDB vector borrowed from its owning data chunk.
///
/// Storage representation and writability are runtime properties. Casting only
/// changes the logical element type and preserves the chunk lifetime.
/// Read rows through [`Self::reader`] and write them through [`Self::writer`].
///
/// ```compile_fail,E0599
/// use duckdb_neo::vector::Vector;
///
/// fn read(vector: &Vector<'_, i32>) {
///     let _ = vector.get(0);
/// }
/// ```
///
/// ```compile_fail,E0599
/// use duckdb_neo::vector::Vector;
///
/// fn write(vector: &mut Vector<'_, i32>) {
///     let _ = vector.write(0, Some(42));
/// }
/// ```
// `repr(C)` fixes field order; the nested `VectorView<T>` is also `repr(C)`.
#[repr(C)]
#[derive(Debug)]
pub struct Vector<'a, T: VectorElement> {
    pub(crate) handle: ffi::duckdb_v2_vector_handle,
    pub(crate) view: Option<VectorView<T>>,
    pub(crate) access: Access,

    logical_type: LogicalType,
    kind: StorageKind,
    len: usize,
    heap: Option<Arena>,
    /// Child vectors of nested types; `None` for vectors without children.
    children: Option<VectorCollection>,
    metadata: TypeMetadata,
    /// The store of the wrapper tree, shared with children and rebuilt wrappers.
    offsets: Rc<ChildOffsets>,
    /// This vector's entry in `offsets`; `None` unless it is a `LIST` or `MAP`.
    child_offset: Option<Rc<Cell<usize>>>,
    _chunk: PhantomData<&'a ()>,
    _type: PhantomData<T>,
}

impl<'a, T: VectorElement> Vector<'a, T> {
    /// The handle must remain valid for the lifetime of the owning chunk.
    pub(crate) fn from_handle(handle: &ffi::duckdb_v2_vector_handle, access: Access) -> Result<Self> {
        Self::from_handle_in(handle, access, &Rc::default())
    }

    /// Wrap a handle of the wrapper tree that owns `offsets`.
    pub(crate) fn from_handle_in(
        handle: &ffi::duckdb_v2_vector_handle,
        access: Access,
        offsets: &Rc<ChildOffsets>,
    ) -> Result<Self> {
        let logical_type = check_api_call!(ffi::duckdb_v2_vector_get_logical_type, *handle, RET)
            .map(|handle| LogicalType { handle })?;

        let kind =
            check_api_call!(ffi::duckdb_v2_vector_get_vector_type, *handle, RET).map(StorageKind::try_from)??;
        let len = check_api_call!(ffi::duckdb_v2_vector_get_size, *handle, RET).map(|x| x as usize)?;

        let view = Self::acquire_view(*handle, kind)?;
        let params = TypeMetadata::read(&logical_type)?;

        let child_count = check_api_call!(ffi::duckdb_v2_vector_get_child_count, *handle, RET).map(|x| x as usize)?;
        let mut child_handles = Vec::with_capacity(child_count as usize);
        for index in 0..child_count {
            child_handles.push(check_api_call!(
                ffi::duckdb_v2_vector_get_child,
                *handle,
                index as u64,
                RET
            )?);
        }
        let children = (!child_handles.is_empty())
            .then(|| VectorCollection::with_offsets(child_handles, access, RowCount::FirstVector, offsets.clone()));

        let vector = Self {
            handle: *handle,
            logical_type,
            kind,
            len: len as usize,
            view,
            access,
            heap: None,
            children,
            metadata: params,
            offsets: offsets.clone(),
            child_offset: (params == TypeMetadata::List).then(|| offsets.cell(*handle)),
            _chunk: PhantomData,
            _type: PhantomData,
        };
        vector.validate_as::<T>()?;
        Ok(vector)
    }
}

impl<'a> Vector<'a, Unknown> {
    /// Validate and attach a logical element type.
    pub fn cast<T: VectorElement>(self) -> Result<Vector<'a, T>> {
        self.validate_as::<T>()?;
        Ok(self.cast_unchecked())
    }

    /// Validate and attach a logical element type to a borrowed vector.
    pub fn cast_ref<T: VectorElement>(&self) -> Result<&Vector<'a, T>> {
        self.validate_as::<T>()?;
        // SAFETY: T was validated above.
        Ok(unsafe { self.cast_ref_unchecked() })
    }

    /// # Safety
    /// This vector must have been validated as `T`.
    pub(crate) unsafe fn cast_ref_unchecked<T: VectorElement>(&self) -> &Vector<'a, T> {
        // SAFETY: Both repr(C) wrappers differ only in markers, including inside Option<VectorView<T>>.
        unsafe { &*(self as *const Vector<'a, Unknown> as *const Vector<'a, T>) }
    }

    pub(crate) unsafe fn cast_ref_unchecked_mut<T: VectorElement>(&mut self) -> &mut Vector<'a, T> {
        // SAFETY: Both repr(C) wrappers differ only in markers, including inside Option<VectorView<T>>.
        unsafe { &mut *(self as *mut Vector<'a, Unknown> as *mut Vector<'a, T>) }
    }
}

impl<'a, T: VectorElement> Vector<'a, T> {
    /// Return the number of logical rows.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Return whether the vector contains no rows.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Return the vector's runtime storage representation.
    pub fn storage_kind(&self) -> StorageKind {
        self.kind
    }

    /// Return the vector's logical type.
    pub fn logical_type(&self) -> &LogicalType {
        &self.logical_type
    }

    /// Return whether this vector is writable output that was borrowed mutably.
    pub fn is_writable(&self) -> bool {
        self.access == Access::Writable
    }

    /// Return the vector's child vectors, or `None` if it has no children.
    pub fn children(&self) -> Option<&VectorCollection> {
        self.children.as_ref()
    }

    /// Prepare exclusive write access and allocate a writable validity mask.
    ///
    /// Fails unless this is writable output with flat storage. Resize the vector
    /// before acquiring its writer.
    pub fn writer(&mut self) -> Result<VectorWriter<'_, T>> {
        VectorWriter::new(self)
    }

    fn acquire_view<U: VectorElement>(
        handle: ffi::duckdb_v2_vector_handle,
        kind: StorageKind,
    ) -> Result<Option<VectorView<U>>> {
        if kind == StorageKind::Other {
            return Ok(None);
        }

        check_api_call!(ffi::duckdb_v2_vector_get_view, handle, RET).map(|view| {
            Some(VectorView {
                view,
                kind,
                validity_writable: false,
                _marker: PhantomData,
            })
        })
    }

    /// Return the writable validity mask, allocating it on first use.
    ///
    /// The pointer is cached in the view, which every buffer-replacing operation reacquires.
    pub(crate) fn writable_validity(&mut self) -> Result<*mut u64> {
        let handle = self.handle;
        let view = self.view.as_mut().ok_or_else(not_flat)?;
        if !view.validity_writable {
            view.view.validity = check_api_call!(ffi::duckdb_v2_vector_flat_get_validity_mutable, handle, RET)?;
            view.validity_writable = true;
        }
        Ok(view.view.validity.cast_mut())
    }

    /// Refresh the vector's view and mutable buffers after a state change, e.g. set_size.
    fn refresh_buffers(&mut self) -> Result<()> {
        self.view = Self::acquire_view(self.handle, self.kind)?;
        self.heap = None;
        if let Some(children) = &mut self.children {
            children.evict_all();
        }
        Ok(())
    }

    pub(crate) fn cast_unchecked<U: VectorElement>(self) -> Vector<'a, U> {
        Vector {
            handle: self.handle,
            logical_type: self.logical_type,
            kind: self.kind,
            len: self.len,
            view: self.view.map(|view| view.cast_owned::<U>()),
            access: self.access,
            heap: self.heap,
            children: self.children,
            metadata: self.metadata,
            offsets: self.offsets,
            child_offset: self.child_offset,
            _chunk: self._chunk,
            _type: PhantomData,
        }
    }

    /// Replace this vector with every row of `source` cast to this vector's type, like SQL `CAST`.
    ///
    /// Fails if any row does not convert. The cast uses the cast functions and settings of `link`.
    pub fn cast_rows<T2: VectorElement, C: HasFactory>(
        self,
        source: &Vector<'_, T2>,
        link: &C,
    ) -> Result<Vector<'a, T>> {
        check_api_call!(
            ffi::duckdb_v2_vector_cast,
            link.factory()?.raw(),
            source.handle,
            self.handle,
            ffi::DUCKDB_V2_CAST_MODE::DUCKDB_V2_CAST_MODE_NORMAL,
            RET,
            std::ptr::null_mut()
        )?;
        Self::from_handle_in(&self.handle, self.access, &self.offsets)
    }

    /// Replace this vector with every row of `source` cast to this vector's type, like SQL `TRY_CAST`.
    ///
    /// Rows that do not convert become `NULL`. The returned error, if any, is the
    /// conversion error of one such row.
    pub fn try_cast_rows<T2: VectorElement, C: HasFactory>(
        self,
        source: &Vector<'_, T2>,
        link: &C,
    ) -> Result<(Vector<'a, T>, Option<Error>)> {
        let mut cast_err = std::ptr::null_mut();

        check_api_call!(
            ffi::duckdb_v2_vector_cast,
            link.factory()?.raw(),
            source.handle,
            self.handle,
            ffi::DUCKDB_V2_CAST_MODE::DUCKDB_V2_CAST_MODE_TRY,
            RET,
            &mut cast_err
        )?;

        let row_error = if cast_err.is_null() {
            None
        } else {
            let error = unsafe { Error::from_code_and_handle(DuckDBError::TypeConversion.into(), &mut cast_err) };
            Some(error)
        };
        Ok((
            Self::from_handle_in(&self.handle, self.access, &self.offsets)?,
            row_error,
        ))
    }

    pub(crate) fn metadata(&self) -> TypeMetadata {
        self.metadata
    }

    /// Size the first child of a `LIST` or `MAP` and restart appends at its first row.
    ///
    /// # Safety
    /// Writes must initialize every child row below `size` before DuckDB reads the vector.
    pub(crate) unsafe fn size_appended_child(&mut self, size: usize) -> Result<()> {
        self.children_mut()
            .expect("validated nested vector has a child")
            .get_untyped_vector_at_mut(0)?
            .set_size(size)?;
        self.child_offset_cell().set(0);
        Ok(())
    }

    pub(crate) fn move_appended_child_offset(&mut self, offset: usize) -> Result<()> {
        let size = self
            .children()
            .expect("validated nested vector has a child")
            .get_untyped_vector_at(0)?
            .len();
        if offset > size {
            return Err(Error::invalid_parameter(format!(
                "child offset {offset} is past its {size} rows"
            )));
        }
        self.child_offset_cell().set(offset);
        Ok(())
    }

    pub(crate) fn child_offset_cell(&self) -> &Cell<usize> {
        self.child_offset
            .as_deref()
            .expect("LIST and MAP vectors track their child offset")
    }

    pub(crate) fn validate_as<U: VectorElement>(&self) -> Result<()> {
        match U::validate(self.logical_type(), self.children.as_ref()) {
            Ok(true) => Ok(()),
            Ok(false) => Err(Error::invalid_input(format!(
                "Vector logical type mismatch: expected {:?}, got {:?}",
                U::TYPE_ID,
                self.logical_type.to_text()?
            ))),
            Err(e) => Err(Error::invalid_input(format!(
                "Vector logical type validation failed: expected {:?}, got {:?}: {}",
                U::TYPE_ID,
                self.logical_type.to_text()?,
                e.message
            ))),
        }
    }

    /// # Safety
    /// Reference without copying; source storage must outlive destination reads and remain unchanged.
    pub unsafe fn reference<T2: VectorElement>(self, source: &Vector<'_, T2>) -> Result<Vector<'a, T2>> {
        if self.access == Access::Shared {
            return Err(not_exclusive());
        }
        check_api_call!(ffi::duckdb_v2_vector_reference, self.handle, source.handle)?;

        // The reference replaces the storage kind, size and child vectors, so rebuild from the handle.
        // A write or resize would change storage the source wrapper still reads, so never stay writable.
        Vector::from_handle_in(&self.handle, Access::Exclusive, &self.offsets)
    }

    /// Write one owned [`Value`] through DuckDB's generic value API.
    ///
    /// This is the slow path; prefer [`VectorWriter::set`] for typed vector access.
    pub fn write_value_slow(&mut self, index: usize, value: Value) -> Result<()> {
        if !self.is_writable() {
            return Err(not_writable());
        }
        check_api_call!(ffi::duckdb_v2_vector_set_value, self.handle, index as u64, value.handle,)?;
        // DuckDB may allocate a validity mask or write nested values into the children.
        self.refresh_buffers()
    }

    /// Set one row to `NULL` through DuckDB's generic value API.
    ///
    /// This is the slow path; prefer [`VectorWriter::set`] with `None`.
    pub fn set_null_slow(&mut self, index: usize) -> Result<()> {
        if !self.is_writable() {
            return Err(not_writable());
        }
        check_api_call!(ffi::duckdb_v2_vector_set_null, self.handle, index as u64)?;
        self.refresh_buffers()
    }

    /// Return the vector's child vectors for reshaping or writing, or `None` if it has no children.
    ///
    /// Children get at most the access of this vector. The children cannot be
    /// swapped with another vector's, since readers trust their validated types:
    ///
    /// ```compile_fail,E0596
    /// # use duckdb_neo::{DuckDBType, data_chunk::DataChunk, environment::{Environment, StorageLocation}, types::List};
    /// # fn main() -> duckdb_neo::Result<()> {
    /// # let env = Environment::new()?;
    /// # let conn = env.open(StorageLocation::InMemory)?.connect()?;
    /// let types = [List::<String>::logical_type(&conn)?, List::<i64>::logical_type(&conn)?];
    /// let mut chunk = DataChunk::create(&conn, &types, true)?;
    /// let [mut a, mut b] = chunk.get_vector_array_mut([0, 1])?;
    /// std::mem::swap(&mut *a.children_mut().unwrap(), &mut *b.children_mut().unwrap());
    /// # Ok(())
    /// # }
    /// ```
    pub fn children_mut(&mut self) -> Option<ChildrenMut<'_>> {
        self.children.as_mut().map(ChildrenMut)
    }

    pub(crate) fn children_collection_mut(&mut self) -> Option<&mut VectorCollection> {
        self.children.as_mut()
    }

    fn cache_children(&self) -> Result<()> {
        match &self.children {
            Some(children) => children.cache_all(),
            None => Ok(()),
        }
    }

    /// Explicitly materialize the vector as flat storage.
    ///
    /// The vector must be borrowed mutably, e.g. through
    /// [`crate::data_chunk::DataChunkRef::get_vector_at_mut`], since flattening
    /// replaces storage that other wrappers could still reference.
    pub fn flatten(&mut self) -> Result<()> {
        if self.kind == StorageKind::Flat {
            return Ok(());
        }
        if self.access == Access::Shared {
            return Err(not_exclusive());
        }
        check_api_call!(ffi::duckdb_v2_vector_flatten, self.handle)?;

        // Flattening replaces the buffer and child vectors, so rebuild from the handle.
        *self = Self::from_handle_in(&self.handle, self.access, &self.offsets)?;
        Ok(())
    }

    fn set_not_writable(&mut self) {
        self.access = Access::Exclusive;
        if let Some(children) = &mut self.children
            && children.access == Access::Writable
        {
            children.access = Access::Exclusive;
        }
    }

    /// Set the number of logical rows on writable output.
    pub fn set_size(&mut self, len: usize) -> Result<()> {
        if !self.is_writable() {
            return Err(not_writable());
        }
        check_api_call!(ffi::duckdb_v2_vector_set_size, self.handle, len as u64)?;
        self.len = len;

        self.refresh_buffers()
    }

    /// Turn writable output into a constant vector, returning the rebuilt vector.
    ///
    /// `value` must have the vector's logical type. Every logical row contains
    /// `value`, or is `NULL` when `value` is `NULL`.
    ///
    /// The returned vector is not writable: a constant holds a single
    /// slot, so per-row writes would go past its buffer.
    pub fn make_constant(self, value: Value, count: usize) -> Result<Self> {
        if !self.is_writable() {
            return Err(not_writable());
        }
        check_api_call!(
            ffi::duckdb_v2_vector_make_constant,
            self.handle,
            value.handle,
            count as u64
        )?;

        check_api_call!(ffi::duckdb_v2_vector_constant_set_valid, self.handle, !value.is_null()?)?;

        // The constant brings its own buffer and child vectors, so rebuild from the handle.
        let mut vector = Self::from_handle_in(&self.handle, Access::Exclusive, &self.offsets)?;
        vector.len = count;
        Ok(vector)
    }

    /// Turn writable output into an arithmetic sequence, returning the rebuilt vector.
    ///
    /// Produces `count` values following `start + index * increment`. The
    /// sequence uses [`StorageKind::Other`] and must be flattened before typed
    /// reads. The returned vector is not writable and has no children.
    pub fn make_sequence(mut self, start: i64, increment: i64, count: usize) -> Result<Self> {
        if !self.is_writable() {
            return Err(not_writable());
        }
        check_api_call!(
            ffi::duckdb_v2_vector_make_sequence,
            self.handle,
            start,
            increment,
            count as u64
        )?;
        // The sequence replaces the buffer that owned any child vectors.
        self.children = None;
        self.kind = StorageKind::Other;
        self.len = count;
        self.set_not_writable();
        self.refresh_buffers()?;
        Ok(self)
    }
}

impl<T: VectorElement> Vector<'_, T> {
    /// Prepare a storage-specific reader, caching descendant views once.
    ///
    /// Returns an error for [`StorageKind::Other`]; flatten the vector explicitly first.
    pub fn reader(&self) -> Result<VectorReader<'_, T>> {
        VectorReader::new(self)
    }

    /// Materialize one row as an owned [`Value`].
    ///
    /// This is the slow path; prefer [`VectorReader::get`] or [`VectorReader::iter`].
    pub fn get_value_slow(&self, index: usize) -> Result<Value> {
        check_api_call!(ffi::duckdb_v2_vector_get_value, self.handle, index as u64, RET).map(|handle| Value { handle })
    }
}

fn not_writable() -> Error {
    Error::invalid_input(
        "vector is not writable: it was not supplied as writable output, was borrowed with get_vector_at, or was made constant or a sequence",
    )
}

fn not_flat() -> Error {
    Error::invalid_input("vector is not flat")
}

fn not_exclusive() -> Error {
    Error::invalid_input("vector is borrowed shared: use get_vector_at_mut to change its storage")
}

pub(crate) fn other_not_readable() -> Error {
    Error::invalid_input("OTHER vectors must be flattened before reading")
}

fn out_of_bounds(index: usize, len: usize) -> Error {
    Error::invalid_parameter(format!("Vector index {} is out of bounds for length {}", index, len))
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests;

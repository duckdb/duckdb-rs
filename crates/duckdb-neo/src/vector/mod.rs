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
//! Element types plug in through [`NativeElement`]: a [`VectorReader`] and [`VectorWriter`]
//! read and write a vector's own slots, e.g. scalar references or a `LIST` row's
//! [`ListIndex`](crate::types::ListIndex). Nested rows are read and written through
//! [`WithReader`] and [`WithWriter`], whose readers and writers, such as [`ListReader`] and
//! [`ListWriter`], prepare their children once and compose for nested types.

use std::{cell::OnceCell, marker::PhantomData};

use crate::builder_helpers::ffi_enum_redeclaration;
use crate::error::DuckDBError;
use crate::raw::RawExt;
use crate::{
    Result, check_api_call,
    connection::HasFactory,
    data_chunk::{ChildrenMut, VectorCollection},
    error::Error,
    ffi,
    logical_type::LogicalType,
    types::DecimalSignature,
    value::Value,
};

mod element;
use duckdb_loadable_macros::AsRaw;
pub use element::*;

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

pub use crate::{arena::Arena, bytes::DuckDBBytes};

mod array_writer;
mod child;
mod combined;
mod init;
mod list_writer;
mod map_writer;
mod nested_reader;
mod reader;
mod rows;
mod union_writer;
mod writer;

pub use array_writer::ArrayWriter;
pub use child::ChildVector;
pub use list_writer::ListWriter;
pub use map_writer::MapWriter;
pub use nested_reader::{ArrayReader, Elements, Entries, ListReader, MapReader};
pub use union_writer::{
    UnionMember, UnionMemberReader, UnionMemberWriter, UnionMembers, UnionReader, UnionReaders, UnionWriter,
    UnionWriters,
};

#[doc(hidden)]
pub use combined::VectorReaderTuple;
pub use combined::{CombinedVectorIterator, EnumeratedCombinedVectorIterator};
pub use reader::{EnumeratedVectorReaderIter, Selection, VectorReader, VectorReaderIter};
pub use rows::{RowIter, RowReader, RowWriter, WithReader, WithWriter};
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
#[repr(C)]
#[derive(Debug, AsRaw)]
pub struct Vector<'a, T: NativeElement> {
    pub(crate) handle: ffi::duckdb_v2_vector_handle,
    pub(crate) access: Access,

    logical_type: LogicalType,
    kind: StorageKind,
    len: usize,
    /// Prepared read metadata, dropped by every operation that may replace storage.
    view: OnceCell<reader::CachedReader>,
    heap: Option<Arena>,
    /// Child vectors of nested types; `None` for vectors without children.
    children: Option<VectorCollection>,
    decimal: OnceCell<DecimalSignature>,
    _chunk: PhantomData<&'a ()>,
    _type: PhantomData<T>,
}

impl<'a, T: NativeElement> Vector<'a, T> {
    /// The handle must remain valid for the lifetime of the owning chunk.
    pub(crate) fn from_handle(handle: &ffi::duckdb_v2_vector_handle, access: Access) -> Result<Self> {
        let logical_type = check_api_call!(ffi::duckdb_v2_vector_get_logical_type, *handle, RET)
            .map(|handle| LogicalType { handle })?;

        let kind =
            check_api_call!(ffi::duckdb_v2_vector_get_vector_type, *handle, RET).map(StorageKind::try_from)??;
        let len = check_api_call!(ffi::duckdb_v2_vector_get_size, *handle, RET).map(|x| x as usize)?;

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

        let children = (!child_handles.is_empty()).then(|| VectorCollection::children(child_handles, access));

        let vector = Self {
            handle: *handle,
            logical_type,
            kind,
            len,
            access,
            view: OnceCell::new(),
            heap: None,
            children,
            decimal: OnceCell::new(),
            _chunk: PhantomData,
            _type: PhantomData,
        };
        vector.validate_as::<T>()?;
        Ok(vector)
    }
}

impl<'a> Vector<'a, Unknown> {
    /// Validate and attach a logical element type.
    pub fn cast<T: NativeElement>(self) -> Result<Vector<'a, T>> {
        self.validate_as::<T>()?;
        Ok(self.cast_unchecked())
    }

    /// Validate and attach a logical element type to a borrowed vector.
    pub fn cast_ref<T: NativeElement>(&self) -> Result<&Vector<'a, T>> {
        self.validate_as::<T>()?;
        // SAFETY: T was validated above.
        Ok(unsafe { self.cast_ref_unchecked() })
    }

    /// # Safety
    /// This vector must have been validated as `T`.
    pub(crate) unsafe fn cast_ref_unchecked<T: NativeElement>(&self) -> &Vector<'a, T> {
        // SAFETY: Both repr(C) wrappers differ only in markers.
        unsafe { &*(self as *const Vector<'a, Unknown> as *const Vector<'a, T>) }
    }
}

impl<'a, T: NativeElement> Vector<'a, T> {
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

    /// Drop the cached view and arena after a state change, e.g. set_size, and refresh the children in place.
    ///
    /// DuckDB keeps child vectors when it resizes or writes their parent, so child wrappers, and writers
    /// pointing to them, stay valid; only their sizes and buffers change.
    fn refresh_buffers(&mut self) -> Result<()> {
        self.view.take();
        self.heap = None;
        if let Some(children) = &mut self.children {
            children.refresh_cached()?;
        }
        Ok(())
    }

    /// Re-read this child's size and drop its buffers after its parent changed.
    pub(crate) fn refresh_from_handle(&mut self) -> Result<()> {
        self.len = vector_len(self.handle)?;
        self.refresh_buffers()
    }

    pub(crate) fn cast_unchecked<U: NativeElement>(self) -> Vector<'a, U> {
        Vector {
            handle: self.handle,
            logical_type: self.logical_type,
            kind: self.kind,
            len: self.len,
            access: self.access,
            view: self.view,
            heap: self.heap,
            children: self.children,
            decimal: self.decimal,
            _chunk: self._chunk,
            _type: PhantomData,
        }
    }

    /// Replace this vector with every row of `source` cast to this vector's type, like SQL `CAST`.
    ///
    /// Fails if any row does not convert. The cast uses the cast functions and settings of `link`.
    ///
    /// # Safety
    /// The cast can leave this vector sharing storage with `source`, e.g. for same-type or
    /// reinterpreting casts. Neither may be written or resized, through any wrapper, while the other is read.
    pub unsafe fn cast_rows<T2: NativeElement, C: HasFactory>(self, source: &Vector<'_, T2>, link: &C) -> Result<()> {
        if !self.is_writable() {
            return Err(not_writable());
        }
        let result = check_api_call!(
            ffi::duckdb_v2_vector_cast,
            link.factory()?.raw(),
            source.handle,
            self.handle,
            ffi::DUCKDB_V2_CAST_MODE::DUCKDB_V2_CAST_MODE_NORMAL,
            RET,
            std::ptr::null_mut()
        );
        result.map(|_converted| ())
    }

    /// Replace this vector with every row of `source` cast to this vector's type, like SQL `TRY_CAST`.
    ///
    /// Rows that do not convert become `NULL`, and the cast still succeeds. When any row did
    /// not convert, returns the conversion error of one such row.
    ///
    /// # Safety
    /// As for [`Self::cast_rows`].
    pub unsafe fn try_cast_rows<T2: NativeElement, C: HasFactory>(
        self,
        source: &Vector<'_, T2>,
        link: &C,
    ) -> Result<Option<Error>> {
        if !self.is_writable() {
            return Err(not_writable());
        }
        let mut cast_err = std::ptr::null_mut();

        let result = check_api_call!(
            ffi::duckdb_v2_vector_cast,
            link.factory()?.raw(),
            source.handle,
            self.handle,
            ffi::DUCKDB_V2_CAST_MODE::DUCKDB_V2_CAST_MODE_TRY,
            RET,
            &mut cast_err
        );
        result?;

        if cast_err.is_null() {
            Ok(None)
        } else {
            let error = unsafe { Error::from_code_and_handle(DuckDBError::TypeConversion.into(), &mut cast_err) };
            Ok(Some(error))
        }
    }

    #[cfg(feature = "rust_decimal")]
    pub(crate) fn decimal_signature(&self) -> Result<DecimalSignature> {
        if let Some(signature) = self.decimal.get() {
            return Ok(*signature);
        }
        let signature = DecimalSignature::from_logical_type(&self.logical_type)?;
        Ok(*self.decimal.get_or_init(|| signature))
    }

    /// Return the prepared read metadata, fetched once per state; `OTHER` storage has none.
    fn view(&self) -> Result<Option<&reader::CachedReader>> {
        if self.kind == StorageKind::Other {
            return Ok(None);
        }
        if let Some(view) = self.view.get() {
            return Ok(Some(view));
        }
        let view = check_api_call!(ffi::duckdb_v2_vector_get_view, self.handle, RET)?;
        // SAFETY: DuckDB returned a current view of this vector, including its constant validity slot.
        Ok(Some(self.view.get_or_init(|| unsafe {
            reader::CachedReader::new(view, self.kind, self.len)
        })))
    }

    /// Return the size of the first child of a `LIST` or `MAP`.
    pub(crate) fn list_child_len(&self) -> Result<usize> {
        Ok(self
            .children()
            .expect("validated LIST and MAP vectors have a child")
            .get_untyped_vector_at(0)?
            .len())
    }

    /// Resize the first child of a `LIST` or `MAP`, refusing to cut the elements of a valid row.
    pub(crate) fn resize_list_child(&mut self, len: usize) -> Result<()> {
        self.check_list_child_shrink(len)?;
        self.children_collection_mut()
            .expect("validated LIST and MAP vectors have a child")
            .get_untyped_vector_at_mut(0)?
            .set_size(len)
    }

    /// As [`Self::resize_list_child`], leaving new child rows uninitialized.
    ///
    /// # Safety
    /// As for [`Vector::set_size_uninit`], for the new rows of the child and their descendants.
    pub(crate) unsafe fn resize_list_child_uninit(&mut self, len: usize) -> Result<()> {
        self.check_list_child_shrink(len)?;
        // SAFETY: the caller initializes the new child rows.
        unsafe {
            self.children_collection_mut()
                .expect("validated LIST and MAP vectors have a child")
                .get_untyped_vector_at_mut(0)?
                .set_size_uninit(len)
        }
    }

    /// Write `entries` into rows `offset..` of a `LIST` or `MAP`, checked once against the child's size.
    pub(crate) fn write_list_entries(
        writer: &mut VectorWriter<'_, T>,
        offset: usize,
        entries: &[crate::types::ListIndex],
        what: &str,
    ) -> Result<()> {
        let end = offset
            .checked_add(entries.len())
            .filter(|end| *end <= writer.len())
            .ok_or_else(|| out_of_bounds(offset.saturating_add(entries.len()).saturating_sub(1), writer.len()))?;
        let child_len = writer.vector().list_child_len()? as u64;
        if let Some(entry) = entries
            .iter()
            .find(|entry| entry.end().is_none_or(|end| end > child_len))
        {
            crate::types::list::check_entry(entry, child_len as usize, what)?;
        }
        // SAFETY: the rows are in bounds, LIST and MAP rows store list entries, and every entry
        // covers initialized child rows.
        unsafe {
            std::ptr::copy_nonoverlapping(
                entries.as_ptr(),
                writer.as_mut_ptr().cast::<crate::types::ListIndex>().add(offset),
                entries.len(),
            );
            writer.mark_valid_range(offset, end);
        }
        Ok(())
    }

    fn check_list_child_shrink(&mut self, len: usize) -> Result<()> {
        if !self.is_writable() {
            return Err(not_writable());
        }
        if len < self.list_child_len()? {
            let reader = VectorReader::<Unknown>::new(
                // SAFETY: both wrappers differ only in markers.
                unsafe { &*(self as *const Self).cast::<Vector<'_, Unknown>>() },
            )?;
            for row in 0..reader.len() {
                // SAFETY: the row is in bounds, and LIST and MAP rows store list entries.
                let entry = unsafe { reader.list_entry_unchecked(row) };
                if let Some(entry) = entry.filter(|entry| entry.end().is_none_or(|end| end > len as u64)) {
                    return Err(Error::invalid_input(format!(
                        "row {row} uses child rows {}..+{}, past the new {len}",
                        entry.offset(),
                        entry.length()
                    )));
                }
            }
        }
        Ok(())
    }

    pub(crate) fn validate_as<U: NativeElement>(&self) -> Result<()> {
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
    /// Neither vector may be written or resized, through any wrapper, while the other is read.
    pub unsafe fn reference<T2: NativeElement>(self, source: &Vector<'_, T2>) -> Result<Vector<'a, T2>> {
        if self.access == Access::Shared {
            return Err(not_exclusive());
        }
        check_api_call!(ffi::duckdb_v2_vector_reference, self.handle, source.handle)?;

        // The reference replaces the storage kind, size and child vectors, so rebuild from the handle.
        // A write or resize would change storage the source wrapper still reads, so never stay writable.
        Vector::from_handle(&self.handle, Access::Exclusive)
    }

    /// Return the vector's child vectors, or `None` if it has no children.
    pub fn children(&self) -> Option<&VectorCollection> {
        self.children.as_ref()
    }

    /// Return the vector's child vectors for writing or flattening, or `None` if it has no children.
    ///
    /// Children get at most the access of this vector and are borrowed as [`ChildVector`]s,
    /// which keep the shape the parent gives them. The children cannot be swapped with
    /// another vector's, since readers trust their validated types:
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
        *self = Self::from_handle(&self.handle, self.access)?;
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
    ///
    /// New rows, including those of nested children, read as zeroed values such as `0`, `''`
    /// and `[]`, or as `NULL` for types where zeroed storage is not a valid value.
    pub fn set_size(&mut self, len: usize) -> Result<()> {
        let old = self.len;
        // SAFETY: the rows the resize exposes are initialized before the result is returned.
        let result = unsafe { self.set_size_uninit(len) };
        self.init_rows(old, self.len)?;
        result
    }

    /// Set the number of logical rows on writable output, leaving new rows uninitialized.
    ///
    /// # Safety
    /// Every row the resize exposes, and the matching rows of nested children, must be written
    /// or marked `NULL` before anything reads this vector.
    pub unsafe fn set_size_uninit(&mut self, len: usize) -> Result<()> {
        if !self.is_writable() {
            return Err(not_writable());
        }
        let result = check_api_call!(ffi::duckdb_v2_vector_set_size, self.handle, len as u64);
        // A failed reservation can still replace buffers, and keeps DuckDB's size.
        self.refresh_buffers()?;
        self.len = match result {
            Ok(()) => len,
            Err(_) => vector_len(self.handle)?,
        };
        result
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
        let mut vector = Self::from_handle(&self.handle, Access::Exclusive)?;
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

impl<T: NativeElement> Vector<'_, T> {
    /// Prepare exclusive write access and allocate a writable validity mask.
    ///
    /// Fails unless this is writable output with flat storage. Resize the vector
    /// before acquiring its writer.
    pub fn writer(&mut self) -> Result<VectorWriter<'_, T>> {
        VectorWriter::new(self)
    }

    /// Write a borrowed [`Value`] through DuckDB's generic value API.
    ///
    /// This is the slow path; prefer [`VectorWriter::set`] for typed vector access.
    pub fn write_value_slow(&mut self, index: usize, value: &Value) -> Result<()> {
        if !self.is_writable() {
            return Err(not_writable());
        }
        let result = check_api_call!(ffi::duckdb_v2_vector_set_value, self.handle, index as u64, value.handle,);
        // DuckDB may allocate a validity mask or write nested values into the children.
        self.refresh_buffers()?;
        result
    }

    /// Set one row to `NULL` through DuckDB's generic value API.
    ///
    /// This is the slow path; prefer [`VectorWriter::set`] with `None`.
    pub fn set_null_slow(&mut self, index: usize) -> Result<()> {
        if !self.is_writable() {
            return Err(not_writable());
        }
        let result = check_api_call!(ffi::duckdb_v2_vector_set_null, self.handle, index as u64);
        self.refresh_buffers()?;
        result
    }
}

impl<T: NativeElement> Vector<'_, T> {
    /// Prepare a storage-specific reader, along with readers for every descendant.
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

fn vector_len(handle: ffi::duckdb_v2_vector_handle) -> Result<usize> {
    check_api_call!(ffi::duckdb_v2_vector_get_size, handle, RET).map(|len| len as usize)
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

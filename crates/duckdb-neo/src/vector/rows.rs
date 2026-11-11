//! Row readers and writers that prepare nested vectors once, so they compose: a [`ListWriter`](super::ListWriter)
//! of a derived struct writes its elements through the struct's own writer.

use std::ptr::NonNull;

use super::{Vector, VectorElement, VectorReader, VectorWriter, WritableVectorElement, out_of_bounds};
use crate::Result;

/// Reads the rows of one vector, with the readers of any children prepared up front.
pub trait RowReader {
    /// One non-NULL row.
    type Item<'r>
    where
        Self: 'r;

    /// Return the number of rows.
    fn len(&self) -> usize;

    /// Return whether there are no rows.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Return whether a row is SQL NULL. Out-of-range rows are errors.
    fn is_null(&self, row: usize) -> Result<bool>;

    /// Return a row, or `None` for SQL NULL. Out-of-range rows are errors.
    #[inline]
    fn get(&self, row: usize) -> Result<Option<Self::Item<'_>>> {
        if row >= self.len() {
            return Err(out_of_bounds(row, self.len()));
        }
        // SAFETY: the row is in bounds.
        Ok(unsafe { self.get_unchecked(row) })
    }

    /// # Safety
    /// `row` must be less than [`Self::len`].
    #[doc(hidden)]
    unsafe fn get_unchecked(&self, row: usize) -> Option<Self::Item<'_>>;
}

/// Writes the rows of one vector, with the writers of any children prepared up front.
pub trait RowWriter {
    /// The borrowed input for one non-NULL row.
    type Value: ?Sized;

    /// Return the number of rows.
    fn len(&self) -> usize;

    /// Return whether there are no rows.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Write a non-NULL row.
    fn set(&mut self, row: usize, value: &Self::Value) -> Result<()>;

    /// Make a row SQL NULL, along with the descendants DuckDB requires to be NULL.
    fn set_null(&mut self, row: usize) -> Result<()>;

    /// Write a row; `None` writes SQL NULL.
    #[inline]
    fn set_option(&mut self, row: usize, value: Option<&Self::Value>) -> Result<()> {
        match value {
            Some(value) => self.set(row, value),
            None => self.set_null(row),
        }
    }

    /// Resize the rows like [`Vector::set_size`], keeping the writer and its children usable.
    #[doc(hidden)]
    fn resize_rows(&mut self, len: usize) -> Result<()>;

    /// Reacquire buffers after an ancestor resized or rewrote this writer's vector.
    #[doc(hidden)]
    fn refresh(&mut self) -> Result<()>;
}

/// Element types with a [`RowReader`], e.g. a [`ListReader`](super::ListReader) for `LIST`.
pub trait WithReader: VectorElement {
    /// The reader for vectors of this type.
    type Reader<'a>: RowReader
    where
        Self: 'a;

    /// Prepare a reader for `vector`.
    fn row_reader<'a>(vector: &'a Vector<'a, Self>) -> Result<Self::Reader<'a>>;
}

/// Element types with a [`RowWriter`], e.g. a [`ListWriter`](super::ListWriter) for `LIST`.
///
/// # Safety
/// The writer must keep every row it writes valid for the logical type that `validate` accepted.
pub unsafe trait WithWriter: VectorElement {
    /// The writer for vectors of this type.
    type Writer<'w>: RowWriter
    where
        Self: 'w;

    /// Prepare a writer for `vector`.
    ///
    /// # Safety
    /// `vector` must be writable output validated as `Self`, exclusively borrowed for `'w`, and its
    /// wrapper must outlive the writer.
    #[doc(hidden)]
    unsafe fn row_writer<'w>(vector: NonNull<Vector<'w, Self>>) -> Result<Self::Writer<'w>>;
}

impl<T: WithReader> Vector<'_, T> {
    /// Prepare the [`WithReader::Reader`] of this vector, e.g. a [`ListReader`](super::ListReader).
    pub fn row_reader(&self) -> Result<T::Reader<'_>> {
        T::row_reader(self)
    }
}

impl<T: WithWriter> Vector<'_, T> {
    /// Prepare the [`WithWriter::Writer`] of this vector, e.g. a [`ListWriter`](super::ListWriter).
    pub fn row_writer(&mut self) -> Result<T::Writer<'_>> {
        // SAFETY: the exclusive borrow covers the writer, and validation checked the type.
        unsafe { T::row_writer(NonNull::from(self).cast()) }
    }
}

impl<'a, T: VectorElement + 'a> RowReader for VectorReader<'a, T> {
    type Item<'r>
        = T::Ref<'a>
    where
        Self: 'r;

    #[inline]
    fn len(&self) -> usize {
        VectorReader::len(self)
    }

    #[inline]
    fn is_null(&self, row: usize) -> Result<bool> {
        VectorReader::is_null(self, row)
    }

    #[inline(always)]
    unsafe fn get_unchecked(&self, row: usize) -> Option<Self::Item<'_>> {
        // SAFETY: the caller guarantees bounds.
        unsafe { VectorReader::get_unchecked(self, row) }
    }
}

impl<T: WritableVectorElement> RowWriter for VectorWriter<'_, T> {
    type Value = T::Write;

    #[inline]
    fn len(&self) -> usize {
        VectorWriter::len(self)
    }

    #[inline(always)]
    fn set(&mut self, row: usize, value: &T::Write) -> Result<()> {
        VectorWriter::set(self, row, Some(value))
    }

    #[inline(always)]
    fn set_null(&mut self, row: usize) -> Result<()> {
        VectorWriter::set(self, row, None)
    }

    fn resize_rows(&mut self, len: usize) -> Result<()> {
        self.resize(len)
    }

    fn refresh(&mut self) -> Result<()> {
        self.refresh_all()
    }
}

macro_rules! leaf_rows {
    ($([$($generics:tt)*] $type:ty $(where [$($bounds:tt)+])?),* $(,)?) => {$(
        impl<$($generics)*> WithReader for $type $(where $($bounds)+)? {
            type Reader<'a>
                = VectorReader<'a, Self>
            where
                Self: 'a;

            fn row_reader<'a>(vector: &'a Vector<'a, Self>) -> Result<Self::Reader<'a>> {
                vector.reader()
            }
        }

        // SAFETY: the vector writer only writes values of its validated type.
        unsafe impl<$($generics)*> WithWriter for $type $(where $($bounds)+)? {
            type Writer<'w>
                = VectorWriter<'w, Self>
            where
                Self: 'w;

            unsafe fn row_writer<'w>(vector: NonNull<Vector<'w, Self>>) -> Result<Self::Writer<'w>> {
                // SAFETY: the caller upholds the borrow contract.
                unsafe { VectorWriter::from_raw(vector) }
            }
        }
    )*};
}

use crate::types::*;

leaf_rows!(
    [] bool, [] i8, [] i16, [] i32, [] i64, [] u8, [] u16, [] u32, [] u64, [] f32, [] f64,
    [] HugeInt, [] UHugeInt,
    [] String, [] UncheckedString, [] BlobValue, [] BitValue, ['s] &'s str,
    [] BigNum, [] BigNumValue,
    [] ShortDecimal, [] WordDecimal, [] LongDecimal, [] HugeDecimal,
    [const WIDTH: u8, const SCALE: u8] Decimal<WIDTH, SCALE> where [Width<WIDTH>: DecimalWidth],
    [] DateValue, [] TimeValue, [] TimeNsValue, [] TimeTzValue, [] TimestampValue, [] TimestampSecValue,
    [] TimestampMsValue, [] TimestampNsValue, [] TimestampTzValue, [] TimestampTzNsValue, [] IntervalValue,
    [] UuidValueRaw, [] Variant,
);

#[cfg(feature = "uuid")]
leaf_rows!([] uuid::Uuid);

#[cfg(feature = "rust_decimal")]
leaf_rows!([] rust_decimal::Decimal);

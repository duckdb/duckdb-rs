use std::{iter::FusedIterator, ops::Range};

use crate::{Result, error::Error};

use super::{VectorElement, VectorReader};

/// Iterates over equally sized readers, returning one flat tuple per logical row.
///
/// Accepts tuples of one through twelve [`VectorReader`]s. Each column preserves
/// its element type, borrowed lifetime, and independent SQL NULL value. Different
/// storage layouts may be combined without flattening.
///
/// ```
/// # use duckdb_neo::{Parameters, Result, environment::{Environment, StorageLocation}};
/// use duckdb_neo::vector::CombinedVectorIterator;
/// # fn main() -> Result<()> {
/// # let env = Environment::new()?;
/// # let conn = env.open(StorageLocation::InMemory)?.connect()?;
/// let chunk = conn.query("SELECT 42::INTEGER, 'duck'::VARCHAR", Parameters::None)?
///     .next_chunk()?.unwrap();
/// let numbers = chunk.get_vector_at::<i32>(0)?.reader()?;
/// let strings = chunk.get_vector_at::<String>(1)?.reader()?;
/// let mut rows = CombinedVectorIterator::new((numbers, strings))?;
/// assert_eq!(rows.next(), Some((Some(&42), Some("duck"))));
/// assert_eq!(rows.next(), None);
/// # Ok(())
/// # }
/// ```
///
/// Readers keep their source vectors borrowed for as long as iteration needs them:
///
/// ```compile_fail,E0502
/// use duckdb_neo::{Result, vector::{CombinedVectorIterator, Vector}};
///
/// fn resize(vector: &mut Vector<'_, i32>) -> Result<()> {
///     let mut rows = CombinedVectorIterator::new((vector.reader()?,))?;
///     vector.set_size(10)?;
///     let _ = rows.next();
///     Ok(())
/// }
/// ```
#[derive(Debug)]
pub struct CombinedVectorIterator<Readers> {
    readers: Readers,
    rows: Range<usize>,
    all_flat: bool,
}

impl<Readers: VectorReaderTuple> CombinedVectorIterator<Readers> {
    /// Combine prepared readers, rejecting unequal logical row counts.
    pub fn new(readers: Readers) -> Result<Self> {
        let count = readers.row_count()?;
        let all_flat = readers.all_flat();
        Ok(Self {
            readers,
            rows: 0..count,
            all_flat,
        })
    }

    /// Add row numbers without an additional per-row iterator call or counter.
    ///
    /// Number the remaining rows from zero, matching [`Iterator::enumerate`].
    pub fn enumerate(self) -> EnumeratedCombinedVectorIterator<Readers> {
        let origin = self.rows.start;
        EnumeratedCombinedVectorIterator { inner: self, origin }
    }

    #[inline(always)]
    fn fold_indexed<B, F>(self, mut value: B, mut f: F) -> B
    where
        F: FnMut(B, usize, Readers::Row) -> B,
    {
        if self.all_flat {
            for index in self.rows {
                // SAFETY: Construction verified every reader is flat and equally sized.
                value = f(value, index, unsafe { self.readers.read_flat_unchecked(index) });
            }
        } else {
            for index in self.rows {
                // SAFETY: Construction verified equal lengths and this range stays in bounds.
                value = f(value, index, unsafe { self.readers.read_unchecked(index) });
            }
        }
        value
    }
}

impl<Readers: VectorReaderTuple> Iterator for CombinedVectorIterator<Readers> {
    type Item = Readers::Row;

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        let index = self.rows.next()?;
        if self.all_flat {
            // SAFETY: Construction checked equal lengths and flat storage for every reader.
            Some(unsafe { self.readers.read_flat_unchecked(index) })
        } else {
            // SAFETY: Construction checked equal lengths; the shared range stays in bounds.
            Some(unsafe { self.readers.read_unchecked(index) })
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.rows.size_hint()
    }

    #[inline(always)]
    fn fold<B, F>(self, value: B, mut f: F) -> B
    where
        F: FnMut(B, Self::Item) -> B,
    {
        self.fold_indexed(value, |value, _, row| f(value, row))
    }
}

impl<Readers: VectorReaderTuple> ExactSizeIterator for CombinedVectorIterator<Readers> {}

impl<Readers: VectorReaderTuple> FusedIterator for CombinedVectorIterator<Readers> {}

/// Row-numbered adapter returned by [`CombinedVectorIterator::enumerate`].
#[derive(Debug)]
pub struct EnumeratedCombinedVectorIterator<Readers> {
    inner: CombinedVectorIterator<Readers>,
    origin: usize,
}

impl<Readers: VectorReaderTuple> Iterator for EnumeratedCombinedVectorIterator<Readers> {
    type Item = (usize, Readers::Row);

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        let index = self.inner.rows.start;
        self.inner.next().map(|row| (index - self.origin, row))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }

    #[inline(always)]
    fn fold<B, F>(self, value: B, mut f: F) -> B
    where
        F: FnMut(B, Self::Item) -> B,
    {
        let origin = self.origin;
        self.inner
            .fold_indexed(value, |value, index, row| f(value, (index - origin, row)))
    }
}

impl<Readers: VectorReaderTuple> ExactSizeIterator for EnumeratedCombinedVectorIterator<Readers> {}

impl<Readers: VectorReaderTuple> FusedIterator for EnumeratedCombinedVectorIterator<Readers> {}

mod private {
    pub trait Sealed {}
}

/// Supported reader tuples; sealed because iteration trusts their length validation.
#[doc(hidden)]
pub trait VectorReaderTuple: private::Sealed {
    type Row;

    fn row_count(&self) -> Result<usize>;

    fn all_flat(&self) -> bool;

    /// # Safety
    /// `index` must be within the logical row count of every reader in the tuple.
    unsafe fn read_unchecked(&self, index: usize) -> Self::Row;

    /// # Safety
    /// Every reader must be flat, and `index` must be within their shared logical row count.
    unsafe fn read_flat_unchecked(&self, index: usize) -> Self::Row;
}

macro_rules! impl_reader_tuple {
    ($(($lifetime:lifetime, $element:ident, $index:tt)),+ $(,)?) => {
        impl<$($lifetime,)+ $($element: VectorElement + $lifetime,)+> private::Sealed
            for ($(VectorReader<$lifetime, $element>,)+)
        {}

        impl<$($lifetime,)+ $($element: VectorElement + $lifetime,)+> VectorReaderTuple
            for ($(VectorReader<$lifetime, $element>,)+)
        {
            type Row = ($(Option<$element::Ref<$lifetime>>,)+);

            fn row_count(&self) -> Result<usize> {
                let count = self.0.len();
                $(
                    if self.$index.len() != count {
                        return Err(Error::invalid_input(format!(
                            "combined reader column {} has length {}, expected {}",
                            $index + 1, self.$index.len(), count,
                        )));
                    }
                )+
                Ok(count)
            }

            fn all_flat(&self) -> bool {
                true $(&& self.$index.storage_kind() == super::StorageKind::Flat)+
            }

            #[inline(always)]
            unsafe fn read_unchecked(&self, index: usize) -> Self::Row {
                // SAFETY: The caller guarantees the index is within every prepared reader.
                ($(unsafe { self.$index.get_unchecked(index) },)+)
            }

            #[inline(always)]
            unsafe fn read_flat_unchecked(&self, index: usize) -> Self::Row {
                // SAFETY: The caller guarantees flat storage and in-bounds rows.
                ($(unsafe { self.$index.get_flat_unchecked(index) },)+)
            }
        }
    };
}

impl_reader_tuple!(('a, A, 0));
impl_reader_tuple!(('a, A, 0), ('b, B, 1));
impl_reader_tuple!(('a, A, 0), ('b, B, 1), ('c, C, 2));
impl_reader_tuple!(('a, A, 0), ('b, B, 1), ('c, C, 2), ('d, D, 3));
impl_reader_tuple!(('a, A, 0), ('b, B, 1), ('c, C, 2), ('d, D, 3), ('e, E, 4));
impl_reader_tuple!(('a, A, 0), ('b, B, 1), ('c, C, 2), ('d, D, 3), ('e, E, 4), ('f, F, 5));
impl_reader_tuple!(('a, A, 0), ('b, B, 1), ('c, C, 2), ('d, D, 3), ('e, E, 4), ('f, F, 5), ('g, G, 6));
impl_reader_tuple!(('a, A, 0), ('b, B, 1), ('c, C, 2), ('d, D, 3), ('e, E, 4), ('f, F, 5), ('g, G, 6), ('h, H, 7));
impl_reader_tuple!(('a, A, 0), ('b, B, 1), ('c, C, 2), ('d, D, 3), ('e, E, 4), ('f, F, 5), ('g, G, 6), ('h, H, 7), ('i, I, 8));
impl_reader_tuple!(('a, A, 0), ('b, B, 1), ('c, C, 2), ('d, D, 3), ('e, E, 4), ('f, F, 5), ('g, G, 6), ('h, H, 7), ('i, I, 8), ('j, J, 9));
impl_reader_tuple!(('a, A, 0), ('b, B, 1), ('c, C, 2), ('d, D, 3), ('e, E, 4), ('f, F, 5), ('g, G, 6), ('h, H, 7), ('i, I, 8), ('j, J, 9), ('k, K, 10));
impl_reader_tuple!(('a, A, 0), ('b, B, 1), ('c, C, 2), ('d, D, 3), ('e, E, 4), ('f, F, 5), ('g, G, 6), ('h, H, 7), ('i, I, 8), ('j, J, 9), ('k, K, 10), ('l, L, 11));

#[cfg(test)]
mod tests;

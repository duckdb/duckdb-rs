use std::{borrow::Borrow, ptr::NonNull};

use super::{RowWriter, Unknown, Vector, VectorWriter, WithWriter, out_of_bounds};
use crate::{Result, error::Error, types::List, types::ListIndex};

/// Appends `LIST` rows to a writable flat vector, growing the element child as needed.
///
/// Elements are appended after the child's written elements, through the element type's own
/// [`RowWriter`], so lists of nested types compose: [`Self::push_with`] hands out the element
/// writer of, e.g., a derived struct or an inner `LIST`. [`Self::finish`] trims the child to the
/// elements the rows use; skipping it only leaves unused, initialized elements.
///
/// ```
/// # use duckdb_neo::{DuckDBType, Result, data_chunk::DataChunk, environment::{Environment, StorageLocation}, types::List};
/// # fn main() -> Result<()> {
/// # let conn = Environment::new()?.open(StorageLocation::InMemory)?.connect()?;
/// let mut chunk = DataChunk::create(&conn, &[List::<i32>::logical_type(&conn)?], true)?;
/// let mut vector = chunk.get_vector_at_mut::<List<i32>>(0)?;
/// vector.set_size(3)?;
/// let mut writer = vector.list_writer()?;
/// writer.set_elements(2, [Some(&1), None])?;
/// writer.set_null(1)?;
/// writer.push(Some(&3))?;
/// writer.finish_row(0)?;
/// writer.finish()?;
/// assert_eq!(vector.get_value_slow(2)?.get::<List<i32>>()?, Some(List(vec![Some(1), None])));
/// assert_eq!(vector.get_value_slow(0)?.get::<List<i32>>()?, Some(List(vec![Some(3)])));
/// # Ok(())
/// # }
/// ```
///
/// Columnar writers fill the elements directly and then point rows at them; entries are checked
/// against the child's size:
///
/// ```
/// # use duckdb_neo::{DuckDBType, Result, data_chunk::DataChunk, environment::{Environment, StorageLocation}, types::{List, ListIndex}};
/// # fn main() -> Result<()> {
/// # let conn = Environment::new()?.open(StorageLocation::InMemory)?.connect()?;
/// # let mut chunk = DataChunk::create(&conn, &[List::<i32>::logical_type(&conn)?], true)?;
/// # let mut vector = chunk.get_vector_at_mut::<List<i32>>(0)?;
/// vector.set_size(2)?;
/// let mut writer = vector.list_writer()?;
/// writer.reserve(3)?;
/// for (index, value) in [7, 8, 9].iter().enumerate() {
///     writer.elements().set(index, value)?;
/// }
/// writer.set_entry(0, ListIndex::new(0, 2))?;
/// writer.set_entry(1, ListIndex::new(2, 1))?;
/// assert!(writer.set_entry(1, ListIndex::new(2, 2)).is_err());
/// # Ok(())
/// # }
/// ```
pub struct ListWriter<'w, T: WithWriter> {
    parent: VectorWriter<'w, List<T>>,
    /// The element child, whose size bounds every entry; the element writer could be swapped out.
    child: NonNull<Vector<'w, Unknown>>,
    elements: T::Writer<'w>,
    /// The first element of the row being appended.
    row_start: usize,
    /// The next element to append.
    offset: usize,
    /// One past the last element any row uses; elements before the writer's start are kept.
    used: usize,
}

impl<'w, T: WithWriter + 'w> ListWriter<'w, T> {
    /// # Safety
    /// As for [`WithWriter::row_writer`].
    pub(super) unsafe fn from_raw(mut vector: NonNull<Vector<'w, List<T>>>) -> Result<Self> {
        // SAFETY: the caller guarantees an exclusive borrow of a validated, writable vector.
        let borrowed = unsafe { vector.as_mut() };
        let child = borrowed
            .children
            .as_mut()
            .expect("validated LIST vector has a child")
            .cached_iter_mut()?
            .next()
            .expect("validated LIST vector has a child");
        let child = NonNull::from(child).cast::<Vector<'w, Unknown>>();
        // SAFETY: the writer keeps `vector` exclusively borrowed and only touches its child through
        // `elements`, which `List<T>::validate` checked as `T`; resizing the parent keeps the child wrapper.
        let (parent, elements) = unsafe { (VectorWriter::from_raw(vector)?, T::row_writer(child.cast())?) };
        // SAFETY: the child wrapper lives as long as the borrowed vector.
        let len = unsafe { child.as_ref() }.len();
        Ok(Self {
            parent,
            child,
            elements,
            row_start: len,
            offset: len,
            used: len,
        })
    }

    fn child_len(&self) -> usize {
        // SAFETY: the child wrapper lives as long as the borrowed vector.
        unsafe { self.child.as_ref() }.len()
    }

    /// Return the number of rows.
    pub fn len(&self) -> usize {
        self.parent.len()
    }

    /// Return whether the vector has no rows.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Return the next element [`Self::push`] writes.
    pub fn child_offset(&self) -> usize {
        self.offset
    }

    /// Grow the element child so that `additional` elements fit after [`Self::child_offset`].
    ///
    /// New elements are initialized like [`Vector::set_size`].
    pub fn reserve(&mut self, additional: usize) -> Result<()> {
        let needed = self
            .offset
            .checked_add(additional)
            .ok_or_else(|| Error::invalid_input("LIST element count overflow"))?;
        if needed > self.child_len() {
            self.elements.resize_rows(needed)?;
        }
        Ok(())
    }

    #[cold]
    fn grow(&mut self) -> Result<()> {
        let len = self.child_len();
        self.elements.resize_rows(len.saturating_mul(2).max(len + 1).max(64))
    }

    /// Borrow the element writer, for columnar writes; point rows at the elements with [`Self::set_entry`].
    pub fn elements(&mut self) -> &mut T::Writer<'w> {
        &mut self.elements
    }

    /// Point a row at elements of the child, checked against the child's size.
    pub fn set_entry(&mut self, row: usize, entry: ListIndex) -> Result<()> {
        if row >= self.len() {
            return Err(out_of_bounds(row, self.len()));
        }
        let end = entry
            .offset
            .checked_add(entry.length)
            .filter(|end| *end <= self.child_len() as u64)
            .ok_or_else(|| {
                Error::invalid_input(format!(
                    "LIST entry {}..+{} is past the {} child elements",
                    entry.offset,
                    entry.length,
                    self.child_len()
                ))
            })?;
        self.used = self.used.max(end as usize);
        // SAFETY: the row is in bounds, and the entry covers initialized child elements.
        unsafe { self.write_entry(row, entry) };
        Ok(())
    }

    /// Make a row SQL NULL, with an empty entry.
    pub fn set_null(&mut self, row: usize) -> Result<()> {
        if row >= self.len() {
            return Err(out_of_bounds(row, self.len()));
        }
        // SAFETY: the row is in bounds, and an empty entry references no elements.
        unsafe {
            self.parent.write_internal(row, Some(ListIndex::new(0, 0)))?;
            self.parent.set_validity_unchecked(row, false);
        }
        Ok(())
    }

    /// Append one element, written by `write` through the element writer at the given child row;
    /// finish the row with [`Self::finish_row`].
    #[inline]
    pub fn push_with(&mut self, write: impl FnOnce(&mut T::Writer<'w>, usize) -> Result<()>) -> Result<()> {
        if self.offset >= self.child_len() {
            self.grow()?;
        }
        write(&mut self.elements, self.offset)?;
        self.offset += 1;
        Ok(())
    }

    /// Append one element to the row being written; `None` is a NULL element.
    #[inline]
    pub fn push(&mut self, value: Option<&<T::Writer<'w> as RowWriter>::Value>) -> Result<()> {
        self.push_with(|elements, index| elements.set_option(index, value))
    }

    /// Make the elements pushed since the last finished row the elements of `row`.
    pub fn finish_row(&mut self, row: usize) -> Result<()> {
        if row >= self.len() {
            return Err(out_of_bounds(row, self.len()));
        }
        if self.offset > self.child_len() {
            return Err(Error::invalid_input(
                "LIST elements were written past the element child",
            ));
        }
        let entry = ListIndex::new(self.row_start as u64, (self.offset - self.row_start) as u64);
        self.row_start = self.offset;
        self.used = self.used.max(self.offset);
        // SAFETY: the row is in bounds, and the entry covers the elements just pushed.
        unsafe { self.write_entry(row, entry) };
        Ok(())
    }

    /// Write a whole row from its elements.
    #[inline]
    pub fn set_elements<'v, I>(&mut self, row: usize, values: I) -> Result<()>
    where
        I: IntoIterator<Item = Option<&'v <T::Writer<'w> as RowWriter>::Value>>,
        <T::Writer<'w> as RowWriter>::Value: 'v,
    {
        if row >= self.len() {
            return Err(out_of_bounds(row, self.len()));
        }
        let values = values.into_iter();
        self.reserve(values.size_hint().0)?;
        for value in values {
            if let Err(error) = self.push(value) {
                self.offset = self.row_start;
                return Err(error);
            }
        }
        self.finish_row(row)
    }

    /// Trim the element child to the elements rows use.
    pub fn finish(mut self) -> Result<()> {
        let keep = self.used.max(self.offset);
        if self.child_len() > keep {
            // Shrinking keeps every element a row uses, including rows written before this writer.
            self.elements.resize_rows(keep)?;
        }
        Ok(())
    }

    #[inline(always)]
    unsafe fn write_entry(&mut self, row: usize, entry: ListIndex) {
        // SAFETY: the caller guarantees the row's bounds and that the entry covers initialized elements.
        unsafe {
            let _ = self.parent.write_internal(row, Some(entry));
            self.parent.set_validity_unchecked(row, true);
        }
    }
}

impl<'w, T> RowWriter for ListWriter<'w, T>
where
    T: WithWriter + Borrow<<T::Writer<'w> as RowWriter>::Value> + 'w,
{
    type Value = List<T>;

    fn len(&self) -> usize {
        ListWriter::len(self)
    }

    #[inline]
    fn set(&mut self, row: usize, value: &List<T>) -> Result<()> {
        self.set_elements(row, value.0.iter().map(|value| value.as_ref().map(Borrow::borrow)))
    }

    fn set_null(&mut self, row: usize) -> Result<()> {
        ListWriter::set_null(self, row)
    }

    fn resize_rows(&mut self, len: usize) -> Result<()> {
        self.parent.resize(len)?;
        self.elements.refresh()
    }

    fn refresh(&mut self) -> Result<()> {
        self.parent.refresh_all()?;
        self.elements.refresh()
    }
}

// SAFETY: rows only reference elements below the child's size, checked against the child itself.
unsafe impl<T> WithWriter for List<T>
where
    T: WithWriter + for<'w> Borrow<<T::Writer<'w> as RowWriter>::Value>,
{
    type Writer<'w>
        = ListWriter<'w, T>
    where
        Self: 'w;

    unsafe fn row_writer<'w>(vector: NonNull<Vector<'w, Self>>) -> Result<Self::Writer<'w>> {
        // SAFETY: the caller upholds the borrow contract.
        unsafe { ListWriter::from_raw(vector) }
    }
}

impl<T: WithWriter> std::fmt::Debug for ListWriter<'_, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ListWriter")
            .field("len", &self.len())
            .field("child_offset", &self.offset)
            .field("child_len", &self.child_len())
            .finish()
    }
}

impl<T: WithWriter> Vector<'_, List<T>> {
    /// Prepare a [`ListWriter`] that appends after the elements already written.
    pub fn list_writer(&mut self) -> Result<ListWriter<'_, T>> {
        // SAFETY: the exclusive borrow covers the writer, and validation checked the type.
        unsafe { ListWriter::from_raw(NonNull::from(self).cast()) }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        DuckDBType, Result,
        connection::Connection,
        data_chunk::DataChunk,
        environment::{Environment, StorageLocation},
        types::{List, ListIndex},
        vector::{RowReader, Vector},
    };

    fn connection() -> Result<Connection> {
        Environment::new()?.open(StorageLocation::InMemory)?.connect()
    }

    fn rows<T: crate::FromValue + crate::vector::NativeElement>(
        vector: &Vector<'_, List<T>>,
    ) -> Result<Vec<Option<List<T>>>> {
        (0..vector.len())
            .map(|row| vector.get_value_slow(row)?.get::<List<T>>())
            .collect()
    }

    fn child_len<T: crate::vector::NativeElement>(vector: &Vector<'_, List<T>>) -> Result<usize> {
        Ok(vector.children().unwrap().get_untyped_vector_at(0)?.len())
    }

    #[test]
    fn list_writer_grows_and_trims_the_child() -> Result<()> {
        let conn = connection()?;
        let mut chunk = DataChunk::create(&conn, &[List::<String>::logical_type(&conn)?], true)?;
        let mut vector = chunk.get_vector_at_mut::<List<String>>(0)?;
        vector.set_size(3)?;
        let long = "a string longer than the inline storage".to_owned();
        let many = (0..5000).map(|index| index.to_string()).collect::<Vec<_>>();
        {
            let mut writer = vector.list_writer()?;
            writer.set_elements(1, many.iter().map(|value| Some(value.as_str())))?;
            writer.set_null(2)?;
            writer.set_elements(0, [Some(long.as_str()), None])?;
            assert_eq!(writer.child_offset(), 5002);
            writer.finish()?;
        }
        assert_eq!(child_len(&vector)?, 5002);
        let read = rows(&vector)?;
        assert_eq!(read[0], Some(List(vec![Some(long), None])));
        assert_eq!(read[1], Some(List(many.into_iter().map(Some).collect())));
        assert_eq!(read[2], None);
        Ok(())
    }

    #[test]
    fn list_writer_checks_entries_against_the_child() -> Result<()> {
        let conn = connection()?;
        let mut chunk = DataChunk::create(&conn, &[List::<i32>::logical_type(&conn)?], true)?;
        let mut vector = chunk.get_vector_at_mut::<List<i32>>(0)?;
        vector.set_size(2)?;
        {
            let mut writer = vector.list_writer()?;
            writer.reserve(3)?;
            assert_eq!(writer.elements().len(), 3);
            writer.elements().set(0, &7)?;
            writer.elements().set(1, &8)?;
            writer.set_entry(0, ListIndex::new(0, 2))?;
            // Unwritten elements below the child's size are initialized.
            writer.set_entry(1, ListIndex::new(2, 1))?;
            for entry in [ListIndex::new(2, 2), ListIndex::new(4, 0), ListIndex::new(u64::MAX, 2)] {
                assert!(writer.set_entry(1, entry).is_err(), "{entry:?}");
            }
            assert!(writer.set_entry(2, ListIndex::new(0, 0)).is_err());
            assert!(writer.set_null(2).is_err());
            assert!(writer.finish_row(2).is_err());
        }
        assert_eq!(child_len(&vector)?, 3);
        assert_eq!(
            rows(&vector)?,
            [Some(List(vec![Some(7), Some(8)])), Some(List(vec![Some(0)]))]
        );
        Ok(())
    }

    #[test]
    fn list_writer_appends_after_earlier_rows() -> Result<()> {
        let conn = connection()?;
        let mut chunk = DataChunk::create(&conn, &[List::<i32>::logical_type(&conn)?], true)?;
        let mut vector = chunk.get_vector_at_mut::<List<i32>>(0)?;
        vector.set_size(3)?;
        vector.set_child_size(2)?;
        vector.writer()?.set(0, &ListIndex::new(0, 2))?;
        {
            let mut writer = vector.list_writer()?;
            assert_eq!(writer.child_offset(), 2);
            writer.set_elements(1, [Some(&9)])?;
            writer.finish()?;
        }
        {
            let mut writer = vector.list_writer()?;
            writer.set_elements(2, [Some(&3)])?;
            writer.finish()?;
        }
        assert_eq!(child_len(&vector)?, 4);
        assert_eq!(
            rows(&vector)?,
            [
                Some(List(vec![Some(0), Some(0)])),
                Some(List(vec![Some(9)])),
                Some(List(vec![Some(3)]))
            ]
        );
        Ok(())
    }

    #[test]
    fn list_writer_writes_nested_lists() -> Result<()> {
        let conn = connection()?;
        let mut chunk = DataChunk::create(&conn, &[List::<List<i32>>::logical_type(&conn)?], true)?;
        let mut vector = chunk.get_vector_at_mut::<List<List<i32>>>(0)?;
        vector.set_size(2)?;
        let inner = [List(vec![Some(1), Some(2)]), List(vec![Some(3)])];
        {
            let mut writer = vector.list_writer()?;
            writer.set_elements(0, inner.iter().map(Some))?;
            writer.set_elements(1, [None])?;
        }
        assert_eq!(
            vector.get_value_slow(0)?.get::<List<List<i32>>>()?,
            Some(List(inner.into_iter().map(Some).collect()))
        );
        assert_eq!(
            vector.get_value_slow(1)?.get::<List<List<i32>>>()?,
            Some(List(vec![None]))
        );
        Ok(())
    }

    #[test]
    fn unfinished_list_writer_leaves_a_readable_vector() -> Result<()> {
        let conn = connection()?;
        let mut chunk = DataChunk::create(&conn, &[List::<String>::logical_type(&conn)?], true)?;
        let mut vector = chunk.get_vector_at_mut::<List<String>>(0)?;
        vector.set_size(1)?;
        let mut writer = vector.list_writer()?;
        writer.reserve(100)?;
        writer.elements().set(0, "written")?;
        writer.set_entry(0, ListIndex::new(0, 100))?;
        let reader = vector.list_reader()?;
        let row = reader.get(0)?.unwrap();
        assert_eq!(row.get(0), Some(Some("written")));
        assert!((1..100).all(|index| row.get(index) == Some(Some(""))));
        Ok(())
    }
}

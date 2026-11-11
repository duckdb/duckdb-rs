use std::{borrow::Borrow, ptr::NonNull};

use super::{RowWriter, Unknown, Vector, VectorWriter, WithWriter, out_of_bounds};
use crate::{Result, error::Error, types::List, types::ListIndex};

/// Writes `LIST` rows into a writable flat vector whose element child is already sized.
///
/// Size the element child with [`Vector::set_child_size`] first: the writer fills it from the first
/// element, through the element type's own [`RowWriter`], so lists of nested types compose. [`Self::set_elements`] writes a whole row; columnar writers fill [`Self::elements`] and
/// point rows at them with [`Self::set_entry`]. Both move [`Self::child_offset`] past the row's elements.
///
/// Every writer starts at the first element, so write all rows through one writer: a second writer
/// overwrites the elements of the rows the first one wrote.
///
/// ```
/// # use duckdb_neo::{DuckDBType, Result, data_chunk::DataChunk, environment::{Environment, StorageLocation}, types::List};
/// # fn main() -> Result<()> {
/// # let conn = Environment::new()?.open(StorageLocation::InMemory)?.connect()?;
/// let mut chunk = DataChunk::create(&conn, &[List::<i32>::logical_type(&conn)?], true)?;
/// let mut vector = chunk.get_vector_at_mut::<List<i32>>(0)?;
/// vector.set_size(2)?;
/// vector.set_child_size(2)?;
/// let mut writer = vector.list_writer()?;
/// writer.set_elements(0, [Some(&1), None])?;
/// writer.set_null(1)?;
/// assert!(writer.set_elements(1, [Some(&3)]).is_err());
/// assert_eq!(vector.get_value_slow(0)?.get::<List<i32>>()?, Some(List(vec![Some(1), None])));
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
/// vector.set_child_size(3)?;
/// let mut writer = vector.list_writer()?;
/// let start = writer.child_offset();
/// for (index, value) in [7, 8, 9].iter().enumerate() {
///     writer.elements().set(start + index, value)?;
/// }
/// writer.set_entry(0, ListIndex::new(start as u64, 2))?;
/// writer.set_entry(1, ListIndex::new(start as u64 + 2, 1))?;
/// assert!(writer.set_entry(1, ListIndex::new(2, 2)).is_err());
/// assert_eq!(writer.child_offset(), 3);
/// # Ok(())
/// # }
/// ```
pub struct ListWriter<'w, T: WithWriter> {
    parent: VectorWriter<'w, List<T>>,
    /// The element child, whose size bounds every entry; the element writer could be swapped out.
    child: NonNull<Vector<'w, Unknown>>,
    elements: T::Writer<'w>,
    /// The first element no row written by this writer uses.
    offset: usize,
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
        Ok(Self {
            parent,
            child,
            elements,
            offset: 0,
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

    /// Return the first element no row written by this writer uses, where [`Self::set_elements`] writes next.
    pub fn child_offset(&self) -> usize {
        self.offset
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
        self.offset = self.offset.max(end as usize);
        // SAFETY: the row is in bounds, and the entry covers initialized child elements.
        unsafe { self.write_entry(row, entry) };
        Ok(())
    }

    /// Make a row SQL NULL.
    pub fn set_null(&mut self, row: usize) -> Result<()> {
        if row >= self.len() {
            return Err(out_of_bounds(row, self.len()));
        }
        // SAFETY: the row is in bounds.
        unsafe { self.parent.set_validity_unchecked(row, false) };
        Ok(())
    }

    /// Write a whole row from its elements, at [`Self::child_offset`].
    ///
    /// Fails, leaving the row unchanged, if the elements do not fit the child.
    #[inline]
    pub fn set_elements<'v, I>(&mut self, row: usize, values: I) -> Result<()>
    where
        I: IntoIterator<Item = Option<&'v <T::Writer<'w> as RowWriter>::Value>>,
        <T::Writer<'w> as RowWriter>::Value: 'v,
    {
        if row >= self.len() {
            return Err(out_of_bounds(row, self.len()));
        }
        let (start, capacity) = (self.offset, self.child_len());
        let mut end = start;
        for value in values {
            if end >= capacity {
                return Err(Error::invalid_input(format!(
                    "LIST elements do not fit the {capacity} child elements; size the child first"
                )));
            }
            self.elements.set_option(end, value)?;
            end += 1;
        }
        self.offset = end;
        // SAFETY: the row is in bounds, and the entry covers the elements just written.
        unsafe { self.write_entry(row, ListIndex::new(start as u64, (end - start) as u64)) };
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

    unsafe fn resize_rows(&mut self, len: usize) -> Result<()> {
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
    /// Prepare a [`ListWriter`] that fills the sized element child from its first element.
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
    fn list_writer_fills_the_sized_child() -> Result<()> {
        let conn = connection()?;
        let mut chunk = DataChunk::create(&conn, &[List::<String>::logical_type(&conn)?], true)?;
        let mut vector = chunk.get_vector_at_mut::<List<String>>(0)?;
        vector.set_size(3)?;
        vector.set_child_size(5002)?;
        let long = "a string longer than the inline storage".to_owned();
        let many = (0..5000).map(|index| index.to_string()).collect::<Vec<_>>();
        {
            let mut writer = vector.list_writer()?;
            writer.set_elements(1, many.iter().map(|value| Some(value.as_str())))?;
            writer.set_null(2)?;
            writer.set_elements(0, [Some(long.as_str()), None])?;
            assert_eq!(writer.child_offset(), 5002);
            let error = writer.set_elements(2, [Some("no room")]).unwrap_err();
            assert!(error.message.contains("do not fit the 5002 child elements"), "{error}");
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
        vector.set_child_size(3)?;
        {
            let mut writer = vector.list_writer()?;
            assert_eq!(writer.elements().len(), 3);
            writer.elements().set(0, &7)?;
            writer.elements().set(1, &8)?;
            writer.set_entry(0, ListIndex::new(0, 2))?;
            // Unwritten elements below the child's size are initialized.
            writer.set_entry(1, ListIndex::new(2, 1))?;
            assert_eq!(writer.child_offset(), 3);
            for entry in [ListIndex::new(2, 2), ListIndex::new(4, 0), ListIndex::new(u64::MAX, 2)] {
                assert!(writer.set_entry(1, entry).is_err(), "{entry:?}");
            }
            assert!(writer.set_entry(2, ListIndex::new(0, 0)).is_err());
            assert!(writer.set_null(2).is_err());
            assert!(writer.set_elements(2, []).is_err());
        }
        assert_eq!(
            rows(&vector)?,
            [Some(List(vec![Some(7), Some(8)])), Some(List(vec![Some(0)]))]
        );
        Ok(())
    }

    #[test]
    fn set_entry_moves_the_offset_past_its_elements() -> Result<()> {
        let conn = connection()?;
        let mut chunk = DataChunk::create(&conn, &[List::<i32>::logical_type(&conn)?], true)?;
        let mut vector = chunk.get_vector_at_mut::<List<i32>>(0)?;
        vector.set_size(2)?;
        vector.set_child_size(3)?;
        {
            let mut writer = vector.list_writer()?;
            writer.elements().set(0, &7)?;
            writer.elements().set(1, &8)?;
            writer.set_entry(0, ListIndex::new(0, 2))?;
            writer.set_elements(1, [Some(&9)])?;
        }
        assert_eq!(
            rows(&vector)?,
            [Some(List(vec![Some(7), Some(8)])), Some(List(vec![Some(9)]))]
        );
        Ok(())
    }

    #[test]
    fn list_writer_writes_nested_lists() -> Result<()> {
        let conn = connection()?;
        let mut chunk = DataChunk::create(&conn, &[List::<List<i32>>::logical_type(&conn)?], true)?;
        let mut vector = chunk.get_vector_at_mut::<List<List<i32>>>(0)?;
        vector.set_size(2)?;
        vector.set_child_size(3)?;
        vector
            .children_mut()
            .unwrap()
            .get_vector_at_mut::<List<i32>>(0)?
            .set_child_size(3)?;
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
    fn unwritten_elements_read_as_initialized() -> Result<()> {
        let conn = connection()?;
        let mut chunk = DataChunk::create(&conn, &[List::<String>::logical_type(&conn)?], true)?;
        let mut vector = chunk.get_vector_at_mut::<List<String>>(0)?;
        vector.set_size(1)?;
        vector.set_child_size(100)?;
        let mut writer = vector.list_writer()?;
        writer.elements().set(0, "written")?;
        writer.set_entry(0, ListIndex::new(0, 100))?;
        let reader = vector.list_reader()?;
        let row = reader.get(0)?.unwrap();
        assert_eq!(row.get(0), Some(Some("written")));
        assert!((1..100).all(|index| row.get(index) == Some(Some(""))));
        Ok(())
    }

    #[test]
    fn untyped_structs_and_unions_compose_into_lists() -> Result<()> {
        use crate::{
            logical_type::LogicalType,
            types::{Struct, Union},
            vector::RowReader,
        };
        let conn = connection()?;
        let structs = LogicalType::from_text(&conn, "STRUCT(a INTEGER)[]")?;
        let unions = LogicalType::from_text(&conn, "UNION(n INTEGER, s VARCHAR)[]")?;
        let mut chunk = DataChunk::create(&conn, &[structs, unions], true)?;
        let mut vector = chunk.get_vector_at_mut::<List<Struct>>(0)?;
        vector.set_size(1)?;
        vector.set_child_size(2)?;
        vector.list_writer()?.set_elements(0, [Some(&()), None])?;
        assert_eq!(vector.get_value_slow(0)?.dbg_string()?, "[{'a': 0}, NULL]");
        let reader = vector.list_reader()?;
        assert_eq!(reader.get(0)?.unwrap().iter().collect::<Vec<_>>(), [Some(()), None]);
        let mut unions = chunk.get_vector_at_mut::<List<Union>>(1)?;
        unions.set_size(1)?;
        let reader = unions.list_reader()?;
        assert_eq!(reader.get(0)?.unwrap().len(), 0);
        Ok(())
    }
}

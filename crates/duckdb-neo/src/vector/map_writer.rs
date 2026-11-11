use std::{borrow::Borrow, ptr::NonNull};

use super::{RowWriter, Unknown, Vector, VectorWriter, WithWriter, out_of_bounds};
use crate::{
    Result,
    error::Error,
    types::{ListIndex, Map, Struct},
};

/// Writes `MAP` rows into a writable flat vector whose entries child is already sized.
///
/// Works like [`ListWriter`](super::ListWriter): size the entries with [`Vector::set_child_size`]
/// first, then fill them from the first entry by writing whole rows with [`Self::set_entries`], or fill [`Self::keys`] and
/// [`Self::values`] and point rows at them with [`Self::set_entry`]. As with lists, write all rows
/// through one writer. DuckDB requires unique, non-NULL keys per row, which the writer does not check.
///
/// ```
/// # use duckdb_neo::{DuckDBType, Result, data_chunk::DataChunk, environment::{Environment, StorageLocation}, types::Map};
/// # fn main() -> Result<()> {
/// # let conn = Environment::new()?.open(StorageLocation::InMemory)?.connect()?;
/// let mut chunk = DataChunk::create(&conn, &[Map::<String, i32>::logical_type(&conn)?], true)?;
/// let mut vector = chunk.get_vector_at_mut::<Map<String, i32>>(0)?;
/// vector.set_size(2)?;
/// vector.set_child_size(2)?;
/// let mut writer = vector.map_writer()?;
/// writer.set_entries(0, [("a", Some(&1)), ("b", None)])?;
/// writer.set_null(1)?;
/// let row = vector.get_value_slow(0)?.get::<Map<String, i32>>()?.unwrap();
/// assert_eq!(row, Map(vec![("a".to_owned(), Some(1)), ("b".to_owned(), None)]));
/// # Ok(())
/// # }
/// ```
pub struct MapWriter<'w, K: WithWriter, V: WithWriter> {
    parent: VectorWriter<'w, Map<K, V>>,
    /// The entries child, whose size bounds every entry; the field writers could be swapped out.
    entries: VectorWriter<'w, Struct>,
    keys: K::Writer<'w>,
    values: V::Writer<'w>,
    /// The first entry no row written by this writer uses.
    offset: usize,
}

impl<'w, K: WithWriter + 'w, V: WithWriter + 'w> MapWriter<'w, K, V> {
    /// # Safety
    /// As for [`WithWriter::row_writer`].
    pub(super) unsafe fn from_raw(mut vector: NonNull<Vector<'w, Map<K, V>>>) -> Result<Self> {
        // SAFETY: the caller guarantees an exclusive borrow of a validated, writable vector.
        let borrowed = unsafe { vector.as_mut() };
        let entries = borrowed
            .children
            .as_mut()
            .expect("validated MAP vector has entries")
            .cached_iter_mut()?
            .next()
            .expect("validated MAP vector has entries");
        let mut entries = NonNull::from(entries).cast::<Vector<'w, Struct>>();
        // SAFETY: the entries wrapper lives as long as the borrowed vector.
        let mut fields = unsafe { entries.as_mut() }
            .children
            .as_mut()
            .expect("validated MAP entries have two fields")
            .cached_iter_mut()?
            .map(|field| NonNull::from(field).cast::<Vector<'w, Unknown>>());
        let (key, value) = (fields.next().unwrap(), fields.next().unwrap());
        // SAFETY: the writer keeps `vector` exclusively borrowed and only touches its entries and their
        // disjoint fields, which `Map<K, V>::validate` checked as `K` and `V`. Resizing the entries keeps
        // the field wrappers.
        let (parent, entries, keys, values) = unsafe {
            (
                VectorWriter::from_raw(vector)?,
                VectorWriter::from_raw(entries)?,
                K::row_writer(key.cast())?,
                V::row_writer(value.cast())?,
            )
        };
        Ok(Self {
            parent,
            entries,
            keys,
            values,
            offset: 0,
        })
    }

    fn entries_len(&self) -> usize {
        self.entries.vector().len()
    }

    /// Return the number of rows.
    pub fn len(&self) -> usize {
        self.parent.len()
    }

    /// Return whether the vector has no rows.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Return the first entry no row written by this writer uses, where [`Self::set_entries`] writes next.
    pub fn child_offset(&self) -> usize {
        self.offset
    }

    /// Borrow the key writer, for columnar writes; point rows at the entries with [`Self::set_entry`].
    pub fn keys(&mut self) -> &mut K::Writer<'w> {
        &mut self.keys
    }

    /// Borrow the value writer, for columnar writes.
    pub fn values(&mut self) -> &mut V::Writer<'w> {
        &mut self.values
    }

    /// Point a row at entries of the child, checked against the child's size.
    pub fn set_entry(&mut self, row: usize, entry: ListIndex) -> Result<()> {
        if row >= self.len() {
            return Err(out_of_bounds(row, self.len()));
        }
        let end = entry
            .offset
            .checked_add(entry.length)
            .filter(|end| *end <= self.entries_len() as u64)
            .ok_or_else(|| {
                Error::invalid_input(format!(
                    "MAP entry {}..+{} is past the {} entries",
                    entry.offset,
                    entry.length,
                    self.entries_len()
                ))
            })?;
        self.offset = self.offset.max(end as usize);
        // SAFETY: the row is in bounds, and the entry covers initialized entries.
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

    /// Write a whole row from its keys and values, at [`Self::child_offset`]; `None` is a NULL value.
    ///
    /// Fails, leaving the row unchanged, if the entries do not fit the child.
    #[inline]
    pub fn set_entries<'v, I>(&mut self, row: usize, entries: I) -> Result<()>
    where
        I: IntoIterator<
            Item = (
                &'v <K::Writer<'w> as RowWriter>::Value,
                Option<&'v <V::Writer<'w> as RowWriter>::Value>,
            ),
        >,
        <K::Writer<'w> as RowWriter>::Value: 'v,
        <V::Writer<'w> as RowWriter>::Value: 'v,
    {
        if row >= self.len() {
            return Err(out_of_bounds(row, self.len()));
        }
        let (start, capacity) = (self.offset, self.entries_len());
        let mut end = start;
        for (key, value) in entries {
            if end >= capacity {
                return Err(Error::invalid_input(format!(
                    "MAP entries do not fit the {capacity} child entries; size the child first"
                )));
            }
            self.keys.set(end, key)?;
            self.values.set_option(end, value)?;
            end += 1;
        }
        self.offset = end;
        // SAFETY: the row is in bounds, and the entry covers the entries just written.
        unsafe { self.write_entry(row, ListIndex::new(start as u64, (end - start) as u64)) };
        Ok(())
    }

    #[inline(always)]
    unsafe fn write_entry(&mut self, row: usize, entry: ListIndex) {
        // SAFETY: the caller guarantees the row's bounds and that the entry covers initialized entries,
        // which MAP entries make valid.
        unsafe {
            self.entries
                .mark_valid_range(entry.offset as usize, (entry.offset + entry.length) as usize);
            let _ = self.parent.write_internal(row, Some(entry));
            self.parent.set_validity_unchecked(row, true);
        }
    }
}

impl<'w, K, V> RowWriter for MapWriter<'w, K, V>
where
    K: WithWriter + Borrow<<K::Writer<'w> as RowWriter>::Value> + 'w,
    V: WithWriter + Borrow<<V::Writer<'w> as RowWriter>::Value> + 'w,
{
    type Value = Map<K, V>;

    fn len(&self) -> usize {
        MapWriter::len(self)
    }

    #[inline]
    fn set(&mut self, row: usize, value: &Map<K, V>) -> Result<()> {
        self.set_entries(
            row,
            value
                .0
                .iter()
                .map(|(key, value)| (key.borrow(), value.as_ref().map(Borrow::borrow))),
        )
    }

    fn set_null(&mut self, row: usize) -> Result<()> {
        MapWriter::set_null(self, row)
    }

    unsafe fn resize_rows(&mut self, len: usize) -> Result<()> {
        self.parent.resize(len)?;
        self.entries.refresh_all()?;
        self.keys.refresh()?;
        self.values.refresh()
    }

    fn refresh(&mut self) -> Result<()> {
        self.parent.refresh_all()?;
        self.entries.refresh_all()?;
        self.keys.refresh()?;
        self.values.refresh()
    }
}

// SAFETY: rows only reference entries below the child's size, checked against the child itself.
unsafe impl<K, V> WithWriter for Map<K, V>
where
    K: WithWriter + for<'w> Borrow<<K::Writer<'w> as RowWriter>::Value>,
    V: WithWriter + for<'w> Borrow<<V::Writer<'w> as RowWriter>::Value>,
{
    type Writer<'w>
        = MapWriter<'w, K, V>
    where
        Self: 'w;

    unsafe fn row_writer<'w>(vector: NonNull<Vector<'w, Self>>) -> Result<Self::Writer<'w>> {
        // SAFETY: the caller upholds the borrow contract.
        unsafe { MapWriter::from_raw(vector) }
    }
}

impl<K: WithWriter, V: WithWriter> std::fmt::Debug for MapWriter<'_, K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MapWriter")
            .field("len", &self.len())
            .field("child_offset", &self.offset)
            .field("child_len", &self.entries_len())
            .finish()
    }
}

impl<K: WithWriter, V: WithWriter> Vector<'_, Map<K, V>> {
    /// Prepare a [`MapWriter`] that fills the sized entries child from its first entry.
    pub fn map_writer(&mut self) -> Result<MapWriter<'_, K, V>> {
        // SAFETY: the exclusive borrow covers the writer, and validation checked the type.
        unsafe { MapWriter::from_raw(NonNull::from(self).cast()) }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        DuckDBType, Result,
        connection::Connection,
        data_chunk::DataChunk,
        environment::{Environment, StorageLocation},
        types::{List, ListIndex, Map},
        vector::{RowReader, RowWriter, Unknown},
    };

    fn connection() -> Result<Connection> {
        Environment::new()?.open(StorageLocation::InMemory)?.connect()
    }

    #[test]
    fn map_writer_fills_the_sized_entries() -> Result<()> {
        let conn = connection()?;
        let mut chunk = DataChunk::create(&conn, &[Map::<String, i32>::logical_type(&conn)?], true)?;
        let mut vector = chunk.get_vector_at_mut::<Map<String, i32>>(0)?;
        vector.set_size(3)?;
        vector.set_child_size(5000)?;
        let keys = (0..5000).map(|index| format!("key number {index}")).collect::<Vec<_>>();
        let values = (0..5000).collect::<Vec<i32>>();
        {
            let mut writer = vector.map_writer()?;
            writer.set_null(1)?;
            writer.set_entries(
                2,
                keys.iter()
                    .zip(&values)
                    .map(|(key, value)| (key.as_str(), (value % 3 != 0).then_some(value))),
            )?;
            writer.set_entries(0, [])?;
            assert_eq!(writer.child_offset(), 5000);
            let error = writer.set_entries(0, [("no room", None)]).unwrap_err();
            assert!(error.message.contains("do not fit the 5000 child entries"), "{error}");
        }
        let reader = vector.map_reader()?;
        assert!(reader.get(0)?.unwrap().is_empty());
        assert!(reader.get(1)?.is_none());
        let row = reader.get(2)?.unwrap();
        assert_eq!(row.len(), 5000);
        for (index, (key, value)) in row.iter().enumerate() {
            assert_eq!(key, keys[index]);
            assert_eq!(value.copied(), (index % 3 != 0).then_some(index as i32));
        }
        Ok(())
    }

    #[test]
    fn map_writer_writes_nested_values() -> Result<()> {
        let conn = connection()?;
        let mut chunk = DataChunk::create(&conn, &[Map::<i32, List<i32>>::logical_type(&conn)?], true)?;
        let mut vector = chunk.get_vector_at_mut::<Map<i32, List<i32>>>(0)?;
        vector.set_size(2)?;
        vector.set_child_size(3)?;
        {
            let mut children = vector.children_mut().unwrap();
            let mut entries = children.get_vector_at_mut::<Unknown>(0)?;
            let mut fields = entries.children_mut().unwrap();
            fields.get_vector_at_mut::<List<i32>>(1)?.set_child_size(3)?;
        }
        {
            let mut writer = vector.map_writer()?;
            writer.set(0, &Map(vec![(1, Some(List(vec![Some(1)])))]))?;
            writer.set_entries(1, [(&2, Some(&List(vec![Some(2), None]))), (&3, None)])?;
        }
        assert_eq!(
            vector.get_value_slow(0)?.get::<Map<i32, List<i32>>>()?,
            Some(Map(vec![(1, Some(List(vec![Some(1)])))]))
        );
        assert_eq!(
            vector.get_value_slow(1)?.get::<Map<i32, List<i32>>>()?,
            Some(Map(vec![(2, Some(List(vec![Some(2), None]))), (3, None)]))
        );
        Ok(())
    }

    #[test]
    fn map_writer_points_rows_at_columnar_entries() -> Result<()> {
        let conn = connection()?;
        let mut chunk = DataChunk::create(&conn, &[Map::<String, String>::logical_type(&conn)?], true)?;
        let mut vector = chunk.get_vector_at_mut::<Map<String, String>>(0)?;
        vector.set_size(2)?;
        vector.set_child_size(3)?;
        {
            let mut writer = vector.map_writer()?;
            writer.keys().set(0, "a")?;
            writer.values().set(0, "x")?;
            writer.keys().set(1, "b")?;
            writer.values().set_null(1)?;
            writer.set_entry(0, ListIndex::new(0, 2))?;
            assert_eq!(writer.child_offset(), 2);
            writer.set_entries(1, [("c", Some("z"))])?;
            assert!(writer.set_entry(1, ListIndex::new(2, 2)).is_err());
        }
        let reader = vector.map_reader()?;
        let rows = (0..2)
            .map(|row| {
                reader
                    .get(row)
                    .map(|entries| entries.unwrap().iter().collect::<Vec<_>>())
            })
            .collect::<Result<Vec<_>>>()?;
        assert_eq!(rows, [vec![("a", Some("x")), ("b", None)], vec![("c", Some("z"))]]);
        Ok(())
    }
}

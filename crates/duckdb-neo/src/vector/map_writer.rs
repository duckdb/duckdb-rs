use std::{borrow::Borrow, ptr::NonNull};

use super::{RowWriter, Unknown, Vector, VectorWriter, WithWriter, out_of_bounds};
use crate::{
    Result,
    error::Error,
    types::{ListIndex, Map, Struct},
};

/// Appends `MAP` rows to a writable flat vector, growing the entries child as needed.
///
/// Works like [`ListWriter`](super::ListWriter): entries are appended after the written ones,
/// through the key and value types' own [`RowWriter`]s, and [`Self::finish`] trims the entries to
/// the ones the rows use. DuckDB requires unique keys per row, which the writer does not check.
///
/// ```
/// # use duckdb_neo::{DuckDBType, Result, data_chunk::DataChunk, environment::{Environment, StorageLocation}, types::Map};
/// # fn main() -> Result<()> {
/// # let conn = Environment::new()?.open(StorageLocation::InMemory)?.connect()?;
/// let mut chunk = DataChunk::create(&conn, &[Map::<String, i32>::logical_type(&conn)?], true)?;
/// let mut vector = chunk.get_vector_at_mut::<Map<String, i32>>(0)?;
/// vector.set_size(2)?;
/// let mut writer = vector.map_writer()?;
/// writer.push("a", Some(&1))?;
/// writer.push("b", None)?;
/// writer.finish_row(0)?;
/// writer.set_null(1)?;
/// writer.finish()?;
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
    /// The first entry of the row being appended.
    row_start: usize,
    /// The next entry to append.
    offset: usize,
    /// One past the last entry any row uses; entries before the writer's start are kept.
    used: usize,
}

impl<'w, K: WithWriter + 'w, V: WithWriter + 'w> MapWriter<'w, K, V> {
    /// # Safety
    /// As for [`WithWriter::row_writer`].
    pub(super) unsafe fn from_raw(mut vector: NonNull<Vector<'w, Map<K, V>>>) -> Result<Self> {
        // SAFETY: the caller guarantees an exclusive borrow of a validated, writable vector.
        let borrowed = unsafe { vector.as_mut() };
        let offset = borrowed.child_offset();
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
        let used = entries.len();
        Ok(Self {
            parent,
            entries,
            keys,
            values,
            row_start: offset,
            offset,
            used,
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

    /// Return the next entry [`Self::push`] writes.
    pub fn child_offset(&self) -> usize {
        self.offset
    }

    /// Grow the entries so that `additional` entries fit after [`Self::child_offset`].
    pub fn reserve(&mut self, additional: usize) -> Result<()> {
        let needed = self
            .offset
            .checked_add(additional)
            .ok_or_else(|| Error::invalid_input("MAP entry count overflow"))?;
        if needed > self.entries_len() {
            self.resize_entries(needed)?;
        }
        Ok(())
    }

    #[cold]
    fn grow(&mut self) -> Result<()> {
        let len = self.entries_len();
        self.resize_entries(len.saturating_mul(2).max(len + 1).max(64))
    }

    fn resize_entries(&mut self, len: usize) -> Result<()> {
        let old = self.entries_len();
        self.entries.resize(len)?;
        self.keys.refresh()?;
        self.values.refresh()?;
        // SAFETY: MAP entries are never NULL; the resize initialized both fields.
        unsafe { self.entries.mark_valid_range(old, self.entries.len()) };
        Ok(())
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
        self.used = self.used.max(end as usize);
        // SAFETY: the row is in bounds, and the entry covers initialized entries.
        unsafe { self.write_entry(row, entry) };
        Ok(())
    }

    /// Make a row SQL NULL, with an empty entry.
    pub fn set_null(&mut self, row: usize) -> Result<()> {
        if row >= self.len() {
            return Err(out_of_bounds(row, self.len()));
        }
        // SAFETY: the row is in bounds, and an empty entry references no entries.
        unsafe {
            self.parent.write_internal(row, Some(ListIndex::new(0, 0)))?;
            self.parent.set_validity_unchecked(row, false);
        }
        Ok(())
    }

    /// Append one entry, written by `write` through the key and value writers at the given child row;
    /// finish the row with [`Self::finish_row`]. The key must be written: DuckDB forbids NULL keys.
    #[inline]
    pub fn push_with(
        &mut self,
        write: impl FnOnce(&mut K::Writer<'w>, &mut V::Writer<'w>, usize) -> Result<()>,
    ) -> Result<()> {
        if self.offset >= self.entries_len() {
            self.grow()?;
        }
        write(&mut self.keys, &mut self.values, self.offset)?;
        self.offset += 1;
        Ok(())
    }

    /// Append one entry to the row being written; `None` is a NULL value.
    #[inline]
    pub fn push(
        &mut self,
        key: &<K::Writer<'w> as RowWriter>::Value,
        value: Option<&<V::Writer<'w> as RowWriter>::Value>,
    ) -> Result<()> {
        self.push_with(|keys, values, index| {
            keys.set(index, key)?;
            values.set_option(index, value)
        })
    }

    /// Make the entries pushed since the last finished row the entries of `row`.
    pub fn finish_row(&mut self, row: usize) -> Result<()> {
        if row >= self.len() {
            return Err(out_of_bounds(row, self.len()));
        }
        if self.offset > self.entries_len() {
            return Err(Error::invalid_input("MAP entries were written past the entries child"));
        }
        let entry = ListIndex::new(self.row_start as u64, (self.offset - self.row_start) as u64);
        self.row_start = self.offset;
        self.used = self.used.max(self.offset);
        self.parent.vector().child_offset_cell().set(self.offset);
        // SAFETY: the row is in bounds, and the entry covers the entries just pushed.
        unsafe { self.write_entry(row, entry) };
        Ok(())
    }

    /// Trim the entries to the ones rows use.
    pub fn finish(mut self) -> Result<()> {
        let keep = self.used.max(self.offset);
        if self.entries_len() > keep {
            // Shrinking keeps every entry a row uses, including rows written before this writer.
            self.resize_entries(keep)?;
        }
        Ok(())
    }

    #[inline(always)]
    unsafe fn write_entry(&mut self, row: usize, entry: ListIndex) {
        // SAFETY: the caller guarantees the row's bounds and that the entry covers initialized entries.
        unsafe {
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
        self.reserve(value.0.len())?;
        for (key, value) in &value.0 {
            if let Err(error) = self.push(key.borrow(), value.as_ref().map(Borrow::borrow)) {
                self.offset = self.row_start;
                return Err(error);
            }
        }
        self.finish_row(row)
    }

    fn set_null(&mut self, row: usize) -> Result<()> {
        MapWriter::set_null(self, row)
    }

    fn resize_rows(&mut self, len: usize) -> Result<()> {
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
    /// Prepare a [`MapWriter`] that appends after the entries already written.
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
        types::{List, Map},
    };

    fn connection() -> Result<Connection> {
        Environment::new()?.open(StorageLocation::InMemory)?.connect()
    }

    #[test]
    fn map_writer_grows_and_trims_the_entries() -> Result<()> {
        let conn = connection()?;
        let mut chunk = DataChunk::create(&conn, &[Map::<String, i32>::logical_type(&conn)?], true)?;
        let mut vector = chunk.get_vector_at_mut::<Map<String, i32>>(0)?;
        vector.set_size(3)?;
        let keys = (0..5000).map(|index| format!("key number {index}")).collect::<Vec<_>>();
        {
            let mut writer = vector.map_writer()?;
            writer.set_null(1)?;
            for (index, key) in keys.iter().enumerate() {
                writer.push(key, (index % 3 != 0).then_some(&(index as i32)))?;
            }
            writer.finish_row(2)?;
            writer.finish_row(0)?;
            assert_eq!(writer.child_offset(), 5000);
            writer.finish()?;
        }
        assert_eq!(vector.children().unwrap().get_untyped_vector_at(0)?.len(), 5000);
        assert_eq!(vector.child_offset(), 5000);
        let reader = vector.reader()?;
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
    fn map_writer_writes_nested_values_after_earlier_rows() -> Result<()> {
        let conn = connection()?;
        let mut chunk = DataChunk::create(&conn, &[Map::<i32, List<i32>>::logical_type(&conn)?], true)?;
        let mut vector = chunk.get_vector_at_mut::<Map<i32, List<i32>>>(0)?;
        vector.set_size(2)?;
        // SAFETY: row 0 writes the only entry.
        unsafe { vector.set_child_size(1)? };
        {
            let mut children = vector.children_mut().unwrap();
            let mut entries = children.get_vector_at_mut::<crate::types::Struct>(0)?;
            let mut fields = entries.children_mut().unwrap();
            // SAFETY: the rows below only use the inner elements they write.
            unsafe { fields.get_vector_at_mut::<List<i32>>(1)?.set_child_size(3)? };
        }
        vector
            .writer()?
            .set(0, Some(&Map(vec![(1, Some(List(vec![Some(1)])))])))?;
        {
            let mut writer = vector.map_writer()?;
            writer.push(&2, Some(&List(vec![Some(2), None])))?;
            writer.push(&3, None)?;
            writer.finish_row(1)?;
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
    fn leaked_map_writer_leaves_a_readable_vector() -> Result<()> {
        let conn = connection()?;
        let mut chunk = DataChunk::create(&conn, &[Map::<String, String>::logical_type(&conn)?], true)?;
        let mut vector = chunk.get_vector_at_mut::<Map<String, String>>(0)?;
        vector.set_size(1)?;
        let mut writer = vector.map_writer()?;
        writer.reserve(100)?;
        writer.push("key", Some("value"))?;
        writer.finish_row(0)?;
        std::mem::forget(writer);
        let reader = vector.reader()?;
        let row = reader.get(0)?.unwrap();
        assert_eq!(row.iter().collect::<Vec<_>>(), [("key", Some("value"))]);
        assert_eq!(vector.children().unwrap().get_untyped_vector_at(0)?.len(), 100);
        Ok(())
    }
}

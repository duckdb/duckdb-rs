use std::ptr::NonNull;

use super::{Unknown, Vector, VectorElement, VectorWriter, WritableVectorElement, out_of_bounds};
use crate::{
    Result,
    error::Error,
    types::{ListIndex, Map, Struct},
};

/// Appends `MAP` rows to a writable flat vector, growing the entries child as needed.
///
/// Works like [`ListWriter`](super::ListWriter): entries are appended after the written ones,
/// and [`Self::finish`], or dropping the writer, trims the entries to what the rows use.
/// DuckDB requires unique keys per row, which the writer does not check.
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
pub struct MapWriter<'w, K: VectorElement, V: VectorElement> {
    parent: VectorWriter<'w, Map<K, V>>,
    entries: VectorWriter<'w, Struct>,
    /// Key and value writers; `None` while a resize rebuilds the field wrappers they point to.
    fields: Option<(VectorWriter<'w, K>, VectorWriter<'w, V>)>,
    /// The first entry of the row being appended.
    row_start: usize,
    /// The next entry to append.
    offset: usize,
    /// One past the last entry any row uses; entries before the writer's start are kept.
    used: usize,
}

impl<'w, K: VectorElement, V: VectorElement> MapWriter<'w, K, V> {
    pub(super) fn new(vector: &'w mut Vector<'_, Map<K, V>>) -> Result<Self> {
        let offset = vector.child_offset();
        let entries = vector
            .children
            .as_mut()
            .expect("validated MAP vector has entries")
            .cached_iter_mut()?
            .next()
            .expect("validated MAP vector has entries");
        let entries = NonNull::from(entries).cast::<Vector<'w, Struct>>();
        // SAFETY: the writer keeps `vector` exclusively borrowed and only touches its entries,
        // which `Map<K, V>::validate` checked as STRUCT(key K, value V).
        let (parent, mut entries) = unsafe {
            (
                VectorWriter::from_raw(NonNull::from(vector).cast())?,
                VectorWriter::from_raw(entries)?,
            )
        };
        let fields = Some(Self::fields(&mut entries)?);
        let used = entries.len();
        Ok(Self {
            parent,
            entries,
            fields,
            row_start: offset,
            offset,
            used,
        })
    }

    fn fields(entries: &mut VectorWriter<'w, Struct>) -> Result<(VectorWriter<'w, K>, VectorWriter<'w, V>)> {
        let mut fields = entries
            .vector_mut()
            .children
            .as_mut()
            .expect("validated MAP entries have two fields")
            .cached_iter_mut()?
            .map(|field| NonNull::from(field).cast::<Vector<'w, Unknown>>());
        let (key, value) = (fields.next().unwrap(), fields.next().unwrap());
        // SAFETY: the fields are disjoint, validated as K and V, and rebuilt after every resize of the entries.
        unsafe {
            Ok((
                VectorWriter::from_raw(key.cast())?,
                VectorWriter::from_raw(value.cast())?,
            ))
        }
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
        if needed > self.entries.len() {
            self.resize(needed)?;
        }
        Ok(())
    }

    #[cold]
    fn grow(&mut self) -> Result<()> {
        let len = self.entries.len();
        self.resize(len.saturating_mul(2).max(len + 1).max(64))
    }

    fn resize(&mut self, len: usize) -> Result<()> {
        let old = self.entries.len();
        // Resizing the entries rebuilds the field wrappers, so drop the writers that point to them first.
        self.fields = None;
        let result = self.entries.resize(len);
        self.fields = Some(Self::fields(&mut self.entries)?);
        // SAFETY: MAP entries are never NULL; the resize initialized both fields.
        unsafe { self.entries.mark_valid_range(old, self.entries.len()) };
        result
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

    /// Make the entries pushed since the last finished row the entries of `row`.
    pub fn finish_row(&mut self, row: usize) -> Result<()> {
        if row >= self.len() {
            return Err(out_of_bounds(row, self.len()));
        }
        let entry = ListIndex::new(self.row_start as u64, (self.offset - self.row_start) as u64);
        self.row_start = self.offset;
        self.used = self.used.max(self.offset);
        // SAFETY: the row is in bounds, and the entry covers the entries just pushed.
        unsafe {
            let _ = self.parent.write_internal(row, Some(entry));
            self.parent.set_validity_unchecked(row, true);
        }
        Ok(())
    }

    /// Trim the entries to the ones rows use.
    pub fn finish(mut self) -> Result<()> {
        self.trim()
    }

    fn trim(&mut self) -> Result<()> {
        self.parent.vector().child_offset_cell().set(self.offset);
        let keep = self.used.max(self.offset);
        if self.entries.len() > keep {
            // Shrinking keeps every entry a row uses, including rows written before this writer.
            self.resize(keep)?;
        }
        Ok(())
    }
}

impl<K: WritableVectorElement, V: WritableVectorElement> MapWriter<'_, K, V> {
    /// Append one entry to the row being written; finish the row with [`Self::finish_row`].
    #[inline]
    pub fn push(&mut self, key: &K::Write, value: Option<&V::Write>) -> Result<()> {
        if self.offset == self.entries.len() {
            self.grow()?;
        }
        let Some((keys, values)) = &mut self.fields else {
            return Err(Error::invalid_input("MAP writer lost its fields in a failed resize"));
        };
        // SAFETY: the entry is in bounds after growing, and growing marked it valid.
        unsafe {
            keys.set_unchecked(self.offset, Some(key))?;
            values.set_unchecked(self.offset, value)?;
        }
        self.offset += 1;
        Ok(())
    }
}

impl<K: VectorElement, V: VectorElement> Drop for MapWriter<'_, K, V> {
    fn drop(&mut self) {
        let _ = self.trim();
    }
}

impl<K: VectorElement, V: VectorElement> std::fmt::Debug for MapWriter<'_, K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MapWriter")
            .field("len", &self.len())
            .field("child_offset", &self.offset)
            .field("child_len", &self.entries.len())
            .finish()
    }
}

impl<K: VectorElement, V: VectorElement> Vector<'_, Map<K, V>> {
    /// Prepare a [`MapWriter`] that appends after the entries already written.
    pub fn map_writer(&mut self) -> Result<MapWriter<'_, K, V>> {
        MapWriter::new(self)
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

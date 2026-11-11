//! The `MAP` logical type.

use std::fmt::Debug;

use super::{DuckDBType, FromValue, ToValue};
use crate::{
    Parameters, Result,
    connection::FFILink,
    data_chunk::VectorCollection,
    error::Error,
    ffi,
    logical_type::{LogicalType, LogicalTypeID},
    types::Struct,
    value::{Value, ValueInput},
    vector::{HasChildren, NativeElement, Vector, VectorWriter, WritableNativeElement},
};

/// A `MAP` row of `(key, value)` entries in storage order; a `None` value is SQL NULL.
///
/// As a vector element, `Map<K, V>` reads and writes `MAP` vectors with key type `K` and value type `V`.
/// Like a `LIST`, the entries child is sized with [`Vector::set_child_size`] and each row takes the next
/// entries; the [`List`](super::List) docs describe initialization and trimming to
/// [`Vector::child_offset`]. DuckDB requires unique keys, which writes do not check.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Map<K, V>(pub Vec<(K, Option<V>)>);

impl<K, V> Default for Map<K, V> {
    fn default() -> Self {
        Self(Vec::new())
    }
}

impl<K, V> From<Vec<(K, Option<V>)>> for Map<K, V> {
    fn from(entries: Vec<(K, Option<V>)>) -> Self {
        Self(entries)
    }
}

impl<K, V> FromIterator<(K, Option<V>)> for Map<K, V> {
    fn from_iter<T: IntoIterator<Item = (K, Option<V>)>>(iter: T) -> Self {
        Self(iter.into_iter().collect())
    }
}

impl<K: DuckDBType, V: DuckDBType> DuckDBType for Map<K, V> {
    fn logical_type<C: FFILink + ?Sized>(link: &C) -> Result<LogicalType> {
        let key_type = Value::from_logical_type(link, &K::logical_type(link)?)?;
        let value_type = Value::from_logical_type(link, &V::logical_type(link)?)?;
        link.logical_type_create("MAP", Parameters::positional(&[&key_type, &value_type]))
    }
}

impl<K: ToValue + DuckDBType, V: ToValue + DuckDBType> ToValue for Map<K, V> {
    fn value<C: FFILink + ?Sized>(&self, link: &C) -> Result<Value> {
        let mut keys = Vec::with_capacity(self.0.len());
        let mut values = Vec::with_capacity(self.0.len());
        for (key, value) in &self.0 {
            keys.push(key.value(link)?);
            values.push(value.value(link)?);
        }

        let key_type = K::logical_type(link)?;
        let value_type = V::logical_type(link)?;
        link.create_value(ValueInput::Map {
            key_type: &key_type,
            value_type: &value_type,
            keys: &keys,
            values: &values,
        })
    }
}

impl<K: FromValue, V: FromValue> FromValue for Map<K, V> {
    fn _get_inner(value: &Value) -> Result<Self> {
        let logical_type = value.logical_type()?;
        if logical_type.id()? != LogicalTypeID::Map {
            return Err(Error::invalid_input(format!(
                "Expected MAP value, found {}",
                logical_type.to_text()?
            )));
        }

        let children = value.children()?;
        if children.len() % 2 != 0 {
            return Err(Error::invalid_input(format!(
                "MAP exposed an odd number of children: {}",
                children.len()
            )));
        }

        children
            .as_chunks::<2>()
            .0
            .iter()
            .enumerate()
            .map(|(index, entry)| {
                let key = K::from_value(&entry[0])?
                    .ok_or_else(|| Error::invalid_input(format!("MAP entry {index} has a NULL key")))?;
                Ok((key, V::from_value(&entry[1])?))
            })
            .collect::<Result<_>>()
            .map(Map)
    }
}

// SAFETY: a MAP is stored as a LIST of STRUCT(key, value), and `validate` checks both fields.
unsafe impl<K: NativeElement, V: NativeElement> NativeElement for Map<K, V> {
    const TYPE_ID: LogicalTypeID = LogicalTypeID::Map;

    type Ref<'a>
        = MapRow<'a, K, V>
    where
        K: 'a,
        V: 'a;

    type Native<'a> = super::ListIndex;

    fn validate(other: &LogicalType, children: Option<&VectorCollection>) -> Result<bool> {
        if other.id()? != Self::TYPE_ID {
            return Ok(false);
        }

        // MAP stores its entries as one STRUCT(key, value) child.
        let fields = children
            .filter(|children| children.col_count() == 1)
            .map(|children| children.get_untyped_vector_at(0))
            .transpose()?
            .and_then(|entries| entries.children())
            .filter(|fields| fields.col_count() == 2)
            .ok_or_else(|| Error::invalid_input("Map vector must have one STRUCT(key, value) child"))?;

        fields.get_untyped_vector_at(0)?.validate_as::<K>()?;
        fields.get_untyped_vector_at(1)?.validate_as::<V>().map(|()| true)
    }

    #[inline(always)]
    unsafe fn get<'a, U: NativeElement>(
        vector: &crate::vector::VectorReader<'a, U>,
        physical: usize,
        _logical: usize,
    ) -> Self::Ref<'a>
    where
        Self: Sized + 'a,
    {
        let data_ptr = vector.as_ptr() as *const ffi::duckdb_v2_list_entry;
        let list = unsafe { &*data_ptr.add(physical) };

        // SAFETY: Map<K, V>::validate checked the entries and both fields.
        let entries = unsafe { vector.child_reader::<crate::vector::Unknown>(0) };
        let (keys, values) = unsafe { (entries.child_reader::<K>(0), entries.child_reader::<V>(1)) };
        MapRow {
            keys,
            values,
            offset: list.offset as usize,
            length: list.length as usize,
        }
    }
}

impl<K: NativeElement, V: NativeElement> Vector<'_, Map<K, V>> {
    /// Size the entries child to exactly `entries`; writes continue at [`Self::child_offset`],
    /// capped at the new size.
    ///
    /// New entries are initialized like [`Self::set_size`]; see [`List`](super::List) for sizing to an
    /// upper bound and trimming.
    ///
    /// # Safety
    /// No written row may use an entry at or above `entries`.
    pub unsafe fn set_child_size(&mut self, entries: usize) -> Result<()> {
        // SAFETY: the caller keeps written rows below the new size.
        unsafe { self.size_appended_child(entries) }
    }

    /// Return the first entry the next map write uses.
    pub fn child_offset(&self) -> usize {
        self.child_offset_cell().get()
    }

    /// Continue map writes at `offset`, e.g. with a new wrapper for a partially written vector.
    ///
    /// Moving backwards makes later writes replace entries of rows that were already written.
    pub fn set_child_offset(&mut self, offset: usize) -> Result<()> {
        self.move_appended_child_offset(offset)
    }
}

impl<K: NativeElement, V: NativeElement> VectorWriter<'_, Map<K, V>> {
    /// Return the first entry the next map write uses.
    pub fn child_offset(&self) -> usize {
        self.vector().child_offset_cell().get()
    }
}

impl<K: NativeElement, V: NativeElement> crate::vector::VectorReader<'_, Map<K, V>> {
    /// Return a row's range in the entries child, or `None` for SQL NULL, without reading its entries.
    /// Out-of-range indices are errors.
    #[inline]
    pub fn get_list_entry(&self, index: usize) -> Result<Option<super::ListIndex>> {
        // SAFETY: MAP rows are stored as list entries.
        unsafe { self.list_entry(index) }
    }
}

// SAFETY: MAP rows use only entries below the child size set by `set_child_size`.
unsafe impl<K: NativeElement, V: NativeElement> HasChildren for Map<K, V> {
    fn child_len(writer: &VectorWriter<'_, Self>) -> Result<usize> {
        Ok(writer
            .vector()
            .children()
            .expect("validated MAP vector has entries")
            .get_untyped_vector_at(0)?
            .len())
    }
}

/// # Safety
/// `index` must be in bounds, and the entries and both fields must be validated as `K` and `V`.
#[inline(always)]
unsafe fn write_map<'w, K: NativeElement + 'w, V: NativeElement + 'w, E>(
    writer: &mut VectorWriter<'w, Map<K, V>>,
    index: usize,
    entries: Option<impl ExactSizeIterator<Item = E>>,
    mut write: impl FnMut(&mut VectorWriter<'w, Struct>, usize, E) -> Result<()>,
) -> Result<()> {
    let Some(entries) = entries else { return Ok(()) };
    let len = entries.len();
    let offset = writer.vector().child_offset_cell().get();
    // SAFETY: the caller guarantees that the entries were validated as STRUCT(key, value).
    let child = unsafe { writer.child_writer_unchecked::<Struct>(0)? };
    let size = child.len();
    let end = offset.checked_add(len).filter(|end| *end <= size).ok_or_else(|| {
        Error::invalid_input(format!(
            "MAP row needs {len} entries, but only {} of {size} remain; size the child with set_child_size",
            size - offset.min(size)
        ))
    })?;
    for (entry, value) in (offset..end).zip(entries) {
        write(child, entry, value)?;
        // SAFETY: the entry is in bounds, and both of its fields were just written.
        unsafe { child.set_validity_unchecked(entry, true) };
    }
    writer.vector().child_offset_cell().set(end);
    // SAFETY: the caller guarantees an in-bounds row, and the entry covers the entries just written.
    unsafe {
        writer.write_internal(
            index,
            Some(ffi::duckdb_v2_list_entry {
                offset: offset as u64,
                length: len as u64,
            }),
        )
    }
}

// SAFETY: entries only cover keys and values written through the fields' own writers.
unsafe impl<K, V> WritableNativeElement for Map<K, V>
where
    K: WritableNativeElement + std::borrow::Borrow<K::Write>,
    V: WritableNativeElement + std::borrow::Borrow<V::Write>,
{
    type Write = Self;

    #[inline(always)]
    unsafe fn write(vector: &mut VectorWriter<'_, Self>, index: usize, value: &Self::Write) -> Result<()> {
        // SAFETY: the caller guarantees an in-bounds row; Map<K, V>::validate checked the entries and both fields.
        unsafe {
            write_map(vector, index, Some(value.0.iter()), |entries, entry, (key, value)| {
                entries
                    .child_writer_unchecked::<K>(0)?
                    .set_unchecked(entry, key.borrow())?;
                entries
                    .child_writer_unchecked::<V>(1)?
                    .set_option_unchecked(entry, value.as_ref().map(std::borrow::Borrow::borrow))
            })
        }
    }

    #[inline]
    unsafe fn write_null(vector: &mut VectorWriter<'_, Self>, index: usize) -> Result<()> {
        // SAFETY: the caller guarantees an in-bounds row, and an empty entry references no entries.
        unsafe { vector.write_internal(index, Some(super::ListIndex::new(0, 0))) }
    }
}

/// A borrowed map row backed by matching ranges in key and value vectors.
///
/// Reading a key panics if it is NULL, which DuckDB does not allow in a `MAP`.
/// Nested rows borrow the vector, not their parent row or temporary readers.
///
/// ```compile_fail,E0502
/// use duckdb_neo::{Result, types::{List, Map}, vector::Vector};
///
/// fn resize(vector: &mut Vector<'_, Map<i32, List<i32>>>) -> Result<()> {
///     let row = vector.reader()?.get(0)?.unwrap();
///     let (_, values) = row.get_index(0).unwrap();
///     drop(row);
///     vector.set_size(10)?;
///     let _ = values.unwrap().get(0);
///     Ok(())
/// }
/// ```
#[derive(Debug)]
pub struct MapRow<'a, K: NativeElement, V: NativeElement> {
    keys: crate::vector::VectorReader<'a, K>,
    values: crate::vector::VectorReader<'a, V>,
    pub(crate) offset: usize,
    pub(crate) length: usize,
}

impl<'a, K: NativeElement + 'a, V: NativeElement + 'a> MapRow<'a, K, V> {
    /// Return the number of entries in the map.
    pub fn len(&self) -> usize {
        self.length
    }

    /// Return whether the map has no entries.
    pub fn is_empty(&self) -> bool {
        self.length == 0
    }

    /// Iterate over the map's `(key, value)` entries in storage order.
    pub fn iter(&self) -> impl Iterator<Item = (K::Ref<'a>, Option<V::Ref<'a>>)> + '_ {
        // SAFETY: The parent reader cached all descendant views before constructing this row.
        (self.offset..self.offset + self.length)
            .map(move |logical| unsafe { (Self::key(&self.keys, logical), self.values.get_unchecked(logical)) })
    }

    /// Return the entry at `index` in storage order, or `None` if it is out of bounds.
    #[inline]
    pub fn get_index(&self, index: usize) -> Option<(K::Ref<'a>, Option<V::Ref<'a>>)> {
        if index >= self.length {
            return None;
        }
        let logical = self.offset + index;
        // SAFETY: the entry is in bounds, and the parent reader cached all descendant views.
        unsafe { Some((Self::key(&self.keys, logical), self.values.get_unchecked(logical))) }
    }

    /// Return the map's keys in storage order, aligned with [`Self::values`].
    pub fn keys(&self) -> Vec<K::Ref<'a>> {
        self.iter().map(|(key, _)| key).collect()
    }

    /// Return the map's values in storage order, aligned with [`Self::keys`].
    pub fn values(&self) -> Vec<Option<V::Ref<'a>>> {
        self.iter().map(|(_, value)| value).collect()
    }

    unsafe fn key(keys: &crate::vector::VectorReader<'a, K>, logical: usize) -> K::Ref<'a> {
        // SAFETY: the caller guarantees the entry is in bounds.
        unsafe { keys.get_unchecked(logical) }.expect("MAP keys must not be NULL")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        data_chunk::DataChunk,
        environment::{Environment, StorageLocation},
        types::List,
    };

    #[test]
    fn map_conversions_preserve_order() {
        let value = Map::from(vec![(2, Some("b")), (1, None)]);
        let collected = [(2, Some("b")), (1, None)].into_iter().collect::<Map<_, _>>();
        assert_eq!(value, collected);
        assert_eq!(value.0, [(2, Some("b")), (1, None)]);
        assert_eq!(Map::<i32, &str>::default(), Map::from(vec![]));
    }

    #[test]
    fn map_typed_writes_continue_after_slow_writes() -> Result<()> {
        let conn = Environment::new()?.open(StorageLocation::InMemory)?.connect()?;
        let mut chunk = DataChunk::create(&conn, &[Map::<i32, i32>::logical_type(&conn)?], true)?;
        let mut maps = chunk.get_vector_at_mut::<Map<i32, i32>>(0)?;
        maps.set_size(2)?;
        maps.write_value_slow(0, &Map(vec![(1, Some(10)), (2, Some(20))]).value(&conn)?)?;
        assert_eq!(maps.child_offset(), 2);
        // SAFETY: row 1 writes the only new entry.
        unsafe { maps.set_child_size(3)? };
        maps.writer()?.set(1, &Map(vec![(3, Some(30))]))?;
        let reader = maps.reader()?;
        let row = reader.get(0)?.unwrap();
        assert_eq!(
            (row.get_index(0), row.get_index(1)),
            (Some((&1, Some(&10))), Some((&2, Some(&20))))
        );
        assert_eq!(row.get_index(2), None);
        assert_eq!(reader.get(1)?.unwrap().get_index(0), Some((&3, Some(&30))));
        Ok(())
    }

    #[test]
    fn map_write_null_value_empty_map_and_null_row() -> Result<()> {
        let env = Environment::new()?;
        let instance = env.open(StorageLocation::InMemory)?;
        let conn = instance.connect()?;

        let mut chunk = DataChunk::create(&conn, &[Map::<i32, i32>::logical_type(&conn)?], true)?;
        {
            let mut maps = chunk.get_vector_at_mut::<Map<i32, i32>>(0)?;
            maps.set_size(3)?;
            // SAFETY: row 0 writes both entries; the others write none.
            unsafe { maps.set_child_size(2)? };
            maps.writer()?.set(0, &Map(vec![(1, None), (2, Some(20))]))?;
            maps.writer()?.set(1, &Map::default())?;
            maps.writer()?.set_null(2)?;
        }

        let maps = chunk.get_vector_at::<Map<i32, i32>>(0)?;
        let maps_reader = maps.reader()?;
        let row = maps_reader.get(0)?.expect("row 0 is not NULL");
        let entries = [row.get_index(0).unwrap(), row.get_index(1).unwrap()];
        assert_eq!(entries, [(&1, None), (&2, Some(&20))]);
        assert_eq!(row.get_index(2), None);
        assert_eq!(row.values(), [None, Some(&20)]);

        assert!(maps_reader.get(1)?.expect("row 1 is not NULL").keys().is_empty());
        assert!(maps_reader.get(2)?.is_none());

        let entry = maps_reader.get_list_entry(0)?.expect("row 0 is not NULL");
        assert_eq!((entry.offset(), entry.length()), (0, 2));
        let entry = maps_reader.get_list_entry(1)?.expect("row 1 is not NULL");
        assert_eq!((entry.offset(), entry.length()), (2, 0));
        assert_eq!(maps_reader.get_list_entry(2)?, None);
        assert!(maps_reader.get_list_entry(3).is_err());
        Ok(())
    }

    #[test]
    fn map_row_len_is_empty_and_iter() -> Result<()> {
        let env = Environment::new()?;
        let instance = env.open(StorageLocation::InMemory)?;
        let conn = instance.connect()?;

        let mut chunk = DataChunk::create(&conn, &[Map::<i32, i32>::logical_type(&conn)?], true)?;
        {
            let mut maps = chunk.get_vector_at_mut::<Map<i32, i32>>(0)?;
            maps.set_size(2)?;
            // SAFETY: row 0 writes all three entries.
            unsafe { maps.set_child_size(3)? };
            maps.writer()?
                .set(0, &Map(vec![(1, None), (2, Some(20)), (3, Some(30))]))?;
            maps.writer()?.set(1, &Map::default())?;
        }

        let maps = chunk.get_vector_at::<Map<i32, i32>>(0)?;
        let maps_reader = maps.reader()?;
        let row = maps_reader.get(0)?.expect("row 0 is not NULL");
        assert_eq!(row.len(), 3);
        assert!(!row.is_empty());
        let entries: Vec<_> = row.iter().collect();
        assert_eq!(row.keys().into_iter().zip(row.values()).collect::<Vec<_>>(), entries);
        for (index, entry) in entries.iter().enumerate() {
            assert_eq!(row.get_index(index), Some(*entry));
        }
        assert_eq!(row.get_index(row.len()), None);
        assert_eq!(row.get_index(usize::MAX), None);
        assert_eq!(entries, [(&1, None), (&2, Some(&20)), (&3, Some(&30))]);

        let empty = maps_reader.get(1)?.expect("row 1 is not NULL");
        assert_eq!(empty.len(), 0);
        assert!(empty.is_empty());
        assert_eq!(empty.iter().count(), 0);
        assert_eq!(empty.get_index(0), None);
        Ok(())
    }

    #[test]
    fn map_row_with_list_keys() -> Result<()> {
        let env = Environment::new()?;
        let instance = env.open(StorageLocation::InMemory)?;
        let conn = instance.connect()?;

        let mut result = conn.query("SELECT MAP {[1, 2]: 10, [3]: NULL}", Parameters::None)?;
        let chunk = result.next_chunk()?.expect("query returns one chunk");
        let maps = chunk.get_vector_at::<Map<List<i32>, i32>>(0)?;
        let row = maps.reader()?.get(0)?.expect("row 0 is not NULL");

        let entries: Vec<_> = row
            .iter()
            .map(|(key, value)| (key.iter().collect::<Vec<_>>(), value))
            .collect();
        assert_eq!(entries, [(vec![Some(&1), Some(&2)], Some(&10)), (vec![Some(&3)], None)]);

        for (index, entry) in entries.iter().enumerate() {
            let (key, value) = row.get_index(index).unwrap();
            assert_eq!((key.iter().collect::<Vec<_>>(), value), *entry);
        }
        assert!(row.get_index(row.len()).is_none());

        let keys: Vec<_> = row
            .keys()
            .into_iter()
            .map(|key| key.iter().collect::<Vec<_>>())
            .collect();
        assert_eq!(keys, [vec![Some(&1), Some(&2)], vec![Some(&3)]]);
        assert_eq!(row.values(), [Some(&10), None]);
        Ok(())
    }

    #[test]
    fn nested_map_rows_outlive_readers_and_parent_rows() -> Result<()> {
        let conn = Environment::new()?.open(StorageLocation::InMemory)?.connect()?;
        let chunk = conn
            .query(
                "SELECT MAP {1: [MAP {'a key longer than the inline storage': 42, 'null': NULL},
                                 NULL, MAP {}]}::MAP(INTEGER, MAP(VARCHAR, INTEGER)[])",
                Parameters::None,
            )?
            .next_chunk()?
            .unwrap();
        let maps = chunk.get_vector_at::<Map<i32, List<Map<String, i32>>>>(0)?;
        let (keys, values) = {
            let inner = {
                let entries = {
                    let reader = maps.reader()?;
                    let mut rows = reader.iter();
                    rows.next().unwrap().unwrap().get_index(0).unwrap().1.unwrap()
                };
                assert!(entries.get(1).unwrap().is_none());
                assert!(entries.get(2).unwrap().unwrap().is_empty());
                entries.get(0).unwrap().unwrap()
            };
            assert_eq!(
                inner.iter().collect::<Vec<_>>(),
                [("a key longer than the inline storage", Some(&42)), ("null", None),]
            );
            (inner.keys(), inner.values())
        };
        assert_eq!(keys, ["a key longer than the inline storage", "null"]);
        assert_eq!(values, [Some(&42), None]);
        Ok(())
    }

    #[test]
    fn map_writes_initialize_all_entries() -> Result<()> {
        let conn = Environment::new()?.open(StorageLocation::InMemory)?.connect()?;
        let mut chunk = DataChunk::create(&conn, &[Map::<i32, i32>::logical_type(&conn)?], true)?;
        let mut maps = chunk.get_vector_at_mut::<Map<i32, i32>>(0)?;
        maps.set_size(2)?;
        // SAFETY: all entries are initialized below, before any reads.
        unsafe { maps.set_child_size(4)? };
        {
            let mut children = maps.children_mut().unwrap();
            let mut entries = children.get_vector_at_mut::<Struct>(0)?;
            for index in 0..4 {
                entries.set_null_slow(index)?;
            }
        }
        {
            let mut writer = maps.writer()?;
            writer.set(0, &Map(vec![(7, Some(70)), (2, None)]))?;
            writer.set(1, &Map(vec![(9, Some(90)), (4, Some(40))]))?;
            assert_eq!(writer.child_offset(), 4);
        }

        let reader = maps.reader()?;
        assert_eq!(
            reader.get(0)?.unwrap().iter().collect::<Vec<_>>(),
            [(&7, Some(&70)), (&2, None)]
        );
        assert_eq!(
            reader.get(1)?.unwrap().iter().collect::<Vec<_>>(),
            [(&9, Some(&90)), (&4, Some(&40))]
        );
        let entries = maps.children().unwrap().get_vector_at::<Struct>(0)?;
        assert!(entries.reader()?.iter().all(|entry| entry.is_some()));
        Ok(())
    }

    #[test]
    fn map_rows_take_entries_in_write_order() -> Result<()> {
        let env = Environment::new()?;
        let conn = env.open(StorageLocation::InMemory)?.connect()?;
        let mut chunk = DataChunk::create(&conn, &[Map::<i32, String>::logical_type(&conn)?], true)?;
        let mut maps = chunk.get_vector_at_mut::<Map<i32, String>>(0)?;
        maps.set_size(2)?;
        let error = maps.writer()?.set(0, &Map(vec![(1, None)])).unwrap_err();
        assert!(error.message.contains("only 0 of 0 remain"), "{error}");
        // SAFETY: the rows below write all three entries.
        unsafe { maps.set_child_size(3)? };
        {
            let mut writer = maps.writer()?;
            writer.set(1, &Map(vec![(1, Some("one".to_owned())), (2, None)]))?;
            let error = writer
                .set(0, &Map(vec![(3, Some("three".to_owned())), (4, None)]))
                .unwrap_err();
            assert!(
                error
                    .message
                    .contains("MAP row needs 2 entries, but only 1 of 3 remain"),
                "{error}"
            );
            assert_eq!(writer.child_offset(), 2);
            writer.set(0, &Map(vec![(3, Some("three".to_owned()))]))?;
        }
        let reader = maps.reader()?;
        let row = reader.get(0)?.unwrap();
        assert_eq!(row.offset, 2);
        assert_eq!(row.get_index(0), Some((&3, Some("three"))));
        assert_eq!(row.get_index(1), None);
        let row = reader.get(1)?.unwrap();
        assert_eq!(row.offset, 0);
        let entries = [row.get_index(0).unwrap(), row.get_index(1).unwrap()];
        assert_eq!(entries, [(&1, Some("one")), (&2, None)]);
        Ok(())
    }

    #[test]
    fn map_writes_borrow_owned_entries() -> Result<()> {
        let conn = Environment::new()?.open(StorageLocation::InMemory)?.connect()?;
        let mut chunk = DataChunk::create(&conn, &[Map::<String, String>::logical_type(&conn)?], true)?;
        let mut maps = chunk.get_vector_at_mut::<Map<String, String>>(0)?;
        maps.set_size(3)?;
        let rows = [
            Some(Map(vec![
                (
                    "a key longer than the inline storage".to_owned(),
                    Some("a value longer than the inline storage".to_owned()),
                ),
                ("b".to_owned(), None),
            ])),
            None,
            Some(Map(vec![("c".to_owned(), Some("C".to_owned()))])),
        ];
        // SAFETY: the rows below write all three entries.
        unsafe { maps.set_child_size(3)? };
        {
            let mut writer = maps.writer()?;
            for (index, row) in rows.iter().enumerate() {
                writer.set_option(index, row.as_ref())?;
            }
            assert_eq!(writer.child_offset(), 3);
        }
        for (index, row) in rows.iter().enumerate() {
            assert_eq!(
                maps.get_value_slow(index)?.get::<Map<String, String>>()?.as_ref(),
                row.as_ref()
            );
        }
        Ok(())
    }

    #[test]
    fn nested_map_writes_borrow_owned_entries() -> Result<()> {
        let conn = Environment::new()?.open(StorageLocation::InMemory)?.connect()?;
        let mut chunk = DataChunk::create(&conn, &[List::<Map<i32, String>>::logical_type(&conn)?], true)?;
        let mut lists = chunk.get_vector_at_mut::<List<Map<i32, String>>>(0)?;
        lists.set_size(1)?;
        let row = List(vec![
            Some(Map(vec![(1, Some("one".to_owned())), (2, None)])),
            None,
            Some(Map::default()),
        ]);
        {
            let mut children = lists.children_mut().unwrap();
            let mut maps = children.get_vector_at_mut::<Map<i32, String>>(0)?;
            // SAFETY: the row below writes both entries.
            unsafe { maps.set_child_size(2)? };
        }
        // SAFETY: the row below writes all three maps.
        unsafe { lists.set_child_size(3)? };
        lists.writer()?.set(0, &row)?;
        assert_eq!(lists.get_value_slow(0)?.get::<List<Map<i32, String>>>()?, Some(row));
        Ok(())
    }

    #[test]
    #[should_panic(expected = "MAP keys must not be NULL")]
    fn map_row_panics_on_null_key() {
        let conn = Environment::new()
            .unwrap()
            .open(StorageLocation::InMemory)
            .unwrap()
            .connect()
            .unwrap();
        let mut chunk = DataChunk::create(&conn, &[Map::<i32, i32>::logical_type(&conn).unwrap()], true).unwrap();
        let mut maps = chunk.get_vector_at_mut::<Map<i32, i32>>(0).unwrap();
        maps.set_size(1).unwrap();
        // SAFETY: the row below writes the only entry.
        unsafe { maps.set_child_size(1).unwrap() };
        maps.writer().unwrap().set(0, &Map(vec![(1, Some(10))])).unwrap();
        {
            let mut children = maps.children_mut().unwrap();
            let mut entries = children.get_vector_at_mut::<Struct>(0).unwrap();
            let mut fields = entries.children_mut().unwrap();
            fields.get_vector_at_mut::<i32>(0).unwrap().set_null_slow(0).unwrap();
        }
        let reader = maps.reader().unwrap();
        let _ = reader.get(0).unwrap().unwrap().get_index(0);
    }
}

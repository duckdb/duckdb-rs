use super::{RowReader, Vector, VectorReader, WithReader};
use crate::{
    Result,
    types::{Array, List, ListIndex, Map, Union},
};

/// Reads `LIST` rows, with the element reader prepared once.
///
/// ```
/// # use duckdb_neo::{Parameters, Result, environment::{Environment, StorageLocation}, types::List, vector::RowReader};
/// # fn main() -> Result<()> {
/// # let conn = Environment::new()?.open(StorageLocation::InMemory)?.connect()?;
/// let chunk = conn.query("SELECT [[1, NULL], [3]]::INTEGER[][]", Parameters::None)?.next_chunk()?.unwrap();
/// let vector = chunk.get_vector_at::<List<List<i32>>>(0)?;
/// let lists = vector.list_reader()?;
/// let row = lists.get(0)?.unwrap();
/// let inner = row.get(0).unwrap().unwrap();
/// assert_eq!(inner.iter().collect::<Vec<_>>(), [Some(&1), None]);
/// assert_eq!(row.get(1).unwrap().unwrap().iter().collect::<Vec<_>>(), [Some(&3)]);
/// # Ok(())
/// # }
/// ```
pub struct ListReader<'a, T: WithReader> {
    rows: VectorReader<'a, List<T>>,
    elements: T::Reader<'a>,
}

impl<'a, T: WithReader + 'a> ListReader<'a, T> {
    pub(super) fn new(vector: &'a Vector<'a, List<T>>) -> Result<Self> {
        let rows = vector.reader()?;
        let child = vector
            .children()
            .expect("validated LIST vector has a child")
            .get_untyped_vector_at(0)?;
        // SAFETY: `List<T>::validate` checked the child as `T`.
        let elements = T::row_reader(unsafe { child.cast_ref_unchecked::<T>() })?;
        Ok(Self { rows, elements })
    }

    /// Return a row's range in the element child, or `None` for SQL NULL.
    pub fn entry(&self, row: usize) -> Result<Option<ListIndex>> {
        self.rows.get(row)
    }

    /// Borrow the reader of the element child, for columnar reads.
    pub fn elements(&self) -> &T::Reader<'a> {
        &self.elements
    }
}

impl<'a, T: WithReader + 'a> RowReader for ListReader<'a, T> {
    type Item<'r>
        = Elements<'r, T::Reader<'a>>
    where
        Self: 'r;

    fn len(&self) -> usize {
        self.rows.len()
    }

    fn is_null(&self, row: usize) -> Result<bool> {
        self.rows.is_null(row)
    }

    #[inline(always)]
    unsafe fn get_unchecked(&self, row: usize) -> Option<Self::Item<'_>> {
        // SAFETY: the caller guarantees bounds; LIST rows store list entries.
        let entry = unsafe { self.rows.list_entry_unchecked(row) }?;
        Some(Elements {
            reader: &self.elements,
            offset: entry.offset as usize,
            len: entry.length as usize,
        })
    }
}

impl<T: WithReader + std::fmt::Debug> std::fmt::Debug for ListReader<'_, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ListReader").field("len", &self.rows.len()).finish()
    }
}

/// A range of child rows: the elements of one `LIST` or `ARRAY` row.
#[derive(Debug)]
pub struct Elements<'r, R> {
    reader: &'r R,
    offset: usize,
    len: usize,
}

impl<R> Clone for Elements<'_, R> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<R> Copy for Elements<'_, R> {}

impl<'r, R: RowReader> Elements<'r, R> {
    /// Return the number of elements.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Return whether there are no elements.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Return the elements' range in the child.
    pub fn entry(&self) -> ListIndex {
        ListIndex::new(self.offset as u64, self.len as u64)
    }

    /// Return an element; the outer `None` means `index` is out of bounds, the inner one SQL NULL.
    #[inline]
    pub fn get(&self, index: usize) -> Option<Option<R::Item<'r>>> {
        // SAFETY: DuckDB keeps every entry within its child.
        (index < self.len).then(|| unsafe { self.reader.get_unchecked(self.offset + index) })
    }

    /// Iterate over the elements.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = Option<R::Item<'r>>> + 'r {
        let reader = self.reader;
        // SAFETY: DuckDB keeps every entry within its child.
        (self.offset..self.offset + self.len).map(move |index| unsafe { reader.get_unchecked(index) })
    }
}

/// Reads `MAP` rows, with the key and value readers prepared once.
///
/// Reading a key panics if it is NULL, which DuckDB does not allow in a `MAP`.
///
/// ```
/// # use duckdb_neo::{Parameters, Result, environment::{Environment, StorageLocation}, types::Map, vector::RowReader};
/// # fn main() -> Result<()> {
/// # let conn = Environment::new()?.open(StorageLocation::InMemory)?.connect()?;
/// let chunk = conn.query("SELECT MAP {'a': 1, 'b': NULL}", Parameters::None)?.next_chunk()?.unwrap();
/// let vector = chunk.get_vector_at::<Map<String, i32>>(0)?;
/// let maps = vector.map_reader()?;
/// let row = maps.get(0)?.unwrap();
/// assert_eq!(row.iter().collect::<Vec<_>>(), [("a", Some(&1)), ("b", None)]);
/// # Ok(())
/// # }
/// ```
pub struct MapReader<'a, K: WithReader, V: WithReader> {
    rows: VectorReader<'a, Map<K, V>>,
    keys: K::Reader<'a>,
    values: V::Reader<'a>,
}

impl<'a, K: WithReader + 'a, V: WithReader + 'a> MapReader<'a, K, V> {
    pub(super) fn new(vector: &'a Vector<'a, Map<K, V>>) -> Result<Self> {
        let rows = vector.reader()?;
        let entries = vector
            .children()
            .expect("validated MAP vector has entries")
            .get_untyped_vector_at(0)?;
        let fields = entries.children().expect("validated MAP entries have two fields");
        let (key, value) = (fields.get_untyped_vector_at(0)?, fields.get_untyped_vector_at(1)?);
        // SAFETY: `Map<K, V>::validate` checked the fields as `K` and `V`.
        let (keys, values) = unsafe {
            (
                K::row_reader(key.cast_ref_unchecked::<K>())?,
                V::row_reader(value.cast_ref_unchecked::<V>())?,
            )
        };
        Ok(Self { rows, keys, values })
    }

    /// Return a row's range in the entries child, or `None` for SQL NULL.
    pub fn entry(&self, row: usize) -> Result<Option<ListIndex>> {
        self.rows.get(row)
    }

    /// Borrow the reader of every entry's key, for columnar reads.
    pub fn keys(&self) -> &K::Reader<'a> {
        &self.keys
    }

    /// Borrow the reader of every entry's value, for columnar reads.
    pub fn values(&self) -> &V::Reader<'a> {
        &self.values
    }
}

impl<'a, K: WithReader + 'a, V: WithReader + 'a> RowReader for MapReader<'a, K, V> {
    type Item<'r>
        = Entries<'r, K::Reader<'a>, V::Reader<'a>>
    where
        Self: 'r;

    fn len(&self) -> usize {
        self.rows.len()
    }

    fn is_null(&self, row: usize) -> Result<bool> {
        self.rows.is_null(row)
    }

    #[inline(always)]
    unsafe fn get_unchecked(&self, row: usize) -> Option<Self::Item<'_>> {
        // SAFETY: the caller guarantees bounds; MAP rows store list entries.
        let entry = unsafe { self.rows.list_entry_unchecked(row) }?;
        Some(Entries {
            keys: &self.keys,
            values: &self.values,
            offset: entry.offset as usize,
            len: entry.length as usize,
        })
    }
}

impl<K: WithReader, V: WithReader> std::fmt::Debug for MapReader<'_, K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MapReader").field("len", &self.rows.len()).finish()
    }
}

/// The `(key, value)` entries of one `MAP` row, in storage order.
#[derive(Debug)]
pub struct Entries<'r, K, V> {
    keys: &'r K,
    values: &'r V,
    offset: usize,
    len: usize,
}

impl<K, V> Clone for Entries<'_, K, V> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<K, V> Copy for Entries<'_, K, V> {}

impl<'r, K: RowReader, V: RowReader> Entries<'r, K, V> {
    /// Return the number of entries.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Return whether there are no entries.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Return the entries' range in the child.
    pub fn entry(&self) -> ListIndex {
        ListIndex::new(self.offset as u64, self.len as u64)
    }

    /// Return the entry at `index`, or `None` if it is out of bounds.
    #[inline]
    pub fn get(&self, index: usize) -> Option<(K::Item<'r>, Option<V::Item<'r>>)> {
        // SAFETY: DuckDB keeps every entry within its child.
        (index < self.len).then(|| unsafe { Self::read(self.keys, self.values, self.offset + index) })
    }

    /// Iterate over the entries.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = (K::Item<'r>, Option<V::Item<'r>>)> + 'r {
        let (keys, values) = (self.keys, self.values);
        // SAFETY: DuckDB keeps every entry within its child.
        (self.offset..self.offset + self.len).map(move |index| unsafe { Self::read(keys, values, index) })
    }

    /// Iterate over the keys.
    pub fn keys(&self) -> impl ExactSizeIterator<Item = K::Item<'r>> + 'r {
        self.iter().map(|(key, _)| key)
    }

    /// Iterate over the values; `None` values are SQL NULL.
    pub fn values(&self) -> impl ExactSizeIterator<Item = Option<V::Item<'r>>> + 'r {
        self.iter().map(|(_, value)| value)
    }

    /// Return the value of the first entry whose key equals `key`.
    pub fn get_key<Q: Copy>(&self, key: Q) -> Option<Option<V::Item<'r>>>
    where
        K::Item<'r>: PartialEq<Q>,
    {
        self.iter().find(|(entry, _)| *entry == key).map(|(_, value)| value)
    }

    #[inline(always)]
    unsafe fn read(keys: &'r K, values: &'r V, index: usize) -> (K::Item<'r>, Option<V::Item<'r>>) {
        // SAFETY: the caller guarantees the entry is within the child.
        unsafe {
            (
                keys.get_unchecked(index).expect("MAP keys must not be NULL"),
                values.get_unchecked(index),
            )
        }
    }
}

/// Reads `ARRAY` rows, with the element reader prepared once.
pub struct ArrayReader<'a, T: WithReader, const N: usize> {
    rows: VectorReader<'a, Array<T, N>>,
    elements: T::Reader<'a>,
}

impl<'a, T: WithReader + 'a, const N: usize> ArrayReader<'a, T, N> {
    pub(super) fn new(vector: &'a Vector<'a, Array<T, N>>) -> Result<Self> {
        let rows = vector.reader()?;
        let child = vector
            .children()
            .expect("validated ARRAY vector has a child")
            .get_untyped_vector_at(0)?;
        // SAFETY: `Array<T, N>::validate` checked the child as `T`.
        let elements = T::row_reader(unsafe { child.cast_ref_unchecked::<T>() })?;
        Ok(Self { rows, elements })
    }

    /// Borrow the reader of the element child, for columnar reads.
    pub fn elements(&self) -> &T::Reader<'a> {
        &self.elements
    }
}

impl<'a, T: WithReader + 'a, const N: usize> RowReader for ArrayReader<'a, T, N> {
    type Item<'r>
        = Elements<'r, T::Reader<'a>>
    where
        Self: 'r;

    fn len(&self) -> usize {
        self.rows.len()
    }

    fn is_null(&self, row: usize) -> Result<bool> {
        self.rows.is_null(row)
    }

    #[inline(always)]
    unsafe fn get_unchecked(&self, row: usize) -> Option<Self::Item<'_>> {
        // SAFETY: the caller guarantees bounds; every physical row owns `N` child elements.
        let physical = unsafe { self.rows.physical_index_unchecked(row) }?;
        Some(Elements {
            reader: &self.elements,
            offset: physical * N,
            len: N,
        })
    }
}

impl<T: WithReader, const N: usize> std::fmt::Debug for ArrayReader<'_, T, N> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArrayReader").field("len", &self.rows.len()).finish()
    }
}

impl<T: WithReader> WithReader for List<T> {
    type Reader<'a>
        = ListReader<'a, T>
    where
        Self: 'a;

    fn row_reader<'a>(vector: &'a Vector<'a, Self>) -> Result<Self::Reader<'a>> {
        ListReader::new(vector)
    }
}

impl<K: WithReader, V: WithReader> WithReader for Map<K, V> {
    type Reader<'a>
        = MapReader<'a, K, V>
    where
        Self: 'a;

    fn row_reader<'a>(vector: &'a Vector<'a, Self>) -> Result<Self::Reader<'a>> {
        MapReader::new(vector)
    }
}

impl<T: WithReader, const N: usize> WithReader for Array<T, N> {
    type Reader<'a>
        = ArrayReader<'a, T, N>
    where
        Self: 'a;

    fn row_reader<'a>(vector: &'a Vector<'a, Self>) -> Result<Self::Reader<'a>> {
        ArrayReader::new(vector)
    }
}

impl WithReader for Union {
    type Reader<'a> = VectorReader<'a, Self>;

    fn row_reader<'a>(vector: &'a Vector<'a, Self>) -> Result<Self::Reader<'a>> {
        vector.reader()
    }
}

impl<T: WithReader> Vector<'_, List<T>> {
    /// Prepare a [`ListReader`].
    pub fn list_reader(&self) -> Result<ListReader<'_, T>> {
        ListReader::new(self)
    }
}

impl<K: WithReader, V: WithReader> Vector<'_, Map<K, V>> {
    /// Prepare a [`MapReader`].
    pub fn map_reader(&self) -> Result<MapReader<'_, K, V>> {
        MapReader::new(self)
    }
}

impl<T: WithReader, const N: usize> Vector<'_, Array<T, N>> {
    /// Prepare an [`ArrayReader`].
    pub fn array_reader(&self) -> Result<ArrayReader<'_, T, N>> {
        ArrayReader::new(self)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        Parameters, Result,
        environment::{Environment, StorageLocation},
        types::{Array, List, Map},
        vector::RowReader,
    };

    #[test]
    fn nested_readers_follow_selections_and_nulls() -> Result<()> {
        let conn = Environment::new()?.open(StorageLocation::InMemory)?.connect()?;
        let chunk = conn
            .query(
                "SELECT l, m, a FROM (VALUES
                    ([MAP {'k': [1, 2]}], ['x', 'a string longer than inline storage']::VARCHAR[2], 1),
                    (NULL, NULL, 2),
                    ([MAP {}, NULL], [NULL, 'y'], 3)) t(l, a, i), LATERAL (SELECT l AS m)
                 WHERE i <> 2 OR TRUE ORDER BY i DESC",
                Parameters::None,
            )?
            .next_chunk()?
            .unwrap();
        let lists = chunk.get_vector_at::<List<Map<String, List<i32>>>>(0)?;
        let reader = lists.list_reader()?;
        assert_eq!(reader.len(), 3);
        let first = reader.get(0)?.unwrap();
        assert_eq!(first.len(), 2);
        assert!(first.get(0).unwrap().unwrap().is_empty());
        assert!(first.get(1).unwrap().is_none());
        assert!(first.get(2).is_none());
        assert!(reader.get(1)?.is_none());
        let map = reader.get(2)?.unwrap().get(0).unwrap().unwrap();
        let (key, value) = map.get(0).unwrap();
        assert_eq!(key, "k");
        assert_eq!(value.unwrap().iter().collect::<Vec<_>>(), [Some(&1), Some(&2)]);
        assert_eq!(map.get_key("k").unwrap().unwrap().len(), 2);
        assert!(map.get_key("missing").is_none());
        assert!(reader.get(3).is_err());

        let arrays = chunk.get_vector_at::<Array<String, 2>>(2)?;
        let reader = arrays.array_reader()?;
        assert_eq!(reader.get(0)?.unwrap().iter().collect::<Vec<_>>(), [None, Some("y")]);
        assert!(reader.get(1)?.is_none());
        assert_eq!(
            reader.get(2)?.unwrap().iter().collect::<Vec<_>>(),
            [Some("x"), Some("a string longer than inline storage")]
        );
        Ok(())
    }

    #[test]
    fn nested_readers_read_every_storage_layout() -> Result<()> {
        let conn = Environment::new()?.open(StorageLocation::InMemory)?.connect()?;
        let mut result = conn.query(
            "SELECT * FROM test_vector_types(NULL::INTEGER[], NULL::MAP(VARCHAR, INTEGER), all_flat := false)",
            Parameters::None,
        )?;
        let mut rows = 0;
        while let Some(chunk) = result.next_chunk()? {
            let lists = chunk.get_vector_at::<List<i32>>(0)?;
            let reader = lists.list_reader()?;
            for row in 0..reader.len() {
                let read = reader
                    .get(row)?
                    .map(|elements| elements.iter().map(|value| value.copied()).collect::<Vec<_>>());
                assert_eq!(read, lists.get_value_slow(row)?.get::<List<i32>>()?.map(|list| list.0));
                rows += 1;
            }
            let maps = chunk.get_vector_at::<Map<String, i32>>(1)?;
            let reader = maps.map_reader()?;
            for row in 0..reader.len() {
                let read = reader.get(row)?.map(|entries| {
                    entries
                        .iter()
                        .map(|(key, value)| (key.to_owned(), value.copied()))
                        .collect::<Vec<_>>()
                });
                assert_eq!(
                    read,
                    maps.get_value_slow(row)?.get::<Map<String, i32>>()?.map(|map| map.0)
                );
            }
        }
        assert!(rows > 0);
        Ok(())
    }
}

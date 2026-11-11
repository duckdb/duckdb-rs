//! The `MAP` logical type.

use super::{DuckDBType, FromValue, ListIndex, ToValue, list::check_entry};
use crate::{
    Parameters, Result,
    connection::FFILink,
    data_chunk::VectorCollection,
    error::Error,
    logical_type::{LogicalType, LogicalTypeID},
    value::{Value, ValueInput},
    vector::{NativeElement, ReadableNativeElement, Vector, VectorReader, VectorWriter, WritableNativeElement},
};

/// A `MAP` row of `(key, value)` entries in storage order; a `None` value is SQL NULL.
///
/// A `MAP` vector is stored like a `LIST` of `STRUCT(key, value)` entries: its own rows read and
/// write natively as [`ListIndex`] entries, checked against the entries child. Read and write whole
/// rows with a [`MapReader`](crate::vector::MapReader) and [`MapWriter`](crate::vector::MapWriter).
/// DuckDB requires unique keys, which writes do not check.
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

    type Native<'a> = ListIndex;

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
}

// SAFETY: MAP rows store list entries.
unsafe impl<K: NativeElement, V: NativeElement> ReadableNativeElement for Map<K, V> {
    type Ref<'a>
        = ListIndex
    where
        Self: 'a;

    #[inline(always)]
    unsafe fn get<'a, U: NativeElement>(reader: &VectorReader<'a, U>, physical: usize, _logical: usize) -> ListIndex
    where
        Self: 'a,
    {
        // SAFETY: the caller guarantees a valid row of MAP storage.
        unsafe { *reader.as_ptr().cast::<ListIndex>().add(physical) }
    }
}

// SAFETY: entries are checked against the entries child's size, whose rows are always initialized.
// A valid entry whose key is NULL is not unsafe: readers panic on it.
unsafe impl<K: NativeElement, V: NativeElement> WritableNativeElement for Map<K, V> {
    type Write = ListIndex;

    #[inline]
    unsafe fn write(writer: &mut VectorWriter<'_, Self>, index: usize, value: &ListIndex) -> Result<()> {
        check_entry(value, writer.vector().list_child_len()?, "MAP entries")?;
        // SAFETY: the caller guarantees an in-bounds row, and the entry covers initialized entries.
        unsafe { writer.write_internal(index, Some(*value)) }
    }

    #[inline]
    unsafe fn write_null(writer: &mut VectorWriter<'_, Self>, index: usize) -> Result<()> {
        // SAFETY: the caller guarantees an in-bounds row, and an empty entry references no entries.
        unsafe { writer.write_internal(index, Some(ListIndex::default())) }
    }
}

impl<K: NativeElement, V: NativeElement> Vector<'_, Map<K, V>> {
    /// Resize the entries child to `entries`; new entries are initialized like [`Vector::set_size`].
    ///
    /// Shrinking fails if a valid row's entry would reach past the new size.
    pub fn set_child_size(&mut self, entries: usize) -> Result<()> {
        self.resize_list_child(entries)
    }

    /// As [`Self::set_child_size`], leaving new entries uninitialized.
    ///
    /// # Safety
    /// Every new entry's key and value, and their descendants, must be written or marked NULL before
    /// anything reads this vector; the entry rows themselves must be marked valid.
    pub unsafe fn set_child_size_uninit(&mut self, entries: usize) -> Result<()> {
        // SAFETY: the caller initializes the new entries.
        unsafe { self.resize_list_child_uninit(entries) }
    }
}

impl<K: NativeElement, V: NativeElement> VectorWriter<'_, Map<K, V>> {
    /// Write `entries` into rows `offset..` and mark them valid, checked once against the entries child.
    pub fn write_entries(&mut self, offset: usize, entries: &[ListIndex]) -> Result<()> {
        Vector::write_list_entries(self, offset, entries, "MAP entries")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        data_chunk::DataChunk,
        environment::{Environment, StorageLocation},
    };

    #[test]
    fn map_rows_read_and_write_their_entries() -> Result<()> {
        let conn = Environment::new()?.open(StorageLocation::InMemory)?.connect()?;
        let chunk = conn
            .query(
                "SELECT * FROM (VALUES (MAP {'a': 1, 'b': NULL}), (NULL))",
                Parameters::None,
            )?
            .next_chunk()?
            .unwrap();
        let reader = chunk.get_vector_at::<Map<String, i32>>(0)?.reader()?;
        assert_eq!(reader.get(0)?, Some(ListIndex::new(0, 2)));
        assert_eq!(reader.get(1)?, None);

        let mut chunk = DataChunk::create(&conn, &[Map::<i32, i32>::logical_type(&conn)?], true)?;
        let mut vector = chunk.get_vector_at_mut::<Map<i32, i32>>(0)?;
        vector.set_size(2)?;
        vector.set_child_size(1)?;
        {
            let mut writer = vector.writer()?;
            writer.set(0, &ListIndex::new(0, 1))?;
            writer.set_null(1)?;
            let error = writer.set(1, &ListIndex::new(1, 1)).unwrap_err();
            assert!(error.message.contains("past the 1 MAP entries"), "{error}");
        }
        assert_eq!(
            vector.get_value_slow(0)?.get::<Map<i32, i32>>()?,
            Some(Map(vec![(0, Some(0))]))
        );
        assert_eq!(vector.get_value_slow(1)?.get::<Map<i32, i32>>()?, None);
        assert!(vector.set_child_size(0).is_err());
        Ok(())
    }

    #[test]
    fn map_value_round_trip() -> Result<()> {
        let conn = Environment::new()?.open(StorageLocation::InMemory)?.connect()?;
        let map = Map(vec![("a".to_owned(), Some(1)), ("b".to_owned(), None)]);
        assert_eq!(map.value(&conn)?.get::<Map<String, i32>>()?, Some(map));
        Ok(())
    }
}

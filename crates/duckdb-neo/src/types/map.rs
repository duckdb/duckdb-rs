//! The `MAP` logical type.

use std::collections::HashMap;
use std::fmt::Debug;
use std::marker::PhantomData;

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
    vector::{Vector, VectorElement, WritableVectorElement},
};

/// Reads and writes `MAP` vectors with key type `K` and value type `V`.
/// Rows are written from a `HashMap`, in its iteration order.
#[derive(Debug)]
pub struct Map<K, V>(PhantomData<(K, V)>);

/// Key-value entries represented as a DuckDB `MAP`.
#[derive(Debug, PartialEq, Eq, Default)]
pub struct MapValue<K, V> {
    /// Entries in insertion order.
    pub entries: Vec<(K, V)>,
}

impl<K, V> From<Vec<(K, V)>> for MapValue<K, V> {
    fn from(entries: Vec<(K, V)>) -> Self {
        Self { entries }
    }
}

impl<K, V> FromIterator<(K, V)> for MapValue<K, V> {
    fn from_iter<T: IntoIterator<Item = (K, V)>>(iter: T) -> Self {
        Self {
            entries: iter.into_iter().collect(),
        }
    }
}

impl<K: DuckDBType, V: DuckDBType> DuckDBType for MapValue<K, V> {
    fn logical_type<C: FFILink + ?Sized>(link: &C) -> Result<LogicalType> {
        let key_type = Value::from_logical_type(link, &K::logical_type(link)?)?;
        let value_type = Value::from_logical_type(link, &V::logical_type(link)?)?;
        link.logical_type_create("MAP", Parameters::positional(&[&key_type, &value_type]))
    }
}

impl<K: ToValue + DuckDBType, V: ToValue + DuckDBType> ToValue for MapValue<K, V> {
    fn value<C: FFILink + ?Sized>(&self, link: &C) -> Result<Value> {
        let mut keys = Vec::with_capacity(self.entries.len());
        let mut values = Vec::with_capacity(self.entries.len());
        for (key, value) in &self.entries {
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

impl<K: FromValue, V: FromValue> FromValue for MapValue<K, Option<V>> {
    fn _get_inner(value: &Value) -> Result<Self> {
        let logical_type = value.logical_type()?;
        if logical_type.id()? != LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_MAP {
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

        let entries = children
            .as_chunks::<2>()
            .0
            .iter()
            .enumerate()
            .map(|(index, entry)| {
                let key = K::from_value(&entry[0])?
                    .ok_or_else(|| Error::invalid_input(format!("MAP entry {index} has a NULL key")))?;
                Ok((key, V::from_value(&entry[1])?))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self { entries })
    }
}

// SAFETY: a MAP is stored as a LIST of STRUCT(key, value), and `validate` checks both fields.
unsafe impl<K: VectorElement, V: VectorElement> VectorElement for Map<K, V> {
    const TYPE_ID: LogicalTypeID = LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_MAP;

    type Ref<'a>
        = MapRow<'a, K, V>
    where
        K: 'a,
        V: 'a;

    type Internal = super::List<()>;

    fn validate(other: &LogicalType, children: Option<&VectorCollection>) -> Result<bool> {
        if other.id()? != Self::TYPE_ID {
            return Ok(false);
        }

        // MAP stores its entries as one STRUCT(key, value) child.
        let fields = children
            .filter(|children| children.col_count() == 1)
            .map(|children| children.get_vector_at::<Struct>(0))
            .transpose()?
            .and_then(|entries| entries.children())
            .filter(|fields| fields.col_count() == 2)
            .ok_or_else(|| Error::invalid_input("Map vector must have one STRUCT(key, value) child"))?;

        fields.get_untyped_vector_at(0)?.validate_as::<K>()?;
        fields.get_untyped_vector_at(1)?.validate_as::<V>().map(|()| true)
    }

    unsafe fn get<'a, U: VectorElement>(vector: &'a Vector<'_, U>, physical: usize, _logical: usize) -> Self::Ref<'a>
    where
        Self: Sized + 'a,
    {
        let data_ptr = vector.view.as_ref().unwrap().as_ptr() as *const ffi::duckdb_v2_list_entry;
        let list = unsafe { &*data_ptr.add(physical) };

        MapRow::<K, V> {
            children: vector
                .children()
                .expect("validated nested vector has children")
                .cached_unchecked(0)
                .children()
                .expect("validated map entries have key and value children"),
            offset: list.offset as usize,
            length: list.length as usize,
            _marker: PhantomData,
        }
    }
}

// SAFETY: the entry covers child rows written through the fields' own `write`.
unsafe impl<K: WritableVectorElement, V: WritableVectorElement> WritableVectorElement for Map<K, V> {
    type Write<'a>
        = HashMap<K::Write<'a>, Option<V::Write<'a>>>
    where
        K: 'a,
        V: 'a;

    unsafe fn write(vector: &mut Vector<'_, Self>, index: usize, value: Option<Self::Write<'_>>) -> Result<()> {
        let Some(value) = value else {
            return vector.write_raw::<ffi::duckdb_v2_list_entry>(index, None);
        };

        // Reject the row before appending, or a failed write would leave orphaned child elements.
        vector.check_row_writable(index)?;
        let len = value.len();
        // Write one child at a time, so only one child wrapper is borrowed.
        let (keys, values): (Vec<_>, Vec<_>) = value.into_iter().unzip();
        let entries = vector
            .children_collection_mut()
            .expect("validated nested vector has children")
            .cached_mut::<Struct>(0)?;

        let result = (|| {
            // Append after the entries, including any written through another wrapper.
            let offset = entries.current_size()?;
            entries.set_size(offset + len)?;
            let fields = entries
                .children_collection_mut()
                .expect("validated map entries have key and value children");

            let key_vector = fields.cached_mut::<K>(0)?;
            for (child_index, key) in keys.into_iter().enumerate() {
                key_vector.write(offset + child_index, Some(key))?;
            }
            let value_vector = fields.cached_mut::<V>(1)?;
            for (child_index, value) in values.into_iter().enumerate() {
                value_vector.write(offset + child_index, value)?;
            }
            Ok(offset)
        })();

        let offset = result?;
        vector.write_raw(
            index,
            Some(ffi::duckdb_v2_list_entry {
                offset: offset as u64,
                length: len as u64,
            }),
        )
    }
}

/// A borrowed map row backed by matching ranges in key and value vectors.
#[derive(Debug)]
pub struct MapRow<'a, K, V> {
    pub(crate) children: &'a VectorCollection,
    pub(crate) offset: usize,
    pub(crate) length: usize,
    pub(crate) _marker: PhantomData<(K, V)>,
}

impl<'a, K: Debug + VectorElement, V: VectorElement + Debug> MapRow<'a, K, V>
where
    for<'b> K::Ref<'b>: PartialEq<&'b K> + Eq + std::hash::Hash,
    for<'b> <V as VectorElement>::Ref<'b>: Debug,
{
    /// Convert the map row into a `HashMap`.
    ///
    /// # ! Experimental. Subject to change.
    /// TODO: REVIEW
    pub fn to_hash_map(&self) -> Result<HashMap<K::Ref<'a>, Option<V::Ref<'a>>>> {
        let mut map = HashMap::new();
        map.reserve(self.length);

        for logical in self.offset..self.offset + self.length {
            if let Some(key) = { self.children.cached_unchecked(0).get_as_unchecked::<K>(logical) } {
                if let Some(value) = self.children.cached_unchecked(1).get_as_unchecked::<V>(logical) {
                    map.insert(key, Some(value));
                } else {
                    map.insert(key, None);
                }
            }
        }

        Ok(map)
    }
}

impl<'a, K: VectorElement + 'a, V: VectorElement + 'a> MapRow<'a, K, V> {
    /// Return the number of entries in the map.
    pub fn len(&self) -> usize {
        self.length
    }

    /// Return whether the map has no entries.
    pub fn is_empty(&self) -> bool {
        self.length == 0
    }

    /// Iterate over the map's `(key, value)` entries in storage order.
    pub fn iter(&self) -> impl Iterator<Item = (Option<K::Ref<'a>>, Option<V::Ref<'a>>)> + use<'a, K, V> {
        let children = self.children;
        (self.offset..self.offset + self.length).map(move |logical| {
            (
                children.cached_unchecked(0).get_as_unchecked::<K>(logical),
                children.cached_unchecked(1).get_as_unchecked::<V>(logical),
            )
        })
    }

    /// Return the map's keys in storage order, aligned with [`Self::values`].
    pub fn keys(&self) -> Vec<Option<K::Ref<'a>>> {
        self.iter().map(|(key, _)| key).collect()
    }

    /// Return the map's values in storage order, aligned with [`Self::keys`].
    pub fn values(&self) -> Vec<Option<V::Ref<'a>>> {
        self.iter().map(|(_, value)| value).collect()
    }
}

impl<'a, K: VectorElement, V: VectorElement> MapRow<'a, K, V>
where
    for<'b> K::Ref<'b>: PartialEq<&'b K>,
{
    /// Return the associated value; outer `None` means missing, inner `None` means NULL.
    pub fn get(&self, key: &K) -> Option<Option<V::Ref<'a>>> {
        (self.offset..self.offset + self.length)
            .find(|&logical| {
                self.children
                    .cached_unchecked(0)
                    .get_as_unchecked::<K>(logical)
                    .is_some_and(|value| value == key)
            })
            .map(|logical| self.children.cached_unchecked(1).get_as_unchecked::<V>(logical))
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
    fn map_value_conversions_preserve_order() {
        let value = MapValue::from(vec![(2, "b"), (1, "a")]);
        let collected = [(2, "b"), (1, "a")].into_iter().collect::<MapValue<_, _>>();
        assert_eq!(value, collected);
        assert_eq!(value.entries, [(2, "b"), (1, "a")]);
        assert_eq!(MapValue::<i32, &str>::default(), MapValue::from(vec![]));
    }

    #[test]
    fn map_write_null_value_empty_map_and_null_row() -> Result<()> {
        let env = Environment::new()?;
        let instance = env.open(StorageLocation::InMemory)?;
        let conn = instance.connect()?;

        let mut chunk = DataChunk::create(&conn, &[MapValue::<i32, i32>::logical_type(&conn)?], true)?;
        {
            let mut maps = chunk.get_vector_at_mut::<Map<i32, i32>>(0)?;
            maps.set_size(3)?;
            maps.write(0, Some(HashMap::from([(1, None), (2, Some(20))])))?;
            maps.write(1, Some(HashMap::new()))?;
            maps.write(2, None)?;
        }

        let maps = chunk.get_vector_at::<Map<i32, i32>>(0)?;
        let row = maps.get(0)?.expect("row 0 is not NULL");
        assert_eq!(row.get(&1), Some(None));
        assert_eq!(row.get(&2), Some(Some(&20)));
        assert_eq!(row.get(&3), None);
        let mut values = row.values();
        values.sort();
        assert_eq!(values, [None, Some(&20)]);

        assert!(maps.get(1)?.expect("row 1 is not NULL").keys().is_empty());
        assert!(maps.get(2)?.is_none());
        Ok(())
    }

    #[test]
    fn map_row_len_is_empty_and_iter() -> Result<()> {
        let env = Environment::new()?;
        let instance = env.open(StorageLocation::InMemory)?;
        let conn = instance.connect()?;

        let mut chunk = DataChunk::create(&conn, &[MapValue::<i32, i32>::logical_type(&conn)?], true)?;
        {
            let mut maps = chunk.get_vector_at_mut::<Map<i32, i32>>(0)?;
            maps.set_size(2)?;
            maps.write(0, Some(HashMap::from([(1, None), (2, Some(20)), (3, Some(30))])))?;
            maps.write(1, Some(HashMap::new()))?;
        }

        let maps = chunk.get_vector_at::<Map<i32, i32>>(0)?;
        let row = maps.get(0)?.expect("row 0 is not NULL");
        assert_eq!(row.len(), 3);
        assert!(!row.is_empty());
        let mut entries: Vec<_> = row.iter().collect();
        assert_eq!(row.keys().into_iter().zip(row.values()).collect::<Vec<_>>(), entries);
        entries.sort();
        assert_eq!(
            entries,
            [(Some(&1), None), (Some(&2), Some(&20)), (Some(&3), Some(&30))]
        );

        let empty = maps.get(1)?.expect("row 1 is not NULL");
        assert_eq!(empty.len(), 0);
        assert!(empty.is_empty());
        assert_eq!(empty.iter().count(), 0);
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
        let row = maps.get(0)?.expect("row 0 is not NULL");

        let entries: Vec<_> = row
            .iter()
            .map(|(key, value)| (key.map(|key| key.iter().collect::<Vec<_>>()), value))
            .collect();
        assert_eq!(
            entries,
            [
                (Some(vec![Some(&1), Some(&2)]), Some(&10)),
                (Some(vec![Some(&3)]), None)
            ]
        );

        let keys: Vec<_> = row
            .keys()
            .into_iter()
            .map(|key| key.unwrap().iter().collect::<Vec<_>>())
            .collect();
        assert_eq!(keys, [vec![Some(&1), Some(&2)], vec![Some(&3)]]);
        assert_eq!(row.values(), [Some(&10), None]);
        Ok(())
    }
}

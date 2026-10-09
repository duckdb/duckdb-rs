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
    value::{Value, ValueInput},
    vector::{Vector, VectorElement, WritableVectorElement},
};

/// Reads a `MAP` vector with key type `K` and value type `V`.
#[derive(Debug)]
pub struct Map<K, V>(pub ffi::duckdb_v2_list_entry, pub PhantomData<(K, V)>);

/// Key-value entries represented as a DuckDB `MAP`.
#[derive(Debug)]
pub struct MapValue<K, V> {
    /// Entries in insertion order.
    pub entries: Vec<(K, V)>,
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
        let logical_type = value.fetch_logical_type()?;
        if logical_type.type_id() != LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_MAP {
            return Err(Error::invalid_input(format!(
                "Expected MAP value, found {}",
                logical_type.to_string()?
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

impl<K: VectorElement, V: VectorElement> VectorElement for Map<K, V> {
    const TYPE_ID: LogicalTypeID = LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_MAP;

    type Ref<'a>
        = MapRow<'a, K, V>
    where
        K: 'a,
        V: 'a;

    type Internal = super::List<()>;

    fn validate(other: &LogicalType, children: Option<&VectorCollection>) -> Result<bool> {
        if other.type_id() != Self::TYPE_ID {
            return Ok(false);
        }

        let Some(children) = children.filter(|children| children.col_count() == 2) else {
            return Err(Error::invalid_input("Map vector must have exactly two children"));
        };

        children.get_untyped_vector_at(0)?.validate_as::<K>()?;
        children.get_untyped_vector_at(1)?.validate_as::<V>()
    }

    fn get<'a, U: VectorElement>(vector: &'a Vector<'_, U>, physical: usize, _logical: usize) -> Self::Ref<'a>
    where
        Self: Sized + 'a,
    {
        let data_ptr = vector.view.as_ref().unwrap().as_ptr() as *const ffi::duckdb_v2_list_entry;
        let list = unsafe { &*data_ptr.add(physical) };

        MapRow::<K, V> {
            children: vector.children().expect("validated nested vector has children"),
            offset: list.offset as usize,
            length: list.length as usize,
            _marker: PhantomData,
        }
    }
}

impl<K: WritableVectorElement, V: WritableVectorElement> WritableVectorElement for Map<K, V> {
    type Write<'a>
        = HashMap<K::Write<'a>, V::Write<'a>>
    where
        K: 'a,
        V: 'a;

    fn write(vector: &mut Vector<'_, Self>, index: usize, value: Option<Self::Write<'_>>) -> Result<()> {
        let Some(value) = value else {
            return vector.write_raw::<ffi::duckdb_v2_list_entry>(index, None);
        };

        // Reject the row before appending, or a failed write would leave orphaned child elements.
        vector.check_row_writable(index)?;
        let len = value.len();
        // Write one child at a time, so only one child wrapper is borrowed.
        let (keys, values): (Vec<_>, Vec<_>) = value.into_iter().unzip();
        let children = vector
            .children_collection_mut()
            .expect("validated nested vector has children");

        let result = (|| {
            // Append after the child's elements, including any written through another wrapper.
            let key_vector = children.cached_mut::<K>(0)?;
            let offset = key_vector.current_size()?;
            key_vector.set_size(offset + len)?;
            for (child_index, key) in keys.into_iter().enumerate() {
                key_vector.write(offset + child_index, Some(key))?;
            }
            let value_vector = children.cached_mut::<V>(1)?;
            value_vector.set_size(offset + len)?;
            for (child_index, value) in values.into_iter().enumerate() {
                value_vector.write(offset + child_index, Some(value))?;
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

impl<'a, K: Debug + VectorElement, V: VectorElement + Debug> MapRow<'a, K, V>
where
    for<'b> K::Ref<'b>: PartialEq<&'b K>,
    for<'b> <V as VectorElement>::Ref<'b>: Debug,
{
    /// Return the value associated with `key`, if it exists.
    pub fn get(&self, key: &K) -> Result<Option<V::Ref<'a>>> {
        let mut index = None;

        for logical in self.offset..self.offset + self.length {
            if self
                .children
                .cached_unchecked(0)
                .get_as_unchecked::<K>(logical)
                .is_some_and(|value| value == key)
            {
                index = Some(logical);
                break;
            }
        }

        if index.is_none() {
            return Err(Error::invalid_parameter(format!("Key '{:?}' not found in map", key)));
        }

        Ok(self.children.cached_unchecked(1).get_as_unchecked::<V>(index.unwrap()))
    }

    /// Return the map's keys.
    pub fn keys(&self) -> Result<Vec<K::Ref<'a>>> {
        let mut keys = Vec::new();

        self.children.cached_unchecked(0).validate_as::<K>()?;

        for logical in self.offset..self.offset + self.length {
            if let Some(key) = self.children.cached_unchecked(0).get_as_unchecked::<K>(logical) {
                keys.push(key);
            }
        }
        Ok(keys)
    }

    /// Return the map's values.
    pub fn values(&self) -> Result<Vec<V::Ref<'a>>> {
        let mut values: Vec<_> = Vec::new();

        self.children.cached_unchecked(1).validate_as::<V>()?;

        for logical in self.offset..self.offset + self.length {
            if let Some(value) = self.children.cached_unchecked(1).get_as_unchecked::<V>(logical) {
                values.push(value);
            }
        }

        Ok(values)
    }
}

//! The `MAP` logical type.

use std::collections::HashMap;
use std::fmt::Debug;
use std::marker::PhantomData;

use super::{DuckDBType, ToValue};
use crate::{
    Parameters, Result,
    connection::FFILink,
    error::{DuckDBError, Error},
    ffi,
    logical_type::{LogicalType, LogicalTypeID},
    value::{Value, ValueInput},
    vector::{Unknown, Vector, VectorElement, WritableVectorElement},
};

/// Reads a `MAP` vector with key type `K` and value type `V`.
pub struct Map<K, V>(pub ffi::duckdb_v2_list_entry, pub PhantomData<(K, V)>);

/// Key-value entries represented as a DuckDB `MAP`.
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

impl<K: VectorElement, V: VectorElement> VectorElement for Map<K, V> {
    const TYPE_ID: LogicalTypeID = LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_MAP;

    type Ref<'a>
        = MapRow<'a, K, V>
    where
        K: 'a,
        V: 'a;

    type Internal = super::List<()>;

    fn validate(other: &LogicalType, children: &[Vector<'_, Unknown>]) -> Result<bool> {
        if other.type_id() != Self::TYPE_ID {
            return Ok(false);
        }

        if children.len() != 2 {
            return Err(Error {
                code: DuckDBError::DUCKDB_V2_ERROR_INPUT_INVALID,
                message: "Map vector must have exactly two children".to_string(),
            });
        }

        children[0].validate_as::<K>()?;
        children[1].validate_as::<V>()
    }

    fn get<'a, U: VectorElement>(vector: &'a Vector<'_, U>, physical: usize, _logical: usize) -> Self::Ref<'a>
    where
        Self: Sized + 'a,
    {
        let data_ptr = vector.view.as_ref().unwrap().as_ptr() as *const ffi::duckdb_v2_list_entry;
        let list = unsafe { &*data_ptr.add(physical) };

        MapRow::<K, V> {
            children: &vector.children,
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

        let offset = vector.child_write_offset;
        let len = value.len();
        let mut children = std::mem::take(&mut vector.children).into_iter();
        let mut keys = children.next().expect("validated map key child").cast_unchecked::<K>();
        let mut values = children
            .next()
            .expect("validated map value child")
            .cast_unchecked::<V>();

        let result = (|| {
            keys.set_size(offset + len)?;
            values.set_size(offset + len)?;
            for (child_index, (key, value)) in value.into_iter().enumerate() {
                keys.write(offset + child_index, Some(key))?;
                values.write(offset + child_index, Some(value))?;
            }
            Ok(())
        })();

        vector.children = vec![keys.cast_unchecked::<Unknown>(), values.cast_unchecked::<Unknown>()];
        result?;
        vector.child_write_offset += len;
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
pub struct MapRow<'a, K, V> {
    pub(crate) children: &'a [Vector<'a, Unknown>],
    pub(crate) offset: usize,
    pub(crate) length: usize,
    pub(crate) _marker: PhantomData<(K, V)>,
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
            if self.children[0]
                .get_as_unchecked::<K>(logical)
                .is_some_and(|value| value == key)
            {
                index = Some(logical);
                break;
            }
        }

        if index.is_none() {
            return Err(Error {
                code: DuckDBError::DUCKDB_V2_ERROR_INPUT_PARAMETER_INVALID,
                message: format!("Key '{:?}' not found in map", key),
            });
        }

        Ok(self.children[1].get_as_unchecked::<V>(index.unwrap()))
    }

    /// Return the map's keys.
    pub fn keys(&self) -> Result<Vec<K::Ref<'a>>> {
        let mut keys = Vec::new();

        self.children[0].validate_as::<K>()?;

        for logical in self.offset..self.offset + self.length {
            if let Some(key) = self.children[0].get_as_unchecked::<K>(logical) {
                keys.push(key);
            }
        }
        Ok(keys)
    }

    /// Return the map's values.
    pub fn values(&self) -> Result<Vec<V::Ref<'a>>> {
        let mut values: Vec<_> = Vec::new();

        self.children[1].validate_as::<V>()?;

        for logical in self.offset..self.offset + self.length {
            if let Some(value) = self.children[1].get_as_unchecked::<V>(logical) {
                values.push(value);
            }
        }

        Ok(values)
    }
}

//! The `UNION` logical type.

use std::marker::PhantomData;

use super::{DuckDBType, ToValue};
use crate::{
    Result,
    connection::FFILink,
    data_chunk::VectorCollection,
    error::Error,
    logical_type::{LogicalType, LogicalTypeID},
    parameter::{Parameters, QueryParameter},
    value::Value,
    vector::{Unknown, Vector, VectorElement, WritableVectorElement},
};
/// Reads a `UNION` vector through its tag and member child vectors.
#[derive(Debug)]
pub struct Union;

/// Defines the named members of a [`UnionValue`].
pub trait UnionSchema {
    /// Return member names and types in tag order.
    fn members<C: FFILink + ?Sized>(link: &C) -> Result<Vec<(&'static str, LogicalType)>>;
}

/// A DuckDB `UNION` with schema `S`; implicit-cast costs select the active member.
/// Conversion fails when multiple members tie for the cheapest cast.
pub struct UnionValue<S, T> {
    /// The active member value.
    pub value: T,
    _schema: PhantomData<S>,
}

impl<S, T: std::fmt::Debug> std::fmt::Debug for UnionValue<S, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UnionValue").field("value", &self.value).finish()
    }
}

impl<S, T> UnionValue<S, T> {
    /// Create a union value from its active member.
    pub fn new(value: T) -> Self {
        Self {
            value,
            _schema: PhantomData,
        }
    }
}

impl<S: UnionSchema, T> DuckDBType for UnionValue<S, T> {
    fn logical_type<C: FFILink + ?Sized>(link: &C) -> Result<LogicalType> {
        let members = S::members(link)?;
        let values = members
            .into_iter()
            .map(|(name, logical_type)| Ok((name, Value::from_logical_type(link, &logical_type)?)))
            .collect::<Result<Vec<_>>>()?;
        let parameters = values
            .iter()
            .map(|(name, value)| (*name, value as &dyn QueryParameter))
            .collect::<Vec<_>>();
        link.logical_type_create("UNION", Parameters::named(&parameters))
    }
}

impl<S: UnionSchema, T: ToValue> ToValue for UnionValue<S, T> {
    fn value<C: FFILink + ?Sized>(&self, link: &C) -> Result<Value> {
        let value = self.value.value(link)?;
        link.value_cast(&value, &Self::logical_type(link)?)
    }
}

// SAFETY: members are read only through the child vectors, and `validate` checks the tag.
unsafe impl VectorElement for Union {
    const TYPE_ID: LogicalTypeID = LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_UNION;
    type Ref<'a> = UnionRow<'a>;

    type Internal = Union;

    fn validate(other: &LogicalType, children: Option<&VectorCollection>) -> Result<bool> {
        if other.id()? != Self::TYPE_ID {
            return Ok(false);
        }

        let Some(children) = children.filter(|children| children.col_count() > 0) else {
            return Err(Error::invalid_input("Union vector is missing its tag child"));
        };
        children.get_untyped_vector_at(0)?.validate_as::<u8>().map(|()| true)
    }

    unsafe fn get<'a, U: VectorElement>(
        vector: &crate::vector::VectorReader<'a, U>,
        physical: usize,
        _logical: usize,
    ) -> Self::Ref<'a>
    where
        Self: Sized + 'a,
    {
        let children = vector.children().expect("validated nested vector has children");
        // SAFETY: The parent reader prepared the tag, which validate checked as u8.
        let tag = unsafe {
            crate::vector::VectorReader::from_prepared(children.cached_unchecked(0).cast_ref_unchecked::<u8>())
        };
        UnionRow {
            children,
            tag,
            logical: physical,
        }
    }
}

/// A borrowed union row with access to its tag and member child vectors.
#[derive(Debug)]
pub struct UnionRow<'a> {
    children: &'a VectorCollection,
    tag: crate::vector::VectorReader<'a, u8>,
    logical: usize,
}

impl<'a> UnionRow<'a> {
    /// Return the active union member index.
    pub fn member(&self) -> u8 {
        // SAFETY: The tag reader was prepared with the parent and this row is in bounds.
        unsafe { *self.tag.get_unchecked(self.logical).unwrap() }
    }

    /// Return a union member by index after validating its logical type.
    pub fn get<T: VectorElement>(&self, index: usize) -> Result<Option<T::Ref<'a>>> {
        let child_index = index
            .checked_add(1)
            .filter(|&index| index < self.children.col_count())
            .ok_or_else(|| {
                Error::invalid_parameter(format!(
                    "Union member index {} is out of bounds ({} members)",
                    index,
                    self.children.col_count() - 1
                ))
            })?;

        self.children
            .cached_unchecked(child_index)
            .cast_ref::<T>()?
            .reader()?
            .get(self.logical)
    }
}

trait UnionFieldWrite {
    fn validate(&self, child: &Vector<'_, Unknown>) -> Result<()>;

    fn write(self: Box<Self>, child: &mut Vector<'_, Unknown>, index: usize) -> Result<()>;
}

#[derive(Debug)]
struct TypedUnionField<'a, T: WritableVectorElement + 'a> {
    member: Option<T::Write<'a>>,
}

impl<'a, T> UnionFieldWrite for TypedUnionField<'a, T>
where
    T: WritableVectorElement + 'a,
    T::Write<'a>: 'a,
{
    fn validate(&self, child: &Vector<'_, Unknown>) -> Result<()> {
        child.validate_as::<T>()
    }

    fn write(self: Box<Self>, child: &mut Vector<'_, Unknown>, index: usize) -> Result<()> {
        child.writer()?.write_as::<T>(index, self.member)
    }
}

/// A typed value for one writable `UNION` row.
pub struct UnionWriter<'a> {
    tag: u8,
    value: Box<dyn UnionFieldWrite + 'a>,
}

impl std::fmt::Debug for UnionWriter<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UnionWriter")
            .field("tag", &self.tag)
            .finish_non_exhaustive()
    }
}

impl<'a> UnionWriter<'a> {
    /// Select a zero-based union member and provide its typed value.
    pub fn set_value<T>(index: u8, value: Option<T::Write<'a>>) -> Self
    where
        T: WritableVectorElement + 'a,
        T::Write<'a>: 'a,
    {
        Self {
            tag: index,
            value: Box::new(TypedUnionField::<T> { member: value }),
        }
    }
}

// SAFETY: the tag is range-checked, and members are written through the children's own `write`.
unsafe impl WritableVectorElement for Union {
    type Write<'a> = UnionWriter<'a>;

    unsafe fn write(
        vector: &mut crate::vector::VectorWriter<'_, '_, Self>,
        index: usize,
        value: Option<Self::Write<'_>>,
    ) -> Result<()> {
        let Some(value) = value else {
            // A union is a struct of tag and members; this nulls all of them.
            return vector.set_null_slow(index);
        };

        let member_count = vector
            .children()
            .map_or(0, VectorCollection::col_count)
            .saturating_sub(1);
        if value.tag as usize >= member_count {
            return Err(Error::invalid_parameter(format!(
                "Union member index {} is out of bounds ({} members)",
                value.tag, member_count
            )));
        }

        let member = 1 + value.tag as usize;
        value.value.validate(
            vector
                .children()
                .ok_or_else(|| Error::invalid_input("Union vector is missing its member children"))?
                .get_untyped_vector_at(member)?,
        )?;

        let vector_len = vector.len();
        let children = vector
            .children_collection_mut()
            .expect("validated nested vector has children");

        for i in 0..children.col_count() {
            let child = children.cached_mut::<Unknown>(i)?;
            if child.len() != vector_len {
                child.set_size(vector_len)?;
            }
            if i == 0 {
                child.writer()?.write_as::<u8>(index, Some(value.tag))?;
            } else if i != member {
                // Nested fixed-width rows need NULL descendants, which only the slow path sets.
                let nested = matches!(
                    child.logical_type().id()?,
                    LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_STRUCT
                        | LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_TUPLE
                        | LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_UNION
                        | LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_ARRAY
                );
                if nested {
                    child.set_null_slow(index)?;
                } else {
                    // SAFETY: The row is in bounds and these types need no descendant NULL updates.
                    unsafe { child.writer()?.set_row_validity(index, false)? };
                }
            }
        }
        // The member's own write sets its validity.
        value.value.write(children.cached_mut::<Unknown>(member)?, index)
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
    fn union_value_debug_does_not_require_schema_debug() {
        struct Schema;
        let value = UnionValue::<Schema, _>::new(42);
        assert_eq!(format!("{value:?}"), "UnionValue { value: 42 }");
    }

    #[test]
    fn union_type_mismatch_preserves_row() -> Result<()> {
        let env = Environment::new()?;
        let conn = env.open(StorageLocation::InMemory)?.connect()?;
        let value = conn
            .query(
                "SELECT union_value(str := 'duck')::UNION(num INTEGER, str VARCHAR)",
                Parameters::None,
            )?
            .next_chunk()?
            .unwrap()
            .get_vector_at::<Union>(0)?
            .get_value_slow(0)?;
        let mut chunk = DataChunk::create(&conn, &[value.logical_type()?], true)?;
        {
            let mut vector = chunk.get_vector_at_mut::<Union>(0)?;
            vector.set_size(2)?;
            vector.write_value_slow(0, value)?;
            vector.writer()?.set(1, None)?;
            for index in 0..2 {
                let invalid = UnionWriter::set_value::<String>(0, Some("bad"));
                assert!(vector.writer()?.set(index, Some(invalid)).is_err());
            }
        }
        let vector = chunk.get_vector_at::<Union>(0)?;
        let vector_reader = vector.reader()?;
        let row = vector_reader.get(0)?.unwrap();
        assert_eq!(row.member(), 1);
        assert_eq!(row.get::<i32>(0)?, None);
        assert_eq!(row.get::<String>(1)?, Some("duck"));
        assert!(vector_reader.get(1)?.is_none());
        Ok(())
    }

    #[test]
    fn union_member_index_overflow_returns_error() -> Result<()> {
        let env = Environment::new()?;
        let conn = env.open(StorageLocation::InMemory)?.connect()?;
        let chunk = conn
            .query("SELECT union_value(x := 42)", Parameters::None)?
            .next_chunk()?
            .unwrap();
        let row = chunk.get_vector_at::<Union>(0)?.reader()?.get(0)?.unwrap();
        assert!(row.get::<u8>(usize::MAX).is_err());
        assert_eq!(row.get::<i32>(0)?, Some(&42));
        Ok(())
    }
}

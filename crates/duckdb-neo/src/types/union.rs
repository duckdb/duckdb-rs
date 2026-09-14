//! The `UNION` logical type.

use std::marker::PhantomData;

use super::{DuckDBType, ToValue};
use crate::{
    Result,
    connection::FFILink,
    error::{DuckDBError, Error},
    logical_type::{LogicalType, LogicalTypeID},
    parameter::{Parameters, QueryParameter},
    value::Value,
    vector::{Unknown, Vector, VectorElement, WritableVectorElement},
};

/// Reads a `UNION` vector through its tag and member child vectors.
pub struct Union;

/// Defines the named members of a [`UnionValue`].
pub trait UnionSchema {
    /// Return member names and types in tag order.
    fn members<C: FFILink + ?Sized>(link: &C) -> Result<Vec<(&'static str, LogicalType)>>;
}

/// An active member represented as a DuckDB `UNION` with schema `S`.
pub struct UnionValue<S, T> {
    /// The active member value.
    pub value: T,
    _schema: PhantomData<S>,
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
        link.value_cast(&value, Self::logical_type(link)?)
    }
}

impl VectorElement for Union {
    const TYPE_ID: LogicalTypeID = LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_UNION;
    type Ref<'a> = UnionRow<'a>;

    type Internal = Union;

    fn validate(other: &LogicalType, children: &[Vector<'_, Unknown>]) -> Result<bool> {
        if other.type_id() != Self::TYPE_ID {
            return Ok(false);
        }

        let tag = children.first().ok_or_else(|| Error {
            code: DuckDBError::DUCKDB_V2_ERROR_INPUT_INVALID,
            message: "Union vector is missing its tag child".to_string(),
        })?;
        tag.validate_as::<u8>()
    }

    fn get<'a, U: VectorElement>(vector: &'a Vector<'_, U>, physical: usize, _logical: usize) -> Self::Ref<'a>
    where
        Self: Sized + 'a,
    {
        UnionRow {
            children: &vector.children,
            logical: physical,
        }
    }
}

/// A borrowed union row with access to its tag and member child vectors.
pub struct UnionRow<'a> {
    children: &'a [Vector<'a, Unknown>],
    logical: usize,
}

impl<'a> UnionRow<'a> {
    /// Return the active union member index.
    pub fn member(&self) -> u8 {
        *self.children[0].get_as_unchecked::<u8>(self.logical).unwrap()
    }

    /// Return a union member by index after validating its logical type.
    pub fn get<T: VectorElement>(&self, index: usize) -> Result<Option<T::Ref<'a>>> {
        let index = index + 1;

        if index >= self.children.len() {
            return Err(Error {
                code: DuckDBError::DUCKDB_V2_ERROR_INPUT_PARAMETER_INVALID,
                message: format!(
                    "Union member index {} is out of bounds ({} members)",
                    index - 1,
                    self.children.len() - 1
                ),
            });
        }

        let child = self.children.get(index).ok_or_else(|| Error {
            code: DuckDBError::DUCKDB_V2_ERROR_INPUT_INVALID,
            message: format!("Union member {} is missing its child vector", index - 1),
        })?;

        child.get_as_checked::<T>(self.logical)
    }
}

trait UnionFieldWrite {
    fn write(self: Box<Self>, child: &mut Vector<'_, Unknown>, index: usize) -> Result<()>;
}

struct TypedUnionField<'a, T: WritableVectorElement + 'a> {
    member: Option<T::Write<'a>>,
}

impl<'a, T> UnionFieldWrite for TypedUnionField<'a, T>
where
    T: WritableVectorElement + 'a,
    T::Write<'a>: 'a,
{
    fn write(self: Box<Self>, child: &mut Vector<'_, Unknown>, index: usize) -> Result<()> {
        child.write_as::<T>(index, self.member)
    }
}

/// A typed value for one writable `UNION` row.
pub struct UnionWriter<'a> {
    tag: u8,
    value: Box<dyn UnionFieldWrite + 'a>,
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

impl WritableVectorElement for Union {
    type Write<'a> = UnionWriter<'a>;

    fn write(vector: &mut Vector<'_, Self>, index: usize, value: Option<Self::Write<'_>>) -> Result<()> {
        let Some(value) = value else {
            vector.set_row_validity(index, false)?;
            for child in &mut vector.children {
                child.set_row_validity(index, false)?;
            }
            return Ok(());
        };

        let member_count = vector.children.len().saturating_sub(1);
        if value.tag as usize >= member_count {
            return Err(Error {
                code: DuckDBError::DUCKDB_V2_ERROR_INPUT_PARAMETER_INVALID,
                message: format!(
                    "Union member index {} is out of bounds ({} members)",
                    value.tag, member_count
                ),
            });
        }

        vector.set_row_validity(index, true)?;
        vector.children[0].write_as::<u8>(index, Some(value.tag))?;
        for child in &mut vector.children[1..] {
            child.set_row_validity(index, false)?;
        }
        value.value.write(&mut vector.children[1 + value.tag as usize], index)
    }
}

//! The fixed-length `ARRAY` logical type.

use std::marker::PhantomData;

use super::{DuckDBType, ToValue};
use crate::{
    Parameters, Result,
    connection::FFILink,
    error::{DuckDBError, Error},
    logical_type::{LogicalType, LogicalTypeID},
    value::{Value, ValueInput},
    vector::{Unknown, Vector, VectorElement},
};

/// A fixed-length array element parameterized by its child element type.
#[repr(C)]
pub struct Array<T> {
    offset: u64,
    length: u64,
    _marker: PhantomData<T>,
}

impl<T: DuckDBType, const N: usize> DuckDBType for [T; N] {
    fn logical_type<C: FFILink + ?Sized>(link: &C) -> Result<LogicalType> {
        let child_type = Value::from_logical_type(link, &T::logical_type(link)?)?;
        let length = (N as u64).value(link)?;
        link.logical_type_create("ARRAY", Parameters::positional(&[&child_type, &length]))
    }
}

impl<T: ToValue + DuckDBType, const N: usize> ToValue for [T; N] {
    fn value<C: FFILink + ?Sized>(&self, link: &C) -> Result<Value> {
        let children = self.iter().map(|value| value.value(link)).collect::<Result<Vec<_>>>()?;
        let child_type = T::logical_type(link)?;
        link.create_value(ValueInput::Array {
            child_type: &child_type,
            children: &children,
        })
    }
}

impl<T: VectorElement> VectorElement for Array<T> {
    const TYPE_ID: LogicalTypeID = LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_ARRAY;

    type Ref<'a>
        = ArrayRef<'a, T>
    where
        T: 'a;

    type Internal = Array<T>;

    fn validate(other: &LogicalType, children: &[Vector<'_, Unknown>]) -> Result<bool> {
        if other.type_id() != Self::TYPE_ID {
            return Ok(false);
        }

        let child = children.first().ok_or_else(|| Error {
            code: DuckDBError::DUCKDB_V2_ERROR_INPUT_INVALID,
            message: "Array vector is missing its child vector".to_string(),
        })?;
        child.validate_as::<T>()
    }

    fn get<'a, U: VectorElement>(vector: &'a Vector<'_, U>, physical: usize, _logical: usize) -> Self::Ref<'a>
    where
        Self: Sized + 'a,
    {
        let slice_size = vector.children[0].len() / vector.len();

        ArrayRef {
            offset: physical * slice_size,
            size: slice_size,
            _length: vector.children[0].len(),
            child: &vector.children[0],
            _marker: PhantomData,
        }
    }
}

/// A borrowed array row backed by a contiguous range in its child vector.
pub struct ArrayRef<'a, T> {
    offset: usize,
    size: usize,
    _length: usize,
    child: &'a Vector<'a, Unknown>,
    _marker: PhantomData<T>,
}

impl<'a, T: VectorElement> ArrayRef<'a, T> {
    /// Iterate over the array's values.
    pub fn iter(&self) -> ArrayIterator<'a, T> {
        ArrayIterator {
            child: self.child,
            offset: self.offset,
            length: self.size,
            index: 0,
            _type: PhantomData,
        }
    }

    /// Return the number of values in the array.
    pub fn size(&self) -> usize {
        self.size
    }
}

/// Traverses the child-vector range belonging to an [`ArrayRef`].
pub struct ArrayIterator<'a, T> {
    child: &'a Vector<'a, Unknown>,
    offset: usize,
    length: usize,
    index: usize,
    _type: PhantomData<T>,
}

impl<'a, T: VectorElement + 'a> Iterator for ArrayIterator<'a, T> {
    type Item = Option<T::Ref<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.length {
            return None;
        }

        let logical = self.offset + self.index;
        self.index += 1;

        Some(self.child.get_as_unchecked::<T>(logical))
    }
}

//! The fixed-length `ARRAY` logical type.

use std::marker::PhantomData;

use super::{DuckDBType, FromValue, ToValue};
use crate::{
    Parameters, Result,
    connection::FFILink,
    data_chunk::VectorCollection,
    error::Error,
    logical_type::{LogicalType, LogicalTypeID},
    value::{Value, ValueInput},
    vector::{Unknown, Vector, VectorElement, WritableVectorElement},
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

impl<T: FromValue, const N: usize> FromValue for [Option<T>; N] {
    fn _get_inner(value: &Value) -> Result<Self> {
        let logical_type = value.fetch_logical_type()?;
        if logical_type.type_id() != LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_ARRAY {
            return Err(Error::invalid_input(format!(
                "Expected ARRAY value, found {}",
                logical_type.to_string()?
            )));
        }

        let children = value.children()?;
        if children.len() != N {
            return Err(Error::invalid_input(format!(
                "ARRAY has {} elements, expected {N}",
                children.len()
            )));
        }
        let values = children.iter().map(T::from_value).collect::<Result<Vec<_>>>()?;
        values.try_into().map_err(|values: Vec<Option<T>>| {
            Error::invalid_input(format!("ARRAY has {} elements, expected {N}", values.len()))
        })
    }
}

/// Read the element count of an `ARRAY` type.
pub(crate) fn array_size(logical_type: &LogicalType) -> Result<usize> {
    let (_, array_size) = logical_type.get_param(1)?;
    Ok(array_size
        .get::<u64>()?
        .ok_or(Error::api_error("Failed to get array_size from logical type"))? as usize)
}

impl<T: VectorElement> VectorElement for Array<T> {
    const TYPE_ID: LogicalTypeID = LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_ARRAY;

    type Ref<'a>
        = ArrayRef<'a, T>
    where
        T: 'a;

    type Internal = Array<T>;

    fn validate(other: &LogicalType, children: Option<&VectorCollection>) -> Result<bool> {
        if other.type_id() != Self::TYPE_ID {
            return Ok(false);
        }

        let Some(children) = children.filter(|children| children.col_count() == 1) else {
            return Err(Error::invalid_input("Array vector must have exactly one child"));
        };
        children.get_untyped_vector_at(0)?.validate_as::<T>()
    }

    fn get<'a, U: VectorElement>(vector: &'a Vector<'_, U>, physical: usize, _logical: usize) -> Self::Ref<'a>
    where
        Self: Sized + 'a,
    {
        let child = vector
            .children()
            .expect("validated nested vector has children")
            .cached_unchecked(0);
        ArrayRef {
            offset: physical * vector.array_size(),
            size: vector.array_size(),
            _length: child.len(),
            child,
            _marker: PhantomData,
        }
    }
}

impl<T: WritableVectorElement> WritableVectorElement for Array<T> {
    type Write<'a>
        = Vec<Option<T::Write<'a>>>
    where
        T: 'a;

    fn write(vector: &mut Vector<'_, Self>, index: usize, value: Option<Self::Write<'_>>) -> Result<()> {
        let array_size = vector.array_size();

        if let Some(values) = &value
            && values.len() != array_size
        {
            return Err(Error::invalid_parameter(format!(
                "Array row has {} elements, expected {}",
                values.len(),
                array_size
            )));
        }

        let child_len = vector
            .len
            .checked_mul(array_size)
            .ok_or_else(|| Error::invalid_input("Array child length overflow"))?;
        let child = vector
            .children_collection_mut()
            .expect("validated nested vector has children")
            .cached_mut::<T>(0)?;
        if child.len() != child_len {
            child.set_size(child_len)?;
        }

        let Some(values) = value else {
            return vector.set_null_slow(index);
        };

        let offset = index
            .checked_mul(array_size)
            .ok_or_else(|| Error::invalid_input("Array child offset overflow"))?;
        for (child_index, value) in values.into_iter().enumerate() {
            child.write(offset + child_index, value)?;
        }
        vector.set_row_validity(index, true)
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

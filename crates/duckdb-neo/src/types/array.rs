//! The fixed-length `ARRAY` logical type.
//!
//! `[T; N]` writes owned arrays and validates their length when attached to a vector.
//! Use `[Option<T>; N]` for nullable elements; readers preserve element NULLs for either form.

use std::marker::PhantomData;

use super::{DuckDBType, FromValue, ToValue};
use crate::{
    Parameters, Result,
    connection::FFILink,
    data_chunk::VectorCollection,
    error::Error,
    logical_type::{LogicalType, LogicalTypeID},
    value::{Value, ValueInput},
    vector::{OwnedVectorElement, VectorElement, WritableVectorElement},
};

/// A fixed-length array element marker; `[T; N]` is the owned form.
#[derive(Debug)]
pub struct Array<T>(PhantomData<T>);

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
        let logical_type = value.logical_type()?;
        if logical_type.id()? != LogicalTypeID::Array {
            return Err(Error::invalid_input(format!(
                "Expected ARRAY value, found {}",
                logical_type.to_text()?
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
        Ok(values
            .try_into()
            .unwrap_or_else(|_| unreachable!("checked exactly N children")))
    }
}

impl<T: FromValue, const N: usize> FromValue for [T; N] {
    fn _get_inner(value: &Value) -> Result<Self> {
        let values = <[Option<T>; N]>::_get_inner(value)?
            .into_iter()
            .enumerate()
            .map(|(index, value)| value.ok_or_else(|| Error::invalid_input(format!("ARRAY element {index} is NULL"))))
            .collect::<Result<Vec<_>>>()?;
        Ok(values
            .try_into()
            .unwrap_or_else(|_| unreachable!("checked exactly N elements")))
    }
}

// SAFETY: the marker layout matches Array, and validation also checks the fixed length.
unsafe impl<T: VectorElement, const N: usize> VectorElement for [T; N] {
    const TYPE_ID: LogicalTypeID = LogicalTypeID::Array;
    type Internal<'a> = ();
    type Ref<'a>
        = ArrayRef<'a, T>
    where
        T: 'a;

    fn validate(other: &LogicalType, children: Option<&VectorCollection>) -> Result<bool> {
        Ok(Array::<T>::validate(other, children)? && array_size(other)? == N)
    }

    unsafe fn get<'a, U: VectorElement>(
        reader: &crate::vector::VectorReader<'a, U>,
        physical: usize,
        logical: usize,
    ) -> Self::Ref<'a>
    where
        Self: 'a,
    {
        unsafe { Array::<T>::get(reader, physical, logical) }
    }
}

// SAFETY: validation fixes the length; element writers initialize values and propagate NULLs.
unsafe impl<T: OwnedVectorElement, const N: usize> WritableVectorElement for [T; N] {
    type Write<'a> = Self;

    unsafe fn write(
        vector: &mut crate::vector::VectorWriter<'_, Self>,
        index: usize,
        value: Option<Self>,
    ) -> Result<()> {
        unsafe { Self::write_ref(vector, index, value.as_ref()) }
    }
}

// SAFETY: validation fixes the length; element writers initialize values and propagate NULLs.
unsafe impl<T: OwnedVectorElement, const N: usize> OwnedVectorElement for [T; N] {
    unsafe fn write_ref(
        vector: &mut crate::vector::VectorWriter<'_, Self>,
        index: usize,
        value: Option<&Self>,
    ) -> Result<()> {
        let len = vector
            .len()
            .checked_mul(N)
            .ok_or_else(|| Error::invalid_input("Array child length overflow"))?;
        let child = vector
            .children_collection_mut()
            .expect("validated array has a child")
            .cached_mut::<T>(0)?;
        if child.len() != len {
            child.set_size(len)?;
        }
        let mut writer = child.writer()?;
        let offset = index * N;
        match value {
            Some(values) => {
                for (index, value) in values.iter().enumerate() {
                    // SAFETY: the validated parent row covers N in-bounds child rows.
                    unsafe { T::write_ref(&mut writer, offset + index, Some(value))? };
                }
            }
            None => {
                for index in 0..N {
                    // SAFETY: a NULL array must also null each of its in-bounds elements.
                    unsafe { T::write_ref(&mut writer, offset + index, None)? };
                }
            }
        }
        drop(writer);
        // SAFETY: the caller guarantees an in-bounds row, whose elements were written above.
        unsafe { vector.set_validity_unchecked(index, value.is_some()) };
        Ok(())
    }
}

/// Read the element count of an `ARRAY` type.
pub(crate) fn array_size(logical_type: &LogicalType) -> Result<usize> {
    let (_, array_size) = logical_type.get_param(1)?;
    Ok(array_size
        .get::<i64>()?
        .ok_or(Error::api_error("Failed to get array_size from logical type"))? as usize)
}

// SAFETY: rows are read from the child, which `validate` checks as `T`.
unsafe impl<T: VectorElement> VectorElement for Array<T> {
    const TYPE_ID: LogicalTypeID = LogicalTypeID::Array;

    type Ref<'a>
        = ArrayRef<'a, T>
    where
        T: 'a;

    type Internal<'a> = ();

    fn validate(other: &LogicalType, children: Option<&VectorCollection>) -> Result<bool> {
        if other.id()? != Self::TYPE_ID {
            return Ok(false);
        }

        let Some(children) = children.filter(|children| children.col_count() == 1) else {
            return Err(Error::invalid_input("Array vector must have exactly one child"));
        };
        children.get_untyped_vector_at(0)?.validate_as::<T>().map(|()| true)
    }

    unsafe fn get<'a, U: VectorElement>(
        vector: &crate::vector::VectorReader<'a, U>,
        physical: usize,
        _logical: usize,
    ) -> Self::Ref<'a>
    where
        Self: Sized + 'a,
    {
        ArrayRef {
            offset: physical * vector.metadata().array_size(),
            size: vector.metadata().array_size(),
            // SAFETY: Array<T>::validate checked the prepared child as T.
            child: unsafe { vector.child_reader::<T>(0) },
        }
    }
}

// SAFETY: rows are written through the child's own `write`.
unsafe impl<T: WritableVectorElement> WritableVectorElement for Array<T> {
    type Write<'a> = Vec<Option<T::Write<'a>>>;

    unsafe fn write(
        vector: &mut crate::vector::VectorWriter<'_, Self>,
        index: usize,
        value: Option<Self::Write<'_>>,
    ) -> Result<()> {
        let array_size = vector.metadata().array_size();

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
            .len()
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
            // DuckDB requires a NULL array row's elements to be NULL too; this recurses.
            return vector.set_null_slow(index);
        };

        let offset = index
            .checked_mul(array_size)
            .ok_or_else(|| Error::invalid_input("Array child offset overflow"))?;
        let mut writer = child.writer()?;
        for (child_index, value) in values.into_iter().enumerate() {
            writer.set(offset + child_index, value)?;
        }
        Ok(())
    }
}

/// A borrowed array row backed by a contiguous range in its child vector.
#[derive(Debug)]
pub struct ArrayRef<'a, T: VectorElement> {
    offset: usize,
    size: usize,
    child: crate::vector::VectorReader<'a, T>,
}

impl<'a, T: VectorElement> ArrayRef<'a, T> {
    /// Iterate over the array's values.
    pub fn iter(&self) -> ArrayIterator<'a, T> {
        ArrayIterator {
            child: self.child.clone(),
            offset: self.offset,
            length: self.size,
            index: 0,
        }
    }

    /// Return the number of values in the array.
    pub fn len(&self) -> usize {
        self.size
    }

    /// Return whether the array has no values.
    pub fn is_empty(&self) -> bool {
        self.size == 0
    }
}

impl<'a, T: VectorElement + 'a> IntoIterator for &ArrayRef<'a, T> {
    type Item = Option<T::Ref<'a>>;
    type IntoIter = ArrayIterator<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// Traverses the child-vector range belonging to an [`ArrayRef`].
#[derive(Debug)]
pub struct ArrayIterator<'a, T: VectorElement> {
    child: crate::vector::VectorReader<'a, T>,
    offset: usize,
    length: usize,
    index: usize,
}

impl<'a, T: VectorElement + 'a> Iterator for ArrayIterator<'a, T> {
    type Item = Option<T::Ref<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.length {
            return None;
        }

        let logical = self.offset + self.index;
        self.index += 1;

        // SAFETY: ArrayRef retains the parent's cached descendant views.
        Some(unsafe { self.child.get_unchecked(logical) })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.length - self.index;
        (remaining, Some(remaining))
    }
}

impl<'a, T: VectorElement + 'a> ExactSizeIterator for ArrayIterator<'a, T> {}
impl<'a, T: VectorElement + 'a> std::iter::FusedIterator for ArrayIterator<'a, T> {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        connection::Connection,
        data_chunk::DataChunk,
        environment::{Environment, StorageLocation},
    };

    fn connection() -> Result<Connection> {
        Environment::new()?.open(StorageLocation::InMemory)?.connect()
    }

    #[test]
    fn array_size_reads_bigint_parameter() -> Result<()> {
        let conn = connection()?;
        let logical_type = <[i32; 3]>::logical_type(&conn)?;
        let (_, size) = logical_type.get_param(1)?;
        assert_eq!(size.logical_type()?.id()?, LogicalTypeID::BigInt);
        assert_eq!(array_size(&logical_type)?, 3);
        Ok(())
    }

    #[test]
    fn array_iterator_tracks_remaining_length() -> Result<()> {
        let conn = connection()?;
        let chunk = conn
            .query("SELECT [11, NULL, 12]::INTEGER[3]", Parameters::None)?
            .next_chunk()?
            .unwrap();
        let vector = chunk.get_vector_at::<Array<i32>>(0)?;
        let row = vector.reader()?.get(0)?.unwrap();
        assert_eq!(row.len(), 3);
        assert!(!row.is_empty());
        let mut iter = (&row).into_iter();
        fn assert_traits(_: &impl ExactSizeIterator, _: &impl std::iter::FusedIterator) {}
        assert_traits(&iter, &iter);
        assert_eq!(iter.size_hint(), (3, Some(3)));
        assert_eq!(iter.next(), Some(Some(&11)));
        assert_eq!(iter.len(), 2);
        assert_eq!(iter.next(), Some(None));
        assert_eq!(iter.next(), Some(Some(&12)));
        assert_eq!(iter.size_hint(), (0, Some(0)));
        assert_eq!(iter.next(), None);
        assert_eq!(iter.next(), None);
        assert_eq!((&row).into_iter().count(), 3);
        Ok(())
    }

    #[test]
    fn array_write_checks_parent_bounds() -> Result<()> {
        let conn = connection()?;
        let mut chunk = DataChunk::create(&conn, &[<[i32; 2]>::logical_type(&conn)?], true)?;
        let mut vector = chunk.get_vector_at_mut::<Array<i32>>(0)?;
        vector.set_size(1)?;
        vector.writer()?.set(0, Some(vec![Some(11), Some(12)]))?;
        let child_len = vector.children().unwrap().get_untyped_vector_at(0)?.len();

        let err = vector.writer()?.set(1, Some(vec![Some(21), Some(22)])).unwrap_err();
        assert!(err.to_string().contains("Vector index 1 is out of bounds for length 1"));
        assert_eq!(vector.children().unwrap().get_untyped_vector_at(0)?.len(), child_len);
        assert_eq!(
            vector.reader()?.get(0)?.unwrap().iter().collect::<Vec<_>>(),
            vec![Some(&11), Some(&12)]
        );
        Ok(())
    }
}

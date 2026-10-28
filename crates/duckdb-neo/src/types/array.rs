//! The fixed-length `ARRAY` logical type.
//!
//! [`Array<T, N>`] holds a row's `N` elements, each `None` for SQL NULL; vectors of
//! `Array<T, N>` read rows as [`ArrayRef`]. The element child always holds `N` elements
//! per row, so writes go through one cached child writer without resizing.

use super::{DuckDBType, FromValue, ToValue};
use crate::{
    Parameters, Result,
    connection::FFILink,
    data_chunk::VectorCollection,
    error::Error,
    logical_type::{LogicalType, LogicalTypeID},
    value::{Value, ValueInput},
    vector::{OwnedVectorElement, VectorElement, VectorWriter, WritableVectorElement},
};

/// An owned `ARRAY` row of exactly `N` elements; each `None` element is SQL NULL.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Array<T, const N: usize>(pub [Option<T>; N]);

impl<T, const N: usize> Default for Array<T, N> {
    fn default() -> Self {
        Self(std::array::from_fn(|_| None))
    }
}

impl<T, const N: usize> From<[Option<T>; N]> for Array<T, N> {
    fn from(values: [Option<T>; N]) -> Self {
        Self(values)
    }
}

impl<T: DuckDBType, const N: usize> DuckDBType for Array<T, N> {
    fn logical_type<C: FFILink + ?Sized>(link: &C) -> Result<LogicalType> {
        let child_type = Value::from_logical_type(link, &T::logical_type(link)?)?;
        let length = (N as u64).value(link)?;
        link.logical_type_create("ARRAY", Parameters::positional(&[&child_type, &length]))
    }
}

impl<T: ToValue + DuckDBType, const N: usize> ToValue for Array<T, N> {
    fn value<C: FFILink + ?Sized>(&self, link: &C) -> Result<Value> {
        let children = self
            .0
            .iter()
            .map(|value| value.value(link))
            .collect::<Result<Vec<_>>>()?;
        let child_type = T::logical_type(link)?;
        link.create_value(ValueInput::Array {
            child_type: &child_type,
            children: &children,
        })
    }
}

impl<T: FromValue, const N: usize> FromValue for Array<T, N> {
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
        Ok(Self(
            values
                .try_into()
                .unwrap_or_else(|_| unreachable!("checked exactly N children")),
        ))
    }
}

/// Read the element count of an `ARRAY` type.
pub(crate) fn array_size(logical_type: &LogicalType) -> Result<usize> {
    let (_, array_size) = logical_type.get_param(1)?;
    Ok(array_size
        .get::<i64>()?
        .ok_or(Error::api_error("Failed to get array_size from logical type"))? as usize)
}

// SAFETY: rows are read from the child, which `validate` checks as `T` with `N` elements per row.
unsafe impl<T: VectorElement, const N: usize> VectorElement for Array<T, N> {
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
        children.get_untyped_vector_at(0)?.validate_as::<T>()?;
        Ok(array_size(other)? == N)
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
            offset: physical * N,
            size: N,
            // SAFETY: Array<T, N>::validate checked the prepared child as T.
            child: unsafe { vector.child_reader::<T>(0) },
        }
    }
}

/// # Safety
/// `index` must be in bounds, and the child must be validated as `T`.
#[inline]
unsafe fn write_array<T: VectorElement, V, const N: usize>(
    vector: &mut VectorWriter<'_, Array<T, N>>,
    index: usize,
    values: Option<impl Iterator<Item = V>>,
    mut write: impl FnMut(&mut VectorWriter<'_, T>, usize, Option<V>) -> Result<()>,
) -> Result<()> {
    // SAFETY: the caller guarantees that the child was validated as T; preparing it sized it to N per row.
    let child = unsafe { vector.child_writer_unchecked::<T>(0)? };
    let offset = index * N;
    match values {
        Some(values) => {
            for (element, value) in (offset..offset + N).zip(values) {
                write(child, element, Some(value))?;
            }
        }
        // DuckDB requires a NULL array row's elements to be NULL too.
        None => {
            for element in offset..offset + N {
                write(child, element, None)?;
            }
        }
    }
    Ok(())
}

// SAFETY: each row writes its `N` in-bounds elements through the child's own writer.
unsafe impl<T: WritableVectorElement, const N: usize> WritableVectorElement for Array<T, N> {
    type Write<'a> = Array<T::Write<'a>, N>;

    #[inline]
    unsafe fn write(vector: &mut VectorWriter<'_, Self>, index: usize, value: Option<Self::Write<'_>>) -> Result<()> {
        // SAFETY: the caller guarantees an in-bounds row; validation checked the child as T.
        unsafe {
            write_array(
                vector,
                index,
                value.map(|value| value.0.into_iter()),
                |child, element, value| child.set_unchecked(element, value.flatten()),
            )
        }
    }
}

// SAFETY: each row writes its `N` in-bounds elements through the child's own writer.
unsafe impl<T: OwnedVectorElement, const N: usize> OwnedVectorElement for Array<T, N> {
    #[inline]
    unsafe fn write_ref(vector: &mut VectorWriter<'_, Self>, index: usize, value: Option<&Self>) -> Result<()> {
        // SAFETY: the caller guarantees an in-bounds row; validation checked the child as T.
        unsafe {
            write_array(
                vector,
                index,
                value.map(|value| value.0.iter()),
                |child, element, value| T::write_ref(child, element, value.and_then(Option::as_ref)),
            )?;
            vector.set_validity_unchecked(index, value.is_some());
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
        let logical_type = Array::<i32, 3>::logical_type(&conn)?;
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
        let vector = chunk.get_vector_at::<Array<i32, 3>>(0)?;
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
        let mut chunk = DataChunk::create(&conn, &[Array::<i32, 2>::logical_type(&conn)?], true)?;
        let mut vector = chunk.get_vector_at_mut::<Array<i32, 2>>(0)?;
        vector.set_size(1)?;
        vector.writer()?.set(0, Some(Array([Some(11), Some(12)])))?;
        let child_len = vector.children().unwrap().get_untyped_vector_at(0)?.len();

        let err = vector.writer()?.set(1, Some(Array([Some(21), Some(22)]))).unwrap_err();
        assert!(err.to_string().contains("Vector index 1 is out of bounds for length 1"));
        assert_eq!(vector.children().unwrap().get_untyped_vector_at(0)?.len(), child_len);
        assert_eq!(
            vector.reader()?.get(0)?.unwrap().iter().collect::<Vec<_>>(),
            vec![Some(&11), Some(&12)]
        );
        Ok(())
    }

    #[test]
    fn array_type_fixes_the_element_count() -> Result<()> {
        let conn = connection()?;
        let chunk = conn
            .query("SELECT [1, 2, 3]::INTEGER[3]", Parameters::None)?
            .next_chunk()?
            .unwrap();
        assert!(chunk.get_vector_at::<Array<i32, 2>>(0).is_err());
        assert!(chunk.get_vector_at::<Array<i64, 3>>(0).is_err());
        assert_eq!(Array::<i32, 2>::default(), Array([None, None]));
        assert_eq!(Array::from([Some(1), None]), Array([Some(1), None]));
        Ok(())
    }

    #[test]
    fn array_rows_rewrite_their_own_elements() -> Result<()> {
        let conn = connection()?;
        let mut chunk = DataChunk::create(&conn, &[Array::<String, 2>::logical_type(&conn)?], true)?;
        let mut vector = chunk.get_vector_at_mut::<Array<String, 2>>(0)?;
        vector.set_size(3)?;
        let owned = Array([Some("a string longer than the inline storage".to_owned()), None]);
        {
            let mut writer = vector.writer()?;
            writer.set(0, Some(Array([Some("a"), Some("b")])))?;
            writer.set_ref(1, Some(&owned))?;
            writer.set(2, Some(Array([Some("c"), Some("d")])))?;
            // Rewriting reuses the row's elements, and a NULL row nulls them.
            writer.set(0, Some(Array([None, Some("e")])))?;
            writer.set_ref(2, None)?;
        }
        assert_eq!(
            vector.get_value_slow(0)?.get::<Array<String, 2>>()?,
            Some(Array([None, Some("e".to_owned())]))
        );
        assert_eq!(vector.get_value_slow(1)?.get::<Array<String, 2>>()?, Some(owned));
        assert!(vector.reader()?.get(2)?.is_none());
        let elements = vector.children().unwrap().get_vector_at::<String>(0)?;
        assert_eq!(elements.len(), 6);
        assert_eq!(elements.reader()?.iter().skip(4).collect::<Vec<_>>(), [None, None]);
        Ok(())
    }

    #[test]
    fn arrays_nest_in_lists() -> Result<()> {
        use crate::types::List;

        let conn = connection()?;
        let mut chunk = DataChunk::create(&conn, &[List::<Array<i32, 2>>::logical_type(&conn)?], true)?;
        let mut vector = chunk.get_vector_at_mut::<List<Array<i32, 2>>>(0)?;
        vector.set_size(2)?;
        // SAFETY: the rows below write all three arrays, whose elements follow from the array size.
        unsafe { vector.set_child_size(3)? };
        {
            let mut writer = vector.writer()?;
            writer.set(0, Some(List(vec![Some(Array([Some(1), Some(2)])), None])))?;
            writer.set(1, Some(List(vec![Some(Array([None, Some(4)]))])))?;
        }
        assert_eq!(
            vector.get_value_slow(0)?.get::<List<Array<i32, 2>>>()?,
            Some(List(vec![Some(Array([Some(1), Some(2)])), None]))
        );
        assert_eq!(
            vector.get_value_slow(1)?.get::<List<Array<i32, 2>>>()?,
            Some(List(vec![Some(Array([None, Some(4)]))]))
        );
        Ok(())
    }
}

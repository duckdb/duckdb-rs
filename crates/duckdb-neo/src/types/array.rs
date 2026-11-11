//! The fixed-length `ARRAY` logical type.
//!
//! [`Array<T, N>`] holds a row's `N` elements, each `None` for SQL NULL. An `ARRAY` vector's own
//! rows only carry validity; every row owns `N` elements of the child. Read and write whole rows
//! with an [`ArrayReader`](crate::vector::ArrayReader) and [`ArrayWriter`](crate::vector::ArrayWriter).

use super::{DuckDBType, FromValue, ToValue};
use crate::{
    Parameters, Result,
    connection::FFILink,
    data_chunk::VectorCollection,
    error::Error,
    logical_type::{LogicalType, LogicalTypeID},
    value::{Value, ValueInput},
    vector::{NativeElement, ReadableNativeElement, VectorReader, VectorWriter, WritableNativeElement},
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

// SAFETY: there is no physical row data, and `validate` checks the child as `T` with `N` elements per row.
unsafe impl<T: NativeElement, const N: usize> NativeElement for Array<T, N> {
    const TYPE_ID: LogicalTypeID = LogicalTypeID::Array;

    type Native<'a> = ();

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
}

// SAFETY: `get` reads nothing.
unsafe impl<T: NativeElement, const N: usize> ReadableNativeElement for Array<T, N> {
    type Ref<'a>
        = ()
    where
        Self: 'a;

    #[inline]
    unsafe fn get<'a, U: NativeElement>(_reader: &VectorReader<'a, U>, _physical: usize, _logical: usize)
    where
        Self: 'a,
    {
    }
}

// SAFETY: the elements are always initialized, so a valid row needs no data of its own.
unsafe impl<T: NativeElement, const N: usize> WritableNativeElement for Array<T, N> {
    type Write = ();

    #[inline]
    unsafe fn write(_writer: &mut VectorWriter<'_, Self>, _index: usize, _value: &()) -> Result<()> {
        Ok(())
    }

    #[inline]
    unsafe fn write_null(writer: &mut VectorWriter<'_, Self>, index: usize) -> Result<()> {
        writer.null_child_rows(0, index * N, (index + 1) * N)
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
    fn array_size_reads_bigint_parameter() -> Result<()> {
        let conn = Environment::new()?.open(StorageLocation::InMemory)?.connect()?;
        let logical_type = Array::<i32, 3>::logical_type(&conn)?;
        let (_, size) = logical_type.get_param(1)?;
        assert_eq!(size.logical_type()?.id()?, LogicalTypeID::BigInt);
        assert_eq!(array_size(&logical_type)?, 3);
        Ok(())
    }

    #[test]
    fn array_null_rows_null_their_elements() -> Result<()> {
        let conn = Environment::new()?.open(StorageLocation::InMemory)?.connect()?;
        let mut chunk = DataChunk::create(&conn, &[Array::<i32, 2>::logical_type(&conn)?], true)?;
        let mut vector = chunk.get_vector_at_mut::<Array<i32, 2>>(0)?;
        vector.set_size(2)?;
        {
            let mut writer = vector.writer()?;
            writer.set(0, &())?;
            writer.set_null(1)?;
        }
        assert_eq!(
            vector.get_value_slow(0)?.get::<Array<i32, 2>>()?,
            Some(Array([Some(0), Some(0)]))
        );
        assert!(vector.reader()?.get(1)?.is_none());
        let elements = vector.children().unwrap().get_vector_at::<i32>(0)?.reader()?;
        assert_eq!(elements.iter().collect::<Vec<_>>(), [Some(&0), Some(&0), None, None]);
        Ok(())
    }
}

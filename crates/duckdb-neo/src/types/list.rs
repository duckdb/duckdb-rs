//! The `LIST` logical type.
//!
//! [`List<T>`] holds a row's elements, each `None` for SQL NULL. A `LIST` vector's own rows are
//! [`ListIndex`] entries into its element child; read and write whole rows with a
//! [`ListReader`](crate::vector::ListReader) and [`ListWriter`](crate::vector::ListWriter).

use super::{DuckDBType, FromValue, ToValue};
use crate::{
    Parameters, Result,
    connection::FFILink,
    data_chunk::VectorCollection,
    error::Error,
    logical_type::{LogicalType, LogicalTypeID},
    value::{Value, ValueInput},
    vector::{NativeElement, ReadableNativeElement, Vector, VectorReader, VectorWriter, WritableNativeElement},
};

/// DuckDB's physical list entry: a range of elements in the child vector.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct ListIndex {
    pub(crate) offset: u64,
    pub(crate) length: u64,
}

impl ListIndex {
    /// Create an entry covering `length` child elements from `offset`.
    pub const fn new(offset: u64, length: u64) -> Self {
        Self { offset, length }
    }

    /// Return the starting index in the child vector.
    pub fn offset(&self) -> u64 {
        self.offset
    }

    /// Return the number of child elements in this entry.
    pub fn length(&self) -> u64 {
        self.length
    }

    /// Return one past the last child element, or `None` on overflow.
    pub fn end(&self) -> Option<u64> {
        self.offset.checked_add(self.length)
    }
}

/// An owned `LIST` row; each `None` element is SQL NULL.
///
/// A `LIST` vector reads and writes its rows natively as [`ListIndex`] entries, checked against the
/// size of the element child, which [`Vector::set_child_size`] sets. A [`ListWriter`](crate::vector::ListWriter)
/// appends whole rows instead, growing the child as it goes:
///
/// ```
/// use duckdb_neo::{DuckDBType, Result, data_chunk::DataChunk, environment::{Environment, StorageLocation}, types::List, vector::RowWriter};
///
/// # fn main() -> Result<()> {
/// let conn = Environment::new()?.open(StorageLocation::InMemory)?.connect()?;
/// let mut chunk = DataChunk::create(&conn, &[List::<i32>::logical_type(&conn)?], true)?;
/// let mut vector = chunk.get_vector_at_mut::<List<i32>>(0)?;
/// vector.set_size(2)?;
/// let mut writer = vector.list_writer()?;
/// writer.set(0, &List(vec![Some(1), Some(2)]))?;
/// writer.set(1, &List(vec![Some(3)]))?;
/// writer.finish()?;
/// assert_eq!(vector.children().unwrap().get_untyped_vector_at(0)?.len(), 3);
/// assert_eq!(vector.get_value_slow(1)?.get::<List<i32>>()?, Some(List(vec![Some(3)])));
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct List<T>(pub Vec<Option<T>>);

impl<T> From<Vec<Option<T>>> for List<T> {
    fn from(values: Vec<Option<T>>) -> Self {
        Self(values)
    }
}

impl<T> FromIterator<Option<T>> for List<T> {
    fn from_iter<I: IntoIterator<Item = Option<T>>>(values: I) -> Self {
        Self(values.into_iter().collect())
    }
}

impl<T: DuckDBType> DuckDBType for List<T> {
    fn logical_type<C: FFILink + ?Sized>(link: &C) -> Result<LogicalType> {
        let child_type = Value::from_logical_type(link, &T::logical_type(link)?)?;
        link.logical_type_create("LIST", Parameters::positional(&[&child_type]))
    }
}

impl<T: ToValue + DuckDBType> ToValue for List<T> {
    fn value<C: FFILink + ?Sized>(&self, link: &C) -> Result<Value> {
        let children = self
            .0
            .iter()
            .map(|value| value.value(link))
            .collect::<Result<Vec<_>>>()?;
        let child_type = T::logical_type(link)?;
        link.create_value(ValueInput::List {
            child_type: &child_type,
            children: &children,
        })
    }
}

impl<T: FromValue> FromValue for List<T> {
    fn _get_inner(value: &Value) -> Result<Self> {
        let logical_type = value.logical_type()?;
        if logical_type.id()? != LogicalTypeID::List {
            return Err(Error::invalid_input(format!(
                "Expected LIST value, found {}",
                logical_type.to_text()?
            )));
        }

        value
            .children()?
            .iter()
            .map(T::from_value)
            .collect::<Result<_>>()
            .map(List)
    }
}

// SAFETY: `ListIndex` matches DuckDB's list entry, and `validate` checks the child as `T`.
unsafe impl<T: NativeElement> NativeElement for List<T> {
    const TYPE_ID: LogicalTypeID = LogicalTypeID::List;

    type Native<'a> = ListIndex;

    fn validate(other: &LogicalType, children: Option<&VectorCollection>) -> Result<bool> {
        if other.id()? != Self::TYPE_ID {
            return Ok(false);
        }

        let Some(children) = children.filter(|children| children.col_count() == 1) else {
            return Err(Error::invalid_input("List vector must have exactly one child"));
        };
        children.get_untyped_vector_at(0)?.validate_as::<T>().map(|()| true)
    }
}

// SAFETY: LIST rows store list entries.
unsafe impl<T: NativeElement> ReadableNativeElement for List<T> {
    type Ref<'a>
        = ListIndex
    where
        T: 'a;

    #[inline(always)]
    unsafe fn get<'a, U: NativeElement>(reader: &VectorReader<'a, U>, physical: usize, _logical: usize) -> ListIndex
    where
        Self: 'a,
    {
        // SAFETY: the caller guarantees a valid row of LIST storage.
        unsafe { *reader.as_ptr().cast::<ListIndex>().add(physical) }
    }
}

// SAFETY: entries are checked against the element child's size, whose rows are always initialized.
unsafe impl<T: NativeElement> WritableNativeElement for List<T> {
    type Write = ListIndex;

    #[inline]
    unsafe fn write(writer: &mut VectorWriter<'_, Self>, index: usize, value: &ListIndex) -> Result<()> {
        check_entry(value, writer.vector().list_child_len()?, "LIST elements")?;
        // SAFETY: the caller guarantees an in-bounds row, and the entry covers initialized elements.
        unsafe { writer.write_internal(index, Some(*value)) }
    }

    #[inline]
    unsafe fn write_null(writer: &mut VectorWriter<'_, Self>, index: usize) -> Result<()> {
        // SAFETY: the caller guarantees an in-bounds row, and an empty entry references no elements.
        unsafe { writer.write_internal(index, Some(ListIndex::default())) }
    }
}

/// Reject an entry past the `len` rows of the child.
#[inline]
pub(crate) fn check_entry(entry: &ListIndex, len: usize, what: &str) -> Result<()> {
    match entry.end() {
        Some(end) if end <= len as u64 => Ok(()),
        _ => Err(Error::invalid_input(format!(
            "entry {}..+{} is past the {len} {what}",
            entry.offset, entry.length
        ))),
    }
}

impl<T: NativeElement> Vector<'_, List<T>> {
    /// Resize the element child to `elements`; new elements are initialized like [`Vector::set_size`].
    ///
    /// Shrinking fails if a valid row's entry would reach past the new size.
    pub fn set_child_size(&mut self, elements: usize) -> Result<()> {
        self.resize_list_child(elements)
    }
}

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
    fn list_entry_accessors() {
        let entry = ListIndex::new(7, 3);
        assert_eq!(entry.offset(), 7);
        assert_eq!(entry.length(), 3);
        assert_eq!(entry.end(), Some(10));
        assert_eq!(ListIndex::new(u64::MAX, 1).end(), None);
    }

    #[test]
    fn list_rows_read_their_entries() -> Result<()> {
        let conn = connection()?;
        let chunk = conn
            .query(
                "SELECT * FROM (VALUES (['a', NULL]), (NULL), ([]::VARCHAR[]))",
                Parameters::None,
            )?
            .next_chunk()?
            .unwrap();
        let reader = chunk.get_vector_at::<List<String>>(0)?.reader()?;
        assert_eq!(reader.get(0)?, Some(ListIndex::new(0, 2)));
        assert_eq!(reader.get(1)?, None);
        assert_eq!(reader.get(2)?.map(|entry| entry.length()), Some(0));
        Ok(())
    }

    #[test]
    fn list_entries_are_checked_against_the_child() -> Result<()> {
        let conn = connection()?;
        let mut chunk = DataChunk::create(&conn, &[List::<i32>::logical_type(&conn)?], true)?;
        let mut vector = chunk.get_vector_at_mut::<List<i32>>(0)?;
        vector.set_size(3)?;
        vector.set_child_size(3)?;
        {
            let mut writer = vector.writer()?;
            writer.set(0, &ListIndex::new(0, 2))?;
            writer.set(1, &ListIndex::new(2, 1))?;
            writer.set_null(2)?;
            for entry in [ListIndex::new(2, 2), ListIndex::new(4, 0), ListIndex::new(u64::MAX, 2)] {
                let error = writer.set(2, &entry).unwrap_err();
                assert!(error.message.contains("past the 3 LIST elements"), "{error}");
            }
        }
        assert_eq!(
            vector.get_value_slow(0)?.get::<List<i32>>()?,
            Some(List(vec![Some(0), Some(0)]))
        );
        assert_eq!(vector.get_value_slow(2)?.get::<List<i32>>()?, None);
        assert!(vector.reader()?.get(2)?.is_none());
        Ok(())
    }

    #[test]
    fn list_child_shrinks_only_below_unused_elements() -> Result<()> {
        let conn = connection()?;
        let mut chunk = DataChunk::create(&conn, &[List::<i32>::logical_type(&conn)?], true)?;
        let mut vector = chunk.get_vector_at_mut::<List<i32>>(0)?;
        vector.set_size(2)?;
        vector.set_child_size(10)?;
        vector.writer()?.set(0, &ListIndex::new(1, 3))?;
        vector.writer()?.set_null(1)?;
        let error = vector.set_child_size(3).unwrap_err();
        assert!(error.message.contains("past the new 3"), "{error}");
        vector.set_child_size(4)?;
        assert_eq!(vector.children().unwrap().get_untyped_vector_at(0)?.len(), 4);
        // A NULL row's entry is never read.
        vector.writer()?.set_null(0)?;
        vector.set_child_size(0)?;
        Ok(())
    }
}

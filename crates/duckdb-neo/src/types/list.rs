//! The `LIST` logical type.
//!
//! [`List<T>`] holds a row's elements, each `None` for SQL NULL; vectors of `List<T>`
//! read rows as [`ListRef`]. Writes never resize the element child; [`List`] describes
//! how to size it.

use super::{DuckDBType, FromValue, ToValue};
use crate::{
    Parameters, Result,
    connection::FFILink,
    data_chunk::VectorCollection,
    error::Error,
    logical_type::{LogicalType, LogicalTypeID},
    value::{Value, ValueInput},
    vector::{OwnedVectorElement, Vector, VectorElement, VectorWriter, WritableVectorElement},
};

/// DuckDB's physical list entry: a range of elements in the child vector.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListIndex {
    pub(crate) offset: u64,
    pub(crate) length: u64,
}

impl ListIndex {
    /// Return the starting index in the child vector.
    pub fn offset(&self) -> u64 {
        self.offset
    }

    /// Return the number of child elements in this entry.
    pub fn length(&self) -> u64 {
        self.length
    }
}

/// An owned `LIST` row; each `None` element is SQL NULL.
///
/// # Writing `LIST` vectors
///
/// Writes never resize the element child. Size it with [`Vector::set_child_size`]; each row
/// write then takes the next unwritten elements, in any row order, and fails without taking
/// any when too few remain. Nested `LIST`s and `MAP`s keep their own offsets, so size each level.
///
/// DuckDB reads the whole child, so every element below its size must be written before the
/// vector is read. When the total is unknown, size for an upper bound, write the rows, then trim
/// to [`Vector::child_offset`] with another `set_child_size` call, which keeps written elements.
/// A failed row write takes no elements, but skipping ahead with [`Vector::set_child_offset`]
/// leaves a gap that must be written or trimmed away.
///
/// ```
/// use duckdb_neo::{DuckDBType, Result, data_chunk::DataChunk, environment::{Environment, StorageLocation}, types::List};
///
/// # fn main() -> Result<()> {
/// let conn = Environment::new()?.open(StorageLocation::InMemory)?.connect()?;
/// let mut chunk = DataChunk::create(&conn, &[List::<i32>::logical_type(&conn)?], true)?;
/// let mut vector = chunk.get_vector_at_mut::<List<i32>>(0)?;
/// vector.set_size(2)?;
/// // SAFETY: the vector is trimmed to the written elements before it is read.
/// unsafe { vector.set_child_size(16)? };
/// {
///     let mut writer = vector.writer()?;
///     writer.set(0, Some(List(vec![Some(1), Some(2)])))?;
///     writer.set(1, Some(List(vec![Some(3)])))?;
/// }
/// let written = vector.child_offset();
/// // SAFETY: every element below `written` was written above.
/// unsafe { vector.set_child_size(written)? };
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
unsafe impl<T: VectorElement> VectorElement for List<T> {
    type Ref<'a>
        = ListRef<'a, T>
    where
        T: 'a;

    type Internal<'a> = ListIndex;

    const TYPE_ID: LogicalTypeID = LogicalTypeID::List;

    fn validate(other: &LogicalType, children: Option<&VectorCollection>) -> Result<bool> {
        if other.id()? != Self::TYPE_ID {
            return Ok(false);
        }

        let Some(children) = children.filter(|children| children.col_count() == 1) else {
            return Err(Error::invalid_input("List vector must have exactly one child"));
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
        let data_ptr = vector.as_ptr() as *const ListIndex;
        let list = unsafe { &*data_ptr.add(physical) };
        ListRef {
            list,
            // SAFETY: List<T>::validate checked the child as T.
            child: unsafe { vector.child_reader::<T>(0) },
        }
    }
}

impl<T: VectorElement> Vector<'_, List<T>> {
    /// Size the element child to exactly `elements` and restart writes at its first element.
    ///
    /// # Safety
    /// Every element below `elements` must be written before DuckDB reads this vector; see
    /// [`List`] for sizing to an upper bound and trimming.
    pub unsafe fn set_child_size(&mut self, elements: usize) -> Result<()> {
        // SAFETY: the caller upholds the initialization contract.
        unsafe { self.size_appended_child(elements) }
    }

    /// Return the first element the next list write uses.
    pub fn child_offset(&self) -> usize {
        self.child_offset_cell().get()
    }

    /// Continue list writes at `offset`, e.g. with a new wrapper for a partially written vector.
    ///
    /// Moving backwards makes later writes replace elements of rows that were already written.
    pub fn set_child_offset(&mut self, offset: usize) -> Result<()> {
        self.move_appended_child_offset(offset)
    }
}

impl<T: VectorElement> VectorWriter<'_, List<T>> {
    /// Return the first element the next list write uses.
    pub fn child_offset(&self) -> usize {
        self.child_offset_cell().get()
    }
}

/// # Safety
/// `index` must be in bounds, and the child must be validated as `T`.
#[inline]
unsafe fn write_list<T: VectorElement, V>(
    vector: &mut VectorWriter<'_, List<T>>,
    index: usize,
    values: Option<impl ExactSizeIterator<Item = V>>,
    mut write: impl FnMut(&mut VectorWriter<'_, T>, usize, V) -> Result<()>,
) -> Result<()> {
    let Some(values) = values else { return Ok(()) };
    let len = values.len();
    let offset = vector.child_offset_cell().get();
    // SAFETY: the caller guarantees that the child was validated as T.
    let child = unsafe { vector.child_writer_unchecked::<T>(0)? };
    let size = child.len();
    let end = offset.checked_add(len).filter(|end| *end <= size).ok_or_else(|| {
        Error::invalid_input(format!(
            "LIST row needs {len} elements, but only {} of {size} remain; size the child with set_child_size",
            size - offset.min(size)
        ))
    })?;
    for (element, value) in (offset..end).zip(values) {
        write(child, element, value)?;
    }
    vector.child_offset_cell().set(end);
    // SAFETY: the caller guarantees an in-bounds row, and the entry covers the elements just written.
    unsafe {
        vector.write_internal(
            index,
            Some(ListIndex {
                offset: offset as u64,
                length: len as u64,
            }),
        )
    }
}

// SAFETY: entries only cover elements written through the child's own writer.
unsafe impl<T: WritableVectorElement> WritableVectorElement for List<T> {
    type Write<'a> = List<T::Write<'a>>;

    #[inline]
    unsafe fn write(vector: &mut VectorWriter<'_, Self>, index: usize, value: Option<Self::Write<'_>>) -> Result<()> {
        // SAFETY: the caller guarantees an in-bounds row; validation checked the child as T.
        unsafe {
            write_list(
                vector,
                index,
                value.map(|value| value.0.into_iter()),
                |child, element, value| child.set_unchecked(element, value),
            )
        }
    }
}

// SAFETY: entries only cover elements written through the child's own writer.
unsafe impl<T: OwnedVectorElement> OwnedVectorElement for List<T> {
    #[inline]
    unsafe fn write_ref(vector: &mut VectorWriter<'_, Self>, index: usize, value: Option<&Self>) -> Result<()> {
        // SAFETY: the caller guarantees an in-bounds row; validation checked the child as T.
        unsafe {
            write_list(
                vector,
                index,
                value.map(|value| value.0.iter()),
                |child, element, value| T::write_ref(child, element, value.as_ref()),
            )?;
            vector.set_validity_unchecked(index, value.is_some());
        }
        Ok(())
    }
}

/// A borrowed list row backed by a range in its child vector.
#[derive(Debug)]
pub struct ListRef<'a, T: VectorElement> {
    list: &'a ListIndex,
    child: crate::vector::VectorReader<'a, T>,
}

impl<'a, T: VectorElement> ListRef<'a, T> {
    /// Return an element; outer `None` means the index is out of bounds.
    pub fn get(&self, index: usize) -> Option<Option<T::Ref<'a>>> {
        // SAFETY: The parent reader cached all descendant views before constructing this row.
        (index < self.len()).then(|| unsafe { self.child.get_unchecked(self.list.offset as usize + index) })
    }
    /// Iterate over the list's values.
    pub fn iter(&self) -> ListIterator<'a, T> {
        ListIterator {
            child: self.child.clone(),
            offset: self.list.offset as usize,
            length: self.list.length as usize,
            index: 0,
        }
    }

    /// Return the number of values in the list.
    pub fn len(&self) -> usize {
        self.list.length as usize
    }

    /// Return whether the list contains no values.
    pub fn is_empty(&self) -> bool {
        self.list.length == 0
    }
}

impl<'a, T: VectorElement + 'a> IntoIterator for ListRef<'a, T> {
    type Item = Option<T::Ref<'a>>;
    type IntoIter = ListIterator<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a, T: VectorElement + 'a> IntoIterator for &ListRef<'a, T> {
    type Item = Option<T::Ref<'a>>;
    type IntoIter = ListIterator<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// Traverses the child-vector range belonging to a [`ListRef`].
#[derive(Debug)]
pub struct ListIterator<'a, T: VectorElement> {
    child: crate::vector::VectorReader<'a, T>,
    offset: usize,
    length: usize,
    index: usize,
}

impl<'a, T: VectorElement + 'a> Iterator for ListIterator<'a, T> {
    type Item = Option<T::Ref<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.length {
            return None;
        }

        let logical = self.offset + self.index;
        self.index += 1;

        // SAFETY: ListRef retains the parent's cached descendant views.
        Some(unsafe { self.child.get_unchecked(logical) })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.length - self.index;
        (remaining, Some(remaining))
    }
}

impl<'a, T: VectorElement + 'a> ExactSizeIterator for ListIterator<'a, T> {}
impl<'a, T: VectorElement + 'a> std::iter::FusedIterator for ListIterator<'a, T> {}

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

    fn entries(vector: &Vector<'_, List<i32>>) -> Result<Vec<Option<Vec<Option<i32>>>>> {
        Ok(vector
            .reader()?
            .iter()
            .map(|row| row.map(|row| row.iter().map(|value| value.copied()).collect()))
            .collect())
    }

    #[test]
    fn list_entry_accessors() {
        let entry = ListIndex { offset: 7, length: 3 };
        assert_eq!(entry.offset(), 7);
        assert_eq!(entry.length(), 3);
    }

    #[test]
    fn list_ref_get_and_iteration() -> Result<()> {
        let conn = Environment::new()?.open(StorageLocation::InMemory)?.connect()?;
        let chunk = conn
            .query("SELECT ['a', NULL, 'b'], []::VARCHAR[]", Parameters::None)?
            .next_chunk()?
            .unwrap();
        let vector = chunk.get_vector_at::<List<String>>(0)?;
        let row = vector.reader()?.get(0)?.unwrap();
        assert_eq!(row.get(0), Some(Some("a")));
        assert_eq!(row.get(1), Some(None));
        assert_eq!(row.get(2), Some(Some("b")));
        assert_eq!(row.get(3), None);
        assert_eq!(row.get(usize::MAX), None);
        assert_eq!((&row).into_iter().collect::<Vec<_>>(), vec![Some("a"), None, Some("b")]);
        let mut iter = row.into_iter();
        fn assert_traits(_: &impl ExactSizeIterator, _: &impl std::iter::FusedIterator) {}
        assert_traits(&iter, &iter);
        assert_eq!(iter.size_hint(), (3, Some(3)));
        assert_eq!(iter.next(), Some(Some("a")));
        assert_eq!(iter.len(), 2);
        assert_eq!(iter.next(), Some(None));
        assert_eq!(iter.next(), Some(Some("b")));
        assert_eq!(iter.size_hint(), (0, Some(0)));
        assert_eq!(iter.next(), None);
        assert_eq!(iter.next(), None);

        let empty_vector = chunk.get_vector_at::<List<String>>(1)?;
        let empty = empty_vector.reader()?.get(0)?.unwrap();
        assert!(empty.is_empty());
        assert_eq!(empty.get(0), None);
        assert_eq!(empty.iter().len(), 0);
        assert_eq!(empty.iter().next(), None);
        Ok(())
    }

    #[test]
    fn list_rows_take_elements_in_write_order() -> Result<()> {
        let conn = connection()?;
        let mut chunk = DataChunk::create(&conn, &[List::<i32>::logical_type(&conn)?], true)?;
        let mut vector = chunk.get_vector_at_mut::<List<i32>>(0)?;
        vector.set_size(3)?;
        // SAFETY: the rows below write all three elements.
        unsafe { vector.set_child_size(3)? };
        {
            let mut writer = vector.writer()?;
            writer.set(2, Some(List(vec![Some(1), Some(2)])))?;
            writer.set(1, None)?;
            writer.set(0, Some(List(vec![None])))?;
        }
        assert_eq!(
            entries(&vector)?,
            [Some(vec![None]), None, Some(vec![Some(1), Some(2)])]
        );
        let reader = vector.reader()?;
        assert_eq!(reader.get(2)?.unwrap().list.offset(), 0);
        assert_eq!(reader.get(0)?.unwrap().list.offset(), 2);
        Ok(())
    }

    #[test]
    fn list_rows_that_do_not_fit_write_nothing() -> Result<()> {
        let conn = connection()?;
        let mut chunk = DataChunk::create(&conn, &[List::<i32>::logical_type(&conn)?], true)?;
        let mut vector = chunk.get_vector_at_mut::<List<i32>>(0)?;
        vector.set_size(2)?;
        let error = vector.writer()?.set(0, Some(List(vec![Some(1)]))).unwrap_err();
        assert!(error.message.contains("only 0 of 0 remain"), "{error}");
        // SAFETY: the rows below write all three elements.
        unsafe { vector.set_child_size(3)? };
        {
            let mut writer = vector.writer()?;
            writer.set(0, Some(List(vec![Some(1), Some(2)])))?;
            let error = writer.set(1, Some(List(vec![Some(3), Some(4)]))).unwrap_err();
            assert!(
                error.message.contains("needs 2 elements, but only 1 of 3 remain"),
                "{error}"
            );
            assert_eq!(writer.child_offset(), 2);
            writer.set(1, Some(List(vec![Some(3)])))?;
            assert!(writer.set(0, Some(List(vec![Some(5)]))).is_err());
            // Empty lists need no elements.
            writer.set(0, Some(List(vec![])))?;
        }
        assert_eq!(entries(&vector)?, [Some(vec![]), Some(vec![Some(3)])]);
        Ok(())
    }

    #[test]
    fn list_writes_borrow_owned_elements() -> Result<()> {
        let conn = connection()?;
        let mut chunk = DataChunk::create(&conn, &[List::<String>::logical_type(&conn)?], true)?;
        let mut vector = chunk.get_vector_at_mut::<List<String>>(0)?;
        vector.set_size(2)?;
        let rows = [
            Some(List(vec![
                Some("a string longer than the inline storage".to_owned()),
                None,
            ])),
            Some(List(vec![Some("b".to_owned())])),
        ];
        // SAFETY: the rows below write all three elements.
        unsafe { vector.set_child_size(3)? };
        {
            let mut writer = vector.writer()?;
            for (index, row) in rows.iter().enumerate() {
                writer.set_ref(index, row.as_ref())?;
            }
        }
        for (index, row) in rows.iter().enumerate() {
            assert_eq!(
                vector.get_value_slow(index)?.get::<List<String>>()?.as_ref(),
                row.as_ref()
            );
        }
        Ok(())
    }

    #[test]
    fn list_child_offset_moves_within_the_child() -> Result<()> {
        let conn = connection()?;
        let mut chunk = DataChunk::create(&conn, &[List::<i32>::logical_type(&conn)?], true)?;
        let mut vector = chunk.get_vector_at_mut::<List<i32>>(0)?;
        vector.set_size(2)?;
        // SAFETY: row 0 writes both elements; rewinding then rewrites them for row 1.
        unsafe { vector.set_child_size(2)? };
        vector.writer()?.set(0, Some(List(vec![Some(1), Some(2)])))?;
        assert!(vector.set_child_offset(3).is_err());
        vector.set_child_offset(0)?;
        vector.writer()?.set(1, Some(List(vec![Some(7), Some(8)])))?;
        // Both rows now share the rewritten range.
        assert_eq!(
            entries(&vector)?,
            [Some(vec![Some(7), Some(8)]), Some(vec![Some(7), Some(8)])]
        );
        drop(vector);
        chunk.clear()?;
        assert_eq!(chunk.get_vector_at_mut::<List<i32>>(0)?.child_offset(), 0);
        Ok(())
    }
}

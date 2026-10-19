//! [`List`] is a borrowed-vector `LIST` element; [`Vec<T>`] is its owned scalar form.
//! `Vec<u8>` maps to `UTINYINT[]`; use [`super::BlobValue`] for `BLOB`.

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

/// A physical list entry parameterized by its child element type.
#[repr(C)]
#[derive(Debug)]
pub struct List<L> {
    pub(crate) offset: u64,
    pub(crate) length: u64,
    pub(crate) _marker: PhantomData<L>,
}

impl<L> List<L> {
    /// Return the starting index in the child vector.
    pub fn offset(&self) -> u64 {
        self.offset
    }

    /// Return the number of child elements in this entry.
    pub fn length(&self) -> u64 {
        self.length
    }
}

impl<T: DuckDBType> DuckDBType for Vec<T> {
    fn logical_type<C: FFILink + ?Sized>(link: &C) -> Result<LogicalType> {
        let child_type = Value::from_logical_type(link, &T::logical_type(link)?)?;
        link.logical_type_create("LIST", Parameters::positional(&[&child_type]))
    }
}

impl<T: ToValue + DuckDBType> ToValue for Vec<T> {
    fn value<C: FFILink + ?Sized>(&self, link: &C) -> Result<Value> {
        let children = self.iter().map(|value| value.value(link)).collect::<Result<Vec<_>>>()?;
        let child_type = T::logical_type(link)?;
        link.create_value(ValueInput::List {
            child_type: &child_type,
            children: &children,
        })
    }
}

impl<T: FromValue> FromValue for Vec<Option<T>> {
    fn _get_inner(value: &Value) -> Result<Self> {
        let logical_type = value.logical_type()?;
        if logical_type.id()? != LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_LIST {
            return Err(Error::invalid_input(format!(
                "Expected LIST value, found {}",
                logical_type.to_text()?
            )));
        }

        value.children()?.iter().map(T::from_value).collect::<Result<Vec<_>>>()
    }
}

// SAFETY: `List` matches DuckDB's list entry, and `validate` checks the child as `L`.
unsafe impl<L: VectorElement> VectorElement for List<L> {
    type Ref<'a>
        = ListRef<'a, L>
    where
        L: 'a;

    type Internal = List<L>;

    const TYPE_ID: LogicalTypeID = LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_LIST;

    fn validate(other: &LogicalType, children: Option<&VectorCollection>) -> Result<bool> {
        if other.id()? != Self::TYPE_ID {
            return Ok(false);
        }

        let Some(children) = children.filter(|children| children.col_count() == 1) else {
            return Err(Error::invalid_input("List vector must have exactly one child"));
        };
        children.get_untyped_vector_at(0)?.validate_as::<L>().map(|()| true)
    }

    unsafe fn get<'a, U: VectorElement>(vector: &'a Vector<'_, U>, physical: usize, _logical: usize) -> Self::Ref<'a>
    where
        Self: Sized + 'a,
    {
        let data_ptr = vector.view.as_ref().unwrap().as_ptr() as *const List<L>;
        let list = unsafe { &*data_ptr.add(physical) };
        ListRef {
            list,
            child: vector
                .children()
                .expect("validated nested vector has children")
                .cached_unchecked(0),
        }
    }
}

// SAFETY: the entry covers child rows appended through the child's own `write`.
unsafe impl<T: WritableVectorElement> WritableVectorElement for List<T> {
    type Write<'a>
        = Vec<Option<T::Write<'a>>>
    where
        T: 'a;

    unsafe fn write(vector: &mut Vector<'_, Self>, index: usize, value: Option<Self::Write<'_>>) -> Result<()> {
        let Some(values) = value else {
            return vector.write_raw::<List<T>>(index, None);
        };

        // Reject the row before appending, or a failed write would leave orphaned child elements.
        vector.check_row_writable(index)?;
        let len = values.len();
        let child = vector
            .children_collection_mut()
            .expect("validated nested vector has children")
            .cached_mut::<T>(0)?;

        let result = (|| {
            // Append after the child's elements, including any written through another wrapper.
            let offset = child.current_size()?;
            child.set_size(offset + len)?;
            for (child_index, value) in values.into_iter().enumerate() {
                child.write(offset + child_index, value)?;
            }
            Ok(offset)
        })();
        let offset = result?;
        vector.write_raw::<List<T>>(
            index,
            Some(List {
                offset: offset as u64,
                length: len as u64,
                _marker: PhantomData,
            }),
        )
    }
}

/// A borrowed list row backed by a range in its child vector.
#[derive(Debug)]
pub struct ListRef<'a, T> {
    list: &'a List<T>,
    child: &'a Vector<'a, Unknown>,
}

impl<'a, T: VectorElement> ListRef<'a, T> {
    /// Return an element; outer `None` means the index is out of bounds.
    pub fn get(&self, index: usize) -> Option<Option<T::Ref<'a>>> {
        (index < self.len()).then(|| self.child.get_as_unchecked::<T>(self.list.offset as usize + index))
    }
    /// Iterate over the list's values.
    pub fn iter(&self) -> ListIterator<'a, T> {
        ListIterator {
            child: self.child,
            offset: self.list.offset as usize,
            length: self.list.length as usize,
            index: 0,
            _type: PhantomData,
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
pub struct ListIterator<'a, T> {
    child: &'a Vector<'a, Unknown>,
    offset: usize,
    length: usize,
    index: usize,
    _type: PhantomData<T>,
}

impl<'a, T: VectorElement + 'a> Iterator for ListIterator<'a, T> {
    type Item = Option<T::Ref<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.length {
            return None;
        }

        let logical = self.offset + self.index;
        self.index += 1;

        Some(self.child.get_as_unchecked::<T>(logical))
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
    use crate::environment::{Environment, StorageLocation};

    #[test]
    fn list_entry_accessors() {
        let entry = List::<i32> {
            offset: 7,
            length: 3,
            _marker: PhantomData,
        };
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
        let row = vector.get(0)?.unwrap();
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
        let empty = empty_vector.get(0)?.unwrap();
        assert!(empty.is_empty());
        assert_eq!(empty.get(0), None);
        assert_eq!(empty.iter().len(), 0);
        assert_eq!(empty.iter().next(), None);
        Ok(())
    }
}

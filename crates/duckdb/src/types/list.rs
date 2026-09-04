//! The `LIST` logical type.
//!
//! [`List`] is the vector-element representation of one list row (an offset
//! and length into a child vector); [`Vec<T>`] is the owned scalar
//! representation used with [`super::ToValue`]/[`super::FromValue`].

use std::marker::PhantomData;

use super::{DuckDBType, ToValue};
use crate::{
    Parameters, Result,
    connection::FFILink,
    error::{DuckDBError, Error},
    logical_type::{LogicalType, LogicalTypeID},
    value::{Value, ValueInput},
    vector::{Unknown, Vector, VectorElement, VectorView, WritableVectorElement},
};

/// A physical list entry parameterized by its child element type.
#[repr(C)]
pub struct List<L> {
    pub(crate) offset: u64,
    pub(crate) length: u64,
    pub(crate) _marker: PhantomData<L>,
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

impl<L: VectorElement> VectorElement for List<L> {
    type Ref<'a>
        = ListRef<'a, L>
    where
        L: 'a;

    type Internal = List<L>;

    const TYPE_ID: LogicalTypeID = LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_LIST;

    fn validate(other: &LogicalType, children: &[Vector<'_, Unknown>]) -> Result<bool> {
        if other.type_id() != Self::TYPE_ID {
            return Ok(false);
        }

        if children.len() != 1 {
            return Err(Error {
                code: DuckDBError::DUCKDB_V2_ERROR_INPUT_INVALID,
                message: "List vector must have exactly one child".to_string(),
            });
        }

        let child = children.first().unwrap();
        child.validate_as::<L>()
    }

    fn get<'a, U: VectorElement>(vector: &'a Vector<'_, U>, physical: usize, _logical: usize) -> Self::Ref<'a>
    where
        Self: Sized + 'a,
    {
        let data_ptr = vector.view.as_ref().unwrap().as_ptr() as *const List<L>;
        let list = unsafe { &*data_ptr.add(physical) };
        ListRef {
            list,
            child: &vector.children[0],
        }
    }
}

impl<T: WritableVectorElement> WritableVectorElement for List<T> {
    type Write<'a>
        = Vec<Option<T::Write<'a>>>
    where
        T: 'a;

    fn write(vector: &mut Vector<'_, Self>, index: usize, value: Option<Self::Write<'_>>) -> Result<()> {
        let Some(values) = value else {
            return vector.write_raw::<List<T>>(index, None);
        };

        let offset = vector.child_write_offset;
        let len = values.len();
        let mut child = std::mem::take(&mut vector.children)
            .into_iter()
            .next()
            .expect("validated list child")
            .cast_unchecked::<T>();
        let result = (|| {
            child.set_size(offset + len)?;
            for (child_index, value) in values.into_iter().enumerate() {
                child.write(offset + child_index, value)?;
            }
            Ok(())
        })();
        vector.children = vec![child.into_unknown()];
        result?;
        vector.child_write_offset += len;
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
pub struct ListRef<'a, T> {
    list: &'a List<T>,
    child: &'a Vector<'a, Unknown>,
}

impl<'a, T: VectorElement> ListRef<'a, T> {
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

    // TODO: Maybe remove?
    /// Return a unified view of the list's child vector.
    pub fn view(&self) -> &VectorView<T> {
        unsafe {
            self.child
                .view
                .as_ref()
                .expect("list child must be readable")
                .cast::<T>()
        }
    }
}

/// Traverses the child-vector range belonging to a [`ListRef`].
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
}

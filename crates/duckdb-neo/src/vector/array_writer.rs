use std::{borrow::Borrow, ptr::NonNull};

use super::{RowWriter, Unknown, Vector, VectorWriter, WithWriter, out_of_bounds};
use crate::{Result, types::Array};

/// Writes `ARRAY` rows, each owning `N` elements of the child, through the element type's own [`RowWriter`].
///
/// ```
/// # use duckdb_neo::{DuckDBType, Result, data_chunk::DataChunk, environment::{Environment, StorageLocation}, types::Array, vector::RowWriter};
/// # fn main() -> Result<()> {
/// # let conn = Environment::new()?.open(StorageLocation::InMemory)?.connect()?;
/// let mut chunk = DataChunk::create(&conn, &[Array::<i32, 2>::logical_type(&conn)?], true)?;
/// let mut vector = chunk.get_vector_at_mut::<Array<i32, 2>>(0)?;
/// vector.set_size(2)?;
/// let mut writer = vector.array_writer()?;
/// RowWriter::set(&mut writer, 0, &Array([Some(1), None]))?;
/// writer.set_null(1)?;
/// # drop(writer);
/// assert_eq!(vector.get_value_slow(0)?.get::<Array<i32, 2>>()?, Some(Array([Some(1), None])));
/// assert_eq!(vector.get_value_slow(1)?.get::<Array<i32, 2>>()?, None);
/// # Ok(())
/// # }
/// ```
pub struct ArrayWriter<'w, T: WithWriter, const N: usize> {
    parent: VectorWriter<'w, Array<T, N>>,
    elements: T::Writer<'w>,
}

impl<'w, T: WithWriter + 'w, const N: usize> ArrayWriter<'w, T, N> {
    /// # Safety
    /// As for [`WithWriter::row_writer`].
    pub(super) unsafe fn from_raw(mut vector: NonNull<Vector<'w, Array<T, N>>>) -> Result<Self> {
        // SAFETY: the caller guarantees an exclusive borrow of a validated, writable vector.
        let child = unsafe { vector.as_mut() }
            .children
            .as_mut()
            .expect("validated ARRAY vector has a child")
            .cached_iter_mut()?
            .next()
            .expect("validated ARRAY vector has a child");
        let child = NonNull::from(child).cast::<Vector<'w, Unknown>>();
        // SAFETY: the writer keeps `vector` exclusively borrowed and only touches its child through
        // `elements`, which `Array<T, N>::validate` checked as `T`.
        let (parent, elements) = unsafe { (VectorWriter::from_raw(vector)?, T::row_writer(child.cast())?) };
        Ok(Self { parent, elements })
    }

    /// Return the number of rows.
    pub fn len(&self) -> usize {
        self.parent.len()
    }

    /// Return whether the vector has no rows.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Borrow the element writer, for columnar writes; row `r` owns elements `r * N..(r + 1) * N`.
    pub fn elements(&mut self) -> &mut T::Writer<'w> {
        &mut self.elements
    }

    /// Make a row valid, keeping whatever its elements hold; they are always initialized.
    pub fn set_valid(&mut self, row: usize) -> Result<()> {
        if row >= self.len() {
            return Err(out_of_bounds(row, self.len()));
        }
        // SAFETY: the row is in bounds, and every element is initialized.
        unsafe { self.parent.set_validity_unchecked(row, true) };
        Ok(())
    }

    /// Make a row SQL NULL, along with its elements as DuckDB requires.
    pub fn set_null(&mut self, row: usize) -> Result<()> {
        if row >= self.len() {
            return Err(out_of_bounds(row, self.len()));
        }
        for element in row * N..(row + 1) * N {
            self.elements.set_null(element)?;
        }
        // SAFETY: the row is in bounds.
        unsafe { self.parent.set_validity_unchecked(row, false) };
        Ok(())
    }
}

impl<'w, T, const N: usize> RowWriter for ArrayWriter<'w, T, N>
where
    T: WithWriter + Borrow<<T::Writer<'w> as RowWriter>::Value> + 'w,
{
    type Value = Array<T, N>;

    fn len(&self) -> usize {
        ArrayWriter::len(self)
    }

    fn set(&mut self, row: usize, value: &Array<T, N>) -> Result<()> {
        if row >= self.len() {
            return Err(out_of_bounds(row, self.len()));
        }
        for (element, value) in (row * N..(row + 1) * N).zip(&value.0) {
            if let Err(error) = self.elements.set_option(element, value.as_ref().map(Borrow::borrow)) {
                // Earlier elements were written, so NULL the row to keep its elements consistent.
                ArrayWriter::set_null(self, row)?;
                return Err(error);
            }
        }
        // SAFETY: the row is in bounds, and its elements were just written.
        unsafe { self.parent.set_validity_unchecked(row, true) };
        Ok(())
    }

    fn set_null(&mut self, row: usize) -> Result<()> {
        ArrayWriter::set_null(self, row)
    }

    unsafe fn resize_rows(&mut self, len: usize) -> Result<()> {
        // DuckDB resizes the child to `len * N` along with the parent.
        self.parent.resize(len)?;
        self.elements.refresh()
    }

    fn refresh(&mut self) -> Result<()> {
        self.parent.refresh_all()?;
        self.elements.refresh()
    }
}

// SAFETY: every row's elements are initialized, and NULL rows null their elements.
unsafe impl<T, const N: usize> WithWriter for Array<T, N>
where
    T: WithWriter + for<'w> Borrow<<T::Writer<'w> as RowWriter>::Value>,
{
    type Writer<'w>
        = ArrayWriter<'w, T, N>
    where
        Self: 'w;

    unsafe fn row_writer<'w>(vector: NonNull<Vector<'w, Self>>) -> Result<Self::Writer<'w>> {
        // SAFETY: the caller upholds the borrow contract.
        unsafe { ArrayWriter::from_raw(vector) }
    }
}

impl<T: WithWriter, const N: usize> std::fmt::Debug for ArrayWriter<'_, T, N> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArrayWriter").field("len", &self.len()).finish()
    }
}

impl<T: WithWriter, const N: usize> Vector<'_, Array<T, N>> {
    /// Prepare an [`ArrayWriter`].
    pub fn array_writer(&mut self) -> Result<ArrayWriter<'_, T, N>> {
        // SAFETY: the exclusive borrow covers the writer, and validation checked the type.
        unsafe { ArrayWriter::from_raw(NonNull::from(self).cast()) }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        DuckDBType, Result,
        data_chunk::DataChunk,
        environment::{Environment, StorageLocation},
        types::{Array, List, Map},
        vector::{RowReader, RowWriter},
    };

    #[test]
    fn nested_writers_compose() -> Result<()> {
        let conn = Environment::new()?.open(StorageLocation::InMemory)?.connect()?;
        let types = [
            List::<List<String>>::logical_type(&conn)?,
            List::<Map<String, List<i32>>>::logical_type(&conn)?,
            List::<Array<String, 2>>::logical_type(&conn)?,
        ];
        let mut chunk = DataChunk::create(&conn, &types, true)?;
        let [lists, maps, arrays] = chunk.get_vector_array_mut([0, 1, 2])?;
        let long = "a string longer than the inline storage".to_owned();

        let mut lists = lists.cast::<List<List<String>>>()?;
        lists.set_size(3)?;
        let nested = List(vec![
            Some(List(vec![Some(long.clone()), None])),
            None,
            Some(List(vec![])),
        ]);
        {
            let mut writer = lists.list_writer()?;
            // Enough rows to grow the inner lists' rows and elements several times.
            for _ in 0..500 {
                writer.push(Some(&nested.0[0].clone().unwrap()))?;
            }
            writer.finish_row(0)?;
            RowWriter::set(&mut writer, 1, &nested)?;
            writer.push_with(|inner, index| {
                inner.push(Some("built"))?;
                inner.finish_row(index)
            })?;
            writer.finish_row(2)?;
            writer.finish()?;
        }
        let read = lists.get_value_slow(0)?.get::<List<List<String>>>()?.unwrap();
        assert_eq!(read.0.len(), 500);
        assert!(read.0.iter().all(|inner| inner == &nested.0[0]));
        assert_eq!(lists.get_value_slow(1)?.get::<List<List<String>>>()?, Some(nested));
        assert_eq!(
            lists.get_value_slow(2)?.get::<List<List<String>>>()?,
            Some(List(vec![Some(List(vec![Some("built".to_owned())]))]))
        );

        let mut maps = maps.cast::<List<Map<String, List<i32>>>>()?;
        maps.set_size(2)?;
        let map = Map(vec![
            ("a".to_owned(), Some(List(vec![Some(1), None]))),
            (long.clone(), None),
        ]);
        {
            let mut writer = maps.list_writer()?;
            for _ in 0..300 {
                writer.push(Some(&map))?;
            }
            writer.finish_row(0)?;
            writer.set_null(1)?;
        }
        let reader = maps.list_reader()?;
        let row = reader.get(0)?.unwrap();
        assert_eq!(row.len(), 300);
        let entries = row.get(299).unwrap().unwrap();
        assert!(matches!(entries.get_key(long.as_str()), Some(None)));
        assert_eq!(
            entries.get_key("a").unwrap().unwrap().iter().collect::<Vec<_>>(),
            [Some(&1), None]
        );
        assert!(reader.get(1)?.is_none());

        let mut arrays = arrays.cast::<List<Array<String, 2>>>()?;
        arrays.set_size(1)?;
        {
            let mut writer = arrays.list_writer()?;
            for index in 0..100 {
                if index % 3 == 0 {
                    writer.push(None)?;
                } else {
                    writer.push(Some(&Array([Some(index.to_string()), None])))?;
                }
            }
            writer.finish_row(0)?;
        }
        let read = arrays.get_value_slow(0)?.get::<List<Array<String, 2>>>()?.unwrap();
        for (index, element) in read.0.iter().enumerate() {
            let expected = (index % 3 != 0).then(|| Array([Some(index.to_string()), None]));
            assert_eq!(element, &expected, "{index}");
        }
        Ok(())
    }
}

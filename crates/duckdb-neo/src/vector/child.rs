use std::ops::Deref;

use super::{ListWriter, MapWriter, UnionMembers, UnionWriter, Unknown, Vector, VectorElement, VectorWriter};
use crate::{
    Result,
    data_chunk::ChildrenMut,
    types::{List, Map},
    value::Value,
};

/// A child of a nested vector, borrowed mutably through [`Vector::children_mut`].
///
/// Readers index children by the rows of their parent, so a child keeps the size and
/// storage its parent gives it. Reads go through [`Deref`]; mutably, a child only allows
/// row writes and flattening, which keep every row. Resize the parent instead, or size
/// `LIST` and `MAP` elements with `set_child_size`:
///
/// ```compile_fail,E0596
/// # use duckdb_neo::{AsDuckDB, DuckDBType, data_chunk::DataChunk, environment::{Environment, StorageLocation}};
/// # #[derive(AsDuckDB)]
/// # struct Row { x: i64 }
/// # fn main() -> duckdb_neo::Result<()> {
/// # let conn = Environment::new()?.open(StorageLocation::InMemory)?.connect()?;
/// let mut chunk = DataChunk::create(&conn, &[Row::logical_type(&conn)?], true)?;
/// let mut rows = chunk.get_vector_at_mut::<Row>(0)?;
/// rows.set_size(1)?;
/// rows.children_mut().unwrap().get_vector_at_mut::<i64>(0)?.set_size(0)?;
/// # Ok(())
/// # }
/// ```
///
/// Operations that replace a child's storage are unavailable as well:
///
/// ```compile_fail,E0507
/// # use duckdb_neo::{AsDuckDB, DuckDBType, ToValue, data_chunk::DataChunk, environment::{Environment, StorageLocation}};
/// # #[derive(AsDuckDB)]
/// # struct Row { x: i64 }
/// # fn main() -> duckdb_neo::Result<()> {
/// # let conn = Environment::new()?.open(StorageLocation::InMemory)?.connect()?;
/// let mut chunk = DataChunk::create(&conn, &[Row::logical_type(&conn)?], true)?;
/// let mut rows = chunk.get_vector_at_mut::<Row>(0)?;
/// let mut children = rows.children_mut().unwrap();
/// children.get_vector_at_mut::<i64>(0)?.make_constant(7_i64.value(&conn)?, 1)?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct ChildVector<'a, T: VectorElement>(pub(crate) Vector<'a, T>);

impl<'a, T: VectorElement> Deref for ChildVector<'a, T> {
    type Target = Vector<'a, T>;

    fn deref(&self) -> &Vector<'a, T> {
        &self.0
    }
}

impl<T: VectorElement> ChildVector<'_, T> {
    /// See [`Vector::writer`].
    pub fn writer(&mut self) -> Result<VectorWriter<'_, T>> {
        self.0.writer()
    }

    /// See [`Vector::write_value_slow`].
    pub fn write_value_slow(&mut self, index: usize, value: &Value) -> Result<()> {
        self.0.write_value_slow(index, value)
    }

    /// See [`Vector::set_null_slow`].
    pub fn set_null_slow(&mut self, index: usize) -> Result<()> {
        self.0.set_null_slow(index)
    }

    /// See [`Vector::flatten`]; flattening keeps every row.
    pub fn flatten(&mut self) -> Result<()> {
        self.0.flatten()
    }

    /// See [`Vector::children_mut`].
    pub fn children_mut(&mut self) -> Option<ChildrenMut<'_>> {
        self.0.children_mut()
    }

    /// See [`Vector::union_writer`].
    pub fn union_writer<M: UnionMembers>(&mut self) -> Result<UnionWriter<'_, M>> {
        self.0.union_writer()
    }
}

impl<'a> ChildVector<'a, Unknown> {
    /// See [`Vector::cast`].
    pub fn cast<T: VectorElement>(self) -> Result<ChildVector<'a, T>> {
        self.0.cast().map(ChildVector)
    }
}

impl<T: VectorElement> ChildVector<'_, List<T>> {
    /// Size the element child, as for a top-level [`List`] vector.
    ///
    /// # Safety
    /// The contract of the top-level `set_child_size` applies.
    pub unsafe fn set_child_size(&mut self, elements: usize) -> Result<()> {
        // SAFETY: the caller upholds the contract of `Vector::set_child_size`.
        unsafe { self.0.set_child_size(elements) }
    }

    /// Continue writes at `offset`, as for a top-level vector.
    pub fn set_child_offset(&mut self, offset: usize) -> Result<()> {
        self.0.set_child_offset(offset)
    }

    /// See [`Vector::list_writer`].
    pub fn list_writer(&mut self) -> Result<ListWriter<'_, T>> {
        self.0.list_writer()
    }
}

impl<K: VectorElement, V: VectorElement> ChildVector<'_, Map<K, V>> {
    /// Size the entries child, as for a top-level [`Map`] vector.
    ///
    /// # Safety
    /// The contract of the top-level `set_child_size` applies.
    pub unsafe fn set_child_size(&mut self, entries: usize) -> Result<()> {
        // SAFETY: the caller upholds the contract of `Vector::set_child_size`.
        unsafe { self.0.set_child_size(entries) }
    }

    /// Continue writes at `offset`, as for a top-level vector.
    pub fn set_child_offset(&mut self, offset: usize) -> Result<()> {
        self.0.set_child_offset(offset)
    }

    /// See [`Vector::map_writer`].
    pub fn map_writer(&mut self) -> Result<MapWriter<'_, K, V>> {
        self.0.map_writer()
    }
}

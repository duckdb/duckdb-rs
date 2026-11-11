use std::{marker::PhantomData, ptr::NonNull};

use super::{Unknown, Vector, VectorElement, VectorWriter, WritableVectorElement, out_of_bounds};
use crate::{Result, data_chunk::VectorCollection, error::Error, logical_type::LogicalTypeID};

/// The member types of a `UNION`, as a tuple in declaration order: `(i32, String)` for
/// `UNION(num INTEGER, str VARCHAR)`.
///
/// # Safety
/// `validate` must return `Ok(true)` only if member `i` of the union validates as the `i`-th tuple
/// element, so that [`UnionMember`] reports its type.
pub unsafe trait UnionMembers {
    /// The number of members.
    const COUNT: usize;

    /// Check each member vector; `children` holds the tag followed by the members.
    fn validate(children: &VectorCollection) -> Result<bool>;
}

/// The type of member `I` of a [`UnionMembers`] tuple.
pub trait UnionMember<const I: usize>: UnionMembers {
    /// The member's vector element type.
    type Member: VectorElement;
}

fn validate_member<T: VectorElement>(children: &VectorCollection, index: usize) -> Result<bool> {
    let child = children.get_untyped_vector_at(index)?;
    T::validate(child.logical_type(), child.children())
}

macro_rules! union_members {
    ($count:expr; $($name:ident $index:tt),+) => {
        // SAFETY: each member is validated as its tuple element.
        unsafe impl<$($name: VectorElement),+> UnionMembers for ($($name,)+) {
            const COUNT: usize = $count;

            fn validate(children: &VectorCollection) -> Result<bool> {
                $(
                    if !validate_member::<$name>(children, $index + 1)? {
                        return Ok(false);
                    }
                )+
                Ok(true)
            }
        }
        union_members!(@member [$($name),+] $($name $index),+);
    };
    (@member [$($all:ident),+] $name:ident $index:tt $(, $rest:ident $rest_index:tt)*) => {
        impl<$($all: VectorElement),+> UnionMember<$index> for ($($all,)+) {
            type Member = $name;
        }
        union_members!(@member [$($all),+] $($rest $rest_index),*);
    };
    (@member [$($all:ident),+]) => {};
}

union_members!(1; A 0);
union_members!(2; A 0, B 1);
union_members!(3; A 0, B 1, C 2);
union_members!(4; A 0, B 1, C 2, D 3);
union_members!(5; A 0, B 1, C 2, D 3, E 4);
union_members!(6; A 0, B 1, C 2, D 3, E 4, F 5);
union_members!(7; A 0, B 1, C 2, D 3, E 4, F 5, G 6);
union_members!(8; A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7);

/// Writes `UNION` rows whose member types `M` are checked once, when the writer is created.
///
/// The writer takes over every row: creating it makes each row the first member with a NULL
/// value. [`Self::set`] then writes the tag and the active member, keeping the other members
/// NULL as DuckDB requires, and [`Self::set_null`] nulls the row and its members.
///
/// ```
/// # use duckdb_neo::{Result, data_chunk::DataChunk, environment::{Environment, StorageLocation}, logical_type::LogicalType, types::Union};
/// # fn main() -> Result<()> {
/// # let conn = Environment::new()?.open(StorageLocation::InMemory)?.connect()?;
/// let union_type = LogicalType::from_text(&conn, "UNION(num INTEGER, str VARCHAR)")?;
/// let mut chunk = DataChunk::create(&conn, &[union_type], true)?;
/// let mut vector = chunk.get_vector_at_mut::<Union>(0)?;
/// vector.set_size(3)?;
/// let mut writer = vector.union_writer::<(i32, String)>()?;
/// writer.set::<0>(0, Some(&42))?;
/// writer.set::<1>(1, Some("duck"))?;
/// writer.set_null(2)?;
/// # drop(writer);
/// assert_eq!(vector.get_value_slow(1)?.dbg_string()?, "duck");
/// assert!(vector.reader()?.get(2)?.is_none());
/// # Ok(())
/// # }
/// ```
///
/// Member types are checked against the union:
///
/// ```
/// # use duckdb_neo::{Result, data_chunk::DataChunk, environment::{Environment, StorageLocation}, logical_type::LogicalType, types::Union};
/// # fn main() -> Result<()> {
/// # let conn = Environment::new()?.open(StorageLocation::InMemory)?.connect()?;
/// # let union_type = LogicalType::from_text(&conn, "UNION(num INTEGER, str VARCHAR)")?;
/// # let mut chunk = DataChunk::create(&conn, &[union_type], true)?;
/// # let mut vector = chunk.get_vector_at_mut::<Union>(0)?;
/// assert!(vector.union_writer::<(String, i32)>().is_err());
/// assert!(vector.union_writer::<(i32,)>().is_err());
/// # Ok(())
/// # }
/// ```
pub struct UnionWriter<'w, M: UnionMembers> {
    parent: VectorWriter<'w, Unknown>,
    tags: VectorWriter<'w, u8>,
    members: Box<[VectorWriter<'w, Unknown>]>,
    /// Whether each member has children that a NULL row must null too.
    nested: Box<[bool]>,
    _members: PhantomData<M>,
}

impl<'w, M: UnionMembers> UnionWriter<'w, M> {
    pub(super) fn new<U: VectorElement>(vector: &'w mut Vector<'_, U>) -> Result<Self> {
        if vector.logical_type().id()? != LogicalTypeID::Union {
            return Err(Error::invalid_input(format!(
                "union_writer requires a UNION vector, got {}",
                vector.logical_type().to_text()?
            )));
        }
        let children = vector.children.as_ref().expect("UNION vectors have a tag child");
        if children.col_count() != M::COUNT + 1 || !validate_member::<u8>(children, 0)? || !M::validate(children)? {
            return Err(Error::invalid_input(format!(
                "union_writer member types do not match {}",
                vector.logical_type().to_text()?
            )));
        }
        let len = vector.len();
        let mut children = vector
            .children
            .as_mut()
            .expect("UNION vectors have a tag child")
            .cached_iter_mut()?
            .map(|child| NonNull::from(child).cast::<Vector<'w, Unknown>>())
            .collect::<Vec<_>>()
            .into_iter();
        let tags = children.next().expect("UNION vectors have a tag child");
        let mut nested = Vec::with_capacity(M::COUNT);
        for member in children.clone() {
            // SAFETY: the member wrappers stay alive while the writer borrows the vector.
            let member = unsafe { &mut *member.as_ptr() };
            member.null_rows(0, len)?;
            nested.push(member.children().is_some());
        }
        // SAFETY: the writer keeps `vector` exclusively borrowed and only touches its children
        // through these disjoint writers, whose types were validated above.
        let (mut parent, mut tags, members) = unsafe {
            (
                VectorWriter::from_raw(NonNull::from(vector).cast::<Vector<'w, Unknown>>())?,
                VectorWriter::<u8>::from_raw(tags.cast())?,
                children
                    .map(|member| VectorWriter::from_raw(member.cast()))
                    .collect::<Result<Box<[_]>>>()?,
            )
        };
        // SAFETY: every row becomes the first member, whose value was nulled above.
        unsafe {
            tags.as_mut_ptr().write_bytes(0, len);
            tags.fill_validity(true)?;
            parent.fill_validity(true)?;
        }
        Ok(Self {
            parent,
            tags,
            members,
            nested: nested.into(),
            _members: PhantomData,
        })
    }

    /// Return the number of rows.
    pub fn len(&self) -> usize {
        self.parent.len()
    }

    /// Return whether the vector has no rows.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Make a row SQL NULL, along with its tag and every member.
    pub fn set_null(&mut self, row: usize) -> Result<()> {
        if row >= self.len() {
            return Err(out_of_bounds(row, self.len()));
        }
        // SAFETY: the row is in bounds of the parent and of every child, which share its rows.
        unsafe {
            self.parent.set_validity_unchecked(row, false);
            self.tags.set_validity_unchecked(row, false);
            for member in 0..M::COUNT {
                self.null_member(member, row)?;
            }
        }
        Ok(())
    }

    /// # Safety
    /// `row` must be in bounds and `member` below `M::COUNT`.
    #[inline]
    unsafe fn null_member(&mut self, member: usize, row: usize) -> Result<()> {
        if self.nested[member] {
            self.members[member].vector_mut().null_rows(row, row + 1)
        } else {
            // SAFETY: the caller guarantees bounds; the member has no descendants to null.
            unsafe { self.members[member].set_validity_unchecked(row, false) };
            Ok(())
        }
    }

    /// Make member `I` the row's active member, holding `value`; `None` is a NULL member value.
    #[inline]
    pub fn set<const I: usize>(
        &mut self,
        row: usize,
        value: Option<&<<M as UnionMember<I>>::Member as WritableVectorElement>::Write>,
    ) -> Result<()>
    where
        M: UnionMember<I>,
        <M as UnionMember<I>>::Member: WritableVectorElement + 'w,
    {
        if row >= self.len() {
            return Err(out_of_bounds(row, self.len()));
        }
        // SAFETY: the row is in bounds of every child; creating the writer initialized every tag.
        unsafe {
            let tag = self.tags.as_mut_ptr().add(row);
            let previous = *tag as usize;
            if previous != I {
                if previous < M::COUNT {
                    self.null_member(previous, row)?;
                }
                *tag = I as u8;
            }
            self.tags.set_validity_unchecked(row, true);
            self.parent.set_validity_unchecked(row, true);
            // SAFETY: `M::validate` checked member `I` as `UnionMember<I>::Member`.
            self.members[I]
                .cast_unchecked::<<M as UnionMember<I>>::Member>()
                .set_unchecked(row, value)
        }
    }
}

impl<M: UnionMembers> std::fmt::Debug for UnionWriter<'_, M> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UnionWriter")
            .field("len", &self.len())
            .field("members", &M::COUNT)
            .finish()
    }
}

impl<U: VectorElement> Vector<'_, U> {
    /// Prepare a [`UnionWriter`] for a `UNION` vector whose members are `M`.
    pub fn union_writer<M: UnionMembers>(&mut self) -> Result<UnionWriter<'_, M>> {
        UnionWriter::new(self)
    }
}

#[cfg(test)]
mod tests {
    use duckdb_loadable_macros::AsDuckDB;

    use crate::{
        Result,
        connection::Connection,
        data_chunk::DataChunk,
        environment::{Environment, StorageLocation},
        logical_type::LogicalType,
        types::Union,
        vector::Vector,
    };

    #[derive(Debug, AsDuckDB)]
    struct Inner {
        a: i32,
    }

    fn connection() -> Result<Connection> {
        Environment::new()?.open(StorageLocation::InMemory)?.connect()
    }

    fn nulls<T: crate::vector::VectorElement>(vector: &Vector<'_, Union>, child: usize) -> Result<Vec<bool>> {
        let reader = vector.children().unwrap().get_vector_at::<T>(child)?.reader()?;
        (0..reader.len()).map(|row| reader.is_null(row)).collect()
    }

    #[test]
    fn union_writer_keeps_inactive_members_null() -> Result<()> {
        let conn = connection()?;
        let union_type = LogicalType::from_text(&conn, "UNION(num INTEGER, str VARCHAR)")?;
        let mut chunk = DataChunk::create(&conn, &[union_type], true)?;
        let mut vector = chunk.get_vector_at_mut::<Union>(0)?;
        // SAFETY: the union writer initializes every row.
        unsafe { vector.set_size_uninit(5)? };
        {
            let mut writer = vector.union_writer::<(i32, String)>()?;
            writer.set::<0>(0, Some(&7))?;
            writer.set::<1>(1, Some("long enough to leave the inline storage"))?;
            writer.set::<0>(1, None)?;
            writer.set::<1>(2, Some("x"))?;
            writer.set_null(2)?;
            writer.set::<0>(3, Some(&1))?;
            writer.set::<1>(3, Some("y"))?;
            assert!(writer.set::<0>(5, Some(&1)).is_err());
            assert!(writer.set_null(5).is_err());
        }
        let values = (0..5)
            .map(|row| vector.get_value_slow(row)?.dbg_string())
            .collect::<Result<Vec<_>>>()?;
        assert_eq!(values, ["7", "NULL", "NULL", "y", "NULL"]);
        let reader = vector.reader()?;
        assert_eq!(
            (0..5).map(|row| reader.is_null(row)).collect::<Result<Vec<_>>>()?,
            [false, false, true, false, false]
        );
        assert_eq!(
            *vector
                .children()
                .unwrap()
                .get_vector_at::<u8>(0)?
                .reader()?
                .get(3)?
                .unwrap(),
            1
        );
        assert_eq!(nulls::<i32>(&vector, 1)?, [false, true, true, true, true]);
        assert_eq!(nulls::<String>(&vector, 2)?, [true, true, true, false, true]);
        Ok(())
    }

    #[test]
    fn union_writer_nulls_nested_members() -> Result<()> {
        let conn = connection()?;
        let union_type = LogicalType::from_text(&conn, "UNION(rec STRUCT(a INTEGER), num INTEGER)")?;
        let mut chunk = DataChunk::create(&conn, &[union_type], true)?;
        let mut vector = chunk.get_vector_at_mut::<Union>(0)?;
        vector.set_size(3)?;
        {
            let mut writer = vector.union_writer::<(Inner, i32)>()?;
            writer.set::<0>(0, Some(&Inner { a: 1 }))?;
            writer.set::<0>(1, Some(&Inner { a: 2 }))?;
            writer.set::<1>(1, Some(&5))?;
            writer.set::<0>(2, Some(&Inner { a: 3 }))?;
            writer.set_null(2)?;
        }
        let inner = vector.children().unwrap().get_untyped_vector_at(1)?;
        let fields = inner.children().unwrap().get_vector_at::<i32>(0)?.reader()?;
        assert_eq!(
            (0..3).map(|row| fields.is_null(row)).collect::<Result<Vec<_>>>()?,
            [false, true, true]
        );
        assert_eq!(nulls::<Inner>(&vector, 1)?, [false, true, true]);
        assert_eq!(vector.get_value_slow(1)?.dbg_string()?, "5");
        Ok(())
    }

    #[test]
    fn union_writer_rejects_non_unions() -> Result<()> {
        let conn = connection()?;
        let mut chunk = DataChunk::create(&conn, &[LogicalType::from_text(&conn, "STRUCT(num INTEGER)")?], true)?;
        let mut vector = chunk.get_vector_at_mut::<crate::types::Struct>(0)?;
        let error = vector.union_writer::<(i32,)>().unwrap_err();
        assert!(error.message.contains("requires a UNION"), "{error}");
        Ok(())
    }
}

use std::ptr::NonNull;

use super::{
    NativeElement, RowWriter, Unknown, Vector, VectorReader, VectorWriter, WithReader, WithWriter, out_of_bounds,
};
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
    type Member: NativeElement;
}

/// Member tuples whose members all have a [`RowReader`](super::RowReader).
pub trait UnionReaders: UnionMembers {
    /// The tuple of member readers.
    type Readers<'a>
    where
        Self: 'a;

    /// Prepare the member readers; `members` are the validated member vectors.
    #[doc(hidden)]
    fn readers<'a>(members: &[&'a Vector<'a, Unknown>]) -> Result<Self::Readers<'a>>
    where
        Self: 'a;
}

/// Member tuples whose members all have a [`RowWriter`].
///
/// # Safety
/// `writers` must build writers of the validated member types.
pub unsafe trait UnionWriters: UnionMembers {
    /// The tuple of member writers.
    type Writers<'w>
    where
        Self: 'w;

    /// # Safety
    /// As for [`WithWriter::row_writer`], for each of the validated member vectors.
    #[doc(hidden)]
    unsafe fn writers<'w>(members: &[NonNull<Vector<'w, Unknown>>]) -> Result<Self::Writers<'w>>
    where
        Self: 'w;

    /// NULL `row` of member `member`.
    #[doc(hidden)]
    fn set_null(writers: &mut Self::Writers<'_>, member: usize, row: usize) -> Result<()>;

    #[doc(hidden)]
    fn refresh(writers: &mut Self::Writers<'_>) -> Result<()>;
}

/// The reader of member `I`.
pub trait UnionMemberReader<const I: usize>: UnionReaders + UnionMember<I, Member: WithReader> {
    /// Borrow member `I`'s reader.
    #[doc(hidden)]
    fn reader<'r, 'a>(
        readers: &'r Self::Readers<'a>,
    ) -> &'r <<Self as UnionMember<I>>::Member as WithReader>::Reader<'a>
    where
        Self: 'a;
}

/// The writer of member `I`.
pub trait UnionMemberWriter<const I: usize>: UnionWriters + UnionMember<I, Member: WithWriter> {
    /// Borrow member `I`'s writer.
    #[doc(hidden)]
    fn writer<'r, 'w>(
        writers: &'r mut Self::Writers<'w>,
    ) -> &'r mut <<Self as UnionMember<I>>::Member as WithWriter>::Writer<'w>
    where
        Self: 'w;
}

fn validate_member<T: NativeElement>(children: &VectorCollection, index: usize) -> Result<bool> {
    let child = children.get_untyped_vector_at(index)?;
    T::validate(child.logical_type(), child.children())
}

macro_rules! union_members {
    ($count:expr; $($name:ident $index:tt),+) => {
        // SAFETY: each member is validated as its tuple element.
        unsafe impl<$($name: NativeElement),+> UnionMembers for ($($name,)+) {
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

        impl<$($name: WithReader),+> UnionReaders for ($($name,)+) {
            type Readers<'a>
                = ($($name::Reader<'a>,)+)
            where
                Self: 'a;

            fn readers<'a>(members: &[&'a Vector<'a, Unknown>]) -> Result<Self::Readers<'a>>
            where
                Self: 'a,
            {
                // SAFETY: `validate` checked every member as its tuple element.
                Ok(($($name::row_reader(unsafe { members[$index].cast_ref_unchecked::<$name>() })?,)+))
            }
        }

        // SAFETY: each writer is built for its validated member.
        unsafe impl<$($name: WithWriter),+> UnionWriters for ($($name,)+) {
            type Writers<'w>
                = ($($name::Writer<'w>,)+)
            where
                Self: 'w;

            unsafe fn writers<'w>(members: &[NonNull<Vector<'w, Unknown>>]) -> Result<Self::Writers<'w>>
            where
                Self: 'w,
            {
                // SAFETY: the caller upholds the borrow contract for each disjoint member.
                Ok(($(unsafe { $name::row_writer(members[$index].cast()) }?,)+))
            }

            fn set_null(writers: &mut Self::Writers<'_>, member: usize, row: usize) -> Result<()> {
                match member {
                    $($index => writers.$index.set_null(row),)+
                    _ => Ok(()),
                }
            }

            fn refresh(writers: &mut Self::Writers<'_>) -> Result<()> {
                $(writers.$index.refresh()?;)+
                Ok(())
            }
        }
        union_members!(@member [$($name),+] $($name $index),+);
    };
    (@member [$($all:ident),+] $name:ident $index:tt $(, $rest:ident $rest_index:tt)*) => {
        impl<$($all: NativeElement),+> UnionMember<$index> for ($($all,)+) {
            type Member = $name;
        }

        impl<$($all: WithReader),+> UnionMemberReader<$index> for ($($all,)+) {
            fn reader<'r, 'a>(readers: &'r Self::Readers<'a>) -> &'r $name::Reader<'a>
            where
                Self: 'a,
            {
                &readers.$index
            }
        }

        impl<$($all: WithWriter),+> UnionMemberWriter<$index> for ($($all,)+) {
            fn writer<'r, 'w>(writers: &'r mut Self::Writers<'w>) -> &'r mut $name::Writer<'w>
            where
                Self: 'w,
            {
                &mut writers.$index
            }
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
union_members!(9; A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7, I 8);
union_members!(10; A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7, I 8, J 9);
union_members!(11; A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7, I 8, J 9, K 10);
union_members!(12; A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7, I 8, J 9, K 10, L 11);
union_members!(13; A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7, I 8, J 9, K 10, L 11, M 12);
union_members!(14; A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7, I 8, J 9, K 10, L 11, M 12, N 13);
union_members!(15; A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7, I 8, J 9, K 10, L 11, M 12, N 13, O 14);
union_members!(16; A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7, I 8, J 9, K 10, L 11, M 12, N 13, O 14, P 15);

fn check_union<M: UnionMembers>(vector: &Vector<'_, Unknown>) -> Result<()> {
    if vector.logical_type().id()? != LogicalTypeID::Union {
        return Err(Error::invalid_input(format!(
            "union readers and writers require a UNION vector, got {}",
            vector.logical_type().to_text()?
        )));
    }
    let children = vector.children().expect("UNION vectors have a tag child");
    if children.col_count() != M::COUNT + 1 || !validate_member::<u8>(children, 0)? || !M::validate(children)? {
        return Err(Error::invalid_input(format!(
            "union member types do not match {}",
            vector.logical_type().to_text()?
        )));
    }
    Ok(())
}

/// Reads `UNION` rows whose member types `M` are checked once, when the reader is created.
///
/// ```
/// # use duckdb_neo::{Parameters, Result, environment::{Environment, StorageLocation}, types::Union, vector::RowReader};
/// # fn main() -> Result<()> {
/// # let conn = Environment::new()?.open(StorageLocation::InMemory)?.connect()?;
/// let chunk = conn
///     .query("SELECT union_value(str := 'duck')::UNION(num INTEGER, str VARCHAR)", Parameters::None)?
///     .next_chunk()?
///     .unwrap();
/// let vector = chunk.get_vector_at::<Union>(0)?;
/// let reader = vector.union_reader::<(i32, String)>()?;
/// assert_eq!(reader.tag(0)?, Some(1));
/// assert_eq!(reader.member::<1>().get(0)?, Some("duck"));
/// # Ok(())
/// # }
/// ```
pub struct UnionReader<'a, M: UnionReaders + 'a> {
    rows: VectorReader<'a, Unknown>,
    tags: VectorReader<'a, u8>,
    members: M::Readers<'a>,
}

impl<'a, M: UnionReaders + 'a> UnionReader<'a, M> {
    /// Prepare a reader for a `UNION` vector whose members are `M`.
    pub fn new<U: NativeElement>(vector: &'a Vector<'a, U>) -> Result<Self> {
        // SAFETY: both wrappers differ only in markers.
        let vector = unsafe { &*(vector as *const Vector<'a, U>).cast::<Vector<'a, Unknown>>() };
        check_union::<M>(vector)?;
        let rows = vector.reader()?;
        let children = vector.children().expect("UNION vectors have a tag child");
        let members = (1..=M::COUNT)
            .map(|index| children.get_untyped_vector_at(index))
            .collect::<Result<Vec<_>>>()?;
        // SAFETY: `check_union` validated the tag as `u8`.
        let tags = unsafe { children.get_untyped_vector_at(0)?.cast_ref_unchecked::<u8>() }.reader()?;
        Ok(Self {
            rows,
            tags,
            members: M::readers(&members)?,
        })
    }

    /// Return the number of rows.
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Return whether there are no rows.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Return the active member of a row, or `None` for SQL NULL. Out-of-range rows are errors.
    pub fn tag(&self, row: usize) -> Result<Option<u8>> {
        if self.rows.is_null(row)? {
            return Ok(None);
        }
        Ok(self.tags.get(row)?.copied())
    }

    /// # Safety
    /// `row` must be less than [`Self::len`].
    #[doc(hidden)]
    #[inline(always)]
    pub unsafe fn tag_unchecked(&self, row: usize) -> Option<u8> {
        // SAFETY: the caller guarantees bounds; tags share the union's rows.
        unsafe {
            self.rows.physical_index_unchecked(row)?;
            self.tags.get_unchecked(row).copied()
        }
    }

    /// Borrow member `I`'s reader, indexed by the union's rows; inactive members are NULL.
    pub fn member<const I: usize>(&self) -> &<<M as UnionMember<I>>::Member as WithReader>::Reader<'a>
    where
        M: UnionMemberReader<I>,
    {
        M::reader(&self.members)
    }
}

impl<'a, M: UnionReaders + 'a> std::fmt::Debug for UnionReader<'a, M> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UnionReader")
            .field("len", &self.len())
            .field("members", &M::COUNT)
            .finish()
    }
}

/// Writes `UNION` rows whose member types `M` are checked once, when the writer is created.
///
/// [`Self::set`] writes the tag and the active member and nulls every other member of the row,
/// as DuckDB requires, and [`Self::set_null`] nulls the row and its members. Rows that are not
/// written keep their contents; [`Vector::set_size`] makes new rows the first member.
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
/// writer.set::<0>(0, &42)?;
/// writer.set::<1>(1, "duck")?;
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
pub struct UnionWriter<'w, M: UnionWriters + 'w> {
    parent: VectorWriter<'w, Unknown>,
    tags: VectorWriter<'w, u8>,
    members: M::Writers<'w>,
}

impl<'w, M: UnionWriters + 'w> UnionWriter<'w, M> {
    /// # Safety
    /// As for [`WithWriter::row_writer`], for any element type validated as a `UNION`.
    #[doc(hidden)]
    pub unsafe fn from_raw(mut vector: NonNull<Vector<'w, Unknown>>) -> Result<Self> {
        // SAFETY: the caller guarantees an exclusive borrow of a writable vector.
        let borrowed = unsafe { vector.as_mut() };
        check_union::<M>(borrowed)?;
        let mut children = borrowed
            .children
            .as_mut()
            .expect("UNION vectors have a tag child")
            .cached_iter_mut()?
            .map(|child| NonNull::from(child).cast::<Vector<'w, Unknown>>())
            .collect::<Vec<_>>()
            .into_iter();
        let tags = children.next().expect("UNION vectors have a tag child");
        let members = children.collect::<Vec<_>>();
        // SAFETY: the writer keeps `vector` exclusively borrowed and only touches its children
        // through these disjoint writers, whose types `check_union` validated.
        let (parent, tags, members) = unsafe {
            (
                VectorWriter::from_raw(vector)?,
                VectorWriter::<u8>::from_raw(tags.cast())?,
                M::writers(&members)?,
            )
        };
        Ok(Self { parent, tags, members })
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
        for member in 0..M::COUNT {
            M::set_null(&mut self.members, member, row)?;
        }
        // SAFETY: the row is in bounds of the parent and its tags, which share its rows.
        unsafe {
            self.parent.set_validity_unchecked(row, false);
            self.tags.set_validity_unchecked(row, false);
        }
        Ok(())
    }

    /// Make member `I` the row's active member and borrow its writer, to write the member's `row`.
    /// The other members of the row become NULL; member `I` keeps its contents until written.
    #[inline]
    pub fn member<const I: usize>(
        &mut self,
        row: usize,
    ) -> Result<&mut <<M as UnionMember<I>>::Member as WithWriter>::Writer<'w>>
    where
        M: UnionMemberWriter<I>,
    {
        if row >= self.len() {
            return Err(out_of_bounds(row, self.len()));
        }
        for member in (0..M::COUNT).filter(|member| *member != I) {
            M::set_null(&mut self.members, member, row)?;
        }
        // SAFETY: the row is in bounds of every child.
        unsafe {
            self.tags.as_mut_ptr().add(row).write(I as u8);
            self.tags.set_validity_unchecked(row, true);
            self.parent.set_validity_unchecked(row, true);
        }
        Ok(M::writer(&mut self.members))
    }

    /// Make member `I` the row's active member, holding `value`; a failed write leaves the row NULL.
    #[inline]
    pub fn set<const I: usize>(
        &mut self,
        row: usize,
        value: &<<<M as UnionMember<I>>::Member as WithWriter>::Writer<'w> as RowWriter>::Value,
    ) -> Result<()>
    where
        M: UnionMemberWriter<I>,
    {
        let written = self.member::<I>(row)?.set(row, value);
        if written.is_err() {
            self.set_null(row)?;
        }
        written
    }

    /// Make member `I` the row's active member, holding `value`; `None` is a NULL member value.
    /// A failed write leaves the row NULL.
    #[inline]
    pub fn set_option<const I: usize>(
        &mut self,
        row: usize,
        value: Option<&<<<M as UnionMember<I>>::Member as WithWriter>::Writer<'w> as RowWriter>::Value>,
    ) -> Result<()>
    where
        M: UnionMemberWriter<I>,
    {
        let written = self.member::<I>(row)?.set_option(row, value);
        if written.is_err() {
            self.set_null(row)?;
        }
        written
    }

    /// Resize the rows like [`Vector::set_size`].
    #[doc(hidden)]
    pub fn resize_rows(&mut self, len: usize) -> Result<()> {
        self.parent.resize(len)?;
        self.tags.refresh_all()?;
        M::refresh(&mut self.members)
    }

    #[doc(hidden)]
    pub fn refresh(&mut self) -> Result<()> {
        self.parent.refresh_all()?;
        self.tags.refresh_all()?;
        M::refresh(&mut self.members)
    }
}

impl<'w, M: UnionWriters + 'w> std::fmt::Debug for UnionWriter<'w, M> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UnionWriter")
            .field("len", &self.len())
            .field("members", &M::COUNT)
            .finish()
    }
}

impl<U: NativeElement> Vector<'_, U> {
    /// Prepare a [`UnionReader`] for a `UNION` vector whose members are `M`.
    pub fn union_reader<'a, M: UnionReaders + 'a>(&'a self) -> Result<UnionReader<'a, M>> {
        UnionReader::new(self)
    }

    /// Prepare a [`UnionWriter`] for a `UNION` vector whose members are `M`.
    pub fn union_writer<'w, M: UnionWriters + 'w>(&'w mut self) -> Result<UnionWriter<'w, M>> {
        // SAFETY: the exclusive borrow covers the writer.
        unsafe { UnionWriter::from_raw(NonNull::from(self).cast()) }
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
    #[duckdb(writer)]
    struct Inner {
        a: i32,
    }

    fn connection() -> Result<Connection> {
        Environment::new()?.open(StorageLocation::InMemory)?.connect()
    }

    fn nulls<T: crate::vector::NativeElement>(vector: &Vector<'_, Union>, child: usize) -> Result<Vec<bool>> {
        let reader = vector.children().unwrap().get_vector_at::<T>(child)?.reader()?;
        (0..reader.len()).map(|row| reader.is_null(row)).collect()
    }

    #[test]
    fn union_writer_keeps_inactive_members_null() -> Result<()> {
        let conn = connection()?;
        let union_type = LogicalType::from_text(&conn, "UNION(num INTEGER, str VARCHAR)")?;
        let mut chunk = DataChunk::create(&conn, &[union_type], true)?;
        let mut vector = chunk.get_vector_at_mut::<Union>(0)?;
        vector.set_size(5)?;
        {
            let mut writer = vector.union_writer::<(i32, String)>()?;
            writer.set::<0>(0, &7)?;
            writer.set::<1>(1, "long enough to leave the inline storage")?;
            writer.set_option::<0>(1, None)?;
            writer.set::<1>(2, "x")?;
            writer.set_null(2)?;
            writer.set::<0>(3, &1)?;
            writer.set::<1>(3, "y")?;
            assert!(writer.set::<0>(5, &1).is_err());
            assert!(writer.set_null(5).is_err());
        }
        let values = (0..5)
            .map(|row| vector.get_value_slow(row)?.dbg_string())
            .collect::<Result<Vec<_>>>()?;
        // Unwritten rows keep the zeroed first member that `set_size` gave them.
        assert_eq!(values, ["7", "NULL", "NULL", "y", "0"]);
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
        assert_eq!(nulls::<i32>(&vector, 1)?, [false, true, true, true, false]);
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
            writer.set::<0>(0, &Inner { a: 1 })?;
            writer.set::<0>(1, &Inner { a: 2 })?;
            writer.set::<1>(1, &5)?;
            writer.set::<0>(2, &Inner { a: 3 })?;
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
        assert!(error.message.contains("require a UNION"), "{error}");
        Ok(())
    }

    #[test]
    fn union_members_compose_with_nested_writers_and_readers() -> Result<()> {
        use crate::{types::List, vector::RowReader};
        let conn = connection()?;
        let union_type = LogicalType::from_text(&conn, "UNION(items INTEGER[], num INTEGER)")?;
        let mut chunk = DataChunk::create(&conn, &[union_type], true)?;
        let mut vector = chunk.get_vector_at_mut::<Union>(0)?;
        vector.set_size(4)?;
        {
            let mut writer = vector.union_writer::<(List<i32>, i32)>()?;
            let items = writer.member::<0>(0)?;
            items.push(Some(&1))?;
            items.push(None)?;
            items.finish_row(0)?;
            writer.set::<1>(1, &5)?;
            writer.member::<0>(2)?;
            writer.set_null(3)?;
        }
        let reader = vector.union_reader::<(List<i32>, i32)>()?;
        assert_eq!(reader.len(), 4);
        assert_eq!(reader.tag(0)?, Some(0));
        let items = reader.member::<0>().get(0)?.unwrap();
        assert_eq!(items.iter().collect::<Vec<_>>(), [Some(&1), None]);
        assert_eq!(reader.tag(1)?, Some(1));
        assert_eq!(reader.member::<1>().get(1)?, Some(&5));
        assert!(reader.member::<0>().get(1)?.is_none());
        // A member chosen but never written keeps its initialized value.
        assert_eq!(reader.tag(2)?, Some(0));
        assert!(reader.member::<0>().get(2)?.unwrap().is_empty());
        assert_eq!(reader.tag(3)?, None);
        assert!(reader.tag(4).is_err());
        assert!(vector.union_reader::<(i32, i32)>().is_err());
        Ok(())
    }
}

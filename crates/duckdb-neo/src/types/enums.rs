//! The `ENUM` logical type.
//!
//! [`EnumIndex`] reads and writes a vector's dictionary indices in the integer DuckDB picks from
//! the dictionary size: `u8` up to 255 entries, `u16` up to 65,535, and `u32` beyond.

use crate::{
    Error, Result,
    data_chunk::VectorCollection,
    logical_type::{LogicalType, LogicalTypeID},
    vector::{NativeElement, ReadableNativeElement, VectorReader, VectorWriter, WritableNativeElement},
};

mod sealed {
    pub trait Sealed {}
    impl Sealed for u8 {}
    impl Sealed for u16 {}
    impl Sealed for u32 {}
}

/// Unsigned integers DuckDB stores `ENUM` dictionary indices in.
pub trait EnumStorage:
    sealed::Sealed + Copy + Into<u32> + std::fmt::Debug + Eq + Ord + std::hash::Hash + 'static
{
    /// The dictionary sizes DuckDB stores in this integer.
    const ENTRIES: std::ops::RangeInclusive<usize>;
}

impl EnumStorage for u8 {
    const ENTRIES: std::ops::RangeInclusive<usize> = 0..=0xFF;
}

impl EnumStorage for u16 {
    const ENTRIES: std::ops::RangeInclusive<usize> = 0x100..=0xFFFF;
}

impl EnumStorage for u32 {
    const ENTRIES: std::ops::RangeInclusive<usize> = 0x1_0000..=0xFFFF_FFFF;
}

/// The dictionary index of an `ENUM` row, stored as `T`.
///
/// Writes check the index against the vector's dictionary.
///
/// ```
/// # use duckdb_neo::{Result, data_chunk::DataChunk, environment::{Environment, StorageLocation}, logical_type::LogicalType, types::EnumIndex};
/// # fn main() -> Result<()> {
/// # let conn = Environment::new()?.open(StorageLocation::InMemory)?.connect()?;
/// let mood = LogicalType::from_text(&conn, "ENUM('sad', 'happy')")?;
/// let mut chunk = DataChunk::create(&conn, &[mood], true)?;
/// let mut vector = chunk.get_vector_at_mut::<EnumIndex<u8>>(0)?;
/// vector.set_size(2)?;
/// let mut writer = vector.writer()?;
/// writer.set(0, &EnumIndex(1))?;
/// assert!(writer.set(1, &EnumIndex(2)).is_err());
/// # drop(writer);
/// assert_eq!(vector.get_value_slow(0)?.dbg_string()?, "happy");
/// # Ok(())
/// # }
/// ```
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EnumIndex<T: EnumStorage>(pub T);

impl<T: EnumStorage> EnumIndex<T> {
    /// Return the index into the dictionary.
    pub fn index(self) -> u32 {
        self.0.into()
    }
}

// SAFETY: `validate` accepts only the dictionary sizes DuckDB stores as `T`.
unsafe impl<T: EnumStorage> NativeElement for EnumIndex<T> {
    const TYPE_ID: LogicalTypeID = LogicalTypeID::Enum;

    type Native<'a> = T;

    fn validate(other: &LogicalType, _children: Option<&VectorCollection>) -> Result<bool> {
        if other.id()? != Self::TYPE_ID {
            return Ok(false);
        }
        Ok(T::ENTRIES.contains(&other.param_count()?))
    }
}

// SAFETY: `get` reads the validated physical slot.
unsafe impl<T: EnumStorage> ReadableNativeElement for EnumIndex<T> {
    type Ref<'a> = &'a EnumIndex<T>;

    #[inline]
    unsafe fn get<'a, U: NativeElement>(reader: &VectorReader<'a, U>, physical: usize, _logical: usize) -> Self::Ref<'a>
    where
        Self: 'a,
    {
        // SAFETY: the caller guarantees a valid row of this storage.
        unsafe { &*reader.as_ptr().cast::<EnumIndex<T>>().add(physical) }
    }
}

// SAFETY: indices are checked against the vector's dictionary.
unsafe impl<T: EnumStorage> WritableNativeElement for EnumIndex<T> {
    type Write = Self;

    #[inline]
    unsafe fn write(writer: &mut VectorWriter<'_, Self>, index: usize, value: &Self) -> Result<()> {
        check_index(value.index(), writer.vector().enum_size()?)?;
        // SAFETY: the caller guarantees an in-bounds row, and the index is in the dictionary.
        unsafe { writer.write_internal(index, Some(*value)) }
    }
}

impl<T: EnumStorage> VectorWriter<'_, EnumIndex<T>> {
    /// Copy `values` into rows `offset..offset + values.len()` and mark those rows valid.
    ///
    /// Every index is checked against the dictionary before anything is written.
    pub fn write_indices(&mut self, offset: usize, values: &[EnumIndex<T>]) -> Result<()> {
        if let Some(max) = values.iter().max() {
            check_index(max.index(), self.vector().enum_size()?)?;
        }
        // SAFETY: every index is in the dictionary.
        unsafe { self.write_slice_unchecked(offset, values) }
    }
}

fn check_index(index: u32, size: usize) -> Result<()> {
    if (index as usize) < size {
        return Ok(());
    }
    Err(Error::invalid_input(format!(
        "ENUM index {index} is past the {size} dictionary entries"
    )))
}

#[cfg(test)]
mod tests {
    use super::EnumIndex;
    use crate::{
        Parameters, Result,
        data_chunk::DataChunk,
        environment::{Environment, StorageLocation},
        logical_type::LogicalType,
    };

    fn connection() -> Result<crate::connection::Connection> {
        Environment::new()?.open(StorageLocation::InMemory)?.connect()
    }

    fn enum_type(conn: &crate::connection::Connection, entries: usize) -> Result<LogicalType> {
        let names = (0..entries).map(|entry| format!("'e{entry}'")).collect::<Vec<_>>();
        let name = format!("enum_{entries}");
        let sql = format!("CREATE TYPE IF NOT EXISTS {name} AS ENUM ({})", names.join(", "));
        let mut result = conn.query(&sql, Parameters::None)?;
        while result.next_chunk()?.is_some() {}
        LogicalType::from_text(conn, &name)
    }

    #[test]
    fn enum_storage_follows_the_dictionary_size() -> Result<()> {
        let conn = connection()?;
        for (entries, width) in [(0, 1), (3, 1), (255, 1), (256, 2), (65_535, 2), (65_536, 4)] {
            let mut chunk = DataChunk::create(&conn, &[enum_type(&conn, entries)?], true)?;
            let fits = [
                chunk.get_vector_at::<EnumIndex<u8>>(0).is_ok(),
                chunk.get_vector_at::<EnumIndex<u16>>(0).is_ok(),
                chunk.get_vector_at::<EnumIndex<u32>>(0).is_ok(),
            ];
            let expected = [width == 1, width == 2, width == 4];
            assert_eq!(fits, expected, "{entries} entries");
            assert!(chunk.get_vector_at_mut::<u8>(0).is_err());
        }
        Ok(())
    }

    #[test]
    fn enum_writes_check_the_dictionary() -> Result<()> {
        let conn = connection()?;
        let mut chunk = DataChunk::create(&conn, &[enum_type(&conn, 300)?], true)?;
        let mut vector = chunk.get_vector_at_mut::<EnumIndex<u16>>(0)?;
        vector.set_size(4)?;
        {
            let mut writer = vector.writer()?;
            writer.set(0, &EnumIndex(299))?;
            let error = writer.set(1, &EnumIndex(300)).unwrap_err();
            assert!(error.message.contains("ENUM index 300 is past the 300"), "{error}");
            assert!(writer.write_indices(1, &[EnumIndex(1), EnumIndex(300)]).is_err());
            writer.write_indices(1, &[EnumIndex(7), EnumIndex(8)])?;
            writer.set_null(3)?;
        }
        let values = (0..4)
            .map(|row| vector.get_value_slow(row)?.dbg_string())
            .collect::<Result<Vec<_>>>()?;
        assert_eq!(values, ["e299", "e7", "e8", "NULL"]);
        let reader = vector.reader()?;
        assert_eq!(reader.get(1)?.map(|index| index.index()), Some(7));
        Ok(())
    }

    #[test]
    fn empty_enum_rows_start_null() -> Result<()> {
        let conn = connection()?;
        let mut chunk = DataChunk::create(&conn, &[enum_type(&conn, 0)?], true)?;
        let mut vector = chunk.get_vector_at_mut::<EnumIndex<u8>>(0)?;
        vector.set_size(3)?;
        assert!(vector.reader()?.iter().all(|row| row.is_none()));
        assert!(vector.writer()?.set(0, &EnumIndex(0)).is_err());
        Ok(())
    }

    #[test]
    fn enum_indices_read_from_queries() -> Result<()> {
        let conn = connection()?;
        let mut result = conn.query(
            "SELECT s::ENUM('a', 'b', 'c') FROM (VALUES ('b'), (NULL), ('c'), ('a')) t(s)",
            Parameters::None,
        )?;
        let mut indices = Vec::new();
        while let Some(chunk) = result.next_chunk()? {
            let reader = chunk.get_vector_at::<EnumIndex<u8>>(0)?.reader()?;
            indices.extend(reader.iter().map(|value| value.map(|value| value.index())));
        }
        assert_eq!(indices, [Some(1), None, Some(2), Some(0)]);
        Ok(())
    }

    #[test]
    fn enum_indices_compose_into_lists() -> Result<()> {
        use crate::{
            types::List,
            vector::{RowReader, RowWriter},
        };
        let conn = connection()?;
        let list = LogicalType::from_text(&conn, "ENUM('a', 'b', 'c')[]")?;
        let mut chunk = DataChunk::create(&conn, &[list], true)?;
        let mut vector = chunk.get_vector_at_mut::<List<EnumIndex<u8>>>(0)?;
        vector.set_size(1)?;
        {
            let mut writer = vector.list_writer()?;
            writer.set(0, &List(vec![Some(EnumIndex(2)), None, Some(EnumIndex(0))]))?;
            assert!(writer.set(0, &List(vec![Some(EnumIndex(3))])).is_err());
        }
        assert_eq!(vector.get_value_slow(0)?.dbg_string()?, "[c, NULL, a]");
        let reader = vector.list_reader()?;
        let row = reader.get(0)?.unwrap();
        let indices = row
            .iter()
            .map(|index| index.map(|index| index.index()))
            .collect::<Vec<_>>();
        assert_eq!(indices, [Some(2), None, Some(0)]);
        Ok(())
    }
}

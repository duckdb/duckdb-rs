use duckdb_loadable_macros::AsDuckDB;

use super::{ReaderStorage, VectorReader};
use crate::{
    Parameters, Result, ToValue,
    data_chunk::DataChunk,
    environment::{Environment, StorageLocation},
    scalar::MicroScalar,
    types::{DuckDBType, List},
    value::Value,
    vector::{RowReader, RowWriter, Selection, StorageKind, Unknown, Vector},
};

fn assert_physical_reads(reader: &VectorReader<'_, i32>) -> Result<()> {
    let data = reader.as_ptr();
    let validity = reader.validity();
    let selection = reader.selection();
    for logical in 0..reader.len() {
        let physical = selection.index(logical);
        let is_null = validity.is_some_and(|mask| mask[physical / 64] & (1u64 << (physical % 64)) == 0);
        assert_eq!(is_null, reader.is_null(logical)?);
        let value = if is_null {
            None
        } else {
            assert!(!data.is_null());
            // SAFETY: Selection maps an in-bounds row to an initialized, non-NULL slot.
            Some(unsafe { *data.add(physical) })
        };
        assert_eq!(value, reader.get(logical)?.copied());
    }
    Ok(())
}

#[test]
fn selection_maps_logical_to_physical_indices() {
    assert_eq!(Selection::Identity.index(123), 123);
    assert_eq!(Selection::Constant.index(usize::MAX), 0);
    let selection = Selection::Dictionary(&[129, 0, 129, 64]);
    assert_eq!(selection.index(0), 129);
    assert_eq!(selection.index(1), 0);
    assert_eq!(selection.index(2), 129);
    assert_eq!(selection.index(3), 64);
}

#[test]
#[should_panic]
fn dictionary_selection_checks_bounds() {
    Selection::Dictionary(&[3]).index(1);
}

#[test]
fn reader_exposes_flat_data_and_physical_validity() -> Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    let mut chunk = DataChunk::create(&conn, &[i32::logical_type(&conn)?], true)?;
    let mut vector = chunk.get_vector_at_mut::<i32>(0)?;
    vector.set_size(130)?;
    for index in 0..vector.len() {
        vector.writer()?.set(index, &(index as i32))?;
    }
    let reader = vector.reader()?;
    assert_eq!(reader.selection(), Selection::Identity);
    assert!(reader.validity().unwrap().iter().all(|word| *word == u64::MAX));
    // SAFETY: Every physical slot was initialized above.
    let values = unsafe { vector.reader()?.as_slice() }.unwrap();
    assert_eq!(values, (0..130).collect::<Vec<i32>>());
    assert_physical_reads(&reader)?;

    for index in [0, 64, 129] {
        vector.writer()?.set_null(index)?;
    }
    let reader = vector.reader()?;
    let mask = reader.validity().unwrap();
    assert_eq!(mask.len(), 3);
    assert_eq!(mask[0] & 1, 0);
    assert_eq!(mask[1] & 1, 0);
    assert_eq!(mask[2] & 2, 0);
    // SAFETY: Setting NULL only cleared validity; the initialized data is unchanged.
    assert_eq!(unsafe { reader.as_slice() }.unwrap()[129], 129);
    assert_physical_reads(&reader)?;

    vector.set_size(0)?;
    let reader = vector.reader()?;
    assert_eq!(reader.selection(), Selection::Identity);
    assert!(reader.validity().is_none_or(|mask| mask.is_empty()));
    // SAFETY: Empty flat storage exposes no slots.
    assert!(unsafe { reader.as_slice() }.is_none_or(|values| values.is_empty()));
    Ok(())
}

#[test]
fn flat_reader_handles_empty_rows_nulls_and_bounds() -> Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    let mut chunk = DataChunk::create(&conn, &[i32::logical_type(&conn)?], true)?;
    let mut vector = chunk.get_vector_at_mut::<i32>(0)?;
    vector.set_size(0)?;
    let reader = vector.reader()?;
    assert!(matches!(reader.cached().storage, ReaderStorage::Flat));
    assert!(reader.is_empty());
    assert_eq!(reader.len(), 0);
    assert_eq!(reader.iter().next(), None);
    assert!(reader.get(0).is_err());
    assert!(reader.is_null(0).is_err());

    vector.set_size(130)?;
    for index in 0..vector.len() {
        let value = (index % 3 != 0).then_some(index as i32);
        vector.writer()?.set_option(index, value.as_ref())?;
    }
    let reader: VectorReader<'_, i32> = vector.reader()?;
    assert_eq!(reader.vector().storage_kind(), StorageKind::Flat);
    assert!(!reader.is_empty());
    assert_eq!(reader.len(), 130);
    for index in 0..reader.len() {
        let expected = (index % 3 != 0).then_some(index as i32);
        assert_eq!(reader.get(index)?.copied(), expected);
        assert_eq!(reader.is_null(index)?, expected.is_none());
    }
    assert!(reader.get(130).is_err());
    assert!(reader.get(usize::MAX).is_err());
    assert!(reader.is_null(130).is_err());
    assert!(reader.is_null(usize::MAX).is_err());
    assert_eq!(reader.iter().count(), 130);
    assert_eq!(reader.iter().count(), 130);
    assert_physical_reads(&reader)?;
    Ok(())
}

#[test]
fn reader_iterator_tracks_remaining_rows() -> Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    let mut chunk = DataChunk::create(&conn, &[i32::logical_type(&conn)?], true)?;
    let mut vector = chunk.get_vector_at_mut::<i32>(0)?;
    vector.set_size(3)?;
    for (index, value) in [Some(1), None, Some(3)].into_iter().enumerate() {
        vector.writer()?.set_option(index, value.as_ref())?;
    }
    let mut iter = vector.reader()?.iter();
    fn assert_traits(_: &impl ExactSizeIterator, _: &impl std::iter::FusedIterator) {}
    assert_traits(&iter, &iter);
    assert_eq!(iter.size_hint(), (3, Some(3)));
    assert_eq!(iter.next(), Some(Some(&1)));
    assert_eq!(iter.len(), 2);
    assert_eq!(iter.next(), Some(None));
    assert_eq!(iter.len(), 1);
    assert_eq!(iter.next(), Some(Some(&3)));
    assert_eq!(iter.size_hint(), (0, Some(0)));
    assert_eq!(iter.next(), None);
    assert_eq!(iter.next(), None);

    let reader = vector.reader()?;
    let mut rows = reader.iter();
    rows.next();
    let mut rows = rows.enumerate();
    assert_traits(&rows, &rows);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows.next(), Some((0, None)));
    assert_eq!(rows.next(), Some((1, Some(&3))));
    assert_eq!(rows.next(), None);
    assert_eq!(rows.next(), None);
    assert!(
        reader
            .iter()
            .enumerate()
            .eq(reader.iter().collect::<Vec<_>>().into_iter().enumerate())
    );
    Ok(())
}

#[test]
fn constant_reader_does_not_index_the_shared_selection() -> Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    let ty = i32::logical_type(&conn)?;
    for count in [0, 1, 2 * crate::vector::CONSTANT_SELECTION_LEN + 1] {
        for expected in [Some(42), None] {
            // A constant column is not writable output, so each constant needs a fresh chunk.
            let mut chunk = DataChunk::create(&conn, std::slice::from_ref(&ty), true)?;
            let value = match expected {
                Some(value) => value.value(&conn)?,
                None => Value::null(&conn, &ty)?,
            };
            let vector = chunk.get_vector_at_mut::<i32>(0)?.make_constant(value, count)?;
            let reader = vector.reader()?;
            assert!(matches!(reader.cached().storage, ReaderStorage::Constant(_)));
            assert_eq!(reader.vector().storage_kind(), StorageKind::Constant);
            assert_eq!(reader.len(), count);
            assert_eq!(reader.is_empty(), count == 0);
            assert_eq!(reader.selection(), Selection::Constant);
            assert_eq!(reader.selection().index(usize::MAX), 0);
            assert!(reader.validity().is_none_or(|mask| mask.len() == 1));
            if let Some(value) = expected {
                // SAFETY: The non-NULL constant has one initialized physical slot.
                assert_eq!(unsafe { reader.as_slice() }.unwrap(), &[value]);
            }
            assert_physical_reads(&reader)?;
            assert!(reader.get(count).is_err());
            assert!(reader.is_null(count).is_err());
            assert_eq!(reader.iter().count(), count);
            for (index, value) in reader.iter().enumerate() {
                assert_eq!(value.copied(), expected);
                assert_eq!(reader.get(index)?.copied(), expected);
                assert_eq!(reader.is_null(index)?, expected.is_none());
            }
        }
    }
    Ok(())
}

#[derive(Debug, AsDuckDB)]
#[duckdb(reader)]
struct ParamStruct {
    value: i32,
}

#[test]
fn reader_values_borrow_the_vector_not_the_reader() -> Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    let chunk = conn
        .query(
            "SELECT 'hello'::VARCHAR, [[1, NULL], [3]]::INTEGER[][], {'value': 42}",
            Parameters::None,
        )?
        .next_chunk()?
        .unwrap();
    let text = chunk.get_vector_at::<String>(0)?.reader()?.get(0)?.unwrap();
    assert_eq!(text, "hello");

    let vector = chunk.get_vector_at::<List<List<i32>>>(1)?;
    let lists = vector.list_reader()?;
    let row = lists.get(0)?.unwrap();
    let values = row
        .iter()
        .map(|row| row.unwrap().iter().map(|value| value.copied()).collect::<Vec<_>>())
        .collect::<Vec<_>>();
    assert_eq!(values, [vec![Some(1), None], vec![Some(3)]]);

    let vector = chunk.get_vector_at::<ParamStruct>(2)?;
    let reader = vector.reader()?;
    assert!(reader.as_ptr().is_null());
    // SAFETY: STRUCT has no physical data slots to initialize.
    assert!(unsafe { reader.as_slice() }.is_none());
    assert!(reader.get(0)?.is_some());
    let row = vector.row_reader()?.get(0)?.unwrap();
    assert_eq!(row.value, Some(&42));
    Ok(())
}

fn same_view(reader: &VectorReader<'_, impl crate::vector::NativeElement>, vector: &Vector<'_, Unknown>) -> bool {
    let (copied, cached) = (reader.view(), vector.view.get().unwrap().view);
    (copied.data, copied.validity, copied.sel, copied.count) == (cached.data, cached.validity, cached.sel, cached.count)
}

#[test]
fn nested_readers_copy_child_metadata() -> Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    let chunk = conn
        .query("SELECT [[1, NULL], [3]]::INTEGER[][]", Parameters::None)?
        .next_chunk()?
        .unwrap();
    let vector = chunk.get_vector_at::<List<List<i32>>>(0)?;
    let reader = vector.reader()?;
    assert_eq!(reader.view().data, reader.clone().view().data);
    let lists = vector.list_reader()?;
    let inner = vector.children().unwrap().get_untyped_vector_at(0)?;
    let elements = inner.children().unwrap().get_untyped_vector_at(0)?;
    assert!(same_view(lists.elements().elements(), elements));
    assert_eq!(lists.elements().len(), 2);
    assert_eq!(
        lists.elements().get(0)?.unwrap().iter().collect::<Vec<_>>(),
        [Some(&1), None]
    );
    assert_eq!(
        lists.elements().elements().iter().collect::<Vec<_>>(),
        [Some(&1), None, Some(&3)]
    );
    Ok(())
}

#[test]
fn reader_handles_do_not_own_storage() {
    type Reader<'a> = VectorReader<'a, List<List<i32>>>;
    assert_eq!(
        std::mem::size_of::<Reader<'_>>(),
        std::mem::size_of::<&()>() + std::mem::size_of::<super::CachedReader>()
    );
    assert!(!std::mem::needs_drop::<Reader<'_>>());
}

#[test]
fn reader_rejects_other_storage_until_explicitly_flattened() -> Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    let mut chunk = DataChunk::create(&conn, &[i64::logical_type(&conn)?], true)?;
    let mut vector = chunk.get_vector_at_mut::<i64>(0)?.make_sequence(3, 2, 3)?;
    let error = vector.reader().unwrap_err();
    assert!(error.message.contains("flatten"), "{}", error.message);
    assert_eq!(vector.storage_kind(), StorageKind::Other);
    vector.flatten()?;
    let reader = vector.reader()?;
    assert_eq!(reader.vector().storage_kind(), StorageKind::Flat);
    assert_eq!(reader.iter().collect::<Vec<_>>(), [Some(&3), Some(&5), Some(&7)]);
    Ok(())
}

#[test]
fn reader_handles_native_storage_layouts() -> Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    MicroScalar::create(
        "reader_probe",
        |_ctx, mut vector: Vector<'_, i32>, mut output: Vector<'_, i32>| {
            if vector.storage_kind() == StorageKind::Other {
                assert!(vector.reader().is_err());
                vector.flatten()?;
            }
            let reader = vector.reader()?;
            assert_eq!(reader.vector().storage_kind(), vector.storage_kind());
            assert_physical_reads(&reader)?;
            let stepped = reader.iter().map(|value| value.copied()).collect::<Vec<_>>();
            let mut folded = Vec::new();
            reader.iter().for_each(|value| folded.push(value.copied()));
            assert_eq!(folded, stepped);
            let mut rest = reader.iter();
            rest.next();
            let enumerated = rest.enumerate().fold(Vec::new(), |mut rows, (row, value)| {
                rows.push((row, value.copied()));
                rows
            });
            assert_eq!(
                enumerated,
                stepped.iter().skip(1).copied().enumerate().collect::<Vec<_>>()
            );
            if reader.vector().storage_kind() == StorageKind::Dictionary {
                assert!(matches!(reader.cached().storage, ReaderStorage::Dictionary(_)));
                let Selection::Dictionary(_) = reader.selection() else {
                    panic!("dictionary reader must expose its selection");
                };
                let reader = vector.reader()?;

                assert_eq!(reader.iter().collect::<Vec<_>>(), [Some(&i32::MAX), None]);
                assert_eq!(reader.get(0)?, Some(&i32::MAX));
                assert_eq!(reader.get(1)?, None);
                assert!(!reader.is_null(0)?);
                assert!(reader.is_null(1)?);
                assert!(reader.get(2).is_err());
                assert!(reader.is_null(2).is_err());
            }
            for (index, value) in reader.iter().enumerate() {
                output.writer()?.set_option(index, value)?;
            }
            Ok(())
        },
    )
    .register(&conn)?;
    let mut result = conn.query(
        "SELECT reader_probe(test_vector) FROM test_vector_types(NULL::INTEGER)",
        Parameters::None,
    )?;
    let mut values = Vec::new();
    while let Some(chunk) = result.next_chunk()? {
        let vector = chunk.get_vector_at::<i32>(0)?;
        let reader: VectorReader<'_, i32> = vector.reader()?;
        values.extend(reader.iter().map(|value| value.copied()));
    }
    assert_eq!(
        values,
        [
            Some(i32::MIN),
            Some(i32::MAX),
            None,
            Some(i32::MIN),
            Some(i32::MIN),
            Some(i32::MIN),
            Some(i32::MAX),
            None,
            Some(3),
            Some(5),
            Some(7),
        ]
    );
    Ok(())
}

fn assert_view_is_current<T: crate::vector::NativeElement>(vector: &crate::vector::Vector<'_, T>) -> Result<()> {
    let cached = vector.reader()?.view();
    let fresh = crate::check_api_call!(crate::ffi::duckdb_v2_vector_get_view, vector.handle, RET)?;
    assert_eq!(
        (cached.data, cached.validity, cached.count),
        (fresh.data, fresh.validity, fresh.count)
    );
    Ok(())
}

#[test]
fn cached_views_follow_writes_and_resizes() -> Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    let mut chunk = DataChunk::create(&conn, &[i32::logical_type(&conn)?], true)?;
    let mut vector = chunk.get_vector_at_mut::<i32>(0)?;
    vector.set_size(2)?;
    // The slow path writes valid rows without allocating a validity mask.
    vector.write_value_slow(0, &1_i32.value(&conn)?)?;
    vector.write_value_slow(1, &2_i32.value(&conn)?)?;
    assert_eq!(vector.reader()?.iter().collect::<Vec<_>>(), [Some(&1), Some(&2)]);
    assert!(vector.reader()?.validity().is_none());

    // The writer allocates the mask after the view was cached.
    vector.writer()?.set_null(1)?;
    assert_view_is_current(&vector)?;
    assert_eq!(vector.reader()?.iter().collect::<Vec<_>>(), [Some(&1), None]);

    vector.set_size(5000)?;
    assert_view_is_current(&vector)?;
    vector.writer()?.set(4999, &7)?;
    let reader = vector.reader()?;
    assert_eq!(reader.len(), 5000);
    assert_eq!(reader.get(0)?, Some(&1));
    assert_eq!(reader.get(4999)?, Some(&7));
    Ok(())
}

#[derive(Debug, AsDuckDB)]
#[duckdb(reader, writer)]
struct CachedField {
    x: Option<i32>,
}

#[test]
fn cached_child_views_follow_parent_writes() -> Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    let mut chunk = DataChunk::create(&conn, &[CachedField::logical_type(&conn)?], true)?;
    let mut vector = chunk.get_vector_at_mut::<CachedField>(0)?;
    vector.set_size(2)?;
    {
        let mut children = vector.children_mut().unwrap();
        let mut x = children.get_vector_at_mut::<i32>(0)?;
        x.write_value_slow(0, &1_i32.value(&conn)?)?;
        x.write_value_slow(1, &2_i32.value(&conn)?)?;
    }
    let read = |vector: &crate::vector::Vector<'_, CachedField>| -> Result<Vec<Option<Option<i32>>>> {
        Ok(vector
            .row_reader()?
            .iter()
            .map(|row| row.map(|row| row.x.copied()))
            .collect())
    };
    assert_eq!(read(&vector)?, [Some(Some(1)), Some(Some(2))]);
    assert!(
        vector
            .children()
            .unwrap()
            .get_vector_at::<i32>(0)?
            .reader()?
            .validity()
            .is_none()
    );

    vector.row_writer()?.set(1, &CachedField { x: None })?;
    assert_eq!(read(&vector)?, [Some(Some(1)), Some(None)]);
    assert_view_is_current(vector.children().unwrap().get_vector_at::<i32>(0)?)?;
    Ok(())
}

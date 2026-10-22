use super::CombinedVectorIterator;
use crate::{
    Parameters, Result, ToValue,
    builder_helpers::scalar_callback,
    data_chunk::DataChunk,
    environment::{Environment, StorageLocation},
    scalar::ScalarFunctionBuilder,
    signature::{FunctionParameter, SignatureBuilder},
    types::{DateValue, DuckDBType, Variant},
    value::Value,
    vector::{StorageKind, VectorReader},
};

#[test]
fn combined_rows_preserve_types_and_independent_nulls() -> Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    let chunk = conn
        .query(
            "SELECT * FROM (VALUES
                ('duck'::VARCHAR, 1::INTEGER, 1.5::DOUBLE, true, DATE '1970-01-01'),
                (NULL, 2, NULL, false, DATE '1970-01-02'),
                ('goose', NULL, 2.5, NULL, NULL)) t(s,n,d,b,dt)",
            Parameters::None,
        )?
        .next_chunk()?
        .unwrap();
    let s = chunk.get_vector_at::<String>(0)?.reader()?;
    let n = chunk.get_vector_at::<i32>(1)?.reader()?;
    let d = chunk.get_vector_at::<f64>(2)?.reader()?;
    let b = chunk.get_vector_at::<bool>(3)?.reader()?;
    let dt = chunk.get_vector_at::<DateValue>(4)?.reader()?;
    let rows = CombinedVectorIterator::new((s, n, d, b, dt))?.collect::<Vec<_>>();
    assert_eq!(
        rows,
        [
            (Some("duck"), Some(&1), Some(&1.5), Some(&true), Some(&DateValue(0))),
            (None, Some(&2), None, Some(&false), Some(&DateValue(1))),
            (Some("goose"), None, Some(&2.5), None, None),
        ]
    );
    Ok(())
}

#[test]
fn combined_iterator_tracks_remaining_rows_and_is_fused() -> Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    let chunk = conn
        .query("SELECT i::INTEGER, i::BIGINT FROM range(3) t(i)", Parameters::None)?
        .next_chunk()?
        .unwrap();
    let first = chunk.get_vector_at::<i32>(0)?.reader()?;
    let second = chunk.get_vector_at::<i64>(1)?.reader()?;
    let mut rows = CombinedVectorIterator::new((first, second))?;
    fn assert_traits(_: &impl ExactSizeIterator, _: &impl std::iter::FusedIterator) {}
    assert_traits(&rows, &rows);
    assert_eq!(rows.size_hint(), (3, Some(3)));
    assert_eq!(rows.next(), Some((Some(&0), Some(&0))));
    assert_eq!(rows.len(), 2);
    assert_eq!(rows.next(), Some((Some(&1), Some(&1))));
    assert_eq!(rows.len(), 1);
    assert_eq!(rows.next(), Some((Some(&2), Some(&2))));
    assert_eq!(rows.size_hint(), (0, Some(0)));
    assert_eq!(rows.next(), None);
    assert_eq!(rows.next(), None);
    Ok(())
}

#[test]
fn combined_iterator_accepts_empty_readers() -> Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    let chunk = DataChunk::create(&conn, &[i32::logical_type(&conn)?, String::logical_type(&conn)?], true)?;
    let first = chunk.get_vector_at::<i32>(0)?.reader()?;
    let second = chunk.get_vector_at::<String>(1)?.reader()?;
    let mut rows = CombinedVectorIterator::new((first, second))?;
    assert_eq!(rows.len(), 0);
    assert_eq!(rows.next(), None);
    assert_eq!(rows.next(), None);
    Ok(())
}

#[test]
fn combined_iterator_rejects_mismatched_lengths() -> Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    let ty = i32::logical_type(&conn)?;
    let mut chunk = DataChunk::create(&conn, &[ty.clone(), ty.clone()], true)?;
    let [first, second] = chunk.get_vector_array_mut([0, 1])?;
    let mut first = first.cast::<i32>()?;
    let mut second = second.cast::<i32>()?;
    for (left, right) in [(0, 1), (3, 2)] {
        first.set_size(left)?;
        second.set_size(right)?;
        let a = first.reader()?;
        let b = second.reader()?;
        let error = CombinedVectorIterator::new((a, b)).unwrap_err();
        assert!(error.message.contains("column 2"), "{}", error.message);
        let error = CombinedVectorIterator::new((a, a, b)).unwrap_err();
        assert!(error.message.contains("column 3"), "{}", error.message);
    }
    first.set_size(3)?;
    let second = second.make_constant(7_i32.value(&conn)?, 1)?;
    assert!(CombinedVectorIterator::new((first.reader()?, second.reader()?)).is_err());
    Ok(())
}

#[test]
fn combined_iterator_handles_large_and_null_constants() -> Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    let types = [i32::logical_type(&conn)?, i64::logical_type(&conn)?];
    let mut chunk = DataChunk::create(&conn, &types, true)?;
    let [constant, flat] = chunk.get_vector_array_mut([0, 1])?;
    let constant = constant.cast::<i32>()?;
    let mut flat = flat.cast::<i64>()?;
    let count = 2 * crate::vector::CONSTANT_SELECTION_LEN + 1;
    flat.set_size(count)?;
    {
        let mut writer = flat.writer()?;
        for index in 0..count {
            writer.set(index, (index % 5 != 0).then_some(index as i64))?;
        }
    }
    let constant = constant.make_constant(42_i32.value(&conn)?, count)?;
    let rows = CombinedVectorIterator::new((constant.reader()?, flat.reader()?))?;
    for (index, (constant, flat)) in rows.enumerate() {
        assert_eq!(constant, Some(&42));
        assert_eq!(flat.copied(), (index % 5 != 0).then_some(index as i64));
    }
    drop(constant);
    drop(flat);
    let [constant, flat] = chunk.get_vector_array_mut([0, 1])?;
    let constant = constant
        .cast::<i32>()?
        .make_constant(Value::null(&conn, &types[0])?, count)?;
    let flat = flat.cast::<i64>()?;
    let flat = flat.reader()?;
    for (index, (constant, flat)) in CombinedVectorIterator::new((constant.reader()?, flat))?.enumerate() {
        assert_eq!(constant, None);
        assert_eq!(flat.copied(), (index % 5 != 0).then_some(index as i64));
    }
    Ok(())
}

#[test]
fn combined_iterator_supports_one_and_twelve_columns() -> Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    let chunk = conn
        .query("SELECT 7::INTEGER", Parameters::None)?
        .next_chunk()?
        .unwrap();
    let r = chunk.get_vector_at::<i32>(0)?.reader()?;
    assert_eq!(CombinedVectorIterator::new((r,))?.next(), Some((Some(&7),)));
    let row = CombinedVectorIterator::new((r, r, r, r, r, r, r, r, r, r, r, r))?
        .next()
        .unwrap();
    assert_eq!(
        [
            row.0, row.1, row.2, row.3, row.4, row.5, row.6, row.7, row.8, row.9, row.10, row.11
        ],
        [Some(&7); 12]
    );
    Ok(())
}

fn pair_readers<'a, 'b>(
    first: VectorReader<'a, i32>,
    second: VectorReader<'b, String>,
) -> Result<CombinedVectorIterator<(VectorReader<'a, i32>, VectorReader<'b, String>)>> {
    CombinedVectorIterator::new((first, second))
}

#[test]
fn combined_rows_borrow_vectors_not_the_iterator() -> Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    let numbers = conn
        .query("SELECT 42::INTEGER", Parameters::None)?
        .next_chunk()?
        .unwrap();
    let strings = conn
        .query("SELECT 'duck'::VARCHAR", Parameters::None)?
        .next_chunk()?
        .unwrap();
    let row = {
        let first = numbers.get_vector_at::<i32>(0)?.reader()?;
        let second = strings.get_vector_at::<String>(0)?.reader()?;
        let mut rows = pair_readers(first, second)?;
        rows.next().unwrap()
    };
    assert_eq!(row, (Some(&42), Some("duck")));
    Ok(())
}

#[test]
fn combined_iterator_supports_owned_row_values() -> Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    let chunk = conn
        .query("SELECT 42::VARIANT, 7::INTEGER", Parameters::None)?
        .next_chunk()?
        .unwrap();
    let variant = chunk.get_vector_at::<Variant>(0)?.reader()?;
    let number = chunk.get_vector_at::<i32>(1)?.reader()?;
    let (variant, number) = CombinedVectorIterator::new((variant, number))?.next().unwrap();
    assert!(!variant.unwrap().is_null()?);
    assert_eq!(number, Some(&7));
    Ok(())
}

#[test]
fn enumerated_rows_restart_indices_after_partial_consumption() -> Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    let chunk = conn
        .query("SELECT i::INTEGER FROM range(4) t(i)", Parameters::None)?
        .next_chunk()?
        .unwrap();
    let reader = chunk.get_vector_at::<i32>(0)?.reader()?;
    let mut rows = CombinedVectorIterator::new((reader,))?;
    assert_eq!(rows.next(), Some((Some(&0),)));
    let mut numbered = rows.enumerate();
    assert_eq!(numbered.len(), 3);
    assert_eq!(numbered.next(), Some((0, (Some(&1),))));
    let rest = numbered.fold(Vec::new(), |mut values, (index, (value,))| {
        values.push((index, value.copied()));
        values
    });
    assert_eq!(rest, [(1, Some(2)), (2, Some(3))]);
    Ok(())
}

#[test]
fn flat_and_mixed_folds_preserve_nulls_and_indices() -> Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    let mut chunk = DataChunk::create(&conn, &[i32::logical_type(&conn)?, i32::logical_type(&conn)?], true)?;
    let [first, second] = chunk.get_vector_array_mut([0, 1])?;
    let mut first = first.cast::<i32>()?;
    let mut second = second.cast::<i32>()?;
    first.set_size(3)?;
    second.set_size(3)?;
    for (index, value) in [Some(1), None, Some(3)].into_iter().enumerate() {
        first.writer()?.set(index, value)?;
        second.writer()?.set(index, Some(9))?;
    }
    let flat = CombinedVectorIterator::new((first.reader()?, second.reader()?))?;
    assert!(flat.all_flat);
    let flat = flat.enumerate().fold(Vec::new(), |mut values, (index, (a, b))| {
        values.push((index, a.copied(), b.copied()));
        values
    });
    assert_eq!(flat, [(0, Some(1), Some(9)), (1, None, Some(9)), (2, Some(3), Some(9))]);
    let second = second.make_constant(9_i32.value(&conn)?, 3)?;
    let mixed = CombinedVectorIterator::new((first.reader()?, second.reader()?))?;
    assert!(!mixed.all_flat);
    let mixed = mixed.enumerate().fold(Vec::new(), |mut values, (index, (a, b))| {
        values.push((index, a.copied(), b.copied()));
        values
    });
    assert_eq!(mixed, flat);
    Ok(())
}

#[test]
fn enumerated_rows_are_exact_size_and_fused() -> Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    let chunk = conn
        .query("SELECT 42::INTEGER", Parameters::None)?
        .next_chunk()?
        .unwrap();
    let reader = chunk.get_vector_at::<i32>(0)?.reader()?;
    let mut rows = CombinedVectorIterator::new((reader,))?.enumerate();
    fn assert_traits(_: &impl ExactSizeIterator, _: &impl std::iter::FusedIterator) {}
    assert_traits(&rows, &rows);
    assert_eq!(rows.size_hint(), (1, Some(1)));
    assert_eq!(rows.next(), Some((0, (Some(&42),))));
    assert_eq!(rows.size_hint(), (0, Some(0)));
    assert_eq!(rows.next(), None);
    assert_eq!(rows.next(), None);
    Ok(())
}

scalar_callback!(CombinedProbe, i32, |input, output, ctx, _user_data| {
    let mut vector = input.get_vector_at_mut::<i32>(0)?;
    if vector.storage_kind() == StorageKind::Other {
        vector.flatten()?;
    }
    let reader = vector.reader()?;
    let mut scratch = DataChunk::create(ctx, &[i32::logical_type(ctx)?], true)?;
    let constant = scratch
        .get_vector_at_mut::<i32>(0)?
        .make_constant(9_i32.value(ctx)?, reader.len())?;
    let rows = CombinedVectorIterator::new((reader, constant.reader()?, reader))?;
    let mut output = output;
    output.set_size(rows.len())?;
    let mut writer = output.writer()?;
    for (index, (first, constant, repeated)) in rows.enumerate() {
        assert_eq!(constant, Some(&9));
        assert_eq!(first, repeated);
        writer.set(index, first.copied())?;
    }
    Ok(())
});

#[test]
fn combined_iterator_handles_native_dictionary_layouts() -> Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    ScalarFunctionBuilder::new(
        "combined_probe",
        SignatureBuilder::new()
            .param(FunctionParameter::new::<i32>("input"))
            .return_type(i32::logical_type(&conn)?),
        CombinedProbe,
    )
    .register(&conn)?;
    let mut result = conn.query(
        "SELECT combined_probe(test_vector) FROM test_vector_types(NULL::INTEGER)",
        Parameters::None,
    )?;
    let mut values = Vec::new();
    while let Some(chunk) = result.next_chunk()? {
        values.extend(
            chunk
                .get_vector_at::<i32>(0)?
                .reader()?
                .iter()
                .map(|value| value.copied()),
        );
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

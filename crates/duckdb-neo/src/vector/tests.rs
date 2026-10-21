use std::collections::HashMap;

use crate::logical_type::LogicalTypeID;
use crate::scalar::ScalarFunctionBuilder;
use crate::types::{Any, StructValue};
use crate::{
    Parameters, ToValue,
    builder_helpers::scalar_callback,
    connection::FFILink,
    data_chunk::DataChunk,
    environment::{Environment, StorageLocation},
    error::{DuckDBError, Error},
    logical_type::LogicalType,
    query_result::QueryResultStep,
    signature::{FunctionParameter, SignatureBuilder},
    types::{
        Array, BigNum, BigNumValue, BitValue, BlobValue, DateValue, DecimalSignature, DecimalValue, DuckDBType,
        IntervalValue, List, ListRef, LongDecimal, Map, MapValue, ShortDecimal, Struct, StructSchema, StructWrite,
        TimeNsValue, TimeTzValue, TimeValue, TimestampMsValue, TimestampNsValue, TimestampSecValue, TimestampTzNsValue,
        TimestampTzValue, TimestampValue, Union, UnionSchema, UnionValue, UnionWriter, UuidValueRaw, Variant,
        WordDecimal,
    },
    value::Value,
    vector::StorageKind,
};

#[test]
fn vector_from_handle_rejects_type_mismatches() -> crate::Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    let mut result = conn.query(
        "SELECT 42::INTEGER, [1, 2]::INTEGER[], 1.234::DECIMAL(18, 3)",
        Parameters::None,
    )?;
    let chunk = result.next_chunk()?.expect("one result chunk");

    let err = super::Vector::<i64>::from_handle(&chunk.get_vector_handle_at(0), super::Access::Shared)
        .expect_err("scalar type mismatch");
    assert!(err.message.contains("INTEGER"), "{}", err.message);

    let err = super::Vector::<List<i64>>::from_handle(&chunk.get_vector_handle_at(1), super::Access::Shared)
        .expect_err("nested child type mismatch");
    assert!(err.message.contains("INTEGER[]"), "{}", err.message);

    let err = super::Vector::<crate::types::Decimal<18, 2>>::from_handle(
        &chunk.get_vector_handle_at(2),
        super::Access::Shared,
    )
    .expect_err("decimal scale mismatch");
    assert!(err.message.contains("DECIMAL(18,3)"), "{}", err.message);
    Ok(())
}

#[test]
fn vector_dynamic_fields_validate_before_reading() -> crate::Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    let chunk = conn
        .query(
            "SELECT {'value': 42, 'empty': NULL::INTEGER, 'items': [1, NULL]::INTEGER[]},
                    union_value(num := 42)::UNION(num INTEGER, str VARCHAR)",
            Parameters::None,
        )?
        .next_chunk()?
        .expect("one result chunk");

    let row = chunk.get_vector_at::<Struct>(0)?.get(0)?.unwrap();
    assert_eq!(row.get::<i32>("value")?, Some(&42));
    assert!(row.get::<String>("value").is_err());
    assert_eq!(row.get::<i32>("empty")?, None);
    assert!(row.get::<String>("empty").is_err());
    let items = row.get::<List<i32>>("items")?.unwrap();
    assert_eq!(items.iter().collect::<Vec<_>>(), [Some(&1), None]);
    assert!(row.get::<List<String>>("items").is_err());

    let row = chunk.get_vector_at::<Union>(1)?.get(0)?.unwrap();
    assert_eq!(row.member(), 0);
    assert_eq!(row.get::<i32>(0)?, Some(&42));
    assert!(row.get::<String>(0).is_err());
    assert_eq!(row.get::<String>(1)?, None);
    assert!(row.get::<i32>(1).is_err());
    Ok(())
}

#[test]
fn vector_decimal_mismatch_reports_parameters() -> crate::Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    let mut result = conn.query("SELECT 1.234::DECIMAL(18, 3)", Parameters::None)?;
    let chunk = result.next_chunk()?.expect("one result chunk");
    let err = chunk
        .get_vector_at::<crate::types::Decimal<18, 2>>(0)
        .expect_err("decimal scale mismatch");
    assert!(err.message.contains("DECIMAL(18,3)"), "{}", err.message);
    Ok(())
}

#[test]
fn vector_valid_write_preserves_absent_validity_mask() -> crate::Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    let mut chunk = DataChunk::create(&conn, &[i32::logical_type(&conn)?], true)?;
    let mut vector = chunk.get_vector_at_mut::<i32>(0)?;
    vector.set_size(1)?;
    assert!(vector.get_view().unwrap().validity().is_none());
    vector.write(0, Some(42))?;
    assert!(vector.get_view().unwrap().validity().is_none());
    assert_eq!(vector.get(0)?, Some(&42));
    vector.write(0, None)?;
    assert!(vector.get_view().unwrap().validity().is_some());
    assert_eq!(vector.get(0)?, None);
    vector.write(0, Some(7))?;
    assert_eq!(vector.get(0)?, Some(&7));
    Ok(())
}

#[test]
fn vector_mutable_view_initializes_and_preserves_validity_mask() -> crate::Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    let mut chunk = DataChunk::create(&conn, &[i32::logical_type(&conn)?], true)?;
    let mut vector = chunk.get_vector_at_mut::<i32>(0)?;
    vector.set_size(65)?;
    for index in 0..vector.len() {
        vector.write(index, Some(index as i32))?;
    }
    assert!(vector.get_view()?.validity().is_none());

    {
        let mut view = vector.get_view_mut()?;
        // SAFETY: Every row contains an initialized INTEGER value.
        let validity = unsafe { view.validity_mut() }.expect("writable validity mask");
        assert_eq!(validity.len(), 2);
        assert_eq!(validity[0], u64::MAX);
        assert_ne!(validity[1] & 1, 0);
        validity[0] &= !1;
        validity[1] &= !1;
    }

    assert_eq!(vector.get(0)?, None);
    assert_eq!(vector.get(63)?, Some(&63));
    assert_eq!(vector.get(64)?, None);

    {
        let mut view = vector.get_view_mut()?;
        // SAFETY: The NULL rows still contain their initialized INTEGER values.
        let validity = unsafe { view.validity_mut() }.expect("writable validity mask");
        assert_eq!(validity[0] & 1, 0);
        assert_eq!(validity[1] & 1, 0);
        validity[0] |= 1;
        validity[1] |= 1;
    }
    assert_eq!(vector.get(0)?, Some(&0));
    assert_eq!(vector.get(64)?, Some(&64));
    Ok(())
}

#[test]
fn vector_mutable_view_guards_do_not_swap_cached_views() -> crate::Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    let mut first_chunk = DataChunk::create(&conn, &[i32::logical_type(&conn)?], true)?;
    let mut second_chunk = DataChunk::create(&conn, &[i32::logical_type(&conn)?], true)?;
    let mut first = first_chunk.get_vector_at_mut::<i32>(0)?;
    let mut second = second_chunk.get_vector_at_mut::<i32>(0)?;
    first.set_size(1)?;
    second.set_size(2)?;
    first.write(0, Some(10))?;
    second.write(0, Some(20))?;
    second.write(1, Some(30))?;
    {
        let mut first_view = first.get_view_mut()?;
        let mut second_view = second.get_view_mut()?;
        std::mem::swap(&mut first_view, &mut second_view);
        assert_eq!(first_view.len(), 2);
        assert_eq!(second_view.len(), 1);
        // SAFETY: Both flat INTEGER buffers are fully initialized.
        unsafe {
            first_view.as_slice_mut().unwrap()[1] = 40;
            second_view.as_slice_mut().unwrap()[0] = 50;
        }
    }
    assert_eq!(first.get(0)?, Some(&50));
    assert_eq!(second.get(0)?, Some(&20));
    assert_eq!(second.get(1)?, Some(&40));
    Ok(())
}

#[test]
fn vector_mutable_view_with_arena_writes_strings() -> crate::Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    let mut chunk = DataChunk::create(&conn, &[String::logical_type(&conn)?], true)?;
    let mut vector = chunk.get_vector_at_mut::<String>(0)?;
    let values = ["", "héllo", "123456789012", "1234567890123", "a longer string with 🦆"];
    vector.set_size(values.len())?;
    {
        let (mut view, arena) = vector.get_view_mut_with_arena()?;
        let data = view.as_mut_ptr();
        for (index, value) in values.iter().enumerate() {
            // SAFETY: Each slot is in bounds and its UTF-8 bytes belong to this vector.
            unsafe { data.add(index).write(arena.add_str(value)?) };
        }
        // SAFETY: Every slot now contains initialized bytes owned by this vector.
        let data = unsafe { view.as_slice_mut() }.unwrap();
        for (encoded, value) in data.iter().zip(values) {
            assert_eq!(encoded.get_data(), value.as_bytes());
            assert_eq!(encoded.size(), value.len());
        }
        // SAFETY: Clearing a VARCHAR row's validity has no descendant requirements.
        unsafe { view.validity_mut() }.unwrap()[0] &= !(1 << 1);
    }
    assert!(vector.heap.is_some());
    for (index, value) in values.iter().enumerate() {
        assert_eq!(vector.get(index)?, (index != 1).then_some(*value));
    }
    Ok(())
}

#[test]
fn vector_mutable_view_with_arena_writes_binary_bytes() -> crate::Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    let mut chunk = DataChunk::create(&conn, &[BlobValue::logical_type(&conn)?], true)?;
    let mut vector = chunk.get_vector_at_mut::<BlobValue>(0)?;
    let values: &[&[u8]] = &[b"", &[0, 255, 128], b"123456789012", &[255; 13]];
    vector.set_size(values.len())?;
    {
        let (mut view, arena) = vector.get_view_mut_with_arena()?;
        let data = view.as_mut_ptr();
        for (index, value) in values.iter().enumerate() {
            // SAFETY: Each slot is in bounds and its BLOB bytes belong to this vector.
            unsafe { data.add(index).write(arena.add_bytes(value)?) };
        }
        // SAFETY: The returned bytes remain in this vector's arena until all reads finish.
        let value = unsafe { super::DuckDBBytes::new(arena, b"direct constructor")? };
        assert_eq!(value.get_data(), b"direct constructor");
    }
    for (index, value) in values.iter().enumerate() {
        assert_eq!(vector.get(index)?, Some(*value));
    }
    Ok(())
}

#[test]
fn vector_byte_writes_store_encoded_values() -> crate::Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    let mut chunk = DataChunk::create(&conn, &[String::logical_type(&conn)?], true)?;
    let mut vector = chunk.get_vector_at_mut::<String>(0)?;
    let values = [
        Some("hello"),
        Some("123456789012"),
        Some("1234567890123"),
        None,
        Some(""),
        Some("a longer string with 🦆"),
    ];
    vector.set_size(values.len())?;
    for (index, value) in values.iter().copied().enumerate() {
        vector.write(index, value)?;
        if let Some(value) = value {
            // SAFETY: This non-NULL slot has been initialized by the write above.
            let encoded = unsafe { &*vector.get_view()?.as_ptr().add(index) };
            assert_eq!(encoded.size(), value.len());
            assert_eq!(encoded.get_data(), value.as_bytes());
        }
    }
    for (index, value) in values.iter().copied().enumerate() {
        assert_eq!(vector.get(index)?, value);
    }
    Ok(())
}

#[test]
fn vector_byte_writes_acquire_arena_only_for_long_values() -> crate::Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    let mut chunk = DataChunk::create(&conn, &[String::logical_type(&conn)?], true)?;
    let mut vector = chunk.get_vector_at_mut::<String>(0)?;
    vector.set_size(4)?;
    vector.write(0, Some(""))?;
    vector.write(1, Some("123456789012"))?;
    vector.write(2, None)?;
    assert!(vector.heap.is_none());
    assert_eq!(vector.get(0)?, Some(""));
    assert_eq!(vector.get(1)?, Some("123456789012"));
    assert_eq!(vector.get(2)?, None);

    vector.write(3, Some("1234567890123"))?;
    assert!(vector.heap.is_some());
    assert_eq!(vector.get(3)?, Some("1234567890123"));
    Ok(())
}

#[test]
fn vector_mutable_view_with_arena_rejects_read_only_vectors() -> crate::Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    let chunk = DataChunk::create(&conn, &[String::logical_type(&conn)?], true)?;
    let mut shared = super::Vector::<String>::from_handle(&chunk.get_vector_handle_at(0), super::Access::Shared)?;
    let error = shared.get_view_mut_with_arena().expect_err("shared input");
    assert!(error.message.contains("not writable"), "{}", error.message);
    assert!(shared.heap.is_none());
    Ok(())
}

#[test]
fn vector_byte_backed_types_expose_arenas() -> crate::Result<()> {
    fn check<T: super::HasDuckDBBytes + DuckDBType>() -> crate::Result<()> {
        let env = Environment::new()?;
        let conn = env.open(StorageLocation::InMemory)?.connect()?;
        let mut chunk = DataChunk::create(&conn, &[T::logical_type(&conn)?], true)?;
        let mut vector = chunk.get_vector_at_mut::<T>(0)?;
        vector.arena()?;
        let (mut view, arena) = vector.get_view_mut_with_arena()?;
        assert!(view.is_empty());
        // SAFETY: The empty vector has no row initialization or descendant requirements.
        assert!(unsafe { view.validity_mut() }.unwrap().is_empty());
        // SAFETY: The encoded value is read before the vector or its arena can change.
        let bytes = unsafe { arena.add_bytes(b"encoded storage")? };
        assert_eq!(bytes.get_data(), b"encoded storage");
        Ok(())
    }

    check::<String>()?;
    check::<BlobValue>()?;
    check::<BitValue>()?;
    check::<BigNum>()?;
    check::<BigNumValue>()?;
    Ok(())
}

#[test]
fn vector_cast_rows_preserves_target_type() -> crate::Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    let mut source = DataChunk::create(&conn, &[i32::logical_type(&conn)?], true)?;
    let mut input = source.get_vector_at_mut::<i32>(0)?;
    input.set_size(2)?;
    input.write(0, Some(42))?;
    input.write(1, None)?;

    let mut strict = DataChunk::create(&conn, &[i64::logical_type(&conn)?], true)?;
    let output: super::Vector<'_, i64> = strict.get_vector_at_mut::<i64>(0)?.cast_rows(&input, &conn)?;
    assert_eq!(output.iter()?.collect::<Vec<_>>(), [Some(&42), None]);

    let mut relaxed = DataChunk::create(&conn, &[i64::logical_type(&conn)?], true)?;
    let (output, error): (super::Vector<'_, i64>, _) =
        relaxed.get_vector_at_mut::<i64>(0)?.try_cast_rows(&input, &conn)?;
    assert!(error.is_none());
    assert_eq!(output.iter()?.collect::<Vec<_>>(), [Some(&42), None]);
    Ok(())
}

#[test]
fn vector_iterator_reports_remaining_rows() -> crate::Result<()> {
    fn assert_iterator_traits<I: ExactSizeIterator + std::iter::FusedIterator>(_: &I) {}

    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    let mut chunk = DataChunk::create(&conn, &[i32::logical_type(&conn)?], true)?;
    let mut vector = chunk.get_vector_at_mut::<i32>(0)?;
    let mut empty = vector.iter()?;
    assert_iterator_traits(&empty);
    assert_eq!(empty.size_hint(), (0, Some(0)));
    assert_eq!(empty.len(), 0);
    assert_eq!(empty.next(), None);

    vector.set_size(2)?;
    vector.write(0, Some(42))?;
    vector.write(1, None)?;
    let mut iter = vector.iter()?;
    assert_iterator_traits(&iter);
    assert_eq!(iter.size_hint(), (2, Some(2)));
    assert_eq!(iter.len(), 2);
    assert_eq!(iter.next(), Some(Some(&42)));
    assert_eq!(iter.size_hint(), (1, Some(1)));
    assert_eq!(iter.len(), 1);
    assert_eq!(iter.next(), Some(None));
    assert_eq!(iter.size_hint(), (0, Some(0)));
    assert_eq!(iter.len(), 0);
    assert_eq!(iter.next(), None);
    assert_eq!(iter.next(), None);
    Ok(())
}

#[test]
fn vector_write_checks_rows_before_dispatch() -> crate::Result<()> {
    use super::{Vector, VectorElement, WritableVectorElement};

    #[derive(Debug)]
    struct Probe;

    // SAFETY: Probe uses INTEGER's physical representation.
    unsafe impl VectorElement for Probe {
        const TYPE_ID: LogicalTypeID = LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER;
        type Internal = i32;
        type Ref<'a> = &'a i32;

        unsafe fn get<'a, U: VectorElement>(vector: &'a Vector<'_, U>, physical: usize, logical: usize) -> Self::Ref<'a>
        where
            Self: 'a,
        {
            // SAFETY: The caller guarantees a valid INTEGER row.
            unsafe { i32::get(vector, physical, logical) }
        }
    }

    // SAFETY: Probe rejects writes without changing the vector.
    unsafe impl WritableVectorElement for Probe {
        type Write<'a> = ();

        unsafe fn write(_vector: &mut Vector<'_, Self>, _index: usize, _value: Option<()>) -> crate::Result<()> {
            Err(Error::invalid_input("element writer called"))
        }
    }

    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    let mut chunk = DataChunk::create(&conn, &[i32::logical_type(&conn)?], true)?;
    {
        let mut vector = chunk.get_vector_at_mut::<Probe>(0)?;
        vector.set_size(1)?;
        assert_eq!(vector.write(0, Some(())).unwrap_err().message, "element writer called");
        for index in [1, usize::MAX] {
            for value in [Some(()), None] {
                let err = vector.write(index, value).unwrap_err();
                assert!(err.message.contains("out of bounds"), "{}", err.message);
            }
        }
    }
    {
        let mut vector = chunk.get_vector_at_mut::<super::Unknown>(0)?;
        let err = vector.write_as::<Probe>(1, Some(())).unwrap_err();
        assert!(err.message.contains("out of bounds"), "{}", err.message);
    }
    chunk
        .get_vector_at_mut::<i32>(0)?
        .make_constant(42_i32.value(&conn)?, 1)?;
    assert_eq!(
        chunk.get_vector_at_mut::<Probe>(0)?.write(0, None).unwrap_err().message,
        "vector is not flat"
    );
    assert_eq!(
        chunk
            .get_vector_at_mut::<super::Unknown>(0)?
            .write_as::<Probe>(0, Some(()))
            .unwrap_err()
            .message,
        "vector is not flat"
    );
    Ok(())
}

#[test]
fn vector_byte_writes_check_bounds_before_allocating() -> crate::Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    let mut chunk = DataChunk::create(&conn, &[String::logical_type(&conn)?], true)?;
    let mut vector = chunk.get_vector_at_mut::<String>(0)?;
    vector.set_size(1)?;
    vector.write(0, None)?;
    let value = "long enough to require an arena allocation";
    for index in [1, usize::MAX] {
        assert!(vector.write(index, Some(value)).is_err());
        assert!(vector.write_as::<String>(index, Some(value)).is_err());
        assert!(vector.write(index, None).is_err());
        assert!(vector.heap.is_none());
        assert_eq!(vector.get(0)?, None);
    }
    vector.write(0, Some(value))?;
    assert_eq!(vector.get(0)?, Some(value));
    Ok(())
}

#[test]
fn vector_nested_writes_check_bounds_before_appending() -> crate::Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    let mut lists = DataChunk::create(&conn, &[Vec::<Option<i32>>::logical_type(&conn)?], true)?;
    let mut list = lists.get_vector_at_mut::<List<i32>>(0)?;
    list.set_size(1)?;
    list.write(0, Some(vec![Some(42)]))?;
    let child_len = list.children().unwrap().row_count()?;
    for index in [1, usize::MAX] {
        assert!(list.write(index, Some(vec![Some(7)])).is_err());
        assert!(list.write(index, None).is_err());
        assert_eq!(list.children().unwrap().row_count()?, child_len);
        assert_eq!(list_values(list.get(0)?), Some(vec![Some(42)]));
    }

    let mut maps = DataChunk::create(&conn, &[MapValue::<i32, i32>::logical_type(&conn)?], true)?;
    let mut map = maps.get_vector_at_mut::<Map<i32, i32>>(0)?;
    map.set_size(1)?;
    map.write(0, Some(HashMap::from([(1, Some(42))])))?;
    let child_len = map.children().unwrap().row_count()?;
    for index in [1, usize::MAX] {
        assert!(map.write(index, Some(HashMap::from([(2, Some(7))]))).is_err());
        assert!(map.write(index, None).is_err());
        assert_eq!(map.children().unwrap().row_count()?, child_len);
        assert_eq!(map.get(0)?.unwrap().iter().collect::<Vec<_>>(), [(Some(&1), Some(&42))]);
    }
    Ok(())
}

#[test]
fn vector_internal_writes_report_non_flat_storage() -> crate::Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    let mut chunk = DataChunk::create(&conn, &[i32::logical_type(&conn)?], true)?;
    let mut vector = chunk
        .get_vector_at_mut::<i32>(0)?
        .make_constant(42_i32.value(&conn)?, 1)?;
    assert_eq!(vector.check_row_writable(0).unwrap_err().message, "vector is not flat");
    assert_eq!(
        vector.set_row_validity(0, true).unwrap_err().message,
        "vector is not flat"
    );
    Ok(())
}

#[test]
fn vector_unknown_reads_nullness() -> crate::Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    let mut result = conn.query("SELECT unnest([42, NULL]::INTEGER[])", Parameters::None)?;
    let mut chunk = result.next_chunk()?.expect("one result chunk");
    let [vector] = chunk.get_vector_array_mut([0])?;
    assert_eq!(vector.get(0)?, Some(()));
    assert_eq!(vector.get(1)?, None);
    assert_eq!(vector.iter()?.collect::<Vec<_>>(), [Some(()), None]);
    Ok(())
}

#[test]
fn vector_unknown_cast_accepts_scalar_and_nested_types() -> crate::Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    let mut result = conn.query("SELECT 42::INTEGER, [1, 2]::INTEGER[]", Parameters::None)?;
    let mut chunk = result.next_chunk()?.expect("one result chunk");
    let [scalar, nested] = chunk.get_vector_array_mut([0, 1])?;
    for vector in [scalar, nested] {
        let borrowed = vector.cast_ref::<super::Unknown>()?;
        assert_eq!(borrowed.get(0)?, Some(()));
        let owned = vector.cast::<super::Unknown>()?;
        assert_eq!(owned.get(0)?, Some(()));
    }
    Ok(())
}

struct TestStruct;

impl StructSchema for TestStruct {
    fn fields<C: FFILink + ?Sized>(link: &C) -> crate::Result<Vec<(&'static str, LogicalType)>> {
        Ok(vec![
            ("key", i32::logical_type(link)?),
            ("value", String::logical_type(link)?),
        ])
    }
}

struct TestUnion;

impl UnionSchema for TestUnion {
    fn members<C: FFILink + ?Sized>(link: &C) -> crate::Result<Vec<(&'static str, LogicalType)>> {
        Ok(vec![
            ("key", i32::logical_type(link)?),
            ("value", String::logical_type(link)?),
        ])
    }
}

scalar_callback!(NegateScalar, i32, |input, output, _ctx, _user_data| {
    let input = input.get_vector_at::<i32>(0)?;
    let mut output = output;
    output.set_size(input.len())?;
    for (i, v) in input.iter()?.enumerate() {
        match v {
            Some(x) => output.write(i, Some(-x))?,
            None => output.write(i, None)?,
        }
    }
    let written: Vec<_> = output.iter()?.map(|value| value.copied()).collect();
    assert_eq!(written, vec![Some(-1), Some(-2), Some(-3), Some(-4), Some(-5)]);
    Ok(())
});

scalar_callback!(UpperScalar, String, |input, output, _ctx, _user_data| {
    let input = input.get_vector_at::<String>(0)?;
    let mut output = output;
    output.set_size(input.len())?;
    for (i, v) in input.iter()?.enumerate() {
        match v {
            Some(s) => output.write(i, Some(&s.to_uppercase()))?,
            None => output.write(i, None)?,
        }
    }
    Ok(())
});

scalar_callback!(
    MapScalar,
    Map<i32, String>,
    |input, result, _ctx, _user_data| {
    let keys = input.get_vector_at::<i32>(0)?;
    let values = input.get_vector_at::<String>(1)?;
    let total_len = keys.len();
    let rows: Vec<_> = keys
        .iter()?
        .zip(values.iter()?)
        .map(|(key, value)| {
            (
                key.copied().expect("key must not be null"),
                value.expect("value must not be null").to_string(),
            )
        })
        .collect();
    let mut result = result;
    result.set_size(total_len)?;
    for (index, (key, value)) in rows.iter().enumerate() {
        result.write(
            index,
            Some(HashMap::from([(*key, Some(value.as_str()))])),
        )?;
    }
    let written: Vec<_> = result.iter()?.collect();
    for (row, (key, value)) in written.into_iter().zip(&rows) {
        assert_eq!(row.as_ref().unwrap().get(key), Some(Some(value.as_str())));
    }
    Ok(())
    }
);

scalar_callback!(UnionScalar, Union, |input, output, _ctx, _user_data| {
    let keys = input.get_vector_at::<i32>(0)?;
    let values = input.get_vector_at::<String>(1)?;
    let rows: Vec<(i32, Option<&str>)> = keys
        .iter()?
        .zip(values.iter()?)
        .map(|(key, value)| (*key.unwrap(), value))
        .collect();

    let mut output = output;
    output.set_size(rows.len())?;

    for (index, (key, value)) in rows.iter().enumerate() {
        match *key {
            1 => output.write(
                index,
                Some(UnionWriter::set_value::<i32>(
                    0u8,
                    value.map_or(None, |v| Some(v.len() as i32)),
                )),
            )?,
            2 => output.write(index, Some(UnionWriter::set_value::<String>(1u8, *value)))?,
            _ => unimplemented!("Unexpected key value: {}", key),
        }
    }
    Ok(())
});

scalar_callback!(StructScalar, Struct, |input, output, _ctx, _user_data| {
    let keys = input.get_vector_at::<i32>(0)?;
    let values = input.get_vector_at::<String>(1)?;
    let rows: Vec<_> = keys
        .iter()?
        .zip(values.iter()?)
        .map(|(key, value)| (*key.unwrap(), value.unwrap().to_string()))
        .collect();
    let mut output = output;
    output.set_size(rows.len())?;
    for (index, (key, value)) in rows.iter().enumerate() {
        output.write(
            index,
            Some(
                StructWrite::default()
                    .field::<i32>(Some(*key))
                    .field::<String>(Some(value)),
            ),
        )?;
    }
    for (row, (key, value)) in output.iter()?.zip(rows) {
        let row = row.unwrap();
        assert_eq!(row.get::<i32>("key")?, Some(&key));
        assert_eq!(row.get::<String>("value")?, Some(value.as_str()));
    }
    Ok(())
});

struct SequenceStruct;

impl StructSchema for SequenceStruct {
    fn fields<C: FFILink + ?Sized>(link: &C) -> crate::Result<Vec<(&'static str, LogicalType)>> {
        Ok(vec![("key", i64::logical_type(link)?)])
    }
}

scalar_callback!(StructKeyScalar, i64, |input, output, _ctx, _user_data| {
    let structs = input.get_vector_at::<Struct>(0)?;
    let mut output = output;
    output.set_size(structs.len())?;
    for (index, row) in structs.iter()?.enumerate() {
        let key = row.map(|row| row.get::<i64>("key")).transpose()?.flatten();
        output.write(index, key.copied())?;
    }
    Ok(())
});

// Writes a NULL row whenever the key is NULL.
scalar_callback!(NullableStructScalar, Struct, |input, output, _ctx, _user_data| {
    let keys = input.get_vector_at::<i32>(0)?;
    let mut output = output;
    output.set_size(keys.len())?;
    for (index, key) in keys.iter()?.enumerate() {
        let row = key.map(|key| {
            StructWrite::default()
                .field::<i32>(Some(*key))
                .field::<String>(Some("x"))
        });
        output.write(index, row)?;
    }
    Ok(())
});

// Writes a NULL row whenever the key is NULL.
scalar_callback!(NullableUnionScalar, Union, |input, output, _ctx, _user_data| {
    let keys = input.get_vector_at::<i32>(0)?;
    let mut output = output;
    output.set_size(keys.len())?;
    for (index, key) in keys.iter()?.enumerate() {
        let row = key.map(|key| UnionWriter::set_value::<i32>(0, Some(*key)));
        output.write(index, row)?;
    }
    Ok(())
});

// Odd keys pick the INTEGER member, even keys the STRUCT member.
scalar_callback!(StructMemberUnionScalar, Union, |input, output, _ctx, _user_data| {
    let keys = input.get_vector_at::<i32>(0)?;
    let mut output = output;
    output.set_size(keys.len())?;
    for (index, key) in keys.iter()?.enumerate() {
        let key = *key.unwrap();
        let row = if key % 2 == 1 {
            UnionWriter::set_value::<i32>(0, Some(key))
        } else {
            UnionWriter::set_value::<Struct>(1, Some(StructWrite::default().field::<String>(Some("even"))))
        };
        output.write(index, Some(row))?;
    }
    Ok(())
});

scalar_callback!(ConstantScalar, i32, |_input, result, ctx, _user_data| {
    let val = 42_i32.value(&ctx)?;
    let result = result.make_constant(val, 10)?;
    assert_eq!(unsafe { result.get_view().unwrap().as_slice() }.unwrap(), &[42]);
    Ok(())
});

scalar_callback!(SequenceScalar, i32, |input, result, _ctx, _user_data| {
    let input_len = input.row_count()?;

    result.make_sequence(42, 10, input_len)?;
    Ok(())
});

scalar_callback!(CopyStringScalar, String, |input, result, _ctx, _user_data| {
    let mut vector = input.get_vector_at_mut::<String>(0)?;
    if vector.storage_kind() == StorageKind::Other {
        vector.flatten()?;
    }
    let mut result = result;
    result.set_size(vector.len())?;
    for (i, item) in vector.iter()?.enumerate() {
        result.write(i, item)?;
    }
    Ok(())
});

scalar_callback!(DictionaryProbeScalar, i32, |input, output, _ctx, _user_data| {
    let mut vector = input.get_vector_at_mut::<i32>(0)?;
    let mut output = output;
    output.set_size(vector.len())?;

    if vector.storage_kind() == StorageKind::Dictionary {
        // Dictionary vectors expose a selection vector mapping each logical
        // row into a shared physical child buffer. Reading through the
        // normal iterator must already honour that indirection.
        let selection = vector.get_view().unwrap().selection().unwrap().to_vec();
        assert_eq!(selection.len(), 2);

        let before: Vec<_> = vector.iter()?.map(|v| v.copied()).collect();
        assert_eq!(before, vec![Some(i32::MAX), None]);

        // Flattening should collapse the dictionary into a flat vector while
        // preserving the logical values.
        vector.flatten()?;
        assert_eq!(vector.storage_kind(), StorageKind::Flat);

        let after: Vec<_> = vector.iter()?.map(|v| v.copied()).collect();
        assert_eq!(after, before);
    } else if vector.storage_kind() == StorageKind::Other {
        // The SEQUENCE-encoded chunk emitted by `test_vector_types` must be
        // flattened before it can be read like the other storage kinds.
        vector.flatten()?;
    }

    for (i, v) in vector.iter()?.enumerate() {
        output.write(i, v.copied())?;
    }
    Ok(())
});

#[test]
pub fn test_vector_dictionary() -> crate::Result<()> {
    let env = Environment::new()?;
    let instance = env.open(StorageLocation::InMemory)?;
    let conn = instance.connect()?;

    ScalarFunctionBuilder::new(
        "dict_probe",
        SignatureBuilder::new()
            .param(FunctionParameter::new::<i32>("in"))
            .return_type(i32::logical_type(&conn)?),
        DictionaryProbeScalar,
    )
    .register(&conn)?;

    // `test_vector_types` feeds its output straight into the projection
    // below without an intervening `Fetch`, so `dict_probe` observes the
    // table function's chunks in their native storage kind: FLAT, CONSTANT,
    // DICTIONARY, then OTHER (SEQUENCE) - in that fixed order. The
    // DICTIONARY chunk is the one asserted on inside `DictionaryProbeScalar`.
    let mut result = conn.query(
        "SELECT dict_probe(test_vector) FROM test_vector_types(NULL::INTEGER)",
        Parameters::None,
    )?;

    let mut values = vec![];

    while let Some(chunk) = result.next_chunk()? {
        let vector = chunk.get_vector_at::<i32>(0)?;
        values.extend(vector.iter()?.map(|v| v.copied()));
    }

    assert_eq!(
        values,
        vec![
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

scalar_callback!(ConstantProbeScalar, i32, |input, output, _ctx, _user_data| {
    let mut vector = input.get_vector_at_mut::<i32>(0)?;
    let mut output = output;
    output.set_size(vector.len())?;

    if vector.storage_kind() == StorageKind::Constant {
        // All-constant inputs run on one row; DuckDB broadcasts the result afterwards.
        assert_eq!(vector.len(), 1);

        let view = vector.get_view().unwrap();
        if let Some(selection) = view.selection() {
            assert!(selection.iter().all(|&index| index == 0));
        }
        assert_eq!(unsafe { view.as_slice() }.unwrap().len(), 1);

        let values: Vec<_> = vector.iter()?.map(|v| v.copied()).collect();
        assert_eq!(values, vec![Some(i32::MIN)]);
    } else if vector.storage_kind() == StorageKind::Other {
        // The SEQUENCE-encoded chunk emitted by `test_vector_types` must be
        // flattened before it can be read like the other storage kinds.
        vector.flatten()?;
    }

    for (i, v) in vector.iter()?.enumerate() {
        output.write(i, v.copied())?;
    }
    Ok(())
});

#[test]
pub fn test_vector_constant() -> crate::Result<()> {
    let env = Environment::new()?;
    let instance = env.open(StorageLocation::InMemory)?;
    let conn = instance.connect()?;

    ScalarFunctionBuilder::new(
        "const_probe",
        SignatureBuilder::new()
            .param(FunctionParameter::new::<i32>("in"))
            .return_type(i32::logical_type(&conn)?),
        ConstantProbeScalar,
    )
    .register(&conn)?;

    // All-constant inputs run on one row; DuckDB broadcasts the result afterwards.
    let mut result = conn.query(
        "SELECT const_probe(test_vector) FROM test_vector_types(NULL::INTEGER)",
        Parameters::None,
    )?;

    let mut values = vec![];

    while let Some(chunk) = result.next_chunk()? {
        let vector = chunk.get_vector_at::<i32>(0)?;
        values.extend(vector.iter()?.map(|v| v.copied()));
    }

    assert_eq!(
        values,
        vec![
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

#[test]
fn test_vector_read_write() -> crate::Result<()> {
    let env = Environment::new().expect("Failed to create environment");
    let instance = env
        .open(StorageLocation::InMemory)
        .expect("Failed to open in-memory database");
    let conn = instance.connect().expect("Failed to connect to database");

    ScalarFunctionBuilder::new(
        "test",
        SignatureBuilder::new()
            .param(FunctionParameter::new::<i32>("input"))
            .return_type(i32::logical_type(&conn)?),
        NegateScalar,
    )
    .register(&conn)
    .expect("Failed to register scalar function");

    let statements = conn
        .parse("SELECT test(unnest([1, 2, 3, 4, 5]))")
        .expect("Failed to parse query");

    let mut chunk_count = 0;
    for stmt in statements {
        let stmt = stmt.expect("Failed to get statement");
        let mut result = conn.query(stmt, Parameters::None).expect("Failed to execute statement");

        loop {
            match result.step().expect("Failed to step result") {
                QueryResultStep::Chunk(chunk) => {
                    chunk_count += 1;
                    let vector = chunk.get_vector_at::<i32>(0).expect("Failed to get vector");

                    let out: Vec<Option<i32>> = vector.iter()?.map(|x| x.copied()).collect();
                    assert_eq!(out, vec![Some(-1), Some(-2), Some(-3), Some(-4), Some(-5)]);
                }
                QueryResultStep::Waiting => continue,
                QueryResultStep::Canceled => panic!("Query canceled unexpectedly"),
                QueryResultStep::Finished => break,
            }
        }
    }
    assert_eq!(chunk_count, 1);
    Ok(())
}

#[test]
fn test_logical_type_cast() -> crate::Result<()> {
    let env = Environment::new()?;
    let instance = env.open(StorageLocation::InMemory)?;
    let conn = instance.connect()?;

    let res = conn.query("SELECT 1", Parameters::None)?.next_chunk()?.unwrap();

    let vector = res.get_vector_at::<i32>(0)?;

    for item in vector.iter()? {
        assert_eq!(item, Some(&1));
    }
    Ok(())
}

#[test]
fn test_vector_string() -> crate::Result<()> {
    let env = Environment::new()?;
    let instance = env.open(StorageLocation::InMemory)?;
    let conn = instance.connect()?;

    ScalarFunctionBuilder::new(
        "to_upper",
        SignatureBuilder::new()
            .param(FunctionParameter::new::<String>("IN"))
            .return_type(String::logical_type(&conn)?),
        UpperScalar,
    )
    .register(&conn)?;

    let res = conn
        .query(
            "SELECT to_upper(unnest(['hello', 'world', '123456789012', 'longerthaninline']))",
            Parameters::None,
        )?
        .next_chunk()?
        .unwrap();

    let vector = res.get_vector_at::<String>(0)?;
    let data: Vec<_> = vector.iter()?.collect();

    assert!(data.len() == 4, "Expected 4 rows, got {}", data.len());

    assert_eq!(
        data,
        vec![
            Some("HELLO"),
            Some("WORLD"),
            Some("123456789012"),
            Some("LONGERTHANINLINE"),
        ],
    );
    Ok(())
}

#[test]
fn test_vector_list() -> crate::Result<()> {
    scalar_callback!(ListMultScalar, List<i32>, |input, output, _ctx, _user_data| {
        let input = input.get_vector_at::<List<i32>>(0)?;
        let mut output = output;
        output.set_size(input.len())?;
        for (i, v) in input.iter()?.enumerate() {
            match v {
                Some(list) => {
                    let new_list: Vec<Option<i32>> = list.iter().map(|x| x.map(|v| v * 2)).collect();
                    output.write(i, Some(new_list))?;
                }
                None => output.write(i, None)?,
            }
        }
        Ok(())
    });

    let env = Environment::new()?;
    let instance = env.open(StorageLocation::InMemory)?;
    let conn = instance.connect()?;

    let list_logical_type = Vec::<Option<i32>>::logical_type(&conn)?;

    ScalarFunctionBuilder::new(
        "list_mult",
        SignatureBuilder::new()
            .param(FunctionParameter::with_type("IN", list_logical_type.clone()))
            .return_type(list_logical_type),
        ListMultScalar,
    )
    .register(&conn)?;

    let res = conn
        .query(
            "SELECT list_mult(unnest([[1, 2], [3, NULL, 5], [], NULL]))",
            Parameters::None,
        )?
        .next_chunk()?
        .unwrap();

    let vector = res.get_vector_at::<List<i32>>(0)?;

    let items: Vec<_> = vector
        .iter()?
        .map(|r| r.map(|v| v.iter().collect::<Vec<_>>()))
        .collect();

    let expected = [
        Some(vec![Some(&2), Some(&4)]),
        Some(vec![Some(&6), None, Some(&10)]),
        Some(vec![]),
        None,
    ];

    assert_eq!(items, expected);
    Ok(())
}

#[test]
fn test_vector_struct() -> crate::Result<()> {
    let env = Environment::new()?;
    let instance = env.open(StorageLocation::InMemory)?;
    let conn = instance.connect()?;

    let res = conn
        .query(
            "SELECT unnest([{'key1': 'value1', 'key2': 42}, {'key1': NULL, 'key2': NULL}])",
            Parameters::None,
        )?
        .next_chunk()?
        .unwrap();

    let vector = res.get_vector_at::<Struct>(0)?;

    let mut reader = vector.iter()?;

    let row = reader.next().unwrap().unwrap();

    assert_eq!(row.get::<String>("key1")?, Some("value1"));
    assert_eq!(row.get::<i32>("key2")?, Some(&42));

    let row = reader.next().unwrap().unwrap();
    assert_eq!(row.get::<String>("key1")?, None);
    assert_eq!(row.get::<i32>("key2")?, None);
    assert!(reader.next().is_none());
    Ok(())
}

#[test]
pub fn test_vector_array() -> crate::Result<()> {
    let env = Environment::new()?;
    let instance = env.open(StorageLocation::InMemory)?;
    let conn = instance.connect()?;

    let res = conn
        .query(
            "SELECT unnest([array_value(1, NULL, 3), array_value(3, 4, 5), NULL])",
            Parameters::None,
        )?
        .next_chunk()?
        .unwrap();

    let vector = res.get_vector_at::<Array<i32>>(0)?;
    let mut reader = vector.iter()?;

    let item = reader.next().unwrap().unwrap();
    assert_eq!(item.len(), 3);

    let reader = vector.iter()?;

    let items: Vec<_> = reader.map(|r| r.map(|v| v.iter().collect::<Vec<_>>())).collect();

    let expected = [
        Some(vec![Some(&1), None, Some(&3)]),
        Some(vec![Some(&3), Some(&4), Some(&5)]),
        None,
    ];

    assert_eq!(items, expected);
    Ok(())
}

#[test]
pub fn test_vector_union() -> crate::Result<()> {
    let env = Environment::new()?;
    let instance = env.open(StorageLocation::InMemory)?;
    let conn = instance.connect()?;

    let mut statements = conn.parse(
        "CREATE TABLE tbl1 (u UNION(num INTEGER, str VARCHAR));
        INSERT INTO tbl1 VALUES (1), (42), (NULL), ('two'), (union_value(str := 'three'));
        SELECT u FROM tbl1;",
    )?;

    conn.execute(statements.next().unwrap()?, Parameters::None)?;
    conn.execute(statements.next().unwrap()?, Parameters::None)?;

    let res = conn
        .query(statements.next().unwrap()?, Parameters::None)?
        .next_chunk()?
        .unwrap();

    let vector = res.get_vector_at::<Union>(0)?;
    assert_eq!(vector.len(), 5);
    let mut reader = vector.iter()?;

    let first = reader.next().unwrap().unwrap();
    assert_eq!(first.member(), 0, "Expected first union member to be 0 (num)");
    assert_eq!(first.get::<i32>(0)?, Some(&1));

    assert_eq!(reader.next().unwrap().unwrap().get::<i32>(0)?, Some(&42));

    assert!(reader.next().unwrap().is_none());
    assert_eq!(reader.next().unwrap().unwrap().get::<String>(1)?, Some("two"));

    let last = reader.next().unwrap().unwrap();
    assert_eq!(last.get::<String>(1)?, Some("three"));
    // The wrong member type must be rejected.
    assert_eq!(
        last.get::<i32>(1).unwrap_err().code,
        DuckDBError::DUCKDB_V2_ERROR_INPUT_INVALID
    );
    Ok(())
}

#[test]
pub fn test_vector_map() -> crate::Result<()> {
    let env = Environment::new()?;
    let instance = env.open(StorageLocation::InMemory)?;
    let conn = instance.connect()?;

    let mut result = conn.query(
        "SELECT unnest([MAP {1: 12.1, 2: 41.2}, MAP {1: 112.1, 2: 141.2}, MAP {1: NULL, 2: 41.2}]);",
        Parameters::None,
    )?;
    let res = result.next_chunk()?.unwrap();

    assert_eq!(res.col_count(), 1);

    let vector = res.get_vector_at::<Map<i32, ShortDecimal>>(0)?;
    let mut reader = vector.iter()?;

    assert_eq!(vector.len(), 3);

    let row = reader.next().unwrap().unwrap();

    let hmap = row.to_hash_map()?;

    assert_eq!(hmap.get(&1).unwrap(), &Some(&ShortDecimal(121)));
    assert_eq!(hmap.get(&2).unwrap(), &Some(&ShortDecimal(412)));

    assert_eq!(row.keys(), vec![Some(&1), Some(&2)]);
    assert_eq!(row.values(), vec![Some(&ShortDecimal(121)), Some(&ShortDecimal(412))]);

    assert_eq!(row.get(&1), Some(Some(&ShortDecimal(121))));
    assert_eq!(row.get(&2), Some(Some(&ShortDecimal(412))));

    let row = reader.next().unwrap().unwrap();

    assert_eq!(row.get(&1), Some(Some(&ShortDecimal(1121))));
    assert_eq!(row.get(&2), Some(Some(&ShortDecimal(1412))));

    let row = reader.next().unwrap().unwrap();

    let hmap = row.to_hash_map()?;

    assert_eq!(hmap.get(&1), Some(&None));
    assert_eq!(hmap.get(&2), Some(&Some(&ShortDecimal(412))));

    assert_eq!(row.get(&1), Some(None));
    assert_eq!(row.get(&2), Some(Some(&ShortDecimal(412))));
    assert_eq!(row.values(), vec![None, Some(&ShortDecimal(412))]);

    assert!(reader.next().is_none());

    Ok(())
}

#[test]
pub fn vector_complex_write() -> crate::Result<()> {
    let env = Environment::new()?;
    let instance = env.open(StorageLocation::InMemory)?;
    let conn = instance.connect()?;

    let ltype = MapValue::<i32, String>::logical_type(&conn)?;

    ScalarFunctionBuilder::new(
        "to_map",
        SignatureBuilder::new()
            .param(FunctionParameter::new::<i32>("key"))
            .param(FunctionParameter::new::<String>("value"))
            .return_type(ltype),
        MapScalar,
    )
    .register(&conn)?;

    let mut statements = conn.parse(
        "SELECT UNNEST([to_map(12, 'AA'), to_map(15, 'BB')]); SELECT to_map(unnest([1,2,3]), unnest(['A', 'B', 'C']))",
    )?;

    let statement = statements.next().unwrap()?;

    let mut result = conn.query(statement, Parameters::None)?;

    let mut chunk_count = 0;
    while let Some(item) = result.next_chunk()? {
        chunk_count += 1;
        let res = item.get_vector_at::<Map<i32, String>>(0)?;

        assert!(res.len() == 2);
        let mut reader = res.iter()?;

        let item = reader.next().unwrap().unwrap();

        assert_eq!(item.get(&12), Some(Some("AA")));

        let item = reader.next().unwrap().unwrap();

        assert_eq!(item.get(&15), Some(Some("BB")));
    }
    assert_eq!(chunk_count, 1);

    let statement = statements.next().unwrap()?;

    let mut result = conn.query(statement, Parameters::None)?;

    if let Some(item) = result.next_chunk()? {
        let res = item.get_vector_at::<Map<i32, String>>(0)?;

        assert_eq!(res.len(), 3);
        let expected = [(1, "A"), (2, "B"), (3, "C")];

        for (item, (key, value)) in res.iter()?.zip(expected) {
            assert_eq!(item.unwrap().get(&key), Some(Some(value)));
        }

        return Ok(());
    }
    Err(Error::api_error("Not found"))
}

#[test]
pub fn vector_union_write() -> crate::Result<()> {
    let env = Environment::new()?;
    let instance = env.open(StorageLocation::InMemory)?;
    let conn = instance.connect()?;

    let union_type = UnionValue::<TestUnion, i32>::logical_type(&conn)?;

    ScalarFunctionBuilder::new(
        "to_union",
        SignatureBuilder::new()
            .param(FunctionParameter::new::<i32>("key"))
            .param(FunctionParameter::new::<String>("value"))
            .return_type(union_type),
        UnionScalar,
    )
    .register(&conn)?;

    let statement = conn
        .parse("SELECT to_union(unnest([1, 2, 2]), unnest(['WWWADWWWAample', 'OPAOPDAOADWADtablesss', NULL]))")?
        .next()
        .unwrap()?;
    let mut result = conn.query(statement, Parameters::None)?;
    let mut chunk_count = 0;
    while let Some(mut chunk) = result.next_chunk()? {
        chunk_count += 1;
        let mut vector = chunk.get_vector_at_mut::<Union>(0)?;
        vector.flatten()?;
        let rows: Vec<_> = vector.iter()?.collect();

        assert_eq!(rows[0].as_ref().unwrap().member(), 0);
        assert_eq!(rows[1].as_ref().unwrap().member(), 1);

        assert_eq!(rows[0].as_ref().unwrap().get::<i32>(0)?, Some(&14));
        assert_eq!(
            rows[1].as_ref().unwrap().get::<String>(1)?,
            Some("OPAOPDAOADWADtablesss")
        );

        assert_eq!(rows[2].as_ref().unwrap().member(), 1);
        assert_eq!(rows[2].as_ref().unwrap().get::<String>(1)?, None);
    }
    assert_eq!(chunk_count, 1);
    Ok(())
}

#[test]
pub fn vector_struct_write() -> crate::Result<()> {
    let env = Environment::new()?;
    let instance = env.open(StorageLocation::InMemory)?;
    let conn = instance.connect()?;

    let struct_type = StructValue::<TestStruct>::logical_type(&conn)?;

    ScalarFunctionBuilder::new(
        "to_struct",
        SignatureBuilder::new()
            .param(FunctionParameter::new::<i32>("key"))
            .param(FunctionParameter::new::<String>("value"))
            .return_type(struct_type),
        StructScalar,
    )
    .register(&conn)?;

    let statement = conn
        .parse("SELECT to_struct(unnest([1, 2]), unnest(['A', 'B']))")?
        .next()
        .unwrap()?;
    let mut result = conn.query(statement, Parameters::None)?;
    let mut chunk_count = 0;
    while let Some(chunk) = result.next_chunk()? {
        chunk_count += 1;
        let vector = chunk.get_vector_at::<Struct>(0)?;
        let rows: Vec<_> = vector.iter()?.collect();
        assert_eq!(rows[0].as_ref().unwrap().get::<i32>("key")?, Some(&1));
        assert_eq!(rows[1].as_ref().unwrap().get::<String>("value")?, Some("B"));
    }
    assert_eq!(chunk_count, 1);
    Ok(())
}

#[test]
pub fn vector_nested_null_write() -> crate::Result<()> {
    let env = Environment::new()?;
    let instance = env.open(StorageLocation::InMemory)?;
    let conn = instance.connect()?;

    let key = || FunctionParameter::new::<i32>("key");
    ScalarFunctionBuilder::new(
        "to_struct_or_null",
        SignatureBuilder::new()
            .param(key())
            .return_type(StructValue::<TestStruct>::logical_type(&conn)?),
        NullableStructScalar,
    )
    .register(&conn)?;
    ScalarFunctionBuilder::new(
        "to_union_or_null",
        SignatureBuilder::new()
            .param(key())
            .return_type(UnionValue::<TestUnion, i32>::logical_type(&conn)?),
        NullableUnionScalar,
    )
    .register(&conn)?;

    // A NULL row's children must read as NULL too, including through field extraction.
    let mut result = conn.query(
        "SELECT s.key, s.value, union_tag(u)::VARCHAR, union_extract(u, 'key')
         FROM (SELECT to_struct_or_null(k) AS s, to_union_or_null(k) AS u FROM (VALUES (1), (NULL), (3)) t(k))",
        Parameters::None,
    )?;
    let chunk = result.next_chunk()?.unwrap();
    let keys: Vec<_> = chunk.get_vector_at::<i32>(0)?.iter()?.map(|v| v.copied()).collect();
    assert_eq!(keys, vec![Some(1), None, Some(3)]);
    let values: Vec<_> = chunk.get_vector_at::<String>(1)?.iter()?.collect();
    assert_eq!(values, vec![Some("x"), None, Some("x")]);
    let tags: Vec<_> = chunk.get_vector_at::<String>(2)?.iter()?.collect();
    assert_eq!(tags, vec![Some("key"), None, Some("key")]);
    let members: Vec<_> = chunk.get_vector_at::<i32>(3)?.iter()?.map(|v| v.copied()).collect();
    assert_eq!(members, vec![Some(1), None, Some(3)]);
    Ok(())
}

#[test]
pub fn vector_union_inactive_struct_member_is_null() -> crate::Result<()> {
    let env = Environment::new()?;
    let instance = env.open(StorageLocation::InMemory)?;
    let conn = instance.connect()?;

    ScalarFunctionBuilder::new(
        "to_union_struct",
        SignatureBuilder::new()
            .param(FunctionParameter::new::<i32>("key"))
            .return_type(LogicalType::from_text(
                &conn,
                "UNION(num INTEGER, s STRUCT(x VARCHAR))",
            )?),
        StructMemberUnionScalar,
    )
    .register(&conn)?;

    // Field extraction reads the struct's children directly, so an inactive member needs NULL descendants.
    let mut result = conn.query(
        "SELECT count(*) FILTER (WHERE k % 2 = 1 AND union_extract(u, 's').x IS NOT NULL),
                count(*) FILTER (WHERE k % 2 = 0 AND union_extract(u, 's').x = 'even')
         FROM (SELECT k, to_union_struct(k::INTEGER) AS u FROM range(5000) t(k))",
        Parameters::None,
    )?;
    let chunk = result.next_chunk()?.unwrap();
    assert_eq!(chunk.get_vector_at::<i64>(0)?.get(0)?, Some(&0));
    assert_eq!(chunk.get_vector_at::<i64>(1)?.get(0)?, Some(&2500));
    Ok(())
}

#[test]
pub fn vector_struct_unreadable_child() -> crate::Result<()> {
    let env = Environment::new()?;
    let instance = env.open(StorageLocation::InMemory)?;
    let conn = instance.connect()?;

    ScalarFunctionBuilder::new(
        "struct_key",
        SignatureBuilder::new()
            .param(FunctionParameter::new::<StructValue<'_, SequenceStruct>>("s"))
            .return_type(i64::logical_type(&conn)?),
        StructKeyScalar,
    )
    .register(&conn)?;

    // struct_pack keeps range()'s SEQUENCE child, which must not read as NULL.
    let mut result = conn.query("SELECT struct_key({'key': i}) FROM range(3) t(i)", Parameters::None)?;
    let err = result.next_chunk().expect_err("unflattened child must be rejected");
    assert!(err.message.contains("OTHER vectors must be flattened before reading"));
    Ok(())
}

#[test]
pub fn vector_test_bignum() -> crate::Result<()> {
    let env = Environment::new()?;
    let instance = env.open(StorageLocation::InMemory)?;
    let conn = instance.connect()?;

    let mut result = conn.query(
        "select unnest([1090812098190281092901::BIGNUM, 1090812098190281092902::BIGNUM, -1090812098190281092903::BIGNUM])",
        Parameters::None,
    )?;

    let mut chunk_count = 0;
    while let Some(item) = result.next_chunk()? {
        chunk_count += 1;
        let res = item.get_vector_at::<BigNum>(0)?;

        assert!(res.len() == 3);
        let reader = res.iter()?;

        let expected = [
            Some("1090812098190281092901"),
            Some("1090812098190281092902"),
            Some("-1090812098190281092903"),
        ];

        for (i, item) in reader.enumerate() {
            let decoded = item.unwrap().decode()?;

            let result = decoded.to_string();

            assert_eq!(Some(result.as_str()), expected[i]);
        }
    }
    assert_eq!(chunk_count, 1);

    Ok(())
}

#[test]
pub fn vector_value_types() -> crate::Result<()> {
    let env = Environment::new()?;
    let instance = env.open(StorageLocation::InMemory)?;
    let conn = instance.connect()?;

    macro_rules! assert_round_trip {
        ($value:expr, $type:ty) => {{
            let input = $value;
            let mut result = conn.query("SELECT $1", Parameters::positional(&[&input]))?;
            let chunk = result.next_chunk()?.unwrap();
            let vector = chunk.get_vector_at::<$type>(0)?;
            assert_eq!(vector.get(0)?, Some(&input));
            drop(chunk);
            drop(result);
        }};
    }

    assert_round_trip!(DateValue(-1), DateValue);
    assert_round_trip!(TimeValue(1), TimeValue);
    assert_round_trip!(TimeNsValue(2), TimeNsValue);
    assert_round_trip!(TimeTzValue(3), TimeTzValue);
    assert_round_trip!(TimestampValue(-4), TimestampValue);
    assert_round_trip!(TimestampSecValue(-5), TimestampSecValue);
    assert_round_trip!(TimestampMsValue(-6), TimestampMsValue);
    assert_round_trip!(TimestampNsValue(-7), TimestampNsValue);
    assert_round_trip!(TimestampTzValue(-8), TimestampTzValue);
    assert_round_trip!(TimestampTzNsValue(-9), TimestampTzNsValue);
    assert_round_trip!(UuidValueRaw(i128::MIN + 10), UuidValueRaw);
    assert_round_trip!(
        IntervalValue {
            months: -1,
            days: 2,
            micros: -3,
        },
        IntervalValue
    );

    let mut result = conn.query(
        "SELECT $1",
        Parameters::positional(&[&DecimalValue::new(-123_456i64, 18, 3)]),
    )?;
    let chunk = result.next_chunk()?.unwrap();
    let vector = chunk.get_vector_at::<LongDecimal>(0)?;
    assert_eq!(vector.get(0)?, Some(&LongDecimal(-123_456)));
    drop(chunk);
    drop(result);

    let blob = BlobValue(vec![0_u8, 1, 255]);
    let mut result = conn.query("SELECT $1", Parameters::positional(&[&blob]))?;
    let chunk = result.next_chunk()?.unwrap();
    let vector = chunk.get_vector_at::<BlobValue>(0)?;
    assert_eq!(vector.get(0)?, Some(blob.0.as_slice()));
    drop(chunk);
    drop(result);

    let bit = BitValue(vec![3_u8, 0b1111_0101]);
    let mut result = conn.query("SELECT $1", Parameters::positional(&[&bit]))?;
    let chunk = result.next_chunk()?.unwrap();
    let vector = chunk.get_vector_at::<BitValue>(0)?;
    assert_eq!(vector.get(0)?, Some(bit.0.as_slice()));
    drop(chunk);
    drop(result);

    let bignum = BigNumValue {
        is_negative: true,
        magnitude: vec![1, 2, 3, 4, 5],
    };
    let mut result = conn.query("SELECT $1", Parameters::positional(&[&bignum]))?;
    let chunk = result.next_chunk()?.unwrap();
    let vector = chunk.get_vector_at::<BigNumValue>(0)?;
    let decoded = vector.get(0)?.unwrap().decode()?;
    assert_eq!(decoded.is_negative, bignum.is_negative);
    assert_eq!(decoded.magnitude, bignum.magnitude);

    Ok(())
}

#[test]
pub fn vector_writable_value_types() -> crate::Result<()> {
    let env = Environment::new()?;
    let instance = env.open(StorageLocation::InMemory)?;
    let conn = instance.connect()?;

    macro_rules! assert_copy_write {
        ($type:ty, $value:expr) => {{
            let mut chunk = DataChunk::create(&conn, &[<$type>::logical_type(&conn)?], true)?;
            let mut vector = chunk.get_vector_at_mut::<$type>(0)?;
            vector.set_size(2)?;
            let value = $value;
            vector.write(0, Some(value))?;
            vector.write(1, None)?;
            assert_eq!(vector.get(0)?, Some(&value));
            assert_eq!(vector.get(1)?, None);
        }};
    }

    assert_copy_write!(DateValue, DateValue(-1));
    assert_copy_write!(TimeValue, TimeValue(1));
    assert_copy_write!(TimeNsValue, TimeNsValue(2));
    assert_copy_write!(TimeTzValue, TimeTzValue(3));
    assert_copy_write!(TimestampValue, TimestampValue(-4));
    assert_copy_write!(TimestampSecValue, TimestampSecValue(-5));
    assert_copy_write!(TimestampMsValue, TimestampMsValue(-6));
    assert_copy_write!(TimestampNsValue, TimestampNsValue(-7));
    assert_copy_write!(TimestampTzValue, TimestampTzValue(-8));
    assert_copy_write!(TimestampTzNsValue, TimestampTzNsValue(-9));
    assert_copy_write!(UuidValueRaw, UuidValueRaw(i128::MIN + 10));
    assert_copy_write!(
        IntervalValue,
        IntervalValue {
            months: -1,
            days: 2,
            micros: -3,
        }
    );

    let mut chunk = DataChunk::create(
        &conn,
        &[DecimalSignature { width: 18, scale: 3 }.logical_type(&conn)?],
        true,
    )?;
    assert!(chunk.get_vector_at::<WordDecimal>(0).is_err());
    assert!(chunk.get_vector_at::<i64>(0).is_err());
    let mut vector = chunk.get_vector_at_mut::<LongDecimal>(0)?;
    vector.set_size(2)?;
    vector.write(0, Some(LongDecimal(-123_456)))?;
    vector.write(1, None)?;
    assert_eq!(vector.get(0)?, Some(&LongDecimal(-123_456)));
    assert_eq!(vector.get(1)?, None);

    let mut chunk = DataChunk::create(&conn, &[BlobValue::logical_type(&conn)?], true)?;
    let mut vector = chunk.get_vector_at_mut::<BlobValue>(0)?;
    vector.set_size(2)?;
    vector.write(0, Some(&[0_u8, 1, 255]))?;
    vector.write(1, None)?;
    assert_eq!(vector.get(0)?, Some([0_u8, 1, 255].as_slice()));
    assert_eq!(vector.get(1)?, None);

    let mut chunk = DataChunk::create(&conn, &[BitValue::logical_type(&conn)?], true)?;
    let mut vector = chunk.get_vector_at_mut::<BitValue>(0)?;
    vector.set_size(2)?;
    assert!(vector.write(0, Some(&[])).is_err());
    assert!(vector.write(0, Some(&[16, 0, 0, 0])).is_err());
    assert!(vector.write(0, Some(&[3, 0b0001_0101])).is_err());
    vector.write(0, Some(&[3_u8, 0b1111_0101]))?;
    vector.write(1, None)?;
    assert_eq!(vector.get(0)?, Some([3_u8, 0b1111_0101].as_slice()));
    assert_eq!(vector.get(1)?, None);

    let bignum = BigNumValue {
        is_negative: true,
        magnitude: vec![1, 2, 3, 4, 5],
    };
    let mut chunk = DataChunk::create(&conn, &[BigNumValue::logical_type(&conn)?], true)?;
    let mut vector = chunk.get_vector_at_mut::<BigNumValue>(0)?;
    vector.set_size(2)?;
    vector.write(0, Some(&bignum))?;
    vector.write(1, None)?;
    assert_eq!(vector.get(0)?.unwrap().decode()?, bignum);
    assert!(vector.get(1)?.is_none());

    let mut source = conn.query("SELECT 12345678901234567890::BIGNUM", Parameters::None)?;
    let source_chunk = source.next_chunk()?.unwrap();
    let source_vector = source_chunk.get_vector_at::<BigNum>(0)?;
    let borrowed = source_vector.get(0)?.unwrap();
    let mut chunk = DataChunk::create(&conn, &[BigNum::logical_type(&conn)?], true)?;
    let mut vector = chunk.get_vector_at_mut::<BigNum>(0)?;
    vector.set_size(1)?;
    vector.write(0, Some(borrowed))?;
    assert_eq!(vector.get(0)?.unwrap().decode()?.to_string(), "12345678901234567890");

    let mut chunk = DataChunk::create(&conn, &[<[i32; 3]>::logical_type(&conn)?], true)?;
    let mut vector = chunk.get_vector_at_mut::<Array<i32>>(0)?;
    vector.set_size(2)?;
    vector.write(1, None)?;
    assert_eq!(vector.children().unwrap().get_untyped_vector_at(0)?.len(), 6);
    assert!(vector.write(0, Some(vec![Some(1), Some(2)])).is_err());
    vector.write(0, Some(vec![Some(1), None, Some(3)]))?;
    assert_eq!(
        vector
            .get(0)?
            .unwrap()
            .iter()
            .map(|value| value.copied())
            .collect::<Vec<_>>(),
        vec![Some(1), None, Some(3)]
    );
    assert!(vector.get(1)?.is_none());

    Ok(())
}

#[test]
pub fn test_vector_make_constant() -> crate::Result<()> {
    let env = Environment::new()?;
    let instance = env.open(StorageLocation::InMemory)?;
    let conn = instance.connect()?;

    ScalarFunctionBuilder::new(
        "to_constant",
        SignatureBuilder::new()
            .param(FunctionParameter::new::<i32>("in"))
            .return_type(i32::logical_type(&conn)?),
        ConstantScalar,
    )
    .register(&conn)?;

    let mut statements = conn.parse("SELECT to_constant(unnest([1,2,3]));")?;

    let statement = statements.next().unwrap()?;

    assert!(statements.next().is_none());

    let mut result = conn.query(statement, Parameters::None)?;

    let mut chunk_count = 0;
    while let Some(item) = result.next_chunk()? {
        chunk_count += 1;
        let res = item.get_vector_at::<i32>(0)?;

        assert!(res.len() == 3);
        let mut reader = res.iter()?;

        assert_eq!(reader.next(), Some(Some(&42)));
        assert_eq!(reader.next(), Some(Some(&42)));
        assert_eq!(reader.next(), Some(Some(&42)));
        assert_eq!(reader.next(), None);
    }
    assert_eq!(chunk_count, 1);

    Ok(())
}

#[test]
pub fn test_vector_make_sequence() -> crate::Result<()> {
    let env = Environment::new()?;
    let instance = env.open(StorageLocation::InMemory)?;
    let conn = instance.connect()?;

    ScalarFunctionBuilder::new(
        "to_sequence",
        SignatureBuilder::new()
            .param(FunctionParameter::new::<i32>("in"))
            .return_type(i32::logical_type(&conn)?),
        SequenceScalar,
    )
    .register(&conn)?;

    let mut statements = conn.parse("SELECT to_sequence(unnest([1,2,3]));")?;

    let statement = statements.next().unwrap()?;

    assert!(statements.next().is_none());

    let mut result = conn.query(statement, Parameters::None)?;

    let mut chunk_count = 0;
    while let Some(item) = result.next_chunk()? {
        chunk_count += 1;
        let res = item.get_vector_at::<i32>(0)?;

        assert!(res.len() == 3);
        let mut reader = res.iter()?;

        assert_eq!(reader.next(), Some(Some(&42)));
        assert_eq!(reader.next(), Some(Some(&52)));
        assert_eq!(reader.next(), Some(Some(&62)));
        assert_eq!(reader.next(), None);
    }
    assert_eq!(chunk_count, 1);

    Ok(())
}

#[test]
pub fn test_vector_types() -> crate::Result<()> {
    let env = Environment::new()?;
    let instance = env.open(StorageLocation::InMemory)?;
    let conn = instance.connect()?;

    ScalarFunctionBuilder::new(
        "test",
        SignatureBuilder::new()
            .param(FunctionParameter::new::<String>("in"))
            .return_type(String::logical_type(&conn)?),
        CopyStringScalar,
    )
    .register(&conn)?;

    let mut result = conn.query(
        r#"SELECT test(x) from test_vector_types(null::VARCHAR) as t(x);"#,
        Parameters::None,
    )?;

    let mut concatenated = vec![];

    while let Some(item) = result.next_chunk()? {
        let vector = item.get_vector_at::<String>(0)?;

        concatenated.extend(vector.iter()?.map(|x| x.map(str::to_string)));
    }

    assert_eq!(
        concatenated,
        vec![
            Some("🦆🦆🦆🦆🦆🦆".to_string()),
            Some("goo\0se".to_string()),
            None,
            Some("🦆🦆🦆🦆🦆🦆".to_string()),
            Some("🦆🦆🦆🦆🦆🦆".to_string()),
            Some("🦆🦆🦆🦆🦆🦆".to_string()),
            Some("goo\0se".to_string()),
            None,
            Some("🦆🦆🦆🦆🦆🦆".to_string()),
            Some("goo\0se".to_string()),
            None
        ]
    );

    Ok(())
}

#[test]
pub fn test_vector_set_value() -> crate::Result<()> {
    scalar_callback!(ToVariant, Variant, |input, output, ctx, _user_data| {
        let mut output = output;

        output.set_size(input.row_count()?)?;
        let vector = input.get_untyped_vector_at(0)?;
        for index in 0..vector.len() {
            let value = if !vector.is_null(index)? {
                match vector.logical_type().id()? {
                    LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER => {
                        Some(vector.cast_ref::<i32>()?.get(index)?.unwrap().value(ctx)?)
                    }
                    LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_BOOLEAN => {
                        Some(vector.cast_ref::<bool>()?.get(index)?.unwrap().value(ctx)?)
                    }
                    _ => None,
                }
            } else {
                None
            };

            if let Some(value) = value {
                let lt = LogicalType::from_text(ctx, "VARIANT")?;
                let result = value.cast(ctx, &lt)?;
                output.write_value_slow(index, result)?;
            } else {
                output.set_null_slow(index)?;
            }
        }

        Ok(())
    });

    let env = Environment::new()?;
    let instance = env.open(StorageLocation::InMemory)?;
    let conn = instance.connect()?;

    let rt = LogicalType::from_text(&conn, "VARIANT")?;

    ScalarFunctionBuilder::new(
        "to_variant",
        SignatureBuilder::new()
            .param(FunctionParameter::new::<Any>("in"))
            .return_type(rt),
        ToVariant,
    )
    .register(&conn)?;

    for (column, expected) in [
        ("bool", ["false", "true", "NULL"]),
        ("\"int\"", ["-2147483648", "2147483647", "NULL"]),
    ] {
        let mut result = conn.query(
            format!("SELECT to_variant({column}) FROM test_all_types()"),
            Parameters::None,
        )?;
        let mut results = Vec::new();
        while let Some(chunk) = result.next_chunk()? {
            let vector = chunk.get_vector_at::<Variant>(0)?;
            for row in vector.iter()? {
                results.push(match row {
                    None => "NULL".to_owned(),
                    Some(value) => value.dbg_string()?,
                });
            }
        }
        assert_eq!(results, expected, "{column}");
    }

    Ok(())
}

scalar_callback!(RefScalar, String, |input, output, _ctx, _user_data| {
    let input = input.get_vector_at::<String>(0)?;
    let output = output;

    // SAFETY: DuckDB keeps the callback input alive while consuming the
    // referenced output, and neither vector is accessed concurrently here.
    let referenced = unsafe { output.reference(input)? };
    // Writes would land in DuckDB's read-only input, so the reference is read-only too.
    assert!(!referenced.is_writable());

    Ok(())
});

#[test]
pub fn test_vector_reference_input() -> crate::Result<()> {
    let env = Environment::new()?;
    let instance = env.open(StorageLocation::InMemory)?;
    let conn = instance.connect()?;

    ScalarFunctionBuilder::new(
        "reference",
        SignatureBuilder::new()
            .param(FunctionParameter::new::<String>("in"))
            .return_type(String::logical_type(&conn)?),
        RefScalar,
    )
    .register(&conn)?;

    let mut result = conn.query(
        r#"SELECT reference(x) from test_vector_types(null::VARCHAR) as t(x);"#,
        Parameters::None,
    )?;

    let mut results: Vec<String> = Vec::new();

    while let Some(item) = result.next_chunk()? {
        for index in 0..item.col_count() {
            let vector = item.get_vector_at::<String>(index)?;
            for row in vector.iter()? {
                match row {
                    None => {
                        results.push("NULL".into());
                    }
                    Some(value) => {
                        results.push(value.to_string());
                    }
                }
            }
        }
    }

    assert_eq!(
        results,
        vec![
            "🦆🦆🦆🦆🦆🦆".to_string(),
            "goo\0se".to_string(),
            "NULL".to_string(),
            "🦆🦆🦆🦆🦆🦆".to_string(),
            "🦆🦆🦆🦆🦆🦆".to_string(),
            "🦆🦆🦆🦆🦆🦆".to_string(),
            "goo\0se".to_string(),
            "NULL".to_string(),
            "🦆🦆🦆🦆🦆🦆".to_string(),
            "goo\0se".to_string(),
            "NULL".to_string()
        ]
    );

    Ok(())
}

#[test]
fn test_vector_tstring() -> crate::Result<()> {
    let env = Environment::new()?;
    let instance = env.open(StorageLocation::InMemory)?;
    let conn = instance.connect()?;

    let mut result = conn.query("SELECT * FROM test_vector_types(NULL::BLOB)", Parameters::None)?;

    let expected = [
        Some("thisisalongblob\x00withnullbytes".to_string()),
        Some("\x00\x00\x00a".to_string()),
        None,
        Some("thisisalongblob\x00withnullbytes".to_string()),
        Some("thisisalongblob\x00withnullbytes".to_string()),
        Some("thisisalongblob\x00withnullbytes".to_string()),
        Some("\x00\x00\x00a".to_string()),
        None,
        Some("thisisalongblob\x00withnullbytes".to_string()),
        Some("\x00\x00\x00a".to_string()),
        None,
    ];

    let mut results: Vec<Option<String>> = vec![];

    while let Some(chunk) = result.next_chunk()? {
        let vector = chunk.get_vector_at::<BlobValue>(0)?;

        for item in vector.iter()? {
            if let Some(value) = item {
                let string = String::from_utf8_lossy(value);
                results.push(Some(string.to_string()));
            } else {
                results.push(None);
            }
        }
    }

    assert_eq!(results, expected);

    Ok(())
}

#[test]
fn test_raw_string_access() -> crate::Result<()> {
    let env = Environment::new()?;
    let instance = env.open(StorageLocation::InMemory)?;
    let conn = instance.connect()?;

    let mut result = conn.query("SELECT * FROM test_vector_types(NULL::BLOB)", Parameters::None)?;

    let mut results = vec![];

    while let Some(chunk) = result.next_chunk()? {
        let vector = chunk.get_vector_at::<BlobValue>(0)?;
        assert_eq!(vector.storage_kind(), StorageKind::Flat);

        let view = vector.get_view().unwrap();
        let slice = unsafe { view.as_slice() }.unwrap();

        for (index, item) in slice.iter().enumerate() {
            if view.is_null(index) {
                results.push(None);
            } else {
                let string = String::from_utf8_lossy(item.get_data());
                results.push(Some(string.to_string()));
            }
        }
    }

    let expected = [
        Some("thisisalongblob\x00withnullbytes".to_string()),
        Some("\x00\x00\x00a".to_string()),
        None,
        Some("thisisalongblob\x00withnullbytes".to_string()),
        Some("thisisalongblob\x00withnullbytes".to_string()),
        Some("thisisalongblob\x00withnullbytes".to_string()),
        Some("\x00\x00\x00a".to_string()),
        None,
        Some("thisisalongblob\x00withnullbytes".to_string()),
        Some("\x00\x00\x00a".to_string()),
        None,
    ];

    assert_eq!(results, expected);

    Ok(())
}

#[test]
fn test_raw_integer_access() -> crate::Result<()> {
    let env = Environment::new()?;
    let instance = env.open(StorageLocation::InMemory)?;
    let conn = instance.connect()?;

    let mut result = conn.query("SELECT * FROM test_vector_types(NULL::INTEGER)", Parameters::None)?;

    let mut results = vec![];

    while let Some(chunk) = result.next_chunk()? {
        let vector = chunk.get_vector_at::<i32>(0)?;
        assert_eq!(vector.storage_kind(), StorageKind::Flat);

        let view = vector.get_view().unwrap();
        let slice = unsafe { view.as_slice() }.unwrap();
        assert!(std::panic::catch_unwind(|| view.is_null(view.len())).is_err());

        for (index, item) in slice.iter().enumerate() {
            if view.is_null(index) {
                results.push(None);
            } else {
                results.push(Some(*item));
            }
        }
    }

    let expected = [
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
    ];

    assert_eq!(results, expected);

    Ok(())
}

#[test]
fn test_reference_rebuilds_children() -> crate::Result<()> {
    let env = Environment::new()?;
    let instance = env.open(StorageLocation::InMemory)?;
    let conn = instance.connect()?;
    let types = [Vec::<Option<i32>>::logical_type(&conn)?];

    let mut source = DataChunk::create(&conn, &types, true)?;
    let mut src = source.get_vector_at_mut::<List<i32>>(0)?;
    src.set_size(2)?;
    src.write(0, Some(vec![Some(1), Some(2), Some(3)]))?;
    src.write(1, Some(vec![Some(4)]))?;

    let mut target = DataChunk::create(&conn, &types, true)?;
    let mut dst = target.get_vector_at_mut::<List<i32>>(0)?;
    dst.set_size(1)?;
    dst.write(0, Some(vec![Some(9)]))?;

    // SAFETY: `source` outlives every read of the referenced vector below.
    let mut referenced = unsafe { dst.reference(&src)? };
    // A resize through the reference would reallocate the buffer `src` still reads.
    assert!(!referenced.is_writable());
    assert!(referenced.set_size(4096).is_err());
    assert!(referenced.write(0, None).is_err());
    assert_eq!(src.get(1)?.map(|v| v.len()), Some(1));

    let items: Vec<_> = referenced
        .iter()?
        .map(|r| r.map(|v| v.iter().map(|x| x.copied()).collect::<Vec<_>>()))
        .collect();
    assert_eq!(items, [Some(vec![Some(1), Some(2), Some(3)]), Some(vec![Some(4)])]);

    Ok(())
}

#[test]
fn test_make_sequence_drops_children() -> crate::Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    let types = [Vec::<Option<i32>>::logical_type(&conn)?];

    let mut chunk = DataChunk::create(&conn, &types, true)?;
    let vector = chunk.get_vector_at_mut::<List<i32>>(0)?;
    assert!(vector.children().is_some());
    let sequence = vector.make_sequence(0, 1, 4)?;
    assert!(sequence.children().is_none());
    Ok(())
}

#[test]
fn test_make_constant_rebuilds_children() -> crate::Result<()> {
    let env = Environment::new()?;
    let instance = env.open(StorageLocation::InMemory)?;
    let conn = instance.connect()?;
    let types = [Vec::<Option<i32>>::logical_type(&conn)?];

    let mut chunk = DataChunk::create(&conn, &types, true)?;
    let mut vector = chunk.get_vector_at_mut::<List<i32>>(0)?;
    vector.set_size(1)?;
    vector.write(0, Some(vec![Some(9)]))?;

    let vector = vector.make_constant(vec![Some(7), Some(8)].value(&conn)?, 3)?;

    let items: Vec<_> = vector
        .iter()?
        .map(|r| r.map(|v| v.iter().map(|x| x.copied()).collect::<Vec<_>>()))
        .collect();
    assert_eq!(items, vec![Some(vec![Some(7), Some(8)]); 3]);
    assert!(!vector.is_writable());
    for i in 0..vector.children().unwrap().col_count() {
        assert!(!vector.children().unwrap().get_untyped_vector_at(i)?.is_writable());
    }

    Ok(())
}

#[test]
fn test_make_constant_null_value() -> crate::Result<()> {
    let env = Environment::new()?;
    let instance = env.open(StorageLocation::InMemory)?;
    let conn = instance.connect()?;
    let types = [String::logical_type(&conn)?];

    let mut chunk = DataChunk::create(&conn, &types, true)?;
    let vector = chunk
        .get_vector_at_mut::<String>(0)?
        .make_constant(Value::null(&conn, &types[0])?, 3)?;

    assert_eq!(vector.iter()?.collect::<Vec<_>>(), vec![None; 3]);
    Ok(())
}

#[test]
fn test_make_constant_and_sequence_are_not_writable() -> crate::Result<()> {
    let env = Environment::new()?;
    let instance = env.open(StorageLocation::InMemory)?;
    let conn = instance.connect()?;
    let types = [i32::logical_type(&conn)?, i64::logical_type(&conn)?];

    let mut chunk = DataChunk::create(&conn, &types, true)?;

    let mut constant = chunk
        .get_vector_at_mut::<i32>(0)?
        .make_constant(42_i32.value(&conn)?, 2048)?;
    assert!(!constant.is_writable());
    assert!(constant.write(5, Some(1)).is_err());
    assert!(constant.write(0, None).is_err());
    assert!(constant.set_size(1).is_err());

    let mut sequence = chunk.get_vector_at_mut::<i64>(1)?.make_sequence(0, 1, 16)?;
    assert!(!sequence.is_writable());
    sequence.flatten()?;
    assert!(sequence.write(3, Some(1)).is_err());

    Ok(())
}

#[test]
fn test_large_constant_selection_stays_in_bounds() -> crate::Result<()> {
    let env = Environment::new()?;
    let instance = env.open(StorageLocation::InMemory)?;
    let conn = instance.connect()?;

    let mut chunk = DataChunk::create(&conn, &[i32::logical_type(&conn)?], true)?;
    let constant = chunk
        .get_vector_at_mut::<i32>(0)?
        .make_constant(42_i32.value(&conn)?, 2 * crate::vector::CONSTANT_SELECTION_LEN + 1)?;

    let selection = constant.get_view().unwrap().selection().unwrap();
    assert_eq!(selection.len(), crate::vector::CONSTANT_SELECTION_LEN);
    assert!(selection.iter().all(|&slot| slot == 0));

    Ok(())
}

scalar_callback!(FlatListScalar, i32, |input, output, _ctx, _user_data| {
    let mut list = input.get_vector_at_mut::<List<i32>>(1)?;
    let mut output = output;
    output.set_size(list.len())?;
    list.flatten()?;

    for (i, row) in list.iter()?.enumerate() {
        output.write(i, row.map(|v| v.iter().map(|x| x.copied().unwrap_or(0)).sum()))?;
    }
    Ok(())
});

#[test]
fn test_flatten_rebuilds_children() -> crate::Result<()> {
    let env = Environment::new()?;
    let instance = env.open(StorageLocation::InMemory)?;
    let conn = instance.connect()?;

    ScalarFunctionBuilder::new(
        "flat_list",
        SignatureBuilder::new()
            .param(FunctionParameter::new::<i64>("row"))
            .param(FunctionParameter::new::<Vec<Option<i32>>>("list"))
            .return_type(i32::logical_type(&conn)?),
        FlatListScalar,
    )
    .register(&conn)?;

    let mut result = conn.query("SELECT flat_list(range, [1, 2, 3]) FROM range(5)", Parameters::None)?;
    let mut values = vec![];
    while let Some(chunk) = result.next_chunk()? {
        values.extend(chunk.get_vector_at::<i32>(0)?.iter()?.map(|v| v.copied()));
    }
    assert_eq!(values, vec![Some(6); 5]);

    Ok(())
}

scalar_callback!(SlowWriteInputScalar, i32, |input, output, ctx, _user_data| {
    let mut vector = input.get_vector_at_mut::<i32>(0)?;
    let mut output = output;
    output.set_size(vector.len())?;

    assert!(!vector.is_writable());
    assert!(vector.write_value_slow(0, 999_i32.value(ctx)?).is_err());
    assert!(vector.set_null_slow(0).is_err());

    for (i, v) in vector.iter()?.enumerate() {
        output.write(i, v.copied())?;
    }
    Ok(())
});

#[test]
fn test_slow_writes_reject_read_only_input() -> crate::Result<()> {
    let env = Environment::new()?;
    let instance = env.open(StorageLocation::InMemory)?;
    let conn = instance.connect()?;

    ScalarFunctionBuilder::new(
        "mutate",
        SignatureBuilder::new()
            .param(FunctionParameter::new::<i32>("in"))
            .return_type(i32::logical_type(&conn)?),
        SlowWriteInputScalar,
    )
    .register(&conn)?;

    conn.execute(
        "CREATE TABLE t AS SELECT range::INTEGER AS x FROM range(3)",
        Parameters::None,
    )?;
    conn.execute("SELECT mutate(x) FROM t", Parameters::None)?;

    let chunk = conn
        .query("SELECT x FROM t ORDER BY x", Parameters::None)?
        .next_chunk()?
        .unwrap();
    let values: Vec<_> = chunk.get_vector_at::<i32>(0)?.iter()?.map(|v| v.copied()).collect();
    assert_eq!(values, [Some(0), Some(1), Some(2)]);

    Ok(())
}

scalar_callback!(ArraySumScalar, i32, |input, output, _ctx, _user_data| {
    let array = input.get_vector_at::<Array<i32>>(1)?;
    let mut output = output;
    output.set_size(array.len())?;

    for (i, row) in array.iter()?.enumerate() {
        output.write(i, row.map(|v| v.iter().map(|x| x.copied().unwrap_or(0)).sum()))?;
    }
    Ok(())
});

#[test]
fn test_array_reads_use_type_size() -> crate::Result<()> {
    let env = Environment::new()?;
    let instance = env.open(StorageLocation::InMemory)?;
    let conn = instance.connect()?;

    ScalarFunctionBuilder::new(
        "arr_sum",
        SignatureBuilder::new()
            .param(FunctionParameter::new::<i64>("row"))
            .param(FunctionParameter::new::<[i32; 3]>("arr"))
            .return_type(i32::logical_type(&conn)?),
        ArraySumScalar,
    )
    .register(&conn)?;

    // A constant array argument next to a flat one.
    let mut values = vec![];
    let mut result = conn.query(
        "SELECT arr_sum(range, [1, 2, 3]::INTEGER[3]) FROM range(5)",
        Parameters::None,
    )?;
    while let Some(chunk) = result.next_chunk()? {
        values.extend(chunk.get_vector_at::<i32>(0)?.iter()?.map(|v| v.copied()));
    }
    assert_eq!(values, vec![Some(6); 5]);

    // Flat, constant and dictionary arrays, checked against DuckDB's own sum.
    let chunk = conn
        .query(
            "SELECT count(*) FILTER (arr_sum(0, v) IS DISTINCT FROM list_sum(list_transform(v::INTEGER[], lambda x: coalesce(x, 0)))::INTEGER), count(*)
             FROM test_vector_types(NULL::INTEGER[3]) AS t(v)",
            Parameters::None,
        )?.next_chunk()?.unwrap();
    assert_eq!(chunk.get_vector_at::<i64>(0)?.get(0)?, Some(&0));
    assert!(chunk.get_vector_at::<i64>(1)?.get(0)?.copied().unwrap() > 0);

    Ok(())
}

#[test]
fn test_list_and_map_writes_append_across_wrappers() -> crate::Result<()> {
    let env = Environment::new()?;
    let instance = env.open(StorageLocation::InMemory)?;
    let conn = instance.connect()?;

    let mut list_chunk = DataChunk::create(&conn, &[Vec::<Option<i32>>::logical_type(&conn)?], true)?;
    {
        let mut first = list_chunk.get_vector_at_mut::<List<i32>>(0)?;
        first.set_size(2)?;
        first.write(0, Some(vec![Some(1), Some(2), Some(3)]))?;
    }
    list_chunk
        .get_vector_at_mut::<List<i32>>(0)?
        .write(1, Some(vec![Some(9)]))?;

    let lists = list_chunk.get_vector_at::<List<i32>>(0)?;
    let items: Vec<_> = lists
        .iter()?
        .map(|r| r.map(|v| v.iter().map(|x| x.copied()).collect::<Vec<_>>()))
        .collect();
    assert_eq!(items, [Some(vec![Some(1), Some(2), Some(3)]), Some(vec![Some(9)])]);

    let mut map_chunk = DataChunk::create(&conn, &[MapValue::<i32, i32>::logical_type(&conn)?], true)?;
    {
        let mut first = map_chunk.get_vector_at_mut::<Map<i32, i32>>(0)?;
        first.set_size(2)?;
        first.write(0, Some(HashMap::from([(1, Some(10)), (2, Some(20))])))?;
    }
    map_chunk
        .get_vector_at_mut::<Map<i32, i32>>(0)?
        .write(1, Some(HashMap::from([(3, Some(30))])))?;

    let maps = map_chunk.get_vector_at::<Map<i32, i32>>(0)?;
    let mut rows = maps.iter()?;
    let row = rows.next().unwrap().unwrap();
    assert_eq!((row.get(&1), row.get(&2)), (Some(Some(&10)), Some(Some(&20))));
    let row = rows.next().unwrap().unwrap();
    assert_eq!(row.keys(), vec![Some(&3)]);
    assert_eq!(row.get(&3), Some(Some(&30)));

    Ok(())
}

fn list_values(row: Option<ListRef<'_, i32>>) -> Option<Vec<Option<i32>>> {
    row.map(|row| row.iter().map(|v| v.copied()).collect())
}

#[test]
fn test_children_follow_parent() -> crate::Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    let types = [i32::logical_type(&conn)?, Vec::<Option<i32>>::logical_type(&conn)?];

    // Children are optional and never get more access than their parent.
    let mut read_only = DataChunk::create(&conn, &types, false)?;
    assert!(read_only.get_vector_at::<i32>(0)?.children().is_none());
    let mut parent = read_only.get_vector_at_mut::<List<i32>>(1)?;
    assert!(
        parent
            .children_mut()
            .unwrap()
            .get_vector_at_mut::<i32>(0)?
            .set_size(1)
            .is_err()
    );

    let mut chunk = DataChunk::create(&conn, &types, true)?;
    let mut list = chunk.get_vector_at_mut::<List<i32>>(1)?;
    // An out-of-range row must not append to the child.
    assert!(list.write(0, Some(vec![Some(1)])).is_err());
    list.set_size(1)?;
    list.write(0, Some(vec![Some(1), Some(2)]))?;
    // The row count is read live, and the cached child view is rebuilt after `children_mut`.
    assert_eq!(list.children().unwrap().row_count()?, 2);
    list.children_mut().unwrap().get_vector_at_mut::<i32>(0)?.flatten()?;
    assert_eq!(list_values(list.get(0)?), Some(vec![Some(1), Some(2)]));
    Ok(())
}

#[test]
fn test_nested_children_survive_reallocation() -> crate::Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    // Each step grows the child past its initial capacity, so DuckDB reallocates it.
    let mut chunk = DataChunk::create(
        &conn,
        &[
            <[i32; 1]>::logical_type(&conn)?,
            StructValue::<TestStruct>::logical_type(&conn)?,
            Vec::<Option<i32>>::logical_type(&conn)?,
        ],
        true,
    )?;
    {
        let [array, structs, list] = chunk.get_vector_array_mut([0, 1, 2])?;
        let mut array = array.cast::<Array<i32>>()?;
        let mut structs = structs.cast::<Struct>()?;
        let mut list = list.cast::<List<i32>>()?;
        array.set_size(1)?;
        structs.set_size(1)?;
        array.write(0, Some(vec![Some(3)]))?;
        structs.write(
            0,
            Some(StructWrite::default().field::<i32>(Some(7)).field::<String>(Some("a"))),
        )?;
        // Reads cache the child views, which growing the parent must invalidate.
        assert_eq!(array.get(0)?.unwrap().iter().next(), Some(Some(&3)));
        assert_eq!(structs.get(0)?.unwrap().get::<i32>("key")?, Some(&7));
        array.set_size(3000)?;
        structs.set_size(3000)?;
        // Growing the parents evicted the cached child views, so fresh views see the new size.
        assert_eq!(array.children().unwrap().get_untyped_vector_at(0)?.len(), 3000);
        assert_eq!(structs.children().unwrap().get_untyped_vector_at(0)?.len(), 3000);
        array.write(2999, Some(vec![Some(4)]))?;
        structs.write(
            2999,
            Some(StructWrite::default().field::<i32>(Some(8)).field::<String>(Some("b"))),
        )?;
        assert_eq!(array.get(2999)?.unwrap().iter().next(), Some(Some(&4)));
        assert_eq!(structs.get(2999)?.unwrap().get::<i32>("key")?, Some(&8));
        list.set_size(1)?;
        list.write(0, Some((0..5000).map(Some).collect()))?;
    }
    assert_eq!(
        chunk.get_vector_at::<List<i32>>(2)?.get(0)?.unwrap().iter().last(),
        Some(Some(&4999))
    );
    Ok(())
}

#[test]
fn test_nested_writes_reuse_child_wrappers() -> crate::Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    let mut chunk = DataChunk::create(&conn, &[Vec::<Option<Vec<Option<i32>>>>::logical_type(&conn)?], true)?;
    let mut lists = chunk.get_vector_at_mut::<List<List<i32>>>(0)?;
    lists.set_size(3000)?;
    lists.write(0, Some(vec![Some(vec![Some(0)])]))?;
    lists.write(1, Some(vec![Some(vec![Some(1)])]))?;
    // The cache keeps the writer's wrapper rather than a fresh shared view.
    assert!(lists.children().unwrap().get_untyped_vector_at(0)?.is_writable());

    // Grow both child levels past their initial capacity while the wrappers are reused.
    for row in 2..3000 {
        lists.write(row, Some(vec![Some(vec![Some(row as i32), None]), None]))?;
    }
    for row in [0, 1, 2047, 2999] {
        let outer = lists.get(row)?.unwrap();
        let inner = list_values(outer.iter().next().unwrap());
        assert_eq!(inner.unwrap()[0], Some(row as i32));
    }
    Ok(())
}

#[test]
fn test_write_value_slow_null_is_visible() -> crate::Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    let logical_type = i32::logical_type(&conn)?;
    let mut chunk = DataChunk::create(&conn, std::slice::from_ref(&logical_type), true)?;
    let mut vector = chunk.get_vector_at_mut::<i32>(0)?;
    vector.set_size(1)?;
    vector.write_value_slow(0, crate::value::Value::null(&conn, &logical_type)?)?;
    assert!(vector.is_null(0)?);
    Ok(())
}

#[test]
fn test_collection_append() -> crate::Result<()> {
    use crate::column_data_collection::ColumnDataCollection;
    let env = Environment::new()?;
    let instance = env.open(StorageLocation::InMemory)?;
    let conn = instance.connect()?;
    let types = [Vec::<Option<i32>>::logical_type(&conn)?];
    let mut chunk = DataChunk::create(&conn, &types, true)?;
    chunk
        .get_vector_at_mut::<List<i32>>(0)?
        .make_constant(vec![Some(1), Some(2)].value(&conn)?, 3)?;
    assert_eq!(
        chunk.get_vector_at::<List<i32>>(0)?.storage_kind(),
        StorageKind::Constant
    );

    // Appending flattens nested columns in place, so cached views must be dropped.
    let mut source = ColumnDataCollection::new(&conn, &types)?.to_append()?;
    source.append(&mut chunk)?;
    assert_eq!(chunk.get_vector_at::<List<i32>>(0)?.storage_kind(), StorageKind::Flat);

    // Borrowed chunks from a scan can be appended too.
    let mut target = ColumnDataCollection::new(&conn, &types)?.to_append()?;
    let mut scan = source.to_scan(&conn)?;
    while let Some(mut scanned) = scan.next_chunk()? {
        target.append(&mut scanned)?;
    }
    assert_eq!(target.len()?, 3);
    Ok(())
}

scalar_callback!(CopyOver, i64, |input, result, ctx, _ud| {
    let vec = input.get_vector_at::<String>(0)?;
    let has_bad_row = vec.iter()?.any(|row| row == Some("abc"));

    let (_, row_error) = result.try_cast_rows(vec, ctx)?;
    match row_error {
        Some(error) if has_bad_row => assert!(error.message.contains("'abc'"), "{error}"),
        None if !has_bad_row => {}
        other => panic!("unexpected TRY cast diagnostic: {other:?}"),
    }

    Ok(())
});

#[test]
fn test_vector_cast() -> crate::Result<()> {
    let env = Environment::new()?;
    let instance = env.open(StorageLocation::InMemory)?;
    let conn = instance.connect()?;

    ScalarFunctionBuilder::new(
        "copy_over",
        SignatureBuilder::new()
            .param(FunctionParameter::new::<String>("in"))
            .return_type(i64::logical_type(&conn)?),
        CopyOver,
    )
    .register(&conn)?;

    for (input, expected) in [
        (vec![Some("10"), Some("300"), None], [Some(10), Some(300), None]),
        // A row that does not convert becomes NULL instead of failing the query.
        (vec![Some("10"), Some("abc"), None], [Some(10), None, None]),
    ] {
        let mut query = conn.query("SELECT copy_over(unnest($1))", Parameters::Positional(&[&input]))?;
        let mut results = Vec::new();
        while let Some(chunk) = query.next_chunk()? {
            results.extend(chunk.get_vector_at::<i64>(0)?.iter()?.map(|row| row.copied()));
        }
        assert_eq!(results, expected);
    }

    Ok(())
}

scalar_callback!(CopyOverStrict, i64, |input, result, ctx, _ud| {
    let vec = input.get_vector_at::<String>(0)?;
    result.cast_rows(vec, ctx)?;
    Ok(())
});

#[test]
fn test_vector_cast_fails() -> crate::Result<()> {
    let env = Environment::new()?;
    let instance = env.open(StorageLocation::InMemory)?;
    let conn = instance.connect()?;

    ScalarFunctionBuilder::new(
        "copy_over_strict",
        SignatureBuilder::new()
            .param(FunctionParameter::new::<String>("in"))
            .return_type(i64::logical_type(&conn)?),
        CopyOverStrict,
    )
    .register(&conn)?;

    let error = conn
        .execute(
            "SELECT copy_over_strict(unnest($1))",
            Parameters::Positional(&[&vec![Some("10"), Some("abc"), None]]),
        )
        .unwrap_err();
    assert_eq!(error.code, DuckDBError::DUCKDB_V2_ERROR_TYPE_CONVERSION);
    assert!(
        error.message.contains("Could not convert string 'abc' to INT64"),
        "{error}"
    );

    Ok(())
}

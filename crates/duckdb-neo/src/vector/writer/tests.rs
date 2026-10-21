use crate::{
    Parameters, Result, ToValue,
    builder_helpers::scalar_callback,
    data_chunk::DataChunk,
    environment::{Environment, StorageLocation},
    scalar::ScalarFunctionBuilder,
    signature::{FunctionParameter, SignatureBuilder},
    types::{Array, DuckDBType, List, Struct, StructWrite, Variant},
    value::Value,
    vector::{StorageKind, Unknown, VectorWriter},
};

#[test]
fn writer_sets_values_and_preserves_lazy_validity() -> Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    let mut chunk = DataChunk::create(&conn, &[i32::logical_type(&conn)?], true)?;
    let mut vector = chunk.get_vector_at_mut::<i32>(0)?;
    vector.set_size(2)?;
    {
        let mut writer: VectorWriter<'_, '_, i32> = vector.writer()?;
        assert_eq!(writer.len(), 2);
        assert!(!writer.is_empty());
        assert!(writer.validity().is_none());
        assert!(writer.set(2, Some(99)).is_err());
        assert!(writer.set(usize::MAX, None).is_err());
        assert!(writer.validity().is_none());
        writer.set(0, Some(42))?;
        // SAFETY: Row one is within the writer's two rows.
        unsafe { writer.set_unchecked(1, Some(7))? };
        assert!(writer.validity().is_none());
        writer.set(0, None)?;
        assert_eq!(writer.validity().unwrap()[0] & 1, 0);
        writer.set(0, Some(43))?;
        assert_eq!(writer.validity().unwrap()[0] & 1, 1);
    }
    assert_eq!(vector.reader()?.iter().collect::<Vec<_>>(), [Some(&43), Some(&7)]);
    Ok(())
}

#[test]
fn writer_supports_raw_access_without_a_typed_encoder() -> Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    let mut chunk = DataChunk::create(&conn, &[i32::logical_type(&conn)?], true)?;
    let mut vector = chunk.get_vector_at_mut::<Unknown>(0)?;
    vector.set_size(1)?;
    {
        let mut writer = vector.writer()?;
        let data = writer.as_mut_ptr().cast::<i32>();
        // SAFETY: The chunk's schema is INTEGER and row zero is within the writer.
        unsafe {
            data.write(42);
            writer.set_row_validity(0, true)?;
        }
    }
    assert_eq!(vector.cast_ref::<i32>()?.reader()?.get(0)?, Some(&42));
    Ok(())
}

#[test]
fn writer_raw_data_and_manual_validity_cover_word_boundaries() -> Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    let mut chunk = DataChunk::create(&conn, &[i32::logical_type(&conn)?], true)?;
    let mut vector = chunk.get_vector_at_mut::<i32>(0)?;
    for count in [0, 1, 63, 64, 65, 129] {
        vector.set_size(count)?;
        {
            let mut writer = vector.writer()?;
            assert_eq!(writer.is_empty(), count == 0);
            let data = writer.as_mut_ptr();
            // SAFETY: All rows are INTEGER slots; NULL rows have no descendants.
            unsafe {
                writer.fill_validity(false)?;
                for index in 0..count {
                    data.add(index).write(index as i32);
                }
                assert!(writer.validity().unwrap().iter().all(|word| *word == 0));
                let mask = writer.validity_mut()?;
                assert_eq!(mask.len(), count.div_ceil(64));
                for index in 0..count {
                    if index % 7 != 0 {
                        mask[index / 64] |= 1 << (index % 64);
                    }
                }
                if count > 0 {
                    writer.set_row_validity(count - 1, true)?;
                }
                assert!(writer.set_row_validity(count, false).is_err());
                assert!(writer.set_row_validity(usize::MAX, true).is_err());
            }
        }
        let reader = vector.reader()?;
        for index in 0..count {
            let expected = (index % 7 != 0 || index == count - 1).then_some(index as i32);
            assert_eq!(reader.get(index)?.copied(), expected);
        }
    }
    Ok(())
}

#[test]
fn writer_mutable_slice_preserves_validity() -> Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    let mut chunk = DataChunk::create(&conn, &[i32::logical_type(&conn)?], true)?;
    let mut vector = chunk.get_vector_at_mut::<i32>(0)?;
    vector.set_size(3)?;
    {
        let mut writer = vector.writer()?;
        for index in 0..writer.len() {
            writer.set(index, Some(index as i32))?;
        }
        writer.set(1, None)?;
        // SAFETY: Every physical slot was initialized before clearing validity.
        unsafe { writer.as_slice_mut() }.unwrap().copy_from_slice(&[10, 20, 30]);
        assert_eq!(writer.validity().unwrap()[0] & 2, 0);
    }
    assert_eq!(
        vector.reader()?.iter().collect::<Vec<_>>(),
        [Some(&10), None, Some(&30)]
    );
    vector.set_size(0)?;
    let mut writer = vector.writer()?;
    assert!(writer.is_empty());
    // SAFETY: There are no physical slots to initialize.
    assert!(unsafe { writer.as_slice_mut() }.is_none_or(|values| values.is_empty()));
    Ok(())
}

#[test]
fn writer_rejects_read_only_and_non_flat_vectors() -> Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    let mut read_only = DataChunk::create(&conn, &[i32::logical_type(&conn)?], false)?;
    assert!(read_only.get_vector_at_mut::<i32>(0)?.writer().is_err());
    let mut chunk = DataChunk::create(&conn, &[i32::logical_type(&conn)?, i64::logical_type(&conn)?], true)?;
    let mut constant = chunk
        .get_vector_at_mut::<i32>(0)?
        .make_constant(42_i32.value(&conn)?, 3)?;
    assert!(constant.writer().is_err());
    assert_eq!(constant.storage_kind(), StorageKind::Constant);
    assert_eq!(constant.reader()?.get(2)?, Some(&42));
    drop(constant);
    let mut constant = chunk.get_vector_at_mut::<i32>(0)?;
    assert!(constant.is_writable());
    let error = constant.writer().unwrap_err();
    assert!(error.message.contains("not flat"), "{}", error.message);
    assert_eq!(constant.storage_kind(), StorageKind::Constant);
    drop(constant);
    let mut sequence = chunk.get_vector_at_mut::<i64>(1)?.make_sequence(1, 2, 3)?;
    assert!(sequence.writer().is_err());
    assert_eq!(sequence.storage_kind(), StorageKind::Other);
    Ok(())
}

#[test]
fn writer_encodes_raw_strings_in_its_own_arena() -> Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    let mut chunk = DataChunk::create(&conn, &[String::logical_type(&conn)?], true)?;
    let mut vector = chunk.get_vector_at_mut::<String>(0)?;
    vector.set_size(3)?;
    {
        let mut writer = vector.writer()?;
        // SAFETY: NULL string rows have no descendants.
        unsafe { writer.fill_validity(false)? };
        let data = writer.as_mut_ptr();
        let arena = writer.arena()?;
        // SAFETY: Both slots are in bounds and the vector owns the encoded bytes.
        unsafe {
            data.add(0).write(arena.add_str("short")?);
            data.add(2)
                .write(arena.add_str("longer than DuckDB's inline string storage")?);
        }
        // SAFETY: Both rows now contain initialized, valid UTF-8 values.
        unsafe {
            writer.set_row_validity(0, true)?;
            writer.set_row_validity(2, true)?;
        }
    }
    assert_eq!(
        vector.reader()?.iter().collect::<Vec<_>>(),
        [Some("short"), None, Some("longer than DuckDB's inline string storage")]
    );
    Ok(())
}

#[test]
fn writer_reuses_nested_children_across_growth() -> Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    let mut chunk = DataChunk::create(&conn, &[Vec::<Option<i32>>::logical_type(&conn)?], true)?;
    let mut vector = chunk.get_vector_at_mut::<List<i32>>(0)?;
    vector.set_size(3)?;
    {
        let mut writer = vector.writer()?;
        writer.set(0, Some(vec![Some(1), None]))?;
        writer.set(1, None)?;
        writer.set(2, Some((0..5000).map(Some).collect()))?;
        writer.set(0, Some(vec![Some(42)]))?;
    }
    let reader = vector.reader()?;
    assert_eq!(reader.get(0)?.unwrap().iter().collect::<Vec<_>>(), [Some(&42)]);
    assert!(reader.get(1)?.is_none());
    assert_eq!(reader.get(2)?.unwrap().iter().last(), Some(Some(&4999)));
    Ok(())
}

#[test]
fn writer_nested_nulls_refresh_buffers_and_null_descendants() -> Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    let mut chunk = DataChunk::create(&conn, &[<[i32; 2]>::logical_type(&conn)?], true)?;
    let mut vector = chunk.get_vector_at_mut::<Array<i32>>(0)?;
    vector.set_size(2)?;
    {
        let mut writer = vector.writer()?;
        assert!(writer.as_mut_ptr().is_null());
        // SAFETY: ARRAY values have no parent data slots.
        assert!(unsafe { writer.as_slice_mut() }.is_none());
        assert!(writer.set(0, Some(vec![Some(1)])).is_err());
        writer.set(0, Some(vec![Some(1), Some(2)]))?;
        writer.set(1, None)?;
        // SAFETY: This observes the initialized mask without changing its bits.
        assert_eq!(unsafe { writer.validity_mut()? }[0] & 3, 1);
        writer.set(1, Some(vec![Some(3), Some(4)]))?;
        writer.set(0, None)?;
        // SAFETY: The rows and descendants were initialized by typed writes.
        unsafe { writer.set_row_validity(1, true)? };
        assert_eq!(writer.validity().unwrap()[0] & 3, 2);
    }
    let reader = vector.reader()?;
    assert!(reader.get(0)?.is_none());
    assert_eq!(reader.get(1)?.unwrap().iter().collect::<Vec<_>>(), [Some(&3), Some(&4)]);
    let child = vector.children().unwrap().get_vector_at::<i32>(0)?;
    assert_eq!(
        child.reader()?.iter().collect::<Vec<_>>(),
        [None, None, Some(&3), Some(&4)]
    );
    Ok(())
}

#[test]
fn writer_struct_nulls_propagate_to_children() -> Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    let mut result = conn.query("SELECT {'n': 42} AS value", Parameters::None)?;
    let source = result.next_chunk()?.unwrap();
    let ty = source.get_vector_at::<Struct>(0)?.logical_type();
    let mut chunk = DataChunk::create(&conn, std::slice::from_ref(ty), true)?;
    let mut vector = chunk.get_vector_at_mut::<Struct>(0)?;
    vector.set_size(1)?;
    {
        let mut writer = vector.writer()?;
        writer.set(0, Some(StructWrite::default().field::<i32>(Some(42))))?;
        writer.set(0, None)?;
        assert_eq!(writer.validity().unwrap()[0] & 1, 0);
    }
    assert!(vector.reader()?.get(0)?.is_none());
    assert_eq!(
        vector.children().unwrap().get_vector_at::<i32>(0)?.reader()?.get(0)?,
        None
    );
    Ok(())
}

#[test]
fn writer_value_api_preserves_cast_nullness_and_refreshes_validity() -> Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    let mut chunk = DataChunk::create(&conn, &[Variant::logical_type(&conn)?], true)?;
    let mut vector = chunk.get_vector_at_mut::<Variant>(0)?;
    vector.set_size(2)?;
    {
        let mut writer = vector.writer()?;
        writer.set(0, Some(42_i32.value(&conn)?))?;
        writer.set(1, Some(43_i32.value(&conn)?))?;
        // SAFETY: Both rows have been initialized; clearing a VARIANT row needs no child updates.
        unsafe { writer.validity_mut()?[0] &= !2 };
        writer.set(0, Some(Value::null(&conn, &i32::logical_type(&conn)?)?))?;
        writer.set(1, Some(7_i32.value(&conn)?))?;
        // SAFETY: This observes the refreshed mask without changing its bits.
        assert_eq!(unsafe { writer.validity_mut()? }[0] & 3, 2);
    }
    assert!(vector.reader()?.is_null(0)?);
    assert!(!vector.reader()?.is_null(1)?);
    Ok(())
}

scalar_callback!(WriterInputProbe, i32, |input, output, _ctx, _user_data| {
    let mut vector = input.get_vector_at_mut::<i32>(0)?;
    let original_kind = vector.storage_kind();
    assert!(vector.writer().is_err());
    assert_eq!(vector.storage_kind(), original_kind);
    if original_kind == StorageKind::Other {
        vector.flatten()?;
    }
    let reader = vector.reader()?;
    let mut output = output;
    output.set_size(reader.len())?;
    let mut writer = output.writer()?;
    for (index, value) in reader.iter().enumerate() {
        writer.set(index, value.copied())?;
    }
    Ok(())
});

#[test]
fn writer_rejects_native_input_layouts() -> Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    ScalarFunctionBuilder::new(
        "writer_input_probe",
        SignatureBuilder::new()
            .param(FunctionParameter::new::<i32>("input"))
            .return_type(i32::logical_type(&conn)?),
        WriterInputProbe,
    )
    .register(&conn)?;
    let mut result = conn.query(
        "SELECT writer_input_probe(test_vector) FROM test_vector_types(NULL::INTEGER)",
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

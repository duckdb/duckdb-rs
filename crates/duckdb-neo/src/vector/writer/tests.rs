use duckdb_loadable_macros::AsDuckDB;

use crate::{
    Parameters, Result, ToValue,
    connection::FFILink,
    data_chunk::DataChunk,
    environment::{Environment, StorageLocation},
    scalar::MicroScalar,
    types::{Array, BitValue, BlobValue, DuckDBType, List, UncheckedString, Variant},
    value::Value,
    vector::{RowReader, RowWriter, StorageKind, Unknown, Vector, VectorWriter, WritableNativeElement},
};

#[test]
fn writer_sets_values_with_preallocated_validity() -> Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    let mut chunk = DataChunk::create(&conn, &[i32::logical_type(&conn)?], true)?;
    let mut vector = chunk.get_vector_at_mut::<i32>(0)?;
    vector.set_size(2)?;
    {
        let mut writer: VectorWriter<'_, i32> = vector.writer()?;
        assert_eq!(writer.len(), 2);
        assert!(!writer.is_empty());
        let validity = unsafe { writer.validity_mut() }.unwrap().as_ptr();
        assert_eq!(unsafe { writer.validity_mut() }.unwrap()[0], u64::MAX);
        assert!(writer.set(2, &99).is_err());
        assert!(writer.set_null(usize::MAX).is_err());
        assert_eq!(unsafe { writer.validity_mut() }.unwrap().as_ptr(), validity);
        writer.set(0, &42)?;
        // SAFETY: Row one is within the writer's two rows.
        unsafe { writer.set_unchecked(1, &7)? };
        assert_eq!(unsafe { writer.validity_mut() }.unwrap().as_ptr(), validity);
        writer.set_null(0)?;
        assert_eq!(unsafe { writer.validity_mut() }.unwrap()[0] & 1, 0);
        writer.set(0, &43)?;
        assert_eq!(unsafe { writer.validity_mut() }.unwrap()[0] & 1, 1);
    }
    assert_eq!(vector.reader()?.iter().collect::<Vec<_>>(), [Some(&43), Some(&7)]);
    Ok(())
}

fn write_twice<T: WritableNativeElement>(vector: &mut Vector<'_, T>, value: &T::Write) -> Result<()> {
    vector.set_size(3)?;
    let mut writer = vector.writer()?;
    writer.set(0, value)?;
    // SAFETY: the writer has three rows.
    unsafe { writer.set_unchecked(1, value)? };
    writer.set_null(2)
}

#[test]
fn writer_copies_borrowed_unsized_inputs() -> Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    let mut chunk = DataChunk::create(
        &conn,
        &[
            String::logical_type(&conn)?,
            BlobValue::logical_type(&conn)?,
            BitValue::logical_type(&conn)?,
        ],
        true,
    )?;
    {
        let text = String::from("a borrowed string longer than inline storage");
        let bytes = vec![42; 32];
        let bits = vec![0, 0x5a];
        write_twice(&mut chunk.get_vector_at_mut::<String>(0)?, text.as_str())?;
        write_twice(&mut chunk.get_vector_at_mut::<BlobValue>(1)?, bytes.as_slice())?;
        write_twice(&mut chunk.get_vector_at_mut::<BitValue>(2)?, bits.as_slice())?;
    }
    for index in 0..2 {
        assert_eq!(
            chunk.get_vector_at::<String>(0)?.reader()?.get(index)?,
            Some("a borrowed string longer than inline storage")
        );
        assert_eq!(
            chunk
                .get_vector_at::<BlobValue>(1)?
                .get_value_slow(index)?
                .get::<BlobValue>()?,
            Some(BlobValue(vec![42; 32]))
        );
        assert_eq!(
            chunk
                .get_vector_at::<BitValue>(2)?
                .get_value_slow(index)?
                .get::<BitValue>()?,
            Some(BitValue(vec![0, 0x5a]))
        );
    }
    for column in 0..3 {
        assert!(chunk.get_vector_at::<Unknown>(column)?.reader()?.is_null(2)?);
    }
    Ok(())
}

#[test]
fn writer_borrows_non_clone_fields_and_values() -> Result<()> {
    #[derive(AsDuckDB)]
    #[duckdb(writer)]
    struct Row {
        text: UncheckedString,
        bytes: BlobValue,
    }

    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    let mut chunk = DataChunk::create(&conn, &[Row::logical_type(&conn)?, Variant::logical_type(&conn)?], true)?;
    {
        let row = Row {
            text: UncheckedString("a borrowed field longer than inline storage".to_owned()),
            bytes: BlobValue(vec![42; 32]),
        };
        {
            let mut vector = chunk.get_vector_at_mut::<Row>(0)?;
            vector.set_size(3)?;
            let mut writer = vector.row_writer()?;
            writer.set(0, &row)?;
            writer.set(1, &row)?;
            writer.set_null(2)?;
        }
        let value = 42_i32.value(&conn)?;
        write_twice(&mut chunk.get_vector_at_mut::<Variant>(1)?, &value)?;
        assert_eq!(value.get::<i32>()?, Some(42));
        assert_eq!(row.bytes.0.len(), 32);
    }
    for index in 0..2 {
        let row = chunk
            .get_vector_at::<Row>(0)?
            .get_value_slow(index)?
            .get::<Row>()?
            .unwrap();
        assert_eq!(row.text.0, "a borrowed field longer than inline storage");
        assert_eq!(row.bytes.0, vec![42; 32]);
        let value = chunk.get_vector_at::<Variant>(1)?.get_value_slow(index)?;
        assert_eq!(
            conn.value_cast(&value, &i32::logical_type(&conn)?)?.get::<i32>()?,
            Some(42)
        );
    }
    assert!(chunk.get_vector_at::<Row>(0)?.reader()?.is_null(2)?);
    assert!(chunk.get_vector_at::<Variant>(1)?.reader()?.is_null(2)?);
    Ok(())
}

#[test]
fn writer_rewrites_nulls_across_mask_words() -> Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    for count in [0, 1, 15, 16, 17, 31, 32, 33, 47, 48, 49, 63, 64, 65, 129, 2048, 5000] {
        let mut chunk = DataChunk::create(&conn, &[i32::logical_type(&conn)?], true)?;
        let mut vector = chunk.get_vector_at_mut::<i32>(0)?;
        vector.set_size(count)?;
        {
            let mut writer = vector.writer()?;
            for index in 0..count {
                writer.set_null(index)?;
            }
            for index in 0..count {
                writer.set(index, &(index as i32))?;
            }
            if count > 0 {
                for index in [count / 2, count - 1, 0] {
                    writer.set_null(index)?;
                    writer.set(index, &(index as i32))?;
                }
                writer.set_null(count / 2)?;
            }
        }
        if count > 0 {
            vector.writer()?.set(count / 2, &((count / 2) as i32))?;
        }
        for (index, value) in vector.reader()?.iter().enumerate() {
            assert_eq!(value.copied(), Some(index as i32));
        }
    }
    Ok(())
}

#[test]
fn writer_validity_groups_preserve_neighboring_rows() -> Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    let mut chunk = DataChunk::create(&conn, &[i32::logical_type(&conn)?], true)?;
    let mut vector = chunk.get_vector_at_mut::<i32>(0)?;
    vector.set_size(129)?;
    let mut expected = vec![Some(42); 129];
    {
        let mut writer = vector.writer()?;
        for index in 0..writer.len() {
            writer.set(index, &42)?;
        }
        for group in (0..8).step_by(2) {
            for (index, value) in expected.iter_mut().enumerate().skip(group * 16).take(16) {
                writer.set_null(index)?;
                *value = None;
            }
        }
        for index in [0, 15, 16, 31, 32, 47, 48, 63, 64, 79, 80, 95, 96, 111, 112, 127, 128] {
            writer.set_null(index)?;
            writer.set(index, &(index as i32))?;
            expected[index] = Some(index as i32);
        }
    }
    assert_eq!(
        vector.reader()?.iter().map(|value| value.copied()).collect::<Vec<_>>(),
        expected
    );
    Ok(())
}

#[test]
fn writer_handles_manual_validity_and_padding() -> Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    let mut chunk = DataChunk::create(&conn, &[i32::logical_type(&conn)?], true)?;
    let mut vector = chunk.get_vector_at_mut::<i32>(0)?;
    vector.set_size(65)?;
    {
        let mut writer = vector.writer()?;
        for index in 0..writer.len() {
            writer.set(index, &(index as i32))?;
        }
        // SAFETY: Every INTEGER row was initialized; there are no descendants.
        unsafe {
            writer.fill_validity(false)?;
            writer.set_row_validity(0, false)?;
            writer.set_row_validity(64, true)?;
            writer.fill_validity(true)?;
            writer.set_row_validity(63, false)?;
            writer.set_row_validity(63, true)?;
            writer.fill_validity(true)?;
            writer.validity_mut()?[0] &= !(1 << 2);
        }
        writer.set(2, &2)?;
        // SAFETY: Only padding bits outside the vector's rows are cleared.
        unsafe { writer.validity_mut()?[1] = 1 };
    }
    vector.writer()?.set(64, &64)?;
    for (index, value) in vector.reader()?.iter().enumerate() {
        assert_eq!(value.copied(), Some(index as i32));
    }
    Ok(())
}

#[test]
fn writer_restores_validity_after_slow_writes() -> Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    let mut chunk = DataChunk::create(&conn, &[i32::logical_type(&conn)?], true)?;
    let mut vector = chunk.get_vector_at_mut::<i32>(0)?;
    vector.set_size(3)?;
    {
        let mut writer = vector.writer()?;
        for index in 0..writer.len() {
            writer.set(index, &(index as i32))?;
        }
        writer.set_null_slow(1)?;
        writer.set(1, &1)?;
        // SAFETY: Every INTEGER row has been initialized.
        unsafe { writer.fill_validity(true)? };
        writer.write_value_slow(2, &Value::null(&conn, &i32::logical_type(&conn)?)?)?;
        writer.set(2, &2)?;
    }
    assert_eq!(
        vector.reader()?.iter().collect::<Vec<_>>(),
        [Some(&0), Some(&1), Some(&2)]
    );
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
                assert!(writer.validity_mut().unwrap().iter().all(|word| *word == 0));
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
            writer.set(index, &(index as i32))?;
        }
        writer.set_null(1)?;
        // SAFETY: Every physical slot was initialized before clearing validity.
        unsafe { writer.as_slice_mut() }.unwrap().copy_from_slice(&[10, 20, 30]);
        assert_eq!(unsafe { writer.validity_mut() }.unwrap()[0] & 2, 0);
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
fn writer_sets_string_parts() -> Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    let mut chunk = DataChunk::create(&conn, &[String::logical_type(&conn)?], true)?;
    let mut vector = chunk.get_vector_at_mut::<String>(0)?;
    let cases: &[Option<&[&str]>] = &[
        Some(&[]),
        Some(&["hello", " ", "world"]),
        Some(&["1234", "", "56789012"]),
        Some(&["a", "", "bc", "defghijklm"]),
        Some(&["héllo", "\0", " 🦆"]),
        None,
    ];
    vector.set_size(cases.len())?;
    {
        let mut writer = vector.writer()?;
        assert!(writer.set_parts(cases.len(), None).is_err());
        for (index, parts) in cases.iter().enumerate() {
            writer.set_parts(index, *parts)?;
        }
    }
    let reader = vector.reader()?;
    for (index, parts) in cases.iter().enumerate() {
        assert_eq!(reader.get(index)?, parts.map(|parts| parts.concat()).as_deref());
    }
    drop(vector);
    let mut vector = chunk.get_vector_at_mut::<UncheckedString>(0)?;
    vector.writer()?.set_parts(0, None)?;
    assert_eq!(vector.reader()?.get(0)?, None);
    vector.writer()?.set_parts(0, Some(&["hello", " again"]))?;
    assert_eq!(vector.reader()?.get(0)?, Some("hello again"));
    Ok(())
}

#[test]
fn writer_sets_blob_parts() -> Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    let mut chunk = DataChunk::create(&conn, &[BlobValue::logical_type(&conn)?], true)?;
    let mut vector = chunk.get_vector_at_mut::<BlobValue>(0)?;
    let cases: &[Option<&[&[u8]]>] = &[
        Some(&[]),
        Some(&[b"\xff", b"", b"\0\x80", b"binary"]),
        Some(&[b"\xff\0\x80\x01", b"", b"12345678"]),
        Some(&[b"\xff", b"", b"\0\x80", b"1234567890"]),
        None,
    ];
    vector.set_size(cases.len())?;
    {
        let mut writer = vector.writer()?;
        assert!(writer.set_parts(cases.len(), None).is_err());
        for (index, parts) in cases.iter().enumerate() {
            writer.set_parts(index, *parts)?;
        }
    }
    let reader = vector.reader()?;
    for (index, parts) in cases.iter().enumerate() {
        assert_eq!(reader.get(index)?, parts.map(|parts| parts.concat()).as_deref());
    }
    vector.writer()?.set_parts(0, None)?;
    assert_eq!(vector.reader()?.get(0)?, None);
    vector.writer()?.set_parts(0, Some(&[b"\xff", b"\0"]))?;
    assert_eq!(vector.reader()?.get(0)?, Some(b"\xff\0".as_slice()));
    Ok(())
}

#[test]
fn writer_reuses_nested_children_across_rows() -> Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    let mut chunk = DataChunk::create(&conn, &[List::<i32>::logical_type(&conn)?], true)?;
    let mut vector = chunk.get_vector_at_mut::<List<i32>>(0)?;
    vector.set_size(3)?;
    {
        let mut writer = vector.list_writer()?;
        writer.set(0, &List(vec![Some(1), None]))?;
        writer.set_null(1)?;
        writer.set(2, &(0..5000).map(Some).collect())?;
        // Rewriting a row takes a new range.
        writer.set(0, &List(vec![Some(42)]))?;
        assert_eq!(writer.child_offset(), 5003);
        writer.finish()?;
    }
    let reader = vector.list_reader()?;
    assert_eq!(reader.get(0)?.unwrap().iter().collect::<Vec<_>>(), [Some(&42)]);
    assert!(reader.get(1)?.is_none());
    assert_eq!(reader.get(2)?.unwrap().iter().last(), Some(Some(&4999)));
    Ok(())
}

#[test]
fn writer_nested_nulls_null_descendants() -> Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    let mut chunk = DataChunk::create(&conn, &[Array::<i32, 2>::logical_type(&conn)?], true)?;
    let mut vector = chunk.get_vector_at_mut::<Array<i32, 2>>(0)?;
    vector.set_size(2)?;
    {
        let mut writer = vector.array_writer()?;
        writer.set(0, &Array([Some(1), Some(2)]))?;
        writer.set_null(1)?;
        writer.set(1, &Array([Some(3), Some(4)]))?;
        writer.set_null(0)?;
    }
    {
        let mut writer = vector.writer()?;
        assert!(writer.as_mut_ptr().is_null());
        // SAFETY: ARRAY values have no parent data slots.
        assert!(unsafe { writer.as_slice_mut() }.is_none());
        // SAFETY: This observes the initialized mask without changing its bits.
        assert_eq!(unsafe { writer.validity_mut() }?[0] & 3, 2);
    }
    let reader = vector.array_reader()?;
    assert!(reader.get(0)?.is_none());
    assert_eq!(reader.get(1)?.unwrap().iter().collect::<Vec<_>>(), [Some(&3), Some(&4)]);
    let child = vector.children().unwrap().get_vector_at::<i32>(0)?;
    assert_eq!(
        child.reader()?.iter().collect::<Vec<_>>(),
        [None, None, Some(&3), Some(&4)]
    );
    // A native NULL also nulls the elements.
    vector.writer()?.set_null(1)?;
    let child = vector.children().unwrap().get_vector_at::<i32>(0)?;
    assert_eq!(child.reader()?.iter().collect::<Vec<_>>(), [None, None, None, None]);
    Ok(())
}

#[derive(Debug, AsDuckDB)]
#[duckdb(writer)]
struct NullStruct {
    n: i32,
}

#[test]
fn writer_struct_nulls_propagate_to_children() -> Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    let mut result = conn.query("SELECT {'n': 42} AS value", Parameters::None)?;
    let source = result.next_chunk()?.unwrap();
    let ty = source.get_vector_at::<NullStruct>(0)?.logical_type();
    let mut chunk = DataChunk::create(&conn, std::slice::from_ref(ty), true)?;
    let mut vector = chunk.get_vector_at_mut::<NullStruct>(0)?;
    vector.set_size(1)?;
    {
        let mut writer = vector.row_writer()?;
        writer.set(0, &NullStruct { n: 42 })?;
        writer.set_null(0)?;
    }
    {
        let mut writer = vector.writer()?;
        // SAFETY: This observes the mask without changing its bits.
        assert_eq!(unsafe { writer.validity_mut() }?[0] & 1, 0);
    }
    vector.row_writer()?.set(0, &NullStruct { n: 42 })?;
    // A native NULL also nulls the fields.
    vector.writer()?.set_null(0)?;
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
        writer.set(0, &42_i32.value(&conn)?)?;
        writer.set(1, &43_i32.value(&conn)?)?;
        // SAFETY: Both rows have been initialized; clearing a VARIANT row needs no child updates.
        unsafe { writer.validity_mut()?[0] &= !2 };
        writer.set(0, &Value::null(&conn, &i32::logical_type(&conn)?)?)?;
        writer.set(1, &7_i32.value(&conn)?)?;
        // SAFETY: This observes the refreshed mask without changing its bits.
        assert_eq!(unsafe { writer.validity_mut() }?[0] & 3, 2);
    }
    assert!(vector.reader()?.is_null(0)?);
    assert!(!vector.reader()?.is_null(1)?);
    Ok(())
}

#[test]
fn writer_cast_slow_writes_refresh_cached_validity() -> Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    let mut chunk = DataChunk::create(&conn, &[Variant::logical_type(&conn)?], true)?;
    let mut vector = chunk.get_vector_at_mut::<Unknown>(0)?.cast::<Variant>()?;
    vector.set_size(2)?;
    {
        let mut writer = vector.writer()?;
        writer.set(0, &42_i32.value(&conn)?)?;
        writer.set_null(1)?;
        assert_eq!(
            writer.validity.cast_const(),
            unsafe { writer.validity_mut() }.unwrap().as_ptr()
        );
        // SAFETY: Row zero contains an initialized VARIANT value with no NULL descendants to update.
        unsafe { writer.set_row_validity(0, false)? };
        assert_eq!(unsafe { writer.validity_mut() }.unwrap()[0] & 3, 0);
        writer.set(1, &7_i32.value(&conn)?)?;
        // SAFETY: This observes the refreshed mask without changing its bits.
        assert_eq!(unsafe { writer.validity_mut() }?[0] & 3, 2);
    }
    assert!(vector.reader()?.is_null(0)?);
    assert!(!vector.reader()?.is_null(1)?);
    Ok(())
}

#[test]
fn writer_rejects_native_input_layouts() -> Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    MicroScalar::create(
        "writer_input_probe",
        |_ctx, mut vector: Vector<'_, i32>, mut output: Vector<'_, i32>| {
            let original_kind = vector.storage_kind();
            assert!(vector.writer().is_err());
            assert_eq!(vector.storage_kind(), original_kind);
            if original_kind == StorageKind::Other {
                vector.flatten()?;
            }
            let reader = vector.reader()?;
            let mut writer = output.writer()?;
            for (index, value) in reader.iter().enumerate() {
                writer.set_option(index, value)?;
            }
            Ok(())
        },
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

#[test]
fn writer_reuses_validity_until_buffers_change() -> Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    let mut chunk = DataChunk::create(&conn, &[i32::logical_type(&conn)?], true)?;
    let mut vector = chunk.get_vector_at_mut::<i32>(0)?;
    vector.set_size(2)?;
    let first = {
        let mut writer = vector.writer()?;
        writer.set_null(0)?;
        writer.validity
    };
    assert_eq!(vector.writer()?.validity, first);
    // Growing past the chunk capacity replaces the mask, so the next writer must reacquire it.
    vector.set_size(5000)?;
    {
        let mut writer = vector.writer()?;
        writer.set_null(4999)?;
        writer.set(1, &7)?;
    }
    // Read through DuckDB, since a stale pointer would also be cached in this wrapper's view.
    assert!(vector.get_value_slow(4999)?.is_null()?);
    assert_eq!(vector.get_value_slow(1)?.get::<i32>()?, Some(7));
    Ok(())
}

#[test]
fn writer_typed_writes_follow_slow_writes() -> Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    let mut chunk = DataChunk::create(&conn, &[i32::logical_type(&conn)?], true)?;
    let mut vector = chunk.get_vector_at_mut::<i32>(0)?;
    vector.set_size(3)?;
    {
        let mut writer = vector.writer()?;
        writer.set(0, &1)?;
        writer.set_null_slow(1)?;
        writer.write_value_slow(2, &3_i32.value(&conn)?)?;
        // Slow writes refresh the cached buffers that typed writes use.
        writer.set(1, &2)?;
        writer.set(0, &4)?;
    }
    for (index, expected) in [4, 2, 3].into_iter().enumerate() {
        assert_eq!(vector.get_value_slow(index)?.get::<i32>()?, Some(expected));
    }
    Ok(())
}

#[test]
fn writer_marks_valid_ranges_across_mask_words() -> Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    let mut chunk = DataChunk::create(&conn, &[i32::logical_type(&conn)?], true)?;
    let mut vector = chunk.get_vector_at_mut::<i32>(0)?;
    vector.set_size(300)?;
    for (start, end) in [
        (0, 0),
        (5, 5),
        (0, 1),
        (3, 64),
        (0, 64),
        (63, 65),
        (10, 200),
        (64, 128),
        (1, 300),
    ] {
        let mut writer = vector.writer()?;
        // SAFETY: set_size initialized every row.
        unsafe {
            writer.fill_validity(false)?;
            writer.mark_valid_range(start, end);
        }
        let reader = vector.reader()?;
        for row in 0..300 {
            assert_eq!(
                !reader.is_null(row)?,
                (start..end).contains(&row),
                "{start}..{end} row {row}"
            );
        }
    }
    Ok(())
}

#[test]
fn writer_writes_slices_across_mask_words() -> Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    let mut chunk = DataChunk::create(&conn, &[i64::logical_type(&conn)?], true)?;
    let mut vector = chunk.get_vector_at_mut::<i64>(0)?;
    vector.set_size(200)?;
    let values = (0..130).collect::<Vec<i64>>();
    {
        let mut writer = vector.writer()?;
        writer.fill_null()?;
        writer.write_slice(30, &values)?;
        assert!(writer.write_slice(71, &values).is_err());
        assert!(writer.write_slice(usize::MAX, &values[..2]).is_err());
        writer.write_slice(200, &[])?;
    }
    let read = vector.reader()?.iter().map(|value| value.copied()).collect::<Vec<_>>();
    for (row, value) in read.into_iter().enumerate() {
        let expected = (30..160).contains(&row).then(|| row as i64 - 30);
        assert_eq!(value, expected, "row {row}");
    }
    Ok(())
}

#[test]
fn writer_copies_every_storage_layout() -> Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    MicroScalar::create(
        "copy_probe",
        |_ctx, mut input: Vector<'_, i32>, mut output: Vector<'_, i32>| {
            if input.storage_kind() == StorageKind::Other {
                input.flatten()?;
            }
            let reader = input.reader()?;
            // Pre-fill with garbage values so stale bits would show.
            output.writer()?.write_slice(0, &vec![-1; reader.len()])?;
            output.writer()?.copy_from(&reader)?;
            let copied = output.reader()?.iter().collect::<Vec<_>>();
            assert_eq!(copied, reader.iter().collect::<Vec<_>>());
            Ok(())
        },
    )
    .register(&conn)?;
    let mut result = conn.query(
        "SELECT copy_probe(test_vector) FROM test_vector_types(NULL::INTEGER, all_flat := false)",
        Parameters::None,
    )?;
    let mut rows = 0;
    while let Some(chunk) = result.next_chunk()? {
        rows += chunk.get_vector_at::<i32>(0)?.len();
    }
    assert!(rows > 0);
    Ok(())
}

#[test]
fn writer_fills_nested_nulls() -> Result<()> {
    #[derive(Debug, AsDuckDB)]
    #[duckdb(writer)]
    struct Pair {
        a: i32,
        b: String,
    }
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    let mut chunk = DataChunk::create(&conn, &[Pair::logical_type(&conn)?], true)?;
    let mut vector = chunk.get_vector_at_mut::<Pair>(0)?;
    vector.set_size(3)?;
    vector.writer()?.fill_null()?;
    assert!(vector.reader()?.iter().all(|row| row.is_none()));
    let fields = vector.children().unwrap();
    assert!(
        fields
            .get_vector_at::<i32>(0)?
            .reader()?
            .iter()
            .all(|row| row.is_none())
    );
    assert!(
        fields
            .get_vector_at::<String>(1)?
            .reader()?
            .iter()
            .all(|row| row.is_none())
    );
    Ok(())
}

use super::*;
use arrow::datatypes::{
    Int32Type, TimeUnit, TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType,
};
use libduckdb_sys::{duckdb_timestamp, duckdb_timestamp_ms, duckdb_timestamp_ns, duckdb_timestamp_s};

fn data_chunk_roundtrip_single_array(array: ArrayRef) -> Result<ArrayRef, Box<dyn Error>> {
    let input_data_type = array.data_type().clone();
    let chunk = single_array_data_chunk(array)?;
    let output = data_chunk_to_arrow(&chunk)?.column(0).clone();
    assert_eq!(output.data_type(), &input_data_type);

    Ok(output)
}

/// Asserts the chunk-level Arrow read reproduces `array` exactly.
#[track_caller]
fn assert_roundtrip_identity(array: ArrayRef) -> Result<(), Box<dyn Error>> {
    let output = data_chunk_roundtrip_single_array(array.clone())?;
    assert_eq!(output.as_ref(), array.as_ref());

    Ok(())
}

/// Builds a single-column chunk of `LIST(child)` committed to `rows` rows.
fn list_chunk(child: &LogicalTypeHandle, rows: usize) -> DataChunkHandle {
    let chunk = DataChunkHandle::new(&[LogicalTypeHandle::list(child)]);
    chunk.set_len(rows);
    chunk
}

/// Builds a `MAP(VARCHAR, INTEGER)` chunk with one row holding one entry.
fn single_entry_map_chunk() -> DataChunkHandle {
    let map_type = LogicalTypeHandle::map(
        &LogicalTypeHandle::from(LogicalTypeId::Varchar),
        &LogicalTypeHandle::from(LogicalTypeId::Integer),
    );
    let chunk = DataChunkHandle::new(&[map_type]);
    chunk.set_len(1);
    let mut map = chunk.list_vector(0);
    map.set_len(1);
    map.set_entry(0, 0, 1);
    let mut entries = map.struct_child(1);
    entries.child(0, 1).insert(0, "key");
    unsafe { entries.child(1, 1).copy(&[1_i32]) };
    chunk
}

fn manually_initialized_chunk_to_arrow(chunk: &mut DataChunkHandle) -> Result<RecordBatch, Box<dyn Error>> {
    unsafe { chunk.assume_initialized() };
    data_chunk_to_arrow(chunk)
}

#[track_caller]
fn assert_conversion_err(chunk: &mut DataChunkHandle, expected: &str) {
    assert_eq!(
        manually_initialized_chunk_to_arrow(chunk).unwrap_err().to_string(),
        expected
    );
}

#[test]
fn test_flat_vector_to_arrow_rejects_len_beyond_capacity() {
    let chunk = DataChunkHandle::new(&[LogicalTypeId::Integer.into()]);
    let vector = chunk.flat_vector(0);
    let len = vector.capacity() + 1;

    let err = flat_vector_to_arrow_array(&vector, len).unwrap_err();
    assert_eq!(
        err.to_string(),
        format!(
            "DuckDB vector length {len} exceeds backing capacity {}",
            vector.capacity()
        )
    );
}

#[test]
fn test_data_chunk_to_arrow_rejects_payload_under_construction() {
    let chunk = DataChunkHandle::new(&[LogicalTypeId::Integer.into()]);
    chunk.set_len(1);

    assert_eq!(
        data_chunk_to_arrow(&chunk).unwrap_err().to_string(),
        "DuckDB payload is still under construction; finish writable views, then read through an initialized owner; chunk writers must call DataChunkHandle::assume_initialized"
    );
}

#[test]
fn test_data_chunk_to_arrow_rejects_growth_after_initialization() {
    let mut chunk = DataChunkHandle::new(&[LogicalTypeId::Integer.into()]);
    unsafe { chunk.flat_vector(0).copy(&[42_i32]) };
    chunk.set_len(1);
    unsafe { chunk.assume_initialized() };
    assert!(data_chunk_to_arrow(&chunk).is_ok());

    chunk.set_len(2);

    assert_eq!(
        data_chunk_to_arrow(&chunk).unwrap_err().to_string(),
        "DuckDB payload is still under construction; finish writable views, then read through an initialized owner; chunk writers must call DataChunkHandle::assume_initialized"
    );
}

#[test]
fn test_data_chunk_to_arrow_returns_error_for_unsupported_nested_type() {
    let mut chunk = list_chunk(&LogicalTypeHandle::decimal(10, 2), 1);
    chunk.list_vector(0).set_entry(0, 0, 0);

    assert_conversion_err(
        &mut chunk,
        "DuckDB column 0: Cannot convert DuckDB logical type Decimal to Arrow",
    );
}

#[test]
fn test_data_chunk_to_arrow_rejects_list_entry_beyond_child_len() {
    let list_type = LogicalTypeHandle::list(&LogicalTypeId::Integer.into());
    let mut chunk = DataChunkHandle::new(&[LogicalTypeId::Integer.into(), list_type]);
    chunk.set_len(1);
    chunk.flat_vector(0).set_null(0);

    let mut list = chunk.list_vector(1);
    unsafe { list.set_child(&[10_i32]) };
    drop(list);
    unsafe { crate::core::set_list_entry_unchecked_for_test(&chunk, 1, 0, 0, 2) };

    assert_conversion_err(
        &mut chunk,
        "DuckDB column 1: DuckDB list entry at row 0 ends at 2, beyond child length 1",
    );
}

#[test]
fn test_data_chunk_to_arrow_rejects_overflowing_list_entry() {
    let mut chunk = list_chunk(&LogicalTypeId::Integer.into(), 1);
    unsafe { crate::core::set_list_entry_unchecked_for_test(&chunk, 0, 0, usize::MAX, 1) };

    assert_conversion_err(
        &mut chunk,
        "DuckDB column 0: DuckDB list entry at row 0 overflows usize",
    );
}

#[test]
fn test_data_chunk_to_arrow_accepts_empty_list_with_stale_offset() -> Result<(), Box<dyn Error>> {
    let mut chunk = list_chunk(&LogicalTypeId::Integer.into(), 1);
    chunk.list_vector(0).set_entry(0, usize::MAX, 0);

    let output = manually_initialized_chunk_to_arrow(&mut chunk)?;
    let output = output.column(0).as_list::<i32>();
    assert_eq!(output.value_offsets(), &[0, 0]);
    assert!(output.values().is_empty());
    assert!(output.is_valid(0));

    Ok(())
}

#[test]
fn test_data_chunk_to_arrow_rejects_array_in_list_beyond_child_len() {
    // The child length is measured in array-child rows, not flattened values,
    // so one committed row of ARRAY(2) rejects an entry ending at 2.
    let array_type = LogicalTypeHandle::array(&LogicalTypeId::Integer.into(), 2);
    let mut chunk = list_chunk(&array_type, 1);
    let mut list = chunk.list_vector(0);
    list.set_len(1);
    unsafe { list.array_child().set_child(&[0_i32, 0]) };
    drop(list);
    unsafe { crate::core::set_list_entry_unchecked_for_test(&chunk, 0, 0, 0, 2) };

    assert_conversion_err(
        &mut chunk,
        "DuckDB column 0: DuckDB list entry at row 0 ends at 2, beyond child length 1",
    );
}

#[test]
fn test_data_chunk_to_arrow_reads_list_larger_than_standard_vector() -> Result<(), Box<dyn Error>> {
    const CHILD_COUNT: usize = 3000;

    let mut chunk = list_chunk(&LogicalTypeId::Integer.into(), 1);
    let mut list = chunk.list_vector(0);
    let values = (0..CHILD_COUNT as i32).collect::<Vec<_>>();
    unsafe { list.set_child(&values) };
    list.set_entry(0, 0, CHILD_COUNT);
    drop(list);

    let output = manually_initialized_chunk_to_arrow(&mut chunk)?;
    let output = output.column(0).as_list::<i32>();
    let values = output.values().as_primitive::<Int32Type>();

    assert_eq!(output.value_length(0), CHILD_COUNT as i32);
    assert_eq!(values.value(CHILD_COUNT - 1), (CHILD_COUNT - 1) as i32);

    Ok(())
}

#[test]
fn test_data_chunk_to_arrow_reads_map_larger_than_standard_vector() -> Result<(), Box<dyn Error>> {
    const ENTRY_COUNT: usize = 3000;

    let map_type = LogicalTypeHandle::map(
        &LogicalTypeHandle::from(LogicalTypeId::Integer),
        &LogicalTypeHandle::from(LogicalTypeId::Integer),
    );
    let mut chunk = DataChunkHandle::new(&[map_type]);
    chunk.set_len(1);
    let mut map = chunk.list_vector(0);
    map.set_len(ENTRY_COUNT);
    map.set_entry(0, 0, ENTRY_COUNT);
    let mut entries = map.struct_child(ENTRY_COUNT);
    let values = (0..ENTRY_COUNT as i32).collect::<Vec<_>>();
    unsafe {
        entries.child(0, ENTRY_COUNT).copy(&values);
        entries.child(1, ENTRY_COUNT).copy(&values);
    }
    drop(entries);
    drop(map);

    let output = manually_initialized_chunk_to_arrow(&mut chunk)?;
    let output = output.column(0).as_map();
    let keys = output.keys().as_primitive::<Int32Type>();
    let values = output.values().as_primitive::<Int32Type>();

    assert_eq!(output.value_length(0), ENTRY_COUNT as i32);
    assert_eq!(keys.value(ENTRY_COUNT - 1), (ENTRY_COUNT - 1) as i32);
    assert_eq!(values.value(ENTRY_COUNT - 1), (ENTRY_COUNT - 1) as i32);

    Ok(())
}

#[test]
fn test_data_chunk_to_arrow_handles_empty_list_chunk() -> Result<(), Box<dyn Error>> {
    let lists = ListArray::new(
        Arc::new(Field::new("item", DataType::Int32, true)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0])),
        Arc::new(Int32Array::from(Vec::<i32>::new())),
        None,
    );

    assert_roundtrip_identity(Arc::new(lists))
}

#[test]
fn test_data_chunk_to_arrow_preserves_top_level_struct_nulls() -> Result<(), Box<dyn Error>> {
    let fields = Fields::from(vec![
        Field::new("i", DataType::Int32, true),
        Field::new("s", DataType::Utf8, true),
    ]);
    let struct_array = StructArray::new(
        fields,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
            Arc::new(StringArray::from(vec!["one", "hidden", "three"])) as ArrayRef,
        ],
        Some(NullBuffer::from([true, false, true].as_slice())),
    );

    let output = data_chunk_roundtrip_single_array(Arc::new(struct_array))?;
    let output = output.as_struct();
    assert!(output.is_valid(0));
    assert!(output.is_null(1));
    assert!(output.is_valid(2));
    assert!(output.column(0).is_null(1));
    assert!(output.column(1).is_null(1));

    Ok(())
}

#[test]
fn test_data_chunk_to_arrow_ignores_null_list_entries() -> Result<(), Box<dyn Error>> {
    let mut chunk = list_chunk(&LogicalTypeId::Integer.into(), 3);
    let mut list = chunk.list_vector(0);
    unsafe { list.set_child(&[10_i32, 11, 30]) };
    list.set_entry(0, 0, 2);
    list.set_entry(2, 2, 1);
    list.set_null(1);
    list.set_entry(1, usize::MAX, usize::MAX);
    drop(list);

    let output = manually_initialized_chunk_to_arrow(&mut chunk)?;
    let output = output.column(0).as_list::<i32>();
    assert_eq!(output.value_offsets(), &[0, 2, 2, 3]);
    assert!(output.is_null(1));
    assert_eq!(output.values().as_primitive::<Int32Type>().values(), &[10, 11, 30]);

    Ok(())
}

#[test]
fn test_data_chunk_to_arrow_gathers_non_contiguous_list_entries() -> Result<(), Box<dyn Error>> {
    let mut chunk = list_chunk(&LogicalTypeId::Integer.into(), 2);
    let mut list = chunk.list_vector(0);
    unsafe { list.set_child(&[99_i32, 10, 11, 98, 30]) };
    list.set_entry(0, 1, 2);
    list.set_entry(1, 4, 1);
    drop(list);

    let output = manually_initialized_chunk_to_arrow(&mut chunk)?;
    let output = output.column(0).as_list::<i32>();
    assert_eq!(output.value_offsets(), &[0, 2, 3]);
    assert_eq!(output.values().as_primitive::<Int32Type>().values(), &[10, 11, 30]);

    Ok(())
}

#[test]
fn test_data_chunk_to_arrow_gathers_non_contiguous_nested_lists() -> Result<(), Box<dyn Error>> {
    let inner_type = LogicalTypeHandle::list(&LogicalTypeId::Integer.into());
    let mut chunk = list_chunk(&inner_type, 2);
    let mut outer = chunk.list_vector(0);
    outer.set_len(5);
    outer.set_entry(0, 1, 2);
    outer.set_entry(1, 4, 1);

    let mut inner = outer.list_child();
    unsafe { inner.set_child(&[10_i32, 11, 20, 40, 41]) };
    inner.set_entry(1, 0, 2);
    inner.set_entry(2, 2, 1);
    inner.set_entry(4, 3, 2);
    drop(inner);
    drop(outer);

    let output = manually_initialized_chunk_to_arrow(&mut chunk)?;
    let output = output.column(0).as_list::<i32>();
    let inner = output.values().as_list::<i32>();
    let values = inner.values().as_primitive::<Int32Type>();

    assert_eq!(output.value_offsets(), &[0, 2, 3]);
    assert_eq!(inner.value_offsets(), &[0, 2, 3, 5]);
    assert_eq!(values.values(), &[10, 11, 20, 40, 41]);

    Ok(())
}

#[test]
fn test_data_chunk_to_arrow_masks_array_children_under_null_parents() -> Result<(), Box<dyn Error>> {
    let array_type = LogicalTypeHandle::array(&LogicalTypeId::Varchar.into(), 2);
    let mut chunk = DataChunkHandle::new(&[array_type]);
    chunk.set_len(2);

    let mut array = chunk.array_vector(0);
    let mut child = array.child(4);
    child.insert(0, "one");
    child.insert(1, "two");
    drop(child);
    array.set_null(1);
    drop(array);

    let output = manually_initialized_chunk_to_arrow(&mut chunk)?;
    let output = output.column(0).as_fixed_size_list();
    let values = output.values().as_string::<i32>();
    assert_eq!(values.value(0), "one");
    assert_eq!(values.value(1), "two");
    assert!(values.is_null(2));
    assert!(values.is_null(3));
    assert!(output.is_null(1));

    Ok(())
}

#[test]
fn test_data_chunk_to_arrow_gathers_sparse_array_rows_in_lists() -> Result<(), Box<dyn Error>> {
    let array_type = LogicalTypeHandle::array(&LogicalTypeId::Integer.into(), 2);
    let mut chunk = list_chunk(&array_type, 2);
    let mut list = chunk.list_vector(0);
    list.set_len(4);
    let mut arrays = list.array_child();
    unsafe { arrays.set_child(&[90_i32, 91, 10, 11, 80, 81, 30, 31]) };
    drop(arrays);
    list.set_entry(0, 1, 1);
    list.set_entry(1, 3, 1);
    drop(list);

    let output = manually_initialized_chunk_to_arrow(&mut chunk)?;
    let output = output.column(0).as_list::<i32>();
    let arrays = output.values().as_fixed_size_list();
    let values = arrays.values().as_primitive::<Int32Type>();

    assert_eq!(output.value_offsets(), &[0, 1, 2]);
    assert_eq!(values.values(), &[10, 11, 30, 31]);

    Ok(())
}

#[test]
fn test_data_chunk_to_arrow_masks_unwritten_struct_children() -> Result<(), Box<dyn Error>> {
    let struct_type = LogicalTypeHandle::struct_type(&[("value", LogicalTypeHandle::from(LogicalTypeId::Varchar))]);
    let mut chunk = DataChunkHandle::new(&[struct_type]);
    chunk.set_len(1);

    let mut struct_vector = chunk.struct_vector(0);
    struct_vector.set_null(0);
    drop(struct_vector);

    let output = manually_initialized_chunk_to_arrow(&mut chunk)?;
    let output = output.column(0).as_struct();
    let values = output.column(0).as_string::<i32>();

    assert!(output.is_null(0));
    assert!(values.is_null(0));

    Ok(())
}

#[test]
fn test_data_chunk_to_arrow_rejects_invalid_utf8_struct_field_name() {
    use std::ffi::CString;

    let child = LogicalTypeHandle::from(LogicalTypeId::Integer);
    let name = CString::from_vec_with_nul(vec![b'f', 0x80, 0]).unwrap();
    let mut child_types = [child.ptr];
    let mut child_names = [name.as_ptr()];
    let ptr = unsafe { crate::ffi::duckdb_create_struct_type(child_types.as_mut_ptr(), child_names.as_mut_ptr(), 1) };
    let struct_type = unsafe { LogicalTypeHandle::new(ptr) };
    let mut chunk = DataChunkHandle::new(&[struct_type]);

    assert_conversion_err(
        &mut chunk,
        "DuckDB column 0: invalid utf-8 sequence of 1 bytes from index 1",
    );
}

#[test]
fn test_data_chunk_to_arrow_rejects_null_map_entries() {
    let mut chunk = single_entry_map_chunk();
    chunk.list_vector(0).struct_child(1).set_null(0);

    assert_conversion_err(&mut chunk, "DuckDB column 0: DuckDB map entry at child row 0 is null");
}

#[test]
fn test_data_chunk_to_arrow_rejects_null_map_keys() {
    let mut chunk = single_entry_map_chunk();
    chunk.list_vector(0).struct_child(1).child(0, 1).set_null(0);

    assert_conversion_err(&mut chunk, "DuckDB column 0: DuckDB map key at child row 0 is null");
}

#[test]
fn test_data_chunk_to_arrow_masks_invalid_boolean_payload_under_null() -> Result<(), Box<dyn Error>> {
    let mut chunk = DataChunkHandle::new(&[LogicalTypeId::Boolean.into()]);
    chunk.set_len(1);
    let mut vector = chunk.flat_vector(0);
    // A null slot need not contain a valid Rust `bool` representation.
    unsafe { vector.copy(&[2_u8]) };
    vector.set_null(0);
    drop(vector);

    let output = manually_initialized_chunk_to_arrow(&mut chunk)?;

    assert!(output.column(0).as_boolean().is_null(0));

    Ok(())
}

#[test]
fn test_data_chunk_to_arrow_preserves_timestamp_carriers() -> Result<(), Box<dyn Error>> {
    let mut chunk = DataChunkHandle::new(&[
        LogicalTypeId::TimestampS.into(),
        LogicalTypeId::TimestampMs.into(),
        LogicalTypeId::Timestamp.into(),
        LogicalTypeId::TimestampNs.into(),
        LogicalTypeId::TimestampTZ.into(),
    ]);
    chunk.set_len(2);

    // 2024-01-01T00:00:00Z plus a unit-specific fractional component.
    unsafe {
        chunk.flat_vector(0).copy(&[
            duckdb_timestamp_s { seconds: -1 },
            duckdb_timestamp_s { seconds: 1_704_067_200 },
        ]);
        chunk.flat_vector(1).copy(&[
            duckdb_timestamp_ms { millis: -1_001 },
            duckdb_timestamp_ms {
                millis: 1_704_067_200_123,
            },
        ]);
        chunk.flat_vector(2).copy(&[
            duckdb_timestamp { micros: -1_000_001 },
            duckdb_timestamp {
                micros: 1_704_067_200_123_456,
            },
        ]);
        chunk.flat_vector(3).copy(&[
            duckdb_timestamp_ns { nanos: -1_000_000_123 },
            duckdb_timestamp_ns {
                nanos: 1_704_067_200_000_000_123,
            },
        ]);
        chunk.flat_vector(4).copy(&[
            duckdb_timestamp { micros: -1_000_001 },
            duckdb_timestamp {
                micros: 1_704_067_200_123_456,
            },
        ]);
    }

    let output = manually_initialized_chunk_to_arrow(&mut chunk)?;
    let expected_types = [
        DataType::Timestamp(TimeUnit::Second, None),
        DataType::Timestamp(TimeUnit::Millisecond, None),
        DataType::Timestamp(TimeUnit::Microsecond, None),
        DataType::Timestamp(TimeUnit::Nanosecond, None),
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
    ];
    for (column, expected) in expected_types.iter().enumerate() {
        assert_eq!(output.column(column).data_type(), expected, "column {column}");
    }
    assert_eq!(
        output.column(0).as_primitive::<TimestampSecondType>().values(),
        &[-1, 1_704_067_200]
    );
    assert_eq!(
        output.column(1).as_primitive::<TimestampMillisecondType>().values(),
        &[-1_001, 1_704_067_200_123]
    );
    assert_eq!(
        output.column(2).as_primitive::<TimestampMicrosecondType>().values(),
        &[-1_000_001, 1_704_067_200_123_456]
    );
    assert_eq!(
        output.column(3).as_primitive::<TimestampNanosecondType>().values(),
        &[-1_000_000_123, 1_704_067_200_000_000_123]
    );
    assert_eq!(
        output.column(4).as_primitive::<TimestampMicrosecondType>().values(),
        &[-1_000_001, 1_704_067_200_123_456]
    );

    Ok(())
}

#[test]
fn test_data_chunk_to_arrow_preserves_nested_nanosecond_timestamp() -> Result<(), Box<dyn Error>> {
    let mut chunk = list_chunk(&LogicalTypeId::TimestampNs.into(), 1);
    let mut list = chunk.list_vector(0);
    unsafe {
        list.set_child(&[
            duckdb_timestamp_ns { nanos: -123 },
            duckdb_timestamp_ns {
                nanos: 1_704_067_200_000_000_123,
            },
        ]);
    }
    list.set_entry(0, 0, 2);
    drop(list);

    let output = manually_initialized_chunk_to_arrow(&mut chunk)?;
    let output = output.column(0).as_list::<i32>();
    assert_eq!(
        output.values().data_type(),
        &DataType::Timestamp(TimeUnit::Nanosecond, None)
    );
    assert_eq!(
        output.values().as_primitive::<TimestampNanosecondType>().values(),
        &[-123, 1_704_067_200_000_000_123]
    );

    Ok(())
}

#[test]
fn test_data_chunk_to_arrow_recurses_for_list_of_structs() -> Result<(), Box<dyn Error>> {
    let struct_values = StructArray::from(vec![
        (
            Arc::new(Field::new("i", DataType::Int32, true)),
            Arc::new(Int32Array::from(vec![Some(1), None, Some(3)])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("s", DataType::Utf8, true)),
            Arc::new(StringArray::from(vec![Some("one"), Some("two"), None])) as ArrayRef,
        ),
    ]);
    let lists = ListArray::new(
        Arc::new(Field::new("item", struct_values.data_type().clone(), true)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0, 2, 2, 3])),
        Arc::new(struct_values),
        Some(NullBuffer::from([true, false, true].as_slice())),
    );

    assert_roundtrip_identity(Arc::new(lists))
}

#[test]
fn test_data_chunk_to_arrow_recurses_for_map_with_list_values() -> Result<(), Box<dyn Error>> {
    let list_values = ListArray::new(
        Arc::new(Field::new("item", DataType::Int32, true)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0, 2, 2, 4])),
        Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3), None])),
        Some(NullBuffer::from([true, false, true].as_slice())),
    );
    let entries = StructArray::from(vec![
        (
            Arc::new(Field::new("keys", DataType::Utf8, false)),
            Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("values", list_values.data_type().clone(), true)),
            Arc::new(list_values) as ArrayRef,
        ),
    ]);
    let map = MapArray::new(
        Arc::new(Field::new("entries", entries.data_type().clone(), false)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0, 2, 2, 3])),
        entries,
        Some(NullBuffer::from([true, false, true].as_slice())),
        false,
    );

    assert_roundtrip_identity(Arc::new(map))
}

#[test]
fn test_data_chunk_to_arrow_recurses_for_fixed_size_list() -> Result<(), Box<dyn Error>> {
    let struct_values = StructArray::from(vec![
        (
            Arc::new(Field::new("i", DataType::Int32, true)),
            Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3), Some(4)])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("s", DataType::Utf8, true)),
            Arc::new(StringArray::from(vec![Some("one"), None, Some("three"), Some("four")])) as ArrayRef,
        ),
    ]);
    let lists = FixedSizeListArray::new(
        Arc::new(Field::new("item", struct_values.data_type().clone(), true)),
        2,
        Arc::new(struct_values),
        Some(NullBuffer::from([true, false].as_slice())),
    );

    assert_roundtrip_identity(Arc::new(lists))
}

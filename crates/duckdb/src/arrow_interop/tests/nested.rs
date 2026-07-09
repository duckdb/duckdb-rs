use super::*;

#[test]
fn test_struct_recurses_for_string_variants() -> Result<(), Box<dyn Error>> {
    let struct_array = StructArray::from(vec![
        (
            Arc::new(Field::new("view", DataType::Utf8View, true)),
            Arc::new(StringViewArray::from(vec![Some("foo"), None, Some("baz")])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("large", DataType::LargeUtf8, true)),
            Arc::new(LargeStringArray::from(vec![Some("one"), Some("two"), None])) as ArrayRef,
        ),
    ]);

    let output = roundtrip_single_array(Arc::new(struct_array))?;
    let struct_column = output.as_any().downcast_ref::<StructArray>().unwrap();

    let view_field = struct_column.column(0).as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(view_field.value(0), "foo");
    assert!(view_field.is_null(1));
    assert_eq!(view_field.value(2), "baz");

    let large_field = struct_column.column(1).as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(large_field.value(0), "one");
    assert_eq!(large_field.value(1), "two");
    assert!(large_field.is_null(2));

    Ok(())
}

fn assert_fixed_size_list_strings(
    array: FixedSizeListArray,
    expected: &[[Option<&str>; 2]],
) -> Result<(), Box<dyn Error>> {
    let output = roundtrip_single_array(Arc::new(array))?;
    let list_column = output.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
    assert_eq!(list_column.len(), expected.len());

    for (row, expected_values) in expected.iter().enumerate() {
        let list = list_column.value(row);
        let values = list.as_any().downcast_ref::<StringArray>().unwrap();
        for (index, expected_value) in expected_values.iter().enumerate() {
            match expected_value {
                Some(value) => assert_eq!(values.value(index), *value),
                None => assert!(values.is_null(index)),
            }
        }
    }

    Ok(())
}

#[test]
fn test_fixed_size_list_recurses_for_string_variants() -> Result<(), Box<dyn Error>> {
    let fixed_size_lists = FixedSizeListArray::new(
        Arc::new(Field::new("item", DataType::Utf8View, true)),
        2,
        Arc::new(StringViewArray::from(vec![Some("foo"), None, Some("bar"), Some("baz")])),
        None,
    );
    assert_fixed_size_list_strings(fixed_size_lists, &[[Some("foo"), None], [Some("bar"), Some("baz")]])?;

    let large_values = LargeStringArray::from(vec![Some("red"), Some("green"), None, Some("blue")]);
    let fixed_size_lists = FixedSizeListArray::new(
        Arc::new(Field::new("item", DataType::LargeUtf8, true)),
        2,
        Arc::new(large_values),
        None,
    );
    assert_fixed_size_list_strings(fixed_size_lists, &[[Some("red"), Some("green")], [None, Some("blue")]])?;

    Ok(())
}

#[test]
fn test_list_recurses_for_large_string_binary_and_large_list_children() -> Result<(), Box<dyn Error>> {
    let large_strings = LargeStringArray::from(vec![Some("alpha"), None, Some("gamma")]);
    let list_of_large_strings = ListArray::new(
        Arc::new(Field::new("item", DataType::LargeUtf8, true)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0, 2, 3])),
        Arc::new(large_strings),
        None,
    );
    let output = roundtrip_single_array(Arc::new(list_of_large_strings))?;
    let output = output.as_any().downcast_ref::<ListArray>().unwrap();
    let first_list = output.value(0);
    let first_strings = first_list.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(first_strings.value(0), "alpha");
    assert!(first_strings.is_null(1));
    let second_list = output.value(1);
    let second_strings = second_list.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(second_strings.value(0), "gamma");

    let large_binary = LargeBinaryArray::from_opt_vec(vec![Some(&b"aa"[..]), None, Some(&b"ccc"[..])]);
    let list_of_large_binary = ListArray::new(
        Arc::new(Field::new("item", DataType::LargeBinary, true)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0, 2, 3])),
        Arc::new(large_binary),
        None,
    );
    let output = roundtrip_single_array(Arc::new(list_of_large_binary))?;
    let output = output.as_any().downcast_ref::<ListArray>().unwrap();
    let first_list = output.value(0);
    let first_binary = first_list.as_any().downcast_ref::<BinaryArray>().unwrap();
    assert_eq!(first_binary.value(0), b"aa");
    assert!(first_binary.is_null(1));
    let second_list = output.value(1);
    let second_binary = second_list.as_any().downcast_ref::<BinaryArray>().unwrap();
    assert_eq!(second_binary.value(0), b"ccc");

    let large_lists = LargeListArray::new(
        Arc::new(Field::new("item", DataType::Int32, true)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0_i64, 2, 2, 3])),
        Arc::new(Int32Array::from(vec![Some(1), None, Some(3)])),
        Some(vec![true, false, true].into()),
    );
    let list_of_large_lists = ListArray::new(
        Arc::new(Field::new("item", large_lists.data_type().clone(), true)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0, 2, 3])),
        Arc::new(large_lists),
        None,
    );
    let output = roundtrip_single_array(Arc::new(list_of_large_lists))?;
    let output = output.as_any().downcast_ref::<ListArray>().unwrap();
    let first_list = output.value(0);
    let nested_lists = first_list.as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(nested_lists.len(), 2);
    assert!(nested_lists.is_valid(0));
    assert!(nested_lists.is_null(1));
    let first_nested = nested_lists.value(0);
    let first_nested_values = first_nested.as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(first_nested_values.value(0), 1);
    assert!(first_nested_values.is_null(1));
    let second_list = output.value(1);
    let nested_lists = second_list.as_any().downcast_ref::<ListArray>().unwrap();
    let third_nested = nested_lists.value(0);
    let third_nested_values = third_nested.as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(third_nested_values.value(0), 3);

    Ok(())
}

#[test]
fn test_struct_recurses_for_map_children() -> Result<(), Box<dyn Error>> {
    let keys = ["a", "b", "c"];
    let values = UInt32Array::from(vec![Some(1), None, Some(3)]);
    let offsets = [0, 2, 3];
    let map_array = MapArray::new_from_strings(keys.into_iter(), &values, &offsets)?;
    let expected_map = map_array.clone();

    let struct_array = StructArray::from(vec![(
        Arc::new(Field::new("lookup", map_array.data_type().clone(), true)),
        Arc::new(map_array) as ArrayRef,
    )]);

    let output = roundtrip_single_array(Arc::new(struct_array))?;
    let struct_column = output.as_any().downcast_ref::<StructArray>().unwrap();
    let map_field = struct_column.column(0).as_any().downcast_ref::<MapArray>().unwrap();
    assert_eq!(map_field.keys(), expected_map.keys());
    assert_eq!(map_field.values(), expected_map.values());
    assert_eq!(map_field.value_offsets(), expected_map.value_offsets());

    Ok(())
}

#[test]
fn test_struct_child_unsupported_type_returns_error() -> Result<(), Box<dyn Error>> {
    let dictionary = DictionaryArray::<Int8Type>::try_new(
        Int8Array::from(vec![Some(0), Some(1), Some(0)]),
        Arc::new(StringArray::from(vec!["alpha", "beta"])),
    )?;
    let struct_array = StructArray::from(vec![(
        Arc::new(Field::new("dict", dictionary.data_type().clone(), true)),
        Arc::new(dictionary) as ArrayRef,
    )]);
    let schema = Schema::new(vec![Field::new("a", struct_array.data_type().clone(), true)]);
    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(struct_array) as ArrayRef])?;
    let logical_type = to_duckdb_logical_type(batch.column(0).data_type())?;
    let mut chunk = DataChunkHandle::new(&[logical_type]);

    let err = record_batch_to_duckdb_data_chunk(&batch, &mut chunk).unwrap_err();
    assert!(
        err.to_string().contains("Dictionary") && err.to_string().contains("not supported yet"),
        "unexpected error: {err}"
    );

    Ok(())
}

#[test]
fn test_fixed_size_list_recurses_for_struct_children() -> Result<(), Box<dyn Error>> {
    let int_values = Int32Array::from(vec![Some(1), Some(2), Some(3), Some(4), None, Some(6)]);
    let string_values = StringArray::from(vec![
        Some("one"),
        Some("two"),
        Some("three"),
        Some("four"),
        None,
        Some("six"),
    ]);

    let struct_array = StructArray::from(vec![
        (
            Arc::new(Field::new("i", DataType::Int32, true)),
            Arc::new(int_values) as ArrayRef,
        ),
        (
            Arc::new(Field::new("s", DataType::Utf8, true)),
            Arc::new(string_values) as ArrayRef,
        ),
    ]);

    let fixed_size_lists = FixedSizeListArray::new(
        Arc::new(Field::new("item", struct_array.data_type().clone(), true)),
        2,
        Arc::new(struct_array),
        Some(vec![true, false, true].into()),
    );

    let output = roundtrip_single_array(Arc::new(fixed_size_lists))?;
    let output = output.as_any().downcast_ref::<FixedSizeListArray>().unwrap();

    assert_eq!(output.len(), 3);
    assert!(output.is_valid(0));
    assert!(output.is_null(1));
    assert!(output.is_valid(2));

    let first_list = output.value(0);
    let first_structs = first_list.as_any().downcast_ref::<StructArray>().unwrap();
    let first_ints = first_structs.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
    let first_strings = first_structs.column(1).as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(first_ints.value(0), 1);
    assert_eq!(first_ints.value(1), 2);
    assert_eq!(first_strings.value(0), "one");
    assert_eq!(first_strings.value(1), "two");

    let third_list = output.value(2);
    let third_structs = third_list.as_any().downcast_ref::<StructArray>().unwrap();
    let third_ints = third_structs.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
    let third_strings = third_structs.column(1).as_any().downcast_ref::<StringArray>().unwrap();
    assert!(third_ints.is_null(0));
    assert_eq!(third_ints.value(1), 6);
    assert!(third_strings.is_null(0));
    assert_eq!(third_strings.value(1), "six");

    Ok(())
}

#[test]
fn test_fixed_size_list_recurses_for_nested_container_children() -> Result<(), Box<dyn Error>> {
    let list_values = ListArray::new(
        Arc::new(Field::new("item", DataType::Int32, true)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0, 2, 2, 5, 6])),
        Arc::new(Int32Array::from(vec![
            Some(1),
            Some(2),
            Some(3),
            None,
            Some(5),
            Some(6),
        ])),
        Some(vec![true, false, true, true].into()),
    );
    let fixed_size_lists = FixedSizeListArray::new(
        Arc::new(Field::new("item", list_values.data_type().clone(), true)),
        2,
        Arc::new(list_values),
        None,
    );
    let output = roundtrip_single_array(Arc::new(fixed_size_lists))?;
    let output = output.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
    let first_value = output.value(0);
    let first_lists = first_value.as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(first_lists.len(), 2);
    assert_eq!(
        first_lists
            .value(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(1),
        2
    );
    assert!(first_lists.is_null(1));
    let second_value = output.value(1);
    let second_lists = second_value.as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(second_lists.value_length(0), 3);
    assert_eq!(
        second_lists
            .value(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(0),
        6
    );

    let keys = ["a", "b", "c", "d", "e"];
    let values = UInt32Array::from(vec![Some(1), None, Some(3), Some(4), Some(5)]);
    let map_values = MapArray::new_from_strings(keys.into_iter(), &values, &[0, 2, 2, 3, 5])?;
    let fixed_size_maps = FixedSizeListArray::new(
        Arc::new(Field::new("item", map_values.data_type().clone(), true)),
        2,
        Arc::new(map_values),
        None,
    );
    let output = roundtrip_single_array(Arc::new(fixed_size_maps))?;
    let output = output.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
    let first_value = output.value(0);
    let first_maps = first_value.as_any().downcast_ref::<MapArray>().unwrap();
    assert_eq!(first_maps.value_offsets(), &[0, 2, 2]);
    let second_value = output.value(1);
    let second_maps = second_value.as_any().downcast_ref::<MapArray>().unwrap();
    assert_eq!(second_maps.value_length(0), 1);
    assert_eq!(second_maps.value_length(1), 2);

    let inner_fixed_size_lists = FixedSizeListArray::new(
        Arc::new(Field::new("item", DataType::Int32, true)),
        2,
        Arc::new(Int32Array::from(vec![
            Some(1),
            Some(2),
            Some(3),
            None,
            Some(5),
            Some(6),
            None,
            Some(8),
        ])),
        Some(vec![true, false, true, true].into()),
    );
    let fixed_size_lists = FixedSizeListArray::new(
        Arc::new(Field::new("item", inner_fixed_size_lists.data_type().clone(), true)),
        2,
        Arc::new(inner_fixed_size_lists),
        None,
    );
    let output = roundtrip_single_array(Arc::new(fixed_size_lists))?;
    let output = output.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
    let first_value = output.value(0);
    let first_lists = first_value.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
    assert_eq!(first_lists.len(), 2);
    assert!(first_lists.is_null(1));
    let second_value = output.value(1);
    let second_lists = second_value.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
    assert_eq!(
        second_lists
            .value(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(1),
        6
    );
    assert!(
        second_lists
            .value(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .is_null(0)
    );

    Ok(())
}

#[test]
fn test_struct_recurses_for_list_of_map_children() -> Result<(), Box<dyn Error>> {
    let keys = ["a", "b", "c", "d", "e"];
    let values = UInt32Array::from(vec![Some(1), None, Some(3), Some(4), Some(5)]);
    let map_offsets = [0, 2, 3, 3, 5];
    let maps = MapArray::new_from_strings(keys.into_iter(), &values, &map_offsets)?;

    let list_of_maps = ListArray::new(
        Arc::new(Field::new("item", maps.data_type().clone(), true)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0, 2, 4])),
        Arc::new(maps),
        None,
    );

    let struct_array = StructArray::from(vec![(
        Arc::new(Field::new("maps", list_of_maps.data_type().clone(), true)),
        Arc::new(list_of_maps) as ArrayRef,
    )]);

    let output = roundtrip_single_array(Arc::new(struct_array))?;
    let output = output.as_any().downcast_ref::<StructArray>().unwrap();
    let list_field = output.column(0).as_any().downcast_ref::<ListArray>().unwrap();

    let first_list = list_field.value(0);
    let first_maps = first_list.as_any().downcast_ref::<MapArray>().unwrap();
    assert_eq!(first_maps.len(), 2);
    assert_eq!(first_maps.value_length(0), 2);
    assert_eq!(first_maps.value_length(1), 1);
    let first_keys = first_maps.keys().as_any().downcast_ref::<StringArray>().unwrap();
    let first_values = first_maps.values().as_any().downcast_ref::<UInt32Array>().unwrap();
    assert_eq!(first_keys.value(0), "a");
    assert_eq!(first_keys.value(1), "b");
    assert_eq!(first_keys.value(2), "c");
    assert_eq!(first_values.value(0), 1);
    assert!(first_values.is_null(1));
    assert_eq!(first_values.value(2), 3);

    let second_list = list_field.value(1);
    let second_maps = second_list.as_any().downcast_ref::<MapArray>().unwrap();
    assert_eq!(second_maps.len(), 2);
    assert_eq!(second_maps.value_length(0), 0);
    assert_eq!(second_maps.value_length(1), 2);
    let second_keys = second_maps.keys().as_any().downcast_ref::<StringArray>().unwrap();
    let second_values = second_maps.values().as_any().downcast_ref::<UInt32Array>().unwrap();
    let entry_offset = second_maps.value_offsets()[1] as usize;
    assert_eq!(second_keys.value(entry_offset), "d");
    assert_eq!(second_keys.value(entry_offset + 1), "e");
    assert_eq!(second_values.value(entry_offset), 4);
    assert_eq!(second_values.value(entry_offset + 1), 5);

    Ok(())
}

#[test]
fn test_map_recurses_for_list_values() -> Result<(), Box<dyn Error>> {
    let list_values = ListArray::new(
        Arc::new(Field::new("item", DataType::Int32, true)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0, 2, 2, 5])),
        Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3), None, Some(5)])),
        Some(vec![true, false, true].into()),
    );

    let keys = ["a", "b", "c"];
    let offsets = [0, 2, 3];
    let map_array = MapArray::new_from_strings(keys.into_iter(), &list_values, &offsets)?;

    let output = roundtrip_single_array(Arc::new(map_array))?;
    let output = output.as_any().downcast_ref::<MapArray>().unwrap();
    assert_eq!(output.value_offsets(), &[0, 2, 3]);

    let output_values = output.values().as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(output_values.len(), 3);
    assert!(output_values.is_valid(0));
    assert!(output_values.is_null(1));
    assert!(output_values.is_valid(2));

    let first_value = output_values.value(0);
    let first_ints = first_value.as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(first_ints.value(0), 1);
    assert_eq!(first_ints.value(1), 2);

    let third_value = output_values.value(2);
    let third_ints = third_value.as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(third_ints.value(0), 3);
    assert!(third_ints.is_null(1));
    assert_eq!(third_ints.value(2), 5);

    Ok(())
}

#[test]
fn test_sliced_nested_list_roundtrip() -> Result<(), Box<dyn Error>> {
    let struct_values = StructArray::from(vec![
        (
            Arc::new(Field::new("i", DataType::Int32, true)),
            Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3), None, Some(5)])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("s", DataType::Utf8, true)),
            Arc::new(StringArray::from(vec![
                Some("one"),
                Some("two"),
                Some("three"),
                None,
                Some("five"),
            ])) as ArrayRef,
        ),
    ]);

    let lists = ListArray::new(
        Arc::new(Field::new("item", struct_values.data_type().clone(), true)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0, 1, 3, 3, 5])),
        Arc::new(struct_values),
        Some(vec![true, true, false, true].into()),
    );

    let output = roundtrip_single_array(Arc::new(lists.slice(1, 2)))?;
    let output = output.as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(output.len(), 2);
    assert!(output.is_valid(0));
    assert!(output.is_null(1));

    let first_list = output.value(0);
    let first_structs = first_list.as_any().downcast_ref::<StructArray>().unwrap();
    let ints = first_structs.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
    let strings = first_structs.column(1).as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(ints.value(0), 2);
    assert_eq!(ints.value(1), 3);
    assert_eq!(strings.value(0), "two");
    assert_eq!(strings.value(1), "three");

    Ok(())
}

#[test]
fn test_sliced_fixed_size_list_roundtrip_uses_sliced_values() -> Result<(), Box<dyn Error>> {
    let fixed_size_lists = FixedSizeListArray::new(
        Arc::new(Field::new("item", DataType::Utf8, true)),
        2,
        Arc::new(StringArray::from(vec![
            Some("skip-0"),
            Some("skip-1"),
            Some("keep-0"),
            Some("keep-1"),
            Some("null-0"),
            Some("null-1"),
            Some("keep-2"),
            Some("keep-3"),
        ])),
        Some(NullBuffer::from([true, true, false, true].as_slice())),
    );

    let output = roundtrip_single_array(Arc::new(fixed_size_lists.slice(1, 2)))?;
    let output = output.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
    assert_eq!(output.len(), 2);
    assert!(output.is_valid(0));
    assert!(output.is_null(1));

    let first_list = output.value(0);
    let first_values = first_list.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(first_values.value(0), "keep-0");
    assert_eq!(first_values.value(1), "keep-1");

    Ok(())
}

#[test]
fn test_list_child_reserves_capacity_for_large_nested_children() -> Result<(), Box<dyn Error>> {
    let child_count: i32 = 3000;

    let nested_offsets = (0..=child_count).collect::<Vec<_>>();
    let nested_lists = ListArray::new(
        Arc::new(Field::new("item", DataType::Int32, false)),
        OffsetBuffer::new(ScalarBuffer::from(nested_offsets)),
        Arc::new(Int32Array::from((0..child_count).collect::<Vec<_>>())),
        None,
    );
    let list_of_lists = ListArray::new(
        Arc::new(Field::new("item", nested_lists.data_type().clone(), true)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0, child_count])),
        Arc::new(nested_lists),
        None,
    );

    let output = roundtrip_single_array(Arc::new(list_of_lists))?;
    let output = output.as_any().downcast_ref::<ListArray>().unwrap();
    let nested_output = output.value(0);
    let nested_output = nested_output.as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(nested_output.len(), child_count as usize);
    let last_list = nested_output.value((child_count - 1) as usize);
    let last_values = last_list.as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(last_values.value(0), child_count - 1);

    let fixed_size_lists = FixedSizeListArray::new(
        Arc::new(Field::new("item", DataType::Int32, false)),
        2,
        Arc::new(Int32Array::from((0..(child_count * 2)).collect::<Vec<_>>())),
        None,
    );
    let list_of_fixed_size_lists = ListArray::new(
        Arc::new(Field::new("item", fixed_size_lists.data_type().clone(), true)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0, child_count])),
        Arc::new(fixed_size_lists),
        None,
    );

    let output = roundtrip_single_array(Arc::new(list_of_fixed_size_lists))?;
    let output = output.as_any().downcast_ref::<ListArray>().unwrap();
    let nested_output = output.value(0);
    let nested_output = nested_output.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
    assert_eq!(nested_output.len(), child_count as usize);
    let last_list = nested_output.value((child_count - 1) as usize);
    let last_values = last_list.as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(last_values.value(0), child_count * 2 - 2);
    assert_eq!(last_values.value(1), child_count * 2 - 1);

    Ok(())
}

#[test]
fn test_nested_list_sizes_are_set_to_child_counts() -> Result<(), Box<dyn Error>> {
    let inner_lists = ListArray::new(
        Arc::new(Field::new("item", DataType::Int32, true)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0, 2, 2, 5])),
        Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3), None, Some(5)])),
        None,
    );
    let list_of_lists = ListArray::new(
        Arc::new(Field::new("item", inner_lists.data_type().clone(), true)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0, 3])),
        Arc::new(inner_lists),
        None,
    );

    let schema = Schema::new(vec![Field::new("a", list_of_lists.data_type().clone(), true)]);
    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(list_of_lists) as ArrayRef])?;
    let logical_type = to_duckdb_logical_type(batch.column(0).data_type())?;
    let mut chunk = DataChunkHandle::new(&[logical_type]);
    record_batch_to_duckdb_data_chunk(&batch, &mut chunk)?;

    let outer = chunk.list_vector(0);
    assert_eq!(outer.len(), 3);

    let inner = outer.list_child();
    assert_eq!(inner.len(), 5);

    Ok(())
}

#[test]
fn test_irregular_nested_list_cast_uses_recorded_child_counts() -> Result<(), Box<dyn Error>> {
    let inner_lists = ListArray::new(
        Arc::new(Field::new("item", DataType::Int32, true)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0, 2, 2, 5])),
        Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3), None, Some(5)])),
        None,
    );
    let list_of_lists = ListArray::new(
        Arc::new(Field::new("item", inner_lists.data_type().clone(), true)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0, 3])),
        Arc::new(inner_lists),
        None,
    );

    let db = Connection::open_in_memory()?;
    db.register_table_function::<ArrowVTab>("arrow")?;

    let schema = Schema::new(vec![Field::new("a", list_of_lists.data_type().clone(), true)]);
    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(list_of_lists) as ArrayRef])?;
    let param = arrow_recordbatch_to_query_params(batch);
    let mut stmt = db.prepare("SELECT a::VARCHAR FROM arrow(?, ?)")?;
    let rb = stmt.query_arrow(param)?.next().expect("no record batch");
    let output = rb.column(0).as_any().downcast_ref::<StringArray>().unwrap();

    assert_eq!(output.value(0), "[[1, 2], [], [3, NULL, 5]]");

    Ok(())
}

#[test]
fn test_list_of_fixed_size_lists_roundtrip() -> Result<(), Box<dyn Error>> {
    // field name must be empty to match `query_arrow` behavior, otherwise record batches will not match
    let field = Field::new("", DataType::Int32, true);
    let mut list_builder = ListBuilder::new(FixedSizeListBuilder::new(Int32Builder::new(), 2).with_field(field));

    // Append first list of FixedSizeList items
    {
        let fixed_size_list_builder = list_builder.values();
        fixed_size_list_builder.values().append_value(1);
        fixed_size_list_builder.values().append_value(2);
        fixed_size_list_builder.append(true);

        // Append NULL fixed-size list item
        fixed_size_list_builder.values().append_null();
        fixed_size_list_builder.values().append_null();
        fixed_size_list_builder.append(false);

        fixed_size_list_builder.values().append_value(3);
        fixed_size_list_builder.values().append_value(4);
        fixed_size_list_builder.append(true);

        list_builder.append(true);
    }

    // Append NULL list
    list_builder.append_null();

    check_generic_array_roundtrip(list_builder.finish())?;

    Ok(())
}

#[test]
fn test_list_of_lists_roundtrip() -> Result<(), Box<dyn Error>> {
    // field name must be 'l' to match `query_arrow` behavior, otherwise record batches will not match
    let field = Field::new("l", DataType::Int32, true);
    let mut list_builder = ListBuilder::new(ListBuilder::new(Int32Builder::new()).with_field(field.clone()));

    // Append first list of items
    {
        let list_item_builder = list_builder.values();
        list_item_builder.append_value(vec![Some(1), Some(2)]);

        // Append NULL list item
        list_item_builder.append_null();

        list_item_builder.append_value(vec![Some(3), None, Some(5)]);

        list_builder.append(true);
    }

    // Append NULL list
    list_builder.append_null();

    check_generic_array_roundtrip(list_builder.finish())?;

    Ok(())
}

#[test]
fn test_list_of_structs_roundtrip() -> Result<(), Box<dyn Error>> {
    let field_i = Arc::new(Field::new("i", DataType::Int32, true));
    let field_s = Arc::new(Field::new("s", DataType::Utf8, true));

    let int32_array = Int32Array::from(vec![Some(1), Some(2), Some(3), Some(4), Some(5)]);
    let string_array = StringArray::from(vec![Some("foo"), Some("baz"), Some("bar"), Some("foo"), Some("baz")]);

    let struct_array = StructArray::from(vec![
        (field_i.clone(), Arc::new(int32_array) as Arc<dyn Array>),
        (field_s.clone(), Arc::new(string_array) as Arc<dyn Array>),
    ]);

    check_generic_array_roundtrip(ListArray::new(
        Arc::new(Field::new(
            "item",
            DataType::Struct(vec![field_i, field_s].into()),
            true,
        )),
        OffsetBuffer::new(ScalarBuffer::from(vec![0, 3, 4, 5])),
        Arc::new(struct_array),
        Some(vec![true, false, true].into()),
    ))?;

    Ok(())
}

fn check_map_array_roundtrip(array: MapArray) -> Result<(), Box<dyn Error>> {
    let expected = array.clone();

    let db = Connection::open_in_memory()?;
    db.register_table_function::<ArrowVTab>("arrow")?;

    // Roundtrip a record batch from Rust to DuckDB and back to Rust
    let schema = Schema::new(vec![Field::new("a", array.data_type().clone(), true)]);

    let rb = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array.clone())])?;
    let param = arrow_recordbatch_to_query_params(rb.clone());
    let mut stmt = db.prepare("select a from arrow(?, ?)")?;
    let rb = stmt.query_arrow(param)?.next().expect("no record batch");
    let output_array = rb
        .column(0)
        .as_any()
        .downcast_ref::<MapArray>()
        .expect("Expected MapArray");

    assert_eq!(output_array.keys(), expected.keys());
    assert_eq!(output_array.values(), expected.values());

    Ok(())
}

#[test]
fn test_map_roundtrip() -> Result<(), Box<dyn Error>> {
    // Test 1 - simple MapArray
    let keys = vec!["a", "b", "c", "d", "e", "f", "g", "h"];
    let values_data = UInt32Array::from(vec![
        Some(0u32),
        None,
        Some(20),
        Some(30),
        None,
        Some(50),
        Some(60),
        Some(70),
    ]);
    // Construct a buffer for value offsets, for the nested array:
    //  [[a, b, c], [d, e, f], [g, h]]
    let entry_offsets = [0, 3, 6, 8];
    let map_array = MapArray::new_from_strings(keys.clone().into_iter(), &values_data, &entry_offsets).unwrap();
    check_map_array_roundtrip(map_array)?;

    // Test 2 - large MapArray of 4000 elements to test buffers capacity adjustment
    let keys: Vec<String> = (0..4000).map(|i| format!("key-{i}")).collect();
    let values_data = UInt32Array::from(
        (0..4000)
            .map(|i| if i % 5 == 0 { None } else { Some(i as u32) })
            .collect::<Vec<_>>(),
    );
    let mut entry_offsets: Vec<u32> = (0..=4000).step_by(3).collect();
    entry_offsets.push(4000);
    let map_array =
        MapArray::new_from_strings(keys.iter().map(String::as_str), &values_data, entry_offsets.as_slice()).unwrap();
    check_map_array_roundtrip(map_array)?;

    Ok(())
}

#[test]
fn test_map_roundtrip_preserves_null_rows() -> Result<(), Box<dyn Error>> {
    let entries = StructArray::from(vec![
        (
            Arc::new(Field::new("keys", DataType::Utf8, false)),
            Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("values", DataType::UInt32, true)),
            Arc::new(UInt32Array::from(vec![Some(1), None, Some(3)])) as ArrayRef,
        ),
    ]);
    let map_array = MapArray::new(
        Arc::new(Field::new("entries", entries.data_type().clone(), false)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0, 2, 2, 3])),
        entries,
        Some(NullBuffer::from([true, false, true].as_slice())),
        false,
    );

    let output = roundtrip_single_array(Arc::new(map_array))?;
    let output = output.as_any().downcast_ref::<MapArray>().unwrap();
    assert_eq!(output.value_offsets(), &[0, 2, 2, 3]);
    assert!(output.is_valid(0));
    assert!(output.is_null(1));
    assert!(output.is_valid(2));

    let keys = output.keys().as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(keys.value(0), "a");
    assert_eq!(keys.value(1), "b");
    assert_eq!(keys.value(2), "c");

    let values = output.values().as_any().downcast_ref::<UInt32Array>().unwrap();
    assert_eq!(values.value(0), 1);
    assert!(values.is_null(1));
    assert_eq!(values.value(2), 3);

    Ok(())
}

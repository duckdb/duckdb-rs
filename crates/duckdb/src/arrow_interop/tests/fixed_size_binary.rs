use super::*;

#[test]
fn test_fixed_size_binary_in_struct() -> Result<(), Box<dyn Error>> {
    let fixed_size_binary_data = FixedSizeBinaryArray::try_from_iter(
        vec![
            vec![1u8, 2, 3, 4, 5, 6, 7, 8],
            vec![9u8, 10, 11, 12, 13, 14, 15, 16],
            vec![17u8, 18, 19, 20, 21, 22, 23, 24],
        ]
        .into_iter(),
    )
    .unwrap();

    let int_data = Int32Array::from(vec![100, 200, 300]);

    let struct_array = StructArray::from(vec![
        (
            Arc::new(Field::new("binary_field", DataType::FixedSizeBinary(8), false)),
            Arc::new(fixed_size_binary_data) as ArrayRef,
        ),
        (
            Arc::new(Field::new("int_field", DataType::Int32, false)),
            Arc::new(int_data) as ArrayRef,
        ),
    ]);

    let output = roundtrip_single_array(Arc::new(struct_array))?;
    let struct_column = output.as_any().downcast_ref::<StructArray>().unwrap();
    assert_eq!(struct_column.len(), 3);

    let binary_field = struct_column.column(0).as_any().downcast_ref::<BinaryArray>().unwrap();
    assert_eq!(binary_field.value(0), &[1u8, 2, 3, 4, 5, 6, 7, 8]);
    assert_eq!(binary_field.value(1), &[9u8, 10, 11, 12, 13, 14, 15, 16]);
    assert_eq!(binary_field.value(2), &[17u8, 18, 19, 20, 21, 22, 23, 24]);

    let int_field = struct_column.column(1).as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(int_field.value(0), 100);
    assert_eq!(int_field.value(1), 200);
    assert_eq!(int_field.value(2), 300);

    Ok(())
}

#[test]
fn test_fixed_size_binary_in_fixed_size_list() -> Result<(), Box<dyn Error>> {
    let values = FixedSizeBinaryArray::try_from_iter(
        vec![
            vec![1u8, 2, 3, 4],
            vec![5u8, 6, 7, 8],
            vec![9u8, 10, 11, 12],
            vec![13u8, 14, 15, 16],
        ]
        .into_iter(),
    )
    .unwrap();

    let field = Arc::new(Field::new("item", DataType::FixedSizeBinary(4), false));
    let fixed_size_list = FixedSizeListArray::new(field, 2, Arc::new(values), None);

    let output = roundtrip_single_array(Arc::new(fixed_size_list))?;
    let list_column = output.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
    assert_eq!(list_column.len(), 2);

    let first_list = list_column.value(0);
    let first_binary = first_list.as_any().downcast_ref::<BinaryArray>().unwrap();
    assert_eq!(first_binary.len(), 2);
    assert_eq!(first_binary.value(0), &[1u8, 2, 3, 4]);
    assert_eq!(first_binary.value(1), &[5u8, 6, 7, 8]);

    let second_list = list_column.value(1);
    let second_binary = second_list.as_any().downcast_ref::<BinaryArray>().unwrap();
    assert_eq!(second_binary.len(), 2);
    assert_eq!(second_binary.value(0), &[9u8, 10, 11, 12]);
    assert_eq!(second_binary.value(1), &[13u8, 14, 15, 16]);

    Ok(())
}

#[test]
fn test_fixed_size_binary_with_nulls_in_list() -> Result<(), Box<dyn Error>> {
    let values = FixedSizeBinaryArray::try_from_sparse_iter_with_size(
        vec![Some(vec![1u8, 2, 3, 4]), None, Some(vec![5u8, 6, 7, 8])].into_iter(),
        4,
    )
    .unwrap();

    let list_array = ListArray::new(
        Arc::new(Field::new("item", DataType::FixedSizeBinary(4), true)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0, 3])),
        Arc::new(values),
        None,
    );

    let output = roundtrip_single_array(Arc::new(list_array))?;
    let list_column = output.as_any().downcast_ref::<ListArray>().unwrap();
    let first_list = list_column.value(0);
    let binary_field = first_list.as_any().downcast_ref::<BinaryArray>().unwrap();

    assert_eq!(binary_field.len(), 3);
    assert_eq!(binary_field.value(0), &[1u8, 2, 3, 4]);
    assert!(binary_field.is_null(1));
    assert_eq!(binary_field.value(2), &[5u8, 6, 7, 8]);

    Ok(())
}

#[test]
fn test_fixed_size_binary_with_nulls_in_struct() -> Result<(), Box<dyn Error>> {
    let fixed_size_binary_data = FixedSizeBinaryArray::try_from_sparse_iter_with_size(
        vec![Some(vec![1u8, 2, 3, 4]), None, Some(vec![5u8, 6, 7, 8])].into_iter(),
        4,
    )
    .unwrap();

    let struct_array = StructArray::from(vec![(
        Arc::new(Field::new("binary_field", DataType::FixedSizeBinary(4), true)),
        Arc::new(fixed_size_binary_data) as ArrayRef,
    )]);

    let output = roundtrip_single_array(Arc::new(struct_array))?;
    let struct_column = output.as_any().downcast_ref::<StructArray>().unwrap();
    let binary_field = struct_column.column(0).as_any().downcast_ref::<BinaryArray>().unwrap();

    assert_eq!(binary_field.len(), 3);
    assert_eq!(binary_field.value(0), &[1u8, 2, 3, 4]);
    assert!(binary_field.is_null(1));
    assert_eq!(binary_field.value(2), &[5u8, 6, 7, 8]);

    Ok(())
}

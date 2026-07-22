use super::{
    UUID_BYTE_WIDTH, UUID_EXTENSION_NAME, data_chunk_to_arrow, flat_vector_to_arrow_array,
    record_batch_to_duckdb_data_chunk, to_duckdb_logical_type, to_duckdb_logical_type_for_field,
};
use crate::{
    Connection, Result,
    arrow_interop::test_support::{ARROW_EXTENSION_NAME_KEY, uuid_field, uuid_metadata},
    core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId},
    vtab::arrow::{ArrowVTab, arrow_recordbatch_to_query_params},
};
use arrow::{
    array::{
        Array, ArrayRef, AsArray, BinaryArray, BinaryViewArray, BooleanArray, Date32Array, Date64Array, Decimal32Array,
        Decimal64Array, Decimal128Array, Decimal256Array, DictionaryArray, DurationSecondArray, FixedSizeBinaryArray,
        FixedSizeListArray, FixedSizeListBuilder, GenericByteArray, GenericListArray, Int8Array, Int32Array,
        Int32Builder, Int64Array, IntervalDayTimeArray, IntervalMonthDayNanoArray, IntervalYearMonthArray,
        LargeBinaryArray, LargeListArray, LargeStringArray, ListArray, ListBuilder, MapArray, OffsetSizeTrait,
        PrimitiveArray, StringArray, StringViewArray, StructArray, Time32SecondArray, Time64MicrosecondArray,
        TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
        UInt32Array,
    },
    buffer::{NullBuffer, OffsetBuffer, ScalarBuffer},
    datatypes::{
        ArrowPrimitiveType, ByteArrayType, DataType, Decimal32Type, Decimal64Type, DurationSecondType, Field, Fields,
        Int8Type, IntervalDayTimeType, IntervalMonthDayNanoType, IntervalYearMonthType, Schema, i256,
    },
    record_batch::RecordBatch,
};
use std::{collections::HashMap, error::Error, sync::Arc};

mod fixed_size_binary;
mod from_duckdb_nested;
mod nested;

#[test]
fn test_field_aware_logical_type_preserves_nested_shape() -> Result<(), Box<dyn Error>> {
    let field = Field::new(
        "payload",
        DataType::Struct(Fields::from(vec![
            Field::new(
                "ids",
                DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                true,
            ),
            Field::new("label", DataType::Utf8, true),
        ])),
        true,
    );

    let logical_type = to_duckdb_logical_type_for_field(&field)?;
    assert_eq!(logical_type.id(), LogicalTypeId::Struct);
    assert_eq!(logical_type.child_name(0), "ids");
    assert_eq!(logical_type.child_name(1), "label");

    let ids_type = logical_type.child(0);
    assert_eq!(ids_type.id(), LogicalTypeId::List);
    assert_eq!(ids_type.child(0).id(), LogicalTypeId::Integer);
    assert_eq!(logical_type.child(1).id(), LogicalTypeId::Varchar);

    Ok(())
}

#[test]
fn test_arrow_uuid_extension_logical_type_preserves_nested_metadata() -> Result<(), Box<dyn Error>> {
    let top_level = to_duckdb_logical_type_for_field(&uuid_field("id"))?;
    assert_eq!(top_level.id(), LogicalTypeId::Uuid);

    let struct_type = to_duckdb_logical_type_for_field(&Field::new(
        "payload",
        DataType::Struct(Fields::from(vec![
            uuid_field("id"),
            Field::new("label", DataType::Utf8, true),
        ])),
        true,
    ))?;
    assert_eq!(struct_type.id(), LogicalTypeId::Struct);
    assert_eq!(struct_type.child(0).id(), LogicalTypeId::Uuid);

    let list_type =
        to_duckdb_logical_type_for_field(&Field::new("ids", DataType::List(Arc::new(uuid_field("item"))), true))?;
    assert_eq!(list_type.id(), LogicalTypeId::List);
    assert_eq!(list_type.child(0).id(), LogicalTypeId::Uuid);

    let map_type = to_duckdb_logical_type_for_field(&Field::new(
        "lookup",
        DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(Fields::from(vec![
                    Field::new("keys", DataType::Utf8, false),
                    uuid_field("values"),
                ])),
                false,
            )),
            false,
        ),
        true,
    ))?;
    assert_eq!(map_type.id(), LogicalTypeId::Map);
    assert_eq!(map_type.child(0).id(), LogicalTypeId::Varchar);
    assert_eq!(map_type.child(1).id(), LogicalTypeId::Uuid);

    Ok(())
}

#[test]
fn test_arrow_uuid_extension_rejects_invalid_storage() {
    let field = Field::new("id", DataType::FixedSizeBinary(8), true).with_metadata(uuid_metadata());
    let err = to_duckdb_logical_type_for_field(&field).unwrap_err();
    let expected = format!("{UUID_EXTENSION_NAME} requires FixedSizeBinary({UUID_BYTE_WIDTH})");
    assert!(
        err.to_string().contains(&expected) && err.to_string().contains("FixedSizeBinary(8)"),
        "unexpected error: {err}"
    );
}

#[test]
fn test_arrow_unknown_extension_falls_back_to_storage_type() -> Result<(), Box<dyn Error>> {
    let field = Field::new("payload", DataType::Utf8, true).with_metadata(HashMap::from([(
        ARROW_EXTENSION_NAME_KEY.to_string(),
        "arrow.json".to_string(),
    )]));

    let logical_type = to_duckdb_logical_type_for_field(&field)?;
    assert_eq!(logical_type.id(), LogicalTypeId::Varchar);

    Ok(())
}

#[test]
#[cfg(feature = "appender-arrow")]
fn test_append_struct() -> Result<(), Box<dyn Error>> {
    use arrow::datatypes::Fields;

    let db = Connection::open_in_memory()?;
    db.execute_batch("CREATE TABLE t1 (s STRUCT(v VARCHAR, i INTEGER))")?;
    {
        let struct_array = StructArray::from(vec![
            (
                Arc::new(Field::new("v", DataType::Utf8, true)),
                Arc::new(StringArray::from(vec![Some("foo"), Some("bar")])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("i", DataType::Int32, true)),
                Arc::new(Int32Array::from(vec![Some(1), Some(2)])) as ArrayRef,
            ),
        ]);

        let schema = Schema::new(vec![Field::new(
            "s",
            DataType::Struct(Fields::from(vec![
                Field::new("v", DataType::Utf8, true),
                Field::new("i", DataType::Int32, true),
            ])),
            true,
        )]);

        let record_batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(struct_array)])?;
        let mut app = db.appender("t1")?;
        app.append_record_batch(record_batch)?;
    }
    let mut stmt = db.prepare("SELECT s FROM t1")?;
    let rbs: Vec<RecordBatch> = stmt.query_arrow([])?.collect();
    assert_eq!(rbs.iter().map(|op| op.num_rows()).sum::<usize>(), 2);

    Ok(())
}

#[test]
#[cfg(feature = "appender-arrow")]
fn test_append_struct_contains_null() -> Result<(), Box<dyn Error>> {
    use arrow::datatypes::Fields;

    let db = Connection::open_in_memory()?;
    db.execute_batch("CREATE TABLE t1 (s STRUCT(v VARCHAR, i INTEGER))")?;
    {
        let struct_array = StructArray::try_new(
            vec![
                Arc::new(Field::new("v", DataType::Utf8, true)),
                Arc::new(Field::new("i", DataType::Int32, true)),
            ]
            .into(),
            vec![
                Arc::new(StringArray::from(vec![Some("foo"), Some("bar")])) as ArrayRef,
                Arc::new(Int32Array::from(vec![Some(1), Some(2)])) as ArrayRef,
            ],
            Some(vec![true, false].into()),
        )?;

        let schema = Schema::new(vec![Field::new(
            "s",
            DataType::Struct(Fields::from(vec![
                Field::new("v", DataType::Utf8, true),
                Field::new("i", DataType::Int32, true),
            ])),
            true,
        )]);

        let record_batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(struct_array)])?;
        let mut app = db.appender("t1")?;
        app.append_record_batch(record_batch)?;
    }
    let mut stmt = db.prepare("SELECT s FROM t1 where s IS NOT NULL")?;
    let rbs: Vec<RecordBatch> = stmt.query_arrow([])?.collect();
    assert_eq!(rbs.iter().map(|op| op.num_rows()).sum::<usize>(), 1);

    Ok(())
}

fn check_rust_primitive_array_roundtrip<T1, T2>(
    input_array: PrimitiveArray<T1>,
    expected_array: PrimitiveArray<T2>,
) -> Result<(), Box<dyn Error>>
where
    T1: ArrowPrimitiveType,
    T2: ArrowPrimitiveType,
{
    let db = Connection::open_in_memory()?;
    db.register_table_function::<ArrowVTab>("arrow")?;

    // Roundtrip a record batch from Rust to DuckDB and back to Rust
    let schema = Schema::new(vec![Field::new("a", input_array.data_type().clone(), true)]);

    let rb = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(input_array.clone())])?;
    let param = arrow_recordbatch_to_query_params(rb);
    let mut stmt = db.prepare("select a from arrow(?, ?)")?;
    let rb = stmt.query_arrow(param)?.next().expect("no record batch");

    let output_any_array = rb.column(0);
    match (output_any_array.data_type(), expected_array.data_type()) {
        // TODO: DuckDB doesnt return timestamp_tz properly yet, so we just check that the units are the same
        (DataType::Timestamp(unit_a, _), DataType::Timestamp(unit_b, _)) => assert_eq!(unit_a, unit_b),
        (a, b) => assert_eq!(a, b),
    }

    let maybe_output_array = output_any_array.as_primitive_opt::<T2>();

    match maybe_output_array {
        Some(output_array) => {
            // Check that the output array is the same as the input array
            assert_eq!(output_array.len(), expected_array.len());
            for i in 0..output_array.len() {
                assert_eq!(output_array.is_valid(i), expected_array.is_valid(i));
                if output_array.is_valid(i) {
                    assert_eq!(output_array.value(i), expected_array.value(i));
                }
            }
        }
        None => {
            panic!("Output array is not a PrimitiveArray {:?}", rb.column(0).data_type());
        }
    }

    Ok(())
}

fn check_generic_array_roundtrip<T>(arry: GenericListArray<T>) -> Result<(), Box<dyn Error>>
where
    T: OffsetSizeTrait,
{
    let expected_output_array = arry.clone();

    let db = Connection::open_in_memory()?;
    db.register_table_function::<ArrowVTab>("arrow")?;

    // Roundtrip a record batch from Rust to DuckDB and back to Rust
    let schema = Schema::new(vec![Field::new("a", arry.data_type().clone(), true)]);

    let rb = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arry.clone())])?;
    let param = arrow_recordbatch_to_query_params(rb);
    let mut stmt = db.prepare("select a from arrow(?, ?)")?;
    let rb = stmt.query_arrow(param)?.next().expect("no record batch");

    let output_any_array = rb.column(0);
    assert!(
        output_any_array
            .data_type()
            .equals_datatype(expected_output_array.data_type())
    );

    match output_any_array.as_list_opt::<T>() {
        Some(output_array) => {
            assert_eq!(output_array.len(), expected_output_array.len());
            for i in 0..output_array.len() {
                assert_eq!(output_array.is_valid(i), expected_output_array.is_valid(i));
                if output_array.is_valid(i) {
                    assert!(expected_output_array.value(i).eq(&output_array.value(i)));
                }
            }
        }
        None => panic!("Expected GenericListArray"),
    }

    Ok(())
}

fn check_generic_byte_roundtrip<T1, T2>(
    arry_in: GenericByteArray<T1>,
    arry_out: GenericByteArray<T2>,
) -> Result<(), Box<dyn Error>>
where
    T1: ByteArrayType,
    T2: ByteArrayType,
{
    let db = Connection::open_in_memory()?;
    db.register_table_function::<ArrowVTab>("arrow")?;

    // Roundtrip a record batch from Rust to DuckDB and back to Rust
    let schema = Schema::new(vec![Field::new("a", arry_in.data_type().clone(), false)]);

    let rb = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arry_in.clone())])?;
    let param = arrow_recordbatch_to_query_params(rb);
    let mut stmt = db.prepare("select a from arrow(?, ?)")?;
    let rb = stmt.query_arrow(param)?.next().expect("no record batch");

    let output_any_array = rb.column(0);

    assert!(
        output_any_array.data_type().equals_datatype(arry_out.data_type()),
        "{} != {}",
        output_any_array.data_type(),
        arry_out.data_type()
    );

    match output_any_array.as_bytes_opt::<T2>() {
        Some(output_array) => {
            assert_eq!(output_array.len(), arry_out.len());
            for i in 0..output_array.len() {
                assert_eq!(output_array.is_valid(i), arry_out.is_valid(i));
                assert_eq!(output_array.value_data(), arry_out.value_data())
            }
        }
        None => panic!("Expected GenericByteArray"),
    }

    Ok(())
}

fn roundtrip_single_array(array: ArrayRef) -> Result<ArrayRef, Box<dyn Error>> {
    let db = Connection::open_in_memory()?;
    db.register_table_function::<ArrowVTab>("arrow")?;

    let schema = Schema::new(vec![Field::new("a", array.data_type().clone(), true)]);
    let rb = RecordBatch::try_new(Arc::new(schema), vec![array])?;
    let param = arrow_recordbatch_to_query_params(rb);
    let mut stmt = db.prepare("select a from arrow(?, ?)")?;
    let rb = stmt.query_arrow(param)?.next().expect("no record batch");

    Ok(rb.column(0).clone())
}

/// Writes a single-column record batch into a fresh DuckDB data chunk.
fn single_array_data_chunk(array: ArrayRef) -> Result<DataChunkHandle, Box<dyn Error>> {
    let schema = Schema::new(vec![Field::new("a", array.data_type().clone(), true)]);
    let batch = RecordBatch::try_new(Arc::new(schema), vec![array])?;
    let logical_type = to_duckdb_logical_type(batch.column(0).data_type())?;
    let mut chunk = DataChunkHandle::new(&[logical_type]);
    record_batch_to_duckdb_data_chunk(&batch, &mut chunk)?;

    Ok(chunk)
}

#[test]
fn test_array_roundtrip() -> Result<(), Box<dyn Error>> {
    check_generic_array_roundtrip(ListArray::new(
        Arc::new(Field::new("item", DataType::Utf8, true)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0, 2, 4, 5])),
        Arc::new(StringArray::from(vec![
            Some("foo"),
            Some("baz"),
            Some("bar"),
            Some("foo"),
            Some("baz"),
        ])),
        None,
    ))?;

    Ok(())
}

#[test]
fn test_boolean_array_roundtrip() -> Result<(), Box<dyn Error>> {
    check_generic_array_roundtrip(ListArray::new(
        Arc::new(Field::new("item", DataType::Boolean, true)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0, 2, 4, 5])),
        Arc::new(BooleanArray::from(vec![
            Some(true),
            Some(false),
            Some(true),
            Some(true),
            Some(false),
        ])),
        None,
    ))?;

    Ok(())
}

//field: FieldRef, size: i32, values: ArrayRef, nulls: Option<NullBuffer>
#[test]
fn test_fixed_array_roundtrip() -> Result<(), Box<dyn Error>> {
    let array = FixedSizeListArray::new(
        Arc::new(Field::new("item", DataType::Int32, true)),
        2,
        Arc::new(Int32Array::from(vec![
            Some(1),
            Some(2),
            Some(3),
            Some(4),
            Some(5),
            Some(6),
        ])),
        None,
    );

    let expected_output_array = array.clone();

    let db = Connection::open_in_memory()?;
    db.register_table_function::<ArrowVTab>("arrow")?;

    // Roundtrip a record batch from Rust to DuckDB and back to Rust
    let schema = Schema::new(vec![Field::new("a", array.data_type().clone(), false)]);

    let rb = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array.clone())])?;
    let param = arrow_recordbatch_to_query_params(rb);
    let mut stmt = db.prepare("select a from arrow(?, ?)")?;
    let rb = stmt.query_arrow(param)?.next().expect("no record batch");

    let output_any_array = rb.column(0);
    assert!(
        output_any_array
            .data_type()
            .equals_datatype(expected_output_array.data_type())
    );

    match output_any_array.as_fixed_size_list_opt() {
        Some(output_array) => {
            assert_eq!(output_array.len(), expected_output_array.len());
            for i in 0..output_array.len() {
                assert_eq!(output_array.is_valid(i), expected_output_array.is_valid(i));
                if output_array.is_valid(i) {
                    assert!(expected_output_array.value(i).eq(&output_array.value(i)));
                }
            }
        }
        None => panic!("Expected FixedSizeListArray"),
    }

    Ok(())
}

#[test]
fn test_primitive_roundtrip_contains_nulls() -> Result<(), Box<dyn Error>> {
    let mut builder = arrow::array::PrimitiveBuilder::<arrow::datatypes::Int32Type>::new();
    builder.append_value(1);
    builder.append_null();
    builder.append_value(3);
    builder.append_null();
    builder.append_null();
    builder.append_value(6);
    let array = builder.finish();

    check_rust_primitive_array_roundtrip(array.clone(), array)?;

    Ok(())
}

#[test]
fn test_check_generic_array_roundtrip_contains_null() -> Result<(), Box<dyn Error>> {
    check_generic_array_roundtrip(ListArray::new(
        Arc::new(Field::new("item", DataType::Utf8, true)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0, 2, 4, 5])),
        Arc::new(StringArray::from(vec![
            Some("foo"),
            Some("baz"),
            Some("bar"),
            Some("foo"),
            Some("baz"),
        ])),
        Some(vec![true, false, true].into()),
    ))?;

    Ok(())
}

#[test]
fn test_utf8_roundtrip() -> Result<(), Box<dyn Error>> {
    check_generic_byte_roundtrip(
        StringArray::from(vec![Some("foo"), Some("Baz"), Some("bar")]),
        StringArray::from(vec![Some("foo"), Some("Baz"), Some("bar")]),
    )?;

    // [`LargeStringArray`] will be downcasted to [`StringArray`].
    check_generic_byte_roundtrip(
        LargeStringArray::from(vec![Some("foo"), Some("Baz"), Some("bar")]),
        StringArray::from(vec![Some("foo"), Some("Baz"), Some("bar")]),
    )?;
    Ok(())
}

#[test]
fn test_timestamp_roundtrip() -> Result<(), Box<dyn Error>> {
    check_rust_primitive_array_roundtrip(Int32Array::from(vec![1, 2, 3]), Int32Array::from(vec![1, 2, 3]))?;

    check_rust_primitive_array_roundtrip(
        TimestampMicrosecondArray::from(vec![1, 2, 3]),
        TimestampMicrosecondArray::from(vec![1, 2, 3]),
    )?;

    check_rust_primitive_array_roundtrip(
        TimestampNanosecondArray::from(vec![1, 2, 3]),
        TimestampNanosecondArray::from(vec![1, 2, 3]),
    )?;

    check_rust_primitive_array_roundtrip(
        TimestampSecondArray::from(vec![1, 2, 3]),
        TimestampSecondArray::from(vec![1, 2, 3]),
    )?;

    check_rust_primitive_array_roundtrip(
        TimestampMillisecondArray::from(vec![1, 2, 3]),
        TimestampMillisecondArray::from(vec![1, 2, 3]),
    )?;

    // DuckDB can only return timestamp_tz in microseconds
    // Note: DuckDB by default returns timestamp_tz with UTC because the rust
    // driver doesnt support timestamp_tz properly when reading. In the
    // future we should be able to roundtrip timestamp_tz with other timezones too
    check_rust_primitive_array_roundtrip(
        TimestampNanosecondArray::from(vec![1000, 2000, 3000]).with_timezone_utc(),
        TimestampMicrosecondArray::from(vec![1, 2, 3]).with_timezone_utc(),
    )?;

    check_rust_primitive_array_roundtrip(
        TimestampMillisecondArray::from(vec![1, 2, 3]).with_timezone_utc(),
        TimestampMicrosecondArray::from(vec![1000, 2000, 3000]).with_timezone_utc(),
    )?;

    check_rust_primitive_array_roundtrip(
        TimestampSecondArray::from(vec![1, 2, 3]).with_timezone_utc(),
        TimestampMicrosecondArray::from(vec![1_000_000, 2_000_000, 3_000_000]).with_timezone_utc(),
    )?;

    check_rust_primitive_array_roundtrip(
        TimestampMicrosecondArray::from(vec![1, 2, 3]).with_timezone_utc(),
        TimestampMicrosecondArray::from(vec![1, 2, 3]).with_timezone_utc(),
    )?;

    check_rust_primitive_array_roundtrip(Date32Array::from(vec![1, 2, 3]), Date32Array::from(vec![1, 2, 3]))?;

    let mid = arrow::temporal_conversions::MILLISECONDS_IN_DAY;
    check_rust_primitive_array_roundtrip(
        Date64Array::from(vec![mid, 2 * mid, 3 * mid]),
        Date32Array::from(vec![1, 2, 3]),
    )?;

    check_rust_primitive_array_roundtrip(
        Time32SecondArray::from(vec![1, 2, 3]),
        Time64MicrosecondArray::from(vec![1_000_000, 2_000_000, 3_000_000]),
    )?;

    Ok(())
}

#[test]
fn test_decimal128_roundtrip() -> Result<(), Box<dyn Error>> {
    // With default width and scale
    let array: PrimitiveArray<arrow::datatypes::Decimal128Type> =
        Decimal128Array::from(vec![i128::from(1), i128::from(2), i128::from(3)]);
    check_rust_primitive_array_roundtrip(array.clone(), array)?;

    // With custom width and scale
    let array: PrimitiveArray<arrow::datatypes::Decimal128Type> =
        Decimal128Array::from(vec![i128::from(12345)]).with_data_type(DataType::Decimal128(5, 2));
    check_rust_primitive_array_roundtrip(array.clone(), array)?;

    // With width and zero scale
    let array: PrimitiveArray<arrow::datatypes::Decimal128Type> =
        Decimal128Array::from(vec![i128::from(12345)]).with_data_type(DataType::Decimal128(5, 0));
    check_rust_primitive_array_roundtrip(array.clone(), array)?;

    Ok(())
}

#[test]
fn test_decimal32_roundtrip() -> Result<(), Box<dyn Error>> {
    let array: PrimitiveArray<Decimal32Type> =
        Decimal32Array::from(vec![1234, -1234]).with_data_type(DataType::Decimal32(4, 2));
    let expected = Decimal128Array::from(vec![1234_i128, -1234]).with_data_type(DataType::Decimal128(4, 2));
    check_rust_primitive_array_roundtrip(array, expected)?;

    let array: PrimitiveArray<Decimal32Type> =
        Decimal32Array::from(vec![Some(123456789_i32), None, Some(-123456789_i32)])
            .with_data_type(DataType::Decimal32(9, 2));
    let expected = Decimal128Array::from(vec![Some(123456789_i128), None, Some(-123456789_i128)])
        .with_data_type(DataType::Decimal128(9, 2));
    check_rust_primitive_array_roundtrip(array, expected)?;

    Ok(())
}

#[test]
fn test_decimal64_roundtrip() -> Result<(), Box<dyn Error>> {
    let array: PrimitiveArray<Decimal64Type> =
        Decimal64Array::from(vec![1234, -1234]).with_data_type(DataType::Decimal64(4, 2));
    let expected = Decimal128Array::from(vec![1234_i128, -1234]).with_data_type(DataType::Decimal128(4, 2));
    check_rust_primitive_array_roundtrip(array, expected)?;

    let array: PrimitiveArray<Decimal64Type> =
        Decimal64Array::from(vec![123456789, -123456789]).with_data_type(DataType::Decimal64(9, 2));
    let expected = Decimal128Array::from(vec![123456789_i128, -123456789]).with_data_type(DataType::Decimal128(9, 2));
    check_rust_primitive_array_roundtrip(array, expected)?;

    let array: PrimitiveArray<Decimal64Type> =
        Decimal64Array::from(vec![Some(123456789012345678_i64), None, Some(-123456789012345678_i64)])
            .with_data_type(DataType::Decimal64(18, 2));
    let expected = Decimal128Array::from(vec![
        Some(123456789012345678_i128),
        None,
        Some(-123456789012345678_i128),
    ])
    .with_data_type(DataType::Decimal128(18, 2));
    check_rust_primitive_array_roundtrip(array, expected)?;

    Ok(())
}

#[test]
fn test_narrow_decimal_logical_type_rejects_invalid_width_and_scale() {
    fn logical_type_error(data_type: DataType) -> String {
        match to_duckdb_logical_type(&data_type) {
            Ok(_) => panic!("expected {data_type} to be rejected"),
            Err(err) => err.to_string(),
        }
    }

    let err = logical_type_error(DataType::Decimal32(10, 2));
    assert!(err.contains("decimal width 10 exceeds 9"), "unexpected error: {err}");

    let err = logical_type_error(DataType::Decimal64(19, 2));
    assert!(err.contains("decimal width 19 exceeds 18"), "unexpected error: {err}");

    let err = logical_type_error(DataType::Decimal32(4, -1));
    assert!(
        err.contains("negative decimal scale is not supported"),
        "unexpected error: {err}"
    );

    let err = logical_type_error(DataType::Decimal64(4, -1));
    assert!(
        err.contains("negative decimal scale is not supported"),
        "unexpected error: {err}"
    );
}

#[test]
fn test_decimal128_rejects_out_of_precision_values() -> Result<(), Box<dyn Error>> {
    let db = Connection::open_in_memory()?;
    db.register_table_function::<ArrowVTab>("arrow")?;

    let schema = Schema::new(vec![Field::new("d", DataType::Decimal128(5, 2), true)]);
    let array = Decimal128Array::from(vec![i128::from(999999)]).with_data_type(DataType::Decimal128(5, 2));
    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array) as ArrayRef])?;
    let param = arrow_recordbatch_to_query_params(batch);

    let err = db
        .query_row("SELECT d FROM arrow(?, ?)", param, |row| {
            row.get::<_, crate::types::Value>(0)
        })
        .unwrap_err();

    assert!(
        err.to_string().contains("invalid Arrow decimal value at row 0"),
        "unexpected error: {err}"
    );
    Ok(())
}

#[test]
fn test_decimal128_rejects_invalid_type_without_values() -> Result<(), Box<dyn Error>> {
    let db = Connection::open_in_memory()?;
    db.register_table_function::<ArrowVTab>("arrow")?;

    let schema = Schema::new(vec![Field::new("d", DataType::Decimal128(5, 10), true)]);
    let array = Decimal128Array::from(vec![None::<i128>]).with_data_type(DataType::Decimal128(5, 10));
    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array) as ArrayRef])?;
    let param = arrow_recordbatch_to_query_params(batch);

    let err = db
        .query_row("SELECT d FROM arrow(?, ?)", param, |row| {
            row.get::<_, crate::types::Value>(0)
        })
        .unwrap_err();

    assert!(
        err.to_string().contains("decimal scale 10 exceeds width 5"),
        "unexpected error: {err}"
    );
    Ok(())
}

#[test]
fn test_interval_roundtrip() -> Result<(), Box<dyn Error>> {
    let array: PrimitiveArray<IntervalMonthDayNanoType> = IntervalMonthDayNanoArray::from(vec![
        IntervalMonthDayNanoType::make_value(1, 1, 1000),
        IntervalMonthDayNanoType::make_value(2, 2, 2000),
        IntervalMonthDayNanoType::make_value(3, 3, 3000),
    ]);
    check_rust_primitive_array_roundtrip(array.clone(), array)?;

    let array: PrimitiveArray<IntervalYearMonthType> = IntervalYearMonthArray::from(vec![
        IntervalYearMonthType::make_value(1, 10),
        IntervalYearMonthType::make_value(2, 20),
        IntervalYearMonthType::make_value(3, 30),
    ]);
    let expected_array: PrimitiveArray<IntervalMonthDayNanoType> = IntervalMonthDayNanoArray::from(vec![
        IntervalMonthDayNanoType::make_value(22, 0, 0),
        IntervalMonthDayNanoType::make_value(44, 0, 0),
        IntervalMonthDayNanoType::make_value(66, 0, 0),
    ]);
    check_rust_primitive_array_roundtrip(array, expected_array)?;

    let array: PrimitiveArray<IntervalDayTimeType> = IntervalDayTimeArray::from(vec![
        IntervalDayTimeType::make_value(1, 1),
        IntervalDayTimeType::make_value(2, 2),
        IntervalDayTimeType::make_value(3, 3),
    ]);
    let expected_array: PrimitiveArray<IntervalMonthDayNanoType> = IntervalMonthDayNanoArray::from(vec![
        IntervalMonthDayNanoType::make_value(0, 1, 1_000_000),
        IntervalMonthDayNanoType::make_value(0, 2, 2_000_000),
        IntervalMonthDayNanoType::make_value(0, 3, 3_000_000),
    ]);
    check_rust_primitive_array_roundtrip(array, expected_array)?;

    Ok(())
}

#[test]
fn test_duration_roundtrip() -> Result<(), Box<dyn Error>> {
    let array: PrimitiveArray<DurationSecondType> = DurationSecondArray::from(vec![1, 2, 3]);
    let expected_array: PrimitiveArray<IntervalMonthDayNanoType> = IntervalMonthDayNanoArray::from(vec![
        IntervalMonthDayNanoType::make_value(0, 0, 1_000_000_000),
        IntervalMonthDayNanoType::make_value(0, 0, 2_000_000_000),
        IntervalMonthDayNanoType::make_value(0, 0, 3_000_000_000),
    ]);
    check_rust_primitive_array_roundtrip(array, expected_array)?;

    Ok(())
}

#[test]
fn test_timestamp_tz_insert() -> Result<(), Box<dyn Error>> {
    // TODO: This test should be reworked once we support TIMESTAMP_TZ properly

    let db = Connection::open_in_memory()?;
    db.register_table_function::<ArrowVTab>("arrow")?;

    let array = TimestampMicrosecondArray::from(vec![1]).with_timezone("+05:00");
    let schema = Schema::new(vec![Field::new("a", array.data_type().clone(), false)]);

    // Since we cant get TIMESTAMP_TZ from the rust client yet, we just check that we can insert it properly here.
    let rb = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array)]).expect("failed to create record batch");
    let param = arrow_recordbatch_to_query_params(rb);
    let mut stmt = db.prepare("select typeof(a)::VARCHAR from arrow(?, ?)")?;
    let mut arr = stmt.query_arrow(param)?;
    let rb = arr.next().expect("no record batch");
    assert_eq!(rb.num_columns(), 1);
    let column = rb.column(0).as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(column.value(0), "TIMESTAMP WITH TIME ZONE");
    Ok(())
}

#[test]
fn test_arrow_error() {
    let arc: ArrayRef = Arc::new(Decimal256Array::from(vec![i256::from(1), i256::from(2), i256::from(3)]));
    let batch = RecordBatch::try_from_iter(vec![("x", arc)]).unwrap();

    let db = Connection::open_in_memory().unwrap();
    db.register_table_function::<ArrowVTab>("arrow").unwrap();

    let mut stmt = db.prepare("SELECT * FROM arrow(?, ?)").unwrap();

    let res = stmt.execute(arrow_recordbatch_to_query_params(batch)).err().unwrap();

    assert_eq!(
        res,
        crate::error::Error::DuckDBFailure(
            crate::ffi::Error {
                code: crate::ffi::ErrorCode::Unknown,
                extended_code: 1
            },
            Some(
                "Invalid Input Error: Data type \"Decimal256(76, 10)\" not yet supported by Arrow-to-DuckDB conversion"
                    .to_owned()
            )
        )
    );
}

#[test]
fn test_arrow_binary() {
    let byte_array = BinaryArray::from_iter_values([b"test"].iter());
    let arc: ArrayRef = Arc::new(byte_array);
    let batch = RecordBatch::try_from_iter(vec![("x", arc)]).unwrap();

    let db = Connection::open_in_memory().unwrap();
    db.register_table_function::<ArrowVTab>("arrow").unwrap();

    let mut stmt = db.prepare("SELECT * FROM arrow(?, ?)").unwrap();

    let mut arr = stmt.query_arrow(arrow_recordbatch_to_query_params(batch)).unwrap();
    let rb = arr.next().expect("no record batch");

    let column = rb.column(0).as_any().downcast_ref::<BinaryArray>().unwrap();
    assert_eq!(column.len(), 1);
    assert_eq!(column.value(0), b"test");
}

#[test]
fn test_geometry_data_chunk_to_arrow_uses_wkb_bytes() -> Result<(), Box<dyn Error>> {
    let db = Connection::open_in_memory()?;
    let wkb: Vec<u8> = db.query_row("SELECT ST_AsWKB('POINT EMPTY'::GEOMETRY)", [], |row| row.get(0))?;

    let mut chunk = DataChunkHandle::new(&[LogicalTypeHandle::from(LogicalTypeId::Geometry)]);
    chunk.flat_vector(0).insert(0, wkb.as_slice());
    chunk.set_len(1);
    unsafe { chunk.assume_initialized() };

    let rb = data_chunk_to_arrow(&chunk)?;
    let column = rb.column(0).as_any().downcast_ref::<BinaryArray>().unwrap();

    assert_eq!(column.len(), 1);
    assert_eq!(column.value(0), wkb);
    Ok(())
}

#[test]
fn test_string_view_roundtrip() -> Result<(), Box<dyn Error>> {
    let db = Connection::open_in_memory()?;
    db.register_table_function::<ArrowVTab>("arrow")?;

    let array = StringViewArray::from(vec![Some("foo"), Some("bar"), Some("baz")]);
    let schema = Schema::new(vec![Field::new("a", array.data_type().clone(), false)]);
    let rb = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array.clone())])?;

    let param = arrow_recordbatch_to_query_params(rb);
    let mut stmt = db.prepare("select a from arrow(?, ?)")?;
    let rb = stmt.query_arrow(param)?.next().expect("no record batch");

    let output_array = rb
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("Expected StringArray");

    assert_eq!(output_array.len(), 3);
    assert_eq!(output_array.value(0), "foo");
    assert_eq!(output_array.value(1), "bar");
    assert_eq!(output_array.value(2), "baz");

    Ok(())
}

#[test]
fn test_binary_view_roundtrip() -> Result<(), Box<dyn Error>> {
    let db = Connection::open_in_memory()?;
    db.register_table_function::<ArrowVTab>("arrow")?;

    let array = BinaryViewArray::from(vec![
        Some(b"hello".as_ref()),
        Some(b"world".as_ref()),
        Some(b"!".as_ref()),
    ]);
    let schema = Schema::new(vec![Field::new("a", array.data_type().clone(), false)]);
    let rb = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array.clone())])?;

    let param = arrow_recordbatch_to_query_params(rb);
    let mut stmt = db.prepare("select a from arrow(?, ?)")?;
    let rb = stmt.query_arrow(param)?.next().expect("no record batch");

    let output_array = rb
        .column(0)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .expect("Expected BinaryArray");

    assert_eq!(output_array.len(), 3);
    assert_eq!(output_array.value(0), b"hello");
    assert_eq!(output_array.value(1), b"world");
    assert_eq!(output_array.value(2), b"!");

    Ok(())
}

#[test]
fn test_string_view_nulls_roundtrip() -> Result<(), Box<dyn Error>> {
    let db = Connection::open_in_memory()?;
    db.register_table_function::<ArrowVTab>("arrow")?;

    let array = StringViewArray::from(vec![Some("foo"), None, Some("baz")]);
    let schema = Schema::new(vec![Field::new("a", array.data_type().clone(), true)]);
    let rb = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array.clone())])?;

    let param = arrow_recordbatch_to_query_params(rb);
    let mut stmt = db.prepare("select a from arrow(?, ?)")?;
    let rb = stmt.query_arrow(param)?.next().expect("no record batch");

    let output_array = rb
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("Expected StringArray");

    assert_eq!(output_array.len(), 3);
    assert!(output_array.is_valid(0));
    assert!(!output_array.is_valid(1));
    assert!(output_array.is_valid(2));
    assert_eq!(output_array.value(0), "foo");
    assert_eq!(output_array.value(2), "baz");

    Ok(())
}

#[test]
fn test_binary_view_nulls_roundtrip() -> Result<(), Box<dyn Error>> {
    let db = Connection::open_in_memory()?;
    db.register_table_function::<ArrowVTab>("arrow")?;

    let array = BinaryViewArray::from(vec![Some(b"hello".as_ref()), None, Some(b"!".as_ref())]);
    let schema = Schema::new(vec![Field::new("a", array.data_type().clone(), true)]);
    let rb = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array.clone())])?;

    let param = arrow_recordbatch_to_query_params(rb);
    let mut stmt = db.prepare("select a from arrow(?, ?)")?;
    let rb = stmt.query_arrow(param)?.next().expect("no record batch");

    let output_array = rb
        .column(0)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .expect("Expected BinaryArray");

    assert_eq!(output_array.len(), 3);
    assert!(output_array.is_valid(0));
    assert!(!output_array.is_valid(1));
    assert!(output_array.is_valid(2));
    assert_eq!(output_array.value(0), b"hello");
    assert_eq!(output_array.value(2), b"!");

    Ok(())
}

#[test]
fn test_large_binary_nulls_roundtrip() -> Result<(), Box<dyn Error>> {
    let db = Connection::open_in_memory()?;
    db.register_table_function::<ArrowVTab>("arrow")?;

    let array = LargeBinaryArray::from_opt_vec(vec![Some(&b"hello"[..]), None, Some(&b"!"[..])]);
    let schema = Schema::new(vec![Field::new("a", array.data_type().clone(), true)]);
    let rb = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array.clone())])?;

    let param = arrow_recordbatch_to_query_params(rb);
    let mut stmt = db.prepare("select a from arrow(?, ?)")?;
    let rb = stmt.query_arrow(param)?.next().expect("no record batch");

    let output_array = rb
        .column(0)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .expect("Expected BinaryArray");

    assert_eq!(output_array.len(), 3);
    assert!(output_array.is_valid(0));
    assert!(!output_array.is_valid(1));
    assert!(output_array.is_valid(2));
    assert_eq!(output_array.value(0), b"hello");
    assert_eq!(output_array.value(2), b"!");

    Ok(())
}

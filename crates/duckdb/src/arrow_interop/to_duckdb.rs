use std::sync::Arc;

use arrow::{
    array::{
        Array, AsArray, BinaryArray, BinaryViewArray, BooleanArray, FixedSizeBinaryArray, FixedSizeListArray,
        GenericListArray, GenericStringArray, IntervalMonthDayNanoArray, LargeBinaryArray, LargeStringArray,
        OffsetSizeTrait, PrimitiveArray, StringViewArray, StructArray, as_boolean_array, as_generic_binary_array,
        as_large_list_array, as_list_array, as_map_array, as_primitive_array, as_string_array, as_struct_array,
    },
    buffer::NullBuffer,
    compute::cast,
    datatypes::*,
    record_batch::RecordBatch,
};
use num::{ToPrimitive, cast::AsPrimitive};

use crate::{
    core::{
        ArrayVector, DataChunkHandle, FlatVector, Inserter, ListVector, LogicalTypeId, StructVector, WritableVector,
    },
    types::Decimal,
};

use super::{
    UUID_BYTE_WIDTH,
    schema::{invalid_uuid_storage_error, validate_arrow_decimal_metadata},
};

/// Writes an Arrow array to a `WritableVector`.
pub fn write_arrow_array_to_vector(
    col: &Arc<dyn Array>,
    chunk: &mut dyn WritableVector,
) -> Result<(), Box<dyn std::error::Error>> {
    write_arrow_array_to_vector_ref(col.as_ref(), chunk)
}

fn write_arrow_array_to_vector_ref(
    col: &dyn Array,
    chunk: &mut dyn WritableVector,
) -> Result<(), Box<dyn std::error::Error>> {
    match col.data_type() {
        dt if dt.is_primitive() || matches!(dt, DataType::Boolean) => {
            primitive_array_to_vector(col, &mut chunk.flat_vector())?;
        }
        DataType::Utf8 => {
            string_array_to_vector(as_string_array(col), &mut chunk.flat_vector());
        }
        DataType::LargeUtf8 => {
            string_array_to_vector(
                col.as_any()
                    .downcast_ref::<LargeStringArray>()
                    .ok_or_else(|| Box::<dyn std::error::Error>::from("Unable to downcast to LargeStringArray"))?,
                &mut chunk.flat_vector(),
            );
        }
        DataType::Utf8View => {
            string_view_array_to_vector(
                col.as_any()
                    .downcast_ref::<StringViewArray>()
                    .ok_or_else(|| Box::<dyn std::error::Error>::from("Unable to downcast to StringViewArray"))?,
                &mut chunk.flat_vector(),
            );
        }
        DataType::Binary => {
            binary_array_to_vector(as_generic_binary_array(col), &mut chunk.flat_vector());
        }
        DataType::FixedSizeBinary(_) => {
            fixed_size_binary_array_to_vector(col.as_fixed_size_binary(), &mut chunk.flat_vector())?;
        }
        DataType::LargeBinary => {
            large_binary_array_to_vector(
                col.as_any()
                    .downcast_ref::<LargeBinaryArray>()
                    .ok_or_else(|| Box::<dyn std::error::Error>::from("Unable to downcast to LargeBinaryArray"))?,
                &mut chunk.flat_vector(),
            );
        }
        DataType::BinaryView => {
            binary_view_array_to_vector(
                col.as_any()
                    .downcast_ref::<BinaryViewArray>()
                    .ok_or_else(|| Box::<dyn std::error::Error>::from("Unable to downcast to BinaryViewArray"))?,
                &mut chunk.flat_vector(),
            );
        }
        DataType::List(_) => {
            list_array_to_vector(as_list_array(col), &mut chunk.list_vector())?;
        }
        DataType::LargeList(_) => {
            list_array_to_vector(as_large_list_array(col), &mut chunk.list_vector())?;
        }
        DataType::FixedSizeList(_, _) => {
            fixed_size_list_array_to_vector(as_fixed_size_list_array(col), &mut chunk.array_vector())?;
        }
        DataType::Struct(_) => {
            let struct_array = as_struct_array(col);
            let mut struct_vector = chunk.struct_vector();
            struct_array_to_vector(struct_array, &mut struct_vector)?;
        }
        DataType::Map(_, _) => {
            // [`MapArray`] is physically a [`ListArray`] of key values pairs stored as an `entries` [`StructArray`] with 2 child fields.
            let map_array = as_map_array(col);
            let mut out = chunk.list_vector();
            list_like_array_to_vector(
                map_array.entries(),
                map_array.value_offsets(),
                map_array.nulls(),
                &mut out,
            )?;
        }
        dt => {
            return Err(format!(
                "column with data_type {dt} is not supported yet, please file an issue https://github.com/duckdb/duckdb-rs"
            )
            .into());
        }
    }

    Ok(())
}

/// Converts a `RecordBatch` to a `DataChunk` in the DuckDB format.
///
/// # Arguments
///
/// * `batch` - A reference to the `RecordBatch` to be converted to a `DataChunk`.
/// * `chunk` - A mutable reference to the `DataChunk` to store the converted data.
pub fn record_batch_to_duckdb_data_chunk(
    batch: &RecordBatch,
    chunk: &mut DataChunkHandle,
) -> Result<(), Box<dyn std::error::Error>> {
    chunk.begin_write()?;
    // Fill the row
    assert_eq!(batch.num_columns(), chunk.num_columns());
    for i in 0..batch.num_columns() {
        let col = batch.column(i);
        let mut vector = chunk.writable_vector(i, batch.num_rows())?;
        write_arrow_array_to_vector(col, &mut vector)?;
    }
    chunk.set_len(batch.num_rows());
    // SAFETY: every writer completed successfully, validity masks every null
    // row, and the committed length is exactly the RecordBatch row count.
    unsafe { chunk.mark_initialized()? };
    Ok(())
}

fn primitive_array_to_flat_vector<T: ArrowPrimitiveType>(array: &PrimitiveArray<T>, out_vector: &mut FlatVector<'_>) {
    // assert!(array.len() <= out_vector.capacity());
    unsafe { out_vector.copy::<T::Native>(array.values()) };
    set_nulls_in_flat_vector(array, out_vector);
}

fn primitive_array_to_flat_vector_cast<T: ArrowPrimitiveType>(
    data_type: DataType,
    array: &dyn Array,
    out_vector: &mut FlatVector<'_>,
) {
    let array = cast(array, &data_type).unwrap_or_else(|_| panic!("array is casted into {data_type}"));
    unsafe { out_vector.copy::<T::Native>(array.as_primitive::<T>().values()) };
    set_nulls_in_flat_vector(&array, out_vector);
}

fn primitive_array_to_vector(array: &dyn Array, out: &mut FlatVector<'_>) -> Result<(), Box<dyn std::error::Error>> {
    match array.data_type() {
        DataType::Boolean => {
            boolean_array_to_vector(as_boolean_array(array), out);
        }
        DataType::UInt8 => {
            primitive_array_to_flat_vector::<UInt8Type>(as_primitive_array(array), out);
        }
        DataType::UInt16 => {
            primitive_array_to_flat_vector::<UInt16Type>(as_primitive_array(array), out);
        }
        DataType::UInt32 => {
            primitive_array_to_flat_vector::<UInt32Type>(as_primitive_array(array), out);
        }
        DataType::UInt64 => {
            primitive_array_to_flat_vector::<UInt64Type>(as_primitive_array(array), out);
        }
        DataType::Int8 => {
            primitive_array_to_flat_vector::<Int8Type>(as_primitive_array(array), out);
        }
        DataType::Int16 => {
            primitive_array_to_flat_vector::<Int16Type>(as_primitive_array(array), out);
        }
        DataType::Int32 => {
            primitive_array_to_flat_vector::<Int32Type>(as_primitive_array(array), out);
        }
        DataType::Int64 => {
            primitive_array_to_flat_vector::<Int64Type>(as_primitive_array(array), out);
        }
        DataType::Float32 => {
            primitive_array_to_flat_vector::<Float32Type>(as_primitive_array(array), out);
        }
        DataType::Float64 => {
            primitive_array_to_flat_vector::<Float64Type>(as_primitive_array(array), out);
        }
        DataType::Decimal32(width, scale) => {
            decimal_array_to_vector::<Decimal32Type>(as_primitive_array(array), out, *width, *scale)?;
        }
        DataType::Decimal64(width, scale) => {
            decimal_array_to_vector::<Decimal64Type>(as_primitive_array(array), out, *width, *scale)?;
        }
        DataType::Decimal128(width, scale) => {
            decimal_array_to_vector::<Decimal128Type>(as_primitive_array(array), out, *width, *scale)?;
        }
        DataType::Interval(_) | DataType::Duration(_) => {
            let array = IntervalMonthDayNanoArray::from(
                cast(array, &DataType::Interval(IntervalUnit::MonthDayNano))
                    .expect("array is casted into IntervalMonthDayNanoArray")
                    .as_primitive::<IntervalMonthDayNanoType>()
                    .values()
                    .iter()
                    .map(|a| IntervalMonthDayNanoType::make_value(a.months, a.days, a.nanoseconds / 1000))
                    .collect::<Vec<_>>(),
            );
            primitive_array_to_flat_vector::<IntervalMonthDayNanoType>(as_primitive_array(&array), out);
        }
        // DuckDB only supports TIMESTAMP_TZ at microsecond precision.
        DataType::Timestamp(_, Some(tz)) => primitive_array_to_flat_vector_cast::<TimestampMicrosecondType>(
            DataType::Timestamp(TimeUnit::Microsecond, Some(tz.clone())),
            array,
            out,
        ),
        DataType::Timestamp(unit, None) => match unit {
            TimeUnit::Second => primitive_array_to_flat_vector::<TimestampSecondType>(as_primitive_array(array), out),
            TimeUnit::Millisecond => {
                primitive_array_to_flat_vector::<TimestampMillisecondType>(as_primitive_array(array), out)
            }
            TimeUnit::Microsecond => {
                primitive_array_to_flat_vector::<TimestampMicrosecondType>(as_primitive_array(array), out)
            }
            TimeUnit::Nanosecond => {
                primitive_array_to_flat_vector::<TimestampNanosecondType>(as_primitive_array(array), out)
            }
        },
        DataType::Date32 => {
            primitive_array_to_flat_vector::<Date32Type>(as_primitive_array(array), out);
        }
        DataType::Date64 => primitive_array_to_flat_vector_cast::<Date32Type>(Date32Type::DATA_TYPE, array, out),
        DataType::Time32(_) => {
            primitive_array_to_flat_vector_cast::<Time64MicrosecondType>(Time64MicrosecondType::DATA_TYPE, array, out)
        }
        DataType::Time64(_) => {
            primitive_array_to_flat_vector_cast::<Time64MicrosecondType>(Time64MicrosecondType::DATA_TYPE, array, out)
        }
        datatype => {
            return Err(format!("Data type \"{datatype}\" not yet supported by Arrow-to-DuckDB conversion").into());
        }
    }
    Ok(())
}

/// Convert Arrow decimal arrays to a DuckDB vector.
fn decimal_array_to_vector<T>(
    array: &PrimitiveArray<T>,
    out: &mut FlatVector<'_>,
    width: u8,
    scale: i8,
) -> Result<(), Box<dyn std::error::Error>>
where
    T: DecimalType,
    T::Native: Into<i128>,
{
    assert!(array.len() <= out.capacity());

    let scale = validate_arrow_decimal_metadata::<T>(width, scale)?;

    match width {
        1..=4 => {
            for i in 0..array.len() {
                let value = arrow_decimal_value(array, width, scale, i)?;
                unsafe { out.write(i, value.to_i16().unwrap()) };
            }
        }
        5..=9 => {
            for i in 0..array.len() {
                let value = arrow_decimal_value(array, width, scale, i)?;
                unsafe { out.write(i, value.to_i32().unwrap()) };
            }
        }
        10..=18 => {
            for i in 0..array.len() {
                let value = arrow_decimal_value(array, width, scale, i)?;
                unsafe { out.write(i, value.to_i64().unwrap()) };
            }
        }
        19..=38 => {
            for i in 0..array.len() {
                let value = arrow_decimal_value(array, width, scale, i)?;
                unsafe { out.write(i, value.to_i128().unwrap()) };
            }
        }
        // This should never happen, arrow only supports 1-38 decimal digits
        _ => return Err(format!("Invalid decimal width: {width}").into()),
    }

    // Set nulls
    set_nulls_in_flat_vector(array, out);
    Ok(())
}

fn arrow_decimal_value<T>(
    array: &PrimitiveArray<T>,
    width: u8,
    scale: u8,
    row: usize,
) -> Result<i128, Box<dyn std::error::Error>>
where
    T: DecimalType,
    T::Native: Into<i128>,
{
    if array.is_null(row) {
        // Placeholder only; decimal_array_to_vector applies the validity mask.
        return Ok(0);
    }

    let value: i128 = array.value(row).into();
    Decimal::new(width, scale, value).map_err(|err| format!("invalid Arrow decimal value at row {row}: {err}"))?;
    Ok(value)
}

/// Convert Arrow [BooleanArray] to a duckdb vector.
fn boolean_array_to_vector(array: &BooleanArray, out: &mut FlatVector<'_>) {
    assert!(array.len() <= out.capacity());

    for i in 0..array.len() {
        unsafe { out.write(i, array.value(i)) };
    }
    set_nulls_in_flat_vector(array, out);
}

fn string_array_to_vector<O: OffsetSizeTrait>(array: &GenericStringArray<O>, out: &mut FlatVector<'_>) {
    assert!(array.len() <= out.capacity());

    // TODO: zero copy assignment
    for i in 0..array.len() {
        let s = array.value(i);
        out.insert(i, s);
    }
    set_nulls_in_flat_vector(array, out);
}

fn string_view_array_to_vector(array: &StringViewArray, out: &mut FlatVector<'_>) {
    assert!(array.len() <= out.capacity());

    for i in 0..array.len() {
        let s = array.value(i);
        out.insert(i, s);
    }
    set_nulls_in_flat_vector(array, out);
}

fn binary_array_to_vector(array: &BinaryArray, out: &mut FlatVector<'_>) {
    assert!(array.len() <= out.capacity());

    for i in 0..array.len() {
        let s = array.value(i);
        out.insert(i, s);
    }
    set_nulls_in_flat_vector(array, out);
}

fn binary_view_array_to_vector(array: &BinaryViewArray, out: &mut FlatVector<'_>) {
    assert!(array.len() <= out.capacity());

    for i in 0..array.len() {
        let s = array.value(i);
        out.insert(i, s);
    }
    set_nulls_in_flat_vector(array, out);
}

fn fixed_size_binary_array_to_vector(
    array: &FixedSizeBinaryArray,
    out: &mut FlatVector<'_>,
) -> Result<(), Box<dyn std::error::Error>> {
    assert!(array.len() <= out.capacity());

    if out.logical_type().id() == LogicalTypeId::Uuid {
        return uuid_array_to_vector(array, out);
    }

    for i in 0..array.len() {
        let s = array.value(i);
        out.insert(i, s);
    }
    set_nulls_in_flat_vector(array, out);
    Ok(())
}

fn uuid_array_to_vector(
    array: &FixedSizeBinaryArray,
    out: &mut FlatVector<'_>,
) -> Result<(), Box<dyn std::error::Error>> {
    if array.value_length() != UUID_BYTE_WIDTH {
        return Err(invalid_uuid_storage_error(&DataType::FixedSizeBinary(
            array.value_length(),
        )));
    }

    for i in 0..array.len() {
        let bytes: [u8; UUID_BYTE_WIDTH as usize] = array
            .value(i)
            .try_into()
            .expect("FixedSizeBinary value length was validated for UUID");
        // DuckDB stores UUIDs as HUGEINT with the MSB flipped so they
        // sort correctly as signed i128.
        unsafe { out.write(i, i128::from_be_bytes(bytes) ^ i128::MIN) };
    }
    set_nulls_in_flat_vector(array, out);
    Ok(())
}

fn large_binary_array_to_vector(array: &LargeBinaryArray, out: &mut FlatVector<'_>) {
    assert!(array.len() <= out.capacity());

    for i in 0..array.len() {
        let s = array.value(i);
        out.insert(i, s);
    }
    set_nulls_in_flat_vector(array, out);
}

fn list_array_to_vector<O: OffsetSizeTrait + AsPrimitive<usize>>(
    array: &GenericListArray<O>,
    out: &mut ListVector<'_>,
) -> Result<(), Box<dyn std::error::Error>> {
    let value_array = array.values();
    list_like_array_to_vector(value_array.as_ref(), array.value_offsets(), array.nulls(), out)
}

fn list_like_array_to_vector<O: Copy + AsPrimitive<usize>>(
    values: &dyn Array,
    offsets: &[O],
    nulls: Option<&NullBuffer>,
    out: &mut ListVector<'_>,
) -> Result<(), Box<dyn std::error::Error>> {
    debug_assert!(!offsets.is_empty());
    if let Some(nulls) = nulls {
        debug_assert_eq!(offsets.len(), nulls.len() + 1);
    }

    let mut child = out.writable_child(values.len())?;
    write_arrow_array_to_vector_ref(values, &mut child)?;

    for (i, window) in offsets.windows(2).enumerate() {
        let offset = window[0].as_();
        out.set_entry(i, offset, window[1].as_() - offset);
    }
    set_nulls_in_list_vector(nulls, out);

    Ok(())
}

fn fixed_size_list_array_to_vector(
    array: &FixedSizeListArray,
    out: &mut ArrayVector<'_>,
) -> Result<(), Box<dyn std::error::Error>> {
    let value_array = array.values();
    // arrow-rs normalizes FixedSizeListArray slices by slicing the child
    // values array, so this is already the exact row count * fixed width span.
    debug_assert_eq!(value_array.len(), array.len() * array.value_length() as usize);
    let mut child = out.writable_child(value_array.len())?;
    write_arrow_array_to_vector_ref(value_array.as_ref(), &mut child)?;

    set_nulls_in_array_vector(array, out);

    Ok(())
}

/// Force downcast of an [`Array`], such as an `ArrayRef`, to
/// [`FixedSizeListArray`], panic'ing on failure.
fn as_fixed_size_list_array(arr: &dyn Array) -> &FixedSizeListArray {
    arr.as_any().downcast_ref::<FixedSizeListArray>().unwrap()
}

fn struct_array_to_vector(array: &StructArray, out: &mut StructVector<'_>) -> Result<(), Box<dyn std::error::Error>> {
    for i in 0..array.num_columns() {
        let column = array.column(i);
        let mut child = out.writable_child(i, array.len())?;
        write_arrow_array_to_vector_ref(column.as_ref(), &mut child)?;
    }
    set_nulls_in_struct_vector(array, out);
    Ok(())
}

fn set_nulls_in_flat_vector(array: &dyn Array, out_vector: &mut FlatVector<'_>) {
    if let Some(nulls) = array.nulls() {
        for (i, null) in nulls.into_iter().enumerate() {
            if !null {
                out_vector.set_null(i);
            }
        }
    }
}

fn set_nulls_in_struct_vector(array: &dyn Array, out_vector: &mut StructVector<'_>) {
    if let Some(nulls) = array.nulls() {
        for (i, null) in nulls.into_iter().enumerate() {
            if !null {
                out_vector.set_null(i);
            }
        }
    }
}

fn set_nulls_in_array_vector(array: &dyn Array, out_vector: &mut ArrayVector<'_>) {
    if let Some(nulls) = array.nulls() {
        for (i, null) in nulls.into_iter().enumerate() {
            if !null {
                out_vector.set_null(i);
            }
        }
    }
}

fn set_nulls_in_list_vector(nulls: Option<&NullBuffer>, out_vector: &mut ListVector<'_>) {
    if let Some(nulls) = nulls {
        for (i, null) in nulls.into_iter().enumerate() {
            if !null {
                out_vector.set_null(i);
            }
        }
    }
}

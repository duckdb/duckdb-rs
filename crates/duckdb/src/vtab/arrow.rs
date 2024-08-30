use super::{BindInfo, DataChunkHandle, Free, FunctionInfo, InitInfo, LogicalTypeHandle, LogicalTypeId, VTab};
use std::ptr::null_mut;

use crate::core::{ArrayVector, FlatVector, Inserter, ListVector, StructVector, Vector};
use arrow::array::{
    as_boolean_array, as_generic_binary_array, as_large_list_array, as_list_array, as_primitive_array, as_string_array,
    as_struct_array, Array, ArrayData, AsArray, BinaryArray, BooleanArray, Decimal128Array, FixedSizeListArray,
    GenericListArray, GenericStringArray, LargeStringArray, OffsetSizeTrait, PrimitiveArray, StructArray,
};

use arrow::{
    datatypes::*,
    ffi::{from_ffi, FFI_ArrowArray, FFI_ArrowSchema},
    record_batch::RecordBatch,
};

use num::{cast::AsPrimitive, ToPrimitive};

/// A pointer to the Arrow record batch for the table function.
#[repr(C)]
pub struct ArrowBindData {
    rb: *mut RecordBatch,
}

impl Free for ArrowBindData {
    fn free(&mut self) {
        unsafe {
            if self.rb.is_null() {
                return;
            }
            drop(Box::from_raw(self.rb));
        }
    }
}

/// Keeps track of whether the Arrow record batch has been consumed.
#[repr(C)]
pub struct ArrowInitData {
    done: bool,
}

impl Free for ArrowInitData {}

/// The Arrow table function.
pub struct ArrowVTab;

unsafe fn address_to_arrow_schema(address: usize) -> FFI_ArrowSchema {
    let ptr = address as *mut FFI_ArrowSchema;
    *Box::from_raw(ptr)
}

unsafe fn address_to_arrow_array(address: usize) -> FFI_ArrowArray {
    let ptr = address as *mut FFI_ArrowArray;
    *Box::from_raw(ptr)
}

unsafe fn address_to_arrow_ffi(array: usize, schema: usize) -> (FFI_ArrowArray, FFI_ArrowSchema) {
    let array = address_to_arrow_array(array);
    let schema = address_to_arrow_schema(schema);
    (array, schema)
}

unsafe fn address_to_arrow_record_batch(array: usize, schema: usize) -> RecordBatch {
    let (array, schema) = address_to_arrow_ffi(array, schema);
    let array_data = from_ffi(array, &schema).expect("ok");
    let struct_array = StructArray::from(array_data);
    RecordBatch::from(&struct_array)
}

impl VTab for ArrowVTab {
    type BindData = ArrowBindData;
    type InitData = ArrowInitData;

    unsafe fn bind(bind: &BindInfo, data: *mut ArrowBindData) -> Result<(), Box<dyn std::error::Error>> {
        (*data).rb = null_mut();
        let param_count = bind.get_parameter_count();
        if param_count != 2 {
            return Err(format!("Bad param count: {param_count}, expected 2").into());
        }
        let array = bind.get_parameter(0).to_int64();
        let schema = bind.get_parameter(1).to_int64();
        unsafe {
            let rb = address_to_arrow_record_batch(array as usize, schema as usize);
            for f in rb.schema().fields() {
                let name = f.name();
                let data_type = f.data_type();
                let logical_type = to_duckdb_logical_type(data_type)?;
                bind.add_result_column(name, logical_type);
            }
            (*data).rb = Box::into_raw(Box::new(rb));
        }
        Ok(())
    }

    unsafe fn init(_: &InitInfo, data: *mut ArrowInitData) -> Result<(), Box<dyn std::error::Error>> {
        unsafe {
            (*data).done = false;
        }
        Ok(())
    }

    unsafe fn func(func: &FunctionInfo, output: &mut DataChunkHandle) -> Result<(), Box<dyn std::error::Error>> {
        let init_info = func.get_init_data::<ArrowInitData>();
        let bind_info = func.get_bind_data::<ArrowBindData>();
        unsafe {
            if (*init_info).done {
                output.set_len(0);
            } else {
                let rb = Box::from_raw((*bind_info).rb);
                (*bind_info).rb = null_mut(); // erase ref in case of failure in record_batch_to_duckdb_data_chunk
                record_batch_to_duckdb_data_chunk(&rb, output)?;
                (*bind_info).rb = Box::into_raw(rb);
                (*init_info).done = true;
            }
        }
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![
            LogicalTypeHandle::from(LogicalTypeId::UBigint), // file path
            LogicalTypeHandle::from(LogicalTypeId::UBigint), // sheet name
        ])
    }
}

/// Convert arrow DataType to duckdb type id
pub fn to_duckdb_type_id(data_type: &DataType) -> Result<LogicalTypeId, Box<dyn std::error::Error>> {
    use LogicalTypeId::*;

    let type_id = match data_type {
        DataType::Boolean => Boolean,
        DataType::Int8 => Tinyint,
        DataType::Int16 => Smallint,
        DataType::Int32 => Integer,
        DataType::Int64 => Bigint,
        DataType::UInt8 => UTinyint,
        DataType::UInt16 => USmallint,
        DataType::UInt32 => UInteger,
        DataType::UInt64 => UBigint,
        DataType::Float32 => Float,
        DataType::Float64 => Double,
        DataType::Timestamp(unit, None) => match unit {
            TimeUnit::Second => TimestampS,
            TimeUnit::Millisecond => TimestampMs,
            TimeUnit::Microsecond => Timestamp,
            TimeUnit::Nanosecond => TimestampNs,
        },
        DataType::Timestamp(_, Some(_)) => TimestampTZ,
        DataType::Date32 => Date,
        DataType::Date64 => Date,
        DataType::Time32(_) => Time,
        DataType::Time64(_) => Time,
        DataType::Duration(_) => Interval,
        DataType::Interval(_) => Interval,
        DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_) => Blob,
        DataType::Utf8 | DataType::LargeUtf8 => Varchar,
        DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _) => List,
        DataType::Struct(_) => Struct,
        DataType::Union(_, _) => Union,
        // DataType::Dictionary(_, _) => todo!(),
        // duckdb/src/main/capi/helper-c.cpp does not support decimal
        // DataType::Decimal128(_, _) => Decimal,
        // DataType::Decimal256(_, _) => Decimal,
        DataType::Decimal128(_, _) => Decimal,
        DataType::Decimal256(_, _) => Double,
        DataType::Map(_, _) => Map,
        _ => {
            return Err(format!("Unsupported data type: {:?}", data_type).into());
        }
    };
    Ok(type_id)
}

/// Convert arrow DataType to duckdb logical type
pub fn to_duckdb_logical_type(data_type: &DataType) -> Result<LogicalTypeHandle, Box<dyn std::error::Error>> {
    match data_type {
        DataType::Dictionary(_, value_type) => to_duckdb_logical_type(value_type),
        DataType::Struct(fields) => {
            let mut shape = vec![];
            for field in fields.iter() {
                shape.push((field.name().as_str(), to_duckdb_logical_type(field.data_type())?));
            }
            Ok(LogicalTypeHandle::struct_type(shape.as_slice()))
        }
        DataType::List(child) | DataType::LargeList(child) => {
            Ok(LogicalTypeHandle::list(&to_duckdb_logical_type(child.data_type())?))
        }
        DataType::FixedSizeList(child, array_size) => Ok(LogicalTypeHandle::array(
            &to_duckdb_logical_type(child.data_type())?,
            *array_size as u64,
        )),
        DataType::Decimal128(width, scale) if *scale > 0 => {
            // DuckDB does not support negative decimal scales
            Ok(LogicalTypeHandle::decimal(*width, (*scale).try_into().unwrap()))
        }
        DataType::Boolean | DataType::Utf8 | DataType::LargeUtf8 | DataType::Binary | DataType::LargeBinary => {
            Ok(LogicalTypeHandle::from(to_duckdb_type_id(data_type)?))
        }
        dtype if dtype.is_primitive() => Ok(LogicalTypeHandle::from(to_duckdb_type_id(data_type)?)),
        _ => Err(format!(
            "Unsupported data type: {data_type}, please file an issue https://github.com/wangfenjin/duckdb-rs"
        )
        .into()),
    }
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
    // Fill the row
    assert_eq!(batch.num_columns(), chunk.num_columns());
    for i in 0..batch.num_columns() {
        let col = batch.column(i);
        match col.data_type() {
            dt if dt.is_primitive() || matches!(dt, DataType::Boolean) => {
                primitive_array_to_vector(col, &mut chunk.flat_vector(i))?;
            }
            DataType::Utf8 => {
                string_array_to_vector(as_string_array(col.as_ref()), &mut chunk.flat_vector(i));
            }
            DataType::LargeUtf8 => {
                string_array_to_vector(
                    col.as_ref()
                        .as_any()
                        .downcast_ref::<LargeStringArray>()
                        .ok_or_else(|| Box::<dyn std::error::Error>::from("Unable to downcast to LargeStringArray"))?,
                    &mut chunk.flat_vector(i),
                );
            }
            DataType::Binary => {
                binary_array_to_vector(as_generic_binary_array(col.as_ref()), &mut chunk.flat_vector(i));
            }
            DataType::List(_) => {
                list_array_to_vector(as_list_array(col.as_ref()), &mut chunk.list_vector(i))?;
            }
            DataType::LargeList(_) => {
                list_array_to_vector(as_large_list_array(col.as_ref()), &mut chunk.list_vector(i))?;
            }
            DataType::FixedSizeList(_, _) => {
                fixed_size_list_array_to_vector(as_fixed_size_list_array(col.as_ref()), &mut chunk.array_vector(i))?;
            }
            DataType::Struct(_) => {
                let struct_array = as_struct_array(col.as_ref());
                let mut struct_vector = chunk.struct_vector(i);
                struct_array_to_vector(struct_array, &mut struct_vector)?;
            }
            _ => {
                return Err(format!(
                    "column {} is not supported yet, please file an issue https://github.com/wangfenjin/duckdb-rs",
                    batch.schema().field(i)
                )
                .into());
            }
        }
    }
    chunk.set_len(batch.num_rows());
    Ok(())
}

fn primitive_array_to_flat_vector<T: ArrowPrimitiveType>(array: &PrimitiveArray<T>, out_vector: &mut FlatVector) {
    // assert!(array.len() <= out_vector.capacity());
    out_vector.copy::<T::Native>(array.values());
    set_nulls_in_flat_vector(array, out_vector);
}

fn primitive_array_to_flat_vector_cast<T: ArrowPrimitiveType>(
    data_type: DataType,
    array: &dyn Array,
    out_vector: &mut dyn Vector,
) {
    let array = arrow::compute::kernels::cast::cast(array, &data_type).unwrap();
    let out_vector: &mut FlatVector = out_vector.as_mut_any().downcast_mut().unwrap();
    out_vector.copy::<T::Native>(array.as_primitive::<T>().values());
    set_nulls_in_flat_vector(&array, out_vector);
}

fn primitive_array_to_vector(array: &dyn Array, out: &mut dyn Vector) -> Result<(), Box<dyn std::error::Error>> {
    match array.data_type() {
        DataType::Boolean => {
            boolean_array_to_vector(as_boolean_array(array), out.as_mut_any().downcast_mut().unwrap());
        }
        DataType::UInt8 => {
            primitive_array_to_flat_vector::<UInt8Type>(
                as_primitive_array(array),
                out.as_mut_any().downcast_mut().unwrap(),
            );
        }
        DataType::UInt16 => {
            primitive_array_to_flat_vector::<UInt16Type>(
                as_primitive_array(array),
                out.as_mut_any().downcast_mut().unwrap(),
            );
        }
        DataType::UInt32 => {
            primitive_array_to_flat_vector::<UInt32Type>(
                as_primitive_array(array),
                out.as_mut_any().downcast_mut().unwrap(),
            );
        }
        DataType::UInt64 => {
            primitive_array_to_flat_vector::<UInt64Type>(
                as_primitive_array(array),
                out.as_mut_any().downcast_mut().unwrap(),
            );
        }
        DataType::Int8 => {
            primitive_array_to_flat_vector::<Int8Type>(
                as_primitive_array(array),
                out.as_mut_any().downcast_mut().unwrap(),
            );
        }
        DataType::Int16 => {
            primitive_array_to_flat_vector::<Int16Type>(
                as_primitive_array(array),
                out.as_mut_any().downcast_mut().unwrap(),
            );
        }
        DataType::Int32 => {
            primitive_array_to_flat_vector::<Int32Type>(
                as_primitive_array(array),
                out.as_mut_any().downcast_mut().unwrap(),
            );
        }
        DataType::Int64 => {
            primitive_array_to_flat_vector::<Int64Type>(
                as_primitive_array(array),
                out.as_mut_any().downcast_mut().unwrap(),
            );
        }
        DataType::Float32 => {
            primitive_array_to_flat_vector::<Float32Type>(
                as_primitive_array(array),
                out.as_mut_any().downcast_mut().unwrap(),
            );
        }
        DataType::Float64 => {
            primitive_array_to_flat_vector::<Float64Type>(
                as_primitive_array(array),
                out.as_mut_any().downcast_mut().unwrap(),
            );
        }
        DataType::Decimal128(width, _) => {
            decimal_array_to_vector(
                as_primitive_array(array),
                out.as_mut_any().downcast_mut().unwrap(),
                *width,
            );
        }

        // DuckDB Only supports timetamp_tz in microsecond precision
        DataType::Timestamp(_, Some(tz)) => primitive_array_to_flat_vector_cast::<TimestampMicrosecondType>(
            DataType::Timestamp(TimeUnit::Microsecond, Some(tz.clone())),
            array,
            out,
        ),
        DataType::Timestamp(unit, None) => match unit {
            TimeUnit::Second => primitive_array_to_flat_vector::<TimestampSecondType>(
                as_primitive_array(array),
                out.as_mut_any().downcast_mut().unwrap(),
            ),
            TimeUnit::Millisecond => primitive_array_to_flat_vector::<TimestampMillisecondType>(
                as_primitive_array(array),
                out.as_mut_any().downcast_mut().unwrap(),
            ),
            TimeUnit::Microsecond => primitive_array_to_flat_vector::<TimestampMicrosecondType>(
                as_primitive_array(array),
                out.as_mut_any().downcast_mut().unwrap(),
            ),
            TimeUnit::Nanosecond => primitive_array_to_flat_vector::<TimestampNanosecondType>(
                as_primitive_array(array),
                out.as_mut_any().downcast_mut().unwrap(),
            ),
        },
        DataType::Date32 => {
            primitive_array_to_flat_vector::<Date32Type>(
                as_primitive_array(array),
                out.as_mut_any().downcast_mut().unwrap(),
            );
        }
        DataType::Date64 => primitive_array_to_flat_vector_cast::<Date32Type>(Date32Type::DATA_TYPE, array, out),
        DataType::Time32(_) => {
            primitive_array_to_flat_vector_cast::<Time64MicrosecondType>(Time64MicrosecondType::DATA_TYPE, array, out)
        }
        DataType::Time64(_) => {
            primitive_array_to_flat_vector_cast::<Time64MicrosecondType>(Time64MicrosecondType::DATA_TYPE, array, out)
        }
        datatype => return Err(format!("Data type \"{datatype}\" not yet supported by ArrowVTab").into()),
    }
    Ok(())
}

/// Convert Arrow [Decimal128Array] to a duckdb vector.
fn decimal_array_to_vector(array: &Decimal128Array, out: &mut FlatVector, width: u8) {
    match width {
        1..=4 => {
            let out_data = out.as_mut_slice();
            for (i, value) in array.values().iter().enumerate() {
                out_data[i] = value.to_i16().unwrap();
            }
        }
        5..=9 => {
            let out_data = out.as_mut_slice();
            for (i, value) in array.values().iter().enumerate() {
                out_data[i] = value.to_i32().unwrap();
            }
        }
        10..=18 => {
            let out_data = out.as_mut_slice();
            for (i, value) in array.values().iter().enumerate() {
                out_data[i] = value.to_i64().unwrap();
            }
        }
        19..=38 => {
            let out_data = out.as_mut_slice();
            for (i, value) in array.values().iter().enumerate() {
                out_data[i] = value.to_i128().unwrap();
            }
        }
        // This should never happen, arrow only supports 1-38 decimal digits
        _ => panic!("Invalid decimal width: {}", width),
    }

    // Set nulls
    set_nulls_in_flat_vector(array, out);
}

/// Convert Arrow [BooleanArray] to a duckdb vector.
fn boolean_array_to_vector(array: &BooleanArray, out: &mut FlatVector) {
    assert!(array.len() <= out.capacity());

    for i in 0..array.len() {
        out.as_mut_slice()[i] = array.value(i);
    }
    set_nulls_in_flat_vector(array, out);
}

fn string_array_to_vector<O: OffsetSizeTrait>(array: &GenericStringArray<O>, out: &mut FlatVector) {
    assert!(array.len() <= out.capacity());

    // TODO: zero copy assignment
    for i in 0..array.len() {
        let s = array.value(i);
        out.insert(i, s);
    }
    set_nulls_in_flat_vector(array, out);
}

fn binary_array_to_vector(array: &BinaryArray, out: &mut FlatVector) {
    assert!(array.len() <= out.capacity());

    for i in 0..array.len() {
        let s = array.value(i);
        out.insert(i, s);
    }
    set_nulls_in_flat_vector(array, out);
}

fn list_array_to_vector<O: OffsetSizeTrait + AsPrimitive<usize>>(
    array: &GenericListArray<O>,
    out: &mut ListVector,
) -> Result<(), Box<dyn std::error::Error>> {
    let value_array = array.values();
    let mut child = out.child(value_array.len());
    match value_array.data_type() {
        dt if dt.is_primitive() => {
            primitive_array_to_vector(value_array.as_ref(), &mut child)?;
        }
        DataType::Utf8 => {
            string_array_to_vector(as_string_array(value_array.as_ref()), &mut child);
        }
        DataType::Binary => {
            binary_array_to_vector(as_generic_binary_array(value_array.as_ref()), &mut child);
        }
        _ => {
            return Err("Nested list is not supported yet.".into());
        }
    }

    for i in 0..array.len() {
        let offset = array.value_offsets()[i];
        let length = array.value_length(i);
        out.set_entry(i, offset.as_(), length.as_());
    }
    set_nulls_in_list_vector(array, out);

    Ok(())
}

fn fixed_size_list_array_to_vector(
    array: &FixedSizeListArray,
    out: &mut ArrayVector,
) -> Result<(), Box<dyn std::error::Error>> {
    let value_array = array.values();
    let mut child = out.child(value_array.len());
    match value_array.data_type() {
        dt if dt.is_primitive() => {
            primitive_array_to_vector(value_array.as_ref(), &mut child)?;
        }
        DataType::Utf8 => {
            string_array_to_vector(as_string_array(value_array.as_ref()), &mut child);
        }
        DataType::Binary => {
            binary_array_to_vector(as_generic_binary_array(value_array.as_ref()), &mut child);
        }
        _ => {
            return Err("Nested array is not supported yet.".into());
        }
    }

    set_nulls_in_array_vector(array, out);

    Ok(())
}

/// Force downcast of an [`Array`], such as an [`ArrayRef`], to
/// [`FixedSizeListArray`], panic'ing on failure.
fn as_fixed_size_list_array(arr: &dyn Array) -> &FixedSizeListArray {
    arr.as_any().downcast_ref::<FixedSizeListArray>().unwrap()
}

fn struct_array_to_vector(array: &StructArray, out: &mut StructVector) -> Result<(), Box<dyn std::error::Error>> {
    for i in 0..array.num_columns() {
        let column = array.column(i);
        match column.data_type() {
            dt if dt.is_primitive() || matches!(dt, DataType::Boolean) => {
                primitive_array_to_vector(column, &mut out.child(i))?;
            }
            DataType::Utf8 => {
                string_array_to_vector(as_string_array(column.as_ref()), &mut out.child(i));
            }
            DataType::Binary => {
                binary_array_to_vector(as_generic_binary_array(column.as_ref()), &mut out.child(i));
            }
            DataType::List(_) => {
                list_array_to_vector(as_list_array(column.as_ref()), &mut out.list_vector_child(i))?;
            }
            DataType::LargeList(_) => {
                list_array_to_vector(as_large_list_array(column.as_ref()), &mut out.list_vector_child(i))?;
            }
            DataType::FixedSizeList(_, _) => {
                fixed_size_list_array_to_vector(
                    as_fixed_size_list_array(column.as_ref()),
                    &mut out.array_vector_child(i),
                )?;
            }
            DataType::Struct(_) => {
                let struct_array = as_struct_array(column.as_ref());
                let mut struct_vector = out.struct_vector_child(i);
                struct_array_to_vector(struct_array, &mut struct_vector)?;
            }
            _ => {
                unimplemented!(
                    "Unsupported data type: {}, please file an issue https://github.com/wangfenjin/duckdb-rs",
                    column.data_type()
                );
            }
        }
    }
    set_nulls_in_struct_vector(array, out);
    Ok(())
}

/// Pass RecordBatch to duckdb.
///
/// # Safety
/// The caller must ensure that the pointer is valid
/// It's recommended to always use this function with arrow()
pub fn arrow_recordbatch_to_query_params(rb: RecordBatch) -> [usize; 2] {
    let data = ArrayData::from(StructArray::from(rb));
    arrow_arraydata_to_query_params(data)
}

/// Pass ArrayData to duckdb.
///
/// # Safety
/// The caller must ensure that the pointer is valid
/// It's recommended to always use this function with arrow()
pub fn arrow_arraydata_to_query_params(data: ArrayData) -> [usize; 2] {
    let array = FFI_ArrowArray::new(&data);
    let schema = FFI_ArrowSchema::try_from(data.data_type()).expect("Failed to convert schema");
    arrow_ffi_to_query_params(array, schema)
}

/// Pass array and schema as a pointer to duckdb.
///
/// # Safety
/// The caller must ensure that the pointer is valid
/// It's recommended to always use this function with arrow()
pub fn arrow_ffi_to_query_params(array: FFI_ArrowArray, schema: FFI_ArrowSchema) -> [usize; 2] {
    let arr = Box::into_raw(Box::new(array));
    let sch = Box::into_raw(Box::new(schema));

    [arr as *mut _ as usize, sch as *mut _ as usize]
}

fn set_nulls_in_flat_vector(array: &dyn Array, out_vector: &mut FlatVector) {
    if let Some(nulls) = array.nulls() {
        for (i, null) in nulls.into_iter().enumerate() {
            if !null {
                out_vector.set_null(i);
            }
        }
    }
}

fn set_nulls_in_struct_vector(array: &dyn Array, out_vector: &mut StructVector) {
    if let Some(nulls) = array.nulls() {
        for (i, null) in nulls.into_iter().enumerate() {
            if !null {
                out_vector.set_null(i);
            }
        }
    }
}

fn set_nulls_in_array_vector(array: &dyn Array, out_vector: &mut ArrayVector) {
    if let Some(nulls) = array.nulls() {
        for (i, null) in nulls.into_iter().enumerate() {
            if !null {
                out_vector.set_null(i);
            }
        }
    }
}

fn set_nulls_in_list_vector(array: &dyn Array, out_vector: &mut ListVector) {
    if let Some(nulls) = array.nulls() {
        for (i, null) in nulls.into_iter().enumerate() {
            if !null {
                out_vector.set_null(i);
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::{arrow_recordbatch_to_query_params, ArrowVTab};
    use crate::{Connection, Result};
    use arrow::{
        array::{
            Array, ArrayRef, AsArray, BinaryArray, Date32Array, Date64Array, Decimal128Array, Decimal256Array,
            FixedSizeListArray, GenericByteArray, GenericListArray, Int32Array, LargeStringArray, ListArray,
            OffsetSizeTrait, PrimitiveArray, StringArray, StructArray, Time32SecondArray, Time64MicrosecondArray,
            TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
        },
        buffer::{OffsetBuffer, ScalarBuffer},
        datatypes::{i256, ArrowPrimitiveType, ByteArrayType, DataType, Field, Fields, Schema},
        record_batch::RecordBatch,
    };
    use std::{error::Error, sync::Arc};

    #[test]
    fn test_vtab_arrow() -> Result<(), Box<dyn Error>> {
        let db = Connection::open_in_memory()?;
        db.register_table_function::<ArrowVTab>("arrow")?;

        let rbs: Vec<RecordBatch> = db
            .prepare("SELECT * FROM read_parquet('./examples/int32_decimal.parquet');")?
            .query_arrow([])?
            .collect();
        let param = arrow_recordbatch_to_query_params(rbs.into_iter().next().unwrap());
        let mut stmt = db.prepare("select sum(value) from arrow(?, ?)")?;
        let mut arr = stmt.query_arrow(param)?;
        let rb = arr.next().expect("no record batch");
        assert_eq!(rb.num_columns(), 1);
        let column = rb.column(0).as_any().downcast_ref::<Decimal128Array>().unwrap();
        assert_eq!(column.len(), 1);
        assert_eq!(column.value(0), i128::from(30000));
        Ok(())
    }

    #[test]
    fn test_vtab_arrow_rust_array() -> Result<(), Box<dyn Error>> {
        let db = Connection::open_in_memory()?;
        db.register_table_function::<ArrowVTab>("arrow")?;

        // This is a show case that it's easy for you to build an in-memory data
        // and pass into duckdb
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let array = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let rb = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array)]).expect("failed to create record batch");
        let param = arrow_recordbatch_to_query_params(rb);
        let mut stmt = db.prepare("select sum(a)::int32 from arrow(?, ?)")?;
        let mut arr = stmt.query_arrow(param)?;
        let rb = arr.next().expect("no record batch");
        assert_eq!(rb.num_columns(), 1);
        let column = rb.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(column.len(), 1);
        assert_eq!(column.value(0), 15);
        Ok(())
    }

    #[test]
    fn test_append_struct() -> Result<(), Box<dyn Error>> {
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
    fn test_append_struct_contains_null() -> Result<(), Box<dyn Error>> {
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
        assert!(output_any_array
            .data_type()
            .equals_datatype(expected_output_array.data_type()));

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

    //field: FieldRef, size: i32, values: ArrayRef, nulls: Option<NullBuffer>
    #[test]
    fn test_fixed_array_roundtrip() -> Result<(), Box<dyn Error>> {
        let array = FixedSizeListArray::new(
            Arc::new(Field::new("item", DataType::Int32, true)),
            2,
            Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3), Some(4), Some(5)])),
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
        assert!(output_any_array
            .data_type()
            .equals_datatype(expected_output_array.data_type()));

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
        let array: PrimitiveArray<arrow::datatypes::Decimal128Type> =
            Decimal128Array::from(vec![i128::from(1), i128::from(2), i128::from(3)]);
        check_rust_primitive_array_roundtrip(array.clone(), array)?;

        // With width and scale
        let array: PrimitiveArray<arrow::datatypes::Decimal128Type> =
            Decimal128Array::from(vec![i128::from(12345)]).with_data_type(DataType::Decimal128(5, 2));
        check_rust_primitive_array_roundtrip(array.clone(), array)?;
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

        let res = match stmt.execute(arrow_recordbatch_to_query_params(batch)) {
            Ok(..) => None,
            Err(e) => Some(e),
        }
        .unwrap();

        assert_eq!(
            res,
            crate::error::Error::DuckDBFailure(
                crate::ffi::Error {
                    code: crate::ffi::ErrorCode::Unknown,
                    extended_code: 1
                },
                Some("Invalid Input Error: Data type \"Decimal256(76, 10)\" not yet supported by ArrowVTab".to_owned())
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
}

use std::ptr::null_mut;

use arrow::{
    array::{
        as_boolean_array, as_generic_binary_array, as_large_list_array, as_list_array, as_primitive_array,
        as_string_array, as_struct_array, as_union_array, Array, ArrayData, ArrayRef, AsArray, BinaryArray,
        BooleanArray, Decimal128Array, FixedSizeListArray, GenericStringArray, LargeStringArray, OffsetSizeTrait,
        PrimitiveArray, StructArray,
    },
    datatypes::*,
    ffi::{from_ffi, FFI_ArrowArray, FFI_ArrowSchema},
    record_batch::RecordBatch,
};
use num::{cast::AsPrimitive, ToPrimitive};

use crate::{
    core::{DataChunk, DataChunkHandle, FlatVector, Inserter, LogicalTypeHandle, LogicalTypeId, Vector},
    vtab::{BindInfo, Free, FunctionInfo, InitInfo, VTab},
};

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

    fn bind(bind: &BindInfo, data: *mut ArrowBindData) -> Result<(), Box<dyn std::error::Error>> {
        unsafe {
            (*data).rb = null_mut();
        }
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

    fn init(_: &InitInfo, data: *mut ArrowInitData) -> Result<(), Box<dyn std::error::Error>> {
        unsafe {
            (*data).done = false;
        }
        Ok(())
    }

    fn func(func: &FunctionInfo, output: DataChunkHandle) -> Result<(), Box<dyn std::error::Error>> {
        let init_info = func.get_init_data::<ArrowInitData>();
        let bind_info = func.get_bind_data::<ArrowBindData>();
        unsafe {
            if (*init_info).done {
                output.set_len(0);
            } else {
                let rb = Box::from_raw((*bind_info).rb);
                (*bind_info).rb = null_mut(); // erase ref in case of failure in record_batch_to_duckdb_data_chunk
                let mut chunk = DataChunk::from_arrow_schema(rb.schema_ref(), output.with_capacity(rb.num_rows()));
                record_batch_to_duckdb_data_chunk(&rb, &mut chunk)?;
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
        DataType::Union(fields, _mode) => {
            let mut shape = vec![];
            for (_, field) in fields.iter() {
                shape.push((field.name().as_str(), to_duckdb_logical_type(field.data_type())?));
            }
            Ok(LogicalTypeHandle::union_type(shape.as_slice()))
        }
        DataType::List(child) | DataType::LargeList(child) => {
            Ok(LogicalTypeHandle::list(&to_duckdb_logical_type(child.data_type())?))
        }
        DataType::FixedSizeList(child, array_size) => Ok(LogicalTypeHandle::array(
            &to_duckdb_logical_type(child.data_type())?,
            *array_size,
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

fn fill_duckdb_vector_from_arrow_vector(
    input: &ArrayRef,
    output: &mut Vector,
) -> Result<(), Box<dyn std::error::Error>> {
    match input.data_type() {
        dt if dt.is_primitive() || matches!(dt, DataType::Boolean) => {
            primitive_array_to_vector(input, output.try_as_flat_mut()?)
        }
        DataType::Utf8 => {
            let array = as_string_array(input.as_ref());
            let out: &mut FlatVector = output.try_as_flat_mut()?;
            assert!(array.len() <= out.capacity());

            string_array_to_vector(array, out);
            Ok(())
        }
        DataType::LargeUtf8 => {
            let out: &mut FlatVector = output.try_as_flat_mut()?;
            string_array_to_vector(
                input
                    .as_ref()
                    .as_any()
                    .downcast_ref::<LargeStringArray>()
                    .ok_or_else(|| Box::<dyn std::error::Error>::from("Unable to downcast to LargeStringArray"))?,
                out,
            );
            Ok(())
        }
        DataType::Binary => {
            binary_array_to_vector(as_generic_binary_array(input.as_ref()), output.try_as_flat_mut()?);
            Ok(())
        }
        DataType::List(_) => {
            let array = as_list_array(input.as_ref());
            let out = output.try_as_list_mut()?;
            let value_array = array.values();
            for i in 0..array.len() {
                let offset = array.value_offsets()[i];
                let length = array.value_length(i);
                out.set_entry(i, offset.as_(), length.as_());
            }

            out.reserve_child_capacity(value_array.len());
            out.set_child_len(value_array.len());
            fill_duckdb_vector_from_arrow_vector(value_array, &mut out.child)?;
            Ok(())
        }
        DataType::LargeList(_) => {
            let array = as_large_list_array(input.as_ref());
            let out = &mut output.try_as_list_mut()?;
            let value_array = array.values();

            for i in 0..array.len() {
                let offset = array.value_offsets()[i];
                let length = array.value_length(i);
                out.set_entry(i, offset.as_(), length.as_());
            }

            out.reserve_child_capacity(value_array.len());
            out.set_child_len(value_array.len());
            fill_duckdb_vector_from_arrow_vector(value_array, &mut out.child)?;
            Ok(())
        }
        DataType::FixedSizeList(_, _) => {
            let array = as_fixed_size_list_array(input.as_ref());
            let out = &mut output.try_as_array_mut()?;
            let value_array = array.values();
            fill_duckdb_vector_from_arrow_vector(value_array, &mut out.child)?;
            Ok(())
        }
        DataType::Struct(_) => {
            let struct_array = as_struct_array(input.as_ref());
            let struct_vector = output.try_as_struct_mut()?;
            for i in 0..struct_array.num_columns() {
                fill_duckdb_vector_from_arrow_vector(struct_array.column(i), &mut struct_vector.children[i])?;
            }
            Ok(())
        }
        DataType::Union(fields, mode) => {
            assert_eq!(
                mode,
                &UnionMode::Sparse,
                "duckdb only supports Sparse array for union types"
            );

            let union_array = as_union_array(input.as_ref());
            let union_vector = output.try_as_union_mut().unwrap();

            // set the tag array
            union_vector.tags.copy(union_array.type_ids());

            // copy the members
            for (i, (type_id, _)) in fields.iter().enumerate() {
                let column = union_array.child(type_id);
                fill_duckdb_vector_from_arrow_vector(column, &mut union_vector.members[i])?;
            }
            Ok(())
        }
        df => {
            unimplemented!(
                "column {} is not supported yet, please file an issue https://github.com/wangfenjin/duckdb-rs",
                df
            );
        }
    }
}

/// Converts a `RecordBatch` to a `DataChunk` in the DuckDB format.
///
/// # Arguments
///
/// * `batch` - A reference to the `RecordBatch` to be converted to a `DataChunk`.
/// * `chunk` - A mutable reference to the `DataChunk` to store the converted data.
/// ```
pub fn record_batch_to_duckdb_data_chunk(
    batch: &RecordBatch,
    chunk: &mut DataChunkHandle,
) -> Result<(), Box<dyn std::error::Error>> {
    // Fill the row
    assert_eq!(batch.num_columns(), chunk.num_columns());
    for i in 0..batch.num_columns() {
        let col = batch.column(i);
        fill_duckdb_vector_from_arrow_vector(col, &mut chunk.columns[i])?;
    }
    chunk.set_len(batch.num_rows());
    Ok(())
}

fn primitive_array_to_flat_vector<T: ArrowPrimitiveType>(array: &PrimitiveArray<T>, out_vector: &mut FlatVector) {
    // assert!(array.len() <= out_vector.capacity());
    out_vector.copy::<T::Native>(array.values());
    if let Some(nulls) = array.nulls() {
        for (i, null) in nulls.into_iter().enumerate() {
            if !null {
                out_vector.set_null(i);
            }
        }
    }
}

fn primitive_array_to_flat_vector_cast<T: ArrowPrimitiveType>(
    data_type: DataType,
    array: &dyn Array,
    out_vector: &mut FlatVector,
) {
    let array = arrow::compute::kernels::cast::cast(array, &data_type).unwrap();
    out_vector.copy::<T::Native>(array.as_primitive::<T>().values());

    if let Some(nulls) = array.nulls() {
        for (i, null) in nulls.iter().enumerate() {
            if !null {
                out_vector.set_null(i);
            }
        }
    }
}

fn primitive_array_to_vector(array: &dyn Array, out: &mut FlatVector) -> Result<(), Box<dyn std::error::Error>> {
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
        DataType::Decimal128(width, _) => {
            decimal_array_to_vector(as_primitive_array(array), out, *width);
        }

        // DuckDB Only supports timetamp_tz in microsecond precision
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
    if let Some(nulls) = array.nulls() {
        for (i, null) in nulls.into_iter().enumerate() {
            if !null {
                out.set_null(i);
            }
        }
    }
}

/// Convert Arrow [BooleanArray] to a duckdb vector.
fn boolean_array_to_vector(array: &BooleanArray, out: &mut FlatVector) {
    assert!(array.len() <= out.capacity());

    for i in 0..array.len() {
        out.as_mut_slice()[i] = array.value(i);
    }
}

fn string_array_to_vector<O: OffsetSizeTrait>(array: &GenericStringArray<O>, out: &mut FlatVector) {
    assert!(array.len() <= out.capacity());

    // TODO: zero copy assignment
    for i in 0..array.len() {
        let s = array.value(i);
        out.insert(i, s);
    }
}

fn binary_array_to_vector(array: &BinaryArray, out: &mut FlatVector) {
    assert!(array.len() <= out.capacity());

    for i in 0..array.len() {
        let s = array.value(i);
        out.insert(i, s);
    }
}

/// Force downcast of an [`Array`], such as an [`ArrayRef`], to
/// [`FixedSizeListArray`], panic'ing on failure.
fn as_fixed_size_list_array(arr: &dyn Array) -> &FixedSizeListArray {
    arr.as_any().downcast_ref::<FixedSizeListArray>().unwrap()
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

#[cfg(test)]
mod test {
    use std::{error::Error, sync::Arc};

    use arrow::{
        array::{
            Array, ArrayRef, AsArray, BinaryArray, Date32Array, Date64Array, Decimal128Array, Decimal256Array,
            FixedSizeListArray, GenericByteArray, GenericListArray, Int32Array, LargeStringArray, ListArray,
            OffsetSizeTrait, PrimitiveArray, StringArray, Time32SecondArray, Time64MicrosecondArray,
            TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
        },
        buffer::{OffsetBuffer, ScalarBuffer},
        datatypes::{i256, ArrowPrimitiveType, ByteArrayType, DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use arrow_convert::{ArrowDeserialize, ArrowField, ArrowSerialize};

    use super::{arrow_recordbatch_to_query_params, ArrowVTab};
    use crate::{Connection, Result};

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

        #[derive(ArrowField, ArrowSerialize)]
        struct T1Row {
            v: String,
            i: i32,
        }

        {
            let rows = vec![
                T1Row {
                    v: "foo".to_string(),
                    i: 1,
                },
                T1Row {
                    v: "bar".to_string(),
                    i: 2,
                },
            ];
            let mut app = db.appender("t1")?;
            app.append_rows_arrow(&rows, false)?;
        }

        let mut stmt = db.prepare("SELECT s FROM t1")?;
        let rbs: Vec<RecordBatch> = stmt.query_arrow([])?.collect();
        assert_eq!(rbs.iter().map(|op| op.num_rows()).sum::<usize>(), 2);
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
        let schema = Schema::new(vec![Field::new("a", arry.data_type().clone(), false)]);

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
    fn test_append_union() -> Result<(), Box<dyn Error>> {
        let db = Connection::open_in_memory()?;

        db.execute_batch("CREATE TABLE tbl1 (u UNION(num INT, str VARCHAR));")?;

        #[derive(PartialEq, Eq, Debug, ArrowField, ArrowSerialize, ArrowDeserialize)]
        #[arrow_field(type = "sparse")]
        enum TestUnion {
            Num(i32),
            Str(String),
        }

        {
            let rows = vec![TestUnion::Num(1), TestUnion::Str("foo".into()), TestUnion::Num(34)];

            let mut app = db.appender("tbl1")?;
            app.append_rows_arrow(&rows, false)?;
        }
        let mut stmt = db.prepare("SELECT u FROM tbl1")?;
        let records: Vec<TestUnion> = stmt.query_arrow_deserialized([])?;
        assert_eq!(
            records,
            vec![TestUnion::Num(1), TestUnion::Str("foo".into()), TestUnion::Num(34)]
        );
        Ok(())
    }

    #[test]
    fn test_nested_struct_in_fixed_list() -> Result<(), Box<dyn Error>> {
        let db = Connection::open_in_memory()?;

        db.execute_batch(
            r#"
        CREATE TYPE POINT AS STRUCT(x INT, y INT);
        CREATE TABLE tbl1 (line POINT[2]);
        INSERT INTO tbl1 VALUES (ARRAY[{x: 1, y: 2}, {x: 3, y: 4}]);
        "#,
        )?;

        let mut stmt = db.prepare("SELECT line FROM tbl1")?;
        let rbs: Vec<RecordBatch> = stmt.query_arrow([])?.collect();
        assert_eq!(rbs.iter().map(|op| op.num_rows()).sum::<usize>(), 1);
        Ok(())
    }

    #[test]
    fn test_list_column() -> Result<(), Box<dyn Error>> {
        let db = Connection::open_in_memory()?;

        db.execute_batch("CREATE TABLE tbl1 (scalar Int[])")?;

        {
            let mut appender = db.appender("tbl1")?;
            appender
                .append_rows_arrow(&vec![vec![4, 5], vec![6, 7], vec![8, 9, 10]], false)
                .unwrap();
        }

        let mut stmt = db.prepare("SELECT * FROM tbl1")?;
        let rbs: Vec<RecordBatch> = stmt.query_arrow([])?.collect();
        assert_eq!(rbs.iter().map(|op| op.num_rows()).sum::<usize>(), 3);
        Ok(())
    }

    #[test]
    fn test_array_column() -> Result<(), Box<dyn Error>> {
        let db = Connection::open_in_memory()?;

        db.execute_batch("CREATE TABLE tbl1 (scalar CHAR[2])")?;

        {
            let mut appender = db.appender("tbl1")?;
            appender
                .append_rows_arrow(&vec![[4, 5], [6, 7], [8, 9]], false)
                .unwrap();
        }

        let mut stmt = db.prepare("SELECT * FROM tbl1")?;
        let rbs: Vec<RecordBatch> = stmt.query_arrow([])?.collect();
        assert_eq!(rbs.iter().map(|op| op.num_rows()).sum::<usize>(), 3);
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

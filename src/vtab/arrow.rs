use super::{
    vector::{FlatVector, ListVector, Vector},
    BindInfo, DataChunk, Free, FunctionInfo, InitInfo, LogicalType, LogicalTypeId, VTab,
};

use crate::vtab::vector::Inserter;
use arrow::array::{
    as_boolean_array, as_large_list_array, as_list_array, as_primitive_array, as_string_array, Array, ArrayData,
    ArrowPrimitiveType, BooleanArray, Decimal128Array, FixedSizeListArray, GenericListArray, OffsetSizeTrait,
    PrimitiveArray, StringArray, StructArray,
};

use arrow::{
    datatypes::*,
    ffi::{ArrowArray, FFI_ArrowArray, FFI_ArrowSchema},
    record_batch::RecordBatch,
};

use num::cast::AsPrimitive;

#[repr(C)]
struct ArrowBindData {
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

#[repr(C)]
struct ArrowInitData {
    done: bool,
}

impl Free for ArrowInitData {}

struct ArrowVTab;

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
    let array = ArrowArray::new(array, schema);
    let array_data = ArrayData::try_from(array).expect("ok");
    let struct_array = StructArray::from(array_data);
    RecordBatch::from(&struct_array)
}

impl VTab for ArrowVTab {
    type BindData = ArrowBindData;
    type InitData = ArrowInitData;

    fn bind(bind: &BindInfo, data: *mut ArrowBindData) -> Result<(), Box<dyn std::error::Error>> {
        let param_count = bind.get_parameter_count();
        assert!(param_count == 2);
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

    fn func(func: &FunctionInfo, output: &mut DataChunk) -> Result<(), Box<dyn std::error::Error>> {
        let init_info = func.get_init_data::<ArrowInitData>();
        let bind_info = func.get_bind_data::<ArrowBindData>();
        unsafe {
            if (*init_info).done {
                output.set_len(0);
            } else {
                let rb = Box::from_raw((*bind_info).rb);
                record_batch_to_duckdb_data_chunk(&rb, output)?;
                (*bind_info).rb = Box::into_raw(rb);
                (*init_info).done = true;
            }
        }
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalType>> {
        Some(vec![
            LogicalType::new(LogicalTypeId::UBigint), // file path
            LogicalType::new(LogicalTypeId::UBigint), // sheet name
        ])
    }
}

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
        DataType::Timestamp(_, _) => Timestamp,
        DataType::Date32 => Time,
        DataType::Date64 => Time,
        DataType::Time32(_) => Time,
        DataType::Time64(_) => Time,
        DataType::Duration(_) => Interval,
        DataType::Interval(_) => Interval,
        DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_) => Blob,
        DataType::Utf8 | DataType::LargeUtf8 => Varchar,
        DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _) => List,
        DataType::Struct(_) => Struct,
        DataType::Union(_, _, _) => Union,
        DataType::Dictionary(_, _) => todo!(),
        // duckdb/src/main/capi/helper-c.cpp does not support decimal
        // DataType::Decimal128(_, _) => Decimal,
        // DataType::Decimal256(_, _) => Decimal,
        DataType::Decimal128(_, _) => Double,
        DataType::Decimal256(_, _) => Double,
        DataType::Map(_, _) => Map,
        _ => {
            return Err(format!("Unsupported data type: {:?}", data_type).into());
        }
    };
    Ok(type_id)
}

pub fn to_duckdb_logical_type(data_type: &DataType) -> Result<LogicalType, Box<dyn std::error::Error>> {
    if data_type.is_primitive()
        || matches!(
            data_type,
            DataType::Boolean | DataType::Utf8 | DataType::LargeUtf8 | DataType::Binary | DataType::LargeBinary
        )
    {
        Ok(LogicalType::new(to_duckdb_type_id(data_type)?))
    } else if let DataType::Dictionary(_, value_type) = data_type {
        to_duckdb_logical_type(value_type)
    // } else if let DataType::Struct(fields) = data_type {
    //     let mut shape = vec![];
    //     for field in fields.iter() {
    //         shape.push((
    //             field.name().as_str(),
    //             to_duckdb_logical_type(field.data_type())?,
    //         ));
    //     }
    //     Ok(LogicalType::struct_type(shape.as_slice()))
    } else if let DataType::List(child) = data_type {
        Ok(LogicalType::list(&to_duckdb_logical_type(child.data_type())?))
    } else if let DataType::LargeList(child) = data_type {
        Ok(LogicalType::list(&to_duckdb_logical_type(child.data_type())?))
    } else if let DataType::FixedSizeList(child, _) = data_type {
        Ok(LogicalType::list(&to_duckdb_logical_type(child.data_type())?))
    } else {
        println!("Unsupported data type: {data_type}, please file an issue https://github.com/wangfenjin/duckdb-rs");
        todo!()
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
    chunk: &mut DataChunk,
) -> Result<(), Box<dyn std::error::Error>> {
    // Fill the row
    assert_eq!(batch.num_columns(), chunk.num_columns());
    for i in 0..batch.num_columns() {
        let col = batch.column(i);
        match col.data_type() {
            dt if dt.is_primitive() || matches!(dt, DataType::Boolean) => {
                primitive_array_to_vector(col, &mut chunk.flat_vector(i));
            }
            DataType::Utf8 => {
                string_array_to_vector(as_string_array(col.as_ref()), &mut chunk.flat_vector(i));
            }
            DataType::List(_) => {
                list_array_to_vector(as_list_array(col.as_ref()), &mut chunk.list_vector(i));
            }
            DataType::LargeList(_) => {
                list_array_to_vector(as_large_list_array(col.as_ref()), &mut chunk.list_vector(i));
            }
            DataType::FixedSizeList(_, _) => {
                fixed_size_list_array_to_vector(as_fixed_size_list_array(col.as_ref()), &mut chunk.list_vector(i));
            }
            // DataType::Struct(_) => {
            //     let struct_array = as_struct_array(col.as_ref());
            //     let mut struct_vector = chunk.struct_vector(i);
            //     struct_array_to_vector(struct_array, &mut struct_vector);
            // }
            _ => {
                println!(
                    "column {} is not supported yet, please file an issue https://github.com/wangfenjin/duckdb-rs",
                    batch.schema().field(i)
                );
                todo!()
            }
        }
    }
    chunk.set_len(batch.num_rows());
    Ok(())
}

fn primitive_array_to_flat_vector<T: ArrowPrimitiveType>(array: &PrimitiveArray<T>, out_vector: &mut FlatVector) {
    // assert!(array.len() <= out_vector.capacity());
    out_vector.copy::<T::Native>(array.values());
}

fn primitive_array_to_vector(array: &dyn Array, out: &mut dyn Vector) {
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
        DataType::Decimal128(_, _) => {
            decimal_array_to_vector(
                array
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .expect("Unable to downcast to BooleanArray"),
                out.as_mut_any().downcast_mut().unwrap(),
            );
        }
        // DataType::Decimal256(_, _) => {
        //     primitive_array_to_flat_vector::<Decimal256Type>(
        //         as_primitive_array(array),
        //         out.as_mut_any().downcast_mut().unwrap(),
        //     );
        // }
        _ => {
            todo!()
        }
    }
}

/// Convert Arrow [BooleanArray] to a duckdb vector.
fn decimal_array_to_vector(array: &Decimal128Array, out: &mut FlatVector) {
    assert!(array.len() <= out.capacity());

    for i in 0..array.len() {
        out.as_mut_slice()[i] = array.value_as_string(i).parse::<f64>().unwrap();
    }
}

/// Convert Arrow [BooleanArray] to a duckdb vector.
fn boolean_array_to_vector(array: &BooleanArray, out: &mut FlatVector) {
    assert!(array.len() <= out.capacity());

    for i in 0..array.len() {
        out.as_mut_slice()[i] = array.value(i);
    }
}

fn string_array_to_vector(array: &StringArray, out: &mut FlatVector) {
    assert!(array.len() <= out.capacity());

    // TODO: zero copy assignment
    for i in 0..array.len() {
        let s = array.value(i);
        out.insert(i, s);
    }
}

fn list_array_to_vector<O: OffsetSizeTrait + AsPrimitive<usize>>(array: &GenericListArray<O>, out: &mut ListVector) {
    let value_array = array.values();
    let mut child = out.child(value_array.len());
    match value_array.data_type() {
        dt if dt.is_primitive() => {
            primitive_array_to_vector(value_array.as_ref(), &mut child);
            for i in 0..array.len() {
                let offset = array.value_offsets()[i];
                let length = array.value_length(i);
                out.set_entry(i, offset.as_(), length.as_());
            }
        }
        _ => {
            println!("Nested list is not supported yet.");
            todo!()
        }
    }
}

fn fixed_size_list_array_to_vector(array: &FixedSizeListArray, out: &mut ListVector) {
    let value_array = array.values();
    let mut child = out.child(value_array.len());
    match value_array.data_type() {
        dt if dt.is_primitive() => {
            primitive_array_to_vector(value_array.as_ref(), &mut child);
            for i in 0..array.len() {
                let offset = array.value_offset(i);
                let length = array.value_length();
                out.set_entry(i, offset as usize, length as usize);
            }
            out.set_len(value_array.len());
        }
        _ => {
            println!("Nested list is not supported yet.");
            todo!()
        }
    }
}

/// Force downcast of an [`Array`], such as an [`ArrayRef`], to
/// [`FixedSizeListArray`], panic'ing on failure.
fn as_fixed_size_list_array(arr: &dyn Array) -> &FixedSizeListArray {
    arr.as_any().downcast_ref::<FixedSizeListArray>().unwrap()
}

// fn struct_array_to_vector(array: &StructArray, out: &mut StructVector) {
//     for i in 0..array.num_columns() {
//         let column = array.column(i);
//         match column.data_type() {
//             dt if dt.is_primitive() || matches!(dt, DataType::Boolean) => {
//                 primitive_array_to_vector(column, &mut out.child(i));
//             }
//             DataType::Utf8 => {
//                 string_array_to_vector(as_string_array(column.as_ref()), &mut out.child(i));
//             }
//             DataType::List(_) => {
//                 list_array_to_vector(
//                     as_list_array(column.as_ref()),
//                     &mut out.list_vector_child(i),
//                 );
//             }
//             DataType::LargeList(_) => {
//                 list_array_to_vector(
//                     as_large_list_array(column.as_ref()),
//                     &mut out.list_vector_child(i),
//                 );
//             }
//             DataType::FixedSizeList(_, _) => {
//                 fixed_size_list_array_to_vector(
//                     as_fixed_size_list_array(column.as_ref()),
//                     &mut out.list_vector_child(i),
//                 );
//             }
//             DataType::Struct(_) => {
//                 let struct_array = as_struct_array(column.as_ref());
//                 let mut struct_vector = out.struct_vector_child(i);
//                 struct_array_to_vector(struct_array, &mut struct_vector);
//             }
//             _ => {
//                 println!("Unsupported data type: {}, please file an issue https://github.com/wangfenjin/duckdb-rs", column.data_type());
//                 todo!()
//             }
//         }
//     }
// }

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
    use super::{arrow_recordbatch_to_query_params, ArrowVTab};
    use crate::{Connection, Result};
    use arrow::{
        array::{Float64Array, Int32Array},
        datatypes::{DataType, Field, Schema},
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
        let column = rb.column(0).as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(column.len(), 1);
        assert_eq!(column.value(0), 300.0);
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
}

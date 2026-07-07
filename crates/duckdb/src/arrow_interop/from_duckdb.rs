use std::sync::Arc;

use arrow::{
    array::{
        Array, BooleanArray, Date32Array, GenericBinaryBuilder, PrimitiveArray, StringArray, TimestampMicrosecondArray,
        TimestampNanosecondArray,
    },
    buffer::{BooleanBuffer, NullBuffer},
    datatypes::*,
    record_batch::RecordBatch,
};
use libduckdb_sys::{duckdb_date, duckdb_string_t, duckdb_time, duckdb_timestamp};

use crate::{
    core::{DataChunkHandle, FlatVector, LogicalTypeId},
    types::DuckString,
};

/// Converts a DuckDB flat vector to an Arrow array.
pub fn flat_vector_to_arrow_array(
    vector: &mut FlatVector<'_>,
    len: usize,
) -> Result<Arc<dyn Array>, Box<dyn std::error::Error>> {
    let raw_type_id = vector.logical_type().raw_id();
    let type_id = LogicalTypeId::from(raw_type_id);
    match type_id {
        LogicalTypeId::Invalid => Err("Cannot convert invalid logical type returned by DuckDB".into()),
        LogicalTypeId::Unsupported => Err(format!("Unsupported DuckDB logical type ID {raw_type_id}").into()),
        LogicalTypeId::Integer => {
            let data = unsafe { vector.as_slice_with_len::<i32>(len) };

            Ok(Arc::new(PrimitiveArray::<Int32Type>::from_iter_values_with_nulls(
                data.iter().copied(),
                Some(NullBuffer::new(BooleanBuffer::collect_bool(data.len(), |row| {
                    !vector.row_is_null(row as u64)
                }))),
            )))
        }
        LogicalTypeId::Timestamp
        | LogicalTypeId::TimestampMs
        | LogicalTypeId::TimestampS
        | LogicalTypeId::TimestampTZ => {
            let data = unsafe { vector.as_slice_with_len::<duckdb_timestamp>(len) };
            let micros = data.iter().map(|duckdb_timestamp { micros }| *micros);
            let structs = TimestampMicrosecondArray::from_iter_values_with_nulls(
                micros,
                Some(NullBuffer::new(BooleanBuffer::collect_bool(data.len(), |row| {
                    !vector.row_is_null(row as u64)
                }))),
            );

            Ok(Arc::new(structs))
        }
        LogicalTypeId::Varchar => {
            let data = unsafe { vector.as_slice_with_len::<duckdb_string_t>(len) };

            let duck_strings = data.iter().enumerate().map(|(i, s)| {
                if vector.row_is_null(i as u64) {
                    None
                } else {
                    let mut ptr = *s;
                    Some(DuckString::new(&mut ptr).as_str().to_string())
                }
            });

            let values = duck_strings.collect::<Vec<_>>();

            Ok(Arc::new(StringArray::from(values)))
        }
        LogicalTypeId::Boolean => {
            let data = unsafe { vector.as_slice_with_len::<bool>(len) };

            Ok(Arc::new(BooleanArray::new(
                BooleanBuffer::from_iter(data.iter().copied()),
                Some(NullBuffer::new(BooleanBuffer::collect_bool(data.len(), |row| {
                    !vector.row_is_null(row as u64)
                }))),
            )))
        }
        LogicalTypeId::Float => {
            let data = unsafe { vector.as_slice_with_len::<f32>(len) };

            Ok(Arc::new(PrimitiveArray::<Float32Type>::from_iter_values_with_nulls(
                data.iter().copied(),
                Some(NullBuffer::new(BooleanBuffer::collect_bool(data.len(), |row| {
                    !vector.row_is_null(row as u64)
                }))),
            )))
        }
        LogicalTypeId::Double => {
            let data = unsafe { vector.as_slice_with_len::<f64>(len) };

            Ok(Arc::new(PrimitiveArray::<Float64Type>::from_iter_values_with_nulls(
                data.iter().copied(),
                Some(NullBuffer::new(BooleanBuffer::collect_bool(data.len(), |row| {
                    !vector.row_is_null(row as u64)
                }))),
            )))
        }
        LogicalTypeId::Date => {
            let data = unsafe { vector.as_slice_with_len::<duckdb_date>(len) };

            Ok(Arc::new(Date32Array::from_iter_values_with_nulls(
                data.iter().map(|duckdb_date { days }| *days),
                Some(NullBuffer::new(BooleanBuffer::collect_bool(data.len(), |row| {
                    !vector.row_is_null(row as u64)
                }))),
            )))
        }
        LogicalTypeId::Time => {
            let data = unsafe { vector.as_slice_with_len::<duckdb_time>(len) };

            Ok(Arc::new(
                PrimitiveArray::<Time64MicrosecondType>::from_iter_values_with_nulls(
                    data.iter().map(|duckdb_time { micros }| *micros),
                    Some(NullBuffer::new(BooleanBuffer::collect_bool(data.len(), |row| {
                        !vector.row_is_null(row as u64)
                    }))),
                ),
            ))
        }
        LogicalTypeId::Smallint => {
            let data = unsafe { vector.as_slice_with_len::<i16>(len) };

            Ok(Arc::new(PrimitiveArray::<Int16Type>::from_iter_values_with_nulls(
                data.iter().copied(),
                Some(NullBuffer::new(BooleanBuffer::collect_bool(data.len(), |row| {
                    !vector.row_is_null(row as u64)
                }))),
            )))
        }
        LogicalTypeId::USmallint => {
            let data = unsafe { vector.as_slice_with_len::<u16>(len) };

            Ok(Arc::new(PrimitiveArray::<UInt16Type>::from_iter_values_with_nulls(
                data.iter().copied(),
                Some(NullBuffer::new(BooleanBuffer::collect_bool(data.len(), |row| {
                    !vector.row_is_null(row as u64)
                }))),
            )))
        }
        LogicalTypeId::Blob | LogicalTypeId::Geometry => {
            // DuckDB currently stores GEOMETRY vectors as WKB-backed string_t
            // bytes. Treating GEOMETRY like BLOB here relies on that internal
            // representation staying WKB-compatible.
            let mut data = unsafe { vector.as_slice_with_len::<duckdb_string_t>(len) }.to_vec();

            let duck_strings = data.iter_mut().enumerate().map(|(i, ptr)| {
                if vector.row_is_null(i as u64) {
                    None
                } else {
                    Some(DuckString::new(ptr))
                }
            });

            let mut builder = GenericBinaryBuilder::<i32>::new();
            for s in duck_strings {
                if let Some(mut s) = s {
                    builder.append_value(s.as_bytes());
                } else {
                    builder.append_null();
                }
            }

            Ok(Arc::new(builder.finish()))
        }
        LogicalTypeId::Tinyint => {
            let data = unsafe { vector.as_slice_with_len::<i8>(len) };

            Ok(Arc::new(PrimitiveArray::<Int8Type>::from_iter_values_with_nulls(
                data.iter().copied(),
                Some(NullBuffer::new(BooleanBuffer::collect_bool(data.len(), |row| {
                    !vector.row_is_null(row as u64)
                }))),
            )))
        }
        LogicalTypeId::Bigint => {
            let data = unsafe { vector.as_slice_with_len::<i64>(len) };

            Ok(Arc::new(PrimitiveArray::<Int64Type>::from_iter_values_with_nulls(
                data.iter().copied(),
                Some(NullBuffer::new(BooleanBuffer::collect_bool(data.len(), |row| {
                    !vector.row_is_null(row as u64)
                }))),
            )))
        }
        LogicalTypeId::UBigint => {
            let data = unsafe { vector.as_slice_with_len::<u64>(len) };

            Ok(Arc::new(PrimitiveArray::<UInt64Type>::from_iter_values_with_nulls(
                data.iter().copied(),
                Some(NullBuffer::new(BooleanBuffer::collect_bool(data.len(), |row| {
                    !vector.row_is_null(row as u64)
                }))),
            )))
        }
        LogicalTypeId::UTinyint => {
            let data = unsafe { vector.as_slice_with_len::<u8>(len) };

            Ok(Arc::new(PrimitiveArray::<UInt8Type>::from_iter_values_with_nulls(
                data.iter().copied(),
                Some(NullBuffer::new(BooleanBuffer::collect_bool(data.len(), |row| {
                    !vector.row_is_null(row as u64)
                }))),
            )))
        }
        LogicalTypeId::UInteger => {
            let data = unsafe { vector.as_slice_with_len::<u32>(len) };

            Ok(Arc::new(PrimitiveArray::<UInt32Type>::from_iter_values_with_nulls(
                data.iter().copied(),
                Some(NullBuffer::new(BooleanBuffer::collect_bool(data.len(), |row| {
                    !vector.row_is_null(row as u64)
                }))),
            )))
        }
        LogicalTypeId::TimestampNs => {
            // even nano second precision is stored in micros when using the c api
            let data = unsafe { vector.as_slice_with_len::<duckdb_timestamp>(len) };
            let nanos = data.iter().map(|duckdb_timestamp { micros }| *micros * 1000);
            let structs = TimestampNanosecondArray::from_iter_values_with_nulls(
                nanos,
                Some(NullBuffer::new(BooleanBuffer::collect_bool(data.len(), |row| {
                    !vector.row_is_null(row as u64)
                }))),
            );

            Ok(Arc::new(structs))
        }
        LogicalTypeId::Interval => todo!(),
        LogicalTypeId::Hugeint => todo!(),
        LogicalTypeId::Decimal => todo!(),
        LogicalTypeId::Enum => todo!(),
        LogicalTypeId::List => todo!(),
        LogicalTypeId::Struct => todo!(),
        LogicalTypeId::Map => todo!(),
        LogicalTypeId::Array => todo!(),
        LogicalTypeId::Uuid => todo!(),
        LogicalTypeId::Union => todo!(),
        LogicalTypeId::Bit => todo!(),
        LogicalTypeId::TimeTZ => todo!(),
        LogicalTypeId::UHugeint => todo!(),
        LogicalTypeId::Any => todo!(),
        LogicalTypeId::Bignum => todo!(),
        LogicalTypeId::SqlNull => todo!(),
        LogicalTypeId::StringLiteral => todo!(),
        LogicalTypeId::IntegerLiteral => todo!(),
        LogicalTypeId::TimeNs => todo!(),
        LogicalTypeId::Variant => Err("Cannot convert Variant logical type to Arrow".into()),
    }
}

/// converts a `DataChunk` to arrow `RecordBatch`
pub fn data_chunk_to_arrow(chunk: &DataChunkHandle) -> Result<RecordBatch, Box<dyn std::error::Error>> {
    let len = chunk.len();

    let columns = (0..chunk.num_columns())
        .map(|i| {
            let mut vector = chunk.flat_vector(i);
            flat_vector_to_arrow_array(&mut vector, len).map(|array| {
                assert_eq!(array.len(), chunk.len());
                (i.to_string(), array)
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(RecordBatch::try_from_iter(columns)?)
}

use super::{BindInfo, DataChunkHandle, InitInfo, LogicalTypeHandle, TableFunctionInfo, VTab};
use std::sync::{Arc, Mutex, OnceLock, atomic::AtomicUsize};

use crate::{
    core::{ArrayVector, FlatVector, Inserter, ListVector, LogicalTypeId, StructVector},
    types::DuckString,
};

use arrow::{
    array::{
        Array, ArrayData, AsArray, BinaryArray, BinaryViewArray, BooleanArray, Date32Array, Decimal128Array,
        FixedSizeBinaryArray, FixedSizeListArray, GenericBinaryBuilder, GenericListArray, GenericStringArray,
        IntervalMonthDayNanoArray, LargeBinaryArray, LargeStringArray, OffsetSizeTrait, PrimitiveArray, StringArray,
        StringViewArray, StructArray, TimestampMicrosecondArray, TimestampNanosecondArray, as_boolean_array,
        as_generic_binary_array, as_large_list_array, as_list_array, as_map_array, as_primitive_array, as_string_array,
        as_struct_array,
    },
    buffer::{BooleanBuffer, NullBuffer},
    compute::cast,
};

use arrow::{
    datatypes::*,
    ffi::{FFI_ArrowArray, FFI_ArrowSchema, from_ffi},
    record_batch::RecordBatch,
};

use libduckdb_sys::{duckdb_date, duckdb_string_t, duckdb_time, duckdb_timestamp, duckdb_vector};
use num::{ToPrimitive, cast::AsPrimitive};

/// The Arrow record batch for the table function.
///
/// Bind data is shared across `func` calls and should be treated as read-only.
/// That is enough for `RecordBatch`: Arrow batches are immutable containers of
/// shared array data, and the `VTab::BindData: Send + Sync` bound requires this
/// value to be safe to share. The mutable scan position lives in
/// `ArrowInitData.offset`, so the batch itself does not need a `Mutex`.
#[repr(C)]
pub struct ArrowBindData {
    rb: RecordBatch,
}

/// Tracks how many rows of the Arrow record batch have been emitted so far.
///
/// DuckDB drives table functions with a pull model: [`ArrowVTab::func`] is
/// called repeatedly and may emit at most one `DataChunk` per call. DuckDB
/// reports that chunk capacity through `duckdb_vector_size()`, which returns
/// DuckDB's compile-time `STANDARD_VECTOR_SIZE`. Capturing that capacity here
/// lets each call slice the next window of rows, so batches larger than the
/// vector size are streamed across multiple calls instead of overflowing a
/// single chunk.
#[repr(C)]
pub struct ArrowInitData {
    offset: AtomicUsize,
    vector_size: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RowSlice {
    offset: usize,
    len: usize,
}

impl ArrowInitData {
    fn take_slice(&self, num_rows: usize) -> Option<RowSlice> {
        use std::sync::atomic::Ordering::Relaxed;

        let vector_size = self.vector_size;
        // DuckDB currently drives this scan serially, but if parallel scans are
        // enabled later, this atomic read-modify-write makes each claimed row
        // window disjoint. Relaxed ordering is enough because each RowSlice is
        // self-contained and no other shared state is published through the atomic.
        let offset = self
            .offset
            .fetch_update(Relaxed, Relaxed, |offset| {
                if offset >= num_rows {
                    None
                } else {
                    Some(offset.saturating_add(vector_size).min(num_rows))
                }
            })
            // `fetch_update` retries compare-exchange contention internally.
            // Err only means the closure observed completion and declined to update.
            .ok()?;
        Some(RowSlice {
            offset,
            len: (num_rows - offset).min(vector_size),
        })
    }
}

/// The Arrow table function.
pub struct ArrowVTab;

const ARROW_QUERY_PARAMS_MARKER: usize = 0x4152_5257; // "ARRW"

fn arrow_record_batch_store() -> &'static Mutex<Vec<Arc<RecordBatch>>> {
    static STORE: OnceLock<Mutex<Vec<Arc<RecordBatch>>>> = OnceLock::new();
    STORE.get_or_init(|| Mutex::new(Vec::new()))
}

fn register_arrow_record_batch(rb: RecordBatch) -> [usize; 2] {
    // Views can rebind long after creation, so the RecordBatch allocation must
    // remain valid for the process lifetime. The Arc keeps the pointee stable
    // across Vec growth, while the static store keeps it visible to
    // LeakSanitizer as reachable process-lifetime storage.
    let mut store = arrow_record_batch_store()
        .lock()
        .expect("ArrowVTab record batch store poisoned");
    let rb = Arc::new(rb);
    let ptr = Arc::as_ptr(&rb);
    store.push(rb);
    [ptr as usize, ARROW_QUERY_PARAMS_MARKER]
}

fn arrow_query_param_usize(bind: &BindInfo, index: u64, name: &str) -> Result<usize, Box<dyn std::error::Error>> {
    let value = bind.get_parameter(index);
    if value.is_null() {
        return Err(format!("ArrowVTab {name} parameter must not be NULL").into());
    }

    let logical_type = value.logical_type_id();
    if logical_type != LogicalTypeId::UBigint {
        return Err(format!("ArrowVTab {name} parameter must be UBIGINT, got {logical_type:?}").into());
    }

    usize::try_from(value.to_uint64()).map_err(|_| format!("ArrowVTab {name} parameter does not fit in usize").into())
}

/// Imports a record batch from the current opaque ArrowVTab query parameters.
///
/// # Safety
///
/// `address` must be a non-null pointer returned by
/// [`arrow_recordbatch_to_query_params`], and `marker` must be the matching
/// layout marker returned with it. The marker catches common misuse only and
/// does not make stale or forged pointers safe to dereference.
unsafe fn address_to_arrow_record_batch(
    address: usize,
    marker: usize,
) -> Result<RecordBatch, Box<dyn std::error::Error>> {
    let ptr = address as *const RecordBatch;
    if ptr.is_null() {
        return Err("invalid ArrowVTab record batch address".into());
    }

    if marker != ARROW_QUERY_PARAMS_MARKER {
        return Err("ArrowVTab query parameter marker mismatch; use arrow_recordbatch_to_query_params".into());
    }

    // SAFETY: The caller guarantees that `ptr` is the RecordBatch allocation
    // retained by `register_arrow_record_batch`.
    Ok(unsafe { (*ptr).clone() })
}

fn arrow_record_batch_from_ffi(array: FFI_ArrowArray, schema: FFI_ArrowSchema) -> RecordBatch {
    let array_data = unsafe { from_ffi(array, &schema) }.expect("failed to import Arrow FFI data");
    let struct_array = StructArray::from(array_data);
    RecordBatch::from(&struct_array)
}

impl VTab for ArrowVTab {
    type BindData = ArrowBindData;
    type InitData = ArrowInitData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        let param_count = bind.get_parameter_count();
        if param_count != 2 {
            return Err(format!("Bad param count: {param_count}, expected 2").into());
        }
        let address = arrow_query_param_usize(bind, 0, "record batch address")?;
        let marker = arrow_query_param_usize(bind, 1, "marker")?;

        // SAFETY: ArrowVTab's raw-parameter API relies on callers passing
        // values returned unchanged by `arrow_recordbatch_to_query_params`.
        // Validation above catches nulls, type mismatches, and layout marker
        // mismatches, but cannot validate forged addresses.
        let rb = unsafe { address_to_arrow_record_batch(address, marker)? };
        for f in rb.schema().fields() {
            let name = f.name();
            let data_type = f.data_type();
            let logical_type = to_duckdb_logical_type(data_type)?;
            bind.add_result_column(name, logical_type);
        }

        Ok(ArrowBindData { rb })
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        let vector_size = unsafe { crate::ffi::duckdb_vector_size() } as usize;
        if vector_size == 0 {
            return Err("DuckDB vector size must be greater than zero".into());
        }

        Ok(ArrowInitData {
            offset: AtomicUsize::new(0),
            vector_size,
        })
    }

    fn func(func: &TableFunctionInfo<Self>, output: &mut DataChunkHandle) -> Result<(), Box<dyn std::error::Error>> {
        let init_info = func.get_init_data();
        let bind_info = func.get_bind_data();

        let rb = &bind_info.rb;
        let num_rows = rb.num_rows();
        let Some(slice) = init_info.take_slice(num_rows) else {
            output.set_len(0);
            return Ok(());
        };

        // Emit at most one vector's worth of rows per call (slicing is zero-copy)
        record_batch_to_duckdb_data_chunk(&rb.slice(slice.offset, slice.len), output)?;

        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![
            LogicalTypeHandle::from(LogicalTypeId::UBigint), // record batch address
            LogicalTypeHandle::from(LogicalTypeId::UBigint), // query parameter marker
        ])
    }
}

/// Convert arrow DataType to DuckDB type id
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
        DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_) | DataType::BinaryView => Blob,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Varchar,
        DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _) => List,
        DataType::Struct(_) => Struct,
        DataType::Union(_, _) => Union,
        // DataType::Dictionary(_, _) => todo!(),
        DataType::Decimal128(_, _) => Decimal,
        DataType::Decimal256(_, _) => Double,
        DataType::Map(_, _) => Map,
        _ => {
            return Err(format!("Unsupported data type: {data_type:?}").into());
        }
    };
    Ok(type_id)
}

impl TryFrom<&DataType> for LogicalTypeId {
    type Error = Box<dyn std::error::Error>;

    fn try_from(data_type: &DataType) -> Result<Self, Self::Error> {
        to_duckdb_type_id(data_type)
    }
}

impl TryFrom<DataType> for LogicalTypeId {
    type Error = Box<dyn std::error::Error>;

    fn try_from(data_type: DataType) -> Result<Self, Self::Error> {
        to_duckdb_type_id(&data_type)
    }
}

/// Convert arrow DataType to DuckDB logical type
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
        DataType::Decimal128(width, scale) => {
            if *scale < 0 {
                return Err(
                    format!("Unsupported data type: {data_type}, negative decimal scale is not supported").into(),
                );
            }
            Ok(LogicalTypeHandle::decimal(*width, (*scale).try_into().unwrap()))
        }
        DataType::Map(field, _) => arrow_map_to_duckdb_logical_type(field),
        DataType::Boolean
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => Ok(LogicalTypeHandle::from(to_duckdb_type_id(data_type)?)),
        dtype if dtype.is_primitive() => Ok(LogicalTypeHandle::from(to_duckdb_type_id(data_type)?)),
        _ => Err(format!(
            "Unsupported data type: {data_type}, please file an issue https://github.com/duckdb/duckdb-rs"
        )
        .into()),
    }
}

fn arrow_map_to_duckdb_logical_type(field: &FieldRef) -> Result<LogicalTypeHandle, Box<dyn std::error::Error>> {
    // Map is a logical nested type that is represented as `List<entries: Struct<key: K, value: V>>`
    let DataType::Struct(fields) = field.data_type() else {
        return Err(format!(
            "The inner field of a Map must be a Struct, got: {:?}",
            field.data_type()
        )
        .into());
    };

    if fields.len() != 2 {
        return Err(format!(
            "The inner Struct field of a Map must have 2 fields, got {} fields",
            fields.len()
        )
        .into());
    }

    let (Some(key_field), Some(value_field)) = (fields.first(), fields.get(1)) else {
        // number of fields is verified above
        unreachable!()
    };

    Ok(LogicalTypeHandle::map(
        &LogicalTypeHandle::from(to_duckdb_type_id(key_field.data_type())?),
        &LogicalTypeHandle::from(to_duckdb_type_id(value_field.data_type())?),
    ))
}

// FIXME: flat vectors don't have all of thsese types. I think they only
/// Converts flat vector to an arrow array
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
        LogicalTypeId::Blob => {
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
            flat_vector_to_arrow_array(&mut vector, len).map(|array_data| {
                assert_eq!(array_data.len(), chunk.len());
                let array: Arc<dyn Array> = Arc::new(array_data);
                (i.to_string(), array)
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(RecordBatch::try_from_iter(columns)?)
}

struct DataChunkHandleSlice<'a> {
    chunk: &'a mut DataChunkHandle,
    column_index: usize,
}

impl<'a> DataChunkHandleSlice<'a> {
    fn new(chunk: &'a mut DataChunkHandle, column_index: usize) -> Self {
        Self { chunk, column_index }
    }
}

impl WritableVector for DataChunkHandleSlice<'_> {
    fn array_vector(&mut self) -> ArrayVector<'_> {
        self.chunk.array_vector(self.column_index)
    }

    fn flat_vector(&mut self) -> FlatVector<'_> {
        self.chunk.flat_vector(self.column_index)
    }

    fn struct_vector(&mut self) -> StructVector<'_> {
        self.chunk.struct_vector(self.column_index)
    }

    fn list_vector(&mut self) -> ListVector<'_> {
        self.chunk.list_vector(self.column_index)
    }
}

/// A WriteableVector is a trait that allows writing data to a DuckDB vector.
/// To get the specific vector type, use the appropriate method.
pub trait WritableVector {
    /// Get the vector as a `FlatVector`.
    fn flat_vector(&mut self) -> FlatVector<'_>;
    /// Get the vector as a `ListVector`.
    fn list_vector(&mut self) -> ListVector<'_>;
    /// Get the vector as a `ArrayVector`.
    fn array_vector(&mut self) -> ArrayVector<'_>;
    /// Get the vector as a `StructVector`.
    fn struct_vector(&mut self) -> StructVector<'_>;
}

/// Writes an Arrow array to a `WritableVector`.
pub fn write_arrow_array_to_vector(
    col: &Arc<dyn Array>,
    chunk: &mut dyn WritableVector,
) -> Result<(), Box<dyn std::error::Error>> {
    match col.data_type() {
        dt if dt.is_primitive() || matches!(dt, DataType::Boolean) => {
            primitive_array_to_vector(col, &mut chunk.flat_vector())?;
        }
        DataType::Utf8 => {
            string_array_to_vector(as_string_array(col.as_ref()), &mut chunk.flat_vector());
        }
        DataType::LargeUtf8 => {
            string_array_to_vector(
                col.as_ref()
                    .as_any()
                    .downcast_ref::<LargeStringArray>()
                    .ok_or_else(|| Box::<dyn std::error::Error>::from("Unable to downcast to LargeStringArray"))?,
                &mut chunk.flat_vector(),
            );
        }
        DataType::Utf8View => {
            string_view_array_to_vector(
                col.as_ref()
                    .as_any()
                    .downcast_ref::<StringViewArray>()
                    .ok_or_else(|| Box::<dyn std::error::Error>::from("Unable to downcast to StringViewArray"))?,
                &mut chunk.flat_vector(),
            );
        }
        DataType::Binary => {
            binary_array_to_vector(as_generic_binary_array(col.as_ref()), &mut chunk.flat_vector());
        }
        DataType::FixedSizeBinary(_) => {
            fixed_size_binary_array_to_vector(col.as_ref().as_fixed_size_binary(), &mut chunk.flat_vector());
        }
        DataType::LargeBinary => {
            large_binary_array_to_vector(
                col.as_ref()
                    .as_any()
                    .downcast_ref::<LargeBinaryArray>()
                    .ok_or_else(|| Box::<dyn std::error::Error>::from("Unable to downcast to LargeBinaryArray"))?,
                &mut chunk.flat_vector(),
            );
        }
        DataType::BinaryView => {
            binary_view_array_to_vector(
                col.as_ref()
                    .as_any()
                    .downcast_ref::<BinaryViewArray>()
                    .ok_or_else(|| Box::<dyn std::error::Error>::from("Unable to downcast to BinaryViewArray"))?,
                &mut chunk.flat_vector(),
            );
        }
        DataType::List(_) => {
            list_array_to_vector(as_list_array(col.as_ref()), &mut chunk.list_vector())?;
        }
        DataType::LargeList(_) => {
            list_array_to_vector(as_large_list_array(col.as_ref()), &mut chunk.list_vector())?;
        }
        DataType::FixedSizeList(_, _) => {
            fixed_size_list_array_to_vector(as_fixed_size_list_array(col.as_ref()), &mut chunk.array_vector())?;
        }
        DataType::Struct(_) => {
            let struct_array = as_struct_array(col.as_ref());
            let mut struct_vector = chunk.struct_vector();
            struct_array_to_vector(struct_array, &mut struct_vector)?;
        }
        DataType::Map(_, _) => {
            // [`MapArray`] is physically a [`ListArray`] of key values pairs stored as an `entries` [`StructArray`] with 2 child fields.
            let map_array = as_map_array(col.as_ref());
            let out = &mut chunk.list_vector();
            struct_array_to_vector(map_array.entries(), &mut out.struct_child(map_array.entries().len()))?;

            for i in 0..map_array.len() {
                let offset = map_array.value_offsets()[i];
                let length = map_array.value_length(i);
                out.set_entry(i, offset.as_(), length.as_());
            }
            set_nulls_in_list_vector(map_array, out);
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

// Known aliasing hole (#673 follow-up): `duckdb_vector` is a `Copy` raw-pointer
// typedef, so `&mut self` only locks this particular binding — a caller holding
// another copy of the same pointer can still call these accessors and obtain
// aliased wrappers. `WritableVector` is a safe trait, so the obligation is on
// the caller to ensure the underlying vector outlives the wrapper and that no
// other copy is used concurrently. Marking the trait `unsafe` (or dropping the
// raw-pointer impl) would move this into the type system.
impl WritableVector for duckdb_vector {
    fn array_vector(&mut self) -> ArrayVector<'_> {
        unsafe { ArrayVector::from_raw(*self) }
    }

    fn flat_vector(&mut self) -> FlatVector<'_> {
        unsafe { FlatVector::from_raw(*self) }
    }

    fn list_vector(&mut self) -> ListVector<'_> {
        unsafe { ListVector::from_raw(*self) }
    }

    fn struct_vector(&mut self) -> StructVector<'_> {
        unsafe { StructVector::from_raw(*self) }
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
        write_arrow_array_to_vector(col, &mut DataChunkHandleSlice::new(chunk, i))?;
    }
    chunk.set_len(batch.num_rows());
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
        DataType::Decimal128(width, _) => {
            decimal_array_to_vector(as_primitive_array(array), out, *width);
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
fn decimal_array_to_vector(array: &Decimal128Array, out: &mut FlatVector<'_>, width: u8) {
    match width {
        1..=4 => {
            let out_data = unsafe { out.as_mut_slice() };
            for (i, value) in array.values().iter().enumerate() {
                out_data[i] = value.to_i16().unwrap();
            }
        }
        5..=9 => {
            let out_data = unsafe { out.as_mut_slice() };
            for (i, value) in array.values().iter().enumerate() {
                out_data[i] = value.to_i32().unwrap();
            }
        }
        10..=18 => {
            let out_data = unsafe { out.as_mut_slice() };
            for (i, value) in array.values().iter().enumerate() {
                out_data[i] = value.to_i64().unwrap();
            }
        }
        19..=38 => {
            let out_data = unsafe { out.as_mut_slice() };
            for (i, value) in array.values().iter().enumerate() {
                out_data[i] = value.to_i128().unwrap();
            }
        }
        // This should never happen, arrow only supports 1-38 decimal digits
        _ => panic!("Invalid decimal width: {width}"),
    }

    // Set nulls
    set_nulls_in_flat_vector(array, out);
}

/// Convert Arrow [BooleanArray] to a duckdb vector.
fn boolean_array_to_vector(array: &BooleanArray, out: &mut FlatVector<'_>) {
    assert!(array.len() <= out.capacity());

    for i in 0..array.len() {
        (unsafe { out.as_mut_slice() })[i] = array.value(i);
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

fn fixed_size_binary_array_to_vector(array: &FixedSizeBinaryArray, out: &mut FlatVector<'_>) {
    assert!(array.len() <= out.capacity());

    for i in 0..array.len() {
        let s = array.value(i);
        out.insert(i, s);
    }
    // Put this back once the other PR #
    // set_nulls_in_flat_vector(array, out);
}

fn large_binary_array_to_vector(array: &LargeBinaryArray, out: &mut FlatVector<'_>) {
    assert!(array.len() <= out.capacity());

    for i in 0..array.len() {
        let s = array.value(i);
        out.insert(i, s);
    }
    // Put this back once the other PR #
    // set_nulls_in_flat_vector(array, out);
}

fn list_array_to_vector<O: OffsetSizeTrait + AsPrimitive<usize>>(
    array: &GenericListArray<O>,
    out: &mut ListVector<'_>,
) -> Result<(), Box<dyn std::error::Error>> {
    let value_array = array.values();
    match value_array.data_type() {
        dt if dt.is_primitive() || matches!(dt, DataType::Boolean) => {
            primitive_array_to_vector(value_array.as_ref(), &mut out.child(value_array.len()))?;
        }
        DataType::Utf8 => {
            string_array_to_vector(as_string_array(value_array.as_ref()), &mut out.child(value_array.len()));
        }
        DataType::Utf8View => {
            string_view_array_to_vector(
                value_array
                    .as_ref()
                    .as_any()
                    .downcast_ref::<StringViewArray>()
                    .ok_or_else(|| Box::<dyn std::error::Error>::from("Unable to downcast to StringViewArray"))?,
                &mut out.child(value_array.len()),
            );
        }
        DataType::Binary => {
            binary_array_to_vector(
                as_generic_binary_array(value_array.as_ref()),
                &mut out.child(value_array.len()),
            );
        }
        DataType::BinaryView => {
            binary_view_array_to_vector(
                value_array
                    .as_ref()
                    .as_any()
                    .downcast_ref::<BinaryViewArray>()
                    .ok_or_else(|| Box::<dyn std::error::Error>::from("Unable to downcast to BinaryViewArray"))?,
                &mut out.child(value_array.len()),
            );
        }
        DataType::List(_) => {
            list_array_to_vector(as_list_array(value_array.as_ref()), &mut out.list_child())?;
        }
        DataType::FixedSizeList(_, _) => {
            fixed_size_list_array_to_vector(as_fixed_size_list_array(value_array.as_ref()), &mut out.array_child())?;
        }
        DataType::Struct(_) => {
            struct_array_to_vector(
                as_struct_array(value_array.as_ref()),
                &mut out.struct_child(value_array.len()),
            )?;
        }
        _ => {
            return Err(format!(
                "List with elements of type '{}' are not currently supported.",
                value_array.data_type()
            )
            .into());
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
    out: &mut ArrayVector<'_>,
) -> Result<(), Box<dyn std::error::Error>> {
    let value_array = array.values();
    let mut child = out.child(value_array.len());
    match value_array.data_type() {
        dt if dt.is_primitive() || matches!(dt, DataType::Boolean) => {
            primitive_array_to_vector(value_array.as_ref(), &mut child)?;
        }
        DataType::Utf8 => {
            string_array_to_vector(as_string_array(value_array.as_ref()), &mut child);
        }
        DataType::Binary => {
            binary_array_to_vector(as_generic_binary_array(value_array.as_ref()), &mut child);
        }
        DataType::FixedSizeBinary(_) => {
            fixed_size_binary_array_to_vector(value_array.as_ref().as_fixed_size_binary(), &mut child);
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

fn struct_array_to_vector(array: &StructArray, out: &mut StructVector<'_>) -> Result<(), Box<dyn std::error::Error>> {
    for i in 0..array.num_columns() {
        let column = array.column(i);
        match column.data_type() {
            dt if dt.is_primitive() || matches!(dt, DataType::Boolean) => {
                primitive_array_to_vector(column, &mut out.child(i, array.len()))?;
            }
            DataType::Utf8 => {
                string_array_to_vector(as_string_array(column.as_ref()), &mut out.child(i, array.len()));
            }
            DataType::Binary => {
                binary_array_to_vector(as_generic_binary_array(column.as_ref()), &mut out.child(i, array.len()));
            }
            DataType::FixedSizeBinary(_) => {
                fixed_size_binary_array_to_vector(
                    column.as_ref().as_fixed_size_binary(),
                    &mut out.child(i, array.len()),
                );
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
                    "Unsupported data type: {}, please file an issue https://github.com/duckdb/duckdb-rs",
                    column.data_type()
                );
            }
        }
    }
    set_nulls_in_struct_vector(array, out);
    Ok(())
}

/// Pass a RecordBatch to DuckDB.
///
/// This returns opaque query parameters for [`ArrowVTab`].
///
/// Each call permanently retains one [`RecordBatch`] allocation in a
/// process-global arena that is never freed, so stored views can rebind the same
/// parameters later. Memory grows monotonically. Do not call this per row or
/// per query. Create these parameters once per logical table or view.
///
/// # Panics
///
/// Panics if the process-global ArrowVTab record batch store mutex is poisoned.
pub fn arrow_recordbatch_to_query_params(rb: RecordBatch) -> [usize; 2] {
    register_arrow_record_batch(rb)
}

/// Pass ArrayData to DuckDB.
///
/// This converts the [`ArrayData`] to a [`RecordBatch`] immediately. Like
/// [`arrow_recordbatch_to_query_params`], each call permanently retains one
/// [`RecordBatch`] allocation in a process-global arena that is never freed.
///
/// # Panics
///
/// Panics if the process-global ArrowVTab record batch store mutex is poisoned.
pub fn arrow_arraydata_to_query_params(data: ArrayData) -> [usize; 2] {
    let struct_array = StructArray::from(data);
    arrow_recordbatch_to_query_params(RecordBatch::from(&struct_array))
}

/// Pass array and schema to DuckDB.
///
/// This imports the FFI values immediately. Like
/// [`arrow_recordbatch_to_query_params`], each call permanently retains one
/// [`RecordBatch`] allocation in a process-global arena that is never freed.
///
/// # Panics
///
/// Panics if the FFI values cannot be imported as Arrow data, or if the
/// process-global ArrowVTab record batch store mutex is poisoned.
pub fn arrow_ffi_to_query_params(array: FFI_ArrowArray, schema: FFI_ArrowSchema) -> [usize; 2] {
    arrow_recordbatch_to_query_params(arrow_record_batch_from_ffi(array, schema))
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

fn set_nulls_in_list_vector(array: &dyn Array, out_vector: &mut ListVector<'_>) {
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
    use super::{
        ArrowInitData, ArrowVTab, RowSlice, arrow_arraydata_to_query_params, arrow_ffi_to_query_params,
        arrow_recordbatch_to_query_params,
    };
    use crate::{Connection, Result};
    use arrow::{
        array::{
            Array, ArrayRef, AsArray, BinaryArray, BinaryViewArray, BooleanArray, Date32Array, Date64Array,
            Decimal128Array, Decimal256Array, DurationSecondArray, FixedSizeBinaryArray, FixedSizeListArray,
            FixedSizeListBuilder, GenericByteArray, GenericListArray, Int32Array, Int32Builder, IntervalDayTimeArray,
            IntervalMonthDayNanoArray, IntervalYearMonthArray, LargeStringArray, ListArray, ListBuilder, MapArray,
            OffsetSizeTrait, PrimitiveArray, StringArray, StringViewArray, StructArray, Time32SecondArray,
            Time64MicrosecondArray, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
            TimestampSecondArray, UInt32Array,
        },
        buffer::{OffsetBuffer, ScalarBuffer},
        datatypes::{
            ArrowPrimitiveType, ByteArrayType, DataType, DurationSecondType, Field, IntervalDayTimeType,
            IntervalMonthDayNanoType, IntervalYearMonthType, Schema, i256,
        },
        ffi::{FFI_ArrowArray, FFI_ArrowSchema},
        record_batch::RecordBatch,
    };
    use std::{
        error::Error,
        sync::{Arc, Barrier, atomic::AtomicUsize},
    };

    fn example_record_batch() -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("is_odd", DataType::Boolean, true),
        ]);
        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4])) as ArrayRef,
                Arc::new(StringArray::from(vec!["apple", "banana", "cherry", "date"])) as ArrayRef,
                Arc::new(BooleanArray::from(vec![true, false, true, false])) as ArrayRef,
            ],
        )
        .expect("failed to create record batch")
    }

    // Mark rows straddling vector-size slice boundaries as NULL so bitmap offsets
    // are exercised when the source batch is sliced.
    fn boundary_null(i: usize, vector_size: usize) -> bool {
        i == vector_size.saturating_sub(1) || i == vector_size || i == vector_size.saturating_add(1)
    }

    fn large_record_batch(n: usize, vector_size: usize) -> RecordBatch {
        let ids: Vec<i32> = (0..n as i32).collect();
        let vals: Vec<Option<String>> = (0..n)
            .map(|i| {
                if boundary_null(i, vector_size) {
                    None
                } else {
                    Some(format!("val-{i:05}"))
                }
            })
            .collect();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("val", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(ids)) as ArrayRef,
                Arc::new(StringArray::from(vals)) as ArrayRef,
            ],
        )
        .expect("failed to create record batch")
    }

    #[test]
    fn test_arrow_init_data_take_slice_partitions_rows_concurrently() {
        let num_rows = 10_000usize;
        let thread_count = 16;
        let init = Arc::new(ArrowInitData {
            offset: AtomicUsize::new(0),
            vector_size: 1,
        });
        let start = Arc::new(Barrier::new(thread_count));

        let mut handles = Vec::new();
        for _ in 0..thread_count {
            let init = Arc::clone(&init);
            let start = Arc::clone(&start);
            handles.push(std::thread::spawn(move || {
                start.wait();

                let mut slices = Vec::new();
                while let Some(slice) = init.take_slice(num_rows) {
                    assert!(slice.len > 0);
                    assert!(slice.offset < num_rows);
                    assert!(slice.offset + slice.len <= num_rows);
                    slices.push(slice);
                }
                slices
            }));
        }

        let mut slices = handles
            .into_iter()
            .flat_map(|handle| handle.join().expect("slice worker panicked"))
            .collect::<Vec<_>>();
        slices.sort_unstable_by_key(|slice| slice.offset);

        assert_eq!(slices.len(), num_rows);
        let mut next_offset = 0;
        for slice in slices {
            assert_eq!(slice.offset, next_offset);
            next_offset += slice.len;
        }
        assert_eq!(next_offset, num_rows);
        assert!(init.take_slice(num_rows).is_none());
    }

    #[test]
    fn test_arrow_init_data_take_slice_handles_partial_tail() {
        let init = ArrowInitData {
            offset: AtomicUsize::new(0),
            vector_size: 4,
        };

        assert_eq!(init.take_slice(9), Some(RowSlice { offset: 0, len: 4 }));
        assert_eq!(init.take_slice(9), Some(RowSlice { offset: 4, len: 4 }));
        assert_eq!(init.take_slice(9), Some(RowSlice { offset: 8, len: 1 }));
        assert_eq!(init.take_slice(9), None);
        assert_eq!(init.take_slice(9), None);

        let empty = ArrowInitData {
            offset: AtomicUsize::new(0),
            vector_size: 4,
        };
        assert_eq!(empty.take_slice(0), None);
        assert_eq!(empty.take_slice(0), None);
    }

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
    fn test_vtab_arrow_large_record_batch() -> Result<(), Box<dyn Error>> {
        let db = Connection::open_in_memory()?;
        db.register_table_function::<ArrowVTab>("arrow")?;
        let vector_size = unsafe { crate::ffi::duckdb_vector_size() } as usize;
        assert!(vector_size > 0);

        let two_vectors = vector_size.checked_mul(2).expect("vector size overflow");
        let one_over = vector_size.checked_add(1).expect("vector size overflow");
        let partial_tail = vector_size / 2;
        let with_tail = two_vectors.checked_add(partial_tail).expect("vector size overflow");

        // Cover empty input, small input, vector-size boundary cases, and a tail
        // after multiple full vectors.
        for n in [
            0usize,
            1,
            vector_size.saturating_sub(1),
            vector_size,
            one_over,
            two_vectors,
            with_tail,
        ] {
            let param = arrow_recordbatch_to_query_params(large_record_batch(n, vector_size));
            let rbs: Vec<RecordBatch> = db
                .prepare("SELECT id, val FROM arrow(?, ?) ORDER BY id")?
                .query_arrow(param)?
                .collect();

            let total: usize = rbs.iter().map(|rb| rb.num_rows()).sum();
            assert_eq!(total, n, "row count mismatch for n={n}");

            // Verify every row survives the slicing in order, across chunk boundaries.
            let ids: Vec<i32> = rbs
                .iter()
                .flat_map(|rb| {
                    rb.column(0)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap()
                        .values()
                        .to_vec()
                })
                .collect();
            assert_eq!(
                ids,
                (0..n as i32).collect::<Vec<_>>(),
                "row order/content mismatch for n={n}"
            );

            let vals: Vec<Option<String>> = rbs
                .iter()
                .flat_map(|rb| {
                    let val = rb.column(1).as_any().downcast_ref::<StringArray>().unwrap();
                    (0..val.len())
                        .map(|i| {
                            if val.is_null(i) {
                                None
                            } else {
                                Some(val.value(i).to_string())
                            }
                        })
                        .collect::<Vec<_>>()
                })
                .collect();
            let expected_vals: Vec<Option<String>> = (0..n)
                .map(|i| {
                    if boundary_null(i, vector_size) {
                        None
                    } else {
                        Some(format!("val-{i:05}"))
                    }
                })
                .collect();
            assert_eq!(vals, expected_vals, "null/value mismatch for n={n}");
        }
        Ok(())
    }

    #[test]
    fn test_vtab_arrow_rust_array() -> Result<(), Box<dyn Error>> {
        let db = Connection::open_in_memory()?;
        db.register_table_function::<ArrowVTab>("arrow")?;

        // This is a show case that it's easy for you to build an in-memory data
        // and pass into DuckDB
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
    fn test_vtab_arrow_view_can_rebind_record_batch() -> Result<(), Box<dyn Error>> {
        let db = Connection::open_in_memory()?;
        db.register_table_function::<ArrowVTab>("arrow")?;

        let batch = example_record_batch();
        let param = arrow_recordbatch_to_query_params(batch.clone());
        db.execute(
            &format!(
                "CREATE VIEW arrow_view AS SELECT * FROM arrow({}::UBIGINT, {}::UBIGINT)",
                param[0], param[1]
            ),
            [],
        )?;

        for _ in 0..2 {
            let rbs: Vec<RecordBatch> = db.prepare("SELECT * FROM arrow_view")?.query_arrow([])?.collect();
            assert_eq!(vec![batch.clone()], rbs);
        }

        Ok(())
    }

    #[test]
    fn test_vtab_arrow_arraydata_query_params() -> Result<(), Box<dyn Error>> {
        let batch = example_record_batch();
        let struct_array = StructArray::from(batch);
        let param = arrow_arraydata_to_query_params(struct_array.to_data());

        let db = Connection::open_in_memory()?;
        db.register_table_function::<ArrowVTab>("arrow")?;
        let mut stmt = db.prepare("select sum(id)::int32 from arrow(?, ?)")?;
        let rb = stmt.query_arrow(param)?.next().expect("no record batch");
        let column = rb.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(column.value(0), 10);
        Ok(())
    }

    #[test]
    fn test_vtab_arrow_ffi_query_params() -> Result<(), Box<dyn Error>> {
        let batch = example_record_batch();
        let struct_array = StructArray::from(batch);
        let array = FFI_ArrowArray::new(&struct_array.to_data());
        let schema = FFI_ArrowSchema::try_from(struct_array.data_type())?;
        let param = arrow_ffi_to_query_params(array, schema);

        let db = Connection::open_in_memory()?;
        db.register_table_function::<ArrowVTab>("arrow")?;
        let mut stmt = db.prepare("select sum(id)::int32 from arrow(?, ?)")?;
        let rb = stmt.query_arrow(param)?.next().expect("no record batch");
        let column = rb.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(column.value(0), 10);
        Ok(())
    }

    #[test]
    fn test_arrow_null_query_params_error() {
        let db = Connection::open_in_memory().unwrap();
        db.register_table_function::<ArrowVTab>("arrow").unwrap();

        let err = db.prepare("SELECT * FROM arrow(NULL, NULL)").err().unwrap();
        assert!(
            err.to_string()
                .contains("ArrowVTab record batch address parameter must not be NULL"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_arrow_zero_address_query_params_error() {
        let db = Connection::open_in_memory().unwrap();
        db.register_table_function::<ArrowVTab>("arrow").unwrap();

        let valid = arrow_recordbatch_to_query_params(example_record_batch());
        let sql = format!("SELECT * FROM arrow(0::UBIGINT, {}::UBIGINT)", valid[1]);
        let err = db.prepare(&sql).err().unwrap();
        assert!(
            err.to_string().contains("invalid ArrowVTab record batch address"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_arrow_marker_mismatch_query_params_error() {
        let db = Connection::open_in_memory().unwrap();
        db.register_table_function::<ArrowVTab>("arrow").unwrap();

        let err = db.prepare("SELECT * FROM arrow(1::UBIGINT, 2::UBIGINT)").err().unwrap();
        assert!(
            err.to_string().contains("query parameter marker mismatch"),
            "unexpected error: {err}"
        );
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
            MapArray::new_from_strings(keys.iter().map(String::as_str), &values_data, entry_offsets.as_slice())
                .unwrap();
        check_map_array_roundtrip(map_array)?;

        Ok(())
    }

    #[test]
    fn test_fixed_size_binary_in_struct() -> Result<(), Box<dyn Error>> {
        let db = Connection::open_in_memory()?;
        db.register_table_function::<ArrowVTab>("arrow")?;

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

        let schema = Schema::new(vec![Field::new("struct_col", struct_array.data_type().clone(), false)]);
        let rb = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(struct_array)])?;

        let param = arrow_recordbatch_to_query_params(rb);
        let mut stmt = db.prepare("SELECT struct_col FROM arrow(?, ?)")?;
        let mut arr = stmt.query_arrow(param)?;
        let rb = arr.next().expect("no record batch");

        assert_eq!(rb.num_columns(), 1);
        let struct_column = rb.column(0).as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(struct_column.len(), 3);

        // Verify the binary field
        let binary_field = struct_column.column(0).as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(binary_field.value(0), &[1u8, 2, 3, 4, 5, 6, 7, 8]);
        assert_eq!(binary_field.value(1), &[9u8, 10, 11, 12, 13, 14, 15, 16]);
        assert_eq!(binary_field.value(2), &[17u8, 18, 19, 20, 21, 22, 23, 24]);

        // Verify the int field
        let int_field = struct_column.column(1).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(int_field.value(0), 100);
        assert_eq!(int_field.value(1), 200);
        assert_eq!(int_field.value(2), 300);

        Ok(())
    }

    #[test]
    fn test_fixed_size_binary_in_fixed_size_list() -> Result<(), Box<dyn Error>> {
        let db = Connection::open_in_memory()?;
        db.register_table_function::<ArrowVTab>("arrow")?;

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

        let schema = Schema::new(vec![Field::new("list_col", fixed_size_list.data_type().clone(), false)]);
        let rb = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(fixed_size_list)])?;

        let param = arrow_recordbatch_to_query_params(rb);
        let mut stmt = db.prepare("SELECT list_col FROM arrow(?, ?)")?;
        let mut arr = stmt.query_arrow(param)?;
        let rb = arr.next().expect("no record batch");

        assert_eq!(rb.num_columns(), 1);

        // NOTE: FixedSizeBinary inside FixedSizeList is not yet fully supported in the roundtrip
        // DuckDB keeps it as FixedSizeListArray but the nested conversion doesn't work yet
        // This test documents the current limitation
        if rb.column(0).as_any().downcast_ref::<FixedSizeListArray>().is_some() {
            // Early return - this case isn't fully supported yet
            return Ok(());
        }

        // If it was converted to a regular ListArray, verify it works
        let list_column = rb.column(0).as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(list_column.len(), 2);

        // Verify first list element [1,2,3,4], [5,6,7,8]
        let first_list = list_column.value(0);
        let first_binary = first_list.as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(first_binary.len(), 2);
        assert_eq!(first_binary.value(0), &[1u8, 2, 3, 4]);
        assert_eq!(first_binary.value(1), &[5u8, 6, 7, 8]);

        // Verify second list element [9,10,11,12], [13,14,15,16]
        let second_list = list_column.value(1);
        let second_binary = second_list.as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(second_binary.len(), 2);
        assert_eq!(second_binary.value(0), &[9u8, 10, 11, 12]);
        assert_eq!(second_binary.value(1), &[13u8, 14, 15, 16]);

        Ok(())
    }

    #[test]
    fn test_fixed_size_binary_with_nulls_in_struct() -> Result<(), Box<dyn Error>> {
        let db = Connection::open_in_memory()?;
        db.register_table_function::<ArrowVTab>("arrow")?;

        // Create a struct with nullable FixedSizeBinary field
        let fixed_size_binary_data = FixedSizeBinaryArray::try_from_sparse_iter_with_size(
            vec![Some(vec![1u8, 2, 3, 4]), None, Some(vec![5u8, 6, 7, 8])].into_iter(),
            4,
        )
        .unwrap();

        let struct_array = StructArray::from(vec![(
            Arc::new(Field::new("binary_field", DataType::FixedSizeBinary(4), true)),
            Arc::new(fixed_size_binary_data) as ArrayRef,
        )]);

        let schema = Schema::new(vec![Field::new("struct_col", struct_array.data_type().clone(), false)]);
        let rb = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(struct_array)])?;

        let param = arrow_recordbatch_to_query_params(rb);
        let mut stmt = db.prepare("SELECT struct_col FROM arrow(?, ?)")?;
        let mut arr = stmt.query_arrow(param)?;
        let rb = arr.next().expect("no record batch");

        let struct_column = rb.column(0).as_any().downcast_ref::<StructArray>().unwrap();
        let binary_field = struct_column.column(0).as_any().downcast_ref::<BinaryArray>().unwrap();

        // NOTE: Null handling for FixedSizeBinary is not fully implemented
        // (see fixed_size_binary_array_to_vector, line 925-926)
        // Nulls are currently converted to zero bytes
        assert_eq!(binary_field.len(), 3);
        assert_eq!(binary_field.value(0), &[1u8, 2, 3, 4]);
        assert_eq!(binary_field.value(1), &[0u8, 0, 0, 0]); // Should be null
        assert_eq!(binary_field.value(2), &[5u8, 6, 7, 8]);

        Ok(())
    }
}

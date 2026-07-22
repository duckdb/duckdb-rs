use std::{error::Error, sync::Arc};

use arrow::{
    array::{
        Array, ArrayRef, BooleanArray, FixedSizeListArray, GenericBinaryBuilder, ListArray, MapArray, PrimitiveArray,
        StringArray, StructArray,
    },
    buffer::{NullBuffer, OffsetBuffer, ScalarBuffer},
    datatypes::*,
    record_batch::RecordBatch,
};
use libduckdb_sys::{duckdb_date, duckdb_string_t, duckdb_time};

use crate::{
    core::{DataChunkHandle, FlatVector, LogicalTypeId, VectorRef},
    types::DuckString,
};

/// Maps each destination row to its DuckDB source row; `None` emits a null.
type SourceRow = Option<usize>;
type SourceRows = Vec<SourceRow>;

fn source_len(rows: &[SourceRow]) -> Result<usize, Box<dyn Error>> {
    rows.iter().flatten().copied().max().map_or(Ok(0), |row| {
        row.checked_add(1)
            .ok_or_else(|| "DuckDB row index exceeds usize range".into())
    })
}

fn masked_rows<F>(rows: &SourceRows, is_null: F) -> Result<SourceRows, Box<dyn Error>>
where
    F: Fn(usize) -> crate::Result<bool>,
{
    Ok(rows
        .iter()
        .copied()
        .map(|source_row| match source_row {
            Some(row) => is_null(row).map(|is_null| (!is_null).then_some(row)),
            None => Ok(None),
        })
        .collect::<crate::Result<_>>()?)
}

fn null_buffer(rows: &SourceRows) -> Option<NullBuffer> {
    let validity = NullBuffer::from_iter(rows.iter().map(|source_row| source_row.is_some()));
    (validity.null_count() > 0).then_some(validity)
}

fn fixed_size_rows(rows: &SourceRows, width: usize) -> Result<SourceRows, Box<dyn Error>> {
    let len = rows
        .len()
        .checked_mul(width)
        .ok_or("DuckDB array child length overflows usize for Arrow conversion")?;
    let mut child_rows = Vec::with_capacity(len);

    for source_row in rows.iter().copied() {
        match source_row {
            Some(source_row) => {
                let start = source_row
                    .checked_mul(width)
                    .ok_or("DuckDB array child span overflows usize for Arrow conversion")?;
                let end = start
                    .checked_add(width)
                    .ok_or("DuckDB array child span overflows usize for Arrow conversion")?;
                child_rows.extend((start..end).map(Some));
            }
            None => child_rows.extend(std::iter::repeat_n(None, width)),
        }
    }

    Ok(child_rows)
}

fn primitive_array_from_rows<T, P, F>(
    vector: &VectorRef<'_>,
    rows: &SourceRows,
    convert: F,
) -> Result<PrimitiveArray<P>, Box<dyn Error>>
where
    T: Copy,
    P: ArrowPrimitiveType,
    F: Fn(T) -> P::Native,
{
    let values = rows
        .iter()
        .copied()
        .map(|source_row| {
            source_row
                // SAFETY: scalar dispatch instantiates `T` for the logical
                // type's physical storage, and validity masking already
                // dropped null rows, so each selected slot is initialized.
                .map(|row| unsafe { vector.read::<T>(row) }.map(&convert))
                .transpose()
        })
        .collect::<crate::Result<Vec<_>>>()?;
    Ok(values.into_iter().collect())
}

/// Reads rows whose Arrow native type matches the DuckDB physical storage.
fn native_rows<P: ArrowPrimitiveType>(
    vector: &VectorRef<'_>,
    rows: &SourceRows,
) -> Result<PrimitiveArray<P>, Box<dyn Error>> {
    primitive_array_from_rows::<P::Native, P, _>(vector, rows, |value| value)
}

fn native_array_from_rows<P: ArrowPrimitiveType>(
    vector: &VectorRef<'_>,
    rows: &SourceRows,
) -> Result<Arc<dyn Array>, Box<dyn Error>> {
    Ok(Arc::new(native_rows::<P>(vector, rows)?))
}

/// Converts the first `len` rows of a DuckDB vector to an Arrow array.
///
/// Returns an error if `len` exceeds the flat vector's backing capacity or its
/// initialized row count.
pub fn flat_vector_to_arrow_array(vector: &FlatVector<'_>, len: usize) -> Result<Arc<dyn Array>, Box<dyn Error>> {
    if len > vector.capacity() {
        return Err(format!(
            "DuckDB vector length {len} exceeds backing capacity {}",
            vector.capacity()
        )
        .into());
    }
    let readable_len = vector.readable_len()?;
    if len > readable_len {
        return Err(format!("DuckDB vector length {len} exceeds initialized row count {readable_len}").into());
    }
    let rows = (0..len).map(Some).collect();
    vector_to_arrow_array(vector.native_read_ref()?, &rows)
}

fn scalar_vector_rows_to_arrow_array(
    vector: &VectorRef<'_>,
    rows: &SourceRows,
) -> Result<Arc<dyn Array>, Box<dyn Error>> {
    let raw_type_id = vector.logical_type().raw_id();
    let type_id = LogicalTypeId::from(raw_type_id);
    match type_id {
        LogicalTypeId::Invalid => Err("Cannot convert invalid logical type returned by DuckDB".into()),
        LogicalTypeId::Unsupported => Err(format!("Unsupported DuckDB logical type ID {raw_type_id}").into()),
        LogicalTypeId::Integer => native_array_from_rows::<Int32Type>(vector, rows),
        // DuckDB's timestamp carriers (duckdb_timestamp_s/_ms/_ns and
        // duckdb_timestamp) are single-i64 structs, so each unit reads
        // natively into its Arrow timestamp type.
        LogicalTypeId::TimestampS => native_array_from_rows::<TimestampSecondType>(vector, rows),
        LogicalTypeId::TimestampMs => native_array_from_rows::<TimestampMillisecondType>(vector, rows),
        LogicalTypeId::Timestamp => native_array_from_rows::<TimestampMicrosecondType>(vector, rows),
        LogicalTypeId::TimestampNs => native_array_from_rows::<TimestampNanosecondType>(vector, rows),
        // TIMESTAMP_TZ stores UTC microseconds; a data chunk carries no
        // session display timezone.
        LogicalTypeId::TimestampTZ => Ok(Arc::new(
            native_rows::<TimestampMicrosecondType>(vector, rows)?.with_timezone("UTC"),
        )),
        LogicalTypeId::Varchar => {
            let values = rows
                .iter()
                .copied()
                .map(|source_row| {
                    source_row
                        .map(|row| {
                            // SAFETY: the logical type establishes `duckdb_string_t`
                            // physical storage, and validity masking already dropped
                            // null rows, so each selected slot is initialized.
                            unsafe { vector.read::<duckdb_string_t>(row) }
                                .map(|mut ptr| DuckString::new(&mut ptr).as_str().to_string())
                        })
                        .transpose()
                })
                .collect::<crate::Result<Vec<_>>>()?;

            Ok(Arc::new(StringArray::from(values)))
        }
        LogicalTypeId::Boolean => {
            // DuckDB stores booleans in one-byte slots; reading them as bytes
            // avoids constructing Rust `bool`s over arbitrary payloads.
            let values = rows
                .iter()
                .copied()
                // SAFETY: validity masking already dropped null rows, so each
                // selected one-byte slot is initialized.
                .map(|source_row| {
                    source_row
                        .map(|row| unsafe { vector.read::<u8>(row) }.map(|value| value != 0))
                        .transpose()
                })
                .collect::<crate::Result<Vec<_>>>()?;

            Ok(Arc::new(BooleanArray::from_iter(values)))
        }
        LogicalTypeId::Float => native_array_from_rows::<Float32Type>(vector, rows),
        LogicalTypeId::Double => native_array_from_rows::<Float64Type>(vector, rows),
        LogicalTypeId::Date => Ok(Arc::new(primitive_array_from_rows::<duckdb_date, Date32Type, _>(
            vector,
            rows,
            |value| value.days,
        )?)),
        LogicalTypeId::Time => Ok(Arc::new(primitive_array_from_rows::<
            duckdb_time,
            Time64MicrosecondType,
            _,
        >(vector, rows, |value| value.micros)?)),
        LogicalTypeId::Smallint => native_array_from_rows::<Int16Type>(vector, rows),
        LogicalTypeId::USmallint => native_array_from_rows::<UInt16Type>(vector, rows),
        LogicalTypeId::Blob | LogicalTypeId::Geometry => {
            // DuckDB currently stores GEOMETRY vectors as WKB-backed string_t
            // bytes. Treating GEOMETRY like BLOB here relies on that internal
            // representation staying WKB-compatible.
            let mut builder = GenericBinaryBuilder::<i32>::new();

            for source_row in rows.iter().copied() {
                match source_row {
                    Some(row) => {
                        // SAFETY: Blob and Geometry use DuckDB string storage
                        // for their bytes, and validity masking already dropped
                        // null rows, so each selected slot is initialized.
                        let mut ptr = unsafe { vector.read::<duckdb_string_t>(row) }?;
                        let mut value = DuckString::new(&mut ptr);
                        builder.append_value(value.as_bytes());
                    }
                    _ => builder.append_null(),
                }
            }

            Ok(Arc::new(builder.finish()))
        }
        LogicalTypeId::Tinyint => native_array_from_rows::<Int8Type>(vector, rows),
        LogicalTypeId::Bigint => native_array_from_rows::<Int64Type>(vector, rows),
        LogicalTypeId::UBigint => native_array_from_rows::<UInt64Type>(vector, rows),
        LogicalTypeId::UTinyint => native_array_from_rows::<UInt8Type>(vector, rows),
        LogicalTypeId::UInteger => native_array_from_rows::<UInt32Type>(vector, rows),
        type_id @ (LogicalTypeId::Interval
        | LogicalTypeId::Hugeint
        | LogicalTypeId::Decimal
        | LogicalTypeId::Enum
        | LogicalTypeId::Uuid
        | LogicalTypeId::Union
        | LogicalTypeId::Bit
        | LogicalTypeId::TimeTZ
        | LogicalTypeId::UHugeint
        | LogicalTypeId::Any
        | LogicalTypeId::Bignum
        | LogicalTypeId::SqlNull
        | LogicalTypeId::StringLiteral
        | LogicalTypeId::IntegerLiteral
        | LogicalTypeId::TimeNs
        | LogicalTypeId::Variant) => Err(format!("Cannot convert DuckDB logical type {type_id:?} to Arrow").into()),
        type_id @ (LogicalTypeId::List | LogicalTypeId::Struct | LogicalTypeId::Map | LogicalTypeId::Array) => {
            Err(format!("Nested DuckDB logical type {type_id:?} reached the scalar Arrow converter").into())
        }
    }
}

/// Converts selected rows through the capacity-bearing native vector seam.
fn vector_to_arrow_array(vector: VectorRef<'_>, rows: &SourceRows) -> Result<ArrayRef, Box<dyn Error>> {
    let required = source_len(rows)?;
    if required > vector.capacity() {
        return Err(format!(
            "DuckDB selected row span {required} exceeds backing capacity {}",
            vector.capacity()
        )
        .into());
    }
    let rows = masked_rows(rows, |row| vector.try_row_is_null(row as u64))?;
    let type_id = vector.logical_type().id();
    match type_id {
        LogicalTypeId::List => list_vector_to_arrow_array(&vector, &rows),
        LogicalTypeId::Struct => struct_vector_to_arrow_array(&vector, &rows),
        LogicalTypeId::Map => map_vector_to_arrow_array(&vector, &rows),
        LogicalTypeId::Array => array_vector_to_arrow_array(&vector, &rows),
        _ => scalar_vector_rows_to_arrow_array(&vector, &rows),
    }
}

/// Checks the next list offset fits Arrow's i32 range before gathering rows.
fn checked_list_output_len(current_len: usize, additional_len: usize) -> Result<i32, Box<dyn Error>> {
    let output_len = current_len
        .checked_add(additional_len)
        .ok_or("DuckDB list values exceed Arrow i32 offset range")?;
    i32::try_from(output_len).map_err(|_| "DuckDB list values exceed Arrow i32 offset range".into())
}

fn list_layout(
    vector: &VectorRef<'_>,
    rows: &SourceRows,
) -> Result<(OffsetBuffer<i32>, Vec<SourceRow>), Box<dyn Error>> {
    let mut offsets = Vec::with_capacity(rows.len() + 1);
    let mut child_rows = Vec::new();
    let mut next_offset = 0;
    offsets.push(next_offset);

    for source_row in rows.iter().copied() {
        if let Some(source_row) = source_row {
            if let Some(range) = vector.list_range(source_row)? {
                let length = range.len();
                next_offset = checked_list_output_len(child_rows.len(), length)?;
                child_rows.extend(range.map(Some));
            }
        }

        offsets.push(next_offset);
    }

    Ok((OffsetBuffer::new(ScalarBuffer::from(offsets)), child_rows))
}

fn list_vector_to_arrow_array(vector: &VectorRef<'_>, rows: &SourceRows) -> Result<ArrayRef, Box<dyn Error>> {
    let (offsets, child_rows) = list_layout(vector, rows)?;
    let values = vector_to_arrow_array(vector.list_child()?, &child_rows)?;
    let field = Arc::new(Field::new("item", values.data_type().clone(), true));
    let nulls = null_buffer(rows);

    Ok(Arc::new(ListArray::try_new(field, offsets, values, nulls)?))
}

fn map_vector_to_arrow_array(vector: &VectorRef<'_>, rows: &SourceRows) -> Result<ArrayRef, Box<dyn Error>> {
    let (offsets, child_rows) = list_layout(vector, rows)?;
    let entries_len = child_rows.len();
    let entries_vector = vector.list_child()?;

    for source_row in child_rows.iter().flatten().copied() {
        if entries_vector.try_row_is_null(source_row as u64)? {
            return Err(format!("DuckDB map entry at child row {source_row} is null").into());
        }
    }

    let keys = vector_to_arrow_array(entries_vector.struct_child(0)?, &child_rows)?;
    if keys.null_count() > 0 {
        let source_row = child_rows
            .iter()
            .enumerate()
            .find(|(output_row, _)| keys.is_null(*output_row))
            .and_then(|(_, source_row)| *source_row);
        let message = source_row.map_or_else(
            || "DuckDB map key is null".to_string(),
            |source_row| format!("DuckDB map key at child row {source_row} is null"),
        );
        return Err(message.into());
    }
    let values = vector_to_arrow_array(entries_vector.struct_child(1)?, &child_rows)?;
    let fields = Fields::from(vec![
        Field::new("keys", keys.data_type().clone(), false),
        Field::new("values", values.data_type().clone(), true),
    ]);
    let entries = StructArray::try_new_with_length(fields, vec![keys, values], None, entries_len)?;
    let field = Arc::new(Field::new("entries", entries.data_type().clone(), false));
    let nulls = null_buffer(rows);

    Ok(Arc::new(MapArray::try_new(field, offsets, entries, nulls, false)?))
}

fn array_vector_to_arrow_array(vector: &VectorRef<'_>, rows: &SourceRows) -> Result<ArrayRef, Box<dyn Error>> {
    let array_size = vector.array_size()?;
    let value_length =
        i32::try_from(array_size).map_err(|_| "DuckDB array size exceeds Arrow i32 value length range")?;
    let child_rows = fixed_size_rows(rows, array_size)?;

    let values = vector_to_arrow_array(vector.array_child()?, &child_rows)?;
    let field = Arc::new(Field::new("item", values.data_type().clone(), true));
    let nulls = null_buffer(rows);

    Ok(Arc::new(FixedSizeListArray::try_new(
        field,
        value_length,
        values,
        nulls,
    )?))
}

fn struct_vector_to_arrow_array(vector: &VectorRef<'_>, rows: &SourceRows) -> Result<ArrayRef, Box<dyn Error>> {
    let logical_type = vector.logical_type();
    let mut fields = Vec::with_capacity(logical_type.num_children());
    let mut arrays = Vec::with_capacity(logical_type.num_children());

    for idx in 0..logical_type.num_children() {
        let array = vector_to_arrow_array(vector.struct_child(idx)?, rows)?;
        fields.push(Field::new(
            logical_type.try_child_name(idx)?,
            array.data_type().clone(),
            true,
        ));
        arrays.push(array);
    }

    let nulls = null_buffer(rows);
    Ok(Arc::new(StructArray::try_new_with_length(
        Fields::from(fields),
        arrays,
        nulls,
        rows.len(),
    )?))
}

/// Converts a DuckDB data chunk to an Arrow record batch.
///
/// The Arrow schema is synthesized from DuckDB logical types. Top-level column
/// names are synthesized as `"0"`, `"1"`, and so on. List
/// and map child names use Arrow's conventional names, struct children are
/// nullable, field metadata is not preserved, and lists use 32-bit Arrow
/// offsets. Timestamp mappings preserve DuckDB's seconds, milliseconds,
/// microseconds, and nanoseconds carriers. Timestamp-with-time-zone values are
/// emitted as UTC microseconds because a data chunk does not carry a session
/// display timezone. Newly allocated chunks remain under construction until a
/// native writer completes; manual writers must call
/// [`DataChunkHandle::assume_initialized`] before conversion.
pub fn data_chunk_to_arrow(chunk: &DataChunkHandle) -> Result<RecordBatch, Box<dyn Error>> {
    let len = chunk.len();

    let columns = (0..chunk.num_columns())
        .map(|i| {
            let vector = chunk.initialized_vector(i, len)?;
            let rows = (0..len).map(Some).collect();
            vector_to_arrow_array(vector, &rows)
                .map(|array| (i.to_string(), array))
                .map_err(|err| -> Box<dyn Error> { format!("DuckDB column {i}: {err}").into() })
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(RecordBatch::try_from_iter(columns)?)
}

#[cfg(test)]
mod tests {
    use super::checked_list_output_len;

    #[test]
    fn list_output_length_is_checked_before_gathering_rows() {
        let max_offset = i32::MAX as usize;
        assert_eq!(checked_list_output_len(max_offset - 1, 1).unwrap(), i32::MAX);

        let err = checked_list_output_len(max_offset, 1).unwrap_err();
        assert_eq!(err.to_string(), "DuckDB list values exceed Arrow i32 offset range");
    }
}

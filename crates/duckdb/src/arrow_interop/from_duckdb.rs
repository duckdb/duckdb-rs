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
use libduckdb_sys::{duckdb_date, duckdb_string_t, duckdb_time, duckdb_vector, duckdb_vector_get_column_type};

use crate::{
    core::{ArrayVector, DataChunkHandle, FlatVector, ListVector, LogicalTypeHandle, LogicalTypeId, StructVector},
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

/// Reads the physical value stored at `row` through a raw pointer.
///
/// DuckDB does not zero-initialize vector payloads, so null or unselected
/// slots may be uninitialized. Reading one slot at a time avoids materializing
/// a reference that spans such slots, which `slice::from_raw_parts` would
/// require to be fully initialized.
///
/// # Safety
/// `T` must match the vector's physical storage and the slot at `row` must be
/// initialized; DuckDB initializes every valid (non-null) row.
unsafe fn read_row<T: Copy>(vector: &FlatVector<'_>, row: usize) -> T {
    assert!(
        row < vector.capacity(),
        "row {row} exceeds vector capacity {}",
        vector.capacity()
    );
    // SAFETY: the bounds check above keeps the read inside the backing
    // allocation the wrapper was constructed with.
    unsafe { vector.as_mut_ptr::<T>().add(row).read() }
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

fn primitive_array_from_rows<T, P, F>(vector: &FlatVector<'_>, rows: &SourceRows, convert: F) -> PrimitiveArray<P>
where
    T: Copy,
    P: ArrowPrimitiveType,
    F: Fn(T) -> P::Native,
{
    rows.iter()
        .copied()
        .map(|source_row| {
            source_row
                // SAFETY: scalar dispatch instantiates `T` for the logical
                // type's physical storage, and validity masking already
                // dropped null rows, so each selected slot is initialized.
                .map(|row| convert(unsafe { read_row::<T>(vector, row) }))
        })
        .collect()
}

/// Reads rows whose Arrow native type matches the DuckDB physical storage.
fn native_rows<P: ArrowPrimitiveType>(vector: &FlatVector<'_>, rows: &SourceRows) -> PrimitiveArray<P> {
    primitive_array_from_rows::<P::Native, P, _>(vector, rows, |value| value)
}

fn native_array_from_rows<P: ArrowPrimitiveType>(
    vector: &FlatVector<'_>,
    rows: &SourceRows,
) -> Result<Arc<dyn Array>, Box<dyn Error>> {
    Ok(Arc::new(native_rows::<P>(vector, rows)))
}

/// Converts the first `len` rows of a DuckDB vector to an Arrow array.
///
/// Although the entry wrapper is a [`FlatVector`], the logical type may be a
/// nested list, map, struct, or fixed-size array and is dispatched recursively.
/// Returns an error if `len` exceeds the wrapper's backing capacity.
pub fn flat_vector_to_arrow_array(vector: &FlatVector<'_>, len: usize) -> Result<Arc<dyn Array>, Box<dyn Error>> {
    if len > vector.capacity() {
        return Err(format!(
            "DuckDB vector length {len} exceeds backing capacity {}",
            vector.capacity()
        )
        .into());
    }
    let rows = (0..len).map(Some).collect();
    // SAFETY: `vector` keeps its pointer live, and the length check above
    // bounds every selected row by its backing capacity.
    unsafe { raw_vector_to_arrow_array(vector.ptr(), &rows) }
}

fn scalar_vector_rows_to_arrow_array(
    vector: &FlatVector<'_>,
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
            native_rows::<TimestampMicrosecondType>(vector, rows).with_timezone("UTC"),
        )),
        LogicalTypeId::Varchar => {
            let values = rows
                .iter()
                .map(|source_row| {
                    source_row.map(|row| {
                        // SAFETY: the logical type establishes `duckdb_string_t`
                        // physical storage, and validity masking already dropped
                        // null rows, so each selected slot is initialized.
                        let mut ptr = unsafe { read_row::<duckdb_string_t>(vector, row) };
                        DuckString::new(&mut ptr).as_str().to_string()
                    })
                })
                .collect::<Vec<_>>();

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
                .map(|source_row| source_row.map(|row| unsafe { read_row::<u8>(vector, row) } != 0));

            Ok(Arc::new(BooleanArray::from_iter(values)))
        }
        LogicalTypeId::Float => native_array_from_rows::<Float32Type>(vector, rows),
        LogicalTypeId::Double => native_array_from_rows::<Float64Type>(vector, rows),
        LogicalTypeId::Date => Ok(Arc::new(primitive_array_from_rows::<duckdb_date, Date32Type, _>(
            vector,
            rows,
            |value| value.days,
        ))),
        LogicalTypeId::Time => Ok(Arc::new(primitive_array_from_rows::<
            duckdb_time,
            Time64MicrosecondType,
            _,
        >(vector, rows, |value| value.micros))),
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
                        let mut ptr = unsafe { read_row::<duckdb_string_t>(vector, row) };
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

/// Converts selected rows from a borrowed DuckDB vector.
///
/// Masks `rows` by the vector's validity before dispatching, so the per-type
/// converters receive pre-masked rows and only handle offsets and children.
///
/// # Safety
///
/// `ptr` must remain valid for this call, have the logical type reported by
/// DuckDB, and have backing capacity through the highest source row. Nested
/// list children can legitimately have capacities larger than
/// `duckdb_vector_size()`; `rows` carries that effective child span.
unsafe fn raw_vector_to_arrow_array(ptr: duckdb_vector, rows: &SourceRows) -> Result<ArrayRef, Box<dyn Error>> {
    let capacity = source_len(rows)?;
    // SAFETY: `capacity` is one past the highest selected row, and validity is
    // a vector-level property shared by every logical type.
    let vector = unsafe { FlatVector::with_capacity(ptr, capacity) };
    let rows = masked_rows(rows, |row| vector.try_row_is_null(row as u64))?;
    // SAFETY: callers pass a live vector pointer owned by the parent chunk or
    // nested vector, and DuckDB returns a newly owned logical type handle.
    let logical_type = unsafe { LogicalTypeHandle::new(duckdb_vector_get_column_type(ptr)) };
    match logical_type.id() {
        LogicalTypeId::List => {
            // SAFETY: `capacity` is one past the highest selected list row.
            let vector = unsafe { ListVector::from_raw_with_capacity(ptr, capacity) };
            list_vector_to_arrow_array(&vector, &rows)
        }
        LogicalTypeId::Struct => {
            // SAFETY: `capacity` is one past the highest selected struct row.
            let vector = unsafe { StructVector::from_raw_with_capacity(ptr, capacity) };
            struct_vector_to_arrow_array(&vector, &rows)
        }
        LogicalTypeId::Map => {
            // SAFETY: maps use list storage and `capacity` bounds their rows.
            let vector = unsafe { ListVector::from_raw_with_capacity(ptr, capacity) };
            map_vector_to_arrow_array(&vector, &rows)
        }
        LogicalTypeId::Array => {
            // SAFETY: `capacity` is one past the highest selected array row.
            let vector = unsafe { ArrayVector::from_raw_with_capacity(ptr, capacity) };
            array_vector_to_arrow_array(&vector, &rows)
        }
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
    vector: &ListVector<'_>,
    rows: &SourceRows,
) -> Result<(OffsetBuffer<i32>, Vec<SourceRow>), Box<dyn Error>> {
    let child_len = vector.len();
    let mut offsets = Vec::with_capacity(rows.len() + 1);
    let mut child_rows = Vec::new();
    let mut next_offset = 0;
    offsets.push(next_offset);

    for source_row in rows.iter().copied() {
        if let Some(source_row) = source_row {
            let (offset, length) = vector.try_get_entry(source_row)?;
            if length > 0 {
                let end = offset
                    .checked_add(length)
                    .ok_or_else(|| format!("DuckDB list entry at row {source_row} overflows usize"))?;
                if end > child_len {
                    return Err(format!(
                        "DuckDB list entry at row {source_row} ends at {end}, beyond child length {child_len}"
                    )
                    .into());
                }
                next_offset = checked_list_output_len(child_rows.len(), length)?;
                child_rows.extend((offset..end).map(Some));
            }
        }

        offsets.push(next_offset);
    }

    Ok((OffsetBuffer::new(ScalarBuffer::from(offsets)), child_rows))
}

fn list_vector_to_arrow_array(vector: &ListVector<'_>, rows: &SourceRows) -> Result<ArrayRef, Box<dyn Error>> {
    let (offsets, child_rows) = list_layout(vector, rows)?;
    // SAFETY: the child pointer remains owned by `vector`, and `list_layout`
    // bounded every selected child row by the committed, reserved child span.
    let values = unsafe { raw_vector_to_arrow_array(vector.child_ptr(), &child_rows) }?;
    let field = Arc::new(Field::new("item", values.data_type().clone(), true));
    let nulls = null_buffer(rows);

    Ok(Arc::new(ListArray::try_new(field, offsets, values, nulls)?))
}

fn map_vector_to_arrow_array(vector: &ListVector<'_>, rows: &SourceRows) -> Result<ArrayRef, Box<dyn Error>> {
    let (offsets, child_rows) = list_layout(vector, rows)?;
    let entries_len = child_rows.len();
    // SAFETY: the map child is a struct with one row per list child entry, and
    // safe list size commits reserve that full span.
    let entries_vector = unsafe { StructVector::from_raw_with_capacity(vector.child_ptr(), vector.len()) };

    for source_row in child_rows.iter().flatten().copied() {
        if entries_vector.try_row_is_null(source_row as u64)? {
            return Err(format!("DuckDB map entry at child row {source_row} is null").into());
        }
    }

    // SAFETY: map entry children share the entry struct's reserved row span,
    // and `child_rows` was bounded by the map's committed child length.
    let keys = unsafe { raw_vector_to_arrow_array(entries_vector.child_ptr(0), &child_rows) }?;
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
    // SAFETY: the same entry span and selected rows apply to the value child.
    let values = unsafe { raw_vector_to_arrow_array(entries_vector.child_ptr(1), &child_rows) }?;
    let fields = Fields::from(vec![
        Field::new("keys", keys.data_type().clone(), false),
        Field::new("values", values.data_type().clone(), true),
    ]);
    let entries = StructArray::try_new_with_length(fields, vec![keys, values], None, entries_len)?;
    let field = Arc::new(Field::new("entries", entries.data_type().clone(), false));
    let nulls = null_buffer(rows);

    Ok(Arc::new(MapArray::try_new(field, offsets, entries, nulls, false)?))
}

fn array_vector_to_arrow_array(vector: &ArrayVector<'_>, rows: &SourceRows) -> Result<ArrayRef, Box<dyn Error>> {
    let array_size = usize::try_from(vector.get_array_size())
        .map_err(|_| "DuckDB array size exceeds usize range for Arrow conversion")?;
    if array_size == 0 {
        return Err("DuckDB array size must be greater than zero for Arrow conversion".into());
    }
    let value_length =
        i32::try_from(array_size).map_err(|_| "DuckDB array size exceeds Arrow i32 value length range")?;
    let child_rows = fixed_size_rows(rows, array_size)?;

    // SAFETY: fixed-array child positions are derived with checked arithmetic
    // from source rows already bounded by the parent vector capacity.
    let values = unsafe { raw_vector_to_arrow_array(vector.child_ptr(), &child_rows) }?;
    let field = Arc::new(Field::new("item", values.data_type().clone(), true));
    let nulls = null_buffer(rows);

    Ok(Arc::new(FixedSizeListArray::try_new(
        field,
        value_length,
        values,
        nulls,
    )?))
}

fn struct_vector_to_arrow_array(vector: &StructVector<'_>, rows: &SourceRows) -> Result<ArrayRef, Box<dyn Error>> {
    let logical_type = vector.logical_type();
    let mut fields = Vec::with_capacity(logical_type.num_children());
    let mut arrays = Vec::with_capacity(logical_type.num_children());

    for idx in 0..logical_type.num_children() {
        // SAFETY: struct children share the parent row allocation, and `rows`
        // contains only rows bounded by the parent capacity.
        let array = unsafe { raw_vector_to_arrow_array(vector.child_ptr(idx), rows) }?;
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
/// display timezone.
pub fn data_chunk_to_arrow(chunk: &DataChunkHandle) -> Result<RecordBatch, Box<dyn Error>> {
    let len = chunk.len();

    let columns = (0..chunk.num_columns())
        .map(|i| {
            let vector = chunk.flat_vector(i);
            flat_vector_to_arrow_array(&vector, len)
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

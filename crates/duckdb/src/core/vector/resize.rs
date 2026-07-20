use crate::{
    Result,
    core::{LogicalTypeHandle, LogicalTypeId},
    error::duckdb_failure_from_message,
    ffi::{duckdb_array_type_array_size, duckdb_list_entry, duckdb_string_t},
};

/// Mirrors DuckDB's `DConstants::MAX_VECTOR_SIZE`. DuckDB intentionally uses
/// the same numeric ceiling for list child row counts in
/// `common/types/vector_buffer.cpp` and physical buffer bytes in
/// `common/types/vector.cpp`. Its C list-reserve wrapper does not catch either
/// C++ exception, so both checks must happen before FFI.
pub(super) const MAX_VECTOR_SIZE: u64 = 1 << 37;

/// Returns the largest physical data-buffer width reached by DuckDB resize.
///
/// Keep this in sync with `LogicalType::GetInternalType` in
/// `duckdb-sources/src/common/types.cpp` and `Vector::FindResizeInfos` plus
/// `Vector::Resize` in `duckdb-sources/src/common/types/vector.cpp`.
pub(super) fn max_resize_data_width(logical_type: &LogicalTypeHandle) -> Result<usize> {
    let width = match logical_type.id() {
        LogicalTypeId::Boolean | LogicalTypeId::Tinyint | LogicalTypeId::UTinyint => 1,
        LogicalTypeId::Smallint | LogicalTypeId::USmallint => 2,
        LogicalTypeId::Integer
        | LogicalTypeId::UInteger
        | LogicalTypeId::Float
        | LogicalTypeId::Date
        | LogicalTypeId::SqlNull => 4,
        LogicalTypeId::Bigint
        | LogicalTypeId::UBigint
        | LogicalTypeId::Double
        | LogicalTypeId::Timestamp
        | LogicalTypeId::TimestampS
        | LogicalTypeId::TimestampMs
        | LogicalTypeId::TimestampNs
        | LogicalTypeId::TimestampTZ
        | LogicalTypeId::Time
        | LogicalTypeId::TimeNs
        | LogicalTypeId::TimeTZ => 8,
        LogicalTypeId::Hugeint | LogicalTypeId::UHugeint | LogicalTypeId::Uuid | LogicalTypeId::Interval => 16,
        LogicalTypeId::Varchar
        | LogicalTypeId::Blob
        | LogicalTypeId::Bit
        | LogicalTypeId::Bignum
        | LogicalTypeId::Geometry => std::mem::size_of::<duckdb_string_t>(),
        LogicalTypeId::Decimal => match logical_type.decimal_width() {
            0..=4 => 2,
            5..=9 => 4,
            10..=18 => 8,
            _ => 16,
        },
        LogicalTypeId::Enum => {
            let internal = unsafe { crate::ffi::duckdb_enum_internal_type(logical_type.ptr) };
            match LogicalTypeId::from(internal) {
                LogicalTypeId::UTinyint => 1,
                LogicalTypeId::USmallint => 2,
                LogicalTypeId::UInteger => 4,
                other => {
                    return Err(duckdb_failure_from_message(format!(
                        "DuckDB returned unsupported enum physical type {other:?}"
                    )));
                }
            }
        }
        LogicalTypeId::List | LogicalTypeId::Map => std::mem::size_of::<duckdb_list_entry>(),
        LogicalTypeId::Struct | LogicalTypeId::Union | LogicalTypeId::Variant => {
            max_struct_resize_data_width(logical_type)?
        }
        LogicalTypeId::Array => {
            let child_width = max_resize_data_width(&logical_type.child(0))?;
            let array_size = unsafe { duckdb_array_type_array_size(logical_type.ptr) };
            let array_size = usize::try_from(array_size)
                .map_err(|_| duckdb_failure_from_message("DuckDB array size exceeds usize range"))?;
            child_width
                .checked_mul(array_size)
                .ok_or_else(|| duckdb_failure_from_message("array resize width overflows usize"))?
        }
        unsupported => {
            return Err(duckdb_failure_from_message(format!(
                "cannot preflight DuckDB vector resize for logical type {unsupported:?}"
            )));
        }
    };
    Ok(width)
}

fn max_struct_resize_data_width(logical_type: &LogicalTypeHandle) -> Result<usize> {
    // This intentionally uses DuckDB's physical struct API instead of
    // `LogicalTypeHandle::num_children`/`child`: UNION and VARIANT resize their
    // hidden tag child as part of the physical struct layout.
    let count = unsafe { crate::ffi::duckdb_struct_type_child_count(logical_type.ptr) };
    let mut width = 0;
    for index in 0..count {
        let child_ptr = unsafe { crate::ffi::duckdb_struct_type_child_type(logical_type.ptr, index) };
        if child_ptr.is_null() {
            return Err(duckdb_failure_from_message(format!(
                "DuckDB returned a null physical struct child type at index {index}"
            )));
        }
        let child = unsafe { LogicalTypeHandle::new(child_ptr) };
        width = width.max(max_resize_data_width(&child)?);
    }
    Ok(width)
}

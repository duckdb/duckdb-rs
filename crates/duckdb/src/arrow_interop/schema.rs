use arrow::datatypes::*;

use crate::{
    core::{LogicalTypeHandle, LogicalTypeId},
    types::Decimal,
};

use super::{UUID_BYTE_WIDTH, UUID_EXTENSION_NAME};

/// Convert Arrow [`DataType`] to DuckDB type id.
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
        DataType::Decimal32(_, _) | DataType::Decimal64(_, _) | DataType::Decimal128(_, _) => Decimal,
        // Decimal256 binds as DOUBLE, but primitive_array_to_vector rejects it
        // at execution because the conversion layer has no Decimal256 write path.
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

/// Convert an Arrow [`DataType`] to a DuckDB logical type.
///
/// Nested child fields are converted with [`to_duckdb_logical_type_for_field`],
/// so extension metadata on nested fields is honored.
pub fn to_duckdb_logical_type(data_type: &DataType) -> Result<LogicalTypeHandle, Box<dyn std::error::Error>> {
    match data_type {
        DataType::Dictionary(_, value_type) => to_duckdb_logical_type(value_type),
        DataType::Struct(fields) => {
            let mut shape = vec![];
            for field in fields.iter() {
                shape.push((field.name().as_str(), to_duckdb_logical_type_for_field(field)?));
            }
            Ok(LogicalTypeHandle::struct_type(shape.as_slice()))
        }
        DataType::List(child) | DataType::LargeList(child) => {
            Ok(LogicalTypeHandle::list(&to_duckdb_logical_type_for_field(child)?))
        }
        DataType::FixedSizeList(child, array_size) => Ok(LogicalTypeHandle::array(
            &to_duckdb_logical_type_for_field(child)?,
            *array_size as u64,
        )),
        DataType::Decimal32(width, scale) => to_duckdb_decimal_logical_type::<Decimal32Type>(*width, *scale),
        DataType::Decimal64(width, scale) => to_duckdb_decimal_logical_type::<Decimal64Type>(*width, *scale),
        DataType::Decimal128(width, scale) => to_duckdb_decimal_logical_type::<Decimal128Type>(*width, *scale),
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

/// Convert an Arrow field to a DuckDB logical type.
///
/// This preserves field-local metadata while recursing through Arrow nested
/// types. Recognized extension types are mapped to their DuckDB logical type;
/// recognized extensions with invalid storage return an error. Unknown
/// extension types fall back to their Arrow storage type.
pub fn to_duckdb_logical_type_for_field(field: &Field) -> Result<LogicalTypeHandle, Box<dyn std::error::Error>> {
    match field.extension_type_name() {
        Some(UUID_EXTENSION_NAME) => arrow_uuid_logical_type(field),
        _ => to_duckdb_logical_type(field.data_type()),
    }
}

fn arrow_uuid_logical_type(field: &Field) -> Result<LogicalTypeHandle, Box<dyn std::error::Error>> {
    match field.data_type() {
        DataType::FixedSizeBinary(length) if *length == UUID_BYTE_WIDTH => {
            Ok(LogicalTypeHandle::from(LogicalTypeId::Uuid))
        }
        data_type => Err(invalid_uuid_storage_error(data_type)),
    }
}

pub(super) fn invalid_uuid_storage_error(got: &DataType) -> Box<dyn std::error::Error> {
    format!("{UUID_EXTENSION_NAME} requires FixedSizeBinary({UUID_BYTE_WIDTH}), got {got}").into()
}

fn to_duckdb_decimal_logical_type<T>(width: u8, scale: i8) -> Result<LogicalTypeHandle, Box<dyn std::error::Error>>
where
    T: DecimalType,
{
    let scale = validate_arrow_decimal_metadata::<T>(width, scale)?;
    Ok(LogicalTypeHandle::decimal(width, scale))
}

pub(super) fn validate_arrow_decimal_metadata<T>(width: u8, scale: i8) -> Result<u8, Box<dyn std::error::Error>>
where
    T: DecimalType,
{
    let data_type = T::TYPE_CONSTRUCTOR(width, scale);
    if width > T::MAX_PRECISION {
        return Err(format!(
            "Unsupported data type: {data_type}, decimal width {width} exceeds {}",
            T::MAX_PRECISION
        )
        .into());
    }
    Decimal::validate_signed_scale(width, scale)
        .map_err(|err| format!("Unsupported data type: {data_type}, invalid decimal type: {err}").into())
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

    let key_type = to_duckdb_logical_type_for_field(key_field)?;
    let value_type = to_duckdb_logical_type_for_field(value_field)?;

    Ok(LogicalTypeHandle::map(&key_type, &value_type))
}

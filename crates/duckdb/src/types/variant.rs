use arrow::{
    array::{Array, ArrayRef, AsArray, StructArray},
    datatypes::{DataType, Field},
};
use parquet_variant::Variant;

use super::{Decimal, OrderedMap, TimeUnit, Value, extension::VARIANT_EXTENSION_NAME};

/// DuckDB exports VARIANT as an `arrow.parquet.variant` struct of `metadata`
/// and `value` binaries.
pub(crate) fn is_variant_field(field: &Field) -> bool {
    field.extension_type_name() == Some(VARIANT_EXTENSION_NAME)
}

pub(crate) fn variant_to_value(array: &StructArray, row: usize) -> Value {
    let metadata = binary_value(array.column_by_name("metadata").expect("VARIANT has metadata"), row);
    let value = binary_value(array.column_by_name("value").expect("VARIANT has value"), row);
    // DuckDB writes object fields in declaration order rather than the sorted
    // order the Parquet spec requires, which full validation rejects.
    to_value(&Variant::new(metadata, value))
}

fn binary_value(array: &ArrayRef, row: usize) -> &[u8] {
    match array.data_type() {
        DataType::Binary => array.as_binary::<i32>().value(row),
        DataType::LargeBinary => array.as_binary::<i64>().value(row),
        DataType::BinaryView => array.as_binary_view().value(row),
        other => panic!("VARIANT child must be binary, got {other}"),
    }
}

fn to_value(variant: &Variant<'_, '_>) -> Value {
    match variant {
        Variant::Null => Value::Null,
        Variant::BooleanTrue => Value::Boolean(true),
        Variant::BooleanFalse => Value::Boolean(false),
        Variant::Int8(v) => Value::TinyInt(*v),
        Variant::Int16(v) => Value::SmallInt(*v),
        Variant::Int32(v) => Value::Int(*v),
        Variant::Int64(v) => Value::BigInt(*v),
        Variant::Float(v) => Value::Float(*v),
        Variant::Double(v) => Value::Double(*v),
        Variant::Decimal4(d) => decimal(9, d.scale(), d.integer().into()),
        Variant::Decimal8(d) => decimal(18, d.scale(), d.integer().into()),
        Variant::Decimal16(d) => decimal(Decimal::MAX_WIDTH, d.scale(), d.integer()),
        Variant::Date(d) => Value::Date32(d.to_epoch_days()),
        Variant::Time(t) => Value::Time64(
            TimeUnit::Microsecond,
            t.signed_duration_since(Default::default())
                .num_microseconds()
                .expect("a time of day fits in i64 microseconds"),
        ),
        Variant::TimestampMicros(ts) => Value::Timestamp(TimeUnit::Microsecond, ts.timestamp_micros()),
        Variant::TimestampNtzMicros(ts) => Value::Timestamp(TimeUnit::Microsecond, ts.and_utc().timestamp_micros()),
        Variant::TimestampNanos(ts) => Value::Timestamp(TimeUnit::Nanosecond, nanos(ts.timestamp_nanos_opt())),
        Variant::TimestampNtzNanos(ts) => {
            Value::Timestamp(TimeUnit::Nanosecond, nanos(ts.and_utc().timestamp_nanos_opt()))
        }
        Variant::Uuid(uuid) => Value::Text(uuid.to_string()),
        Variant::String(s) => Value::Text(s.to_string()),
        Variant::ShortString(s) => Value::Text(s.as_str().to_string()),
        Variant::Binary(b) => Value::Blob(b.to_vec()),
        Variant::Object(object) => Value::Struct(OrderedMap::from(
            object
                .iter()
                .map(|(name, field)| (name.to_string(), to_value(&field)))
                .collect::<Vec<_>>(),
        )),
        Variant::List(list) => Value::List(list.iter().map(|item| to_value(&item)).collect()),
    }
}

fn decimal(width: u8, scale: u8, value: i128) -> Value {
    Value::Decimal(Decimal::new(width, scale, value).expect("VARIANT decimals fit their width"))
}

fn nanos(value: Option<i64>) -> i64 {
    value.expect("VARIANT nanosecond timestamps fit in i64")
}

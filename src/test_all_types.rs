use pretty_assertions::assert_eq;
use rust_decimal::Decimal;

use crate::{
    types::{TimeUnit, ValueRef},
    Connection,
};

#[test]
fn test_all_types() -> crate::Result<()> {
    let database = Connection::open_in_memory()?;

    let excluded = vec![
        // uhugeint, time_tz, and dec38_10 aren't supported in the duckdb arrow layer
        "uhugeint",
        "time_tz",
        "dec38_10",
        // union is currently blocked by https://github.com/duckdb/duckdb/pull/11326
        "union",
        // these remaining types are not yet supported by duckdb-rs
        "interval",
        "small_enum",
        "medium_enum",
        "large_enum",
        "int_array",
        "double_array",
        "date_array",
        "timestamp_array",
        "timestamptz_array",
        "varchar_array",
        "nested_int_array",
        "struct",
        "struct_of_arrays",
        "array_of_structs",
        "map",
        "fixed_int_array",
        "fixed_varchar_array",
        "fixed_nested_int_array",
        "fixed_nested_varchar_array",
        "fixed_struct_array",
        "struct_of_fixed_array",
        "fixed_array_of_int_list",
        "list_of_fixed_int_array",
    ];

    let mut binding = database.prepare(&format!(
        "SELECT * EXCLUDE ({}) FROM test_all_types()",
        excluded
            .iter()
            .map(|s| format!("'{}'", s))
            .collect::<Vec<String>>()
            .join(",")
    ))?;
    let mut rows = binding.query([])?;

    let mut idx = -1;
    while let Some(row) = rows.next()? {
        idx += 1;
        for column in row.stmt.column_names() {
            let value = row.get_ref_unwrap(row.stmt.column_index(&column)?);
            test_single(&mut idx, column, value);
        }
    }

    Ok(())
}

fn test_single(idx: &mut i32, column: String, value: ValueRef) {
    match column.as_str() {
        "bool" => match idx {
            0 => assert_eq!(value, ValueRef::Boolean(false)),
            1 => assert_eq!(value, ValueRef::Boolean(true)),
            _ => assert_eq!(value, ValueRef::Null),
        },
        "tinyint" => match idx {
            0 => assert_eq!(value, ValueRef::TinyInt(-128)),
            1 => assert_eq!(value, ValueRef::TinyInt(127)),
            _ => assert_eq!(value, ValueRef::Null),
        },
        "smallint" => match idx {
            0 => assert_eq!(value, ValueRef::SmallInt(-32768)),
            1 => assert_eq!(value, ValueRef::SmallInt(32767)),
            _ => assert_eq!(value, ValueRef::Null),
        },
        "int" => match idx {
            0 => assert_eq!(value, ValueRef::Int(-2147483648)),
            1 => assert_eq!(value, ValueRef::Int(2147483647)),
            _ => assert_eq!(value, ValueRef::Null),
        },
        "bigint" => match idx {
            0 => assert_eq!(value, ValueRef::BigInt(-9223372036854775808)),
            1 => assert_eq!(value, ValueRef::BigInt(9223372036854775807)),
            _ => assert_eq!(value, ValueRef::Null),
        },
        "hugeint" => match idx {
            0 => assert_eq!(value, ValueRef::HugeInt(-170141183460469231731687303715884105728)),
            1 => assert_eq!(value, ValueRef::HugeInt(170141183460469231731687303715884105727)),
            _ => assert_eq!(value, ValueRef::Null),
        },
        "utinyint" => match idx {
            0 => assert_eq!(value, ValueRef::UTinyInt(0)),
            1 => assert_eq!(value, ValueRef::UTinyInt(255)),
            _ => assert_eq!(value, ValueRef::Null),
        },
        "usmallint" => match idx {
            0 => assert_eq!(value, ValueRef::USmallInt(0)),
            1 => assert_eq!(value, ValueRef::USmallInt(65535)),
            _ => assert_eq!(value, ValueRef::Null),
        },
        "uint" => match idx {
            0 => assert_eq!(value, ValueRef::UInt(0)),
            1 => assert_eq!(value, ValueRef::UInt(4294967295)),
            _ => assert_eq!(value, ValueRef::Null),
        },
        "ubigint" => match idx {
            0 => assert_eq!(value, ValueRef::UBigInt(0)),
            1 => assert_eq!(value, ValueRef::UBigInt(18446744073709551615)),
            _ => assert_eq!(value, ValueRef::Null),
        },
        "uhugeint" => match idx {
            0 => assert_eq!(value, ValueRef::UBigInt(0)),
            1 => assert_eq!(value, ValueRef::UBigInt(18446744073709551615)),
            _ => assert_eq!(value, ValueRef::Null),
        },
        "float" => match idx {
            0 => assert_eq!(value, ValueRef::Float(-3.4028235e38)),
            1 => assert_eq!(value, ValueRef::Float(3.4028235e38)),
            _ => assert_eq!(value, ValueRef::Null),
        },
        "double" => match idx {
            0 => assert_eq!(value, ValueRef::Double(-1.7976931348623157e308)),
            1 => assert_eq!(value, ValueRef::Double(1.7976931348623157e308)),
            _ => assert_eq!(value, ValueRef::Null),
        },
        "decimal" => match idx {
            0 => assert_eq!(value, ValueRef::Decimal(Decimal::from_i128_with_scale(0, 0))),
            1 => assert_eq!(value, ValueRef::Decimal(Decimal::from_i128_with_scale(1, 0))),
            _ => assert_eq!(value, ValueRef::Null),
        },
        "date" => match idx {
            0 => assert_eq!(value, ValueRef::Date32(-2147483646)),
            1 => assert_eq!(value, ValueRef::Date32(2147483646)),
            _ => assert_eq!(value, ValueRef::Null),
        },
        "time" => match idx {
            0 => assert_eq!(value, ValueRef::Time64(TimeUnit::Microsecond, 0)),
            1 => assert_eq!(value, ValueRef::Time64(TimeUnit::Microsecond, 86400000000)),
            _ => assert_eq!(value, ValueRef::Null),
        },
        "timestamp" => match idx {
            0 => assert_eq!(value, ValueRef::Timestamp(TimeUnit::Microsecond, -9223372022400000000)),
            1 => assert_eq!(value, ValueRef::Timestamp(TimeUnit::Microsecond, 9223372036854775806)),
            _ => assert_eq!(value, ValueRef::Null),
        },
        "timestamp_s" => match idx {
            0 => assert_eq!(value, ValueRef::Timestamp(TimeUnit::Second, -9223372022400)),
            1 => assert_eq!(value, ValueRef::Timestamp(TimeUnit::Second, 9223372036854)),
            _ => assert_eq!(value, ValueRef::Null),
        },
        "timestamp_ms" => match idx {
            0 => assert_eq!(value, ValueRef::Timestamp(TimeUnit::Millisecond, -9223372022400000)),
            1 => assert_eq!(value, ValueRef::Timestamp(TimeUnit::Millisecond, 9223372036854775)),
            _ => assert_eq!(value, ValueRef::Null),
        },
        "timestamp_ns" => match idx {
            0 => assert_eq!(value, ValueRef::Timestamp(TimeUnit::Nanosecond, -9223372036854775808)),
            1 => assert_eq!(value, ValueRef::Timestamp(TimeUnit::Nanosecond, 9223372036854775806)),
            _ => assert_eq!(value, ValueRef::Null),
        },
        "timestamp_tz" => match idx {
            0 => assert_eq!(value, ValueRef::Timestamp(TimeUnit::Microsecond, -9223372022400000000)),
            1 => assert_eq!(value, ValueRef::Timestamp(TimeUnit::Microsecond, 9223372036854775806)),
            _ => assert_eq!(value, ValueRef::Null),
        },
        "dec_4_1" => match idx {
            0 => assert_eq!(value, ValueRef::Decimal(Decimal::from_i128_with_scale(-9999, 1))),
            1 => assert_eq!(value, ValueRef::Decimal(Decimal::from_i128_with_scale(9999, 1))),
            _ => assert_eq!(value, ValueRef::Null),
        },
        "dec_9_4" => match idx {
            0 => assert_eq!(value, ValueRef::Decimal(Decimal::from_i128_with_scale(-999999999, 4))),
            1 => assert_eq!(value, ValueRef::Decimal(Decimal::from_i128_with_scale(999999999, 4))),
            _ => assert_eq!(value, ValueRef::Null),
        },
        "dec_18_6" => match idx {
            0 => assert_eq!(
                value,
                ValueRef::Decimal(Decimal::from_i128_with_scale(-999999999999999999, 6))
            ),
            1 => assert_eq!(
                value,
                ValueRef::Decimal(Decimal::from_i128_with_scale(999999999999999999, 6))
            ),
            _ => assert_eq!(value, ValueRef::Null),
        },
        "uuid" => match idx {
            0 => assert_eq!(value, ValueRef::Text("00000000-0000-0000-0000-000000000000".as_bytes())),
            1 => assert_eq!(value, ValueRef::Text("ffffffff-ffff-ffff-ffff-ffffffffffff".as_bytes())),
            _ => assert_eq!(value, ValueRef::Null),
        },
        "varchar" => match idx {
            0 => assert_eq!(value, ValueRef::Text("🦆🦆🦆🦆🦆🦆".as_bytes())),
            1 => assert_eq!(value, ValueRef::Text("goo\0se".as_bytes())),
            _ => assert_eq!(value, ValueRef::Null),
        },
        "blob" => match idx {
            0 => assert_eq!(
                value,
                ValueRef::Blob(&[
                    116, 104, 105, 115, 105, 115, 97, 108, 111, 110, 103, 98, 108, 111, 98, 0, 119, 105, 116, 104, 110,
                    117, 108, 108, 98, 121, 116, 101, 115
                ])
            ),
            1 => assert_eq!(value, ValueRef::Blob(&[0, 0, 0, 97])),
            _ => assert_eq!(value, ValueRef::Null),
        },
        "bit" => match idx {
            0 => assert_eq!(value, ValueRef::Blob(&[1, 145, 46, 42, 215]),),
            1 => assert_eq!(value, ValueRef::Blob(&[3, 245])),
            _ => assert_eq!(value, ValueRef::Null),
        },
        _ => todo!("{column:?}"),
    }
}

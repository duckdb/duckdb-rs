use pretty_assertions::assert_eq;
use rust_decimal::Decimal;

use crate::{
    types::{TimeUnit, Type, Value, ValueRef},
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
        "small_enum",
        "medium_enum",
        "large_enum",
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
            if idx != 2 {
                assert_ne!(value.data_type(), Type::Null);
            }
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
        "int_array" => match idx {
            0 => assert_eq!(value.to_owned(), Value::List(vec![])),
            1 => assert_eq!(
                value.to_owned(),
                Value::List(vec![
                    Value::Int(42),
                    Value::Int(999),
                    Value::Null,
                    Value::Null,
                    Value::Int(-42),
                ])
            ),
            _ => assert_eq!(value, ValueRef::Null),
        },
        "double_array" => match idx {
            0 => assert_eq!(value.to_owned(), Value::List(vec![])),
            1 => {
                let value = value.to_owned();

                if let Value::List(values) = value {
                    assert_eq!(values.len(), 6);
                    assert_eq!(values[0], Value::Double(42.0));
                    assert!(unwrap(&values[1]).is_nan());
                    let val = unwrap(&values[2]);
                    assert!(val.is_infinite() && val.is_sign_positive());
                    let val = unwrap(&values[3]);
                    assert!(val.is_infinite() && val.is_sign_negative());
                    assert_eq!(values[4], Value::Null);
                    assert_eq!(values[5], Value::Double(-42.0));
                }
            }
            _ => assert_eq!(value, ValueRef::Null),
        },
        "date_array" => match idx {
            0 => assert_eq!(value.to_owned(), Value::List(vec![])),
            1 => assert_eq!(
                value.to_owned(),
                Value::List(vec![
                    Value::Date32(0),
                    Value::Date32(2147483647),
                    Value::Date32(-2147483647),
                    Value::Null,
                    Value::Date32(19124),
                ])
            ),
            _ => assert_eq!(value, ValueRef::Null),
        },
        "timestamp_array" => match idx {
            0 => assert_eq!(value.to_owned(), Value::List(vec![])),
            1 => assert_eq!(
                value.to_owned(),
                Value::List(vec![
                    Value::Timestamp(TimeUnit::Microsecond, 0,),
                    Value::Timestamp(TimeUnit::Microsecond, 9223372036854775807,),
                    Value::Timestamp(TimeUnit::Microsecond, -9223372036854775807,),
                    Value::Null,
                    Value::Timestamp(TimeUnit::Microsecond, 1652372625000000,),
                ],)
            ),
            _ => assert_eq!(value, ValueRef::Null),
        },
        "timestamptz_array" => match idx {
            0 => assert_eq!(value.to_owned(), Value::List(vec![])),
            1 => assert_eq!(
                value.to_owned(),
                Value::List(vec![
                    Value::Timestamp(TimeUnit::Microsecond, 0,),
                    Value::Timestamp(TimeUnit::Microsecond, 9223372036854775807,),
                    Value::Timestamp(TimeUnit::Microsecond, -9223372036854775807,),
                    Value::Null,
                    Value::Timestamp(TimeUnit::Microsecond, 1652397825000000,),
                ])
            ),
            _ => assert_eq!(value, ValueRef::Null),
        },
        "varchar_array" => match idx {
            0 => assert_eq!(value.to_owned(), Value::List(vec![])),
            1 => assert_eq!(
                value.to_owned(),
                Value::List(vec![
                    Value::Text("🦆🦆🦆🦆🦆🦆".to_string()),
                    Value::Text("goose".to_string()),
                    Value::Null,
                    Value::Text("".to_string()),
                ])
            ),
            _ => assert_eq!(value, ValueRef::Null),
        },
        "nested_int_array" => match idx {
            0 => assert_eq!(value.to_owned(), Value::List(vec![])),
            1 => {
                assert_eq!(
                    value.to_owned(),
                    Value::List(vec![
                        Value::List(vec![],),
                        Value::List(vec![
                            Value::Int(42,),
                            Value::Int(999,),
                            Value::Null,
                            Value::Null,
                            Value::Int(-42,),
                        ],),
                        Value::Null,
                        Value::List(vec![],),
                        Value::List(vec![
                            Value::Int(42,),
                            Value::Int(999,),
                            Value::Null,
                            Value::Null,
                            Value::Int(-42,),
                        ],),
                    ],)
                )
            }
            _ => assert_eq!(value, ValueRef::Null),
        },
        "bit" => match idx {
            0 => assert_eq!(value, ValueRef::Blob(&[1, 145, 46, 42, 215]),),
            1 => assert_eq!(value, ValueRef::Blob(&[3, 245])),
            _ => assert_eq!(value, ValueRef::Null),
        },
        "interval" => match idx {
            0 => assert_eq!(
                value,
                ValueRef::Interval {
                    months: 0,
                    days: 0,
                    nanos: 0
                }
            ),
            1 => assert_eq!(
                value,
                ValueRef::Interval {
                    months: 999,
                    days: 999,
                    nanos: 999999999000
                }
            ),
            _ => assert_eq!(value, ValueRef::Null),
        },
        _ => todo!("{column:?}"),
    }
}

fn unwrap(value: &Value) -> f64 {
    if let Value::Double(val) = value {
        *val
    } else {
        panic!();
    }
}

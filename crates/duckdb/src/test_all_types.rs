use pretty_assertions::assert_eq;
use rust_decimal::Decimal;

use crate::{
    types::{OrderedMap, TimeUnit, Type, Value, ValueRef},
    Connection,
};

#[test]
fn test_all_types() -> crate::Result<()> {
    test_with_database(&Connection::open_in_memory()?)
}

#[test]
fn test_large_arrow_types() -> crate::Result<()> {
    let cfg = crate::Config::default().with("arrow_large_buffer_size", "true")?;
    let database = Connection::open_in_memory_with_flags(cfg)?;

    test_with_database(&database)
}

fn test_with_database(database: &Connection) -> crate::Result<()> {
    // uhugeint, time_tz, and dec38_10 aren't supported in the duckdb arrow layer
    // union is currently blocked by https://github.com/duckdb/duckdb/pull/11326
    let excluded = ["uhugeint", "time_tz", "dec38_10", "union", "varint"];

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
                assert_ne!(value.data_type(), Type::Null, "column {column} is null: {value:?}");
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
            0 => assert_eq!(value, ValueRef::Timestamp(TimeUnit::Nanosecond, -9223286400000000000)),
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
        "float_array" => match idx {
            0 => assert_eq!(value.to_owned(), Value::List(vec![])),
            1 => assert_eq!(
                value.to_owned(),
                Value::List(vec![Value::Float(1.0), Value::Float(2.0)])
            ),
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
        "struct" => match idx {
            0 => assert_eq!(
                value.to_owned(),
                Value::Struct(OrderedMap::from(vec![
                    ("a".to_string(), Value::Null),
                    ("b".to_string(), Value::Null),
                ]))
            ),
            1 => assert_eq!(
                value.to_owned(),
                Value::Struct(OrderedMap::from(vec![
                    ("a".to_string(), Value::Int(42)),
                    ("b".to_string(), Value::Text("🦆🦆🦆🦆🦆🦆".to_string())),
                ]))
            ),
            _ => assert_eq!(value, ValueRef::Null),
        },
        "struct_of_arrays" => match idx {
            0 => assert_eq!(
                value.to_owned(),
                Value::Struct(OrderedMap::from(vec![
                    ("a".to_string(), Value::Null),
                    ("b".to_string(), Value::Null),
                ]))
            ),
            1 => assert_eq!(
                value.to_owned(),
                Value::Struct(OrderedMap::from(vec![
                    (
                        "a".to_string(),
                        Value::List(vec![
                            Value::Int(42),
                            Value::Int(999),
                            Value::Null,
                            Value::Null,
                            Value::Int(-42)
                        ])
                    ),
                    (
                        "b".to_string(),
                        Value::List(vec![
                            Value::Text("🦆🦆🦆🦆🦆🦆".to_string()),
                            Value::Text("goose".to_string()),
                            Value::Null,
                            Value::Text("".to_string()),
                        ]),
                    )
                ]))
            ),
            _ => assert_eq!(value, ValueRef::Null),
        },
        "array_of_structs" => match idx {
            0 => assert_eq!(value.to_owned(), Value::List(vec![])),
            1 => assert_eq!(
                value.to_owned(),
                Value::List(vec![
                    Value::Struct(OrderedMap::from(vec![
                        ("a".to_string(), Value::Null),
                        ("b".to_string(), Value::Null)
                    ])),
                    Value::Struct(OrderedMap::from(vec![
                        ("a".to_string(), Value::Int(42)),
                        ("b".to_string(), Value::Text("🦆🦆🦆🦆🦆🦆".to_string()))
                    ])),
                    Value::Null
                ])
            ),
            _ => assert_eq!(value, ValueRef::Null),
        },
        "map" => match idx {
            0 => assert_eq!(value.to_owned(), Value::Map(OrderedMap::from(vec![]))),
            1 => assert_eq!(
                value.to_owned(),
                Value::Map(OrderedMap::from(vec![
                    (Value::Text("key1".to_string()), Value::Text("🦆🦆🦆🦆🦆🦆".to_string())),
                    (Value::Text("key2".to_string()), Value::Text("goose".to_string())),
                ]))
            ),
            _ => assert_eq!(value, ValueRef::Null),
        },
        "fixed_int_array" => match idx {
            0 => assert_eq!(
                value.to_owned(),
                Value::Array(vec![Value::Null, Value::Int(2), Value::Int(3)])
            ),
            1 => assert_eq!(
                value.to_owned(),
                Value::Array(vec![Value::Int(4), Value::Int(5), Value::Int(6)])
            ),
            _ => assert_eq!(value, ValueRef::Null),
        },
        "fixed_varchar_array" => match idx {
            0 => assert_eq!(
                value.to_owned(),
                Value::Array(vec![
                    Value::Text("a".to_string()),
                    Value::Null,
                    Value::Text("c".to_string())
                ])
            ),
            1 => assert_eq!(
                value.to_owned(),
                Value::Array(vec![
                    Value::Text("d".to_string()),
                    Value::Text("e".to_string()),
                    Value::Text("f".to_string())
                ])
            ),
            _ => assert_eq!(value, ValueRef::Null),
        },
        "fixed_nested_int_array" => match idx {
            0 => assert_eq!(
                value.to_owned(),
                Value::Array(vec![
                    Value::Array(vec![Value::Null, Value::Int(2), Value::Int(3)]),
                    Value::Null,
                    Value::Array(vec![Value::Null, Value::Int(2), Value::Int(3)])
                ])
            ),
            1 => assert_eq!(
                value.to_owned(),
                Value::Array(vec![
                    Value::Array(vec![Value::Int(4), Value::Int(5), Value::Int(6)]),
                    Value::Array(vec![Value::Null, Value::Int(2), Value::Int(3)]),
                    Value::Array(vec![Value::Int(4), Value::Int(5), Value::Int(6)]),
                ])
            ),
            _ => assert_eq!(value, ValueRef::Null),
        },
        "fixed_nested_varchar_array" => match idx {
            0 => assert_eq!(
                value.to_owned(),
                Value::Array(vec![
                    Value::Array(vec![
                        Value::Text("a".to_string()),
                        Value::Null,
                        Value::Text("c".to_string())
                    ]),
                    Value::Null,
                    Value::Array(vec![
                        Value::Text("a".to_string()),
                        Value::Null,
                        Value::Text("c".to_string())
                    ])
                ])
            ),
            1 => assert_eq!(
                value.to_owned(),
                Value::Array(vec![
                    Value::Array(vec![
                        Value::Text("d".to_string()),
                        Value::Text("e".to_string()),
                        Value::Text("f".to_string())
                    ]),
                    Value::Array(vec![
                        Value::Text("a".to_string()),
                        Value::Null,
                        Value::Text("c".to_string())
                    ]),
                    Value::Array(vec![
                        Value::Text("d".to_string()),
                        Value::Text("e".to_string()),
                        Value::Text("f".to_string())
                    ]),
                ])
            ),
            _ => assert_eq!(value, ValueRef::Null),
        },
        "fixed_struct_array" => match idx {
            0 => assert_eq!(
                value.to_owned(),
                Value::Array(vec![
                    Value::Struct(OrderedMap::from(vec![
                        ("a".to_string(), Value::Null),
                        ("b".to_string(), Value::Null)
                    ])),
                    Value::Struct(OrderedMap::from(vec![
                        ("a".to_string(), Value::Int(42)),
                        ("b".to_string(), Value::Text("🦆🦆🦆🦆🦆🦆".to_string()))
                    ])),
                    Value::Struct(OrderedMap::from(vec![
                        ("a".to_string(), Value::Null),
                        ("b".to_string(), Value::Null)
                    ])),
                ])
            ),
            1 => assert_eq!(
                value.to_owned(),
                Value::Array(vec![
                    Value::Struct(OrderedMap::from(vec![
                        ("a".to_string(), Value::Int(42)),
                        ("b".to_string(), Value::Text("🦆🦆🦆🦆🦆🦆".to_string()))
                    ])),
                    Value::Struct(OrderedMap::from(vec![
                        ("a".to_string(), Value::Null),
                        ("b".to_string(), Value::Null)
                    ])),
                    Value::Struct(OrderedMap::from(vec![
                        ("a".to_string(), Value::Int(42)),
                        ("b".to_string(), Value::Text("🦆🦆🦆🦆🦆🦆".to_string()))
                    ])),
                ])
            ),
            _ => assert_eq!(value, ValueRef::Null),
        },
        "struct_of_fixed_array" => match idx {
            0 => assert_eq!(
                value.to_owned(),
                Value::Struct(OrderedMap::from(vec![
                    (
                        "a".to_string(),
                        Value::Array(vec![Value::Null, Value::Int(2), Value::Int(3)])
                    ),
                    (
                        "b".to_string(),
                        Value::Array(vec![
                            Value::Text("a".to_string()),
                            Value::Null,
                            Value::Text("c".to_string())
                        ])
                    ),
                ]))
            ),
            1 => assert_eq!(
                value.to_owned(),
                Value::Struct(OrderedMap::from(vec![
                    (
                        "a".to_string(),
                        Value::Array(vec![Value::Int(4), Value::Int(5), Value::Int(6)]),
                    ),
                    (
                        "b".to_string(),
                        Value::Array(vec![
                            Value::Text("d".to_string()),
                            Value::Text("e".to_string()),
                            Value::Text("f".to_string())
                        ]),
                    )
                ]))
            ),
            _ => assert_eq!(value, ValueRef::Null),
        },
        "fixed_array_of_int_list" => match idx {
            0 => assert_eq!(
                value.to_owned(),
                Value::Array(vec![
                    Value::List(vec![]),
                    Value::List(vec![
                        Value::Int(42),
                        Value::Int(999),
                        Value::Null,
                        Value::Null,
                        Value::Int(-42),
                    ]),
                    Value::List(vec![]),
                ])
            ),
            1 => assert_eq!(
                value.to_owned(),
                Value::Array(vec![
                    Value::List(vec![
                        Value::Int(42),
                        Value::Int(999),
                        Value::Null,
                        Value::Null,
                        Value::Int(-42),
                    ]),
                    Value::List(vec![]),
                    Value::List(vec![
                        Value::Int(42),
                        Value::Int(999),
                        Value::Null,
                        Value::Null,
                        Value::Int(-42),
                    ]),
                ])
            ),
            _ => assert_eq!(value, ValueRef::Null),
        },
        "list_of_fixed_int_array" => match idx {
            0 => assert_eq!(
                value.to_owned(),
                Value::List(vec![
                    Value::Array(vec![Value::Null, Value::Int(2), Value::Int(3)]),
                    Value::Array(vec![Value::Int(4), Value::Int(5), Value::Int(6)]),
                    Value::Array(vec![Value::Null, Value::Int(2), Value::Int(3)]),
                ])
            ),
            1 => assert_eq!(
                value.to_owned(),
                Value::List(vec![
                    Value::Array(vec![Value::Int(4), Value::Int(5), Value::Int(6)]),
                    Value::Array(vec![Value::Null, Value::Int(2), Value::Int(3)]),
                    Value::Array(vec![Value::Int(4), Value::Int(5), Value::Int(6)]),
                ])
            ),
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
        "small_enum" => match idx {
            0 => assert_eq!(value.to_owned(), Value::Enum("DUCK_DUCK_ENUM".to_string())),
            1 => assert_eq!(value.to_owned(), Value::Enum("GOOSE".to_string())),
            _ => assert_eq!(value, ValueRef::Null),
        },
        "medium_enum" => match idx {
            0 => assert_eq!(value.to_owned(), Value::Enum("enum_0".to_string())),
            1 => assert_eq!(value.to_owned(), Value::Enum("enum_1".to_string())),
            _ => assert_eq!(value, ValueRef::Null),
        },
        "large_enum" => match idx {
            0 => assert_eq!(value.to_owned(), Value::Enum("enum_0".to_string())),
            1 => assert_eq!(value.to_owned(), Value::Enum("enum_69999".to_string())),
            _ => assert_eq!(value, ValueRef::Null),
        },
        "union" => match idx {
            0 => assert_eq!(
                value.to_owned(),
                Value::Union(Box::new(Value::Text("Frank".to_owned())))
            ),
            1 => assert_eq!(value.to_owned(), Value::Union(Box::new(Value::SmallInt(5)))),
            _ => assert_eq!(value.to_owned(), Value::Union(Box::new(Value::Null))),
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

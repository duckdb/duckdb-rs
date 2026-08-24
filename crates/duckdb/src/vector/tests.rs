use crate::{
    DuckDBType, Parameters, ToValue,
    builder_helpers::scalar_callback,
    connection::FFILink,
    environment::Environment,
    environment::StorageLocation,
    error::DuckDBError,
    logical_type::LogicalType,
    query_result::QueryResultStep,
    types::{
        Any, BigNumValue, BitValue, BlobValue, DateValue, DecimalValue, IntervalValue, MapValue,
        StructSchema, StructValue, TimeNsValue, TimeTzValue, TimeValue, TimestampMsValue,
        TimestampNsValue, TimestampSecValue, TimestampTzNsValue, TimestampTzValue, TimestampValue,
        UnionSchema, UnionValue, UuidValue,
    },
    vector::{
        Array, Decimal, List, MapWrite, StorageKind, Struct, StructWrite, TString, Union,
        UnionWriter, Variant,
    },
};

#[cfg(feature = "capi-v2-p2")]
use crate::{scalar::ScalarFunctionBuilder, signature::SignatureBuilder};

struct TestStruct;

impl StructSchema for TestStruct {
    fn fields<C: FFILink + ?Sized>(link: &C) -> crate::Result<Vec<(&'static str, LogicalType)>> {
        Ok(vec![
            ("key", i32::logical_type(link)?),
            ("value", String::logical_type(link)?),
        ])
    }
}

struct TestUnion;

impl UnionSchema for TestUnion {
    fn members<C: FFILink + ?Sized>(link: &C) -> crate::Result<Vec<(&'static str, LogicalType)>> {
        Ok(vec![
            ("key", i32::logical_type(link)?),
            ("value", String::logical_type(link)?),
        ])
    }
}

scalar_callback!(NegateScalar, i32, |input, output, _ctx, _user_data| {
    let input = input.get_vector_at::<i32>(0)?;
    let mut output = output;
    output.set_size(input.len())?;
    for (i, v) in input.iter()?.enumerate() {
        match v {
            Some(x) => output.write(i, Some(-x))?,
            None => output.write(i, None)?,
        }
    }
    let written: Vec<_> = output.iter()?.map(|value| value.copied()).collect();
    assert_eq!(
        written,
        vec![Some(-1), Some(-2), Some(-3), Some(-4), Some(-5)]
    );
    Ok(())
});

scalar_callback!(UpperScalar, String, |input, output, _ctx, _user_data| {
    let input = input.get_vector_at::<String>(0)?;
    let mut output = output;
    output.set_size(input.len())?;
    for (i, v) in input.iter()?.enumerate() {
        match v {
            Some(s) => output.write(i, Some(&s.to_uppercase()))?,
            None => output.write(i, None)?,
        }
    }
    Ok(())
});

scalar_callback!(
    MapScalar,
    crate::vector::Map<i32, String>,
    |input, result, _ctx, _user_data| {
    let keys = input.get_vector_at::<i32>(0)?;
    let values = input.get_vector_at::<String>(1)?;
    let total_len = keys.len();
    let rows: Vec<_> = keys
        .iter()?
        .zip(values.iter()?)
        .map(|(key, value)| {
            (
                key.copied().expect("key must not be null"),
                value.expect("value must not be null").to_string(),
            )
        })
        .collect();
    let mut result = result;
    result.set_size(total_len)?;
    for (index, (key, value)) in rows.iter().enumerate() {
        result.write(
            index,
            Some(MapWrite {
                entries: vec![(*key, value.as_str())],
            }),
        )?;
    }
    let written: Vec<_> = result.iter()?.collect();
    for (row, (key, value)) in written.into_iter().zip(&rows) {
        assert_eq!(row.as_ref().unwrap().get(key)?, Some(value.as_str()));
    }
    Ok(())
    }
);

scalar_callback!(UnionScalar, Union, |input, output, _ctx, _user_data| {
    let keys = input.get_vector_at::<i32>(0)?;
    let values = input.get_vector_at::<String>(1)?;
    let rows: Vec<(i32, Option<&str>)> = keys
        .iter()?
        .zip(values.iter()?)
        .map(|(key, value)| (*key.unwrap(), value))
        .collect();

    let mut output = output;
    output.set_size(rows.len())?;
    output
        .children
        .iter_mut()
        .for_each(|v| v.set_size(rows.len()).unwrap());

    for (index, (key, value)) in rows.iter().enumerate() {
        match *key {
            1 => output.write(
                index,
                Some(UnionWriter::set_value::<i32>(
                    0u8,
                    value.map_or(None, |v| Some(v.len() as i32)),
                )),
            )?,
            2 => output.write(index, Some(UnionWriter::set_value::<String>(1u8, *value)))?,
            _ => unimplemented!("Unexpected key value: {}", key),
        }
    }
    Ok(())
});

scalar_callback!(StructScalar, Struct, |input, output, _ctx, _user_data| {
    let keys = input.get_vector_at::<i32>(0)?;
    let values = input.get_vector_at::<String>(1)?;
    let rows: Vec<_> = keys
        .iter()?
        .zip(values.iter()?)
        .map(|(key, value)| (*key.unwrap(), value.unwrap().to_string()))
        .collect();
    let mut output = output;
    output.set_size(rows.len())?;
    for (index, (key, value)) in rows.iter().enumerate() {
        output.write(
            index,
            Some(
                StructWrite::new()
                    .field::<i32>(Some(*key))
                    .field::<String>(Some(value)),
            ),
        )?;
    }
    for (row, (key, value)) in output.iter()?.zip(rows) {
        let row = row.unwrap();
        assert_eq!(row.get::<i32>("key")?, Some(&key));
        assert_eq!(row.get::<String>("value")?, Some(value.as_str()));
    }
    Ok(())
});

scalar_callback!(ConstantScalar, i32, |_input, result, ctx, _user_data| {
    let mut result = result;
    let val = 42_i32.value(&ctx)?;
    result.make_constant(val, true, 10)?;
    Ok(())
});

scalar_callback!(SequenceScalar, i32, |input, result, _ctx, _user_data| {
    let mut result = result;
    result.make_sequence(42, 10, input.row_count()?)?;
    Ok(())
});

scalar_callback!(
    CopyStringScalar,
    String,
    |input, result, _ctx, _user_data| {
        let mut vector = input.get_vector_at::<String>(0)?;
        if vector.storage_kind() == StorageKind::Other {
            vector.flatten()?;
        }
        let mut result = result;
        result.set_size(vector.len())?;
        for (i, item) in vector.iter()?.enumerate() {
            result.write(i, item)?;
        }
        Ok(())
    }
);

#[test]
#[cfg(feature = "capi-v2-p2")]
fn test_vector_read_write() -> crate::Result<()> {
    let env = Environment::new().expect("Failed to create environment");
    let db = env
        .open(StorageLocation::InMemory)
        .expect("Failed to open in-memory database");
    let conn = db.connect().expect("Failed to connect to database");

    ScalarFunctionBuilder::new(
        "test",
        SignatureBuilder::new(
            [Parameter::normal("input", i32::logical_type(&conn)?)],
            i32::logical_type(&conn)?,
        ),
        NegateScalar,
    )
    .register_with_connection(&conn)
    .expect("Failed to register scalar function");

    let statements = conn
        .parse("SELECT test(unnest([1, 2, 3, 4, 5]))")
        .expect("Failed to parse query");

    for stmt in statements {
        let stmt = stmt.expect("Failed to get statement");
        let mut result = conn
            .query(stmt, Parameters::None)
            .expect("Failed to execute statement");

        loop {
            match result.step().expect("Failed to step result") {
                QueryResultStep::Chunk(chunk) => {
                    let vector = chunk.get_vector_at::<i32>(0).expect("Failed to get vector");

                    let out: Vec<Option<i32>> = vector.iter()?.map(|x| x.copied()).collect();
                    assert_eq!(out, vec![Some(-1), Some(-2), Some(-3), Some(-4), Some(-5)]);
                }
                QueryResultStep::Waiting => continue,
                QueryResultStep::Canceled => panic!("Query canceled unexpectedly"),
                QueryResultStep::Finished => break,
            }
        }
    }
    Ok(())
}

#[test]
fn test_logical_type_cast() -> crate::Result<()> {
    let env = Environment::new()?;
    let db = env.open(StorageLocation::InMemory)?;
    let conn = db.connect()?;

    let res = conn.query("SELECT 1", Parameters::None)?.next().unwrap()?;

    let vector = res.get_vector_at::<i32>(0)?;

    for item in vector.iter()? {
        assert_eq!(item, Some(&1));
    }
    Ok(())
}

#[test]
#[cfg(feature = "capi-v2-p2")]
fn test_vector_string() -> crate::Result<()> {
    let env = Environment::new()?;
    let db = env.open(StorageLocation::InMemory)?;
    let conn = db.connect()?;

    ScalarFunctionBuilder::new(
        "to_upper",
        SignatureBuilder::new(
            [Parameter::normal("IN", String::logical_type(&conn)?)],
            String::logical_type(&conn)?,
        ),
        UpperScalar,
    )
    .register_with_connection(&conn)?;

    let res = conn
        .query(
            "SELECT to_upper(unnest(['hello', 'world', '123456789012', 'longerthaninline']))",
            Parameters::None,
        )?
        .next()
        .unwrap()?;

    let vector = res.get_vector_at::<String>(0)?;
    let data: Vec<_> = vector.iter()?.collect();

    assert!(data.len() == 4, "Expected 4 rows, got {}", data.len());

    assert_eq!(
        data,
        vec![
            Some("HELLO"),
            Some("WORLD"),
            Some("123456789012"),
            Some("LONGERTHANINLINE"),
        ],
    );
    Ok(())
}

#[test]
#[cfg(feature = "capi-v2-p2")]
fn test_vector_list() -> crate::Result<()> {
    scalar_callback!(
        ListMultScalar,
        List<i32>,
        |input, output, _ctx, _user_data| {
            let input = input.get_vector_at::<List<i32>>(0)?;
            let mut output = output;
            output.set_size(input.len())?;
            for (i, v) in input.iter()?.enumerate() {
                match v {
                    Some(list) => {
                        let new_list: Vec<Option<i32>> =
                            list.iter().map(|x| x.map(|v| v * 2)).collect();
                        output.write(i, Some(new_list))?;
                    }
                    None => output.write(i, None)?,
                }
            }
            Ok(())
        }
    );

    let env = Environment::new()?;
    let db = env.open(StorageLocation::InMemory)?;
    let conn = db.connect()?;

    let list_logical_type = Vec::<Option<i32>>::logical_type(&conn)?;

    ScalarFunctionBuilder::new(
        "list_mult",
        SignatureBuilder::new(
            [Parameter::normal("IN", list_logical_type.clone())],
            list_logical_type,
        ),
        ListMultScalar,
    )
    .register_with_connection(&conn)?;

    let res = conn
        .query(
            "SELECT list_mult(unnest([[1, 2], [3, NULL, 5], [], NULL]))",
            Parameters::None,
        )?
        .next()
        .unwrap()?;

    let vector = res.get_vector_at::<List<i32>>(0)?;

    let items: Vec<_> = vector
        .iter()?
        .map(|r| r.map(|v| v.iter().collect::<Vec<_>>()))
        .collect();

    let expected = [
        Some(vec![Some(&2), Some(&4)]),
        Some(vec![Some(&6), None, Some(&10)]),
        Some(vec![]),
        None,
    ];

    assert_eq!(items, expected);
    Ok(())
}

struct MyStruct {
    key1: Option<String>,
    key2: Option<i32>,
}

#[test]
fn test_vector_struct() -> crate::Result<()> {
    let env = Environment::new()?;
    let db = env.open(StorageLocation::InMemory)?;
    let conn = db.connect()?;

    let res = conn
        .query(
            "SELECT unnest([{'key1': 'value1', 'key2': 42}, {'key1': NULL, 'key2': NULL}])",
            Parameters::None,
        )?
        .next()
        .unwrap()?;

    let vector = res.get_vector_at::<Struct>(0)?;

    let mut reader = vector.iter()?;

    let row = reader.next().unwrap().unwrap();

    let item = MyStruct {
        key1: row.get::<String>("key1")?.map(str::to_string),
        key2: row.get::<i32>("key2")?.copied(),
    };

    assert_eq!(item.key1, Some("value1".to_string()));
    assert_eq!(item.key2, Some(42));

    let row = reader.next().unwrap().unwrap();

    let item = MyStruct {
        key1: row.get::<String>("key1")?.map(str::to_string),
        key2: row.get::<i32>("key2")?.copied(),
    };

    assert_eq!(item.key1, None);
    Ok(())
}

#[test]
pub fn test_vector_array() -> crate::Result<()> {
    let env = Environment::new()?;
    let db = env.open(StorageLocation::InMemory)?;
    let conn = db.connect()?;

    let res = conn
        .query(
            "SELECT unnest([array_value(1, NULL, 3), array_value(3, 4, 5), NULL])",
            Parameters::None,
        )?
        .next()
        .unwrap()?;

    let vector = res.get_vector_at::<Array<i32>>(0)?;
    let mut reader = vector.iter()?;

    let item = reader.next().unwrap().unwrap();
    assert_eq!(item.size(), 3);

    let reader = vector.iter()?;

    let items: Vec<_> = reader
        .map(|r| r.map(|v| v.iter().collect::<Vec<_>>()))
        .collect();

    let expected = [
        Some(vec![Some(&1), None, Some(&3)]),
        Some(vec![Some(&3), Some(&4), Some(&5)]),
        None,
    ];

    assert_eq!(items, expected);
    Ok(())
}

#[test]
pub fn test_vector_union() -> crate::Result<()> {
    let env = Environment::new()?;
    let db = env.open(StorageLocation::InMemory)?;
    let conn = db.connect()?;

    let mut statements = conn.parse(
        "CREATE TABLE tbl1 (u UNION(num INTEGER, str VARCHAR));
        INSERT INTO tbl1 VALUES (1), (42), (NULL), ('two'), (union_value(str := 'three'));
        SELECT u FROM tbl1;",
    )?;

    conn.execute(statements.next().unwrap()?, Parameters::None)?;
    conn.execute(statements.next().unwrap()?, Parameters::None)?;

    let res = conn
        .query(statements.next().unwrap()?, Parameters::None)?
        .next()
        .unwrap()?;

    let vector = res.get_vector_at::<Union>(0)?;
    assert_eq!(vector.len(), 5);
    let mut reader = vector.iter()?;

    assert_eq!(
        reader.next().unwrap().unwrap().member(),
        0,
        "Expected first union member to be 0 (num)"
    );

    assert_eq!(reader.next().unwrap().unwrap().get::<i32>(0)?, Some(&42));

    assert!(reader.next().unwrap().is_none());
    assert_eq!(
        reader.next().unwrap().unwrap().get::<String>(1)?,
        Some("two")
    );

    // check if failed cast errors
    assert_eq!(
        reader
            .next()
            .unwrap()
            .unwrap()
            .get::<i32>(1)
            .unwrap_err()
            .code,
        DuckDBError::DUCKDB_V2_ERROR_INPUT_INVALID
    );
    Ok(())
}

#[test]
pub fn test_vector_map() -> crate::Result<()> {
    let env = Environment::new()?;
    let db = env.open(StorageLocation::InMemory)?;
    let conn = db.connect()?;

    let res = conn
        .query(
            "SELECT unnest([MAP {1: 12.1, 2: 41.2}, MAP {1: 112.1, 2: 141.2}, MAP {1: 12.1, 2: 41.2}]);",
            Parameters::None,
        )?
        .next()
        .unwrap()?;

    assert_eq!(res.vectors_count()?, 1);

    let vector = res.get_vector_at::<crate::vector::Map<i32, Decimal<i16>>>(0)?;
    let mut reader = vector.iter()?;

    assert_eq!(vector.len(), 3);

    let row = reader.next().unwrap().unwrap();

    assert_eq!(row.keys()?, vec![&1, &2]);
    assert_eq!(row.values()?, vec![&121, &412]);

    assert_eq!(row.get(&1)?, Some(&121));
    assert_eq!(row.get(&2)?, Some(&412));

    let row = reader.next().unwrap().unwrap();

    assert_eq!(row.get(&1)?, Some(&1121));
    assert_eq!(row.get(&2)?, Some(&1412));
    Ok(())
}

#[test]
#[cfg(feature = "capi-v2-p2")]
pub fn vector_complex_write() -> crate::Result<()> {
    let env = Environment::new()?;
    let db = env.open(StorageLocation::InMemory)?;
    let conn = db.connect()?;

    let ltype = MapValue::<i32, String>::logical_type(&conn)?;

    ScalarFunctionBuilder::new(
        "to_map",
        SignatureBuilder::new(
            [
                Parameter::normal("key", i32::logical_type(&conn)?),
                Parameter::normal("value", String::logical_type(&conn)?),
            ],
            ltype,
        ),
        MapScalar,
    )
    .register_with_connection(&conn)?;

    let mut statements = conn.parse("SELECT UNNEST([to_map(12, 'AA'), to_map(15, 'BB')]); SELECT to_map(unnest([1,2,3]), unnest(['A', 'B', 'C']))")?;

    let statement = statements.next().unwrap()?;

    let result = conn.query(statement, Parameters::None)?;

    for item in result {
        let item = item?;

        let res = item.get_vector_at::<crate::vector::Map<i32, String>>(0)?;

        assert!(res.len() == 2);
        let mut reader = res.iter()?;

        let item = reader.next().unwrap().unwrap();

        assert_eq!(item.get(&12)?, Some("AA"));

        let item = reader.next().unwrap().unwrap();

        assert_eq!(item.get(&15)?, Some("BB"));
    }

    let statement = statements.next().unwrap()?;

    let mut result = conn.query(statement, Parameters::None)?;

    if let Some(item) = result.next() {
        let item = item?;

        let res = item.get_vector_at::<crate::vector::Map<i32, String>>(0)?;

        assert_eq!(res.len(), 3);
        let expected = [(1, "A"), (2, "B"), (3, "C")];

        for (item, (key, value)) in res.iter()?.zip(expected) {
            assert_eq!(item.unwrap().get(&key)?, Some(value));
        }

        return Ok(());
    }
    Err(Error {
        code: DuckDBError::DUCKDB_V2_ERROR_API,
        message: "Not found".to_string(),
    })
}

#[test]
#[cfg(feature = "capi-v2-p2")]
pub fn vector_union_write() -> crate::Result<()> {
    let env = Environment::new()?;
    let db = env.open(StorageLocation::InMemory)?;
    let conn = db.connect()?;

    let union_type = UnionValue::<TestUnion, i32>::logical_type(&conn)?;

    ScalarFunctionBuilder::new(
        "to_union",
        SignatureBuilder::new(
            [
                Parameter::normal("key", i32::logical_type(&conn)?),
                Parameter::normal("value", String::logical_type(&conn)?),
            ],
            union_type,
        ),
        UnionScalar,
    )
    .register_with_connection(&conn)?;

    let statement = conn
        .parse(
            "SELECT to_union(unnest([1, 2, 2]), unnest(['WWWADWWWAample', 'OPAOPDAOADWADtablesss', NULL]))",
        )?
        .next()
        .unwrap()?;
    for chunk in conn.query(statement, Parameters::None)? {
        let chunk = chunk?;
        let mut vector = chunk.get_vector_at::<Union>(0)?;
        vector.flatten()?;
        let rows: Vec<_> = vector.iter()?.collect();

        assert_eq!(rows[0].as_ref().unwrap().member(), 0);
        assert_eq!(rows[1].as_ref().unwrap().member(), 1);

        assert_eq!(rows[0].as_ref().unwrap().get::<i32>(0)?, Some(&14));
        assert_eq!(
            rows[1].as_ref().unwrap().get::<String>(1)?,
            Some("OPAOPDAOADWADtablesss")
        );

        assert_eq!(rows[2].as_ref().unwrap().member(), 1);
        assert_eq!(rows[2].as_ref().unwrap().get::<String>(1)?, None);
    }
    Ok(())
}

#[test]
#[cfg(feature = "capi-v2-p2")]
pub fn vector_struct_write() -> crate::Result<()> {
    let env = Environment::new()?;
    let db = env.open(StorageLocation::InMemory)?;
    let conn = db.connect()?;

    let struct_type = StructValue::<TestStruct>::logical_type(&conn)?;

    ScalarFunctionBuilder::new(
        "to_struct",
        SignatureBuilder::new(
            [
                Parameter::normal("key", i32::logical_type(&conn)?),
                Parameter::normal("value", String::logical_type(&conn)?),
            ],
            struct_type,
        ),
        StructScalar,
    )
    .register_with_connection(&conn)?;

    let statement = conn
        .parse("SELECT to_struct(unnest([1, 2]), unnest(['A', 'B']))")?
        .next()
        .unwrap()?;
    for chunk in conn.query(statement, Parameters::None)? {
        let chunk = chunk?;
        let vector = chunk.get_vector_at::<Struct>(0)?;
        let rows: Vec<_> = vector.iter()?.collect();
        assert_eq!(rows[0].as_ref().unwrap().get::<i32>("key")?, Some(&1));
        assert_eq!(rows[1].as_ref().unwrap().get::<String>("value")?, Some("B"));
    }
    Ok(())
}

#[test]
pub fn vector_test_bignum() -> crate::Result<()> {
    let env = Environment::new()?;
    let db = env.open(StorageLocation::InMemory)?;
    let conn = db.connect()?;

    let result = conn.query(
        "select unnest([1090812098190281092901::BIGNUM, 1090812098190281092902::BIGNUM, -1090812098190281092903::BIGNUM])",
        Parameters::None,
    )?;

    for item in result {
        let item = item?;

        let res = item.get_vector_at::<crate::vector::BigNum>(0)?;

        assert!(res.len() == 3);
        let reader = res.iter()?;

        let expected = [
            Some("1090812098190281092901"),
            Some("1090812098190281092902"),
            Some("-1090812098190281092903"),
        ];

        for (i, item) in reader.enumerate() {
            let decoded = item.unwrap().decode()?;

            let result = decoded.to_string();

            assert_eq!(Some(result.as_str()), expected[i]);
        }
    }

    Ok(())
}

#[test]
pub fn vector_value_types() -> crate::Result<()> {
    let env = Environment::new()?;
    let db = env.open(StorageLocation::InMemory)?;
    let conn = db.connect()?;

    macro_rules! assert_round_trip {
        ($value:expr, $type:ty) => {{
            let input = $value;
            let mut result = conn.query("SELECT $1", Parameters::positional(&[&input]))?;
            let chunk = result.next().unwrap()?;
            let vector = chunk.get_vector_at::<$type>(0)?;
            assert_eq!(vector.get(0)?, Some(&input));
            drop(vector);
            drop(chunk);
            drop(result);
        }};
    }

    assert_round_trip!(DateValue(-1), DateValue);
    assert_round_trip!(TimeValue(1), TimeValue);
    assert_round_trip!(TimeNsValue(2), TimeNsValue);
    assert_round_trip!(TimeTzValue(3), TimeTzValue);
    assert_round_trip!(TimestampValue(-4), TimestampValue);
    assert_round_trip!(TimestampSecValue(-5), TimestampSecValue);
    assert_round_trip!(TimestampMsValue(-6), TimestampMsValue);
    assert_round_trip!(TimestampNsValue(-7), TimestampNsValue);
    assert_round_trip!(TimestampTzValue(-8), TimestampTzValue);
    assert_round_trip!(TimestampTzNsValue(-9), TimestampTzNsValue);
    assert_round_trip!(UuidValue(i128::MIN + 10), UuidValue);
    assert_round_trip!(
        IntervalValue {
            months: -1,
            days: 2,
            micros: -3,
        },
        IntervalValue
    );
    assert_round_trip!(
        DecimalValue::<i64, 18, 3>(-123_456),
        DecimalValue<i64, 18, 3>
    );

    let blob = BlobValue(vec![0_u8, 1, 255]);
    let mut result = conn.query("SELECT $1", Parameters::positional(&[&blob]))?;
    let chunk = result.next().unwrap()?;
    let vector = chunk.get_vector_at::<BlobValue<Vec<u8>>>(0)?;
    assert_eq!(vector.get(0)?, Some(blob.0.as_slice()));
    drop(vector);
    drop(chunk);
    drop(result);

    let bit = BitValue(vec![3_u8, 0b0001_0101]);
    let mut result = conn.query("SELECT $1", Parameters::positional(&[&bit]))?;
    let chunk = result.next().unwrap()?;
    let vector = chunk.get_vector_at::<BitValue<Vec<u8>>>(0)?;
    assert_eq!(vector.get(0)?, Some(bit.0.as_slice()));
    drop(vector);
    drop(chunk);
    drop(result);

    let bignum = BigNumValue {
        is_negative: true,
        magnitude: vec![1, 2, 3, 4, 5],
    };
    let mut result = conn.query("SELECT $1", Parameters::positional(&[&bignum]))?;
    let chunk = result.next().unwrap()?;
    let vector = chunk.get_vector_at::<BigNumValue>(0)?;
    let decoded = vector.get(0)?.unwrap().decode()?;
    assert_eq!(decoded.is_negative, bignum.is_negative);
    assert_eq!(decoded.magnitude, bignum.magnitude);

    Ok(())
}

#[test]
#[cfg(feature = "capi-v2-p2")]
pub fn test_vector_make_constant() -> crate::Result<()> {
    let env = Environment::new()?;
    let db = env.open(StorageLocation::InMemory)?;
    let conn = db.connect()?;

    ScalarFunctionBuilder::new(
        "to_constant",
        SignatureBuilder::new(
            [Parameter::normal("in", i32::logical_type(&conn)?)],
            i32::logical_type(&conn)?,
        ),
        ConstantScalar,
    )
    .register_with_connection(&conn)?;

    let mut statements = conn.parse("SELECT to_constant(unnest([1,2,3]));")?;

    let statement = statements.next().unwrap()?;

    assert!(statements.next().is_none());

    let result = conn.query(statement, Parameters::None)?;

    for item in result {
        let item = item?;

        let res = item.get_vector_at::<i32>(0)?;

        assert!(res.len() == 3);
        let mut reader = res.iter()?;

        assert_eq!(reader.next(), Some(Some(&42)));
        assert_eq!(reader.next(), Some(Some(&42)));
        assert_eq!(reader.next(), Some(Some(&42)));
        assert_eq!(reader.next(), None);
    }

    Ok(())
}

#[test]
#[cfg(feature = "capi-v2-p2")]
pub fn test_vector_make_sequence() -> crate::Result<()> {
    let env = Environment::new()?;
    let db = env.open(StorageLocation::InMemory)?;
    let conn = db.connect()?;

    ScalarFunctionBuilder::new(
        "to_sequence",
        SignatureBuilder::new(
            [Parameter::normal("in", i32::logical_type(&conn)?)],
            i32::logical_type(&conn)?,
        ),
        SequenceScalar,
    )
    .register_with_connection(&conn)?;

    let mut statements = conn.parse("SELECT to_sequence(unnest([1,2,3]));")?;

    let statement = statements.next().unwrap()?;

    assert!(statements.next().is_none());

    let result = conn.query(statement, Parameters::None)?;

    for item in result {
        let item = item?;

        let res = item.get_vector_at::<i32>(0)?;

        assert!(res.len() == 3);
        let mut reader = res.iter()?;

        assert_eq!(reader.next(), Some(Some(&42)));
        assert_eq!(reader.next(), Some(Some(&52)));
        assert_eq!(reader.next(), Some(Some(&62)));
        assert_eq!(reader.next(), None);
    }

    Ok(())
}

#[test]
#[cfg(feature = "capi-v2-p2")]
pub fn test_vector_types() -> crate::Result<()> {
    let env = Environment::new()?;
    let db = env.open(StorageLocation::InMemory)?;
    let conn = db.connect()?;

    ScalarFunctionBuilder::new(
        "test",
        SignatureBuilder::new(
            [Parameter::normal("in", String::logical_type(&conn)?)],
            String::logical_type(&conn)?,
        ),
        CopyStringScalar,
    )
    .register_with_connection(&conn)?;

    let result = conn.query(
        r#"SELECT test(x) from test_vector_types(null::VARCHAR) as t(x);"#,
        Parameters::None,
    )?;

    let mut concatenated = vec![];

    for item in result {
        let item = item?;

        let vector = item.get_vector_at::<String>(0)?;

        concatenated.extend(vector.iter()?.map(|x| x.map(str::to_string)));
    }

    assert_eq!(
        concatenated,
        vec![
            Some("🦆🦆🦆🦆🦆🦆".to_string()),
            Some("goo\0se".to_string()),
            None,
            Some("🦆🦆🦆🦆🦆🦆".to_string()),
            Some("🦆🦆🦆🦆🦆🦆".to_string()),
            Some("🦆🦆🦆🦆🦆🦆".to_string()),
            Some("goo\0se".to_string()),
            None,
            Some("🦆🦆🦆🦆🦆🦆".to_string()),
            Some("goo\0se".to_string()),
            None
        ]
    );

    Ok(())
}

// TODO: This should not be a scalar, but an table function.
#[test]
#[cfg(feature = "capi-v2-p2")]
pub fn test_vector_set_value() -> crate::Result<()> {
    scalar_callback!(ToVariant, Variant, |input, output, ctx, _user_data| {
        let mut output = output;
        output.set_size(input.row_count()? * input.vectors_count()?)?;

        let mut idx = 0;

        for vec in input.vectors()? {
            for i in 0..vec.len() {
                let value = if !vec.is_null(i)? {
                    match vec.logical_type().type_id() {
                        DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER => {
                            let val = vec.get_as_checked::<i32>(i)?.unwrap();

                            Some(val.value(&ctx)?)
                        }
                        DUCKDB_V2_LOGICAL_TYPE_ID_BOOLEAN => {
                            let val = vec.get_as_checked::<bool>(i)?.unwrap();

                            Some(val.value(&ctx)?)
                        }
                        DUCKDB_V2_LOGICAL_TYPE_ID_GEOMETRY => {
                            todo!()
                        }
                        DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR => {
                            todo!()
                        }
                        _ => None,
                    }
                } else {
                    None
                };

                if let Some(value) = value {
                    let lt = LogicalType::from_text(&ctx, "VARIANT")?;
                    let result = value.cast_with_context(&ctx, lt)?;
                    output.write_value_slow(idx, result)?;
                } else {
                    output.set_null_slow(idx)?
                }
                idx += 1;
            }
        }

        Ok(())
    });

    let env = Environment::new()?;
    let db = env.open(StorageLocation::InMemory)?;
    let conn = db.connect()?;

    let rt = LogicalType::from_text(&conn, "VARIANT")?;

    ScalarFunctionBuilder::new(
        "to_variant",
        SignatureBuilder::new(
            [Parameter::tail_vararg("in", Any::logical_type(&conn)?)],
            rt,
        ),
        ToVariant,
    )
    .register_with_connection(&conn)?;

    let result = conn.query(
        r#"SELECT to_variant(bool, "int") from test_all_types();"#, // r#"SELECT to_variant(x) from test_vector_types("TESTER") as t(x);"#
        Parameters::None,
    )?;

    let expected = ["false", "true", "NULL"];
    let mut results: Vec<String> = Vec::new();

    for item in result {
        let item = item?;

        for vector in item.vectors()? {
            let vector = vector.cast::<Variant>()?;
            for row in vector.iter()? {
                match row {
                    None => {
                        results.push("NULL".into());
                    }
                    Some(value) => {
                        results.push(value.dbg_string()?);
                    }
                }
            }
        }
    }

    assert_eq!(results, expected);

    Ok(())
}

scalar_callback!(RefScalar, String, |input, output, _ctx, _user_data| {
    let input = input.get_vector_at::<String>(0)?;
    let output = output;

    output.copy_from(&input)?;

    Ok(())
});

#[test]
#[cfg(feature = "capi-v2-p2")]
pub fn test_vector_reference_input() -> crate::Result<()> {
    let env = Environment::new()?;
    let db = env.open(StorageLocation::InMemory)?;
    let conn = db.connect()?;

    ScalarFunctionBuilder::new(
        "reference",
        SignatureBuilder::new(
            [Parameter::normal("in", String::logical_type(&conn)?)],
            String::logical_type(&conn)?,
        ),
        RefScalar,
    )
    .register_with_connection(&conn)?;

    let result = conn.query(
        r#"SELECT reference(x) from test_vector_types(null::VARCHAR) as t(x);"#,
        Parameters::None,
    )?;

    let mut results: Vec<String> = Vec::new();

    for item in result {
        let item = item?;

        for vector in item.vectors()? {
            let vector = vector.cast::<String>()?;
            for row in vector.iter()? {
                match row {
                    None => {
                        results.push("NULL".into());
                    }
                    Some(value) => {
                        results.push(value.to_string());
                    }
                }
            }
        }
    }

    assert_eq!(
        results,
        vec![
            "🦆🦆🦆🦆🦆🦆".to_string(),
            "goo\0se".to_string(),
            "NULL".to_string(),
            "🦆🦆🦆🦆🦆🦆".to_string(),
            "🦆🦆🦆🦆🦆🦆".to_string(),
            "🦆🦆🦆🦆🦆🦆".to_string(),
            "goo\0se".to_string(),
            "NULL".to_string(),
            "🦆🦆🦆🦆🦆🦆".to_string(),
            "goo\0se".to_string(),
            "NULL".to_string()
        ]
    );

    Ok(())
}

#[test]
fn test_vector_tstring() -> crate::Result<()> {
    let env = Environment::new()?;
    let db = env.open(StorageLocation::InMemory)?;
    let conn = db.connect()?;

    let result = conn.query(
        "SELECT * FROM test_vector_types(NULL::BLOB)",
        Parameters::None,
    )?;

    let expected = [
        Some("thisisalongblob\x00withnullbytes".to_string()),
        Some("\x00\x00\x00a".to_string()),
        None,
        Some("thisisalongblob\x00withnullbytes".to_string()),
        Some("thisisalongblob\x00withnullbytes".to_string()),
        Some("thisisalongblob\x00withnullbytes".to_string()),
        Some("\x00\x00\x00a".to_string()),
        None,
        Some("thisisalongblob\x00withnullbytes".to_string()),
        Some("\x00\x00\x00a".to_string()),
        None,
    ];

    let mut results: Vec<Option<String>> = vec![];

    for chunk in result {
        let chunk = chunk?;
        let vector = chunk.get_vector_at::<TString>(0)?;

        for item in vector.iter()? {
            if let Some(value) = item {
                let string = String::from_utf8_lossy(value.get_data());
                results.push(Some(string.to_string()));
            } else {
                results.push(None);
            }
        }
    }

    assert_eq!(results, expected);

    Ok(())
}

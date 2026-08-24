use crate::environment::{Environment, StorageLocation};

use super::*;

struct TestStruct;

impl StructSchema for TestStruct {
    fn fields<C: FFILink + ?Sized>(link: &C) -> Result<Vec<(&'static str, LogicalType)>> {
        Ok(vec![
            ("id", i32::logical_type(link)?),
            ("name", String::logical_type(link)?),
        ])
    }
}

struct TestUnion;

impl UnionSchema for TestUnion {
    fn members<C: FFILink + ?Sized>(link: &C) -> Result<Vec<(&'static str, LogicalType)>> {
        Ok(vec![
            ("number", i32::logical_type(link)?),
            ("text", String::logical_type(link)?),
        ])
    }
}

#[test]
fn test_i32_to_value() -> crate::Result<()> {
    let env = Environment::new()?;
    let db = env.open(StorageLocation::InMemory)?;
    let conn = db.connect()?;

    let value = 42_i32.value(&conn)?;

    assert_eq!(
        value.fetch_logical_type()?.type_id(),
        LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER
    );
    assert_eq!(value.dbg_string()?, "42");

    Ok(())
}

#[test]
fn test_primitive_logical_types() -> crate::Result<()> {
    let env = Environment::new()?;
    let db = env.open(StorageLocation::InMemory)?;
    let conn = db.connect()?;

    macro_rules! assert_primitive {
        ($type:ty, $value:expr, $type_id:ident) => {
            assert_eq!(
                <$type>::logical_type(&conn)?.type_id(),
                LogicalTypeID::$type_id
            );
            assert_eq!(
                ($value as $type)
                    .value(&conn)?
                    .fetch_logical_type()?
                    .type_id(),
                LogicalTypeID::$type_id
            );
        };
    }

    assert_primitive!(bool, true, DUCKDB_V2_LOGICAL_TYPE_ID_BOOLEAN);
    assert_primitive!(u8, 1, DUCKDB_V2_LOGICAL_TYPE_ID_UTINYINT);
    assert_primitive!(i8, 1, DUCKDB_V2_LOGICAL_TYPE_ID_TINYINT);
    assert_primitive!(i16, 1, DUCKDB_V2_LOGICAL_TYPE_ID_SMALLINT);
    assert_primitive!(i32, 1, DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);
    assert_primitive!(i64, 1, DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT);
    assert_primitive!(u16, 1, DUCKDB_V2_LOGICAL_TYPE_ID_USMALLINT);
    assert_primitive!(u32, 1, DUCKDB_V2_LOGICAL_TYPE_ID_UINTEGER);
    assert_primitive!(u64, 1, DUCKDB_V2_LOGICAL_TYPE_ID_UBIGINT);
    assert_primitive!(f32, 1.0, DUCKDB_V2_LOGICAL_TYPE_ID_FLOAT);
    assert_primitive!(f64, 1.0, DUCKDB_V2_LOGICAL_TYPE_ID_DOUBLE);
    assert_primitive!(i128, 1, DUCKDB_V2_LOGICAL_TYPE_ID_HUGEINT);
    assert_primitive!(u128, 1, DUCKDB_V2_LOGICAL_TYPE_ID_UHUGEINT);

    Ok(())
}

#[test]
fn test_remaining_value_creators() -> crate::Result<()> {
    let env = Environment::new()?;
    let db = env.open(StorageLocation::InMemory)?;
    let conn = db.connect()?;

    macro_rules! assert_type {
        ($value:expr, $type_id:ident) => {
            assert_eq!(
                $value.value(&conn)?.fetch_logical_type()?.type_id(),
                LogicalTypeID::$type_id
            );
        };
    }

    assert_type!(BlobValue([0_u8, 1, 2]), DUCKDB_V2_LOGICAL_TYPE_ID_BLOB);
    assert_type!(BitValue([0_u8, 0b1010_1010]), DUCKDB_V2_LOGICAL_TYPE_ID_BIT);
    assert_type!(DateValue(1), DUCKDB_V2_LOGICAL_TYPE_ID_DATE);
    assert_type!(TimeValue(1), DUCKDB_V2_LOGICAL_TYPE_ID_TIME);
    assert_type!(TimeNsValue(1), DUCKDB_V2_LOGICAL_TYPE_ID_TIME_NS);
    assert_type!(TimeTzValue(1), DUCKDB_V2_LOGICAL_TYPE_ID_TIME_TZ);
    assert_type!(TimestampValue(1), DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP);
    assert_type!(
        TimestampSecValue(1),
        DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_SEC
    );
    assert_type!(TimestampMsValue(1), DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_MS);
    assert_type!(TimestampNsValue(1), DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_NS);
    assert_type!(TimestampTzValue(1), DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_TZ);
    assert_type!(
        TimestampTzNsValue(1),
        DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_TZ_NS
    );
    assert_type!(
        IntervalValue {
            months: 1,
            days: 2,
            micros: 3,
        },
        DUCKDB_V2_LOGICAL_TYPE_ID_INTERVAL
    );
    assert_type!(UuidValue(0), DUCKDB_V2_LOGICAL_TYPE_ID_UUID);

    let tuple = (42_i32, "duck").value(&conn)?;
    assert_eq!(
        tuple.fetch_logical_type()?.type_id(),
        LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_TUPLE
    );
    assert_eq!(tuple.dbg_string()?, "(42, duck)");
    assert_eq!(().value(&conn)?.dbg_string()?, "()");

    let logical_type = i32::logical_type(&conn)?;
    let null = Value::null(&logical_type)?;
    assert!(null.is_null()?);

    Ok(())
}

#[test]
fn test_binary_value_creators() -> crate::Result<()> {
    let env = Environment::new()?;
    let db = env.open(StorageLocation::InMemory)?;
    let conn = db.connect()?;

    let empty_blob = BlobValue(Vec::<u8>::new()).value(&conn)?;
    assert_eq!(
        empty_blob.fetch_logical_type()?.type_id(),
        LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_BLOB
    );

    let blob = BlobValue([0_u8, 1, 255]).value(&conn)?;
    assert_eq!(blob.dbg_string()?, "\\x00\\x01\\xFF");

    let bit = BitValue([0_u8, 0b1010_1010]).value(&conn)?;
    assert_eq!(bit.dbg_string()?, "10101010");

    assert!(BitValue(Vec::<u8>::new()).value(&conn).is_err());

    Ok(())
}

#[test]
fn test_temporal_value_creators() -> crate::Result<()> {
    let env = Environment::new()?;
    let db = env.open(StorageLocation::InMemory)?;
    let conn = db.connect()?;

    assert_eq!(DateValue(0).value(&conn)?.dbg_string()?, "1970-01-01");
    assert_eq!(TimeValue(1_000_000).value(&conn)?.dbg_string()?, "00:00:01");
    assert_eq!(
        TimeNsValue(1_000_000_000).value(&conn)?.dbg_string()?,
        "00:00:01"
    );
    assert_eq!(
        TimestampValue(0).value(&conn)?.dbg_string()?,
        "1970-01-01 00:00:00"
    );
    assert_eq!(
        TimestampSecValue(1).value(&conn)?.dbg_string()?,
        "1970-01-01 00:00:01"
    );
    assert_eq!(
        TimestampMsValue(1_000).value(&conn)?.dbg_string()?,
        "1970-01-01 00:00:01"
    );
    assert_eq!(
        TimestampNsValue(1_000_000_000).value(&conn)?.dbg_string()?,
        "1970-01-01 00:00:01"
    );
    assert_eq!(
        IntervalValue {
            months: 1,
            days: 2,
            micros: 3_000_000,
        }
        .value(&conn)?
        .dbg_string()?,
        "1 month 2 days 00:00:03"
    );

    Ok(())
}

#[test]
fn test_uuid_and_tuple_value_creators() -> crate::Result<()> {
    let env = Environment::new()?;
    let db = env.open(StorageLocation::InMemory)?;
    let conn = db.connect()?;

    let uuid = UuidValue(i128::MIN).value(&conn)?;
    assert_eq!(uuid.dbg_string()?, "00000000-0000-0000-0000-000000000000");

    let empty = ().value(&conn)?;
    assert_eq!(empty.child_count()?, 0);

    let tuple = (42_i32, "duck", true).value(&conn)?;
    assert_eq!(tuple.child_count()?, 3);
    assert_eq!(tuple.get_child(0)?.dbg_string()?, "42");
    assert_eq!(tuple.get_child(1)?.dbg_string()?, "duck");
    assert_eq!(tuple.get_child(2)?.dbg_string()?, "true");

    let logical_type = i32::logical_type(&conn)?;
    let null = Value::null(&logical_type)?;
    drop(logical_type);
    assert!(null.is_null()?);
    assert_eq!(
        null.fetch_logical_type()?.type_id(),
        LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER
    );

    Ok(())
}

#[test]
fn test_primitive_value_getters() -> crate::Result<()> {
    let env = Environment::new()?;
    let db = env.open(StorageLocation::InMemory)?;
    let conn = db.connect()?;

    macro_rules! assert_round_trip {
        ($value:expr, $type:ty) => {
            assert_eq!($value.value(&conn)?.get::<$type>()?, $value);
        };
    }

    assert_round_trip!(true, bool);
    assert_round_trip!(u8::MAX, u8);
    assert_round_trip!(i8::MIN, i8);
    assert_round_trip!(i16::MIN, i16);
    assert_round_trip!(i32::MIN, i32);
    assert_round_trip!(i64::MIN, i64);
    assert_round_trip!(u16::MAX, u16);
    assert_round_trip!(u32::MAX, u32);
    assert_round_trip!(u64::MAX, u64);
    assert_round_trip!(i128::MIN, i128);
    assert_round_trip!(u128::MAX, u128);
    assert_round_trip!(1.25_f32, f32);
    assert_round_trip!(1.25_f64, f64);

    assert_eq!("42".value(&conn)?.get::<i32>()?, 42);
    assert_eq!("hello".value(&conn)?.get::<String>()?, "hello");
    assert_eq!("".value(&conn)?.get::<String>()?, "");

    Ok(())
}

#[test]
fn test_raw_value_getters() -> crate::Result<()> {
    let env = Environment::new()?;
    let db = env.open(StorageLocation::InMemory)?;
    let conn = db.connect()?;

    let blob = BlobValue(vec![0_u8, 1, 255]);
    assert_eq!(blob.value(&conn)?.get::<BlobValue<Vec<u8>>>()?, blob);
    assert_eq!(
        BlobValue(Vec::<u8>::new())
            .value(&conn)?
            .get::<BlobValue<Vec<u8>>>()?,
        BlobValue(Vec::new())
    );

    let bit = BitValue(vec![3_u8, 0b0001_0101]);
    assert_eq!(bit.value(&conn)?.get::<BitValue<Vec<u8>>>()?, bit);

    macro_rules! assert_storage_round_trip {
        ($value:expr, $type:ty) => {
            assert_eq!($value.value(&conn)?.get::<$type>()?, $value);
        };
    }

    assert_storage_round_trip!(DateValue(-1), DateValue);
    assert_storage_round_trip!(TimeValue(1), TimeValue);
    assert_storage_round_trip!(TimeNsValue(2), TimeNsValue);
    assert_storage_round_trip!(TimeTzValue(3), TimeTzValue);
    assert_storage_round_trip!(TimestampValue(-4), TimestampValue);
    assert_storage_round_trip!(TimestampSecValue(-5), TimestampSecValue);
    assert_storage_round_trip!(TimestampMsValue(-6), TimestampMsValue);
    assert_storage_round_trip!(TimestampNsValue(-7), TimestampNsValue);
    assert_storage_round_trip!(TimestampTzValue(-8), TimestampTzValue);
    assert_storage_round_trip!(TimestampTzNsValue(-9), TimestampTzNsValue);
    assert_storage_round_trip!(UuidValue(i128::MIN + 10), UuidValue);

    let interval = IntervalValue {
        months: -1,
        days: 2,
        micros: -3,
    };
    assert_eq!(interval.value(&conn)?.get::<IntervalValue>()?, interval);

    let decimal = DecimalValue::<i64, 18, 3>(-123_456).value(&conn)?;
    assert_eq!(
        decimal.get::<DecimalValueRaw>()?,
        DecimalValueRaw {
            value: -123_456,
            width: 18,
            scale: 3,
        }
    );

    let bignum = BigNumValue {
        is_negative: true,
        magnitude: vec![1, 2, 3, 4, 5],
    };
    assert_eq!(bignum.value(&conn)?.get::<BigNumValue>()?, bignum);

    Ok(())
}

#[test]
fn test_nullable_and_type_value_getters() -> crate::Result<()> {
    let env = Environment::new()?;
    let db = env.open(StorageLocation::InMemory)?;
    let conn = db.connect()?;

    assert_eq!(Some(42_i32).value(&conn)?.get::<Option<i32>>()?, Some(42));

    let null = Option::<i32>::None.value(&conn)?;
    assert_eq!(null.get::<Option<i32>>()?, None);
    assert!(null.get::<i32>().is_err());

    let integer = i32::logical_type(&conn)?;
    let type_value = Value::from_logical_type(&conn, &integer)?;
    assert_eq!(
        type_value.get::<LogicalType>()?.type_id(),
        LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER
    );

    Ok(())
}

#[test]
fn test_list_to_value() -> crate::Result<()> {
    let env = Environment::new()?;
    let db = env.open(StorageLocation::InMemory)?;
    let conn = db.connect()?;

    let values = vec![Some(1_i32), None, Some(3_i32)];
    let value = values.value(&conn)?;
    let logical_type = Vec::<Option<i32>>::logical_type(&conn)?;

    assert_eq!(
        logical_type.type_id(),
        LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_LIST
    );
    assert_eq!(
        logical_type.get_param(0)?.1.logical_type()?.type_id(),
        LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER
    );
    assert_eq!(value.dbg_string()?, "[1, NULL, 3]");

    Ok(())
}

#[test]
fn test_array_to_value() -> crate::Result<()> {
    let env = Environment::new()?;
    let db = env.open(StorageLocation::InMemory)?;
    let conn = db.connect()?;

    let values = [Some(1_i32), None, Some(3_i32)];
    let value = values.value(&conn)?;
    let logical_type = <[Option<i32>; 3]>::logical_type(&conn)?;

    assert_eq!(
        logical_type.type_id(),
        LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_ARRAY
    );
    assert_eq!(logical_type.to_string()?, "INTEGER[3]");
    assert_eq!(value.dbg_string()?, "[1, NULL, 3]");

    Ok(())
}

#[test]
fn test_complex_values() -> crate::Result<()> {
    let env = Environment::new()?;
    let db = env.open(StorageLocation::InMemory)?;
    let conn = db.connect()?;

    assert_eq!("hello".value(&conn)?.dbg_string()?, "hello");

    let decimal = DecimalValue::<i32, 9, 2>(1234).value(&conn)?;
    assert_eq!(decimal.dbg_string()?, "12.34");

    let bignum = BigNumValue {
        is_negative: true,
        magnitude: vec![1, 2, 3, 4],
    }
    .value(&conn)?;
    assert_eq!(bignum.dbg_string()?, "-16909060");

    let map = MapValue {
        entries: vec![("a".to_string(), 1_i32), ("b".to_string(), 2_i32)],
    }
    .value(&conn)?;
    assert_eq!(map.dbg_string()?, "{a=1, b=2}");

    let struct_value = StructValue::<TestStruct>::new()
        .field(42_i32)
        .field("duck".to_string())
        .value(&conn)?;
    assert_eq!(struct_value.dbg_string()?, "{'id': 42, 'name': duck}");

    let union = UnionValue::<TestUnion, _>::new(42_i32).value(&conn)?;
    assert_eq!(
        union.fetch_logical_type()?.type_id(),
        LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_UNION
    );
    assert_eq!(union.child_count()?, 2);

    let variant = VariantValue(42_i32).value(&conn)?;
    assert_eq!(
        variant.fetch_logical_type()?.type_id(),
        LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_VARIANT
    );

    Ok(())
}

use duckdb_loadable_macros::{AsVectorElement, AsWritableVectorElement};
use duckdb_neo::{
    AsDuckDB, DuckDBType, Parameters, Result, ToValue,
    connection::Context,
    data_chunk::{DataChunk, VectorCollection},
    environment::{Environment, StorageLocation},
    logical_type::LogicalType,
    scalar::{ScalarCallbacks, ScalarFunctionBuilder},
    signature::{FunctionParameter, SignatureBuilder},
    types::Struct,
    vector::{StorageKind, Unknown, Vector},
};

#[derive(Debug, PartialEq, AsDuckDB)]
struct Row {
    name: Option<String>,
    value: Option<i32>,
}

#[derive(Debug, AsVectorElement, AsWritableVectorElement)]
#[allow(dead_code)]
struct Nested {
    row: Option<Row>,
    items: Option<Vec<Option<i32>>>,
}

#[derive(Debug, PartialEq, AsDuckDB)]
struct Generic<C>
where
    C: Clone,
{
    value: core::option::Option<C>,
}

#[derive(Debug, PartialEq, AsDuckDB)]
struct AllNested {
    row: Option<Row>,
}

#[derive(Debug, AsDuckDB)]
struct Empty {}

#[derive(Debug, AsVectorElement, AsWritableVectorElement)]
#[allow(dead_code)]
struct Fixed {
    values: Option<[Option<i32>; 2]>,
}

#[test]
fn owned_struct_fields_roundtrip_without_a_write_projection() -> Result<()> {
    #[derive(Debug, PartialEq, AsDuckDB)]
    struct Point {
        name: String,
        value: i32,
    }
    #[derive(Debug, PartialEq, AsDuckDB)]
    struct Record {
        primary: Point,
        others: Vec<Option<Point>>,
        labels: Vec<String>,
        nullable_labels: Vec<Option<String>>,
        fixed: [Option<String>; 2],
        matrix: Vec<Option<Vec<i32>>>,
        optional: Option<i64>,
    }
    fn accepts_self<T: for<'a> duckdb_neo::vector::WritableVectorElement<Write<'a> = T>>() {}
    accepts_self::<Record>();
    let record = || Record {
        primary: Point {
            name: "owned nested string".to_owned(),
            value: 42,
        },
        others: vec![
            Some(Point {
                name: "child".to_owned(),
                value: 7,
            }),
            None,
        ],
        labels: vec!["first owned label".to_owned(), "second".to_owned()],
        nullable_labels: vec![None, Some("nullable".to_owned())],
        fixed: [Some("fixed string".to_owned()), None],
        matrix: vec![Some(vec![1, 2]), None, Some(vec![])],
        optional: None,
    };
    let conn = Environment::new()?.open(StorageLocation::InMemory)?.connect()?;
    let mut chunk = DataChunk::create(&conn, &[Record::logical_type(&conn)?], true)?;
    let mut vector = chunk.get_vector_at_mut::<Record>(0)?;
    vector.set_size(3)?;
    {
        let mut writer = vector.writer()?;
        writer.set(2, None)?;
        writer.set(0, Some(record()))?;
        writer.set(1, Some(record()))?;
        writer.set(1, None)?;
        assert!(writer.set(3, Some(record())).is_err());
    }
    let row = vector.reader()?.get(0)?.unwrap();
    assert_eq!(row.primary.unwrap().name, Some("owned nested string"));
    let others = row.others.unwrap();
    assert_eq!(others.get(0).unwrap().unwrap().value, Some(&7));
    assert!(others.get(1).unwrap().is_none());
    assert_eq!(
        row.labels.unwrap().iter().collect::<Vec<_>>(),
        [Some("first owned label"), Some("second")]
    );
    assert_eq!(
        row.nullable_labels.unwrap().iter().collect::<Vec<_>>(),
        [None, Some("nullable")]
    );
    assert_eq!(
        row.fixed.unwrap().iter().collect::<Vec<_>>(),
        [Some("fixed string"), None]
    );
    let matrix = row.matrix.unwrap();
    assert_eq!(
        matrix.get(0).unwrap().unwrap().iter().collect::<Vec<_>>(),
        [Some(&1), Some(&2)]
    );
    assert!(matrix.get(1).unwrap().is_none());
    assert!(matrix.get(2).unwrap().unwrap().is_empty());
    assert!(row.optional.is_none());
    assert!(vector.reader()?.get(1)?.is_none());
    assert!(vector.reader()?.get(2)?.is_none());
    assert_eq!(vector.get_value_slow(0)?.get::<Record>()?, Some(record()));
    let fixed = vector.children().unwrap().get_vector_at::<[Option<String>; 2]>(4)?;
    assert!(fixed.reader()?.get(1)?.is_none());
    let elements = fixed.children().unwrap().get_vector_at::<String>(0)?;
    for index in 2..6 {
        assert!(elements.reader()?.get(index)?.is_none());
    }
    Ok(())
}

#[test]
fn owned_binary_and_bignum_fields_roundtrip() -> Result<()> {
    use duckdb_neo::types::{BigNumValue, BlobValue};
    #[derive(Debug, PartialEq, AsDuckDB)]
    struct Payload {
        blob: BlobValue,
        number: BigNumValue,
    }
    let payload = || Payload {
        blob: BlobValue(vec![0, 255, 42, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9]),
        number: BigNumValue {
            is_negative: true,
            magnitude: vec![1, 0, 42],
        },
    };
    let conn = Environment::new()?.open(StorageLocation::InMemory)?.connect()?;
    let mut chunk = DataChunk::create(&conn, &[Payload::logical_type(&conn)?], true)?;
    let mut vector = chunk.get_vector_at_mut::<Payload>(0)?;
    vector.set_size(2)?;
    {
        let mut writer = vector.writer()?;
        writer.set(0, Some(payload()))?;
        writer.set(1, None)?;
    }
    assert_eq!(vector.get_value_slow(0)?.get::<Payload>()?, Some(payload()));
    let row = vector.reader()?.get(0)?.unwrap();
    assert_eq!(row.blob, Some(payload().blob.0.as_slice()));
    assert_eq!(row.number.unwrap().decode()?, payload().number);
    assert!(vector.reader()?.get(1)?.is_none());
    Ok(())
}

struct RowProbe(std::sync::Arc<std::sync::atomic::AtomicUsize>);

impl ScalarCallbacks for RowProbe {
    type BindData = ();
    type InitData = ();

    fn exec(
        &self,
        _bind: Option<&()>,
        _init: Option<&mut ()>,
        _context: &Context,
        input: &mut VectorCollection,
        output: Vector<'_, Unknown>,
    ) -> Result<()> {
        let mut vector = input.get_vector_at_mut::<Row>(0)?;
        let flag = match vector.storage_kind() {
            StorageKind::Flat => 1,
            StorageKind::Constant => 2,
            StorageKind::Dictionary => 4,
            StorageKind::Other => 8,
        };
        self.0.fetch_or(flag, std::sync::atomic::Ordering::Relaxed);
        if vector.storage_kind() == StorageKind::Other {
            vector.flatten()?;
        }
        if let Some(mut children) = vector.children_mut() {
            for index in 0..children.col_count() {
                let mut child = children.get_untyped_vector_at_mut(index)?;
                let flag = match child.storage_kind() {
                    StorageKind::Flat => 1,
                    StorageKind::Constant => 2,
                    StorageKind::Dictionary => 4,
                    StorageKind::Other => 8,
                };
                self.0.fetch_or(flag, std::sync::atomic::Ordering::Relaxed);
                if child.storage_kind() == StorageKind::Other {
                    child.flatten()?;
                }
            }
        }
        let mut output = output.cast::<i32>()?;
        output.set_size(vector.len())?;
        let mut writer = output.writer()?;
        for (index, row) in vector.reader()?.iter().enumerate() {
            writer.set(index, row.and_then(|row| row.value.copied()))?;
        }
        Ok(())
    }
}

#[test]
fn umbrella_derive_roundtrips_values_and_reads_vectors() -> Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    let input = AllNested {
        row: Some(Row {
            name: Some("duck".to_owned()),
            value: None,
        }),
    };
    let generic = Generic {
        value: Some("generic".to_owned()),
    };
    assert_eq!(input.value(&conn)?.logical_type()?, AllNested::logical_type(&conn)?);
    assert_eq!(
        generic.value(&conn)?.logical_type()?,
        Generic::<String>::logical_type(&conn)?
    );
    let chunk = conn
        .query("SELECT $1, $2", Parameters::positional(&[&input, &generic]))?
        .next_chunk()?
        .unwrap();
    let row = chunk
        .get_vector_at::<AllNested>(0)?
        .reader()?
        .get(0)?
        .unwrap()
        .row
        .unwrap();
    assert_eq!(row.name, Some("duck"));
    assert_eq!(row.value, None);
    assert_eq!(
        chunk
            .get_vector_at::<AllNested>(0)?
            .get_value_slow(0)?
            .get::<AllNested>()?,
        Some(input)
    );
    assert_eq!(
        chunk
            .get_vector_at::<Generic<String>>(1)?
            .reader()?
            .get(0)?
            .unwrap()
            .value,
        Some("generic")
    );
    assert_eq!(
        chunk
            .get_vector_at::<Generic<String>>(1)?
            .get_value_slow(0)?
            .get::<Generic<String>>()?,
        Some(generic)
    );
    let empty = Empty {}.value(&conn)?;
    assert!(empty.get::<Empty>()?.is_some());
    let null = Option::<AllNested>::None.value(&conn)?;
    assert!(null.get::<AllNested>()?.is_none());
    Ok(())
}

#[test]
fn derived_writer_preserves_nulls_and_copies_strings() -> Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    let mut chunk = DataChunk::create(&conn, &[Row::logical_type(&conn)?], true)?;
    let mut vector = chunk.get_vector_at_mut::<Row>(0)?;
    vector.set_size(3)?;
    {
        let text = "a string longer than the inline storage".to_owned();
        let mut writer = vector.writer()?;
        writer.set(
            0,
            Some(Row {
                name: Some(text),
                value: Some(42),
            }),
        )?;
        writer.set(
            1,
            Some(Row {
                name: None,
                value: None,
            }),
        )?;
        writer.set(2, None)?;
        assert!(writer.set(3, None).is_err());
    }
    let row = vector.reader()?.get(0)?.unwrap();
    assert_eq!(row.name, Some("a string longer than the inline storage"));
    assert_eq!(row.value, Some(&42));
    let row = vector.reader()?.get(1)?.unwrap();
    assert_eq!(row.name, None);
    assert_eq!(row.value, None);
    assert!(vector.reader()?.get(2)?.is_none());
    let children = vector.children().unwrap();
    assert_eq!(children.get_vector_at::<String>(0)?.reader()?.get(2)?, None);
    assert_eq!(children.get_vector_at::<i32>(1)?.reader()?.get(2)?, None);
    {
        let mut writer = vector.writer()?;
        writer.set(0, None)?;
        writer.set(
            2,
            Some(Row {
                name: Some("rewritten".to_owned()),
                value: Some(7),
            }),
        )?;
    }
    assert!(vector.reader()?.get(0)?.is_none());
    let row = vector.reader()?.get(2)?.unwrap();
    assert_eq!(row.name, Some("rewritten"));
    assert_eq!(row.value, Some(&7));
    Ok(())
}

#[test]
fn derived_writer_supports_nested_fields_and_growth() -> Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    let logical_type = LogicalType::from_text(
        &conn,
        "STRUCT(row STRUCT(name VARCHAR, value INTEGER), items INTEGER[])",
    )?;
    let mut chunk = DataChunk::create(&conn, &[logical_type], true)?;
    let mut vector = chunk.get_vector_at_mut::<Nested>(0)?;
    vector.set_size(1)?;
    vector.writer()?.set(
        0,
        Some(Nested {
            row: Some(Row {
                name: Some("nested".to_owned()),
                value: Some(9),
            }),
            items: Some(vec![Some(4), None, Some(8)]),
        }),
    )?;
    vector.set_size(3)?;
    {
        let mut writer = vector.writer()?;
        writer.set(1, None)?;
        writer.set(
            2,
            Some(Nested {
                row: None,
                items: Some(vec![]),
            }),
        )?;
    }
    let row = vector.reader()?.get(0)?.unwrap();
    let inner = row.row.unwrap();
    assert_eq!(inner.name, Some("nested"));
    assert_eq!(inner.value, Some(&9));
    assert_eq!(
        row.items.unwrap().iter().collect::<Vec<_>>(),
        [Some(&4), None, Some(&8)]
    );
    assert!(vector.reader()?.get(1)?.is_none());
    let row = vector.reader()?.get(2)?.unwrap();
    assert!(row.row.is_none());
    assert!(row.items.unwrap().is_empty());
    let inner = vector.children().unwrap().get_vector_at::<Row>(0)?;
    for index in [1, 2] {
        assert!(inner.reader()?.get(index)?.is_none());
        let children = inner.children().unwrap();
        assert_eq!(children.get_vector_at::<String>(0)?.reader()?.get(index)?, None);
        assert_eq!(children.get_vector_at::<i32>(1)?.reader()?.get(index)?, None);
    }
    Ok(())
}

#[test]
fn derived_writer_supports_generic_and_empty_structs() -> Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    let mut chunk = DataChunk::create(
        &conn,
        &[Generic::<String>::logical_type(&conn)?, Empty::logical_type(&conn)?],
        true,
    )?;
    {
        let mut vector = chunk.get_vector_at_mut::<Generic<String>>(0)?;
        vector.set_size(2)?;
        let mut writer = vector.writer()?;
        writer.set(
            0,
            Some(Generic::<String> {
                value: Some("generic".to_owned()),
            }),
        )?;
        writer.set(1, Some(Generic::<String> { value: None }))?;
    }
    let vector = chunk.get_vector_at::<Generic<String>>(0)?;
    assert_eq!(vector.reader()?.get(0)?.unwrap().value, Some("generic"));
    assert_eq!(vector.reader()?.get(1)?.unwrap().value, None);
    {
        let mut vector = chunk.get_vector_at_mut::<Empty>(1)?;
        vector.set_size(2)?;
        let mut writer = vector.writer()?;
        writer.set(0, Some(Empty {}))?;
        writer.set(1, None)?;
    }
    let vector = chunk.get_vector_at::<Empty>(1)?;
    assert!(vector.reader()?.get(0)?.is_some());
    assert!(vector.reader()?.get(1)?.is_none());
    Ok(())
}

#[test]
fn derived_writer_uses_custom_vector_validation() -> Result<()> {
    #[derive(Debug, AsVectorElement)]
    #[allow(dead_code)]
    struct Layout {
        value: Option<i64>,
    }
    #[derive(Debug, AsWritableVectorElement)]
    #[allow(dead_code)]
    struct CustomReader {
        value: Option<i64>,
    }
    // SAFETY: Layout validates the writer's fields; this reader exposes no physical storage.
    unsafe impl duckdb_neo::vector::VectorElement for CustomReader {
        const TYPE_ID: duckdb_neo::logical_type::LogicalTypeID = duckdb_neo::logical_type::LogicalTypeID::Struct;
        type Internal<'a> = ();
        type Ref<'a> = ();

        fn validate(ty: &LogicalType, children: Option<&VectorCollection>) -> Result<bool> {
            <Layout as duckdb_neo::vector::VectorElement>::validate(ty, children)
        }

        unsafe fn get<'a, U: duckdb_neo::vector::VectorElement>(
            _reader: &duckdb_neo::vector::VectorReader<'a, U>,
            _physical: usize,
            _logical: usize,
        ) -> Self::Ref<'a>
        where
            Self: 'a,
        {
        }
    }
    let conn = Environment::new()?.open(StorageLocation::InMemory)?.connect()?;
    for sql in [
        "STRUCT(value INTEGER)",
        "STRUCT(other BIGINT)",
        "STRUCT(value BIGINT, other BIGINT)",
    ] {
        let mut chunk = DataChunk::create(&conn, &[LogicalType::from_text(&conn, sql)?], true)?;
        assert!(chunk.get_vector_at_mut::<CustomReader>(0).is_err(), "{sql}");
        assert!(
            chunk.get_vector_at_mut::<Unknown>(0)?.cast::<CustomReader>().is_err(),
            "{sql}"
        );
    }
    let mut chunk = DataChunk::create(&conn, &[LogicalType::from_text(&conn, "STRUCT(value BIGINT)")?], true)?;
    {
        let mut vector = chunk.get_vector_at_mut::<CustomReader>(0)?;
        vector.set_size(1)?;
        vector.writer()?.set(0, Some(CustomReader { value: Some(42) }))?;
    }
    assert_eq!(
        chunk.get_vector_at::<Layout>(0)?.reader()?.get(0)?.unwrap().value,
        Some(&42)
    );
    Ok(())
}

#[test]
fn derived_writer_checks_fixed_array_schema() -> Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    let logical_type = LogicalType::from_text(&conn, "STRUCT(values INTEGER[2])")?;
    let mut chunk = DataChunk::create(&conn, &[logical_type], true)?;
    let mut vector = chunk.get_vector_at_mut::<Fixed>(0)?;
    vector.set_size(1)?;
    vector.writer()?.set(
        0,
        Some(Fixed {
            values: Some([Some(1), Some(2)]),
        }),
    )?;
    let wrong_type = LogicalType::from_text(&conn, "STRUCT(values INTEGER[3])")?;
    let mut wrong = DataChunk::create(&conn, &[wrong_type], true)?;
    assert!(wrong.get_vector_at_mut::<Fixed>(0).is_err());
    let row = vector.reader()?.get(0)?.unwrap();
    assert_eq!(row.values.unwrap().iter().collect::<Vec<_>>(), [Some(&1), Some(&2)]);
    vector.writer()?.set(
        0,
        Some(Fixed {
            values: Some([Some(3), Some(4)]),
        }),
    )?;
    let row = vector.reader()?.get(0)?.unwrap();
    assert_eq!(row.values.unwrap().iter().collect::<Vec<_>>(), [Some(&3), Some(&4)]);
    Ok(())
}

#[test]
fn derived_vector_reads_native_storage_kinds() -> Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    let kinds = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    ScalarFunctionBuilder::new(
        "row_probe",
        SignatureBuilder::new()
            .param(FunctionParameter::new::<Row>("row"))
            .return_type(i32::logical_type(&conn)?),
        RowProbe(kinds.clone()),
    )
    .register(&conn)?;
    let chunk = conn
        .query(
            "SELECT count(*) FILTER (WHERE row_probe(s) IS DISTINCT FROM s.value), count(*)
             FROM test_vector_types(NULL::STRUCT(name VARCHAR, value INTEGER)) t(s)",
            Parameters::None,
        )?
        .next_chunk()?
        .unwrap();
    assert_eq!(chunk.get_vector_at::<i64>(0)?.reader()?.get(0)?, Some(&0));
    assert!(chunk.get_vector_at::<i64>(1)?.reader()?.get(0)?.copied().unwrap() > 0);
    assert_eq!(kinds.load(std::sync::atomic::Ordering::Relaxed) & 7, 7);
    Ok(())
}

#[test]
fn derived_vector_preserves_nulls_and_borrows_fields() -> Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    let chunk = conn
        .query(
            "SELECT CASE WHEN i = 2 THEN NULL::STRUCT(name VARCHAR, value INTEGER)
                    ELSE {'name': CASE WHEN i IN (1, 3) THEN NULL ELSE 'duck' END,
                          'value': CASE WHEN i = 3 THEN NULL ELSE i::INTEGER END} END
             FROM range(4) t(i)",
            Parameters::None,
        )?
        .next_chunk()?
        .unwrap();
    let vector = chunk.get_vector_at::<Row>(0)?;
    let first: RowRef<'_> = vector.reader()?.get(0)?.unwrap();
    assert_eq!(first.name, Some("duck"));
    assert_eq!(first.value, Some(&0));
    let untyped = chunk.get_vector_at::<Struct>(0)?.reader()?.get(0)?.unwrap();
    assert_eq!(
        first.name.unwrap().as_ptr(),
        untyped.get::<String>("name")?.unwrap().as_ptr()
    );
    let second = vector.reader()?.get(1)?.unwrap();
    assert_eq!(second.name, None);
    assert_eq!(second.value, Some(&1));
    assert!(vector.reader()?.get(2)?.is_none());
    let fourth = vector.reader()?.get(3)?.unwrap();
    assert_eq!(fourth.name, None);
    assert_eq!(fourth.value, None);
    assert_eq!(vector.reader()?.iter().count(), 4);
    assert!(vector.reader()?.get(4).is_err());
    Ok(())
}

#[test]
fn derived_vector_reads_nested_children_and_generic_fields() -> Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    let chunk = conn
        .query(
            "SELECT {'row': {'name': 'duck', 'value': 42}, 'items': [4, NULL, 8]::INTEGER[]},
                    {'value': 7}, {'value': 'generic'}",
            Parameters::None,
        )?
        .next_chunk()?
        .unwrap();
    let nested = chunk.get_vector_at::<Nested>(0)?.reader()?.get(0)?.unwrap();
    let row = nested.row.unwrap();
    assert_eq!(row.name, Some("duck"));
    assert_eq!(row.value, Some(&42));
    assert_eq!(
        nested.items.unwrap().iter().collect::<Vec<_>>(),
        [Some(&4), None, Some(&8)]
    );
    let generic: GenericRef<'_, i32> = chunk.get_vector_at::<Generic<i32>>(1)?.reader()?.get(0)?.unwrap();
    assert_eq!(generic.value, Some(&7));
    let generic = chunk.get_vector_at::<Generic<String>>(2)?.reader()?.get(0)?.unwrap();
    assert_eq!(generic.value, Some("generic"));
    assert!(chunk.get_vector_at::<Generic<String>>(1).is_err());
    Ok(())
}

#[test]
fn derived_vector_rejects_invalid_schemas() -> Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    for expression in [
        "42",
        "row('duck', 42)",
        "{'name': 'duck'}",
        "{'name': 'duck', 'value': 42, 'extra': 1}",
        "{'wrong': 'duck', 'value': 42}",
        "{'value': 42, 'name': 'duck'}",
        "{'name': 42, 'value': 7}",
        "{'name': NULL::INTEGER, 'value': 7}",
        "{'name': 'duck', 'value': 7::BIGINT}",
    ] {
        let chunk = conn
            .query(format!("SELECT {expression}"), Parameters::None)?
            .next_chunk()?
            .unwrap();
        assert!(chunk.get_vector_at::<Row>(0).is_err(), "{expression}");
    }
    let chunk = conn
        .query("SELECT {'Name': 'duck', 'VALUE': 42}", Parameters::None)?
        .next_chunk()?
        .unwrap();
    assert_eq!(
        chunk.get_vector_at::<Row>(0)?.reader()?.get(0)?.unwrap().name,
        Some("duck")
    );
    let chunk = conn
        .query(
            "SELECT {'row': {'name': 'duck', 'value': 'wrong'}, 'items': [1]}",
            Parameters::None,
        )?
        .next_chunk()?
        .unwrap();
    assert!(chunk.get_vector_at::<Nested>(0).is_err());
    Ok(())
}

#[test]
fn derived_vector_reads_empty_structs() -> Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    let mut chunk = duckdb_neo::data_chunk::DataChunk::create(&conn, &[Empty::logical_type(&conn)?], true)?;
    {
        let mut vector = chunk.get_vector_at_mut::<Struct>(0)?;
        vector.set_size(2)?;
        vector
            .writer()?
            .set(0, Some(duckdb_neo::types::StructWrite::default()))?;
        vector.writer()?.set(1, None)?;
    }
    let vector = chunk.get_vector_at::<Empty>(0)?;
    assert!(vector.reader()?.get(0)?.is_some());
    assert!(vector.reader()?.get(1)?.is_none());
    Ok(())
}

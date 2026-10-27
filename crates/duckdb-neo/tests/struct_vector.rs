use duckdb_loadable_macros::{AsVectorElement, AsWritableVectorElement};
use duckdb_neo::{
    AsDuckDB, DuckDBType, Parameters, Result, ToValue,
    connection::Context,
    data_chunk::{DataChunk, VectorCollection},
    environment::{Environment, StorageLocation},
    logical_type::LogicalType,
    scalar::{ScalarCallbacks, ScalarFunctionBuilder},
    signature::{FunctionParameter, SignatureBuilder},
    types::{Array, List, Struct},
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
    items: Option<List<i32>>,
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
    values: Option<Array<i32>>,
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
            Some(RowWrite {
                name: Some(&text),
                value: Some(42),
            }),
        )?;
        writer.set(1, Some(RowWrite::default()))?;
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
            Some(RowWrite {
                name: Some("rewritten"),
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
        Some(NestedWrite {
            row: Some(RowWrite {
                name: Some("nested"),
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
            Some(NestedWrite {
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
        writer.set(0, Some(GenericWrite::<String> { value: Some("generic") }))?;
        writer.set(1, Some(GenericWrite::<String>::default()))?;
    }
    let vector = chunk.get_vector_at::<Generic<String>>(0)?;
    assert_eq!(vector.reader()?.get(0)?.unwrap().value, Some("generic"));
    assert_eq!(vector.reader()?.get(1)?.unwrap().value, None);
    {
        let mut vector = chunk.get_vector_at_mut::<Empty>(1)?;
        vector.set_size(2)?;
        let mut writer = vector.writer()?;
        writer.set(0, Some(EmptyWrite::default()))?;
        writer.set(1, None)?;
    }
    let vector = chunk.get_vector_at::<Empty>(1)?;
    assert!(vector.reader()?.get(0)?.is_some());
    assert!(vector.reader()?.get(1)?.is_none());
    Ok(())
}

#[test]
fn derived_writer_validates_independently_of_the_reader() -> Result<()> {
    #[derive(Debug, AsWritableVectorElement)]
    #[allow(dead_code)]
    struct BroadReader {
        value: Option<i64>,
    }
    // SAFETY: this reader exposes no physical storage and reads no values.
    unsafe impl duckdb_neo::vector::VectorElement for BroadReader {
        const TYPE_ID: duckdb_neo::logical_type::LogicalTypeID = duckdb_neo::logical_type::LogicalTypeID::Struct;
        type Internal<'a> = ();
        type Ref<'a> = ();
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
        let mut vector = chunk.get_vector_at_mut::<BroadReader>(0)?;
        vector.set_size(1)?;
        assert!(
            vector
                .writer()?
                .set(0, Some(BroadReaderWrite { value: Some(42) }))
                .is_err()
        );
    }
    Ok(())
}

#[test]
fn derived_writer_preserves_child_value_checks() -> Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    let logical_type = LogicalType::from_text(&conn, "STRUCT(values INTEGER[2])")?;
    let mut chunk = DataChunk::create(&conn, &[logical_type], true)?;
    let mut vector = chunk.get_vector_at_mut::<Fixed>(0)?;
    vector.set_size(1)?;
    vector.writer()?.set(
        0,
        Some(FixedWrite {
            values: Some(vec![Some(1), Some(2)]),
        }),
    )?;
    assert!(
        vector
            .writer()?
            .set(
                0,
                Some(FixedWrite {
                    values: Some(vec![Some(3)])
                })
            )
            .is_err()
    );
    let row = vector.reader()?.get(0)?.unwrap();
    assert_eq!(row.values.unwrap().iter().collect::<Vec<_>>(), [Some(&1), Some(&2)]);
    vector.writer()?.set(
        0,
        Some(FixedWrite {
            values: Some(vec![Some(3), Some(4)]),
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

use duckdb_loadable_macros::AsNativeElement;
use duckdb_neo::{
    AsDuckDB, DuckDBType, Parameters, Result, ToValue,
    connection::Context,
    data_chunk::{DataChunk, VectorCollection},
    environment::{Environment, StorageLocation},
    logical_type::LogicalType,
    scalar::{ScalarCallbacks, ScalarFunctionBuilder},
    signature::{FunctionParameter, SignatureBuilder},
    types::{Array, List, Struct},
    vector::{RowReader as _, RowWriter as _, StorageKind, Unknown, Vector},
};

#[derive(Debug, PartialEq, AsDuckDB)]
#[duckdb(reader, writer)]
struct Row {
    name: Option<String>,
    value: Option<i32>,
}

#[derive(Debug, AsNativeElement)]
#[duckdb(reader, writer)]
#[allow(dead_code)]
struct Nested {
    row: Option<Row>,
    items: Option<List<i32>>,
}

#[derive(Debug, PartialEq, AsDuckDB)]
#[duckdb(reader, writer)]
struct Generic<C>
where
    C: Clone,
{
    value: core::option::Option<C>,
}

#[derive(Debug, PartialEq, AsDuckDB)]
#[duckdb(reader, writer)]
struct AllNested {
    row: Option<Row>,
}

#[derive(Debug, AsDuckDB)]
#[duckdb(reader, writer)]
struct Empty {}

#[derive(Debug, AsNativeElement)]
#[duckdb(reader, writer)]
#[allow(dead_code)]
struct Fixed {
    values: Option<Array<i32, 2>>,
}

#[test]
fn owned_struct_fields_roundtrip_without_a_write_projection() -> Result<()> {
    #[derive(Debug, PartialEq, AsDuckDB)]
    #[duckdb(reader, writer)]
    struct Point {
        name: String,
        value: i32,
    }
    #[derive(Debug, PartialEq, AsDuckDB)]
    #[duckdb(reader, writer)]
    struct Record {
        primary: Point,
        others: List<Point>,
        labels: List<String>,
        nullable_labels: List<String>,
        fixed: Array<String, 2>,
        matrix: List<List<i32>>,
        optional: Option<i64>,
    }
    fn accepts_self<T: duckdb_neo::vector::WithWriter + duckdb_neo::vector::WithReader>() {}
    accepts_self::<Record>();
    let record = || Record {
        primary: Point {
            name: "owned nested string".to_owned(),
            value: 42,
        },
        others: List(vec![
            Some(Point {
                name: "child".to_owned(),
                value: 7,
            }),
            None,
        ]),
        labels: List(vec![Some("first owned label".to_owned()), Some("second".to_owned())]),
        nullable_labels: List(vec![None, Some("nullable".to_owned())]),
        fixed: Array([Some("fixed string".to_owned()), None]),
        matrix: List(vec![Some(List(vec![Some(1), Some(2)])), None, Some(List(vec![]))]),
        optional: None,
    };
    let conn = Environment::new()?.open(StorageLocation::InMemory)?.connect()?;
    let mut chunk = DataChunk::create(&conn, &[Record::logical_type(&conn)?], true)?;
    let mut vector = chunk.get_vector_at_mut::<Record>(0)?;
    vector.set_size(3)?;
    {
        // Two records: 4 others, labels and nullable labels, and 6 inner lists of 4 integers.
        let mut fields = vector.children_mut().unwrap();
        fields.get_vector_at_mut::<List<Point>>(1)?.set_child_size(4)?;
        fields.get_vector_at_mut::<List<String>>(2)?.set_child_size(4)?;
        fields.get_vector_at_mut::<List<String>>(3)?.set_child_size(4)?;
        let mut matrix = fields.get_vector_at_mut::<List<List<i32>>>(5)?;
        matrix.set_child_size(6)?;
        matrix
            .children_mut()
            .unwrap()
            .get_vector_at_mut::<List<i32>>(0)?
            .set_child_size(4)?;
    }
    {
        let mut writer = vector.row_writer()?;
        writer.set_null(2)?;
        writer.set(0, &record())?;
        writer.set(1, &record())?;
        writer.set_null(1)?;
        assert!(writer.set(3, &record()).is_err());
    }
    let reader = vector.row_reader()?;
    let row = reader.get(0)?.unwrap();
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
    assert!(vector.row_reader()?.get(1)?.is_none());
    assert!(vector.row_reader()?.get(2)?.is_none());
    assert_eq!(vector.get_value_slow(0)?.get::<Record>()?, Some(record()));
    let labels = vector.children().unwrap().get_vector_at::<List<String>>(2)?;
    assert!(labels.row_reader()?.get(1)?.is_none());
    // The list field writers filled both records' elements.
    assert_eq!(labels.children().unwrap().get_untyped_vector_at(0)?.len(), 4);
    // NULL records null their arrays' elements too.
    let fixed = vector.children().unwrap().get_vector_at::<Array<String, 2>>(4)?;
    assert!(fixed.row_reader()?.get(1)?.is_none());
    let elements = fixed.children().unwrap().get_vector_at::<String>(0)?;
    for index in 2..6 {
        assert!(elements.row_reader()?.get(index)?.is_none());
    }
    Ok(())
}

#[test]
fn owned_binary_and_bignum_fields_roundtrip() -> Result<()> {
    use duckdb_neo::types::{BigNumValue, BlobValue};
    #[derive(Debug, PartialEq, AsDuckDB)]
    #[duckdb(reader, writer)]
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
        let mut writer = vector.row_writer()?;
        writer.set(0, &payload())?;
        writer.set_null(1)?;
    }
    assert_eq!(vector.get_value_slow(0)?.get::<Payload>()?, Some(payload()));
    let reader = vector.row_reader()?;
    let row = reader.get(0)?.unwrap();
    assert_eq!(row.blob, Some(payload().blob.0.as_slice()));
    assert_eq!(row.number.unwrap().decode()?, payload().number);
    assert!(vector.row_reader()?.get(1)?.is_none());
    Ok(())
}

struct RowProbe(std::sync::Arc<std::sync::atomic::AtomicUsize>);

impl ScalarCallbacks for RowProbe {
    type BindData = ();
    type InitData = ();

    fn exec(
        &self,
        _context: &Context,
        _bind: Option<&()>,
        _init: Option<&mut ()>,
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
        let mut writer = output.row_writer()?;
        for (index, row) in vector.row_reader()?.iter().enumerate() {
            writer.set_option(index, row.and_then(|row| row.value.copied()).as_ref())?;
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
        .row_reader()?
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
            .row_reader()?
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
fn derived_struct_raw_identifiers_roundtrip() -> Result<()> {
    #[derive(Debug, PartialEq, AsDuckDB)]
    #[duckdb(reader, writer)]
    struct Keyword {
        r#type: i32,
        r#match: Option<String>,
    }

    let conn = Environment::new()?.open(StorageLocation::InMemory)?.connect()?;
    let logical_type = Keyword::logical_type(&conn)?;
    assert_eq!(logical_type.get_param(0)?.0, "type");
    assert_eq!(logical_type.get_param(1)?.0, "match");
    let input = Keyword {
        r#type: 42,
        r#match: Some("duck".to_owned()),
    };
    let value = input.value(&conn)?;
    assert_eq!(value.logical_type()?, logical_type);
    assert_eq!(value.get::<Keyword>()?.as_ref(), Some(&input));

    for sql in [
        "SELECT {'type': 42, 'match': 'duck'}",
        "SELECT {'TYPE': 42, 'MATCH': 'duck'}",
    ] {
        let chunk = conn.query(sql, Parameters::None)?.next_chunk()?.unwrap();
        let vector = chunk.get_vector_at::<Keyword>(0)?;
        let reader = vector.row_reader()?;
        let row = reader.get(0)?.unwrap();
        assert_eq!(row.r#type, Some(&42));
        assert_eq!(row.r#match, Some("duck"));
        assert_eq!(vector.get_value_slow(0)?.get::<Keyword>()?.as_ref(), Some(&input));
    }

    let mut chunk = DataChunk::create(&conn, &[logical_type], true)?;
    let mut vector = chunk.get_vector_at_mut::<Keyword>(0)?;
    vector.set_size(2)?;
    {
        let mut writer = vector.row_writer()?;
        writer.set(0, &input)?;
        writer.set(
            1,
            &Keyword {
                r#type: 7,
                r#match: None,
            },
        )?;
    }
    let reader = vector.row_reader()?;
    let row = reader.get(0)?.unwrap();
    assert_eq!(row.r#type, Some(&42));
    assert_eq!(row.r#match, Some("duck"));
    assert_eq!(
        vector.get_value_slow(1)?.get::<Keyword>()?,
        Some(Keyword {
            r#type: 7,
            r#match: None
        })
    );
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
        let mut writer = vector.row_writer()?;
        writer.set(
            0,
            &Row {
                name: Some(text),
                value: Some(42),
            },
        )?;
        writer.set(
            1,
            &Row {
                name: None,
                value: None,
            },
        )?;
        writer.set_null(2)?;
        assert!(writer.set_null(3).is_err());
    }
    let reader = vector.row_reader()?;
    let row = reader.get(0)?.unwrap();
    assert_eq!(row.name, Some("a string longer than the inline storage"));
    assert_eq!(row.value, Some(&42));
    let reader = vector.row_reader()?;
    let row = reader.get(1)?.unwrap();
    assert_eq!(row.name, None);
    assert_eq!(row.value, None);
    assert!(vector.row_reader()?.get(2)?.is_none());
    let children = vector.children().unwrap();
    assert_eq!(children.get_vector_at::<String>(0)?.row_reader()?.get(2)?, None);
    assert_eq!(children.get_vector_at::<i32>(1)?.row_reader()?.get(2)?, None);
    {
        let mut writer = vector.row_writer()?;
        writer.set_null(0)?;
        writer.set(
            2,
            &Row {
                name: Some("rewritten".to_owned()),
                value: Some(7),
            },
        )?;
    }
    assert!(vector.row_reader()?.get(0)?.is_none());
    let reader = vector.row_reader()?;
    let row = reader.get(2)?.unwrap();
    assert_eq!(row.name, Some("rewritten"));
    assert_eq!(row.value, Some(&7));
    Ok(())
}

#[test]
fn derived_writer_supports_nested_fields() -> Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    let logical_type = LogicalType::from_text(
        &conn,
        "STRUCT(row STRUCT(name VARCHAR, value INTEGER), items INTEGER[])",
    )?;
    let mut chunk = DataChunk::create(&conn, &[logical_type], true)?;
    let mut vector = chunk.get_vector_at_mut::<Nested>(0)?;
    vector.set_size(3)?;
    vector
        .children_mut()
        .unwrap()
        .get_vector_at_mut::<List<i32>>(1)?
        .set_child_size(5)?;
    {
        let mut writer = vector.row_writer()?;
        writer.set(
            0,
            &Nested {
                row: Some(Row {
                    name: Some("nested".to_owned()),
                    value: Some(9),
                }),
                items: Some(List(vec![Some(4), None, Some(8)])),
            },
        )?;
        writer.set_null(1)?;
        writer.set(
            2,
            &Nested {
                row: None,
                items: Some(List(vec![Some(5), Some(6)])),
            },
        )?;
    }
    let reader = vector.row_reader()?;
    let row = reader.get(0)?.unwrap();
    let inner = row.row.unwrap();
    assert_eq!(inner.name, Some("nested"));
    assert_eq!(inner.value, Some(&9));
    assert_eq!(
        row.items.unwrap().iter().collect::<Vec<_>>(),
        [Some(&4), None, Some(&8)]
    );
    assert!(vector.row_reader()?.get(1)?.is_none());
    let reader = vector.row_reader()?;
    let row = reader.get(2)?.unwrap();
    assert!(row.row.is_none());
    assert_eq!(row.items.unwrap().iter().collect::<Vec<_>>(), [Some(&5), Some(&6)]);
    let inner = vector.children().unwrap().get_vector_at::<Row>(0)?;
    for index in [1, 2] {
        assert!(inner.row_reader()?.get(index)?.is_none());
        let children = inner.children().unwrap();
        assert_eq!(children.get_vector_at::<String>(0)?.row_reader()?.get(index)?, None);
        assert_eq!(children.get_vector_at::<i32>(1)?.row_reader()?.get(index)?, None);
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
        let mut writer = vector.row_writer()?;
        writer.set(
            0,
            &Generic::<String> {
                value: Some("generic".to_owned()),
            },
        )?;
        writer.set(1, &Generic::<String> { value: None })?;
    }
    let vector = chunk.get_vector_at::<Generic<String>>(0)?;
    assert_eq!(vector.row_reader()?.get(0)?.unwrap().value, Some("generic"));
    assert_eq!(vector.row_reader()?.get(1)?.unwrap().value, None);
    {
        let mut vector = chunk.get_vector_at_mut::<Empty>(1)?;
        vector.set_size(2)?;
        let mut writer = vector.row_writer()?;
        writer.set(0, &Empty {})?;
        writer.set_null(1)?;
    }
    let vector = chunk.get_vector_at::<Empty>(1)?;
    assert!(vector.row_reader()?.get(0)?.is_some());
    assert!(vector.row_reader()?.get(1)?.is_none());
    Ok(())
}

#[test]
fn derived_writer_checks_fixed_array_schema() -> Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    let logical_type = LogicalType::from_text(&conn, "STRUCT(values INTEGER[2])")?;
    let mut chunk = DataChunk::create(&conn, &[logical_type], true)?;
    let mut vector = chunk.get_vector_at_mut::<Fixed>(0)?;
    vector.set_size(2)?;
    vector.row_writer()?.set(
        0,
        &Fixed {
            values: Some(Array([Some(1), Some(2)])),
        },
    )?;
    let wrong_type = LogicalType::from_text(&conn, "STRUCT(values INTEGER[3])")?;
    let mut wrong = DataChunk::create(&conn, &[wrong_type], true)?;
    assert!(wrong.get_vector_at_mut::<Fixed>(0).is_err());
    let reader = vector.row_reader()?;
    let row = reader.get(0)?.unwrap();
    assert_eq!(row.values.unwrap().iter().collect::<Vec<_>>(), [Some(&1), Some(&2)]);
    {
        // Arrays have a fixed child, so rows can be rewritten in place.
        let mut writer = vector.row_writer()?;
        writer.set(
            0,
            &Fixed {
                values: Some(Array([Some(3), None])),
            },
        )?;
        writer.set(1, &Fixed { values: None })?;
    }
    let reader = vector.row_reader()?;
    let row = reader.get(0)?.unwrap();
    assert_eq!(row.values.unwrap().iter().collect::<Vec<_>>(), [Some(&3), None]);
    assert!(vector.row_reader()?.get(1)?.unwrap().values.is_none());
    let chunk = conn
        .query("SELECT {'values': [1, NULL]::INTEGER[2]}", Parameters::None)?
        .next_chunk()?
        .unwrap();
    let reader = chunk.get_vector_at::<Fixed>(0)?.row_reader()?;
    let row = reader.get(0)?.unwrap();
    assert_eq!(row.values.unwrap().iter().collect::<Vec<_>>(), [Some(&1), None]);
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
    assert_eq!(chunk.get_vector_at::<i64>(0)?.row_reader()?.get(0)?, Some(&0));
    assert!(chunk.get_vector_at::<i64>(1)?.row_reader()?.get(0)?.copied().unwrap() > 0);
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
    let reader = vector.row_reader()?;
    let first = reader.get(0)?.unwrap();
    assert_eq!(first.name, Some("duck"));
    assert_eq!(first.value, Some(&0));

    let reader = vector.row_reader()?;
    let second = reader.get(1)?.unwrap();
    assert_eq!(second.name, None);
    assert_eq!(second.value, Some(&1));
    assert!(vector.row_reader()?.get(2)?.is_none());
    let reader = vector.row_reader()?;
    let fourth = reader.get(3)?.unwrap();
    assert_eq!(fourth.name, None);
    assert_eq!(fourth.value, None);
    assert_eq!(vector.row_reader()?.iter().count(), 4);
    assert!(vector.row_reader()?.get(4).is_err());
    let reader = vector.row_reader()?;
    assert_eq!(reader.name().len(), 4);
    assert_eq!(reader.value().get(3)?, None);
    Ok(())
}

#[test]
fn derived_null_children_checks_bounds() -> Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    let mut chunk = DataChunk::create(&conn, &[Row::logical_type(&conn)?], true)?;
    let mut vector = chunk.get_vector_at_mut::<Row>(0)?;
    vector.set_size(1)?;
    let mut writer = vector.writer()?;
    assert!(duckdb_neo::__derive::null_children(&mut writer, 1).is_err());
    assert!(duckdb_neo::__derive::null_children(&mut writer, 100_000).is_err());
    duckdb_neo::__derive::null_children(&mut writer, 0)
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
    let reader = chunk.get_vector_at::<Nested>(0)?.row_reader()?;
    let nested = reader.get(0)?.unwrap();
    let row = nested.row.unwrap();
    assert_eq!(row.name, Some("duck"));
    assert_eq!(row.value, Some(&42));
    assert_eq!(
        nested.items.unwrap().iter().collect::<Vec<_>>(),
        [Some(&4), None, Some(&8)]
    );
    let reader = chunk.get_vector_at::<Generic<i32>>(1)?.row_reader()?;
    let generic = reader.get(0)?.unwrap();
    assert_eq!(generic.value, Some(&7));
    let reader = chunk.get_vector_at::<Generic<String>>(2)?.row_reader()?;
    let generic = reader.get(0)?.unwrap();
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
        chunk.get_vector_at::<Row>(0)?.row_reader()?.get(0)?.unwrap().name,
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
fn derived_refs_implement_standard_traits_when_fields_do() -> Result<()> {
    fn assert_traits<T: Clone + Copy + PartialEq + Eq + std::hash::Hash + std::fmt::Debug>() {}
    assert_traits::<RowRef<&'static str, &'static i32>>();
    assert_traits::<AllNestedRef<RowRef<&'static str, &'static i32>>>();
    assert_traits::<GenericRef<&'static i32>>();
    assert_traits::<EmptyRef>();

    let conn = Environment::new()?.open(StorageLocation::InMemory)?.connect()?;
    let chunk = conn
        .query(
            "SELECT {'row': {'name': 'duck', 'value': 42}}, {'row': {'name': 'duck', 'value': 42}},
                    {'row': {'name': NULL::VARCHAR, 'value': 42}}",
            Parameters::None,
        )?
        .next_chunk()?
        .unwrap();
    let [first, second, third] = [0, 1, 2].map(|index| chunk.get_vector_at::<AllNested>(index).unwrap());
    let reader = first.row_reader()?;
    let first = reader.get(0)?.unwrap();
    let reader = second.row_reader()?;
    let second = reader.get(0)?.unwrap();
    let reader = third.row_reader()?;
    let third = reader.get(0)?.unwrap();
    let copy = first;
    #[allow(clippy::clone_on_copy)]
    let clone = first.clone();
    assert_eq!(copy, clone);
    assert_eq!(first, second);
    assert_ne!(first, third);
    assert_eq!(
        first.row,
        Some(RowRef {
            name: Some("duck"),
            value: Some(&42),
        })
    );
    let rows = std::collections::HashSet::from([first, second, third]);
    assert_eq!(rows.len(), 2);
    Ok(())
}

#[test]
fn set_writes_borrowed_rows_without_moving_them() -> Result<()> {
    let rows = [
        Row {
            name: Some("a string longer than the inline storage".to_owned()),
            value: Some(1),
        },
        Row {
            name: None,
            value: Some(2),
        },
    ];
    let conn = Environment::new()?.open(StorageLocation::InMemory)?.connect()?;
    let mut chunk = DataChunk::create(&conn, &[Row::logical_type(&conn)?], true)?;
    let mut vector = chunk.get_vector_at_mut::<Row>(0)?;
    vector.set_size(4)?;
    {
        let mut writer = vector.row_writer()?;
        for (index, row) in rows.iter().enumerate() {
            writer.set(index, row)?;
        }
        writer.set(2, &rows[0])?;
        writer.set_null(3)?;
        assert!(writer.set(4, &rows[0]).is_err());
    }
    for (index, expected) in [0, 1, 0].into_iter().enumerate() {
        assert_eq!(
            vector.get_value_slow(index)?.get::<Row>()?.as_ref(),
            Some(&rows[expected])
        );
    }
    assert!(vector.row_reader()?.get(3)?.is_none());
    let names = vector.children().unwrap().get_vector_at::<String>(0)?;
    assert_eq!(names.row_reader()?.get(3)?, None);
    Ok(())
}

#[derive(Debug, PartialEq, AsDuckDB)]
#[duckdb(reader, writer)]
struct Matrix {
    items: List<i32>,
    matrix: List<List<i32>>,
}

fn matrix(items: &[i32], inner: &[i32]) -> Matrix {
    Matrix {
        items: items.iter().copied().map(Some).collect(),
        matrix: List(vec![Some(inner.iter().copied().map(Some).collect())]),
    }
}

/// Size a `Matrix` vector's list fields for `rows` rows of `matrix(&[_; 2], &[_; 2])`.
fn size_matrix(vector: &mut duckdb_neo::vector::Vector<'_, Matrix>, rows: usize) -> Result<()> {
    let mut fields = vector.children_mut().unwrap();
    fields.get_vector_at_mut::<List<i32>>(0)?.set_child_size(2 * rows)?;
    let mut matrix = fields.get_vector_at_mut::<List<List<i32>>>(1)?;
    matrix.set_child_size(rows)?;
    matrix
        .children_mut()
        .unwrap()
        .get_vector_at_mut::<List<i32>>(0)?
        .set_child_size(2 * rows)
}

#[test]
fn clearing_a_chunk_restarts_nested_lists() -> Result<()> {
    let conn = Environment::new()?.open(StorageLocation::InMemory)?.connect()?;
    let mut chunk = DataChunk::create(&conn, &[Matrix::logical_type(&conn)?], true)?;
    {
        let mut vector = chunk.get_vector_at_mut::<Matrix>(0)?;
        vector.set_size(1)?;
        size_matrix(&mut vector, 1)?;
        vector.row_writer()?.set(0, &matrix(&[1, 2], &[3, 4]))?;
    }
    chunk.clear()?;
    let mut vector = chunk.get_vector_at_mut::<Matrix>(0)?;
    vector.set_size(1)?;
    size_matrix(&mut vector, 1)?;
    {
        let mut writer = vector.row_writer()?;
        assert_eq!(writer.items.child_offset(), 0);
        assert_eq!(writer.matrix.child_offset(), 0);
        writer.set(0, &matrix(&[5, 6], &[7, 8]))?;
    }
    assert_eq!(
        vector.get_value_slow(0)?.get::<Matrix>()?,
        Some(matrix(&[5, 6], &[7, 8]))
    );
    Ok(())
}

#[derive(Debug, PartialEq, AsDuckDB)]
#[duckdb(reader, writer)]
struct Wide {
    x: Option<i64>,
}

#[derive(Debug, PartialEq, AsDuckDB)]
#[duckdb(reader, writer)]
struct Outer {
    inner: Option<Wide>,
}

#[test]
fn struct_fields_allow_writes_and_flattening() -> Result<()> {
    let conn = Environment::new()?.open(StorageLocation::InMemory)?.connect()?;
    let mut chunk = DataChunk::create(&conn, &[Outer::logical_type(&conn)?], true)?;
    let mut vector = chunk.get_vector_at_mut::<Outer>(0)?;
    vector.set_size(2)?;
    vector.row_writer()?.set_null(1)?;
    {
        let mut children = vector.children_mut().unwrap();
        let [inner] = children.get_vector_array_mut([0])?;
        let mut inner = inner.cast::<Wide>()?;
        inner.flatten()?;
        inner.row_writer()?.set(0, &Wide { x: Some(1) })?;
        let mut fields = inner.children_mut().unwrap();
        let mut x = fields.get_vector_at_mut::<i64>(0)?;
        x.write_value_slow(0, &7_i64.value(&conn)?)?;
        x.set_null_slow(1)?;
    }
    let reader = vector.row_reader()?;
    let row = reader.get(0)?.unwrap();
    assert_eq!(row.inner.unwrap().x, Some(&7));
    assert!(reader.get(1)?.is_none());
    Ok(())
}

#[test]
fn chunks_over_children_are_read_only() -> Result<()> {
    let conn = Environment::new()?.open(StorageLocation::InMemory)?.connect()?;
    let mut chunk = DataChunk::create(&conn, &[Wide::logical_type(&conn)?], true)?;
    let mut vector = chunk.get_vector_at_mut::<Wide>(0)?;
    vector.set_size(1)?;
    vector.row_writer()?.set(0, &Wide { x: Some(42) })?;
    {
        let mut children = vector.children_mut().unwrap();
        let mut fields = children.to_data_chunk(&conn)?;
        let mut x = fields.get_vector_at_mut::<i64>(0)?;
        assert!(!x.is_writable());
        assert!(x.set_size(0).is_err());
        assert!(x.writer().is_err());
        assert_eq!(x.row_reader()?.get(0)?, Some(&42));
    }
    assert_eq!(vector.row_reader()?.get(0)?.unwrap().x, Some(&42));
    Ok(())
}

#[test]
fn casts_need_writable_output() -> Result<()> {
    let conn = Environment::new()?.open(StorageLocation::InMemory)?.connect()?;
    let mut source = conn.query("SELECT 1::BIGINT", Parameters::None)?;
    let source = source.next_chunk()?.unwrap();
    let source = source.get_vector_at::<i64>(0)?;
    let mut chunk = DataChunk::create(&conn, &[i32::logical_type(&conn)?], false)?;
    // SAFETY: the casts fail before touching either vector.
    unsafe {
        assert!(chunk.get_vector_at_mut::<i32>(0)?.cast_rows(source, &conn).is_err());
        assert!(chunk.get_vector_at_mut::<i32>(0)?.try_cast_rows(source, &conn).is_err());
    }
    Ok(())
}

#[test]
fn casts_convert_every_row() -> Result<()> {
    let conn = Environment::new()?.open(StorageLocation::InMemory)?.connect()?;
    let mut source = DataChunk::create(&conn, &[List::<i32>::logical_type(&conn)?], true)?;
    let mut src = source.get_vector_at_mut::<List<i32>>(0)?;
    src.set_size(5000)?;
    src.set_child_size(10_000)?;
    {
        let mut writer = src.row_writer()?;
        for row in 0..5000 {
            let value = (row % 7 != 0).then_some(row as i32);
            writer.set(row, &List(vec![value, Some(-1)]))?;
        }
    }
    for target_type in [List::<i32>::logical_type(&conn)?, List::<i64>::logical_type(&conn)?] {
        let mut target = DataChunk::create(&conn, &[target_type], true)?;
        // SAFETY: neither vector is written or resized after the cast.
        unsafe { target.get_vector_at_mut::<Unknown>(0)?.cast_rows(&src, &conn)? };
        let dst = target.get_vector_at_mut::<Unknown>(0)?;
        assert_eq!(dst.len(), 5000);
        for row in [0, 1, 2047, 2048, 4999] {
            assert_eq!(
                dst.get_value_slow(row)?.get::<List<i64>>()?,
                Some(List(vec![(row % 7 != 0).then_some(row as i64), Some(-1)]))
            );
        }
    }
    Ok(())
}

#[derive(Debug, PartialEq, AsDuckDB)]
#[duckdb(reader, writer)]
struct Fallible {
    x: Option<i32>,
    bits: Option<duckdb_neo::types::BitValue>,
}

#[test]
fn failed_struct_writes_leave_the_row_null() -> Result<()> {
    use duckdb_neo::types::BitValue;

    let conn = Environment::new()?.open(StorageLocation::InMemory)?.connect()?;
    let mut chunk = DataChunk::create(&conn, &[Fallible::logical_type(&conn)?], true)?;
    let mut vector = chunk.get_vector_at_mut::<Fallible>(0)?;
    vector.set_size(2)?;
    let valid = Fallible {
        x: Some(1),
        bits: Some(BitValue(vec![0, 1])),
    };
    let invalid = Fallible {
        x: Some(42),
        bits: Some(BitValue(vec![])),
    };
    {
        let mut writer = vector.row_writer()?;
        writer.set_null(0)?;
        writer.set(1, &valid)?;
        assert!(writer.set(0, &invalid).is_err());
        let invalid = Fallible {
            x: Some(42),
            bits: Some(BitValue(vec![])),
        };
        assert!(writer.set(1, &invalid).is_err());
    }
    let reader = vector.row_reader()?;
    let fields = vector.children().unwrap().get_vector_at::<i32>(0)?.row_reader()?;
    for row in 0..2 {
        assert!(reader.get(row)?.is_none());
        assert_eq!(fields.get(row)?, None, "row {row}");
    }
    Ok(())
}

#[test]
fn failed_array_writes_leave_the_row_null() -> Result<()> {
    use duckdb_neo::types::BitValue;

    let conn = Environment::new()?.open(StorageLocation::InMemory)?.connect()?;
    let mut chunk = DataChunk::create(&conn, &[Array::<BitValue, 2>::logical_type(&conn)?], true)?;
    let mut vector = chunk.get_vector_at_mut::<Array<BitValue, 2>>(0)?;
    vector.set_size(1)?;
    {
        let mut writer = vector.row_writer()?;
        writer.set_null(0)?;
        assert!(
            writer
                .set(0, &Array([Some(BitValue(vec![0, 1])), Some(BitValue(vec![]))]))
                .is_err()
        );
    }
    assert!(vector.row_reader()?.get(0)?.is_none());
    let elements = vector.children().unwrap().get_vector_at::<BitValue>(0)?.row_reader()?;
    assert!(elements.get(0)?.is_none());
    assert!(elements.get(1)?.is_none());
    Ok(())
}

#[derive(Debug, PartialEq, AsDuckDB)]
#[duckdb(reader, writer)]
struct Renamed {
    #[duckdb(rename = "Kind")]
    kind: i32,
    #[duckdb(rename = "display name")]
    name: Option<String>,
}

#[test]
fn renamed_struct_fields_use_their_sql_names() -> Result<()> {
    let conn = Environment::new()?.open(StorageLocation::InMemory)?.connect()?;
    let logical_type = Renamed::logical_type(&conn)?;
    assert_eq!(
        logical_type,
        LogicalType::from_text(&conn, "STRUCT(\"Kind\" INTEGER, \"display name\" VARCHAR)")?
    );
    let input = Renamed {
        kind: 7,
        name: Some("duck".to_owned()),
    };
    assert_eq!(input.value(&conn)?.get::<Renamed>()?.as_ref(), Some(&input));

    let chunk = conn
        .query(
            "SELECT struct_extract($1, 'display name'), {'display name': 'goose', 'Kind': 3}, {'kind': 3, 'name': 'goose'}",
            Parameters::positional(&[&input]),
        )?
        .next_chunk()?
        .unwrap();
    assert_eq!(chunk.get_vector_at::<String>(0)?.row_reader()?.get(0)?, Some("duck"));
    let expected = Renamed {
        kind: 3,
        name: Some("goose".to_owned()),
    };
    assert_eq!(
        chunk.get_vector_at::<Struct>(1)?.get_value_slow(0)?.get::<Renamed>()?,
        Some(expected)
    );
    let error = chunk
        .get_vector_at::<Struct>(2)?
        .get_value_slow(0)?
        .get::<Renamed>()
        .unwrap_err();
    assert!(
        error.message.contains("STRUCT field 'display name' is missing"),
        "{error}"
    );
    assert!(chunk.get_vector_at::<Renamed>(2).is_err());

    let mut chunk = DataChunk::create(&conn, &[logical_type], true)?;
    let mut vector = chunk.get_vector_at_mut::<Renamed>(0)?;
    vector.set_size(1)?;
    vector.row_writer()?.set(0, &input)?;
    let reader = vector.row_reader()?;
    let row = reader.get(0)?.unwrap();
    assert_eq!(format!("{row:?}"), "RenamedRef { kind: Some(7), name: Some(\"duck\") }");
    Ok(())
}

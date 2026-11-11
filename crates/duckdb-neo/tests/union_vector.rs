use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use duckdb_neo::{
    AsDuckDB, AsDuckDBUnion, DuckDBType, Parameters, Result, ToValue,
    connection::{Connection, Context},
    data_chunk::{DataChunk, VectorCollection},
    environment::{Environment, StorageLocation},
    logical_type::{LogicalType, LogicalTypeID},
    scalar::{ScalarCallbacks, ScalarFunctionBuilder},
    signature::{FunctionParameter, SignatureBuilder},
    types::{BitValue, List},
    vector::{StorageKind, Unknown, Vector},
};

#[derive(Debug, Clone, PartialEq, AsDuckDBUnion)]
#[allow(non_camel_case_types)]
pub enum Input {
    Num(i16),
    str(String),
}

#[derive(Debug, Clone, PartialEq, AsDuckDBUnion)]
enum Nullable {
    Key(Option<i32>),
    Label(Option<String>),
}

#[derive(Debug, Clone, PartialEq, AsDuckDBUnion)]
enum Same {
    Left(i32),
    Right(i32),
}

#[derive(Debug, Clone, PartialEq, AsDuckDBUnion)]
enum Either<L, R> {
    Left(L),
    Right(R),
}

#[derive(Debug, Clone, PartialEq, AsDuckDBUnion)]
enum Flag {
    On(bool),
}

#[derive(Debug, Clone, PartialEq, AsDuckDBUnion)]
#[allow(non_camel_case_types)]
enum Keyword {
    r#type(i32),
    r#match(String),
}

#[derive(Debug, Clone, PartialEq, AsDuckDB)]
struct Point {
    x: i32,
    label: String,
}

#[derive(Debug, Clone, PartialEq, AsDuckDBUnion)]
enum Shape {
    Point(Point),
    Points(List<i32>),
    Nested(Input),
}

#[derive(Debug, Clone, PartialEq, AsDuckDB)]
struct Record {
    id: i32,
    input: Option<Input>,
}

fn connect() -> Result<Connection> {
    Environment::new()?.open(StorageLocation::InMemory)?.connect()
}

fn single_value(conn: &Connection, sql: &str) -> Result<duckdb_neo::value::Value> {
    conn.query(sql, Parameters::None)?
        .next_chunk()?
        .unwrap()
        .get_vector_at::<Unknown>(0)?
        .get_value_slow(0)
}

#[test]
fn derived_union_logical_types() -> Result<()> {
    let conn = connect()?;
    let logical_type = Input::logical_type(&conn)?;
    assert_eq!(logical_type.id()?, LogicalTypeID::Union);
    assert_eq!(
        logical_type,
        LogicalType::from_text(&conn, "UNION(Num SMALLINT, str VARCHAR)")?
    );
    assert_eq!(logical_type.get_param(0)?.0, "Num");
    assert_eq!(logical_type.get_param(1)?.0, "str");
    assert_eq!(
        Nullable::logical_type(&conn)?,
        LogicalType::from_text(&conn, "UNION(Key INTEGER, Label VARCHAR)")?
    );
    assert_eq!(
        Either::<i64, String>::logical_type(&conn)?,
        LogicalType::from_text(&conn, "UNION(\"Left\" BIGINT, \"Right\" VARCHAR)")?
    );
    assert_eq!(
        Keyword::logical_type(&conn)?,
        LogicalType::from_text(&conn, "UNION(\"type\" INTEGER, \"match\" VARCHAR)")?
    );
    assert_eq!(
        Shape::logical_type(&conn)?,
        LogicalType::from_text(
            &conn,
            "UNION(Point STRUCT(x INTEGER, label VARCHAR), Points INTEGER[], Nested UNION(Num SMALLINT, str VARCHAR))"
        )?
    );
    Ok(())
}

#[test]
fn derived_union_values_roundtrip() -> Result<()> {
    let conn = connect()?;
    fn roundtrip<T: DuckDBType + ToValue + duckdb_neo::FromValue + PartialEq + std::fmt::Debug>(
        conn: &Connection,
        input: T,
        tag: &str,
    ) -> Result<()> {
        let value = input.value(conn)?;
        assert_eq!(value.logical_type()?, T::logical_type(conn)?);
        assert!(!value.is_null()?);
        let tag_value = value.get_child(0)?.get::<u8>()?.unwrap() as usize;
        assert_eq!(value.logical_type()?.get_param(tag_value)?.0, tag, "{input:?}");
        assert_eq!(value.get::<T>()?, Some(input));
        Ok(())
    }
    roundtrip(&conn, Input::Num(7), "Num")?;
    roundtrip(
        &conn,
        Input::str("a string longer than the inline storage".to_owned()),
        "str",
    )?;
    roundtrip(&conn, Nullable::Key(Some(42)), "Key")?;
    roundtrip(&conn, Nullable::Key(None), "Key")?;
    roundtrip(&conn, Nullable::Label(None), "Label")?;
    roundtrip(&conn, Same::Left(1), "Left")?;
    roundtrip(&conn, Same::Right(1), "Right")?;
    roundtrip(&conn, Either::<i64, String>::Left(-5), "Left")?;
    roundtrip(&conn, Either::<i64, String>::Right("generic".to_owned()), "Right")?;
    roundtrip(&conn, Flag::On(true), "On")?;
    roundtrip(&conn, Keyword::r#match("duck".to_owned()), "match")?;
    roundtrip(
        &conn,
        Shape::Point(Point {
            x: 3,
            label: "point".to_owned(),
        }),
        "Point",
    )?;
    roundtrip(&conn, Shape::Points(List(vec![Some(1), None, Some(3)])), "Points")?;
    roundtrip(&conn, Shape::Nested(Input::str("inner".to_owned())), "Nested")?;

    let null = Option::<Input>::None.value(&conn)?;
    assert!(null.is_null()?);
    assert_eq!(null.get::<Input>()?, None);
    Ok(())
}

#[test]
fn derived_union_nested_null_values_roundtrip() -> Result<()> {
    #[derive(Debug, PartialEq, AsDuckDBUnion)]
    enum Inner {
        Nested(Option<i32>),
    }

    #[derive(Debug, PartialEq, AsDuckDBUnion)]
    enum Outer {
        Number(i32),
        Nested(Option<Inner>),
        Other(Option<Inner>),
    }

    let conn = connect()?;
    let rows = [
        Some(Outer::Number(1)),
        Some(Outer::Nested(None)),
        Some(Outer::Nested(Some(Inner::Nested(None)))),
        Some(Outer::Nested(Some(Inner::Nested(Some(7))))),
        Some(Outer::Other(None)),
        Some(Outer::Other(Some(Inner::Nested(Some(9))))),
        None,
    ];
    let logical_type = Outer::logical_type(&conn)?;
    let mut chunk = DataChunk::create(&conn, std::slice::from_ref(&logical_type), true)?;
    let mut vector = chunk.get_vector_at_mut::<Outer>(0)?;
    vector.set_size(rows.len())?;
    for (index, row) in rows.iter().enumerate() {
        vector.writer()?.set_option(index, row.as_ref())?;
        let value = row.value(&conn)?;
        assert_eq!(value.logical_type()?, logical_type);
        assert_eq!(value.is_null()?, row.is_none(), "row {index}");
        assert_eq!(value.get::<Outer>()?.as_ref(), row.as_ref(), "row {index}");
        assert_eq!(vector.get_value_slow(index)?.get::<Outer>()?.as_ref(), row.as_ref());
    }
    Ok(())
}

#[test]
fn derived_union_variants_can_use_associated_type_names() -> Result<()> {
    #[derive(Debug, PartialEq, AsDuckDBUnion)]
    enum Named {
        Ref(i32),
        Internal(String),
        Write(bool),
    }

    let conn = connect()?;
    let mut chunk = DataChunk::create(&conn, &[Named::logical_type(&conn)?], true)?;
    let mut vector = chunk.get_vector_at_mut::<Named>(0)?;
    vector.set_size(3)?;
    for (index, row) in [Named::Ref(7), Named::Internal("duck".to_owned()), Named::Write(true)]
        .iter()
        .enumerate()
    {
        vector.writer()?.set(index, row)?;
        assert_eq!(row.value(&conn)?.get::<Named>()?.as_ref(), Some(row));
    }
    assert_eq!(vector.reader()?.get(0)?, Some(NamedRef::Ref(Some(&7))));
    assert_eq!(vector.reader()?.get(1)?, Some(NamedRef::Internal(Some("duck"))));
    assert_eq!(vector.reader()?.get(2)?, Some(NamedRef::Write(Some(&true))));
    Ok(())
}

#[test]
fn derived_union_binds_as_query_parameter() -> Result<()> {
    let conn = connect()?;
    let inputs = [Input::Num(7), Input::str("duck".to_owned())];
    let chunk = conn
        .query(
            "SELECT union_tag($1)::VARCHAR, union_extract($1, 'Num'), union_tag($2)::VARCHAR, union_extract($2, 'str'), $3",
            Parameters::positional(&[&inputs[0], &inputs[1], &Same::Right(4)]),
        )?
        .next_chunk()?
        .unwrap();
    assert_eq!(chunk.get_vector_at::<String>(0)?.reader()?.get(0)?, Some("Num"));
    assert_eq!(chunk.get_vector_at::<i16>(1)?.reader()?.get(0)?, Some(&7));
    assert_eq!(chunk.get_vector_at::<String>(2)?.reader()?.get(0)?, Some("str"));
    assert_eq!(chunk.get_vector_at::<String>(3)?.reader()?.get(0)?, Some("duck"));
    assert_eq!(
        chunk.get_vector_at::<Same>(4)?.reader()?.get(0)?,
        Some(SameRef::Right(Some(&4)))
    );
    Ok(())
}

#[test]
fn derived_union_from_value_matches_member_names() -> Result<()> {
    let conn = connect()?;
    let value = single_value(
        &conn,
        "SELECT union_value(STR := 'duck')::UNION(STR VARCHAR, NUM SMALLINT)",
    )?;
    assert_eq!(value.get::<Input>()?, Some(Input::str("duck".to_owned())));
    let value = single_value(
        &conn,
        "SELECT union_value(Num := 3::SMALLINT)::UNION(str VARCHAR, Num SMALLINT)",
    )?;
    assert_eq!(value.get::<Input>()?, Some(Input::Num(3)));
    let value = single_value(&conn, "SELECT NULL::UNION(Num SMALLINT, str VARCHAR)")?;
    assert_eq!(value.get::<Input>()?, None);

    for (sql, message) in [
        ("SELECT 42", "Expected UNION value"),
        ("SELECT {'Num': 1}", "Expected UNION value"),
        (
            "SELECT union_value(Num := 1::SMALLINT)",
            "UNION has 1 members, expected 2",
        ),
        (
            "SELECT union_value(Num := 1::SMALLINT)::UNION(Num SMALLINT, str VARCHAR, extra INTEGER)",
            "UNION has 3 members, expected 2",
        ),
        (
            "SELECT union_value(Num := 1::SMALLINT)::UNION(Num SMALLINT, other VARCHAR)",
            "UNION member 'str' is missing",
        ),
        (
            "SELECT union_value(Num := NULL::SMALLINT)::UNION(Num SMALLINT, str VARCHAR)",
            "UNION member 'Num' is NULL",
        ),
    ] {
        let error = single_value(&conn, sql)?.get::<Input>().unwrap_err();
        assert!(error.message.contains(message), "{sql}: {error}");
    }
    Ok(())
}

#[test]
fn derived_union_reads_vectors() -> Result<()> {
    let conn = connect()?;
    let chunk = conn
        .query(
            "SELECT CASE i
                        WHEN 0 THEN union_value(Num := 7::SMALLINT)::UNION(Num SMALLINT, str VARCHAR)
                        WHEN 1 THEN union_value(str := 'a string longer than the inline storage')
                            ::UNION(Num SMALLINT, str VARCHAR)
                        WHEN 2 THEN NULL
                        ELSE union_value(Num := NULL::SMALLINT)::UNION(Num SMALLINT, str VARCHAR)
                    END,
                    CASE WHEN i % 2 = 0 THEN union_value(Label := NULL::VARCHAR)::UNION(Key INTEGER, Label VARCHAR)
                         ELSE union_value(Key := i::INTEGER)::UNION(Key INTEGER, Label VARCHAR) END
             FROM range(4) t(i)",
            Parameters::None,
        )?
        .next_chunk()?
        .unwrap();
    let vector = chunk.get_vector_at::<Input>(0)?;
    let rows: Vec<Option<InputRef<'_>>> = vector.reader()?.iter().collect();
    assert_eq!(
        rows,
        [
            Some(InputRef::Num(Some(&7))),
            Some(InputRef::str(Some("a string longer than the inline storage"))),
            None,
            Some(InputRef::Num(None)),
        ]
    );
    assert_eq!(vector.get_value_slow(0)?.get::<Input>()?, Some(Input::Num(7)));
    assert!(vector.get_value_slow(3)?.get::<Input>().is_err());
    let rows: Vec<_> = chunk.get_vector_at::<Nullable>(1)?.reader()?.iter().collect();
    assert_eq!(
        rows,
        [
            Some(NullableRef::Label(None)),
            Some(NullableRef::Key(Some(&1))),
            Some(NullableRef::Label(None)),
            Some(NullableRef::Key(Some(&3))),
        ]
    );
    Ok(())
}

#[test]
fn derived_union_reads_nested_members() -> Result<()> {
    let conn = connect()?;
    let shape = Shape::logical_type(&conn)?.to_text()?;
    let chunk = conn
        .query(
            format!(
                "SELECT union_value(Point := {{'x': 1, 'label': 'p'}})::{shape},
                        union_value(Points := [1, NULL, 3])::{shape},
                        union_value(Nested := union_value(str := 'inner')::UNION(Num SMALLINT, str VARCHAR))::{shape}"
            ),
            Parameters::None,
        )?
        .next_chunk()?
        .unwrap();
    let ShapeRef::Point(Some(point)) = chunk.get_vector_at::<Shape>(0)?.reader()?.get(0)?.unwrap() else {
        panic!("expected a point");
    };
    assert_eq!((point.x, point.label), (Some(&1), Some("p")));
    let vector = chunk.get_vector_at::<Shape>(1)?;
    let ShapeRef::Points(Some(points)) = vector.reader()?.get(0)?.unwrap() else {
        panic!("expected points");
    };
    assert_eq!(points.iter().collect::<Vec<_>>(), [Some(&1), None, Some(&3)]);
    let vector = chunk.get_vector_at::<Shape>(2)?;
    assert!(matches!(
        vector.reader()?.get(0)?.unwrap(),
        ShapeRef::Nested(Some(InputRef::str(Some("inner"))))
    ));
    Ok(())
}

#[test]
fn derived_union_rejects_invalid_schemas() -> Result<()> {
    let conn = connect()?;
    for expression in [
        "42",
        "{'Num': 1::SMALLINT, 'str': 'duck'}",
        "union_value(Num := 1::SMALLINT)",
        "union_value(Num := 1::SMALLINT)::UNION(Num SMALLINT, str VARCHAR, extra INTEGER)",
        "union_value(Num := 1::SMALLINT)::UNION(str VARCHAR, Num SMALLINT)",
        "union_value(Num := 1)::UNION(Num INTEGER, str VARCHAR)",
        "union_value(Num := 1::SMALLINT)::UNION(Num SMALLINT, other VARCHAR)",
        "union_value(Num := 1::SMALLINT)::UNION(Num SMALLINT, str BLOB)",
    ] {
        let chunk = conn
            .query(format!("SELECT {expression}"), Parameters::None)?
            .next_chunk()?
            .unwrap();
        assert!(chunk.get_vector_at::<Input>(0).is_err(), "{expression}");
    }
    let chunk = conn
        .query(
            "SELECT union_value(NUM := 1::SMALLINT)::UNION(NUM SMALLINT, STR VARCHAR)",
            Parameters::None,
        )?
        .next_chunk()?
        .unwrap();
    assert_eq!(
        chunk.get_vector_at::<Input>(0)?.reader()?.get(0)?,
        Some(InputRef::Num(Some(&1)))
    );
    Ok(())
}

#[test]
fn derived_union_writes_vectors() -> Result<()> {
    let conn = connect()?;
    let mut chunk = DataChunk::create(&conn, &[Input::logical_type(&conn)?], true)?;
    let mut vector = chunk.get_vector_at_mut::<Input>(0)?;
    vector.set_size(4)?;
    let long = "a string longer than the inline storage".to_owned();
    {
        let mut writer = vector.writer()?;
        writer.set(0, &Input::str(long.clone()))?;
        writer.set(1, &Input::Num(1))?;
        writer.set_null(2)?;
        writer.set(3, &Input::Num(9))?;
        // Switching a row's member must NULL the previously active one.
        writer.set(0, &Input::Num(-1))?;
        writer.set(1, &Input::str(long.clone()))?;
        assert!(writer.set_null(4).is_err());
    }
    let rows: Vec<_> = vector.reader()?.iter().collect();
    assert_eq!(
        rows,
        [
            Some(InputRef::Num(Some(&-1))),
            Some(InputRef::str(Some(long.as_str()))),
            None,
            Some(InputRef::Num(Some(&9))),
        ]
    );
    for (index, expected) in [
        Some(Input::Num(-1)),
        Some(Input::str(long.clone())),
        None,
        Some(Input::Num(9)),
    ]
    .into_iter()
    .enumerate()
    {
        assert_eq!(vector.get_value_slow(index)?.get::<Input>()?, expected, "row {index}");
    }
    let children = vector.children().unwrap();
    let tags: Vec<_> = children
        .get_vector_at::<u8>(0)?
        .reader()?
        .iter()
        .map(|tag| tag.copied())
        .collect();
    assert_eq!(tags, [Some(0), Some(1), None, Some(0)]);
    let numbers: Vec<_> = children
        .get_vector_at::<i16>(1)?
        .reader()?
        .iter()
        .map(|v| v.copied())
        .collect();
    assert_eq!(numbers, [Some(-1), None, None, Some(9)]);
    let strings: Vec<_> = children.get_vector_at::<String>(2)?.reader()?.iter().collect();
    assert_eq!(strings, [None, Some(long.as_str()), None, None]);
    Ok(())
}

#[test]
fn derived_union_writes_null_members_and_duplicate_types() -> Result<()> {
    let conn = connect()?;
    let types = [Nullable::logical_type(&conn)?, Same::logical_type(&conn)?];
    let mut chunk = DataChunk::create(&conn, &types, true)?;
    {
        let [nullable, same] = chunk.get_vector_array_mut([0, 1])?;
        let mut nullable = nullable.cast::<Nullable>()?;
        nullable.set_size(3)?;
        let mut writer = nullable.writer()?;
        writer.set(0, &Nullable::Key(None))?;
        writer.set(1, &Nullable::Label(Some("label".to_owned())))?;
        writer.set(2, &Nullable::Label(None))?;
        let mut same = same.cast::<Same>()?;
        same.set_size(2)?;
        let mut writer = same.writer()?;
        writer.set(0, &Same::Right(5))?;
        writer.set(1, &Same::Left(5))?;
    }
    let nullable = chunk.get_vector_at::<Nullable>(0)?;
    let rows: Vec<_> = nullable.reader()?.iter().collect();
    assert_eq!(
        rows,
        [
            Some(NullableRef::Key(None)),
            Some(NullableRef::Label(Some("label"))),
            Some(NullableRef::Label(None)),
        ]
    );
    assert_eq!(
        nullable.get_value_slow(2)?.get::<Nullable>()?,
        Some(Nullable::Label(None))
    );
    let same = chunk.get_vector_at::<Same>(1)?;
    assert_eq!(same.get_value_slow(0)?.get::<Same>()?, Some(Same::Right(5)));
    assert_eq!(same.get_value_slow(1)?.get::<Same>()?, Some(Same::Left(5)));
    Ok(())
}

#[test]
fn derived_union_nests_in_structs_and_lists() -> Result<()> {
    let conn = connect()?;
    let records = [
        Record {
            id: 1,
            input: Some(Input::Num(2)),
        },
        Record { id: 2, input: None },
        Record {
            id: 3,
            input: Some(Input::str("three".to_owned())),
        },
    ];
    let list = List(vec![Some(Input::Num(1)), None, Some(Input::str("two".to_owned()))]);
    let types = [Record::logical_type(&conn)?, List::<Input>::logical_type(&conn)?];
    let mut chunk = DataChunk::create(&conn, &types, true)?;
    {
        let [record, items] = chunk.get_vector_array_mut([0, 1])?;
        let mut record = record.cast::<Record>()?;
        record.set_size(records.len())?;
        let mut writer = record.writer()?;
        for (index, row) in records.iter().enumerate() {
            writer.set(index, row)?;
        }
        let mut items = items.cast::<List<Input>>()?;
        items.set_size(1)?;
        // SAFETY: the single row below writes all three elements.
        unsafe { items.set_child_size(3)? };
        items.writer()?.set(0, &list)?;
    }
    let vector = chunk.get_vector_at::<Record>(0)?;
    for (index, record) in records.iter().enumerate() {
        assert_eq!(vector.get_value_slow(index)?.get::<Record>()?.as_ref(), Some(record));
    }
    let row = vector.reader()?.get(2)?.unwrap();
    assert_eq!(row.input, Some(InputRef::str(Some("three"))));
    assert!(vector.reader()?.get(1)?.unwrap().input.is_none());
    let items = chunk.get_vector_at::<List<Input>>(1)?;
    assert_eq!(
        items.reader()?.get(0)?.unwrap().iter().collect::<Vec<_>>(),
        [Some(InputRef::Num(Some(&1))), None, Some(InputRef::str(Some("two")))]
    );
    assert_eq!(items.get_value_slow(0)?.get::<List<Input>>()?, Some(list));
    let value = records[0].value(&conn)?;
    assert_eq!(value.get::<Record>()?.as_ref(), Some(&records[0]));
    Ok(())
}

#[test]
fn derived_union_refs_write_vectors() -> Result<()> {
    let conn = connect()?;
    let source = conn
        .query(
            "SELECT CASE i
                        WHEN 0 THEN union_value(Num := 7::SMALLINT)::UNION(Num SMALLINT, str VARCHAR)
                        WHEN 1 THEN union_value(str := 'a string longer than the inline storage')
                            ::UNION(Num SMALLINT, str VARCHAR)
                        WHEN 2 THEN NULL
                        ELSE union_value(str := 'duck')::UNION(Num SMALLINT, str VARCHAR)
                    END,
                    CASE i % 3
                        WHEN 0 THEN union_value(Key := NULL::INTEGER)::UNION(Key INTEGER, Label VARCHAR)
                        WHEN 1 THEN union_value(Label := 'label')::UNION(Key INTEGER, Label VARCHAR)
                        ELSE union_value(Label := NULL::VARCHAR)::UNION(Key INTEGER, Label VARCHAR)
                    END,
                    CASE WHEN i % 2 = 0 THEN union_value(\"Left\" := i)::UNION(\"Left\" BIGINT, \"Right\" VARCHAR)
                         ELSE union_value(\"Right\" := 'right ' || i)::UNION(\"Left\" BIGINT, \"Right\" VARCHAR) END
             FROM range(4) t(i)",
            Parameters::None,
        )?
        .next_chunk()?
        .unwrap();
    let inputs = source.get_vector_at::<Input>(0)?;
    let nullables = source.get_vector_at::<Nullable>(1)?;
    let eithers = source.get_vector_at::<Either<i64, String>>(2)?;
    let (inputs, nullables, eithers) = (inputs.reader()?, nullables.reader()?, eithers.reader()?);

    let types = [
        Input::logical_type(&conn)?,
        Nullable::logical_type(&conn)?,
        Either::<i64, String>::logical_type(&conn)?,
    ];
    let mut chunk = DataChunk::create(&conn, &types, true)?;
    {
        let [input, nullable, either] = chunk.get_vector_array_mut([0, 1, 2])?;
        let mut input = input.cast::<InputRef<'_>>()?;
        input.set_size(4)?;
        let mut writer = input.writer()?;
        // Rewriting shifted rows switches their active member, which must NULL the previous one.
        for row in 0..4 {
            writer.set_option(row, inputs.get((row + 1) % 4)?.as_ref())?;
        }
        for row in 0..4 {
            writer.set_option(row, inputs.get(row)?.as_ref())?;
        }
        let mut nullable = nullable.cast::<NullableRef<'_>>()?;
        nullable.set_size(4)?;
        let mut writer = nullable.writer()?;
        for (row, value) in nullables.iter().enumerate() {
            writer.set_option(row, value.as_ref())?;
        }
        let mut either = either.cast::<EitherRef<'_, i64, String>>()?;
        either.set_size(4)?;
        let mut writer = either.writer()?;
        for (row, value) in eithers.iter().enumerate() {
            writer.set_option(row, value.as_ref())?;
        }
    }
    let copied = chunk.get_vector_at::<Input>(0)?;
    assert_eq!(
        copied.reader()?.iter().collect::<Vec<_>>(),
        inputs.iter().collect::<Vec<_>>()
    );
    assert_eq!(
        copied.get_value_slow(1)?.get::<Input>()?,
        Some(Input::str("a string longer than the inline storage".to_owned()))
    );
    let numbers: Vec<_> = copied
        .children()
        .unwrap()
        .get_vector_at::<i16>(1)?
        .reader()?
        .iter()
        .collect();
    assert_eq!(numbers, [Some(&7), None, None, None]);
    let copied = chunk.get_vector_at::<Nullable>(1)?;
    assert_eq!(
        copied.reader()?.iter().collect::<Vec<_>>(),
        [
            Some(NullableRef::Key(None)),
            Some(NullableRef::Label(Some("label"))),
            Some(NullableRef::Label(None)),
            Some(NullableRef::Key(None)),
        ]
    );
    let copied = chunk.get_vector_at::<Either<i64, String>>(2)?;
    assert_eq!(
        copied.reader()?.iter().collect::<Vec<_>>(),
        eithers.iter().collect::<Vec<_>>()
    );
    assert_eq!(
        copied.get_value_slow(3)?.get::<Either<i64, String>>()?,
        Some(Either::Right("right 3".to_owned()))
    );
    Ok(())
}

#[test]
fn derived_union_refs_reject_mismatched_schemas() -> Result<()> {
    let conn = connect()?;
    let mut chunk = DataChunk::create(&conn, &[Nullable::logical_type(&conn)?], true)?;
    assert!(chunk.get_vector_at_mut::<InputRef<'_>>(0).is_err());
    assert!(chunk.get_vector_at_mut::<NullableRef<'_>>(0).is_ok());
    Ok(())
}

#[derive(Debug, AsDuckDBUnion)]
#[allow(dead_code)]
enum Fallible {
    X(i32),
    Bits(BitValue),
}

#[test]
fn failed_union_writes_leave_the_row_null() -> Result<()> {
    let conn = connect()?;
    let mut chunk = DataChunk::create(&conn, &[Fallible::logical_type(&conn)?], true)?;
    let mut vector = chunk.get_vector_at_mut::<Fallible>(0)?;
    vector.set_size(2)?;
    {
        let mut writer = vector.writer()?;
        writer.set(0, &Fallible::X(1))?;
        writer.set(1, &Fallible::Bits(BitValue(vec![0, 1])))?;
        assert!(writer.set(0, &Fallible::Bits(BitValue(vec![]))).is_err());
        assert!(writer.set(1, &Fallible::Bits(BitValue(vec![]))).is_err());
    }
    let reader = vector.reader()?;
    let children = vector.children().unwrap();
    let tags = children.get_vector_at::<u8>(0)?.reader()?;
    let numbers = children.get_vector_at::<i32>(1)?.reader()?;
    for row in 0..2 {
        assert!(reader.get(row)?.is_none(), "row {row}");
        assert!(tags.get(row)?.is_none(), "row {row}");
        assert!(numbers.get(row)?.is_none(), "row {row}");
    }
    Ok(())
}

#[test]
fn failed_union_ref_writes_leave_the_row_null() -> Result<()> {
    let conn = connect()?;
    let mut chunk = DataChunk::create(&conn, &[Fallible::logical_type(&conn)?], true)?;
    {
        let mut vector = chunk.get_vector_at_mut::<FallibleRef<'_>>(0)?;
        vector.set_size(2)?;
        let mut writer = vector.writer()?;
        writer.set(0, &FallibleRef::X(Some(&1)))?;
        writer.set(1, &FallibleRef::Bits(Some(&[0, 1])))?;
        assert!(writer.set(0, &FallibleRef::Bits(Some(&[]))).is_err());
    }
    let vector = chunk.get_vector_at::<Fallible>(0)?;
    assert!(vector.reader()?.get(0)?.is_none());
    assert!(
        vector
            .children()
            .unwrap()
            .get_vector_at::<i32>(1)?
            .reader()?
            .get(0)?
            .is_none()
    );
    assert_eq!(vector.reader()?.get(1)?, Some(FallibleRef::Bits(Some(&[0, 1]))));
    Ok(())
}

#[test]
fn derived_union_refs_implement_standard_traits() -> Result<()> {
    fn assert_traits<T: Clone + Copy + PartialEq + Eq + std::hash::Hash + std::fmt::Debug>() {}
    assert_traits::<InputRef<'static>>();
    assert_traits::<NullableRef<'static>>();
    assert_traits::<EitherRef<'static, i32, String>>();
    assert_traits::<FlagRef<'static>>();

    let conn = connect()?;
    conn.execute(
        "CREATE TYPE Same AS UNION(\"Left\" INTEGER, \"Right\" INTEGER)",
        Parameters::None,
    )?;
    let chunk = conn
        .query(
            "SELECT unnest([union_value(Left := 1)::Same, union_value(Right := 1)::Same, union_value(Left := 1)::Same, NULL])",
            Parameters::None,
        )?
        .next_chunk()?
        .unwrap();
    let rows: Vec<_> = chunk.get_vector_at::<Same>(0)?.reader()?.iter().flatten().collect();
    assert_eq!(
        rows,
        [
            SameRef::Left(Some(&1)),
            SameRef::Right(Some(&1)),
            SameRef::Left(Some(&1))
        ]
    );
    #[allow(clippy::clone_on_copy)]
    let copy = rows[0].clone();
    assert_eq!(copy, rows[2]);
    assert_ne!(rows[0], rows[1]);
    assert_eq!(format!("{:?}", rows[1]), "Right(Some(1))");
    assert_eq!(std::collections::HashSet::from([rows[0], rows[1], rows[2]]).len(), 2);
    Ok(())
}

/// Describes each row from the typed reader, records storage kinds, and writes the input back.
struct InputProbe(Arc<AtomicUsize>);

fn storage_flag(kind: StorageKind) -> usize {
    match kind {
        StorageKind::Flat => 1,
        StorageKind::Constant => 2,
        StorageKind::Dictionary => 4,
        StorageKind::Other => 8,
    }
}

impl ScalarCallbacks for InputProbe {
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
        let vector = input.get_vector_at::<Input>(0)?;
        self.0.fetch_or(storage_flag(vector.storage_kind()), Ordering::Relaxed);
        for index in 0..vector.children().map_or(0, VectorCollection::col_count) {
            let child = vector.children().unwrap().get_untyped_vector_at(index)?;
            self.0.fetch_or(storage_flag(child.storage_kind()), Ordering::Relaxed);
        }
        let mut output = output.cast::<String>()?;
        output.set_size(vector.len())?;
        let mut writer = output.writer()?;
        for (index, row) in vector.reader()?.iter().enumerate() {
            let text = row.map(|row| match row {
                InputRef::Num(value) => format!("Num:{}", value.map_or("NULL".to_owned(), i16::to_string)),
                InputRef::str(value) => format!("str:{}", value.unwrap_or("NULL")),
            });
            writer.set_option(index, text.as_deref())?;
        }
        Ok(())
    }
}

/// Builds an `Input` from an integer: odd keys pick `Num`, even keys `str`.
struct ToInput;

impl ScalarCallbacks for ToInput {
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
        let keys = input.get_vector_at::<i32>(0)?;
        let mut output = output.cast::<Shape>()?;
        output.set_size(keys.len())?;
        let mut writer = output.writer()?;
        for (index, key) in keys.reader()?.iter().enumerate() {
            let key = *key.unwrap();
            let row = match key % 3 {
                0 => Shape::Nested(Input::Num(key as i16)),
                1 => Shape::Nested(Input::str(format!("key {key}"))),
                _ => Shape::Point(Point {
                    x: key,
                    label: "point".to_owned(),
                }),
            };
            writer.set(index, &row)?;
        }
        Ok(())
    }
}

#[test]
fn derived_union_scalar_functions_read_and_write() -> Result<()> {
    let conn = connect()?;
    let kinds = Arc::new(AtomicUsize::new(0));
    ScalarFunctionBuilder::new(
        "input_probe",
        SignatureBuilder::new()
            .param(FunctionParameter::new::<Input>("u"))
            .return_type(String::logical_type(&conn)?),
        InputProbe(kinds.clone()),
    )
    .register(&conn)?;
    ScalarFunctionBuilder::new(
        "to_shape",
        SignatureBuilder::new()
            .param(FunctionParameter::new::<i32>("key"))
            .return_type(Shape::logical_type(&conn)?),
        ToInput,
    )
    .register(&conn)?;
    conn.execute(
        "CREATE TYPE Input AS UNION(Num SMALLINT, str VARCHAR)",
        Parameters::None,
    )?;
    conn.execute(
        "CREATE TABLE inputs AS
         SELECT i, CASE WHEN i % 7 = 0 THEN NULL
                        WHEN i % 5 = 0 THEN union_value(Num := NULL::SMALLINT)::Input
                        WHEN i % 2 = 0 THEN union_value(Num := (i % 1000)::SMALLINT)::Input
                        ELSE union_value(str := 'row ' || i)::Input END AS u
         FROM range(5000) t(i)",
        Parameters::None,
    )?;
    let expected = "CASE WHEN u IS NULL THEN NULL
                         WHEN union_tag(u) = 'Num' THEN 'Num:' || coalesce(union_extract(u, 'Num')::VARCHAR, 'NULL')
                         ELSE 'str:' || coalesce(union_extract(u, 'str'), 'NULL') END";
    for sql in [
        format!("SELECT count(*) FILTER (WHERE input_probe(u) IS DISTINCT FROM {expected}), count(*) FROM inputs"),
        format!(
            "SELECT count(*) FILTER (WHERE input_probe(u) IS DISTINCT FROM {expected}), count(*)
             FROM inputs WHERE i % 3 = 0"
        ),
        format!(
            "SELECT count(*) FILTER (WHERE input_probe(u) IS DISTINCT FROM {expected}), count(*)
             FROM (SELECT union_value(str := 'constant')::UNION(Num SMALLINT, str VARCHAR) AS u FROM range(3000))"
        ),
    ] {
        let chunk = conn.query(sql.as_str(), Parameters::None)?.next_chunk()?.unwrap();
        assert_eq!(chunk.get_vector_at::<i64>(0)?.reader()?.get(0)?, Some(&0), "{sql}");
        assert!(*chunk.get_vector_at::<i64>(1)?.reader()?.get(0)?.unwrap() > 0, "{sql}");
    }
    assert_eq!(kinds.load(Ordering::Relaxed) & 7, 7);

    // Inactive members must read as NULL through field extraction too, including STRUCT descendants.
    let chunk = conn
        .query(
            "SELECT count(*) FILTER (WHERE k % 3 <> 2 AND union_extract(s, 'Point').x IS NOT NULL),
                    count(*) FILTER (WHERE k % 3 = 2 AND union_extract(s, 'Point').x = k),
                    count(*) FILTER (WHERE k % 3 = 0 AND union_extract(union_extract(s, 'Nested'), 'Num') = k % 3000),
                    count(*) FILTER (WHERE k % 3 = 1 AND union_tag(union_extract(s, 'Nested'))::VARCHAR = 'str'),
                    count(*) FILTER (WHERE k % 3 <> 2 AND union_extract(s, 'Point') IS NOT NULL)
             FROM (SELECT k, to_shape(k::INTEGER) AS s FROM range(3000) t(k))",
            Parameters::None,
        )?
        .next_chunk()?
        .unwrap();
    let counts: Vec<_> = (0..5)
        .map(|index| Ok(*chunk.get_vector_at::<i64>(index)?.reader()?.get(0)?.unwrap()))
        .collect::<Result<_>>()?;
    assert_eq!(counts, [0, 1000, 1000, 1000, 0]);
    Ok(())
}

#[derive(Debug, Clone, PartialEq, AsDuckDBUnion)]
enum Tagged {
    #[duckdb(rename = "num")]
    Num(i16),
    #[duckdb(rename = "free text")]
    Text(String),
}

#[test]
fn renamed_union_variants_use_their_sql_names() -> Result<()> {
    let conn = connect()?;
    let logical_type = Tagged::logical_type(&conn)?;
    assert_eq!(
        logical_type,
        LogicalType::from_text(&conn, "UNION(num SMALLINT, \"free text\" VARCHAR)")?
    );
    let input = Tagged::Text("duck".to_owned());
    assert_eq!(input.value(&conn)?.get::<Tagged>()?, Some(input.clone()));
    let chunk = conn
        .query(
            "SELECT union_tag($1)::VARCHAR, union_value(\"free text\" := 'goose')::UNION(num SMALLINT, \"free text\" VARCHAR)",
            Parameters::positional(&[&input]),
        )?
        .next_chunk()?
        .unwrap();
    assert_eq!(chunk.get_vector_at::<String>(0)?.reader()?.get(0)?, Some("free text"));
    let vector = chunk.get_vector_at::<Tagged>(1)?;
    let row = vector.reader()?.get(0)?.unwrap();
    assert_eq!(row, TaggedRef::Text(Some("goose")));
    assert_eq!(format!("{row:?}"), "Text(Some(\"goose\"))");
    assert_eq!(
        vector.get_value_slow(0)?.get::<Tagged>()?,
        Some(Tagged::Text("goose".to_owned()))
    );
    let chunk = conn
        .query(
            "SELECT union_value(Text := 'goose')::UNION(Num SMALLINT, Text VARCHAR)",
            Parameters::None,
        )?
        .next_chunk()?
        .unwrap();
    assert!(chunk.get_vector_at::<Tagged>(0).is_err());
    Ok(())
}

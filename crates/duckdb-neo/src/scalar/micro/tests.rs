use std::sync::Arc;

use super::*;
use crate::{
    Error, Parameters,
    enums::FunctionProperty,
    environment::{Environment, StorageLocation},
    error::DuckDBError,
    instance::{AttachBuilder, InstanceBuilder},
    types::List,
};

#[test]
fn inferred_signature_and_sized_batches() -> Result<()> {
    let offset = Arc::new(7_i64);
    let weak = Arc::downgrade(&offset);
    let function = MicroScalar::create(
        "micro_add",
        move |ctx, input: Vector<'_, i32>, mut output: Vector<'_, i64>| {
            assert_eq!(input.logical_type(), &i32::logical_type(ctx)?);
            assert_eq!(output.logical_type(), &i64::logical_type(ctx)?);
            assert_eq!(output.len(), input.len());
            let mut writer = output.writer()?;
            for (row, value) in input.reader()?.iter().enumerate() {
                writer.set(row, value.map(|value| i64::from(*value) + *offset))?;
            }
            Ok(())
        },
    );

    {
        let env = Environment::new()?;
        let conn = env.open(StorageLocation::InMemory)?.connect()?;
        function.register(&conn)?;
        assert!(weak.upgrade().is_some());

        let chunk = conn
            .query("SELECT micro_add(input := 35)", Parameters::None)?
            .next_chunk()?
            .unwrap();
        assert_eq!(chunk.get_vector_at::<i64>(0)?.reader()?.get(0)?, Some(&42));

        let mut result = conn.query(
            "SELECT micro_add(CASE WHEN i % 5 = 0 THEN NULL ELSE i::INTEGER END) \
             FROM range(10000) t(i) WHERE i % 3 <> 0 ORDER BY i",
            Parameters::None,
        )?;
        let mut actual = Vec::new();
        while let Some(chunk) = result.next_chunk()? {
            actual.extend(chunk.get_vector_at::<i64>(0)?.reader()?.iter().map(|v| v.copied()));
        }
        let expected: Vec<_> = (0..10000)
            .filter(|i| i % 3 != 0)
            .map(|i| (i % 5 != 0).then_some(i + 7))
            .collect();
        assert_eq!(actual, expected);
    }
    assert!(weak.upgrade().is_none());
    Ok(())
}

#[test]
fn nested_input_and_string_output() -> Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    MicroScalar::create(
        "micro_list_length",
        |_ctx, input: Vector<'_, List<i32>>, mut output: Vector<'_, String>| {
            let mut writer = output.writer()?;
            for (row, value) in input.reader()?.iter().enumerate() {
                let text = value.map(|list| format!("length: {}", list.iter().count()));
                writer.set(row, text.as_deref())?;
            }
            Ok(())
        },
    )
    .register(&conn)?;

    let chunk = conn
        .query(
            "SELECT micro_list_length(v) \
             FROM (VALUES ([1, NULL, 3]), ([]::INTEGER[]), (NULL::INTEGER[])) t(v)",
            Parameters::None,
        )?
        .next_chunk()?
        .unwrap();
    assert_eq!(
        chunk.get_vector_at::<String>(0)?.reader()?.iter().collect::<Vec<_>>(),
        [Some("length: 3"), Some("length: 0"), None]
    );
    Ok(())
}

#[test]
fn properties_control_null_handling() -> Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    for (name, special) in [("micro_default_nulls", false), ("micro_special_nulls", true)] {
        MicroScalar::create(name, |_ctx, input: Vector<'_, i32>, mut output: Vector<'_, i32>| {
            let mut writer = output.writer()?;
            for (row, value) in input.reader()?.iter().enumerate() {
                writer.set(row, Some(value.copied().unwrap_or(42)))?;
            }
            Ok(())
        })
        .set_property(FunctionProperty::HasSpecialNullHandling(special))
        .register(&conn)?;
    }
    let chunk = conn
        .query(
            "SELECT micro_default_nulls(NULL::INTEGER), micro_special_nulls(NULL::INTEGER)",
            Parameters::None,
        )?
        .next_chunk()?
        .unwrap();
    assert_eq!(chunk.get_vector_at::<i32>(0)?.reader()?.get(0)?, None);
    assert_eq!(chunk.get_vector_at::<i32>(1)?.reader()?.get(0)?, Some(&42));
    Ok(())
}

#[test]
fn callback_errors_propagate() -> Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    MicroScalar::create(
        "micro_error",
        |_ctx, _input: Vector<'_, i32>, _output: Vector<'_, i32>| Err(Error::invalid_input("micro failed")),
    )
    .register(&conn)?;

    let error = conn
        .query("SELECT micro_error(1)", Parameters::None)
        .and_then(|mut result| result.drain())
        .expect_err("callback error must reach the caller");
    assert_eq!(error.code, DuckDBError::InputInvalid);
    assert!(error.message.contains("micro failed"), "{error}");
    conn.query("SELECT 42", Parameters::None)?.drain()?;
    Ok(())
}

#[test]
fn callback_panics_are_caught() -> Result<()> {
    let env = Environment::new()?;
    let instance = InstanceBuilder::new().option("threads", "1").build(&env)?;
    instance.attach(AttachBuilder::new(StorageLocation::InMemory).make_default())?;
    let conn = instance.connect()?;
    MicroScalar::create(
        "micro_panic",
        |_ctx, _input: Vector<'_, i32>, _output: Vector<'_, i32>| panic!("micro panicked"),
    )
    .register(&conn)?;

    let error = conn
        .query("SELECT micro_panic(1)", Parameters::None)
        .and_then(|mut result| result.drain())
        .expect_err("panic must reach the caller as an error");
    assert!(error.message.contains("micro panicked"), "{error}");
    Ok(())
}

#[test]
fn captures_are_dropped_without_registration() -> Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    for register in [false, true] {
        let captured = Arc::new(42_i32);
        let weak = Arc::downgrade(&captured);
        let function = MicroScalar::create("", move |_ctx, input: Vector<'_, i32>, mut output: Vector<'_, i32>| {
            let mut writer = output.writer()?;
            for row in 0..input.len() {
                writer.set(row, Some(*captured))?;
            }
            Ok(())
        });
        if register {
            let error = function.register(&conn).expect_err("empty name must be rejected");
            assert!(error.message.contains("Function name cannot be empty"), "{error}");
        } else {
            drop(function);
        }
        assert!(weak.upgrade().is_none());
    }
    Ok(())
}

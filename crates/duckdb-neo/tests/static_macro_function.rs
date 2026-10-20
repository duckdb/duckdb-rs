//! Tests macro registration through a statically linked extension.
//!
//! Registration is process-wide: every instance opened by this binary loads the extension.
#![cfg(feature = "build-extension-static")]

use std::sync::OnceLock;

use duckdb_loadable_macros::duckdb_entrypoint_c_api_v2;
use duckdb_neo::{
    DuckDBType, Parameters,
    connection::Connection,
    environment::{Environment, StorageLocation},
    extension::{Extension, register_static_extension},
    macro_function::{MacroBody, MacroFunctionBuilder},
    signature::{FunctionDocumentationBuilder, FunctionParameter, FunctionParameterKind, SignatureBuilder},
    types::Any,
};

static REJECTED: OnceLock<Vec<(&'static str, String)>> = OnceLock::new();

fn param_a() -> SignatureBuilder {
    SignatureBuilder::new().param(FunctionParameter::new::<i32>("a"))
}

#[duckdb_entrypoint_c_api_v2(extension_name = "neo_static_macros", api_version = "v2.0.0")]
fn entrypoint(extension: &Extension) -> duckdb_neo::Result<()> {
    MacroFunctionBuilder::new(
        "neo_add",
        param_a()
            .param(FunctionParameter::with_default("b", 10_i32))
            .documentation(
                FunctionDocumentationBuilder::new()
                    .description("Adds two integers.")
                    .add_example("", "neo_add(1, 2)")
                    .add_category("numeric"),
            ),
        MacroBody::Scalar("a + b".into()),
    )
    .register(extension)?;

    // ANY leaves the parameter untyped.
    MacroFunctionBuilder::new(
        "neo_pair",
        SignatureBuilder::new().param(FunctionParameter::new::<Any>("x")),
        MacroBody::Scalar("[x, x]".into()),
    )
    .register(extension)?;

    MacroFunctionBuilder::new(
        "neo_squares",
        SignatureBuilder::new().param(FunctionParameter::new::<i64>("n")),
        MacroBody::Table("SELECT i, i * i AS square FROM range(n) t(i)".into()),
    )
    .register(extension)?;

    // Table names resolve at invocation, so the table need not exist during registration.
    MacroFunctionBuilder::new(
        "neo_top_scores",
        SignatureBuilder::new().param(FunctionParameter::with_default("min_score", 50_i32)),
        MacroBody::Table("SELECT name FROM scores WHERE score >= min_score ORDER BY score DESC".into()),
    )
    .register(extension)?;

    let invalid = [
        (
            "syntax error",
            MacroFunctionBuilder::new("neo_bad_syntax", param_a(), MacroBody::Scalar("a +".into())),
        ),
        (
            "empty body",
            MacroFunctionBuilder::new("neo_bad_empty", param_a(), MacroBody::Scalar("".into())),
        ),
        (
            "two expressions",
            MacroFunctionBuilder::new("neo_bad_list", param_a(), MacroBody::Scalar("a, a".into())),
        ),
        (
            "not a SELECT",
            MacroFunctionBuilder::new(
                "neo_bad_insert",
                param_a(),
                MacroBody::Table("INSERT INTO t VALUES (1)".into()),
            ),
        ),
        (
            "return type",
            MacroFunctionBuilder::new(
                "neo_bad_return",
                param_a().return_type(i32::logical_type(extension)?),
                MacroBody::Scalar("a".into()),
            ),
        ),
        (
            "variadic",
            MacroFunctionBuilder::new(
                "neo_bad_variadic",
                param_a().param(FunctionParameter::vararg::<i32>("rest")),
                MacroBody::Scalar("a".into()),
            ),
        ),
        (
            "named only",
            MacroFunctionBuilder::new(
                "neo_bad_named",
                SignatureBuilder::new().param(FunctionParameter::new::<i32>("a").kind(FunctionParameterKind::Named)),
                MacroBody::Scalar("a".into()),
            ),
        ),
        (
            "duplicate",
            MacroFunctionBuilder::new("neo_add", param_a(), MacroBody::Scalar("a".into())),
        ),
    ];
    let rejected = invalid
        .into_iter()
        .map(|(case, builder)| {
            let error = builder
                .register(extension)
                .expect_err(&format!("the {case} macro should be rejected"));
            (case, error.message)
        })
        .collect();
    let _ = REJECTED.set(rejected);

    Ok(())
}

fn connect() -> duckdb_neo::Result<Connection> {
    // SAFETY: the describe function comes from `#[duckdb_entrypoint_c_api_v2]`.
    unsafe { register_static_extension(duckdb_extension_neo_static_macros_describe)? };
    Environment::new()?.open(StorageLocation::InMemory)?.connect()
}

fn query_rows(connection: &Connection, sql: &str) -> duckdb_neo::Result<Vec<String>> {
    let sql = format!("SELECT COLUMNS(*)::VARCHAR FROM ({sql})");
    let mut result = connection.query(sql.as_str(), Parameters::None)?;
    let mut rows = Vec::new();
    while let Some(chunk) = result.next_chunk()? {
        let columns = (0..chunk.col_count())
            .map(|index| chunk.get_vector_at::<String>(index))
            .collect::<duckdb_neo::Result<Vec<_>>>()?;
        for row in 0..chunk.row_count()? {
            let values = columns
                .iter()
                .map(|column| Ok(column.get(row)?.unwrap_or("NULL").to_string()))
                .collect::<duckdb_neo::Result<Vec<_>>>()?;
            rows.push(values.join("|"));
        }
    }
    Ok(rows)
}

#[test]
fn test_scalar_macro() -> duckdb_neo::Result<()> {
    let connection = connect()?;

    assert_eq!(
        query_rows(
            &connection,
            "SELECT neo_add(1, 2), neo_add(5), neo_add(b := 1, a := 2), neo_add(NULL)"
        )?,
        ["3|15|3|NULL"]
    );
    // Typed macro parameters allow only implicit casts, as with SQL `CREATE MACRO`.
    assert_eq!(
        query_rows(&connection, "SELECT neo_add(1::TINYINT), typeof(neo_add(1::TINYINT))")?,
        ["11|INTEGER"]
    );
    let error = connection
        .execute("SELECT neo_add(1.5::DOUBLE)", Parameters::None)
        .unwrap_err();
    assert!(
        error.message.contains("does not support the supplied arguments"),
        "{error}"
    );

    assert_eq!(
        query_rows(&connection, "SELECT neo_pair(1), neo_pair('x'), typeof(neo_pair(1.5))")?,
        ["[1, 1]|[x, x]|DECIMAL(2,1)[]"]
    );
    Ok(())
}

#[test]
fn test_table_macro() -> duckdb_neo::Result<()> {
    let connection = connect()?;

    assert_eq!(
        query_rows(&connection, "SELECT * FROM neo_squares(4)")?,
        ["0|0", "1|1", "2|4", "3|9"]
    );

    connection.execute(
        "CREATE TABLE scores AS FROM (VALUES ('ann', 90), ('bob', 40), ('cid', 70)) t(name, score)",
        Parameters::None,
    )?;
    assert_eq!(query_rows(&connection, "FROM neo_top_scores()")?, ["ann", "cid"]);
    assert_eq!(
        query_rows(&connection, "FROM neo_top_scores(min_score := 0)")?,
        ["ann", "cid", "bob"]
    );
    Ok(())
}

#[test]
fn test_macro_catalog_entries() -> duckdb_neo::Result<()> {
    let connection = connect()?;

    assert_eq!(
        query_rows(
            &connection,
            "SELECT function_name, function_type, parameters, parameter_types, description, examples, categories \
             FROM duckdb_functions() WHERE function_name LIKE 'neo\\_%' ESCAPE '\\' ORDER BY function_name",
        )?,
        [
            "neo_add|macro|[a, b]|[INTEGER, INTEGER]|Adds two integers.|['neo_add(1, 2)']|[numeric]",
            "neo_pair|macro|[x]|[NULL]|NULL|[]|[]",
            "neo_squares|table_macro|[n]|[BIGINT]|NULL|[]|[]",
            "neo_top_scores|table_macro|[min_score]|[INTEGER]|NULL|[]|[]",
        ]
    );

    // Extension-owned macros cannot be dropped directly.
    let error = connection.execute("DROP MACRO neo_add", Parameters::None).unwrap_err();
    assert!(error.message.contains("Cannot drop internal catalog entry"), "{error}");
    Ok(())
}

#[test]
fn test_invalid_macros_are_rejected() -> duckdb_neo::Result<()> {
    connect()?;

    let rejected = REJECTED.get().expect("the extension was loaded");
    let expected = [
        ("syntax error", "syntax error"),
        ("empty body", "A body must be set"),
        ("two expressions", "must be a single expression"),
        ("not a SELECT", "must be a single SELECT query"),
        ("return type", "cannot set a return type"),
        ("variadic", "cannot have variadic parameters"),
        ("named only", "must be a standard parameter"),
        ("duplicate", "already exists"),
    ];
    assert_eq!(rejected.len(), expected.len());
    for ((case, message), (expected_case, expected_message)) in rejected.iter().zip(expected) {
        assert_eq!(*case, expected_case);
        assert!(message.contains(expected_message), "{case}: {message}");
    }
    Ok(())
}

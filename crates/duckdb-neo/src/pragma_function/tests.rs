use std::sync::{
    Arc,
    atomic::{AtomicI64, AtomicUsize, Ordering},
};

use crate::{
    DuckDBType, FromValue, Parameters,
    connection::Connection,
    environment::{Environment, StorageLocation},
    error::{DuckDBError, Error},
    signature::{FunctionDocumentationBuilder, FunctionParameter, FunctionParameterKind, SignatureBuilder},
};

use super::*;

fn connect() -> Result<(Environment, Connection)> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    Ok((env, conn))
}

fn query_i32(conn: &Connection, sql: &str) -> Result<Vec<Option<i32>>> {
    let mut result = conn.query(sql, Parameters::None)?;
    let mut rows = Vec::new();
    while let Some(chunk) = result.next_chunk()? {
        rows.extend(chunk.get_vector_at::<i32>(0)?.reader()?.iter().map(|v| v.copied()));
    }
    Ok(rows)
}

fn constant<T: FromValue>(arguments: &BindArguments<'_>, index: usize) -> Result<Option<T>> {
    let value = arguments.value(index)?.expect("pragma arguments are constant");
    T::from_value(&value)
}

/// `PRAGMA rs_answer` runs `SELECT 42`.
struct Answer;

impl PragmaQueryCallbacks for Answer {
    fn query(&self, _context: &Context, arguments: BindArguments<'_>) -> Result<String> {
        assert!(arguments.is_empty()?);
        Ok("SELECT 42".into())
    }
}

/// `PRAGMA rs_shift(n, shift_by := 0)` runs `SELECT n + shift_by`.
struct Shift;

impl PragmaQueryCallbacks for Shift {
    fn query(&self, _context: &Context, arguments: BindArguments<'_>) -> Result<String> {
        let shift_by = arguments.index("shift_by")?.expect("the shift_by argument is present");
        let n = constant::<i32>(&arguments, 0)?.unwrap();
        let by = constant::<i32>(&arguments, shift_by)?.unwrap();
        Ok(format!("SELECT {}::INTEGER", n + by))
    }
}

/// `PRAGMA rs_bump(*amounts)` adds every amount to a shared counter.
struct Bump {
    total: Arc<AtomicI64>,
    calls: Arc<AtomicUsize>,
}

impl PragmaExecCallbacks for Bump {
    fn exec(&self, _context: &Context, arguments: BindArguments<'_>) -> Result<()> {
        for i in 0..arguments.len()? {
            self.total
                .fetch_add(constant::<i64>(&arguments, i)?.unwrap(), Ordering::SeqCst);
        }
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

struct Failing;

impl PragmaQueryCallbacks for Failing {
    fn query(&self, _context: &Context, _arguments: BindArguments<'_>) -> Result<String> {
        Err(Error::invalid_input("the query pragma failed"))
    }
}

struct Panicking;

impl PragmaExecCallbacks for Panicking {
    fn exec(&self, _context: &Context, _arguments: BindArguments<'_>) -> Result<()> {
        panic!("the exec pragma panicked")
    }
}

#[test]
fn test_query_pragma_without_parameters() -> Result<()> {
    let (_env, conn) = connect()?;

    let docs = FunctionDocumentationBuilder::new().description("Returns the answer");
    let signature = SignatureBuilder::new().documentation(docs);
    PragmaFunctionBuilder::query("rs_answer", signature, Answer).register(&conn)?;

    assert_eq!(query_i32(&conn, "PRAGMA rs_answer")?, [Some(42)]);
    assert_eq!(
        query_i32(
            &conn,
            "SELECT (description = 'Returns the answer')::INTEGER \
             FROM duckdb_functions() WHERE function_name = 'rs_answer'"
        )?,
        [Some(1)]
    );
    Ok(())
}

#[test]
fn test_query_pragma_arguments() -> Result<()> {
    let (_env, conn) = connect()?;

    let signature = SignatureBuilder::new()
        .param(FunctionParameter::new::<i32>("n"))
        .param(FunctionParameter::with_default("shift_by", 0_i32).kind(FunctionParameterKind::Named));
    PragmaFunctionBuilder::query("rs_shift", signature, Shift).register(&conn)?;

    assert_eq!(query_i32(&conn, "PRAGMA rs_shift(7)")?, [Some(7)]);
    assert_eq!(query_i32(&conn, "PRAGMA rs_shift(7, shift_by = 3)")?, [Some(10)]);
    assert!(conn.execute("PRAGMA rs_shift", Parameters::None).is_err());
    Ok(())
}

#[test]
fn test_exec_pragma() -> Result<()> {
    let (_env, conn) = connect()?;

    let total = Arc::new(AtomicI64::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let bump = Bump {
        total: total.clone(),
        calls: calls.clone(),
    };
    let signature = SignatureBuilder::new().param(FunctionParameter::vararg::<i64>("amounts"));
    PragmaFunctionBuilder::exec("rs_bump", signature, bump).register(&conn)?;

    assert_eq!(conn.execute("PRAGMA rs_bump(5)", Parameters::None)?, 0);
    assert_eq!((total.load(Ordering::SeqCst), calls.load(Ordering::SeqCst)), (5, 1));

    conn.execute("PRAGMA rs_bump(1, 2, 3)", Parameters::None)?;
    assert_eq!((total.load(Ordering::SeqCst), calls.load(Ordering::SeqCst)), (11, 2));
    Ok(())
}

#[test]
fn test_pragma_callback_errors() -> Result<()> {
    let (_env, conn) = connect()?;

    PragmaFunctionBuilder::query("rs_failing", SignatureBuilder::new(), Failing).register(&conn)?;
    let error = conn.execute("PRAGMA rs_failing", Parameters::None).unwrap_err();
    assert_eq!(error.code, DuckDBError::DUCKDB_V2_ERROR_INPUT_INVALID);
    assert!(error.message.contains("the query pragma failed"), "{error}");

    PragmaFunctionBuilder::exec("rs_panicking", SignatureBuilder::new(), Panicking).register(&conn)?;
    let error = conn.execute("PRAGMA rs_panicking", Parameters::None).unwrap_err();
    assert!(error.message.contains("the exec pragma panicked"), "{error}");
    Ok(())
}

#[test]
fn test_pragma_registration_errors() -> Result<()> {
    let (_env, conn) = connect()?;

    // A pragma produces no value.
    let signature = SignatureBuilder::new().return_type(i32::logical_type(&conn)?);
    assert!(
        PragmaFunctionBuilder::query("rs_typed", signature, Answer)
            .register(&conn)
            .is_err()
    );

    PragmaFunctionBuilder::query("rs_taken", SignatureBuilder::new(), Answer).register(&conn)?;
    assert!(
        PragmaFunctionBuilder::query("rs_taken", SignatureBuilder::new(), Answer)
            .register(&conn)
            .is_err()
    );
    Ok(())
}

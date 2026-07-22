use std::{
    error::Error,
    sync::atomic::{AtomicU64, Ordering},
};

use arrow::array::Array;
use libduckdb_sys::duckdb_string_t;

use crate::{
    Connection,
    arrow_interop::WritableVector,
    callback::test_support::{HazardousPanicPayload, PanickingDisplayError},
    core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId},
    types::DuckString,
};

use super::{ScalarFunctionSignature, VScalar};

fn unary_bigint_signature() -> Vec<ScalarFunctionSignature> {
    vec![ScalarFunctionSignature::exact(
        vec![LogicalTypeId::Bigint.into()],
        LogicalTypeId::Bigint.into(),
    )]
}

struct ErrorScalar {}

impl VScalar for ErrorScalar {
    type State = ();

    fn invoke(
        _: &Self::State,
        input: &mut DataChunkHandle,
        _: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let vector = input.flat_vector(0);
        let mut msg = unsafe { vector.as_slice_with_len::<duckdb_string_t>(input.len()) }[0];
        let string = DuckString::new(&mut msg).as_str();
        Err(format!("Error: {string}").into())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![LogicalTypeId::Varchar.into()],
            LogicalTypeId::Varchar.into(),
        )]
    }
}

struct PanickingScalar;

impl VScalar for PanickingScalar {
    type State = ();

    fn invoke(
        _: &Self::State,
        _: &mut DataChunkHandle,
        _: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        panic!("scalar callback panic")
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        unary_bigint_signature()
    }
}

struct NulErrorScalar;

impl VScalar for NulErrorScalar {
    type State = ();

    fn invoke(
        _: &Self::State,
        _: &mut DataChunkHandle,
        _: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        Err("before\0after".into())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        unary_bigint_signature()
    }
}

struct PanickingDisplayScalar;

impl VScalar for PanickingDisplayScalar {
    type State = ();

    fn invoke(
        _: &Self::State,
        _: &mut DataChunkHandle,
        _: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        Err(Box::new(PanickingDisplayError))
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        unary_bigint_signature()
    }
}

static HAZARDOUS_PANIC_PAYLOADS: AtomicU64 = AtomicU64::new(0);
static HAZARDOUS_PANIC_PAYLOAD_DROPS: AtomicU64 = AtomicU64::new(0);

struct HazardousPanicPayloadScalar;

impl VScalar for HazardousPanicPayloadScalar {
    type State = ();

    fn invoke(
        _: &Self::State,
        _: &mut DataChunkHandle,
        _: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        HAZARDOUS_PANIC_PAYLOADS.fetch_add(1, Ordering::SeqCst);
        HazardousPanicPayload::panic(&HAZARDOUS_PANIC_PAYLOAD_DROPS)
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        unary_bigint_signature()
    }
}

static SCALAR_STATE_DROPS: AtomicU64 = AtomicU64::new(0);

struct PanickingDropState {
    _allocation: Box<[u8]>,
}

impl Default for PanickingDropState {
    fn default() -> Self {
        Self {
            _allocation: vec![0; 32].into_boxed_slice(),
        }
    }
}

impl Drop for PanickingDropState {
    fn drop(&mut self) {
        SCALAR_STATE_DROPS.fetch_add(1, Ordering::SeqCst);
        panic!("scalar state destructor panic")
    }
}

struct ErrorWithPanickingDropStateScalar;

impl VScalar for ErrorWithPanickingDropStateScalar {
    type State = PanickingDropState;

    fn invoke(
        _: &Self::State,
        _: &mut DataChunkHandle,
        _: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        Err("scalar error before state drop".into())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        unary_bigint_signature()
    }
}

struct LatePanickingScalar;

impl VScalar for LatePanickingScalar {
    type State = ();

    fn invoke(
        _: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let input_values = input.flat_vector(0);
        let input_values = unsafe { input_values.as_slice_with_len::<i64>(input.len()) };
        let mut output_values = output.flat_vector();
        let output_values = unsafe { output_values.as_mut_slice::<i64>() };

        for (input, output) in input_values.iter().zip(output_values.iter_mut()).take(input.len()) {
            if *input == 3_000 {
                panic!("late scalar callback panic")
            }
            *output = *input;
        }
        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        unary_bigint_signature()
    }
}

#[derive(Debug, Clone)]
struct TestState {
    multiplier: usize,
    prefix: String,
}

impl Default for TestState {
    fn default() -> Self {
        Self {
            multiplier: 3,
            prefix: "default".to_string(),
        }
    }
}

struct EchoScalar {}

impl VScalar for EchoScalar {
    type State = TestState;

    fn invoke(
        state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let values = input.flat_vector(0);
        let values = unsafe { values.as_slice_with_len::<duckdb_string_t>(input.len()) };
        let strings = values
            .iter()
            .map(|ptr| DuckString::new(&mut { *ptr }).as_str().to_string())
            .take(input.len());
        let output = output.flat_vector();

        for s in strings {
            let res = format!("{}: {}", state.prefix, s.repeat(state.multiplier));
            output.insert(0, res.as_str());
        }
        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![LogicalTypeId::Varchar.into()],
            LogicalTypeId::Varchar.into(),
        )]
    }
}

struct Repeat {}

impl VScalar for Repeat {
    type State = ();

    fn invoke(
        _: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let output = output.flat_vector();
        let counts = input.flat_vector(1);
        let values = input.flat_vector(0);
        let values = unsafe { values.as_slice_with_len::<duckdb_string_t>(input.len()) };
        let strings = values
            .iter()
            .map(|ptr| DuckString::new(&mut { *ptr }).as_str().to_string());
        let counts = unsafe { counts.as_slice_with_len::<i32>(input.len()) };
        for (count, value) in counts.iter().zip(strings).take(input.len()) {
            output.insert(0, value.repeat((*count) as usize).as_str());
        }

        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![
                LogicalTypeHandle::from(LogicalTypeId::Varchar),
                LogicalTypeHandle::from(LogicalTypeId::Integer),
            ],
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        )]
    }
}

#[test]
fn test_scalar() -> Result<(), Box<dyn Error>> {
    let conn = Connection::open_in_memory()?;

    // Test with default state
    {
        conn.register_scalar_function::<EchoScalar>("echo")?;

        let mut stmt = conn.prepare("select echo('x')")?;
        let mut rows = stmt.query([])?;

        while let Some(row) = rows.next()? {
            let res: String = row.get(0)?;
            assert_eq!(res, "default: xxx");
        }
    }

    // Test with custom state
    {
        conn.register_scalar_function_with_state::<EchoScalar>(
            "echo2",
            &TestState {
                multiplier: 5,
                prefix: "custom".to_string(),
            },
        )?;

        let mut stmt = conn.prepare("select echo2('y')")?;
        let mut rows = stmt.query([])?;

        while let Some(row) = rows.next()? {
            let res: String = row.get(0)?;
            assert_eq!(res, "custom: yyyyy");
        }
    }

    Ok(())
}

#[test]
fn test_scalar_error() -> Result<(), Box<dyn Error>> {
    let conn = Connection::open_in_memory()?;
    conn.register_scalar_function::<ErrorScalar>("error_udf")?;

    let mut stmt = conn.prepare("select error_udf('blurg') as hello")?;
    if let Err(err) = stmt.query([]) {
        assert!(err.to_string().contains("Error: blurg"));
    } else {
        panic!("Expected an error");
    }

    Ok(())
}

#[test]
fn scalar_panics_and_display_failures_become_query_errors() -> Result<(), Box<dyn Error>> {
    let conn = Connection::open_in_memory()?;
    conn.register_scalar_function::<PanickingScalar>("panicking_scalar")?;
    conn.register_scalar_function::<PanickingDisplayScalar>("panicking_display_scalar")?;

    let panic_error = conn.prepare("SELECT panicking_scalar(42)")?.query([]).err().unwrap();
    assert!(panic_error.to_string().contains("scalar callback panic"));

    let display_error = conn
        .prepare("SELECT panicking_display_scalar(42)")?
        .query([])
        .err()
        .unwrap();
    assert!(display_error.to_string().contains("callback error display panic"));

    let value: i64 = conn.query_row("SELECT 1 + 1", [], |row| row.get(0))?;
    assert_eq!(value, 2);
    Ok(())
}

#[test]
fn scalar_panics_after_output_and_the_registered_function_remains_usable() -> Result<(), Box<dyn Error>> {
    let conn = Connection::open_in_memory()?;
    conn.register_scalar_function::<LatePanickingScalar>("late_panicking_scalar")?;

    let error = conn
        .prepare("SELECT late_panicking_scalar(i) FROM range(5000) AS t(i)")?
        .query([])
        .err()
        .unwrap();
    assert!(error.to_string().contains("late scalar callback panic"));

    let value: i64 = conn.query_row("SELECT late_panicking_scalar(7)", [], |row| row.get(0))?;
    assert_eq!(value, 7);
    Ok(())
}

#[test]
fn scalar_errors_escape_interior_nuls_through_registered_callback() -> Result<(), Box<dyn Error>> {
    let conn = Connection::open_in_memory()?;
    conn.register_scalar_function::<NulErrorScalar>("nul_error_scalar")?;

    let error = conn.prepare("SELECT nul_error_scalar(42)")?.query([]).err().unwrap();
    let message = error.to_string();

    assert!(message.contains("before\\0after"));
    Ok(())
}

#[test]
fn scalar_hazardous_panic_payloads_are_contained() -> Result<(), Box<dyn Error>> {
    HAZARDOUS_PANIC_PAYLOADS.store(0, Ordering::SeqCst);
    HAZARDOUS_PANIC_PAYLOAD_DROPS.store(0, Ordering::SeqCst);

    let conn = Connection::open_in_memory()?;
    conn.register_scalar_function::<HazardousPanicPayloadScalar>("hazardous_panic_payload")?;

    let error = conn
        .prepare("SELECT hazardous_panic_payload(42)")?
        .query([])
        .err()
        .unwrap();

    assert!(error.to_string().contains("non-string panic payload"));
    let payloads = HAZARDOUS_PANIC_PAYLOADS.load(Ordering::SeqCst);
    assert!(payloads > 0);
    assert_eq!(HAZARDOUS_PANIC_PAYLOAD_DROPS.load(Ordering::SeqCst), payloads);
    Ok(())
}

#[test]
fn scalar_errors_survive_panicking_state_destructors() -> Result<(), Box<dyn Error>> {
    SCALAR_STATE_DROPS.store(0, Ordering::SeqCst);
    {
        let conn = Connection::open_in_memory()?;
        conn.register_scalar_function::<ErrorWithPanickingDropStateScalar>("error_with_panicking_drop")?;
        let error = conn
            .prepare("SELECT error_with_panicking_drop(1)")?
            .query([])
            .err()
            .unwrap();
        assert!(error.to_string().contains("scalar error before state drop"));
    }

    assert_eq!(SCALAR_STATE_DROPS.load(Ordering::SeqCst), 1);
    Ok(())
}

#[test]
fn test_repeat_scalar() -> Result<(), Box<dyn Error>> {
    let conn = Connection::open_in_memory()?;
    conn.register_scalar_function::<Repeat>("nobie_repeat")?;

    let batches = conn
        .prepare("select nobie_repeat('Ho ho ho 🎅🎄', 3) as message from range(5)")?
        .query_arrow([])?
        .collect::<Vec<_>>();

    for batch in batches.iter() {
        let array = batch.column(0);
        let array = array.as_any().downcast_ref::<::arrow::array::StringArray>().unwrap();
        for i in 0..array.len() {
            assert_eq!(array.value(i), "Ho ho ho 🎅🎄Ho ho ho 🎅🎄Ho ho ho 🎅🎄");
        }
    }

    Ok(())
}

// Counters for testing volatile functions
static VOLATILE_COUNTER: AtomicU64 = AtomicU64::new(0);
static NON_VOLATILE_COUNTER: AtomicU64 = AtomicU64::new(0);

struct CounterScalar {}

impl VScalar for CounterScalar {
    type State = ();

    fn invoke(
        _: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let len = input.len();
        let mut output_vec = output.flat_vector();
        let data = unsafe { output_vec.as_mut_slice::<i64>() };

        for item in data.iter_mut().take(len) {
            *item = NON_VOLATILE_COUNTER.fetch_add(1, Ordering::SeqCst) as i64;
        }
        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![],
            LogicalTypeHandle::from(LogicalTypeId::Bigint),
        )]
    }
}

struct VolatileCounterScalar {}

impl VScalar for VolatileCounterScalar {
    type State = ();

    fn invoke(
        _: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let len = input.len();
        let mut output_vec = output.flat_vector();
        let data = unsafe { output_vec.as_mut_slice::<i64>() };

        for item in data.iter_mut().take(len) {
            *item = VOLATILE_COUNTER.fetch_add(1, Ordering::SeqCst) as i64;
        }
        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![],
            LogicalTypeHandle::from(LogicalTypeId::Bigint),
        )]
    }

    fn volatile() -> bool {
        true
    }
}

#[test]
fn test_volatile_scalar() -> Result<(), Box<dyn Error>> {
    let conn = Connection::open_in_memory()?;

    VOLATILE_COUNTER.store(0, Ordering::SeqCst);
    conn.register_scalar_function::<VolatileCounterScalar>("volatile_counter")?;

    let values: Vec<i64> = conn
        .prepare("SELECT volatile_counter() FROM generate_series(1, 5)")?
        .query_map([], |row| row.get(0))?
        .collect::<Result<_, _>>()?;

    assert_eq!(values, [0, 1, 2, 3, 4]);

    Ok(())
}

#[test]
fn test_non_volatile_scalar() -> Result<(), Box<dyn Error>> {
    let conn = Connection::open_in_memory()?;

    NON_VOLATILE_COUNTER.store(0, Ordering::SeqCst);
    conn.register_scalar_function::<CounterScalar>("non_volatile_counter")?;

    // Constant folding should make every row identical
    let distinct_count: i64 = conn
        .prepare("SELECT COUNT(DISTINCT non_volatile_counter()) FROM generate_series(1, 5)")?
        .query_row([], |row| row.get(0))?;

    assert_eq!(distinct_count, 1);

    Ok(())
}

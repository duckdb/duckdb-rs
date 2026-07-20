use std::{
    error::Error,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use arrow::{
    array::{Array, Int64Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use libduckdb_sys::duckdb_string_t;

use crate::{
    Connection,
    arrow_interop::WritableVector,
    core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId},
    types::DuckString,
};

use super::{ScalarFunctionSignature, VScalar};

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
        vec![ScalarFunctionSignature::exact(
            vec![LogicalTypeId::Bigint.into()],
            LogicalTypeId::Bigint.into(),
        )]
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
        vec![ScalarFunctionSignature::exact(
            vec![LogicalTypeId::Bigint.into()],
            LogicalTypeId::Bigint.into(),
        )]
    }
}

struct InputRewriteScalar;

impl VScalar for InputRewriteScalar {
    type State = ();

    fn invoke(
        _: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![99_i64]))])?;
        crate::arrow_interop::record_batch_to_duckdb_data_chunk(&batch, input)?;

        let mut output = output.flat_vector();
        unsafe { output.write(0, 42_i64) };
        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![LogicalTypeId::Bigint.into()],
            LogicalTypeId::Bigint.into(),
        )]
    }
}

static SCALAR_STATE_DROPS: AtomicU64 = AtomicU64::new(0);

#[derive(Default)]
struct PanickingDropState;

impl Drop for PanickingDropState {
    fn drop(&mut self) {
        SCALAR_STATE_DROPS.fetch_add(1, Ordering::SeqCst);
        panic!("scalar state destructor panic");
    }
}

struct PanickingDropStateScalar;

impl VScalar for PanickingDropStateScalar {
    type State = PanickingDropState;

    fn invoke(
        _: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut output = output.flat_vector();
        for row in 0..input.len() {
            unsafe { output.write(row, 42_i64) };
        }
        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![LogicalTypeId::Bigint.into()],
            LogicalTypeId::Bigint.into(),
        )]
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
        vec![ScalarFunctionSignature::exact(
            vec![LogicalTypeId::Bigint.into()],
            LogicalTypeId::Bigint.into(),
        )]
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
        let mut output = output.flat_vector();

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
        let mut output = output.flat_vector();
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

struct CapacityCheckedScalar {}

impl VScalar for CapacityCheckedScalar {
    type State = ();

    fn invoke(
        _: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let len = input.len();
        if input.capacity() != len {
            return Err(format!(
                "callback input capacity {} differs from cardinality {len}",
                input.capacity()
            )
            .into());
        }
        let values = {
            let vector = input.flat_vector(0);
            if vector.capacity() != len {
                return Err(format!(
                    "callback input vector capacity {} differs from cardinality {len}",
                    vector.capacity()
                )
                .into());
            }
            unsafe { vector.as_slice_with_len::<i64>(len) }.to_vec()
        };

        let mut output = output.flat_vector();
        if output.capacity() != len {
            return Err(format!(
                "callback output capacity {} differs from cardinality {len}",
                output.capacity()
            )
            .into());
        }
        unsafe { output.copy(&values) };
        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![LogicalTypeId::Bigint.into()],
            LogicalTypeId::Bigint.into(),
        )]
    }
}

struct NestedListRoundTripScalar;

impl VScalar for NestedListRoundTripScalar {
    type State = ();

    fn invoke(
        _: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let input_list = input.list_vector(0);
        let input_child = input_list.read_child()?;
        let mut values = Vec::new();
        let mut entries = Vec::with_capacity(input.len());

        for row in 0..input.len() {
            let Some(range) = input_list.try_get_range(row)? else {
                entries.push(None);
                continue;
            };
            let len = range.len();
            let output_offset = values.len();
            for child_row in range {
                values.push(unsafe { input_child.read::<i32>(child_row)? });
            }
            entries.push(Some((output_offset, len)));
        }
        drop(input_child);
        drop(input_list);

        let mut output_list = output.list_vector();
        output_list.try_reserve(values.len())?;
        unsafe { output_list.child(values.len()).copy(&values) };
        for (row, entry) in entries.into_iter().enumerate() {
            if let Some((offset, len)) = entry {
                output_list.set_entry(row, offset, len);
            } else {
                output_list.set_null(row);
            }
        }
        output_list.try_set_len(values.len())?;
        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        let child_type = LogicalTypeHandle::from(LogicalTypeId::Integer);
        vec![ScalarFunctionSignature::exact(
            vec![LogicalTypeHandle::list(&child_type)],
            LogicalTypeHandle::list(&child_type),
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
fn scalar_panics_become_query_errors() -> Result<(), Box<dyn Error>> {
    let conn = Connection::open_in_memory()?;
    conn.register_scalar_function::<PanickingScalar>("panicking_scalar")?;

    let error = conn.prepare("SELECT panicking_scalar(42)")?.query([]).err().unwrap();

    assert!(error.to_string().contains("scalar callback panic"));
    Ok(())
}

#[test]
fn scalar_errors_escape_interior_nuls_through_registered_callback() -> Result<(), Box<dyn Error>> {
    let conn = Connection::open_in_memory()?;
    conn.register_scalar_function::<NulErrorScalar>("nul_error_scalar")?;

    let error = conn.prepare("SELECT nul_error_scalar(42)")?.query([]).err().unwrap();
    let message = error.to_string();

    assert!(message.contains("before\\0after"));
    assert!(!message.contains('\0'));
    Ok(())
}

#[test]
fn scalar_callback_input_cannot_be_rewritten_by_arrow_conversion() -> Result<(), Box<dyn Error>> {
    let conn = Connection::open_in_memory()?;
    conn.register_scalar_function::<InputRewriteScalar>("rewrite_input")?;

    let error = conn
        .query_row("SELECT rewrite_input(1)", [], |row| row.get::<_, i64>(0))
        .unwrap_err();

    assert!(error.to_string().contains("callback input data chunks are read-only"));
    Ok(())
}

#[test]
fn scalar_state_destructor_panics_are_contained() -> Result<(), Box<dyn Error>> {
    SCALAR_STATE_DROPS.store(0, Ordering::SeqCst);
    {
        let conn = Connection::open_in_memory()?;
        conn.register_scalar_function::<PanickingDropStateScalar>("panicking_drop_state")?;
        let value: i64 = conn.query_row("SELECT panicking_drop_state(1)", [], |row| row.get(0))?;
        assert_eq!(value, 42);
    }

    assert!(SCALAR_STATE_DROPS.load(Ordering::SeqCst) > 0);
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

    assert!(SCALAR_STATE_DROPS.load(Ordering::SeqCst) > 0);
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

#[test]
fn test_callback_vectors_use_exact_invocation_capacity() -> Result<(), Box<dyn Error>> {
    let conn = Connection::open_in_memory()?;
    conn.register_scalar_function::<CapacityCheckedScalar>("capacity_checked")?;

    let values = conn
        .prepare("SELECT capacity_checked(i) FROM range(3) AS values(i)")?
        .query_map([], |row| row.get::<_, i64>(0))?
        .collect::<Result<Vec<_>, _>>()?;

    assert_eq!(values, [0, 1, 2]);
    Ok(())
}

#[test]
fn registered_scalar_round_trips_nested_vectors_through_native_seam() -> Result<(), Box<dyn Error>> {
    let conn = Connection::open_in_memory()?;
    conn.register_scalar_function::<NestedListRoundTripScalar>("native_list_round_trip")?;

    let values = conn
        .prepare(
            "SELECT native_list_round_trip(value) = value \
             FROM (VALUES ([1, 2]::INTEGER[]), ([]::INTEGER[]), ([3]::INTEGER[])) input(value)",
        )?
        .query_map([], |row| row.get::<_, bool>(0))?
        .collect::<Result<Vec<_>, _>>()?;

    assert_eq!(values, [true, true, true]);
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
        for row in 0..len {
            let value = NON_VOLATILE_COUNTER.fetch_add(1, Ordering::SeqCst) as i64;
            unsafe { output_vec.write(row, value) };
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
        for row in 0..len {
            let value = VOLATILE_COUNTER.fetch_add(1, Ordering::SeqCst) as i64;
            unsafe { output_vec.write(row, value) };
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

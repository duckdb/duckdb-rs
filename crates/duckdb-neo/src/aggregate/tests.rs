use std::{
    fmt::Display,
    sync::{
        Arc,
        atomic::{AtomicIsize, AtomicUsize, Ordering},
    },
};

use crate::ffi::{DUCKDB_V2_FUNCTION_PROPERTY_KEY, DUCKDB_V2_FUNCTION_PROPERTY_VALUE};

use crate::{
    DuckDBType, Parameters,
    aggregate::{
        AggregateCallbacks, AggregateDestroyCallbacks, AggregateFunctionBuilder, AggregateStatsCallbacks,
        ReturnTypeHandle, States,
    },
    bind_arguments::BindArguments,
    builder_helpers::ArgCount,
    connection::{Connection, Context, SettingScope},
    data_chunk::VectorCollection,
    environment::{Environment, StorageLocation},
    logical_type::LogicalType,
    signature::{FunctionParameter, SignatureBuilder},
    stats::Stats,
    vector::{Unknown, Vector},
};

#[test]
fn aggregate_state_slice_handles_empty_arrays() {
    let items = [std::ptr::null_mut::<u64>()];
    // SAFETY: null inputs are supported, and items is live for every borrowed slice.
    unsafe {
        assert!(super::state_slice::<*mut u64>(std::ptr::null(), 0).is_empty());
        assert!(super::state_slice::<*mut u64>(std::ptr::null(), 1).is_empty());
        assert!(super::state_slice(items.as_ptr(), 0).is_empty());
        assert_eq!(super::state_slice(items.as_ptr(), items.len()), &items);
    }
}

#[test]
fn aggregate_states_iterator_tracks_remaining_states() {
    let (mut first, mut second) = (10, 20);
    let pointers = [&mut first as *mut i32, &mut second as *mut i32];
    // SAFETY: both aligned values remain live and otherwise unborrowed until states is dropped.
    let states = unsafe { States::new(&pointers) };
    let mut iter = states.iter();
    assert_eq!(iter.size_hint(), (2, Some(2)));
    assert_eq!(iter.len(), 2);
    assert_eq!(iter.next_back(), Some(&20));
    assert_eq!(iter.len(), 1);
    assert_eq!(iter.next(), Some(&10));
    assert_eq!(iter.size_hint(), (0, Some(0)));
    assert_eq!(iter.next(), None);
    assert_eq!(iter.next_back(), None);
    assert_eq!((&states).into_iter().copied().collect::<Vec<_>>(), [10, 20]);
}

struct BasicAggregate<T> {
    item: T,
}

// Formula = (user_data + bind_data) +  median()

impl<T: Display + Send + Sync + 'static> AggregateCallbacks for BasicAggregate<T> {
    type BindData = Vec<f32>;
    type StateItem = Vec<i32>;

    fn bind(
        &self,
        context: &Context,
        arguments: BindArguments<'_>,
        result_type_handle: ReturnTypeHandle<'_>,
    ) -> crate::Result<Self::BindData> {
        let mut bind_data: Vec<f32> = Vec::new();

        result_type_handle.override_return(LogicalType::from_text(context, "VARCHAR")?)?;

        for arg in arguments.iter()? {
            assert_eq!(arg.logical_type()?, i32::logical_type(context)?);
            assert!(arg.value()?.is_none());
        }

        bind_data.push(1.2);
        bind_data.push(3.4);

        Ok(bind_data)
    }

    fn init(&self, _bind_data: Option<&Self::BindData>) -> Self::StateItem {
        vec![]
    }

    fn size(&self, _bind_data: Option<&Self::BindData>) -> crate::Result<usize> {
        Ok(std::mem::size_of::<Self::StateItem>())
    }

    fn update(
        &self,
        _bind_data: Option<&Self::BindData>,
        collection: &mut VectorCollection,
        mut states: States<'_, Self::StateItem>,
    ) -> crate::Result<()> {
        let vec = collection.get_vector_at::<i32>(0)?;

        for (i, val) in vec.reader()?.iter().enumerate() {
            if let Some(val) = val {
                states[i].push(*val);
            }
        }

        Ok(())
    }

    fn combine(
        &self,
        _bind_data: Option<&Self::BindData>,
        source: &States<'_, Self::StateItem>,
        mut dest: States<'_, Self::StateItem>,
    ) -> crate::Result<()> {
        for i in 0..dest.len() {
            let values = source[i].clone();
            dest[i].extend(values);
        }

        Ok(())
    }

    fn finalize(
        &self,
        bind_data: Option<&Self::BindData>,
        states: &[&Self::StateItem],
        result: Vector<'_, Unknown>,
        result_offset: usize,
    ) -> crate::Result<()> {
        let mut result = result.cast::<String>()?;

        for (index, state) in states.iter().enumerate() {
            let mut to_write = state.iter().map(|value| value.to_string()).collect::<String>();
            to_write += &format!(
                " + ({:.1} + {:.1}) - {:.1}",
                bind_data.unwrap()[0],
                bind_data.unwrap()[1],
                self.item
            );
            result.writer()?.set(result_offset + index, &to_write)?;
        }

        Ok(())
    }
}

#[test]
pub fn basic_aggregate_test() -> crate::Result<()> {
    let env = Environment::new()?;
    let instance = env.open(StorageLocation::InMemory)?;
    let conn = instance.connect()?;

    AggregateFunctionBuilder::new(
        "to_concatenated",
        SignatureBuilder::new()
            .param(FunctionParameter::new::<i32>("IN"))
            // Will be overwritten to String
            .return_type(u8::logical_type(&conn)?),
        BasicAggregate::<f32> { item: 0.0 },
    )
    .register(&conn)?;

    let mut result = conn.query(
        "SELECT to_concatenated(i) AS result FROM (VALUES (1), (2), (NULL), (3), (4), (5)) AS t(i)",
        Parameters::None,
    )?;

    while let Some(chunk) = result.next_chunk()? {
        let vec = chunk.get_vector_at::<String>(0)?;

        let res = vec.reader()?.get(0)?;

        assert_eq!(res, Some("12345 + (1.2 + 3.4) - 0.0"));
    }

    Ok(())
}

#[test]
pub fn aggregate_test_invalid_build() -> crate::Result<()> {
    let env = Environment::new()?;
    let instance = env.open(StorageLocation::InMemory)?;

    instance
        .config()?
        .set_text("threads", &1.to_string(), SettingScope::Global)?;

    let conn = instance.connect()?;

    let mut result = AggregateFunctionBuilder::new(
        "to_concatenated",
        SignatureBuilder::new()
            .param(FunctionParameter::new::<i32>("IN"))
            .return_type(String::logical_type(&conn)?),
        BasicAggregate::<f32> { item: 2.5 },
    );

    result.properties.insert(
        DUCKDB_V2_FUNCTION_PROPERTY_KEY::DUCKDB_V2_FUNCTION_PROPERTY_KEY_MAX_ENUM,
        DUCKDB_V2_FUNCTION_PROPERTY_VALUE::DUCKDB_V2_FUNCTION_PROPERTY_VALUE_MAX_ENUM,
    );

    let result = result.register(&conn);

    assert!(result.is_err());

    let result = AggregateFunctionBuilder::new(
        "",
        SignatureBuilder::new()
            .param(FunctionParameter::new::<i32>("IN"))
            .return_type(String::logical_type(&conn)?),
        BasicAggregate::<i32> { item: 0 },
    )
    .register(&conn);

    assert!(result.is_err());

    Ok(())
}

#[test]
fn aggregate_test_groups() -> crate::Result<()> {
    let env = Environment::new()?;
    let instance = env.open(StorageLocation::InMemory)?;
    let conn = instance.connect()?;

    AggregateFunctionBuilder::new(
        "to_concatenated",
        SignatureBuilder::new()
            .param(FunctionParameter::new::<i32>("IN"))
            .return_type(String::logical_type(&conn)?),
        BasicAggregate::<i32> { item: 0 },
    )
    .register(&conn)?;

    let mut result = conn.query(
        "SELECT
             (i - 1) // 5 AS group_id,
             to_concatenated(i::INTEGER) AS result
         FROM generate_series(1, 200000) AS t(i)
         GROUP BY group_id
         ORDER BY group_id",
        Parameters::None,
    )?;

    let mut groups = 0;
    while let Some(chunk) = result.next_chunk()? {
        let vec = chunk.get_vector_at::<String>(1)?;

        for value in vec.reader()?.iter() {
            assert!(value.is_some());

            groups += 1;
        }
    }

    assert_eq!(groups, 40000);

    Ok(())
}

/// Reports a state size smaller than its `StateItem`, which must be rejected.
struct UndersizedState;

impl AggregateCallbacks for UndersizedState {
    type BindData = ();
    type StateItem = u64;

    fn bind(
        &self,
        _context: &Context,
        _arguments: BindArguments<'_>,
        _result_type_handle: ReturnTypeHandle<'_>,
    ) -> crate::Result<Self::BindData> {
        Ok(())
    }

    fn size(&self, _bind_data: Option<&Self::BindData>) -> crate::Result<usize> {
        Ok(1)
    }

    fn init(&self, _bind_data: Option<&Self::BindData>) -> Self::StateItem {
        0
    }

    fn update(
        &self,
        _bind_data: Option<&Self::BindData>,
        _collection: &mut VectorCollection,
        _states: States<'_, Self::StateItem>,
    ) -> crate::Result<()> {
        Ok(())
    }

    fn combine(
        &self,
        _bind_data: Option<&Self::BindData>,
        _source: &States<'_, Self::StateItem>,
        _dest: States<'_, Self::StateItem>,
    ) -> crate::Result<()> {
        Ok(())
    }

    fn finalize(
        &self,
        _bind_data: Option<&Self::BindData>,
        _states: &[&Self::StateItem],
        _result: Vector<'_, Unknown>,
        _result_offset: usize,
    ) -> crate::Result<()> {
        Ok(())
    }
}

#[test]
fn aggregate_test_undersized_state() -> crate::Result<()> {
    let env = Environment::new()?;
    let instance = env.open(StorageLocation::InMemory)?;
    let conn = instance.connect()?;

    AggregateFunctionBuilder::new(
        "undersized",
        SignatureBuilder::new()
            .param(FunctionParameter::new::<i32>("IN"))
            .return_type(u64::logical_type(&conn)?),
        UndersizedState,
    )
    .register(&conn)?;

    let result = conn
        .query("SELECT undersized(i::INTEGER) FROM range(10) AS t(i)", Parameters::None)
        .and_then(|mut result| {
            while result.next_chunk()?.is_some() {}
            Ok(())
        });

    let err = result.expect_err("an undersized state must fail the query");
    assert!(err.to_string().contains("smaller than size_of"), "{err}");

    Ok(())
}

/// Sums with a state size that is not a multiple of the state's alignment.
struct PaddedSum;

impl AggregateCallbacks for PaddedSum {
    type BindData = ();
    type StateItem = i64;

    fn bind(
        &self,
        _context: &Context,
        _arguments: BindArguments<'_>,
        _result_type_handle: ReturnTypeHandle<'_>,
    ) -> crate::Result<Self::BindData> {
        Ok(())
    }

    fn size(&self, _bind_data: Option<&Self::BindData>) -> crate::Result<usize> {
        Ok(12)
    }

    fn init(&self, _bind_data: Option<&Self::BindData>) -> Self::StateItem {
        0
    }

    fn update(
        &self,
        _bind_data: Option<&Self::BindData>,
        collection: &mut VectorCollection,
        mut states: States<'_, Self::StateItem>,
    ) -> crate::Result<()> {
        let vec = collection.get_vector_at::<i32>(0)?;
        for (i, val) in vec.reader()?.iter().enumerate() {
            states[i] += i64::from(*val.unwrap_or(&0));
        }
        Ok(())
    }

    fn combine(
        &self,
        _bind_data: Option<&Self::BindData>,
        source: &States<'_, Self::StateItem>,
        mut dest: States<'_, Self::StateItem>,
    ) -> crate::Result<()> {
        for i in 0..dest.len() {
            dest[i] += source[i];
        }
        Ok(())
    }

    fn finalize(
        &self,
        _bind_data: Option<&Self::BindData>,
        states: &[&Self::StateItem],
        result: Vector<'_, Unknown>,
        result_offset: usize,
    ) -> crate::Result<()> {
        let mut result = result.cast::<i64>()?;
        for (index, state) in states.iter().enumerate() {
            result.writer()?.set(result_offset + index, *state)?;
        }
        Ok(())
    }
}

#[test]
fn aggregate_test_window_with_padded_state() -> crate::Result<()> {
    let env = Environment::new()?;
    let instance = env.open(StorageLocation::InMemory)?;
    let conn = instance.connect()?;

    AggregateFunctionBuilder::new(
        "padded_sum",
        SignatureBuilder::new()
            .param(FunctionParameter::new::<i32>("IN"))
            .return_type(i64::logical_type(&conn)?),
        PaddedSum,
    )
    .register(&conn)?;

    // Windows lay states out by the reported size and finalize into unsized results.
    let mut result = conn.query(
        "SELECT padded_sum(i::INTEGER) OVER () AS total,
                padded_sum(i::INTEGER) OVER (ORDER BY i) AS running
         FROM range(1, 101) AS t(i)
         ORDER BY i",
        Parameters::None,
    )?;

    let mut rows = Vec::new();
    while let Some(chunk) = result.next_chunk()? {
        let total = chunk.get_vector_at::<i64>(0)?.reader()?;
        let running = chunk.get_vector_at::<i64>(1)?.reader()?;
        for row in 0..total.len() {
            rows.push((*total.get(row)?.unwrap(), *running.get(row)?.unwrap()));
        }
    }

    let expected = (1..=100i64).map(|i| (5050, i * (i + 1) / 2)).collect::<Vec<_>>();
    assert_eq!(rows, expected);

    Ok(())
}

struct StatsSum {
    calls: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

impl AggregateCallbacks for StatsSum {
    type BindData = i64;
    type StateItem = i64;

    fn bind(
        &self,
        _context: &Context,
        _arguments: BindArguments<'_>,
        _result_type_handle: ReturnTypeHandle<'_>,
    ) -> crate::Result<Self::BindData> {
        Ok(7)
    }

    fn init(&self, _bind_data: Option<&Self::BindData>) -> Self::StateItem {
        0
    }

    fn update(
        &self,
        _bind_data: Option<&Self::BindData>,
        collection: &mut VectorCollection,
        mut states: States<'_, Self::StateItem>,
    ) -> crate::Result<()> {
        let vec = collection.get_vector_at::<i64>(0)?;
        for (i, val) in vec.reader()?.iter().enumerate() {
            states[i] += *val.unwrap_or(&0);
        }
        Ok(())
    }

    fn combine(
        &self,
        _bind_data: Option<&Self::BindData>,
        source: &States<'_, Self::StateItem>,
        mut dest: States<'_, Self::StateItem>,
    ) -> crate::Result<()> {
        for i in 0..dest.len() {
            dest[i] += source[i];
        }
        Ok(())
    }

    fn finalize(
        &self,
        _bind_data: Option<&Self::BindData>,
        states: &[&Self::StateItem],
        result: Vector<'_, Unknown>,
        result_offset: usize,
    ) -> crate::Result<()> {
        let mut result = result.cast::<i64>()?;
        for (index, state) in states.iter().enumerate() {
            result.writer()?.set(result_offset + index, *state)?;
        }
        Ok(())
    }
}

impl AggregateStatsCallbacks for StatsSum {
    fn stats<'a>(
        &self,
        _context: &Context,
        bind_data: Option<&Self::BindData>,
        stats: &Stats<'a>,
        arguments: ArgCount,
        column_stats: impl Fn(usize) -> crate::Result<Stats<'a>>,
    ) -> crate::Result<()> {
        assert_eq!(bind_data, Some(&7));
        assert_eq!(arguments.positional_fixed, 1);
        assert_eq!(arguments.total(), 1);
        let column = column_stats(0)?;
        assert!(!column.can_have_null()?);
        assert!(column.can_have_valid()?);
        stats.set_can_have_null(true)?;
        stats.set_can_have_valid(true)?;
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
}

#[test]
fn aggregate_test_stats_callback() -> crate::Result<()> {
    let env = Environment::new()?;
    let instance = env.open(StorageLocation::InMemory)?;
    let conn = instance.connect()?;
    conn.query(
        "CREATE TABLE stats_input AS SELECT range AS i FROM range(10)",
        Parameters::None,
    )?
    .drain()?;

    let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    AggregateFunctionBuilder::new(
        "stats_sum",
        SignatureBuilder::new()
            .param(FunctionParameter::new::<i64>("IN"))
            .return_type(i64::logical_type(&conn)?),
        StatsSum { calls: calls.clone() },
    )
    .with_stats()
    .register(&conn)?;

    // A table column carries exact storage statistics into the callback.
    let chunk = conn
        .query("SELECT stats_sum(i) FROM stats_input", Parameters::None)?
        .next_chunk()?
        .expect("expected a result chunk");
    assert_eq!(chunk.get_vector_at::<i64>(0)?.reader()?.get(0)?, Some(&45));
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);

    Ok(())
}

impl AggregateDestroyCallbacks for PaddedSum {
    fn destroy(
        &self,
        _bind_data: Option<&Self::BindData>,
        _states: &States<'_, Self::StateItem>,
    ) -> crate::Result<bool> {
        Ok(false)
    }
}

const RUNNING_SUM: &str = "SELECT padded_sum(i::INTEGER) OVER (ROWS UNBOUNDED PRECEDING) FROM range(1, 101) AS t(i)";

fn explain(conn: &Connection, sql: &str) -> crate::Result<String> {
    let mut plan = conn.query(format!("EXPLAIN {sql}"), Parameters::None)?;
    let mut text = String::new();
    while let Some(chunk) = plan.next_chunk()? {
        let plans = chunk.get_vector_at::<String>(1)?.reader()?;
        for row in 0..plans.len() {
            text += plans.get(row)?.expect("EXPLAIN plan is not NULL");
        }
    }
    Ok(text)
}

fn register_padded_sum(conn: &Connection, with_destroy: bool) -> crate::Result<()> {
    let builder = AggregateFunctionBuilder::new(
        "padded_sum",
        SignatureBuilder::new()
            .param(FunctionParameter::new::<i32>("IN"))
            .return_type(i64::logical_type(conn)?),
        PaddedSum,
    );
    if with_destroy { builder.with_destroy() } else { builder }.register(conn)
}

#[test]
fn aggregate_test_plain_state_streams_running_window() -> crate::Result<()> {
    let env = Environment::new()?;
    let instance = env.open(StorageLocation::InMemory)?;
    let conn = instance.connect()?;
    register_padded_sum(&conn, false)?;

    assert!(explain(&conn, RUNNING_SUM)?.contains("Streaming Window"));

    let mut result = conn.query(RUNNING_SUM, Parameters::None)?;
    let mut running = Vec::new();
    while let Some(chunk) = result.next_chunk()? {
        let sums = chunk.get_vector_at::<i64>(0)?.reader()?;
        for row in 0..sums.len() {
            running.push(*sums.get(row)?.unwrap());
        }
    }
    assert_eq!(running, (1..=100i64).map(|i| i * (i + 1) / 2).collect::<Vec<_>>());

    Ok(())
}

#[test]
fn aggregate_test_with_destroy_disables_streaming_window() -> crate::Result<()> {
    let env = Environment::new()?;
    let instance = env.open(StorageLocation::InMemory)?;
    let conn = instance.connect()?;
    register_padded_sum(&conn, true)?;

    let plan = explain(&conn, RUNNING_SUM)?;
    assert!(plan.contains("Window") && !plan.contains("Streaming Window"), "{plan}");

    Ok(())
}

/// A state that tracks how many instances are alive.
struct Tracked {
    sum: i64,
    live: Arc<AtomicIsize>,
}

impl Drop for Tracked {
    fn drop(&mut self) {
        self.live.fetch_sub(1, Ordering::SeqCst);
    }
}

#[derive(Default)]
struct TrackedSum {
    live: Arc<AtomicIsize>,
    destroyed: Arc<AtomicUsize>,
    claim_cleanup: bool,
}

impl AggregateCallbacks for TrackedSum {
    type BindData = ();
    type StateItem = Tracked;

    fn bind(
        &self,
        _context: &Context,
        _arguments: BindArguments<'_>,
        _result_type_handle: ReturnTypeHandle<'_>,
    ) -> crate::Result<Self::BindData> {
        Ok(())
    }

    fn init(&self, _bind_data: Option<&Self::BindData>) -> Self::StateItem {
        self.live.fetch_add(1, Ordering::SeqCst);
        Tracked {
            sum: 0,
            live: self.live.clone(),
        }
    }

    fn update(
        &self,
        _bind_data: Option<&Self::BindData>,
        collection: &mut VectorCollection,
        mut states: States<'_, Self::StateItem>,
    ) -> crate::Result<()> {
        let vec = collection.get_vector_at::<i32>(0)?;
        for (i, val) in vec.reader()?.iter().enumerate() {
            states[i].sum += i64::from(*val.unwrap_or(&0));
        }
        Ok(())
    }

    fn combine(
        &self,
        _bind_data: Option<&Self::BindData>,
        source: &States<'_, Self::StateItem>,
        mut dest: States<'_, Self::StateItem>,
    ) -> crate::Result<()> {
        for i in 0..dest.len() {
            dest[i].sum += source[i].sum;
        }
        Ok(())
    }

    fn finalize(
        &self,
        _bind_data: Option<&Self::BindData>,
        states: &[&Self::StateItem],
        result: Vector<'_, Unknown>,
        result_offset: usize,
    ) -> crate::Result<()> {
        let mut result = result.cast::<i64>()?;
        for (index, state) in states.iter().enumerate() {
            result.writer()?.set(result_offset + index, &state.sum)?;
        }
        Ok(())
    }
}

impl AggregateDestroyCallbacks for TrackedSum {
    fn destroy(
        &self,
        _bind_data: Option<&Self::BindData>,
        states: &States<'_, Self::StateItem>,
    ) -> crate::Result<bool> {
        self.destroyed.fetch_add(states.len(), Ordering::SeqCst);
        if self.claim_cleanup {
            for state in states.as_ptrs() {
                // SAFETY: each state is destroyed once, and returning true skips the automatic drop.
                unsafe { state.drop_in_place() };
            }
        }
        Ok(self.claim_cleanup)
    }
}

const TRACKED_QUERY: &str =
    "SELECT i % 7 AS g, tracked_sum(i::INTEGER) FROM range(10000) AS t(i) GROUP BY g ORDER BY g";

fn run_tracked_sum(aggregate: TrackedSum, with_destroy: bool) -> crate::Result<String> {
    let env = Environment::new()?;
    let instance = env.open(StorageLocation::InMemory)?;
    let conn = instance.connect()?;
    let builder = AggregateFunctionBuilder::new(
        "tracked_sum",
        SignatureBuilder::new()
            .param(FunctionParameter::new::<i32>("IN"))
            .return_type(i64::logical_type(&conn)?),
        aggregate,
    );
    if with_destroy { builder.with_destroy() } else { builder }.register(&conn)?;

    let mut result = conn.query(TRACKED_QUERY, Parameters::None)?;
    let mut sums = Vec::new();
    while let Some(chunk) = result.next_chunk()? {
        let values = chunk.get_vector_at::<i64>(1)?.reader()?;
        for row in 0..values.len() {
            sums.push(*values.get(row)?.unwrap());
        }
    }
    let expected = (0..7i64)
        .map(|g| (0..10000i64).filter(|i| i % 7 == g).sum())
        .collect::<Vec<i64>>();
    assert_eq!(sums, expected);

    explain(
        &conn,
        "SELECT tracked_sum(i::INTEGER) OVER (ROWS UNBOUNDED PRECEDING) FROM range(10) AS t(i)",
    )
}

#[test]
fn aggregate_test_drop_state_without_destroy_callbacks() -> crate::Result<()> {
    let aggregate = TrackedSum::default();
    let (live, destroyed) = (aggregate.live.clone(), aggregate.destroyed.clone());

    let plan = run_tracked_sum(aggregate, false)?;

    assert!(plan.contains("Window") && !plan.contains("Streaming Window"), "{plan}");
    assert_eq!(live.load(Ordering::SeqCst), 0);
    assert_eq!(destroyed.load(Ordering::SeqCst), 0);

    Ok(())
}

#[test]
fn aggregate_test_with_destroy_callbacks() -> crate::Result<()> {
    for claim_cleanup in [false, true] {
        let aggregate = TrackedSum {
            claim_cleanup,
            ..TrackedSum::default()
        };
        let (live, destroyed) = (aggregate.live.clone(), aggregate.destroyed.clone());

        run_tracked_sum(aggregate, true)?;

        assert_eq!(live.load(Ordering::SeqCst), 0, "claim_cleanup = {claim_cleanup}");
        assert!(destroyed.load(Ordering::SeqCst) > 0, "claim_cleanup = {claim_cleanup}");
    }

    Ok(())
}

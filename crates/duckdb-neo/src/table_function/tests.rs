use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

use crate::{
    DuckDBType, Parameters, Result, ToValue,
    bind_arguments::BindArgument,
    connection::{Context, SettingScope},
    data_chunk::DataChunkRef,
    environment::{Environment, StorageLocation},
    error::Error,
    expression::ExpressionType,
    logical_type::LogicalTypeID,
    signature::{FunctionParameter, SignatureBuilder},
    table_function::{
        BindFunctionHandle, ExecColumnInfo, InitColumnData, OrderPreservation, PartitionData, PartitioningData,
        PushdownData, TableFilterPushdownCallbacks, TableFunctionBuilder, TableFunctionCallbacks, TablePartitionInfo,
        TablePartitioningCallbacks,
    },
};

#[test]
fn test_table_function() -> Result<()> {
    struct BindData {
        size: usize,
        accepted_threshold: Mutex<Option<i32>>,
    }

    struct GlobalStateCounter {
        max_rounds: usize,
        count: Mutex<usize>,
    }

    struct MyTableFunction {
        base: i32,
        pushdown_called: Arc<AtomicBool>,
        progress_called: Arc<AtomicBool>,
    }

    impl TableFunctionCallbacks for MyTableFunction {
        type BindData = BindData;
        type GlobalState = GlobalStateCounter;
        type LocalState = i32;

        fn bind(
            &self,
            context: &Context,
            arguments: Vec<BindArgument>,
            bind_handle: BindFunctionHandle<'_>,
        ) -> Result<Self::BindData> {
            let arg = &arguments[0];

            let val = &arg.value;

            assert_eq!(
                arg.logical_type.type_id(),
                LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER
            );
            assert_eq!(val.as_ref().unwrap().dbg_string()?, "10");

            bind_handle.add_result_column("out", &i32::logical_type(context)?)?;

            bind_handle.set_cardinality(1_000 * 10, true)?;
            bind_handle.set_order(OrderPreservation::None)?;

            Ok(BindData {
                size: val.as_ref().unwrap().get::<i32>()?.unwrap() as usize,
                accepted_threshold: Mutex::new(None),
            })
        }

        fn init_global_state(
            &self,
            bind_data: &Self::BindData,
            _context: &Context,
            column_data: InitColumnData<'_>,
        ) -> Result<(Option<Self::GlobalState>, Option<usize>)> {
            assert_eq!(bind_data.size, 10);
            assert_eq!(column_data.count()?, 1);

            Ok((
                Some(GlobalStateCounter {
                    max_rounds: 1_000,
                    count: Mutex::new(0),
                }),
                Some(12),
            ))
        }

        fn init_local_state(
            &self,
            bind_data: &Self::BindData,
            _global_state: Option<&Self::GlobalState>,
            _context: &Context,
            _column_data: InitColumnData<'_>,
        ) -> Result<Option<Self::LocalState>> {
            assert_eq!(bind_data.size, 10);
            Ok(Some(100))
        }

        fn progress(
            &self,
            bind_data: &Self::BindData,
            global_state: Option<&Self::GlobalState>,
            _context: &Context,
        ) -> Result<Option<f64>> {
            assert_eq!(bind_data.size, 10);
            let global_state = global_state.unwrap();
            let prog = global_state
                .count
                .lock()
                .map(|count| Some(*count as f64 / global_state.max_rounds as f64))
                .map_err(|e| Error::api_error(format!("Failed to acquire lock on global state: {}", e)))?;

            assert!(prog.is_some_and(|fraction| (0.0..=1.0).contains(&fraction)));
            self.progress_called.store(true, Ordering::SeqCst);
            Ok(prog)
        }

        fn exec(
            &self,
            bind_data: &Self::BindData,
            global_state: Option<&Self::GlobalState>,
            local_state: Option<&mut Self::LocalState>,
            _context: &Context,
            mut output: DataChunkRef<'_>,
            _column_info: ExecColumnInfo<'_>,
        ) -> Result<()> {
            let mut output_vector = output.get_vector_at_mut::<i32>(0)?;

            let global_state = global_state.unwrap();
            let mut count = global_state.count.lock().unwrap();

            if *count >= global_state.max_rounds {
                output_vector.set_size(0)?;
                return Ok(());
            }

            output_vector.set_size(bind_data.size)?;

            let local_offset = local_state.unwrap();
            let user_offset = self.base;
            let threshold = *bind_data.accepted_threshold.lock().unwrap();

            let mut written = 0;
            for i in 0..bind_data.size {
                let item = i as i32 + *local_offset + user_offset;
                if threshold.is_some_and(|threshold| item <= threshold) {
                    continue;
                }
                output_vector.write(written, Some(item))?;
                written += 1;
            }
            output_vector.set_size(written)?;

            *count += 1;

            Ok(())
        }
    }

    impl TableFilterPushdownCallbacks for MyTableFunction {
        fn pushdown_filter(
            &self,
            bind_data: &Self::BindData,
            context: &Context,
            pushdown_data: PushdownData<'_>,
        ) -> Result<()> {
            self.pushdown_called.store(true, Ordering::SeqCst);

            assert_eq!(pushdown_data.count()?, 1);
            assert_eq!(pushdown_data.get_column_index(0)?, 0);
            assert_eq!(pushdown_data.filter_count()?, 1);

            let filter = pushdown_data.filter(0)?;

            assert_eq!(filter.expression_type()?, ExpressionType::CompareGreaterThan);
            assert_eq!(filter.function_name()?, ">");
            assert_eq!(filter.child_count()?, 2);

            let column_ref = filter.child(0)?;
            assert_eq!(column_ref.expression_type()?, ExpressionType::BoundColumnRef);
            assert_eq!(column_ref.reference_index()?, 0);

            let constant = filter.child(1)?;
            assert_eq!(constant.expression_type()?, ExpressionType::ValueConstant);
            assert_eq!(constant.return_type()?, i32::logical_type(context)?);

            let threshold = constant
                .get_constant_value()?
                .get::<i32>()?
                .expect("filter constant must not be NULL");

            *bind_data.accepted_threshold.lock().unwrap() = Some(threshold);

            // We take full responsibility for enforcing this filter in `exec`,
            // so DuckDB does not need to re-check it on our output rows.
            pushdown_data.accept_pushdown(0)?;

            Ok(())
        }
    }
    let env = Environment::new()?;
    let db = env.open(StorageLocation::InMemory)?;
    let conn = db.connect()?;

    conn.config()?
        .set_text("enable_progress_bar_print", "false", SettingScope::Session)?;
    conn.config()?
        .set_text("enable_progress_bar", "true", SettingScope::Session)?;

    let pushdown_called = Arc::new(AtomicBool::new(false));
    let progress_called = Arc::new(AtomicBool::new(false));

    TableFunctionBuilder::new(
        "my_table_function",
        SignatureBuilder::new().param(FunctionParameter::new::<i32>("offset")),
        MyTableFunction {
            base: 42,
            pushdown_called: pushdown_called.clone(),
            progress_called: progress_called.clone(),
        },
    )
    .with_filter_pushdown()
    .with_progress()
    .register(&conn)?;

    // Each batch starts at 142; the pushed filter keeps 146..152.
    let mut result = conn.query("SELECT * FROM my_table_function(10) WHERE out > 145", Parameters::None)?;

    let mut row_count = 0;

    while let Some(chunk) = result.next_chunk()? {
        let vector = chunk.get_vector_at::<i32>(0)?;

        row_count += vector.len();

        for i in 0..vector.len() {
            let value = vector.get(i)?;

            assert!(value.is_some_and(|v| (146..152).contains(v)));
        }
    }

    assert!(
        pushdown_called.load(Ordering::SeqCst),
        "pushdown_filter was never called"
    );
    assert!(progress_called.load(Ordering::SeqCst), "progress was never called");
    assert_eq!(row_count, 1_000 * (152 - 146));

    Ok(())
}

#[test]
fn test_table_function_partitioning() -> Result<()> {
    const BATCH_COUNT: usize = 8;
    const ROWS_PER_BATCH: usize = 4;

    /// The batch a worker most recently produced, reported back to DuckDB by
    /// `partition_data`. Every batch carries a single distinct `part` value.
    struct ProducedBatch {
        batch_index: usize,
        part: i32,
    }

    struct PartitionedFunction {
        partitioning_calls: Arc<AtomicUsize>,
        partition_data_calls: Arc<AtomicUsize>,
        partition_value_calls: Arc<AtomicUsize>,
        local_init_calls: Arc<AtomicUsize>,
    }

    impl TableFunctionCallbacks for PartitionedFunction {
        type BindData = ();
        type GlobalState = AtomicUsize;
        type LocalState = Option<ProducedBatch>;

        fn bind(
            &self,
            context: &Context,
            _arguments: Vec<BindArgument>,
            bind_handle: BindFunctionHandle<'_>,
        ) -> Result<Self::BindData> {
            bind_handle.add_result_column("part", &i32::logical_type(context)?)?;
            bind_handle.add_result_column("val", &i32::logical_type(context)?)?;
            bind_handle.set_order(OrderPreservation::Insertion)?;

            Ok(())
        }

        fn init_global_state(
            &self,
            _bind_data: &Self::BindData,
            _context: &Context,
            _column_data: InitColumnData<'_>,
        ) -> Result<(Option<Self::GlobalState>, Option<usize>)> {
            Ok((Some(AtomicUsize::new(0)), Some(4)))
        }

        fn init_local_state(
            &self,
            _bind_data: &Self::BindData,
            _global_state: Option<&Self::GlobalState>,
            _context: &Context,
            _column_data: InitColumnData<'_>,
        ) -> Result<Option<Self::LocalState>> {
            self.local_init_calls.fetch_add(1, Ordering::SeqCst);
            Ok(Some(None))
        }

        fn exec(
            &self,
            _bind_data: &Self::BindData,
            global_state: Option<&Self::GlobalState>,
            local_state: Option<&mut Self::LocalState>,
            _context: &Context,
            mut output: DataChunkRef<'_>,
            _column_info: ExecColumnInfo<'_>,
        ) -> Result<()> {
            let batch_index = global_state.unwrap().fetch_add(1, Ordering::SeqCst);

            let [parts, values] = output.get_vector_array_mut([0, 1])?;
            let (mut parts, mut values) = (parts.cast::<i32>()?, values.cast::<i32>()?);

            if batch_index >= BATCH_COUNT {
                parts.set_size(0)?;
                return Ok(());
            }

            let part = batch_index as i32;

            parts.set_size(ROWS_PER_BATCH)?;
            values.set_size(ROWS_PER_BATCH)?;

            for row in 0..ROWS_PER_BATCH {
                parts.write(row, Some(part))?;
                values.write(row, Some(row as i32))?;
            }

            // `partition_data` runs on this thread, right after this batch.
            *local_state.unwrap() = Some(ProducedBatch { batch_index, part });

            Ok(())
        }
    }

    impl TablePartitioningCallbacks for PartitionedFunction {
        fn partitioning(
            &self,
            _bind_data: &Self::BindData,
            _context: &Context,
            partitioning_data: PartitioningData<'_>,
        ) -> Result<TablePartitionInfo> {
            self.partitioning_calls.fetch_add(1, Ordering::SeqCst);

            let column_count = partitioning_data.count()?;

            // Only the `part` column on its own describes our partitions.
            let single_value = column_count == 1 && partitioning_data.get_column_index(0)? == 0;

            Ok(if single_value {
                TablePartitionInfo::SingleValuePartitions
            } else {
                TablePartitionInfo::NotPartitioned
            })
        }

        fn partition_data(
            &self,
            _bind_data: &Self::BindData,
            _global_state: Option<&Self::GlobalState>,
            local_state: Option<&mut Self::LocalState>,
            context: &Context,
            partition_data: PartitionData<'_>,
        ) -> Result<usize> {
            self.partition_data_calls.fetch_add(1, Ordering::SeqCst);

            // DuckDB only asks when it needs the ordering position, the
            // partitioning column values, or both.
            assert!(partition_data.requires_batch_index()? || partition_data.requires_partition_columns()?);

            let batch = local_state
                .and_then(|state| state.as_ref())
                .expect("partition data is only requested after a batch was produced");

            if partition_data.requires_partition_columns()? {
                self.partition_value_calls.fetch_add(1, Ordering::SeqCst);
                for index in 0..partition_data.count()? {
                    // We only ever claim single-value partitions for `part`.
                    assert_eq!(partition_data.get_column_index(index)?, 0);

                    partition_data.set_partition_value(index, &batch.part.value(context)?)?;
                }
            } else {
                assert_eq!(partition_data.count()?, 0);
            }

            Ok(batch.batch_index)
        }
    }

    let env = Environment::new()?;
    let db = env.open(StorageLocation::InMemory)?;
    let conn = db.connect()?;

    let partitioning_calls = Arc::new(AtomicUsize::new(0));
    let partition_data_calls = Arc::new(AtomicUsize::new(0));
    let partition_value_calls = Arc::new(AtomicUsize::new(0));
    let local_init_calls = Arc::new(AtomicUsize::new(0));
    conn.config()?.set_text("threads", "4", SettingScope::Default)?;

    TableFunctionBuilder::new(
        "partitioned_range",
        SignatureBuilder::new(),
        PartitionedFunction {
            partitioning_calls: partitioning_calls.clone(),
            partition_data_calls: partition_data_calls.clone(),
            partition_value_calls: partition_value_calls.clone(),
            local_init_calls: local_init_calls.clone(),
        },
    )
    .with_partitioning()
    .register(&conn)?;

    // Grouping on `part` lets the optimizer ask whether our partitions carry a
    // single distinct value for it, which turns the hash aggregate into a
    // partitioned one and makes DuckDB request our partition data.
    let mut result = conn.query(
        "SELECT part, sum(val) FROM partitioned_range() GROUP BY part ORDER BY part",
        Parameters::None,
    )?;

    let mut rows = Vec::new();

    while let Some(chunk) = result.next_chunk()? {
        let parts = chunk.get_vector_at::<i32>(0)?;
        let sums = chunk.get_vector_at::<i128>(1)?;

        for row in 0..parts.len() {
            rows.push((*parts.get(row)?.unwrap(), *sums.get(row)?.unwrap()));
        }
    }

    let expected_sum = (0..ROWS_PER_BATCH).sum::<usize>() as i128;

    assert_eq!(
        rows,
        (0..BATCH_COUNT as i32)
            .map(|part| (part, expected_sum))
            .collect::<Vec<_>>()
    );

    assert!(
        partitioning_calls.load(Ordering::SeqCst) > 0,
        "partitioning was never called"
    );
    assert!(
        partition_data_calls.load(Ordering::SeqCst) > 0,
        "partition_data was never called"
    );
    assert!(
        partition_value_calls.load(Ordering::SeqCst) > 0,
        "partitioning column values were never requested"
    );

    let mut scan = conn.query("SELECT part, val FROM partitioned_range()", Parameters::None)?;
    let mut ordered_rows = Vec::new();
    while let Some(chunk) = scan.next_chunk()? {
        let parts = chunk.get_vector_at::<i32>(0)?;
        let values = chunk.get_vector_at::<i32>(1)?;
        for row in 0..parts.len() {
            ordered_rows.push((*parts.get(row)?.unwrap(), *values.get(row)?.unwrap()));
        }
    }
    assert_eq!(
        ordered_rows,
        (0..BATCH_COUNT as i32)
            .flat_map(|part| (0..ROWS_PER_BATCH as i32).map(move |value| (part, value)))
            .collect::<Vec<_>>()
    );

    let mut plan = conn.query(
        "EXPLAIN SELECT val, count(*) FROM partitioned_range() GROUP BY val",
        Parameters::None,
    )?;
    let mut plan_rows = 0;
    while let Some(chunk) = plan.next_chunk()? {
        let plans = chunk.get_vector_at::<String>(1)?;
        for row in 0..plans.len() {
            let text = plans.get(row)?.expect("EXPLAIN plan is not NULL");
            assert!(!text.contains("Partitioned Aggregate"));
            plan_rows += 1;
        }
    }
    assert!(plan_rows > 0);

    Ok(())
}

#[derive(Default)]
struct TableLifecycleCounter {
    created: AtomicUsize,
    dropped: AtomicUsize,
}

struct TableLifecycleToken(Arc<TableLifecycleCounter>);

impl TableLifecycleToken {
    fn new(counter: &Arc<TableLifecycleCounter>) -> Self {
        counter.created.fetch_add(1, Ordering::SeqCst);
        Self(counter.clone())
    }
}

impl Drop for TableLifecycleToken {
    fn drop(&mut self) {
        self.0.dropped.fetch_add(1, Ordering::SeqCst);
    }
}

struct ProjectionBindData {
    rows: usize,
    cursor: AtomicUsize,
    _lifetime: TableLifecycleToken,
}

struct ProjectionScan {
    states: bool,
    zero_threads: bool,
    counters: [Arc<TableLifecycleCounter>; 4],
    mappings: Arc<Mutex<Vec<Vec<usize>>>>,
    _lifetime: TableLifecycleToken,
}

impl TableFunctionCallbacks for ProjectionScan {
    type BindData = ProjectionBindData;
    type GlobalState = TableLifecycleToken;
    type LocalState = TableLifecycleToken;

    fn bind(
        &self,
        context: &Context,
        arguments: Vec<BindArgument>,
        bind_handle: BindFunctionHandle<'_>,
    ) -> Result<Self::BindData> {
        let rows = arguments[0].value.as_ref().unwrap().get::<i32>()?.unwrap_or(0);
        let logical_type = i32::logical_type(context)?;
        bind_handle.add_result_column("first", &logical_type)?;
        bind_handle.add_result_column("second", &logical_type)?;
        Ok(ProjectionBindData {
            rows: rows as usize,
            cursor: AtomicUsize::new(0),
            _lifetime: TableLifecycleToken::new(&self.counters[1]),
        })
    }

    fn init_global_state(
        &self,
        _bind_data: &Self::BindData,
        _context: &Context,
        columns: InitColumnData<'_>,
    ) -> Result<(Option<Self::GlobalState>, Option<usize>)> {
        let mapping = (0..columns.count()?)
            .map(|index| columns.get_column_index(index))
            .collect::<Result<Vec<_>>>()?;
        assert!(columns.get_column_index(columns.count()?).is_err());
        self.mappings.lock().unwrap().push(mapping);
        Ok((
            self.states.then(|| TableLifecycleToken::new(&self.counters[2])),
            self.zero_threads.then_some(0),
        ))
    }

    fn init_local_state(
        &self,
        _bind_data: &Self::BindData,
        global_state: Option<&Self::GlobalState>,
        _context: &Context,
        columns: InitColumnData<'_>,
    ) -> Result<Option<Self::LocalState>> {
        assert_eq!(global_state.is_some(), self.states);
        assert!(columns.count()? > 0);
        Ok(self.states.then(|| TableLifecycleToken::new(&self.counters[3])))
    }

    fn exec(
        &self,
        bind_data: &Self::BindData,
        global_state: Option<&Self::GlobalState>,
        local_state: Option<&mut Self::LocalState>,
        _context: &Context,
        mut output: DataChunkRef<'_>,
        columns: ExecColumnInfo<'_>,
    ) -> Result<()> {
        assert_eq!(global_state.is_some(), self.states);
        assert_eq!(local_state.is_some(), self.states);
        let start = bind_data.cursor.fetch_add(1_000, Ordering::SeqCst);
        let size = bind_data.rows.saturating_sub(start).min(1_000);
        for position in 0..columns.count()? {
            let declared = columns.get_column_index(position)?;
            let mut vector = output.get_vector_at_mut::<i32>(position)?;
            vector.set_size(size)?;
            for row in 0..size {
                vector.write(row, Some((start + row + declared * 10_000) as i32))?;
            }
        }
        assert!(columns.get_column_index(columns.count()?).is_err());
        Ok(())
    }
}

#[test]
fn test_table_function_projection_lifecycle() -> Result<()> {
    for states in [false, true] {
        let counters = std::array::from_fn(|_| Arc::new(TableLifecycleCounter::default()));
        let mappings = Arc::new(Mutex::new(Vec::new()));
        {
            let env = Environment::new()?;
            let db = env.open(StorageLocation::InMemory)?;
            let conn = db.connect()?;
            TableFunctionBuilder::new(
                "projection_scan",
                SignatureBuilder::new().param(FunctionParameter::new::<i32>("rows")),
                ProjectionScan {
                    states,
                    zero_threads: false,
                    counters: counters.clone(),
                    mappings: mappings.clone(),
                    _lifetime: TableLifecycleToken::new(&counters[0]),
                },
            )
            .with_projection_pushdown(true)
            .register(&conn)?;

            let mut result = conn.query("SELECT second FROM projection_scan(5000)", Parameters::None)?;
            let mut rows = Vec::new();
            while let Some(chunk) = result.next_chunk()? {
                let vector = chunk.get_vector_at::<i32>(0)?;
                for row in 0..vector.len() {
                    rows.push(vector.get(row)?.copied());
                }
            }
            assert_eq!(rows, (10_000..15_000).map(Some).collect::<Vec<_>>());
            assert!(mappings.lock().unwrap().contains(&vec![1]));

            let mut count = conn.query("SELECT count(*) FROM projection_scan(5000)", Parameters::None)?;
            let chunk = count.next_chunk()?.expect("count returns one row");
            assert_eq!(chunk.get_vector_at::<i64>(0)?.get(0)?, Some(&5_000));
            assert!(count.next_chunk()?.is_none());
            assert!(mappings.lock().unwrap().contains(&vec![0]));

            let mut empty = conn.query("SELECT * FROM projection_scan(NULL)", Parameters::None)?;
            assert!(empty.next_chunk()?.is_none());
        }
        for (index, counter) in counters.iter().enumerate() {
            let created = counter.created.load(Ordering::SeqCst);
            assert_eq!(created, counter.dropped.load(Ordering::SeqCst));
            assert_eq!(created > 0, index < 2 || states);
        }
    }
    Ok(())
}

#[test]
fn test_table_function_zero_threads() -> Result<()> {
    let counters = std::array::from_fn(|_| Arc::new(TableLifecycleCounter::default()));
    let env = Environment::new()?;
    let db = env.open(StorageLocation::InMemory)?;
    let conn = db.connect()?;
    TableFunctionBuilder::new(
        "zero_threads",
        SignatureBuilder::new().param(FunctionParameter::new::<i32>("rows")),
        ProjectionScan {
            states: false,
            zero_threads: true,
            counters: counters.clone(),
            mappings: Arc::new(Mutex::new(Vec::new())),
            _lifetime: TableLifecycleToken::new(&counters[0]),
        },
    )
    .register(&conn)?;
    let error = conn
        .query("SELECT * FROM zero_threads(1)", Parameters::None)
        .and_then(|mut result| result.drain())
        .expect_err("zero threads must fail");
    assert!(error.message.contains("maximum number of threads must be at least 1"));
    Ok(())
}

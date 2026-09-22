use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

use crate::{
    DuckDBType, Parameters,
    bind_arguments::BindArgument,
    connection::{Context, SettingScope},
    data_chunk::DataChunkRef,
    environment::{Environment, StorageLocation},
    error::{DuckDBError, Error},
    expression::ExpressionType,
    logical_type::LogicalTypeID,
    signature::{Parameter, SignatureBuilder},
    table_function::{BindFunctionHandle, ExecColumnInfo, TableFunctionCallbacks, TableFunctionCardinality},
};

#[test]
fn test_table_function() -> crate::Result<()> {
    use crate::Result;
    use crate::table_function::TableFunctionBuilder;

    struct BindData {
        size: usize,
        // Filled in by `pushdown_filter` once DuckDB offers a `>` filter on
        // the output column; `exec` honors it by dropping non-matching rows
        // itself, since accepting the pushdown means DuckDB won't re-apply it.
        accepted_threshold: Mutex<Option<i32>>,
    }

    struct GlobalStateCounter {
        pub max_rounds: usize,
        pub count: Mutex<usize>,
    }

    struct MyTableFunction {
        base: i32,
        pushdown_called: Arc<AtomicBool>,
    }

    impl TableFunctionCallbacks for MyTableFunction {
        type BindData = BindData;
        type GlobalState = GlobalStateCounter;
        type LocalState = i32;

        fn bind(
            &self,
            context: Context,
            arguments: Vec<BindArgument>,
            bind_handle: BindFunctionHandle<'_>,
        ) -> Result<(Self::BindData, Option<crate::table_function::TableFunctionCardinality>)> {
            let arg = &arguments[0];

            let val = &arg.value;

            assert_eq!(
                arg.logical_type.type_id(),
                LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER
            );
            assert_eq!(val.as_ref().unwrap().dbg_string()?, "10");

            bind_handle.add_result_column("out", i32::logical_type(&context)?)?;

            Ok((
                BindData {
                    size: 10,
                    accepted_threshold: Mutex::new(None),
                },
                Some(TableFunctionCardinality {
                    is_exact: true,
                    cardinality: 10_000_000,
                }),
            ))
        }

        fn init_global_state(
            &self,
            _bind_data: Option<&Self::BindData>,
            _context: Context,
            column_data: super::InitColumnData<'_>,
        ) -> crate::Result<(Option<Self::GlobalState>, Option<usize>)> {
            assert_eq!(column_data.get_column_count()?, 1);

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
            _bind_data: Option<&Self::BindData>,
            _context: Context,
            _global_state: Option<&Self::GlobalState>,
            _column_data: super::InitColumnData<'_>,
        ) -> crate::Result<Option<Self::LocalState>> {
            Ok(Some(100))
        }

        fn progress(
            &self,
            _bind_data: Option<&Self::BindData>,
            global_state: Option<&Self::GlobalState>,
            _context: Context,
        ) -> crate::Result<Option<f64>> {
            let global_state = global_state.unwrap();
            let prog = global_state
                .count
                .lock()
                .map(|count| Some(*count as f64 / global_state.max_rounds as f64))
                .map_err(|e| Error {
                    code: DuckDBError::DUCKDB_V2_ERROR_API,
                    message: format!("Failed to acquire lock on global state: {}", e),
                })?;

            Ok(prog)
        }

        fn exec(
            &self,
            bind_data: Option<&Self::BindData>,
            global_state: Option<&Self::GlobalState>,
            local_state: Option<&mut Self::LocalState>,
            _context: Context,
            output: DataChunkRef<'_>,
            _column_info: ExecColumnInfo<'_>,
        ) -> crate::Result<()> {
            let mut output_vector = output.get_vector_at::<i32>(0)?;

            let global_state = global_state.unwrap();
            let mut count = global_state.count.lock().unwrap();

            if *count >= global_state.max_rounds {
                output_vector.set_size(0)?;
                return Ok(());
            }

            output_vector.set_size(bind_data.unwrap().size)?;

            let local_offset = local_state.unwrap();
            let user_offset = self.base;
            let threshold = *bind_data.unwrap().accepted_threshold.lock().unwrap();

            let mut written = 0;
            for i in 0..bind_data.unwrap().size {
                let item = i as i32 + *local_offset + user_offset;
                if threshold.is_some_and(|threshold| item <= threshold) {
                    // We accepted this filter in `pushdown_filter`, so we must
                    // drop non-matching rows ourselves.
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

    impl super::TableFilterPushdownCallbacks for MyTableFunction {
        fn pushdown_filter(
            &self,
            bind_data: Option<&Self::BindData>,
            context: Context,
            column_data: super::PushdownData<'_>,
        ) -> crate::Result<()> {
            self.pushdown_called.store(true, Ordering::SeqCst);

            assert_eq!(column_data.get_column_count()?, 1);
            assert_eq!(column_data.get_column_index(0)?, 0);
            assert_eq!(column_data.filter_count()?, 1);

            let filter = column_data.filter(0)?;

            assert_eq!(filter.expression_type()?, ExpressionType::CompareGreaterThan);
            assert_eq!(filter.function_name()?, ">");
            assert_eq!(filter.child_count()?, 2);

            let column_ref = filter.child(0)?;
            assert_eq!(column_ref.expression_type()?, ExpressionType::BoundColumnRef);
            assert_eq!(column_ref.reference_index()?, 0);

            let constant = filter.child(1)?;
            assert_eq!(constant.expression_type()?, ExpressionType::ValueConstant);
            assert_eq!(constant.return_type()?, i32::logical_type(&context)?);

            let threshold = constant
                .get_constant_value()?
                .get::<i32>()?
                .expect("filter constant must not be NULL");

            *bind_data.unwrap().accepted_threshold.lock().unwrap() = Some(threshold);

            // We take full responsibility for enforcing this filter in `exec`,
            // so DuckDB does not need to re-check it on our output rows.
            column_data.accept_pushdown(0)?;

            Ok(())
        }
    }
    let env = Environment::new()?;
    let db = env.open(StorageLocation::InMemory)?;
    let conn = db.connect()?;

    conn.set_option("enable_progress_bar", "true", Some(SettingScope::Local))?;

    let pushdown_called = Arc::new(AtomicBool::new(false));

    TableFunctionBuilder::new(
        "my_table_function",
        SignatureBuilder::without_return_type([Parameter::normal("offset", i32::logical_type(&conn)?)]),
        MyTableFunction {
            base: 42,
            pushdown_called: pushdown_called.clone(),
        },
    )
    .with_filter_pushdown()
    .register(&conn)?;

    conn.execute("SET preserve_insertion_order=false", Parameters::None)?;

    // Every batch produces values in `142..152`; pushing `out > 145` down
    // into the table function should drop `142..=145` from every batch
    // instead of DuckDB filtering them out after the fact.
    let result = conn.query("SELECT * FROM my_table_function(10) WHERE out > 145", Parameters::None)?;

    let mut row_count = 0;

    for chunk in result {
        let chunk = chunk?;

        let vector = chunk.get_vector_at::<i32>(0)?;

        row_count += vector.len();

        for i in 0..vector.len() {
            let value = vector.get(i)?;

            assert!(value.is_some_and(|v| (&146..&152).contains(&v)));
        }
    }

    assert!(
        pushdown_called.load(Ordering::SeqCst),
        "pushdown_filter was never called"
    );
    assert!(
        row_count > 0,
        "expected at least one row to survive the pushed-down filter"
    );

    Ok(())
}

#[test]
fn test_table_function_partitioning() -> crate::Result<()> {
    use crate::{
        Result, ToValue,
        table_function::{
            PartitionData, PartitioningData, TableFunctionBuilder, TablePartitionInfo, TablePartitioningCallbacks,
        },
    };
    use std::sync::atomic::AtomicUsize;

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
    }

    impl TableFunctionCallbacks for PartitionedFunction {
        type BindData = ();
        type GlobalState = AtomicUsize;
        type LocalState = Option<ProducedBatch>;

        fn bind(
            &self,
            context: Context,
            _arguments: Vec<BindArgument>,
            bind_handle: BindFunctionHandle<'_>,
        ) -> Result<(Self::BindData, Option<TableFunctionCardinality>)> {
            bind_handle.add_result_column("part", i32::logical_type(&context)?)?;
            bind_handle.add_result_column("val", i32::logical_type(&context)?)?;

            Ok(((), None))
        }

        fn init_global_state(
            &self,
            _bind_data: Option<&Self::BindData>,
            _context: Context,
            _column_data: super::InitColumnData<'_>,
        ) -> Result<(Option<Self::GlobalState>, Option<usize>)> {
            Ok((Some(AtomicUsize::new(0)), None))
        }

        fn init_local_state(
            &self,
            _bind_data: Option<&Self::BindData>,
            _context: Context,
            _global_state: Option<&Self::GlobalState>,
            _column_data: super::InitColumnData<'_>,
        ) -> Result<Option<Self::LocalState>> {
            Ok(Some(None))
        }

        fn exec(
            &self,
            _bind_data: Option<&Self::BindData>,
            global_state: Option<&Self::GlobalState>,
            local_state: Option<&mut Self::LocalState>,
            _context: Context,
            output: DataChunkRef<'_>,
            _column_info: ExecColumnInfo<'_>,
        ) -> Result<()> {
            let batch_index = global_state.unwrap().fetch_add(1, Ordering::SeqCst);

            let mut parts = output.get_vector_at::<i32>(0)?;
            let mut values = output.get_vector_at::<i32>(1)?;

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
            _bind_data: Option<&Self::BindData>,
            _context: Context,
            partitioning_data: PartitioningData<'_>,
        ) -> Result<TablePartitionInfo> {
            self.partitioning_calls.fetch_add(1, Ordering::SeqCst);

            let column_count = partitioning_data.get_column_count()?;

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
            _bind_data: Option<&Self::BindData>,
            _global_state: Option<&Self::GlobalState>,
            local_state: Option<&mut Self::LocalState>,
            context: Context,
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
                for index in 0..partition_data.get_column_count()? {
                    // We only ever claim single-value partitions for `part`.
                    assert_eq!(partition_data.get_column_index(index)?, 0);

                    partition_data.set_partition_value(index, &batch.part.value(&context)?)?;
                }
            } else {
                assert_eq!(partition_data.get_column_count()?, 0);
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

    TableFunctionBuilder::new(
        "partitioned_range",
        SignatureBuilder::without_return_type([]),
        PartitionedFunction {
            partitioning_calls: partitioning_calls.clone(),
            partition_data_calls: partition_data_calls.clone(),
            partition_value_calls: partition_value_calls.clone(),
        },
    )
    .with_partitioning()
    .register(&conn)?;

    // Grouping on `part` lets the optimizer ask whether our partitions carry a
    // single distinct value for it, which turns the hash aggregate into a
    // partitioned one and makes DuckDB request our partition data.
    let result = conn.query(
        "SELECT part, sum(val) FROM partitioned_range() GROUP BY part ORDER BY part",
        Parameters::None,
    )?;

    let mut rows = Vec::new();

    for chunk in result {
        let chunk = chunk?;

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

    Ok(())
}

// TODO: Better tests; threads.

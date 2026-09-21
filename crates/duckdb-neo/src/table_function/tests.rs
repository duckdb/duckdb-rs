use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

use crate::{
    DuckDBType, Parameters,
    bind_arguments::BindArgument,
    connection::{Context, SettingScope},
    connection_options::ConfigOptionValue,
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
    let env = Environment::new()?;
    let db = env.open(StorageLocation::InMemory)?;
    let conn = db.connect()?;

    let option = ConfigOptionValue::new("enable_progress_bar", "true")?;
    conn.set_option(&option, Some(SettingScope::Local))?;

    let pushdown_called = Arc::new(AtomicBool::new(false));

    TableFunctionBuilder::new(
        "my_table_function",
        SignatureBuilder::without_return_type([Parameter::normal("offset", i32::logical_type(&conn)?)]),
        MyTableFunction {
            base: 42,
            pushdown_called: pushdown_called.clone(),
        },
    )
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

// TODO: Better tests; threads.

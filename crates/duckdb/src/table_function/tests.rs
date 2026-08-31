use std::sync::Mutex;

use crate::{
    Context, DuckDBType, Environment, Parameters, SettingScope, StorageLocation,
    bind_arguments::BindArguments,
    connection_options::OptionValue,
    data_chunk::DataChunk,
    error::{DuckDBError, Error},
    signature::{Parameter, SignatureBuilder},
    table_function::{BindFunctionHandle, TableFunctionCallbacks, TableFunctionCardinality},
};

#[test]
fn test_table_function() -> crate::Result<()> {
    use crate::Result;
    use crate::table_function::TableFunctionBuilder;

    struct BindData {
        size: usize,
    }

    struct GlobalStateCounter {
        pub max_rounds: usize,
        pub count: Mutex<usize>,
    }

    struct MyTableFunction {
        base: i32,
    }

    impl TableFunctionCallbacks for MyTableFunction {
        type BindData = BindData;
        type GlobalState = GlobalStateCounter;
        type LocalState = i32;

        fn cardinality(
            _bind_data: Option<&Self::BindData>,
            _context: Context,
        ) -> crate::Result<Option<TableFunctionCardinality>> {
            Ok(Some(TableFunctionCardinality {
                is_exact: true,
                cardinality: 10_000_000,
            }))
        }

        fn bind(
            &self,
            context: Context,
            arguments: BindArguments,
            bind_handle: BindFunctionHandle,
        ) -> Result<(Self::BindData, Option<crate::table_function::TableFunctionCardinality>)> {
            let val = arguments.fold(0, &context)?;

            assert_eq!(arguments.names()?, vec!["offset"]);
            assert_eq!(val.dbg_string()?, "10");

            bind_handle.add_result_column("out", i32::logical_type(&context)?)?;

            Ok((
                BindData { size: 10 },
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
            column_data: super::InitColumnData,
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
            _column_data: super::InitColumnData,
        ) -> crate::Result<Option<Self::LocalState>> {
            Ok(Some(100))
        }

        //TODO: Pushdown

        fn progress(
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
            output: DataChunk,
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

            for i in 0..bind_data.unwrap().size {
                let item = i as i32 + *local_offset + user_offset;
                output_vector.write(i, Some(item))?;
            }

            *count += 1;

            Ok(())
        }
    }
    let env = Environment::new()?;
    let db = env.open(StorageLocation::InMemory)?;
    let conn = db.connect()?;

    let option = OptionValue::new("enable_progress_bar", "true")?;
    conn.set_option(&option, Some(SettingScope::Local))?;

    TableFunctionBuilder::new(
        "my_table_function",
        SignatureBuilder::without_return_type([Parameter::normal("offset", i32::logical_type(&conn)?)]),
        MyTableFunction { base: 42 },
    )
    .register_with_connection(&conn)?;

    conn.execute("SET preserve_insertion_order=false", Parameters::None)?;
    let result = conn.query("SELECT * FROM my_table_function(10)", Parameters::None)?;

    for chunk in result {
        let chunk = chunk?;

        let vector = chunk.get_vector_at::<i32>(0)?;

        // dbg!("Vector length: {}", vector.len());

        for i in 0..vector.len() {
            let value = vector.get(i)?;

            //println!("{}", value.unwrap());

            assert!(value.is_some_and(|v| (&142..&152).contains(&v)));
        }
    }

    Ok(())
}

// TODO: Better tests; threads.

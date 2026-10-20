//! Links an extension into the test binary through `duckdb_register_static_extension`.
//!
//! The registration is process-wide, so every instance opened by this binary loads the extension.
#![cfg(feature = "build-extension-static")]

use duckdb_loadable_macros::duckdb_entrypoint_c_api_v2;
use duckdb_neo::{
    DuckDBType, Parameters,
    connection::{Connection, Context},
    data_chunk::VectorCollection,
    environment::{Environment, StorageLocation},
    extension::{Extension, register_static_extension},
    scalar::{ScalarCallbacks, ScalarFunctionBuilder},
    signature::{FunctionParameter, SignatureBuilder},
    vector::{Unknown, Vector},
};

struct TimesTen;

impl ScalarCallbacks for TimesTen {
    type BindData = ();
    type InitData = ();

    fn exec(
        &self,
        _bind_data: Option<&Self::BindData>,
        _init_data: Option<&mut Self::InitData>,
        _context: &Context,
        vectors: &mut VectorCollection,
        output: Vector<'_, Unknown>,
    ) -> duckdb_neo::Result<()> {
        let input = vectors.get_vector_at::<i32>(0)?;
        let mut output = output.cast::<i32>()?;
        output.set_size(input.len())?;
        for (i, item) in input.iter()?.enumerate() {
            output.write(i, item.map(|value| value * 10))?;
        }
        Ok(())
    }
}

#[duckdb_entrypoint_c_api_v2(extension_name = "neo_static_demo", api_version = "v2.0.0")]
fn entrypoint(extension: &Extension) -> duckdb_neo::Result<()> {
    ScalarFunctionBuilder::new(
        "neo_static_times_ten",
        SignatureBuilder::new()
            .param(FunctionParameter::new::<i32>("input"))
            .return_type(i32::logical_type(extension)?),
        TimesTen,
    )
    .register(extension)
}

fn connect() -> duckdb_neo::Result<Connection> {
    // SAFETY: the describe function comes from `#[duckdb_entrypoint_c_api_v2]`.
    unsafe { register_static_extension(duckdb_extension_neo_static_demo_describe)? };
    Environment::new()?.open(StorageLocation::InMemory)?.connect()
}

fn query_string(connection: &Connection, sql: &str) -> duckdb_neo::Result<Option<String>> {
    let mut result = connection.query(sql, Parameters::None)?;
    let chunk = result.next_chunk()?.expect("query should return a chunk");
    Ok(chunk.get_vector_at::<String>(0)?.get(0)?.map(str::to_owned))
}

#[test]
fn test_static_extension_loads_at_startup() -> duckdb_neo::Result<()> {
    let connection = connect()?;

    let mut result = connection.query("SELECT neo_static_times_ten(4)", Parameters::None)?;
    let chunk = result.next_chunk()?.expect("query should return a chunk");
    assert_eq!(chunk.get_vector_at::<i32>(0)?.get(0)?, Some(&40));
    drop(result);

    // DuckDB does not record the version of a static C API extension yet, so only check how it was loaded.
    assert_eq!(
        query_string(
            &connection,
            "SELECT concat_ws('|', loaded, install_mode) \
             FROM duckdb_extensions() WHERE extension_name = 'neo_static_demo'",
        )?
        .as_deref(),
        Some("true|STATICALLY_LINKED")
    );
    Ok(())
}

#[test]
fn test_static_extension_load_resolves_to_linked() -> duckdb_neo::Result<()> {
    let connection = connect()?;
    // Already loaded at startup, and must not go looking for a file.
    connection.execute("LOAD neo_static_demo", Parameters::None)?;
    Ok(())
}

#[test]
fn test_static_extension_registers_once() -> duckdb_neo::Result<()> {
    // Registering the same describe function again is a no-op, not a duplicate.
    connect()?;
    connect()?;
    Ok(())
}

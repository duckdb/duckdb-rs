use duckdb_loadable_macros::duckdb_entrypoint_c_api_v2;
use duckdb_neo::DuckDBType;
use duckdb_neo::{
    extension::Extension,
    scalar::{ScalarCallbacks, ScalarFunctionBuilder},
    signature::{FunctionParameter, SignatureBuilder},
};

struct AmplifyTest {}

impl ScalarCallbacks for AmplifyTest {
    type BindData = ();
    type InitData = ();

    fn exec(
        &self,
        _bind_data: Option<&Self::BindData>,
        _init_data: Option<&mut Self::InitData>,
        _context: &duckdb_neo::connection::Context,
        vectors: &mut duckdb_neo::data_chunk::VectorCollection,
        output: duckdb_neo::vector::Vector<'_, duckdb_neo::vector::Unknown>,
    ) -> duckdb_neo::Result<()> {
        let input = vectors.get_vector_at::<i32>(0)?.reader()?;

        let mut output = output.cast::<i32>()?;
        output.set_size(input.len())?;
        let mut writer = output.writer()?;

        for (i, item) in input.iter().enumerate() {
            writer.set(i, item.map(|value| value * 10))?;
        }

        Ok(())
    }
}

#[duckdb_entrypoint_c_api_v2(extension_name = "test", api_version = "v2.0.0")]
pub fn entrypoint(extension: &Extension) -> Result<(), Box<dyn ::std::error::Error>> {
    ScalarFunctionBuilder::new(
        "test",
        SignatureBuilder::new()
            .param(FunctionParameter::new::<i32>("input"))
            .return_type(i32::logical_type(extension)?),
        AmplifyTest {},
    )
    .register(extension)?;

    Ok(())
}

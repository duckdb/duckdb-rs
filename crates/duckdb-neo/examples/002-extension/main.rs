use duckdb_loadable_macros::duckdb_entrypoint_c_api_v2;
use duckdb_neo::{extension::Extension, scalar::MicroScalar, vector::Vector};

#[duckdb_entrypoint_c_api_v2(extension_name = "test", api_version = "v2.0.0")]
pub fn entrypoint(extension: &Extension) -> Result<(), Box<dyn ::std::error::Error>> {
    MicroScalar::create("test", |_ctx, input: Vector<'_, i32>, mut output: Vector<'_, i32>| {
        let mut writer = output.writer()?;
        for (i, item) in input.reader()?.iter().enumerate() {
            writer.set(i, item.map(|value| value * 10))?;
        }
        Ok(())
    })
    .register(extension)?;

    Ok(())
}

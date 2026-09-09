use std::env;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    if env::var_os("DEP_DUCKDB_STATIC").is_some() {
        return Ok(());
    }

    duckdb_build::emit_dynamic_linking_flags().expect("Failed to emit flags");

    Ok(())
}

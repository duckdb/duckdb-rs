use std::{env, path::Path};

use duckdb_build::RuntimeLocation;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    if env::var_os("DEP_DUCKDB_STATIC").is_some() {
        return Ok(());
    }

    // These arguments apply to this package's executables, not its dependents.
    let lib_dir = env::var_os("DEP_DUCKDB_LIB_DIR").ok_or("libduckdb-sys did not export its library directory")?;
    duckdb_build::emit_runtime_path(RuntimeLocation::Directory(Path::new(&lib_dir)))?;
    Ok(())
}

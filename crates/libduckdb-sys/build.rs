use std::{env, path::PathBuf};

fn generate_bindings(header_path: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let include_dir = header_path.parent().expect("resolved header has a parent");
    let bindings = bindgen::Builder::default()
        .header(header_path.to_string_lossy())
        .clang_arg(format!("-I{}", include_dir.display()))
        .default_enum_style(bindgen::EnumVariation::Rust { non_exhaustive: true })
        .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()))
        .generate()?;
    bindings.write_to_file(PathBuf::from(env::var("OUT_DIR")?).join("bindings.rs"))?;
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let library = duckdb_build::resolve_library()?;

    library.emit_rerun_calls();
    library.emit_linker_flags()?;
    generate_bindings(&library.header)?;

    Ok(())
}

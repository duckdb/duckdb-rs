use std::{env, path::PathBuf};

use duckdb_build::{Installation, LinkMode, RuntimeLocation};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-env-changed=LINK_DUCKDB_STATIC");
    let static_link = env::var_os("CARGO_FEATURE_STATIC").is_some()
        || env::var("LINK_DUCKDB_STATIC").is_ok_and(|value| value.eq_ignore_ascii_case("true"));
    let mode = if static_link {
        LinkMode::Static
    } else {
        LinkMode::Dynamic
    };
    let installation = Installation::discover(mode)?;

    println!("cargo:rustc-link-search=native={}", installation.lib_dir.display());
    println!("cargo:lib_dir={}", installation.lib_dir.display());
    println!("cargo:include_dir={}", installation.include_dir.display());

    if static_link {
        println!("cargo:static=1");
        println!("cargo:rustc-link-lib=static=duckdb_static");
        match env::var("CARGO_CFG_TARGET_OS").as_deref() {
            Ok("macos") | Ok("ios") => println!("cargo:rustc-link-lib=dylib=c++"),
            Ok("windows") if env::var("CARGO_CFG_TARGET_ENV").as_deref() == Ok("msvc") => {}
            _ => println!("cargo:rustc-link-lib=dylib=stdc++"),
        }
    } else {
        println!("cargo:rustc-link-lib=dylib=duckdb");
        duckdb_build::emit_runtime_path(RuntimeLocation::Directory(&installation.lib_dir))?;
    }

    let bindings = bindgen::Builder::default()
        .header(installation.include_dir.join("duckdb_v2.h").to_string_lossy())
        .clang_arg(format!("-I{}", installation.include_dir.display()))
        .default_enum_style(bindgen::EnumVariation::Rust { non_exhaustive: true })
        .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()))
        .generate()?;
    bindings.write_to_file(PathBuf::from(env::var("OUT_DIR")?).join("bindings.rs"))?;
    Ok(())
}

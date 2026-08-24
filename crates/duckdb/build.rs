use std::env;

fn main() {
    // Static linking has no runtime dylib dependency, so no rpath is needed.
    if env::var_os("DEP_DUCKDB_BUNDLED").is_some() {
        return;
    }

    // libduckdb-sys exports its resolved lib dir via `cargo:lib_dir` (links = "duckdb").
    // rustc-link-arg does not propagate to dependents, so re-emit the rpaths here so this
    // crate's binaries/tests can load libduckdb.dylib at runtime.
    let Ok(lib_dir) = env::var("DEP_DUCKDB_LIB_DIR") else {
        return;
    };
    match env::var("CARGO_CFG_TARGET_OS").as_deref() {
        Ok("macos") | Ok("ios") => {
            println!("cargo:rustc-link-arg=-Wl,-rpath,{lib_dir}");
            println!("cargo:rustc-link-arg=-Wl,-rpath,@loader_path");
            println!("cargo:rustc-link-arg=-Wl,-rpath,@loader_path/../lib");
        }
        Ok("linux") | Ok("android") => {
            println!("cargo:rustc-link-arg=-Wl,-rpath,{lib_dir}");
            println!("cargo:rustc-link-arg=-Wl,-rpath,$ORIGIN");
            println!("cargo:rustc-link-arg=-Wl,-rpath,$ORIGIN/../lib");
        }
        _ => {}
    }
}

use crate::{build_bundled::write_bindings, is_compiler, win_target};
use std::{
    env,
    path::{Path, PathBuf},
    process::Command,
};

pub fn main(_out_dir: &str, out_path: &Path) {
    let source_dir = Path::new("duckdb-sources");
    let cmake_lists = source_dir.join("CMakeLists.txt");
    if !cmake_lists.exists() {
        panic!(
            "`bundled-cmake` requires a duckdb-rs checkout with DuckDB sources at {}",
            cmake_lists.display()
        );
    }

    println!("cargo:rerun-if-changed=duckdb-sources");
    println!("cargo:rerun-if-env-changed=DUCKDB_DISABLE_EXTENSION_LOAD");
    println!("cargo:rerun-if-env-changed=DUCKDB_EXTENSION_CONFIGS");
    println!("cargo:rerun-if-env-changed=DUCKDB_CMAKE_BUILD_TYPE");
    println!("cargo:rerun-if-env-changed=CMAKE_BUILD_TYPE");
    println!("cargo:rerun-if-env-changed=CMAKE_GENERATOR");
    println!("cargo:rerun-if-env-changed=MACOSX_DEPLOYMENT_TARGET");

    write_bindings(&source_dir.join("src/include"), out_path);

    let mut config = cmake::Config::new(source_dir);
    config.out_dir(cmake_out_dir());
    prefer_ninja_generator(&mut config);
    configure_macos_deployment_target(&mut config);
    config
        .profile(&cmake_build_type())
        .define("BUILD_UNITTESTS", "0")
        .define("BUILD_SHELL", "0")
        .define("DISABLE_UNITY", "1");

    let enabled_extensions = enabled_extensions();
    if !enabled_extensions.is_empty() {
        config.define("BUILD_EXTENSIONS", enabled_extensions.join(";"));
    }

    let skipped_extensions = skipped_extensions();
    if !skipped_extensions.is_empty() {
        config.define("SKIP_EXTENSIONS", skipped_extensions.join(";"));
    }

    if env_var_truthy("DUCKDB_DISABLE_EXTENSION_LOAD") {
        config.define("DISABLE_EXTENSION_LOAD", "1");
    } else {
        config
            .define("ENABLE_EXTENSION_AUTOLOADING", "1")
            .define("ENABLE_EXTENSION_AUTOINSTALL", "1");
    }

    if let Ok(configs) = env::var("DUCKDB_EXTENSION_CONFIGS") {
        if !configs.trim().is_empty() {
            config.define("DUCKDB_EXTENSION_CONFIGS", configs);
        }
    }

    let dst = config.build();
    let lib_dir = dst.join("lib");
    println!("cargo:rustc-link-search=native={}", lib_dir.display());
    println!("cargo:rustc-link-lib=static=duckdb_generated_extension_loader");
    println!("cargo:rustc-link-lib=static=core_functions_extension");
    for extension in enabled_extensions {
        println!("cargo:rustc-link-lib=static={extension}_extension");
    }
    println!("cargo:rustc-link-lib=static=duckdb_static");
    link_system_libs();
    println!("cargo:lib_dir={}", lib_dir.display());
}

fn cmake_build_type() -> String {
    env::var("DUCKDB_CMAKE_BUILD_TYPE")
        .ok()
        .or_else(|| env::var("CMAKE_BUILD_TYPE").ok())
        .filter(|value| !value.trim().is_empty())
        .map(|value| validate_cmake_build_type(&value))
        .unwrap_or_else(|| "Release".to_owned())
}

fn validate_cmake_build_type(value: &str) -> String {
    match value {
        "Debug" | "Release" | "RelWithDebInfo" | "MinSizeRel" => value.to_owned(),
        _ => {
            panic!("unsupported CMake build type `{value}`; expected one of Debug, Release, RelWithDebInfo, MinSizeRel")
        }
    }
}

fn env_var_truthy(name: &str) -> bool {
    env::var(name)
        .map(|value| matches!(value.to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
        .unwrap_or(false)
}

fn cmake_out_dir() -> PathBuf {
    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR should be set by Cargo"));
    let generator = env::var("CMAKE_GENERATOR")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| {
            if find_program(&["ninja", "ninja-build"]).is_some() {
                "ninja".to_string()
            } else {
                "default".to_string()
            }
        });
    out_dir.join(format!("cmake-{}", sanitize_path_component(&generator)))
}

fn prefer_ninja_generator(config: &mut cmake::Config) {
    if env::var_os("CMAKE_GENERATOR").is_some() {
        return;
    }

    if let Some(ninja) = find_program(&["ninja", "ninja-build"]) {
        config.generator("Ninja");
        config.env("CMAKE_MAKE_PROGRAM", ninja);
    }
}

fn configure_macos_deployment_target(config: &mut cmake::Config) {
    if env::var("CARGO_CFG_TARGET_OS").as_deref() != Ok("macos") || env::var("TARGET").ok() != env::var("HOST").ok() {
        return;
    }

    let deployment_target = env::var("MACOSX_DEPLOYMENT_TARGET").unwrap_or_else(|_| "11.0".to_string());
    let target_arch_env = env::var("CARGO_CFG_TARGET_ARCH");
    let target_arch = match target_arch_env.as_deref() {
        Ok("aarch64") => "arm64",
        Ok(other) => other,
        Err(_) => "arm64",
    };
    config.define("CMAKE_OSX_DEPLOYMENT_TARGET", &deployment_target);
    config.define("CMAKE_OSX_ARCHITECTURES", target_arch);
    config.define("CMAKE_C_FLAGS", "-w");
    config.define("CMAKE_CXX_FLAGS", "-w");
}

fn find_program(candidates: &[&str]) -> Option<String> {
    for candidate in candidates {
        let status = Command::new(candidate).arg("--version").output().ok()?;
        if status.status.success() {
            return Some((*candidate).to_string());
        }
    }
    None
}

fn sanitize_path_component(input: &str) -> String {
    input
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '_'
            }
        })
        .collect()
}

fn enabled_extensions() -> Vec<&'static str> {
    // Match DuckDB's upstream CMake defaults for quasi-core extensions rather
    // than trying to reproduce the cc backend's finer-grained selection.
    let mut extensions = vec!["parquet"];
    if cfg!(feature = "json") {
        extensions.push("json");
    }
    if cfg!(feature = "autocomplete") {
        extensions.push("autocomplete");
    }
    if cfg!(feature = "icu") {
        extensions.push("icu");
    }
    if cfg!(feature = "tpcds") {
        extensions.push("tpcds");
    }
    if cfg!(feature = "tpch") {
        extensions.push("tpch");
    }
    extensions
}

fn skipped_extensions() -> Vec<&'static str> {
    // Keep this in sync with DuckDB's always-loaded base extension config in
    // duckdb-sources/extension/extension_config.cmake. We intentionally keep
    // upstream's default parquet linkage in the CMake backend.
    vec!["jemalloc"]
}

fn link_system_libs() {
    let target_os = env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();

    match target_os.as_str() {
        "linux" => println!("cargo:rustc-link-lib=dylib=dl"),
        "windows" => {
            println!("cargo:rustc-link-lib=dylib=ws2_32");
            println!("cargo:rustc-link-lib=dylib=rstrtmgr");
            if is_compiler("msvc") {
                println!("cargo:rustc-link-lib=dylib=bcrypt");
            }
        }
        _ => {}
    }

    if !(win_target() && is_compiler("msvc")) {
        let cxx_runtime = if target_os == "macos" { "c++" } else { "stdc++" };
        println!("cargo:rustc-link-lib=dylib={cxx_runtime}");
    }
}

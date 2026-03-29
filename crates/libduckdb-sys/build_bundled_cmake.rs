use crate::{is_compiler, win_target, write_bindings};
use std::{
    env, fs, io,
    path::{Path, PathBuf},
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

    let cmake_build_type = cmake_build_type();
    let (cmake_out_dir, ninja_program, generator_description) = select_generator();
    cargo_warning(&format!("bundled-cmake generator: {generator_description}"));
    let mut config = cmake::Config::new(source_dir);
    config.out_dir(cmake_out_dir);
    if let Some(ninja) = ninja_program.as_deref() {
        config.generator("Ninja");
        config.env("CMAKE_MAKE_PROGRAM", ninja);
    }
    configure_macos_deployment_target(&mut config);
    config
        .profile(&cmake_build_type)
        .define("BUILD_UNITTESTS", "0")
        .define("BUILD_SHELL", "0")
        .define("CMAKE_C_FLAGS_INIT", warning_suppression_flag())
        .define("CMAKE_CXX_FLAGS_INIT", warning_suppression_flag())
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
            panic!(
                "DUCKDB_EXTENSION_CONFIGS is not yet supported by bundled-cmake because additional static extension libraries are not auto-linked; using it would produce a broken binary"
            );
        }
    }

    let dst = config.build();
    let lib_dir = dst.join("lib");
    println!("cargo:rustc-link-search=native={}", lib_dir.display());
    // Preserve the static dependency order for single-pass linkers:
    // duckdb_static -> duckdb_generated_extension_loader -> *_extension.
    link_static_library(&lib_dir, &cmake_build_type, "duckdb_static");
    link_static_library(&lib_dir, &cmake_build_type, "duckdb_generated_extension_loader");
    link_static_library(&lib_dir, &cmake_build_type, "core_functions_extension");
    for extension in enabled_extensions {
        link_static_library(&lib_dir, &cmake_build_type, &format!("{extension}_extension"));
    }
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

fn select_generator() -> (PathBuf, Option<String>, String) {
    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR should be set by Cargo"));
    if let Some(generator) = env::var("CMAKE_GENERATOR")
        .ok()
        .filter(|value| !value.trim().is_empty())
    {
        if generator.contains("Ninja") && find_program(&["ninja", "ninja-build"]).is_none() {
            panic!("CMAKE_GENERATOR={generator} requires `ninja` or `ninja-build` on PATH, but neither was found");
        }
        let cmake_out_dir = out_dir.join(format!("cmake-{}", sanitize_path_component(&generator)));
        return (cmake_out_dir, None, format!("{generator} (from CMAKE_GENERATOR)"));
    }

    let ninja_program = find_program(&["ninja", "ninja-build"]);
    let out_dir_component = if ninja_program.is_some() {
        "ninja".to_string()
    } else {
        "default".to_string()
    };
    let generator_description = if let Some(ninja) = ninja_program.as_deref() {
        format!("Ninja (autodetected via {ninja})")
    } else {
        "default CMake generator (ninja not detected)".to_string()
    };
    (
        out_dir.join(format!("cmake-{}", sanitize_path_component(&out_dir_component))),
        ninja_program,
        generator_description,
    )
}

fn warning_suppression_flag() -> &'static str {
    if win_target() && is_compiler("msvc") {
        "/w"
    } else {
        "-w"
    }
}

fn configure_macos_deployment_target(config: &mut cmake::Config) {
    if env::var("CARGO_CFG_TARGET_OS").as_deref() != Ok("macos") {
        return;
    }

    let deployment_target = env::var("MACOSX_DEPLOYMENT_TARGET").unwrap_or_else(|_| "11.0".to_string());
    let target_arch_env =
        env::var("CARGO_CFG_TARGET_ARCH").expect("Cargo should set CARGO_CFG_TARGET_ARCH for macOS builds");
    let target_arch = match target_arch_env.as_str() {
        "aarch64" => "arm64",
        other => other,
    };
    cargo_warning(&format!(
        "bundled-cmake macOS deployment target: {deployment_target} ({target_arch})"
    ));
    config.define("CMAKE_OSX_DEPLOYMENT_TARGET", &deployment_target);
    config.define("CMAKE_OSX_ARCHITECTURES", target_arch);
}

fn find_program(candidates: &[&str]) -> Option<String> {
    for candidate in candidates {
        match which::which(candidate) {
            Ok(_) => return Some(candidate.to_string()),
            Err(which::Error::CannotFindBinaryPath) => {}
            Err(err) => {
                cargo_warning(&format!(
                    "failed to probe `{candidate}` for bundled-cmake generator detection: {err}"
                ));
            }
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

fn link_static_library(lib_dir: &Path, cmake_build_type: &str, name: &str) {
    let library_path = match resolve_static_library(lib_dir, cmake_build_type, name) {
        Ok(Some(path)) => path,
        Ok(None) => {
            let search = describe_library_search(lib_dir, cmake_build_type, name)
                .unwrap_or_else(|err| format!("unable to inspect library directory {}: {err}", lib_dir.display()));
            panic!(
                "expected bundled-cmake to produce static library `{}`; looked in {}",
                name, search
            );
        }
        Err(err) => {
            panic!(
                "failed to inspect bundled-cmake library directory {} while looking for `{}`: {err}",
                lib_dir.display(),
                name
            );
        }
    };
    if let Some(parent) = library_path.parent() {
        if parent != lib_dir {
            println!("cargo:rustc-link-search=native={}", parent.display());
        }
    }
    println!("cargo:rustc-link-lib=static={name}");
}

fn static_library_filename(name: &str) -> String {
    if win_target() {
        format!("{name}.lib")
    } else {
        format!("lib{name}.a")
    }
}

fn resolve_static_library(lib_dir: &Path, cmake_build_type: &str, name: &str) -> io::Result<Option<PathBuf>> {
    let filename = static_library_filename(name);
    for candidate_dir in candidate_library_dirs(lib_dir, cmake_build_type)? {
        let candidate = candidate_dir.join(&filename);
        if candidate.exists() {
            return Ok(Some(candidate));
        }
    }
    Ok(None)
}

fn candidate_library_dirs(lib_dir: &Path, cmake_build_type: &str) -> io::Result<Vec<PathBuf>> {
    let mut candidates = vec![lib_dir.to_path_buf()];
    let build_type_dir = lib_dir.join(cmake_build_type);
    if build_type_dir.exists() {
        candidates.push(build_type_dir);
    }
    let mut child_dirs = fs::read_dir(lib_dir)?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.is_dir())
        .collect::<Vec<_>>();
    child_dirs.sort();
    for child_dir in child_dirs {
        if !candidates.contains(&child_dir) {
            candidates.push(child_dir);
        }
    }
    Ok(candidates)
}

fn describe_library_search(lib_dir: &Path, cmake_build_type: &str, name: &str) -> io::Result<String> {
    let filename = static_library_filename(name);
    let searched = candidate_library_dirs(lib_dir, cmake_build_type)?
        .into_iter()
        .map(|dir| dir.join(&filename).display().to_string())
        .collect::<Vec<_>>();
    let contents = describe_directory_entries(lib_dir)?;
    Ok(format!(
        "paths [{}]; lib dir contents [{}]",
        searched.join(", "),
        contents.join(", ")
    ))
}

fn describe_directory_entries(dir: &Path) -> io::Result<Vec<String>> {
    let mut entries = fs::read_dir(dir)?
        .filter_map(Result::ok)
        .map(|entry| entry.path().display().to_string())
        .collect::<Vec<_>>();
    entries.sort();
    if entries.is_empty() {
        entries.push("<empty>".to_string());
    }
    Ok(entries)
}

fn cargo_warning(message: &str) {
    println!("cargo:warning={message}");
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
    let target_os = env::var("CARGO_CFG_TARGET_OS").expect("Cargo should set CARGO_CFG_TARGET_OS");

    match target_os.as_str() {
        "android" | "freebsd" | "linux" => {
            println!("cargo:rustc-link-lib=dylib=dl");
            println!("cargo:rustc-link-lib=dylib=pthread");
        }
        "macos" => {}
        "windows" => {
            println!("cargo:rustc-link-lib=dylib=ws2_32");
            println!("cargo:rustc-link-lib=dylib=rstrtmgr");
            if is_compiler("msvc") {
                println!("cargo:rustc-link-lib=dylib=bcrypt");
            }
        }
        other => panic!("unsupported target OS `{other}` for bundled-cmake"),
    }

    if !(win_target() && is_compiler("msvc")) {
        let cxx_runtime = if target_os == "macos" { "c++" } else { "stdc++" };
        println!("cargo:rustc-link-lib=dylib={cxx_runtime}");
    }
}

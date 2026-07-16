use crate::{is_compiler, link_windows_system_libs, win_target, write_bindings};
use std::{
    collections::BTreeSet,
    env::{self, VarError},
    fs, io,
    path::{Path, PathBuf},
};

struct Generator {
    name: Option<String>,
    out_dir: PathBuf,
    make_program: Option<String>,
}

pub fn main(out_dir: &str, out_path: &Path) {
    let source_dir = Path::new("duckdb-sources");
    let cmake_lists = source_dir.join("CMakeLists.txt");
    if !cmake_lists.exists() {
        panic!(
            "`bundled-cmake` requires a duckdb-rs checkout with DuckDB sources at {}",
            cmake_lists.display()
        );
    }

    println!("cargo:rerun-if-changed={}", source_dir.display());
    println!("cargo:rerun-if-env-changed=DUCKDB_EXTENSION_CONFIGS");
    println!("cargo:rerun-if-env-changed=DUCKDB_CMAKE_BUILD_TYPE");
    println!("cargo:rerun-if-env-changed=DUCKDB_DISABLE_UNITY");
    println!("cargo:rerun-if-env-changed=DUCKDB_DISABLE_JEMALLOC");
    println!("cargo:rerun-if-env-changed=DUCKDB_DISABLE_EXTENSION_LOAD");
    println!("cargo:rerun-if-env-changed=DISABLE_EXTENSION_LOAD");
    println!("cargo:rerun-if-env-changed=CMAKE_BUILD_TYPE");
    println!("cargo:rerun-if-env-changed=CMAKE_GENERATOR");
    println!("cargo:rerun-if-env-changed=CMAKE_C_COMPILER_LAUNCHER");
    println!("cargo:rerun-if-env-changed=CMAKE_CXX_COMPILER_LAUNCHER");
    println!("cargo:rerun-if-env-changed=MACOSX_DEPLOYMENT_TARGET");

    let include_path = source_dir.join("src/include").canonicalize().unwrap();
    write_bindings(&include_path, out_path);

    // Publish the include directory so downstream crates that compile
    // their own C/C++ code can read it from `DEP_DUCKDB_INCLUDE`.
    // See the matching emission in build_bundled_cc.rs.
    println!("cargo:include={}", include_path.display());

    if let Some(configs) = env_var("DUCKDB_EXTENSION_CONFIGS") {
        if !configs.trim().is_empty() {
            panic!(
                "DUCKDB_EXTENSION_CONFIGS is not yet supported by bundled-cmake because additional static extension libraries are not auto-linked"
            );
        }
    }

    let cmake_build_type = cmake_build_type();
    let generator = select_generator(out_dir);
    let mut config = cmake::Config::new(source_dir);
    config.out_dir(&generator.out_dir);
    if let Some(generator_name) = generator.name.as_deref() {
        config.generator(generator_name);
    }
    if let Some(make_program) = generator.make_program.as_deref() {
        config.define("CMAKE_MAKE_PROGRAM", make_program);
    }
    configure_macos_deployment_target(&mut config);
    config
        .profile(&cmake_build_type)
        .define("BUILD_UNITTESTS", "0")
        .define("BUILD_SHELL", "0")
        .define("CMAKE_INSTALL_LIBDIR", "lib")
        .define("CMAKE_C_FLAGS_INIT", warning_suppression_flag())
        .define("CMAKE_CXX_FLAGS_INIT", warning_suppression_flag());

    // Re-enable C++ exceptions on MSVC. The cmake crate makes its `cxxflag`s the
    // source of -DCMAKE_CXX_FLAGS, replacing CMake's default (which includes /EHsc),
    // so exception handling ends up effectively off: a thrown exception (e.g.
    // invalid-UTF-8 CSV read) then aborts with STATUS_STACK_BUFFER_OVERRUN (#774).
    //
    // Don't `config.define("CMAKE_CXX_FLAGS", ..)` here: it overrides the cxxflag
    // above and silently drops /EHsc, reintroducing #774.
    // Mirrors build_bundled_cc.rs; gcc/clang enable exceptions by default.
    if win_target() && is_compiler("msvc") {
        config.cxxflag("/EHsc");
    }

    // Unity builds (DuckDB's default) combine .cpp files into fewer translation
    // units and compile significantly faster. Allow opting out for debugging.
    // Always set explicitly so the CMake cache doesn't keep a stale value.
    let disable_unity = env_flag("DUCKDB_DISABLE_UNITY");
    config.define("DISABLE_UNITY", if disable_unity { "1" } else { "0" });

    // Always set explicitly so the CMake cache does not keep a stale value when
    // the environment variable changes between builds.
    let disable_extension_load = env_flag_aliases(&["DUCKDB_DISABLE_EXTENSION_LOAD", "DISABLE_EXTENSION_LOAD"]);
    config.define("DISABLE_EXTENSION_LOAD", if disable_extension_load { "1" } else { "0" });

    // Forward compiler launcher for sccache/ccache integration.
    for var in ["CMAKE_C_COMPILER_LAUNCHER", "CMAKE_CXX_COMPILER_LAUNCHER"] {
        if let Some(launcher) = env_var(var) {
            config.define(var, &launcher);
        }
    }

    let enabled_extensions = enabled_extensions();
    if !enabled_extensions.is_empty() {
        config.define("BUILD_EXTENSIONS", enabled_extensions.join(";"));
    }

    // Always set explicitly so old CMake caches do not keep stale allocator
    // state. By default this enables jemalloc on DuckDB's supported 64-bit,
    // non-musl Linux targets and remains a no-op elsewhere. The env var is an
    // escape hatch back to DuckDB's standard allocator.
    let disable_jemalloc = env_flag("DUCKDB_DISABLE_JEMALLOC");
    config.define("SKIP_EXTENSIONS", "");
    config.define("ENABLE_JEMALLOC", if disable_jemalloc { "OFF" } else { "ON" });

    // Upstream CMake defaults these to OFF, but duckdb-rs `bundled` has historically
    // shipped with both enabled. Keep `bundled-cmake` aligned with `bundled` unless
    // extension loading is compiled out entirely.
    config
        .define(
            "ENABLE_EXTENSION_AUTOLOADING",
            if disable_extension_load { "0" } else { "1" },
        )
        .define(
            "ENABLE_EXTENSION_AUTOINSTALL",
            if disable_extension_load { "0" } else { "1" },
        );

    // Windows MAX_PATH caveat: this build emits very deep object paths (Ninja ignores
    // CMAKE_OBJECT_PATH_MAX), so a deep OUT_DIR can exceed 260 chars and fail late with
    // `C1083: Cannot open compiler generated file: ''`. The build script can't move
    // OUT_DIR — shorten the path (short CARGO_TARGET_DIR, drop `--target`, or check out
    // nearer the drive root).
    let dst = config.build();
    let lib_dir = dst.join("lib");
    validate_extension_libraries(&lib_dir, &cmake_build_type, &enabled_extensions);
    println!("cargo:rustc-link-search=native={}", lib_dir.display());
    // Emit in dependents-before-dependencies order for single-pass linkers:
    // loader → extensions → duckdb_static (which satisfies all core symbols).
    link_static_library(&lib_dir, &cmake_build_type, "duckdb_generated_extension_loader");
    link_static_library(&lib_dir, &cmake_build_type, "core_functions_extension");
    for extension in enabled_extensions {
        link_static_library(&lib_dir, &cmake_build_type, &format!("{extension}_extension"));
    }
    link_static_library(&lib_dir, &cmake_build_type, "duckdb_static");
    link_system_libs();

    // Compile the zero-copy Arrow registration shim under the cmake backend.
    // The amalgamation's headers are available via `include_path`; the shim links
    // against the same DuckDB API surface as the cc-backend path.
    // Compiled unconditionally (no vtab-arrow gate), mirroring the cc backend.
    {
        let mut shim = cc::Build::new();
        shim.file("src/arrow_zerocopy_shim.cpp")
            .include(&include_path)
            .cpp(true)
            .flag_if_supported("-std=c++11")
            .flag_if_supported("/utf-8")
            .warnings(false)
            .flag_if_supported("-w");
        if win_target() && is_compiler("msvc") {
            shim.flag("/EHsc");
        }
        shim.compile("arrow_zerocopy_shim");
        println!("cargo:rerun-if-changed=src/arrow_zerocopy_shim.cpp");
    }

    println!("cargo:lib_dir={}", lib_dir.display());
}

fn cmake_build_type() -> String {
    for var in ["DUCKDB_CMAKE_BUILD_TYPE", "CMAKE_BUILD_TYPE"] {
        if let Some(value) = env_var(var).filter(|v| !v.trim().is_empty()) {
            let value = validate_cmake_build_type(&value);
            cargo_warning(&format!("bundled-cmake build type: {value} (from {var})"));
            return value;
        }
    }
    cargo_warning("bundled-cmake build type: Release (default)");
    "Release".to_owned()
}

fn validate_cmake_build_type(value: &str) -> String {
    match value {
        "Debug" | "Release" | "RelWithDebInfo" | "MinSizeRel" => value.to_owned(),
        _ => {
            panic!("unsupported CMake build type `{value}`; expected one of Debug, Release, RelWithDebInfo, MinSizeRel")
        }
    }
}

fn select_generator(out_dir: &str) -> Generator {
    let out_dir = PathBuf::from(out_dir);
    if let Some(generator) = env_var("CMAKE_GENERATOR").filter(|value| !value.trim().is_empty()) {
        let needs_ninja = generator.contains("Ninja");
        let (make_program, probe_error) = if needs_ninja {
            find_program(&["ninja", "ninja-build"])
        } else {
            (None, None)
        };
        if needs_ninja && make_program.is_none() {
            let suffix = probe_error
                .map(|error| format!(" (probe error: {error})"))
                .unwrap_or_default();
            panic!(
                "CMAKE_GENERATOR={generator} requires `ninja` or `ninja-build` on PATH, but neither was found{suffix}"
            );
        }
        cargo_warning(&format!("bundled-cmake generator: {generator} (from CMAKE_GENERATOR)"));
        return Generator {
            out_dir: out_dir.join(format!("cmake-{}", sanitize_path_component(&generator))),
            name: Some(generator),
            make_program,
        };
    }

    let (ninja_program, probe_error) = find_program(&["ninja", "ninja-build"]);
    if let Some(ninja) = ninja_program {
        cargo_warning(&format!("bundled-cmake generator: Ninja (autodetected via {ninja})"));
        Generator {
            out_dir: out_dir.join("cmake-ninja"),
            name: Some("Ninja".to_string()),
            make_program: Some(ninja),
        }
    } else {
        if let Some(probe_error) = probe_error {
            cargo_warning(&format!(
                "failed to probe for bundled-cmake generator detection (`ninja`/`ninja-build`): {probe_error}"
            ));
        }
        cargo_warning("bundled-cmake generator: default CMake generator (ninja not detected)");
        Generator {
            out_dir: out_dir.join("cmake-default"),
            name: None,
            make_program: None,
        }
    }
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

    let target_arch_env =
        env::var("CARGO_CFG_TARGET_ARCH").expect("Cargo should set CARGO_CFG_TARGET_ARCH for macOS builds");
    let target_arch = match target_arch_env.as_str() {
        "aarch64" => "arm64",
        "x86_64" => "x86_64",
        other => {
            panic!("bundled-cmake: unsupported macOS CARGO_CFG_TARGET_ARCH `{other}`; expected `aarch64` or `x86_64`")
        }
    };

    let deployment_target = env_var("MACOSX_DEPLOYMENT_TARGET")
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "11.0".to_string());
    cargo_warning(&format!(
        "bundled-cmake macOS deployment target: {deployment_target} ({target_arch})"
    ));
    config.define("CMAKE_OSX_DEPLOYMENT_TARGET", &deployment_target);
    config.define("CMAKE_OSX_ARCHITECTURES", target_arch);
}

fn find_program(candidates: &[&str]) -> (Option<String>, Option<which::Error>) {
    let mut last_error = None;
    for candidate in candidates {
        match which::which(candidate) {
            Ok(path) => return (Some(path.display().to_string()), None),
            Err(which::Error::CannotFindBinaryPath) => {}
            Err(err) => {
                last_error = Some(err);
            }
        }
    }
    (None, last_error)
}

fn env_var(name: &str) -> Option<String> {
    match env::var(name) {
        Ok(value) => Some(value),
        Err(VarError::NotPresent) => None,
        Err(VarError::NotUnicode(_)) => {
            panic!("bundled-cmake expects a valid UTF-8 value for environment variable `{name}`")
        }
    }
}

fn parse_env_flag(name: &str) -> Option<bool> {
    match env_var(name).as_deref() {
        Some("1" | "true" | "on" | "yes") => Some(true),
        Some("0" | "false" | "off" | "no") => Some(false),
        Some(other) => {
            cargo_warning(&format!(
                "Ignoring unsupported {name} value {other:?}; expected true/false or 1/0, yes/no, on/off"
            ));
            None
        }
        None => None,
    }
}

fn env_flag(name: &str) -> bool {
    parse_env_flag(name).unwrap_or(false)
}

fn env_flag_aliases(names: &[&str]) -> bool {
    let mut values = Vec::new();
    for name in names {
        if let Some(value) = parse_env_flag(name) {
            values.push((*name, value));
        }
    }
    let Some((selected_name, selected_value)) = values.first().copied() else {
        return false;
    };
    if values.iter().any(|(_, value)| *value != selected_value) {
        let sources = values
            .iter()
            .map(|(name, value)| format!("{name}={}", if *value { 1 } else { 0 }))
            .collect::<Vec<_>>()
            .join(", ");
        cargo_warning(&format!(
            "Conflicting values for equivalent environment variables ({sources}); using {selected_name}"
        ));
    }
    selected_value
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

fn validate_extension_libraries(lib_dir: &Path, cmake_build_type: &str, enabled_extensions: &[&str]) {
    // Missing extensions are already caught by link_static_library's panic;
    // this check only guards against CMake producing extensions we did not
    // ask for (e.g. upstream extension-config drift).
    let actual = list_static_extension_libraries(lib_dir, cmake_build_type).unwrap_or_else(|err| {
        panic!(
            "failed to inspect bundled-cmake extension libraries in {}: {err}",
            lib_dir.display()
        )
    });
    let mut expected: BTreeSet<String> = enabled_extensions
        .iter()
        .map(|ext| format!("{ext}_extension"))
        .collect();
    // CMake always builds this base extension through extension_config.cmake.
    expected.insert("core_functions_extension".to_string());

    let unexpected: Vec<String> = actual.difference(&expected).cloned().collect();
    if !unexpected.is_empty() {
        panic!(
            "bundled-cmake produced unexpected static extension libraries: {}",
            unexpected.join(", ")
        );
    }
}

fn link_static_library(lib_dir: &Path, cmake_build_type: &str, name: &str) {
    let Some(library_path) = resolve_static_library(lib_dir, cmake_build_type, name) else {
        let filename = static_library_filename(name);
        let searched = candidate_library_dirs(lib_dir, cmake_build_type)
            .into_iter()
            .map(|dir| dir.join(&filename).display().to_string())
            .collect::<Vec<_>>()
            .join(", ");
        panic!("expected bundled-cmake to produce static library `{name}`; looked in [{searched}]");
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

fn list_static_extension_libraries(lib_dir: &Path, cmake_build_type: &str) -> io::Result<BTreeSet<String>> {
    let mut extension_libraries = BTreeSet::new();
    for candidate_dir in candidate_library_dirs(lib_dir, cmake_build_type) {
        for entry in fs::read_dir(&candidate_dir)? {
            let entry = entry?;
            let file_type = entry.file_type()?;
            if !file_type.is_file() {
                continue;
            }
            let file_name = entry.file_name();
            let Some(file_name) = file_name.to_str() else {
                cargo_warning(&format!(
                    "bundled-cmake ignored non-UTF-8 library filename in {}: {}",
                    candidate_dir.display(),
                    entry.path().display()
                ));
                continue;
            };
            let Some(name) = normalized_static_library_name(file_name) else {
                continue;
            };
            if name.ends_with("_extension") && !name.ends_with("_loadable_extension") {
                extension_libraries.insert(name);
            }
        }
    }
    Ok(extension_libraries)
}

fn normalized_static_library_name(filename: &str) -> Option<String> {
    if let Some(name) = filename.strip_suffix(".lib") {
        return Some(name.to_string());
    }
    let name = filename.strip_prefix("lib")?.strip_suffix(".a")?;
    Some(name.to_string())
}

fn resolve_static_library(lib_dir: &Path, cmake_build_type: &str, name: &str) -> Option<PathBuf> {
    let filename = static_library_filename(name);
    candidate_library_dirs(lib_dir, cmake_build_type)
        .into_iter()
        .map(|dir| dir.join(&filename))
        .find(|path| path.exists())
}

fn candidate_library_dirs(lib_dir: &Path, cmake_build_type: &str) -> Vec<PathBuf> {
    // Single-config generators (Ninja, Make) write directly into lib/.
    // Multi-config generators (MSVC, Xcode) write into lib/<Config>/.
    // Restricting to these two avoids picking up stale extension libraries
    // from a previous build that used a different CMAKE_BUILD_TYPE.
    let mut candidates = vec![lib_dir.to_path_buf()];
    let build_type_dir = lib_dir.join(cmake_build_type);
    if build_type_dir.is_dir() {
        candidates.push(build_type_dir);
    }
    candidates
}

fn cargo_warning(message: &str) {
    println!("cargo:warning={message}");
}

fn enabled_extensions() -> Vec<&'static str> {
    // Match DuckDB's upstream CMake defaults for quasi-core extensions rather
    // than trying to reproduce the cc backend's finer-grained selection.
    // Review build_bundled_cc::extension_enabled when changing this list; the
    // backend mechanisms and supported extension sets intentionally differ.
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

fn link_system_libs() {
    // Keep the per-OS lists below in sync with DuckDB's current linker requirements in
    // duckdb-sources/src/CMakeLists.txt. If upstream drifts after a submodule bump,
    // update every branch here alongside it.
    let target_os = env::var("CARGO_CFG_TARGET_OS").expect("Cargo should set CARGO_CFG_TARGET_OS");

    match target_os.as_str() {
        "linux" => {
            let target_env = env::var("CARGO_CFG_TARGET_ENV").unwrap_or_default();
            if target_env != "gnu" {
                panic!(
                    "bundled-cmake on linux is currently only supported with the `gnu` target env (libstdc++); got `{target_env}`"
                );
            }
            println!("cargo:rustc-link-lib=dylib=dl");
            println!("cargo:rustc-link-lib=dylib=pthread");
            println!("cargo:rustc-link-lib=dylib=stdc++");
        }
        "macos" => {
            println!("cargo:rustc-link-lib=dylib=c++");
        }
        "windows" => link_windows_system_libs(),
        other => {
            panic!(
                "bundled-cmake is currently supported only on Linux, macOS, and Windows; unsupported target OS `{other}`"
            )
        }
    }
}

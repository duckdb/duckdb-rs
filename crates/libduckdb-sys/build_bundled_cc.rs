use crate::{
    build_static_extension_autoregister, env_flag, is_compiler, link_windows_system_libs, win_target, write_bindings,
};
use std::{
    collections::{HashMap, HashSet},
    path::Path,
};

#[derive(serde::Deserialize)]
struct Sources {
    cpp_files: HashSet<String>,
    include_dirs: HashSet<String>,
}

#[derive(serde::Deserialize)]
struct Manifest {
    base: Sources,
    extensions: HashMap<String, Sources>,
}

fn add_extension(
    manifest: &Manifest,
    extension: &str,
    cpp_files: &mut HashSet<String>,
    include_dirs: &mut HashSet<String>,
) {
    let sources = manifest.extensions.get(extension).unwrap_or_else(|| {
        let mut available = manifest.extensions.keys().cloned().collect::<Vec<_>>();
        available.sort();
        panic!(
            "extension `{extension}` is missing from duckdb manifest; available extensions: {}",
            available.join(", ")
        );
    });
    cpp_files.extend(sources.cpp_files.clone());
    include_dirs.extend(sources.include_dirs.clone());
}

fn extension_enabled(extension: &str) -> bool {
    // Review build_bundled_cmake::enabled_extensions when changing this gate;
    // the backend mechanisms and supported extension sets intentionally differ.
    extension == "core_functions"
        || (extension == "parquet" && cfg!(feature = "parquet"))
        || (extension == "json" && cfg!(feature = "json"))
        || (extension == "icu" && cfg!(feature = "bundled-icu"))
}

const STATIC_EXTENSION_AUTOREGISTER: &str = "duckdb/extension/loader/static_extension_autoregister.cpp";
const JEMALLOC_NEW_DELETE: &str = "duckdb/third_party/jemalloc/src/jemalloc_cpp.cpp";

// Mirrors the JEMALLOC_ENABLED platform check in duckdb-sources/CMakeLists.txt.
fn jemalloc_enabled() -> bool {
    println!("cargo:rerun-if-env-changed=DUCKDB_DISABLE_JEMALLOC");
    let target = |name: &str| std::env::var(name).unwrap_or_default();
    !env_flag("DUCKDB_DISABLE_JEMALLOC")
        && target("CARGO_CFG_TARGET_OS") == "linux"
        && target("CARGO_CFG_TARGET_POINTER_WIDTH") == "64"
        && target("CARGO_CFG_TARGET_ENV") != "musl"
}

// jemalloc is C, so it gets its own build; its objects go into libduckdb like CMake's duckdb_static.
fn compile_jemalloc(source_root: &Path, c_files: &[String], is_debug: bool) -> Vec<std::path::PathBuf> {
    let mut cfg = cc::Build::new();
    cfg.include(source_root.join("duckdb/third_party/jemalloc/include"))
        .files(c_files.iter().map(|file| source_root.join(file)))
        .flag_if_supported("-std=gnu99")
        .flag_if_supported("-pthread")
        .warnings(false)
        .flag_if_supported("-w")
        .define(if is_debug { "DEBUG" } else { "NDEBUG" }, None);
    cfg.compile_intermediates()
}

pub fn main(out_dir: &str) {
    let source_root = crate::bundled_sources::unpack(Path::new("duckdb.tar.gz"), Path::new(out_dir))
        .expect("unpacking bundled DuckDB sources");

    let include_path = source_root.join("duckdb/src/include");
    write_bindings(&include_path, out_dir);

    let manifest_file = std::fs::File::open(source_root.join("duckdb/manifest.json")).expect("manifest file");
    let manifest: Manifest = serde_json::from_reader(manifest_file).expect("reading manifest file");

    let mut cpp_files = HashSet::new();
    let mut include_dirs = HashSet::new();
    let mut extensions = manifest
        .extensions
        .keys()
        .filter(|name| extension_enabled(name))
        .cloned()
        .collect::<Vec<String>>();
    extensions.sort();

    cpp_files.extend(manifest.base.cpp_files.clone());
    include_dirs.extend(manifest.base.include_dirs.iter().cloned());

    let mut cfg = cc::Build::new();

    for extension in &extensions {
        add_extension(&manifest, extension, &mut cpp_files, &mut include_dirs);
        cfg.define(
            &format!("DUCKDB_EXTENSION_{}_LINKED", extension.to_uppercase()),
            Some("1"),
        );
    }
    cfg.define("DUCKDB_EXTENSION_AUTOINSTALL_DEFAULT", "1");
    cfg.define("DUCKDB_EXTENSION_AUTOLOAD_DEFAULT", "1");

    println!("cargo:rerun-if-changed=duckdb.tar.gz");

    if cpp_files.remove(STATIC_EXTENSION_AUTOREGISTER) {
        let source = source_root.join(STATIC_EXTENSION_AUTOREGISTER);
        let wrapper = source_root.join(crate::bundled_sources::AUTOREGISTER_WRAPPER);
        build_static_extension_autoregister(
            source.parent().expect("autoregister source directory"),
            &wrapper,
            out_dir,
        );
    }

    let mut include_dirs: Vec<_> = include_dirs.into_iter().collect();
    include_dirs.sort();
    cfg.include(source_root.join("duckdb"));
    cfg.includes(include_dirs.iter().map(|dir| source_root.join("duckdb").join(dir)));

    let mut cpp_files_vec: Vec<String> = cpp_files.into_iter().collect();
    cpp_files_vec.sort();
    let (c_files, cpp_files_vec): (Vec<_>, Vec<_>) = cpp_files_vec.into_iter().partition(|file| file.ends_with(".c"));
    // Only needed for OVERRIDE_NEW_DELETE, which DuckDB leaves off by default.
    let cpp_files_vec = cpp_files_vec.into_iter().filter(|file| file != JEMALLOC_NEW_DELETE);
    for f in cpp_files_vec.map(|file| source_root.join(file)) {
        cfg.file(f);
    }

    // DuckDB 2.0 requires C++17 (mirrors CMAKE_CXX_STANDARD in duckdb-sources/CMakeLists.txt).
    cfg.cpp(true)
        .flag_if_supported("-std=c++17")
        .flag_if_supported("/std:c++17")
        .flag_if_supported("/utf-8")
        .flag_if_supported("/bigobj")
        .warnings(false)
        .flag_if_supported("-w");

    // Enable C++ exceptions on MSVC: without /EHsc, exception handling is off, the
    // C API's try/catch blocks are inert, and a thrown exception (e.g. invalid-UTF-8
    // CSV read) aborts with STATUS_STACK_BUFFER_OVERRUN (#774). Hard flag, not
    // flag_if_supported, so a spurious probe miss can't silently drop it. gcc/clang
    // enable exceptions by default and reject /EHsc, so gate on MSVC. Mirrors
    // build_bundled_cmake.rs.
    if win_target() && is_compiler("msvc") {
        cfg.flag("/EHsc");
    }

    let is_debug = match std::env::var("DEBUG") {
        Ok(v) => v != "false" && v != "0",
        Err(_) => false,
    };
    // Mirror DuckDB's CMake build types: Debug defines DEBUG (enables D_ASSERT
    // and the debug-only verification paths), Release defines NDEBUG. DuckDB's
    // sources assume exactly one of the two is set.
    if is_debug {
        cfg.define("DEBUG", None);
    } else {
        cfg.define("NDEBUG", None);
    }

    if win_target() {
        cfg.define("DUCKDB_BUILD_LIBRARY", None);
    }
    // The unity allocator source picks allocator_jemalloc.cpp or allocator_standard.cpp from this define.
    if jemalloc_enabled() {
        assert!(
            !c_files.is_empty(),
            "duckdb.tar.gz lacks DuckDB's jemalloc sources; regenerate it with scripts/upgrade.py -b \
             or set DUCKDB_DISABLE_JEMALLOC=1"
        );
        cfg.define("DUCKDB_ENABLE_JEMALLOC", None);
        cfg.objects(compile_jemalloc(&source_root, &c_files, is_debug));
        println!("cargo:rustc-cfg=duckdb_jemalloc");
    }
    cfg.compile("duckdb");

    // `cc` does not link DuckDB's Windows system libs automatically (e.g. unresolved
    // `RmStartSession` from the Restart Manager). See link_windows_system_libs.
    if win_target() {
        link_windows_system_libs();
    }

    println!("cargo:lib_dir={out_dir}");
}

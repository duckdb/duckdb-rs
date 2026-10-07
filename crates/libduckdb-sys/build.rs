use std::{
    env,
    path::{Path, PathBuf},
};

#[cfg(not(feature = "bundled"))]
#[path = "src/build_paths.rs"]
mod build_paths;

/// Tells whether we're building for Windows. This is more suitable than a plain
/// `cfg!(windows)`, since the latter does not properly handle cross-compilation
///
/// Note that there is no way to know at compile-time which system we'll be
/// targeting, and this test must be made at run-time (of the build script) See
/// https://doc.rust-lang.org/cargo/reference/environment-variables.html#environment-variables-cargo-sets-for-build-scripts
fn win_target() -> bool {
    std::env::var("CARGO_CFG_WINDOWS").is_ok()
}

/// Tells whether a given compiler will be used `compiler_name` is compared to
/// the content of `CARGO_CFG_TARGET_ENV` (and is always lowercase)
///
/// See [`win_target`]
fn is_compiler(compiler_name: &str) -> bool {
    std::env::var("CARGO_CFG_TARGET_ENV").is_ok_and(|v| v == compiler_name)
}

fn main() {
    let out_dir = env::var("OUT_DIR").unwrap();
    #[cfg(feature = "bundled")]
    build_bundled_backend::main(&out_dir);
    #[cfg(not(feature = "bundled"))]
    build_linked::main(&out_dir)
}

/// Which DuckDB C API header a set of bindings is generated from.
///
/// `V1` is `duckdb.h` (or `duckdb_extension.h` for loadable extensions) and is
/// always emitted as `bindgen.rs`. `V2` is `duckdb_v2.h`, emitted as
/// `bindgen_v2.rs` only when the `capi-v2` feature is enabled.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum CApi {
    V1,
    V2,
}

/// Emit every bindings artifact this build needs into `out_dir`, either by
/// running bindgen (`buildtime_bindgen`) or by copying the pregenerated files.
pub(crate) fn write_all_bindings(header: &HeaderLocation, out_dir: &Path) {
    #[cfg(feature = "capi-v1")]
    write_api_bindings(header, CApi::V1, &out_dir.join("bindgen.rs"));
    #[cfg(feature = "capi-v2")]
    write_api_bindings(header, CApi::V2, &out_dir.join("bindgen_v2.rs"));
    #[cfg(not(any(feature = "capi-v1", feature = "capi-v2")))]
    let _ = (header, out_dir);
}

fn write_api_bindings(header: &HeaderLocation, api: CApi, out_path: &Path) {
    #[cfg(feature = "buildtime_bindgen")]
    buildtime_bindgen::write_to_out_dir(header, api, out_path);

    #[cfg(not(feature = "buildtime_bindgen"))]
    {
        let _ = header;
        copy_pregenerated_bindings(api, out_path);
    }
}

/// Bundled backends know the include directory of the unpacked source tree.
#[cfg(feature = "bundled")]
pub(crate) fn write_bindings(header_dir: &Path, out_dir: &str) {
    let header = HeaderLocation::IncludeDir(header_dir.to_path_buf());
    write_all_bindings(&header, Path::new(out_dir));
}

#[cfg(all(feature = "bundled", not(feature = "bundled-cmake")))]
mod build_bundled_cc;
#[cfg(all(feature = "bundled", feature = "bundled-cmake"))]
mod build_bundled_cmake;
#[cfg(not(feature = "bundled"))]
mod build_linked;
/// Generates the bindings with bindgen from the DuckDB headers.
#[cfg(feature = "buildtime_bindgen")]
mod buildtime_bindgen;

#[cfg(all(feature = "bundled", not(feature = "bundled-cmake")))]
use crate::build_bundled_cc as build_bundled_backend;
#[cfg(all(feature = "bundled", feature = "bundled-cmake"))]
use crate::build_bundled_cmake as build_bundled_backend;

/// Link the Windows system libraries DuckDB needs but that neither `cc` nor a
/// static CMake link adds automatically. Mirrors DuckDB's `DUCKDB_SYSTEM_LIBS`
/// (duckdb-sources/src/CMakeLists.txt) — keep in sync on submodule bumps.
/// Callers apply their own `win_target()` gate.
#[cfg(feature = "bundled")]
pub(crate) fn link_windows_system_libs() {
    println!("cargo:rustc-link-lib=dylib=ws2_32");
    println!("cargo:rustc-link-lib=dylib=rstrtmgr");
    if is_compiler("msvc") {
        println!("cargo:rustc-link-lib=dylib=bcrypt");
    } else {
        println!("cargo:rustc-link-lib=dylib=stdc++");
    }
}

#[cfg(not(feature = "buildtime_bindgen"))]
fn copy_pregenerated_bindings(api: CApi, out_path: &Path) {
    let bindings_path = match api {
        CApi::V1 if is_loadable_extension() => "src/bindgen_bundled_version_loadable.rs",
        CApi::V1 => "src/bindgen_bundled_version.rs",
        CApi::V2 => "src/bindgen_bundled_version_v2.rs",
    };
    println!("cargo:rerun-if-changed={bindings_path}");
    std::fs::copy(bindings_path, out_path).expect("Could not copy bindings to output directory");
}

pub enum HeaderLocation {
    IncludeDir(PathBuf),
    HeaderPath(PathBuf),
    Wrapper,
}

#[allow(dead_code)]
fn is_loadable_extension() -> bool {
    cfg!(feature = "loadable-extension")
}

#[allow(dead_code)]
fn header_filename(api: CApi) -> &'static str {
    match api {
        CApi::V1 if is_loadable_extension() => "duckdb_extension.h",
        CApi::V1 => "duckdb.h",
        CApi::V2 => "duckdb_v2.h",
    }
}

#[allow(dead_code)]
impl HeaderLocation {
    fn from_env(lib_dir: &Path) -> Self {
        if let Ok(include_dir) = env::var("DUCKDB_INCLUDE_DIR") {
            return HeaderLocation::IncludeDir(PathBuf::from(include_dir));
        }
        let header_path = lib_dir.join(header_filename(CApi::V1));
        if header_path.exists() {
            HeaderLocation::IncludeDir(lib_dir.to_path_buf())
        } else {
            HeaderLocation::HeaderPath(header_path)
        }
    }

    #[cfg(feature = "buildtime_bindgen")]
    fn header_path(&self, api: CApi) -> PathBuf {
        match self {
            HeaderLocation::IncludeDir(path) => path.join(header_filename(api)),
            HeaderLocation::HeaderPath(path) => path.with_file_name(header_filename(api)),
            HeaderLocation::Wrapper => PathBuf::from(match api {
                CApi::V1 if is_loadable_extension() => "wrapper_ext.h",
                CApi::V1 => "wrapper.h",
                CApi::V2 => "wrapper_v2.h",
            }),
        }
    }

    fn include_dir(&self) -> Option<&Path> {
        match self {
            HeaderLocation::IncludeDir(path) => Some(path),
            HeaderLocation::HeaderPath(_) | HeaderLocation::Wrapper => None,
        }
    }
}

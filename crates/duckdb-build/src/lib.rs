//! Installation discovery and runtime linking without a native DuckDB dependency.
//!
//! Artifacts default to `~/.duckdb/lib/<ARTIFACT_VERSION>`. Set both
//! `DUCKDB_LIB_DIR` and `DUCKDB_INCLUDE_DIR` to override the entire installation.
//! The opt-in `pkg-config` feature selects system discovery instead of the default.
//! Explicit overrides and system discovery never silently fall back.
//!
//! [`resolve_library`] resolves an installation without emitting linker directives.
//! Applications can use [`emit_dynamic_linking_flags`] in their own build script to embed
//! the installed library's runtime path without loader environment variables.
//! Explicit paths relative to the executable are also supported for packaging.
//! Static builds and loader-visible installations do not need this helper.
//! This crate does not download artifacts or load `.env` files.

use std::{
    env,
    path::{Path, PathBuf},
};

pub mod consts;
pub mod target;
pub use consts::{ARTIFACT_SHA, ARTIFACT_VERSION};

use crate::target::Target;

/// Installation command included in missing-artifact diagnostics.
pub const INSTALL_COMMAND: &str = "curl https://install.duckdb.org | sh";

/// Native library linkage requested by `libduckdb-sys`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LinkMode {
    /// Link the shared DuckDB library.
    Dynamic,
    /// Link an already installed, self-contained DuckDB static archive.
    Static,
}

#[cfg(not(any(feature = "pkg-config", feature = "vcpkg")))]
fn try_from_user_install() -> Result<(Vec<PathBuf>, Vec<PathBuf>), Box<dyn std::error::Error>> {
    let path = dirs::home_dir()
        .ok_or("Failed to get home directory")?
        .join(format!(".duckdb/lib/{}", ARTIFACT_VERSION));

    Ok((vec![path.clone()], vec![path]))
}

#[cfg(feature = "pkg-config")]
fn try_from_pkg_config(
    target: &Target,
    mode: LinkMode,
) -> Result<(Vec<PathBuf>, Vec<PathBuf>, Vec<String>), Box<dyn std::error::Error>> {
    let library = ::pkg_config::Config::new()
        .statik(mode == LinkMode::Static)
        .cargo_metadata(false)
        .probe("duckdb")?;

    let mut names = Vec::new();
    for name in library
        .libs
        .iter()
        .filter(|name| ["duckdb", "duckdb_static"].contains(&name.as_str()))
    {
        names.extend(target.library_names(mode, name)?);
    }
    if names.is_empty() {
        names.extend(target.library_names(mode, "duckdb")?);
        if mode == LinkMode::Static {
            names.extend(target.library_names(mode, "duckdb_static")?);
        }
    }

    Ok((library.link_paths, library.include_paths, names))
}

#[cfg(not(any(feature = "pkg-config", feature = "vcpkg")))]
fn try_from_system_install() -> Result<(Vec<PathBuf>, Vec<PathBuf>), Box<dyn std::error::Error>> {
    let lib_path = PathBuf::from("/usr/local/lib");
    let include_path = PathBuf::from("/usr/local/include");

    Ok((vec![lib_path], vec![include_path]))
}

#[cfg(all(not(feature = "pkg-config"), feature = "vcpkg"))]
fn try_from_vcpkg() -> Result<(Vec<PathBuf>, Vec<PathBuf>), Box<dyn std::error::Error>> {
    let library = vcpkg::Config::new().cargo_metadata(false).find_package("duckdb")?;

    Ok((library.link_paths, library.include_paths))
}

fn try_from_env() -> Result<Option<(Vec<PathBuf>, Vec<PathBuf>)>, Box<dyn std::error::Error>> {
    match (env::var_os("DUCKDB_LIB_DIR"), env::var_os("DUCKDB_INCLUDE_DIR")) {
        (None, None) => Ok(None),
        (Some(lib_path), Some(include_path)) => {
            Ok(Some((vec![PathBuf::from(lib_path)], vec![PathBuf::from(include_path)])))
        }
        _ => Err("Set both DUCKDB_LIB_DIR and DUCKDB_INCLUDE_DIR".into()),
    }
}

fn canonicalize_paths(paths: Vec<PathBuf>) -> Vec<PathBuf> {
    paths.into_iter().filter_map(|path| path.canonicalize().ok()).collect()
}

fn find_file(paths: &[PathBuf], file_names: &[String]) -> Option<PathBuf> {
    for path in paths {
        for file_name in file_names {
            let candidate = path.join(file_name);
            if candidate.is_file() {
                return Some(candidate);
            }
        }
    }
    None
}

fn resolve_paths(
    lib_paths: Vec<PathBuf>,
    include_paths: Vec<PathBuf>,
    target_lib_names: &[String],
) -> Result<Option<(PathBuf, PathBuf)>, Box<dyn std::error::Error>> {
    let lib_paths = canonicalize_paths(lib_paths);
    let include_paths = canonicalize_paths(include_paths);
    let Some(target_lib) = find_file(&lib_paths, target_lib_names) else {
        return Ok(None);
    };
    let Some(target_header) = find_file(&include_paths, &["duckdb_v2.h".to_string()]) else {
        return Ok(None);
    };
    Ok(Some((target_lib, target_header)))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Provider {
    Directory,
    #[cfg(feature = "pkg-config")]
    PkgConfig,
    #[cfg(all(not(feature = "pkg-config"), feature = "vcpkg"))]
    Vcpkg,
}

fn get_paths(
    _target: &Target,
    target_lib_names: &[String],
    _mode: LinkMode,
) -> Result<(PathBuf, PathBuf, Provider), Box<dyn std::error::Error>> {
    if let Some((lib_paths, include_paths)) = try_from_env()? {
        let (library, header) = resolve_paths(lib_paths, include_paths, target_lib_names)?
            .ok_or("Env: The configured DuckDB library or duckdb_v2.h could not be found")?;
        return Ok((library, header, Provider::Directory));
    }

    #[cfg(feature = "pkg-config")]
    {
        let (lib_paths, include_paths, pkg_config_names) = try_from_pkg_config(_target, _mode)?;
        let (library, header) = resolve_paths(lib_paths, include_paths, &pkg_config_names)?
            .ok_or("pkg-config did not provide the requested DuckDB library and duckdb_v2.h")?;
        Ok((library, header, Provider::PkgConfig))
    }

    #[cfg(all(not(feature = "pkg-config"), feature = "vcpkg"))]
    {
        let (lib_paths, include_paths) = try_from_vcpkg()?;
        let (library, header) = resolve_paths(lib_paths, include_paths, target_lib_names)?
            .ok_or("vcpkg did not provide the requested DuckDB library and duckdb_v2.h")?;
        Ok((library, header, Provider::Vcpkg))
    }

    #[cfg(not(any(feature = "pkg-config", feature = "vcpkg")))]
    {
        for provider in [try_from_user_install, try_from_system_install] {
            if let Ok((lib_paths, include_paths)) = provider() {
                if let Ok(Some((library, header))) = resolve_paths(lib_paths, include_paths, target_lib_names) {
                    return Ok((library, header, Provider::Directory));
                }
            }
        }

        Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "DUCKDB_LIB_DIR or DUCKDB_INCLUDE_DIR not set",
        )))
    }
}

pub fn emit_rerun_calls(lib_path: &PathBuf, header_path: &PathBuf) {
    for variable in [
        "DUCKDB_LIB_DIR",
        "DUCKDB_INCLUDE_DIR",
        "HOME",
        "USERPROFILE",
        "LINK_DUCKDB_STATIC",
        "PKG_CONFIG_PATH",
        "VCPKG_ROOT",
    ] {
        println!("cargo:rerun-if-env-changed={variable}");
    }
    println!("cargo:rerun-if-changed={}", lib_path.display());
    println!("cargo:rerun-if-changed={}", header_path.display());
}

pub fn emit_runtime_path(lib_path: &PathBuf) {
    match env::var("CARGO_CFG_TARGET_OS").as_deref() {
        Ok("macos") | Ok("ios") => {
            println!("cargo:rustc-link-arg=-Wl,-rpath,{}", lib_path.display());
            println!("cargo:rustc-link-arg=-Wl,-rpath,@loader_path");
            println!("cargo:rustc-link-arg=-Wl,-rpath,@loader_path/../lib");
        }
        Ok("linux") | Ok("android") => {
            println!("cargo:rustc-link-arg=-Wl,-rpath,{}", lib_path.display());
            println!("cargo:rustc-link-arg=-Wl,-rpath,$ORIGIN");
            println!("cargo:rustc-link-arg=-Wl,-rpath,$ORIGIN/../lib");
        }
        _ => {}
    }
}

/// A validated DuckDB installation and its selected discovery provider.
pub struct ResolvedLibrary {
    pub library: PathBuf,
    pub header: PathBuf,
    pub mode: LinkMode,
    provider: Provider,
}

impl ResolvedLibrary {
    pub fn lib_dir(&self) -> &Path {
        self.library.parent().expect("resolved library has a parent")
    }

    pub fn include_dir(&self) -> &Path {
        self.header.parent().expect("resolved header has a parent")
    }

    pub fn emit_rerun_calls(&self) {
        emit_rerun_calls(&self.library, &self.header);
    }

    pub fn emit_linker_flags(&self) -> Result<(), Box<dyn std::error::Error>> {
        println!("cargo:lib_dir={}", self.lib_dir().display());
        println!("cargo:include_dir={}", self.include_dir().display());

        match self.provider {
            Provider::Directory => {
                println!("cargo:rustc-link-search=native={}", self.lib_dir().display());
                match self.mode {
                    LinkMode::Static => {
                        println!("cargo:rustc-link-lib=static=duckdb_static");
                        if let Some(cxx) = Target::from_env()?.cxx_library() {
                            println!("cargo:rustc-link-lib=dylib={cxx}");
                        }
                    }
                    LinkMode::Dynamic => println!("cargo:rustc-link-lib=dylib=duckdb"),
                }
            }
            #[cfg(feature = "pkg-config")]
            Provider::PkgConfig => {
                ::pkg_config::Config::new()
                    .statik(self.mode == LinkMode::Static)
                    .cargo_metadata(true)
                    .probe("duckdb")?;
            }
            #[cfg(all(not(feature = "pkg-config"), feature = "vcpkg"))]
            Provider::Vcpkg => {
                ::vcpkg::Config::new().cargo_metadata(true).find_package("duckdb")?;
            }
        }

        match self.mode {
            LinkMode::Static => println!("cargo:static=1"),
            LinkMode::Dynamic => emit_runtime_path(&self.lib_dir().to_path_buf()),
        }
        Ok(())
    }
}

pub fn emit_dynamic_linking_flags() -> Result<(), Box<dyn std::error::Error>> {
    let library = resolve_library()?;
    emit_runtime_path(&library.lib_dir().to_path_buf());
    Ok(())
}

pub fn resolve_library() -> Result<ResolvedLibrary, Box<dyn std::error::Error>> {
    let link_mode = if cfg!(feature = "static") {
        LinkMode::Static
    } else {
        LinkMode::Dynamic
    };
    let target = Target::from_env()?;

    let library_name = match link_mode {
        LinkMode::Static => "duckdb_static",
        LinkMode::Dynamic => "duckdb",
    };

    let target_lib = target.library_names(link_mode, library_name)?;

    let (library, header, provider) = get_paths(&target, &target_lib, link_mode)?;
    Ok(ResolvedLibrary {
        library,
        header,
        mode: link_mode,
        provider,
    })
}

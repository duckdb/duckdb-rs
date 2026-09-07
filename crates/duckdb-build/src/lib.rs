//! Installation discovery and runtime linking without a native DuckDB dependency.
//!
//! Libraries and `duckdb_v2.h` default to `~/.duckdb/lib/<ARTIFACT_VERSION>`.
//! `DUCKDB_LIB_DIR` and `DUCKDB_INCLUDE_DIR` override those directories independently.
//! This crate does not download artifacts or load `.env` files.
//!
//! Call [`emit_runtime_path`] from the final application's build script: Cargo does
//! not propagate a dependency's `rustc-link-arg` directives into that application.

use std::{
    env, fs, io,
    path::{Path, PathBuf},
};

mod consts;
pub use consts::{ARTIFACT_SHA, ARTIFACT_VERSION};

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

/// Validated directories for a DuckDB library and its header.
#[derive(Debug)]
pub struct Installation {
    /// Absolute directory containing the native library.
    pub lib_dir: PathBuf,
    /// Absolute directory containing `duckdb_v2.h`.
    pub include_dir: PathBuf,
}

impl Installation {
    /// Discover and validate artifacts for Cargo's target platform.
    ///
    /// No system-library fallback is attempted. Explicit overrides must contain
    /// compatible artifacts; a directory name alone cannot verify their ABI or SHA.
    pub fn discover(mode: LinkMode) -> io::Result<Self> {
        let directories = Directories::from_env()?;
        let target = Target::from_env()?;
        let lib_dir = directories.library()?;
        validate_library(&lib_dir, &target, mode)?;
        let include_dir = directories.include()?;
        let header = require_file(&include_dir, &["duckdb_v2.h"])?;
        println!("cargo:rerun-if-changed={}", cargo_path(&header)?);
        Ok(Self { lib_dir, include_dir })
    }
}

/// Where the final application should look for DuckDB at runtime.
#[derive(Clone, Copy, Debug)]
pub enum RuntimeLocation<'a> {
    /// Use `DUCKDB_LIB_DIR`, or the pinned installation when it is unset.
    Installed,
    /// Use an already resolved directory, for example `DEP_DUCKDB_LIB_DIR`.
    Directory(&'a Path),
    /// Use a path relative to the executable, such as `"."` or `"../lib"`.
    ///
    /// This neither looks up the installation nor copies any libraries.
    RelativeToExecutable(&'a str),
}

/// Emit runtime linker arguments from the final application's build script.
///
/// Installed paths are absolute; packaged paths use `$ORIGIN` on Linux/Android
/// and `@loader_path` on macOS/iOS. For a shared-library target, these tokens are
/// relative to that shared library instead of the executable.
///
/// Windows has no rpath: this function emits nothing there. Put `duckdb.dll`
/// beside the executable or on `PATH`. macOS libraries must have an appropriate
/// `@rpath` install name. Static builds do not need this helper.
pub fn emit_runtime_path(location: RuntimeLocation<'_>) -> io::Result<()> {
    let target = Target::from_env()?;
    let Some(origin) = runtime_origin(&target.os)? else {
        return Ok(());
    };

    let rpath = match location {
        RuntimeLocation::Installed => {
            let lib_dir = Directories::from_env()?.library()?;
            validate_library(&lib_dir, &target, LinkMode::Dynamic)?;
            cargo_path(&lib_dir)?.to_owned()
        }
        RuntimeLocation::Directory(directory) => {
            let directory = directory.canonicalize()?;
            validate_library(&directory, &target, LinkMode::Dynamic)?;
            cargo_path(&directory)?.to_owned()
        }
        RuntimeLocation::RelativeToExecutable(relative) => relative_rpath(origin, relative)?,
    };

    // Separate linker arguments also support directories containing commas.
    println!("cargo:rustc-link-arg=-Xlinker");
    println!("cargo:rustc-link-arg=-rpath");
    println!("cargo:rustc-link-arg=-Xlinker");
    println!("cargo:rustc-link-arg={rpath}");
    Ok(())
}

struct Directories {
    home: Option<PathBuf>,
    lib: Option<PathBuf>,
    include: Option<PathBuf>,
    invocation_dir: PathBuf,
}

impl Directories {
    fn from_env() -> io::Result<Self> {
        for variable in ["DUCKDB_LIB_DIR", "DUCKDB_INCLUDE_DIR", "HOME", "USERPROFILE", "PWD"] {
            println!("cargo:rerun-if-env-changed={variable}");
        }
        let invocation_dir = match env::var_os("PWD") {
            Some(pwd) if Path::new(&pwd).is_absolute() => PathBuf::from(pwd),
            _ => env::current_dir()?,
        };
        Ok(Self {
            home: dirs::home_dir(),
            lib: env::var_os("DUCKDB_LIB_DIR").map(PathBuf::from),
            include: env::var_os("DUCKDB_INCLUDE_DIR").map(PathBuf::from),
            invocation_dir,
        })
    }

    fn library(&self) -> io::Result<PathBuf> {
        self.resolve(self.lib.as_deref(), "DUCKDB_LIB_DIR")
    }

    fn include(&self) -> io::Result<PathBuf> {
        self.resolve(self.include.as_deref(), "DUCKDB_INCLUDE_DIR")
    }

    fn resolve(&self, override_dir: Option<&Path>, variable: &str) -> io::Result<PathBuf> {
        let directory = match override_dir {
            Some(path) if path.as_os_str().is_empty() => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("{variable} must not be empty"),
                ));
            }
            Some(path) => match path.strip_prefix("~") {
                Ok(relative) => self.home(variable)?.join(relative),
                Err(_) => self.invocation_dir.join(path),
            },
            None => self.home(variable)?.join(".duckdb/lib").join(ARTIFACT_VERSION),
        };
        cargo_path(&directory)?;
        directory.canonicalize().map_err(|error| {
            io::Error::new(
                error.kind(),
                format!(
                    "Cannot use DuckDB directory {}: {error}. {}",
                    directory.display(),
                    installation_hint()
                ),
            )
        })
    }

    fn home(&self, variable: &str) -> io::Result<&Path> {
        self.home.as_deref().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!(
                    "Cannot locate the home directory; set {variable} to an absolute path. \
                     Install DuckDB with: {INSTALL_COMMAND}"
                ),
            )
        })
    }
}

struct Target {
    os: String,
    environment: String,
}

impl Target {
    fn from_env() -> io::Result<Self> {
        let read = |name| {
            env::var(name).map_err(|error| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("duckdb-build must run from a Cargo build script ({name}): {error}"),
                )
            })
        };
        Ok(Self {
            os: read("CARGO_CFG_TARGET_OS")?,
            environment: read("CARGO_CFG_TARGET_ENV")?,
        })
    }
}

fn validate_library(directory: &Path, target: &Target, mode: LinkMode) -> io::Result<()> {
    let names: &[&str] = match (mode, target.os.as_str(), target.environment.as_str()) {
        (LinkMode::Static, "windows", "msvc") => &["duckdb_static.lib", "libduckdb_static.lib"],
        (LinkMode::Static, "linux" | "android" | "macos" | "ios", _)
        | (LinkMode::Static, "windows", "gnu" | "gnullvm") => &["libduckdb_static.a"],
        (LinkMode::Dynamic, "macos" | "ios", _) => &["libduckdb.dylib"],
        (LinkMode::Dynamic, "linux" | "android", _) => &["libduckdb.so"],
        (LinkMode::Dynamic, "windows", "msvc") => &["duckdb.lib", "libduckdb.lib"],
        (LinkMode::Dynamic, "windows", "gnu" | "gnullvm") => &["libduckdb.dll.a", "libduckdb.a"],
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                format!("Unsupported DuckDB target: {}-{}", target.os, target.environment),
            ));
        }
    };
    let library = require_file(directory, names)?;
    println!("cargo:rerun-if-changed={}", cargo_path(&library)?);
    if mode == LinkMode::Dynamic && target.os == "windows" {
        let dll = require_file(directory, &["duckdb.dll"])?;
        println!("cargo:rerun-if-changed={}", cargo_path(&dll)?);
    }
    Ok(())
}

fn require_file(directory: &Path, names: &[&str]) -> io::Result<PathBuf> {
    for name in names {
        let file = directory.join(name);
        match fs::metadata(&file) {
            Ok(metadata) if metadata.is_file() => return Ok(file),
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(io::Error::new(
                    error.kind(),
                    format!("Cannot inspect {}: {error}", file.display()),
                ));
            }
        }
    }
    Err(io::Error::new(
        io::ErrorKind::NotFound,
        format!(
            "DuckDB artifact {} not found in {}. {}",
            names.join(" or "),
            directory.display(),
            installation_hint()
        ),
    ))
}

fn installation_hint() -> String {
    format!(
        "Expected DuckDB {ARTIFACT_VERSION} (commit {ARTIFACT_SHA}). \
         Install DuckDB with: {INSTALL_COMMAND}\n\
         Or set DUCKDB_LIB_DIR and DUCKDB_INCLUDE_DIR to compatible library and header directories."
    )
}

fn cargo_path(path: &Path) -> io::Result<&str> {
    let path = path
        .to_str()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "DuckDB paths must be valid UTF-8"))?;
    if path.contains(['\n', '\r']) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "DuckDB paths must not contain newlines",
        ));
    }
    Ok(path)
}

fn runtime_origin(target_os: &str) -> io::Result<Option<&'static str>> {
    match target_os {
        "macos" | "ios" => Ok(Some("@loader_path")),
        "linux" | "android" => Ok(Some("$ORIGIN")),
        "windows" => Ok(None),
        _ => Err(io::Error::new(
            io::ErrorKind::Unsupported,
            format!("Runtime paths are unsupported for {target_os}"),
        )),
    }
}

fn relative_rpath(origin: &str, relative: &str) -> io::Result<String> {
    // Interpret paths for the target, even when cross-compiling from another OS.
    if relative.is_empty() || relative.starts_with('/') || relative.contains(['\\', ':', '\n', '\r']) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Expected a nonempty, slash-separated directory relative to the executable",
        ));
    }
    Ok(format!("{origin}/{relative}"))
}

#[cfg(test)]
mod tests;

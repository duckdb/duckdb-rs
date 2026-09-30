#[cfg(feature = "vcpkg")]
extern crate vcpkg;

use crate::{HeaderLocation, is_compiler, is_loadable_extension, win_target};
use std::{env, fs, path::Path};

pub fn main(out_dir: &str) {
    // We need this to config the LD_LIBRARY_PATH
    let header = find_duckdb(out_dir);

    // Publish the resolved include directory so downstream crates
    // that compile their own C/C++ code can read it from
    // `DEP_DUCKDB_INCLUDE`. Wrapper-only mode (no explicit dir
    // from env / vcpkg / pkg-config / download) intentionally
    // skips the emission — there's no single directory to point
    // at; the system header is on the default search path already.
    if let Some(include_dir) = header.include_dir() {
        println!("cargo:include={}", include_dir.display());
    }

    crate::write_all_bindings(&header, Path::new(out_dir));
}

fn link_directive() -> &'static str {
    // If the user specifies DUCKDB_STATIC, do static
    // linking, unless it's explicitly set to 0.
    match env::var("DUCKDB_STATIC") {
        Ok(v) if v != "0" => "static=duckdb_static",
        _ => "dylib=duckdb",
    }
}

fn emit_link_lib(link_directive: &str) {
    if !is_loadable_extension() {
        println!("cargo:rustc-link-lib={link_directive}");
    }
}

#[cfg(feature = "pkg-config")]
fn duckdb_pkg_config() -> pkg_config::Config {
    let mut config = pkg_config::Config::new();
    if is_loadable_extension() {
        config.cargo_metadata(false);
    }
    config
}

// Prints the necessary cargo link commands and returns the path to the header.
fn find_duckdb(out_dir: &str) -> HeaderLocation {
    println!("cargo:rerun-if-env-changed=DUCKDB_DOWNLOAD_LIB");
    if !is_loadable_extension() {
        println!("cargo:rerun-if-env-changed=DUCKDB_INCLUDE_DIR");
        println!("cargo:rerun-if-env-changed=DUCKDB_LIB_DIR");
        println!("cargo:rerun-if-env-changed=DUCKDB_STATIC");
        if cfg!(feature = "vcpkg") && is_compiler("msvc") {
            println!("cargo:rerun-if-env-changed=VCPKGRS_DYNAMIC");
        }

        // dependents can access `DEP_DUCKDB_LINK_TARGET` (`duckdb` being the
        // `links=` value in our Cargo.toml) to get this value. This might be
        // useful if you need to ensure whatever crypto library sqlcipher relies
        // on is available, for example.
        println!("cargo:link-target=duckdb");
    }

    if win_target() && cfg!(feature = "winduckdb") {
        emit_link_lib("dylib=duckdb");
        return HeaderLocation::Wrapper;
    }
    // Allow users to specify where to find DuckDB.
    if let Ok(dir) = env::var("DUCKDB_LIB_DIR") {
        println!("cargo:rustc-env=LD_LIBRARY_PATH={dir}");
        // Try to use pkg-config to determine link commands
        let pkgconfig_path = Path::new(&dir).join("pkgconfig");
        unsafe { env::set_var("PKG_CONFIG_PATH", pkgconfig_path) };

        #[cfg(feature = "pkg-config")]
        let lib_found = duckdb_pkg_config().probe("duckdb").is_ok();
        #[cfg(not(feature = "pkg-config"))]
        let lib_found = false;

        if !lib_found {
            // Otherwise just emit the bare minimum link commands.
            emit_link_lib(link_directive());
            println!("cargo:rustc-link-search={dir}");
        }

        stage_runtime_lib(Path::new(&dir), out_dir);

        return HeaderLocation::from_env(Path::new(&dir));
    }

    if let Some(header) = try_download(out_dir) {
        return header;
    }

    if let Some(header) = try_vcpkg() {
        return header;
    }

    // See if pkg-config can do everything for us.
    #[cfg(feature = "pkg-config")]
    {
        match duckdb_pkg_config().print_system_libs(false).probe("duckdb") {
            Ok(mut lib) => {
                if let Some(header) = lib.include_paths.pop() {
                    HeaderLocation::IncludeDir(header)
                } else {
                    HeaderLocation::Wrapper
                }
            }
            Err(_) => {
                // No env var set and pkg-config couldn't help; just output the link-lib
                // request and hope that the library exists on the system paths. We used to
                // output /usr/lib explicitly, but that can introduce other linking problems;
                // see https://github.com/rusqlite/rusqlite/issues/207.
                emit_link_lib(link_directive());
                HeaderLocation::Wrapper
            }
        }
    }
    #[cfg(not(feature = "pkg-config"))]
    {
        // No pkg-config available; just output the link-lib request and hope
        // that the library exists on the system paths.
        emit_link_lib(link_directive());
        HeaderLocation::Wrapper
    }
}

fn try_vcpkg() -> Option<HeaderLocation> {
    #[cfg(feature = "vcpkg")]
    // See if vcpkg can find it.
    if is_compiler("msvc")
        && let Ok(mut lib) = vcpkg::Config::new().probe("duckdb")
        && let Some(header) = lib.include_paths.pop()
    {
        return Some(HeaderLocation::IncludeDir(header));
    }
    None
}

#[cfg(feature = "download-lib")]
fn try_download(out_dir: &str) -> Option<HeaderLocation> {
    download_lib::wants()
        .then(|| download_lib::run(out_dir).unwrap_or_else(|err| panic!("Failed to set up libduckdb: {err}")))
}

#[cfg(not(feature = "download-lib"))]
fn try_download(_out_dir: &str) -> Option<HeaderLocation> {
    if env::var("DUCKDB_DOWNLOAD_LIB").is_ok_and(|value| matches!(value.to_ascii_lowercase().as_str(), "1" | "true")) {
        panic!(
            "DUCKDB_DOWNLOAD_LIB is set, but libduckdb-sys was built without the `download-lib` feature. \
             Enable it (`--features download-lib`) to download a pre-built libduckdb, \
             or set DUCKDB_LIB_DIR / use the `bundled` feature instead."
        );
    }
    None
}

/// Download client for prebuilt libduckdb release artifacts.
#[cfg(feature = "download-lib")]
mod download_lib {
    use std::io;
    use std::{env, fs, path::Path};

    use super::{
        HeaderLocation, LibduckdbArchive, copy_libduckdb, emit_link_lib, is_loadable_extension, link_directive,
    };

    /// Pin file, next to Cargo.toml, naming the prebuilt DuckDB libraries that
    /// linked-mode builds download. Shipped with the crate.
    const RELEASE_PIN_FILE: &str = ".duckdb-release";

    /// Where to download prebuilt libraries from: a directory URL that holds the
    /// `duckdb-shared-libs-<platform>.tar.gz` archives, plus a version label used
    /// to name the download cache directory.
    struct DownloadSource {
        base_url: String,
        version: String,
    }

    impl DownloadSource {
        /// The pin from `.duckdb-release`, if the file exists and is complete.
        fn pinned() -> Option<Self> {
            println!("cargo:rerun-if-changed={RELEASE_PIN_FILE}");
            let contents = fs::read_to_string(RELEASE_PIN_FILE).ok()?;
            let mut base_url = None;
            let mut version = None;
            for line in contents.lines().map(str::trim) {
                if line.is_empty() || line.starts_with('#') {
                    continue;
                }
                let Some((key, value)) = line.split_once('=') else {
                    panic!("{RELEASE_PIN_FILE}: expected KEY=VALUE, got {line:?}");
                };
                match key.trim() {
                    "DUCKDB_RELEASE_URL" => base_url = Some(value.trim().trim_end_matches('/').to_owned()),
                    "DUCKDB_RELEASE_VERSION" => version = Some(value.trim().to_owned()),
                    // Consumed by upgrade.sh, not by the build.
                    "DUCKDB_RELEASE_COMMIT" => {}
                    other => panic!("{RELEASE_PIN_FILE}: unknown key {other:?}"),
                }
            }
            match (base_url, version) {
                (Some(base_url), Some(version)) => Some(Self { base_url, version }),
                (None, None) => None,
                _ => panic!("{RELEASE_PIN_FILE}: both DUCKDB_RELEASE_URL and DUCKDB_RELEASE_VERSION are required"),
            }
        }

        /// The GitHub release matching the DuckDB version encoded in the crate
        /// version, used when there is no pin file.
        fn from_crate_version() -> Self {
            let version = duckdb_version_from_pkg_version(env!("CARGO_PKG_VERSION"));
            Self {
                base_url: format!("https://github.com/duckdb/duckdb/releases/download/v{version}"),
                version: format!("v{version}"),
            }
        }

        fn archive_url(&self, archive: &LibduckdbArchive) -> String {
            format!("{}/{}", self.base_url, archive.archive_name)
        }

        /// Directory-safe form of the version label.
        fn cache_key(&self) -> String {
            self.version
                .chars()
                .map(|c| {
                    if c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_') {
                        c
                    } else {
                        '_'
                    }
                })
                .collect()
        }
    }

    /// Whether to download prebuilt libraries instead of probing the system.
    ///
    /// `DUCKDB_DOWNLOAD_LIB=1` forces a download and `DUCKDB_DOWNLOAD_LIB=0`
    /// forbids one. When unset, a download happens whenever the crate ships a
    /// `.duckdb-release` pin, so that a plain `cargo build` links against the
    /// exact DuckDB the pregenerated bindings were generated from.
    pub(super) fn wants() -> bool {
        match env::var("DUCKDB_DOWNLOAD_LIB") {
            Ok(value) => matches!(value.to_ascii_lowercase().as_str(), "1" | "true"),
            // Loadable extensions never link against a library, so there is
            // nothing to download by default.
            Err(_) => !is_loadable_extension() && DownloadSource::pinned().is_some(),
        }
    }

    pub(super) fn run(out_dir: &str) -> Result<HeaderLocation, Box<dyn std::error::Error>> {
        let target = env::var("TARGET")?;
        let archive = LibduckdbArchive::for_target(&target)
            .ok_or_else(|| format!("No pre-built libduckdb available for target '{target}'"))?;

        let source = DownloadSource::pinned().unwrap_or_else(DownloadSource::from_crate_version);

        // Cache downloads beside the profile directory so successive builds reuse them.
        let download_dir = crate::build_paths::download_root(Path::new(out_dir))
            .ok_or_else(|| {
                format!(
                    "Could not determine DuckDB download directory from Cargo OUT_DIR '{out_dir}'. \
                     Set DUCKDB_LIB_DIR to use an existing DuckDB library."
                )
            })?
            .join(&target)
            .join(source.cache_key());
        fs::create_dir_all(&download_dir)?;

        let archive_path = download_dir.join(archive.archive_name);
        let lib_marker = download_dir.join(archive.dynamic_lib);

        if lib_marker.exists() {
            println!("cargo:warning=Reusing libduckdb from {}", download_dir.display());
        } else {
            let url = source.archive_url(archive);
            ensure_libduckdb(&url, &archive_path)?;
            extract_libduckdb(&archive_path, &download_dir)?;
            if !lib_marker.exists() {
                return Err(format!(
                    "Downloaded archive did not contain expected library '{}'",
                    archive.dynamic_lib
                )
                .into());
            }
        }

        // The pregenerated bindings need no header, but bindgen does.
        #[cfg(all(feature = "buildtime_bindgen", feature = "capi-v2"))]
        if !download_dir.join("duckdb_v2.h").exists() {
            return Err(format!(
                "The downloaded DuckDB {} archive does not ship duckdb_v2.h, which `buildtime_bindgen` \
                 needs for the `capi-v2` bindings. Use the pregenerated bindings, point DUCKDB_INCLUDE_DIR \
                 at a directory containing duckdb_v2.h, or pin a newer release in {RELEASE_PIN_FILE}.",
                source.version
            )
            .into());
        }

        configure_link_search(&download_dir);

        copy_libduckdb(&download_dir, archive.dynamic_lib, out_dir)?;

        Ok(HeaderLocation::IncludeDir(download_dir))
    }

    fn configure_link_search(lib_dir: &Path) {
        println!("cargo:rustc-link-search=native={}", lib_dir.display());
        emit_link_lib(link_directive());
    }

    // Ensures the libduckdb archive exists: reuses an existing archive or
    // downloads it into a temp file and atomically renames it into place.
    fn ensure_libduckdb(url: &str, archive_path: &Path) -> Result<(), Box<dyn std::error::Error>> {
        if archive_path.exists() {
            println!("cargo:warning=libduckdb already present at {}", archive_path.display());
            return Ok(());
        }
        let tmp_path = archive_path.with_extension("download");
        if let Some(parent) = archive_path.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut response = http_client().get(url).call()?;
        let mut tmp_file = fs::File::create(&tmp_path)?;
        io::copy(&mut response.body_mut().as_reader(), &mut tmp_file)?;
        fs::rename(&tmp_path, archive_path)?;
        println!("cargo:warning=Downloaded libduckdb from {url}");
        Ok(())
    }

    // The archives are flat tarballs: the shared library (plus the MSVC import
    // library on Windows) and the public headers.
    fn extract_libduckdb(archive_path: &Path, destination: &Path) -> Result<(), Box<dyn std::error::Error>> {
        let file = fs::File::open(archive_path)?;
        let mut archive = tar::Archive::new(flate2::read::GzDecoder::new(file));
        archive.set_preserve_mtime(false);
        archive.unpack(destination)?;
        println!("cargo:warning=Extracted libduckdb to {}", destination.display());
        Ok(())
    }

    fn duckdb_version_from_pkg_version(pkg_version: &str) -> String {
        // duckdb-rs uses 1.MAJOR_MINOR_PATCH.x, e.g. DuckDB 1.5.0 => duckdb-rs 1.10500.x.
        let encoded = pkg_version
            .split('.')
            .nth(1)
            .expect("CARGO_PKG_VERSION should use the documented 1.MAJOR_MINOR_PATCH.x format")
            .parse::<u32>()
            .expect("CARGO_PKG_VERSION should encode the DuckDB version as an integer in its second component");
        let duckdb_major = encoded / 10_000;
        let duckdb_minor = (encoded / 100) % 100;
        let duckdb_patch = encoded % 100;
        format!("{duckdb_major}.{duckdb_minor}.{duckdb_patch}")
    }

    fn http_client() -> ureq::Agent {
        let timeout = env::var("CARGO_HTTP_TIMEOUT")
            .or_else(|_| env::var("HTTP_TIMEOUT"))
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(90);
        ureq::Agent::new_with_config(
            ureq::Agent::config_builder()
                .timeout_global(Some(std::time::Duration::from_secs(timeout)))
                .build(),
        )
    }
}

// The release archives give libduckdb an @rpath-relative install name, and a
// dependency's build script cannot add an rpath to the binaries Cargo links for
// the crates above it. Stage the library beside those binaries instead, so the
// loader path Cargo sets when it runs them resolves it.
//
// Best effort: a static or header-only DUCKDB_LIB_DIR has nothing to stage, and
// callers who point the loader at the original directory keep working either way.
fn stage_runtime_lib(lib_dir: &Path, out_dir: &str) {
    // Nothing to stage unless we asked rustc to link a shared library.
    if is_loadable_extension() || !link_directive().starts_with("dylib=") {
        return;
    }
    let Ok(target) = env::var("TARGET") else {
        return;
    };
    let Some(archive) = LibduckdbArchive::for_target(&target) else {
        return;
    };
    let source = lib_dir.join(archive.dynamic_lib);
    if !source.is_file() {
        return;
    }

    // Refresh the copy when the original is rebuilt in place, so the staged
    // library can never shadow it with stale bytes.
    println!("cargo:rerun-if-changed={}", source.display());

    if let Err(error) = copy_libduckdb(lib_dir, archive.dynamic_lib, out_dir) {
        println!(
            "cargo:warning=Could not stage {} beside the test binaries: {error}",
            archive.dynamic_lib
        );
    }
}

// Copy libduckdb into target/<profile>/deps so executables/tests can load it via
// the loader path Cargo sets when it runs them.
fn copy_libduckdb(source_dir: &Path, lib_filename: &str, out_dir: &str) -> Result<(), Box<dyn std::error::Error>> {
    let Some(deps_dir) = crate::build_paths::profile_deps_dir(Path::new(out_dir)) else {
        return Err(format!(
            "Could not determine runtime library directory from Cargo OUT_DIR '{out_dir}'. \
             Set DUCKDB_LIB_DIR to use an existing DuckDB library."
        )
        .into());
    };
    fs::create_dir_all(&deps_dir)?;
    let source = source_dir.join(lib_filename);
    let dest = deps_dir.join(lib_filename);
    if dest.exists() {
        fs::remove_file(&dest)?;
    }
    fs::copy(&source, &dest)?;
    println!("cargo:warning=Copied libduckdb to {}", dest.display());
    Ok(())
}

/// A DuckDB 2.0 `shared-libs` release artifact for one platform, as produced
/// by `scripts/package_release_artifact.sh` in the DuckDB repository.
struct LibduckdbArchive {
    #[cfg_attr(not(feature = "download-lib"), allow(dead_code))]
    archive_name: &'static str,
    dynamic_lib: &'static str,
}

impl LibduckdbArchive {
    fn for_target(target: &str) -> Option<&'static Self> {
        const OSX: LibduckdbArchive = LibduckdbArchive {
            archive_name: "duckdb-shared-libs-osx-universal.tar.gz",
            dynamic_lib: "libduckdb.dylib",
        };
        const LINUX_AMD64: LibduckdbArchive = LibduckdbArchive {
            archive_name: "duckdb-shared-libs-linux-amd64.tar.gz",
            dynamic_lib: "libduckdb.so",
        };
        const LINUX_ARM64: LibduckdbArchive = LibduckdbArchive {
            archive_name: "duckdb-shared-libs-linux-arm64.tar.gz",
            dynamic_lib: "libduckdb.so",
        };
        const WINDOWS_AMD64: LibduckdbArchive = LibduckdbArchive {
            archive_name: "duckdb-shared-libs-windows-amd64.tar.gz",
            dynamic_lib: "duckdb.dll",
        };
        const WINDOWS_ARM64: LibduckdbArchive = LibduckdbArchive {
            archive_name: "duckdb-shared-libs-windows-arm64.tar.gz",
            dynamic_lib: "duckdb.dll",
        };
        match target {
            t if t.ends_with("apple-darwin") => Some(&OSX),
            "x86_64-unknown-linux-gnu" => Some(&LINUX_AMD64),
            "aarch64-unknown-linux-gnu" => Some(&LINUX_ARM64),
            "x86_64-pc-windows-msvc" => Some(&WINDOWS_AMD64),
            "aarch64-pc-windows-msvc" => Some(&WINDOWS_ARM64),
            _ => None,
        }
    }
}

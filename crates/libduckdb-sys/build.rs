use std::env;
use std::path::{Path, PathBuf};

fn main() {
    // Re-run build if the discovery environment changed
    println!("cargo:rerun-if-env-changed=DUCKDB_LIB_DIR");
    println!("cargo:rerun-if-env-changed=DUCKDB_INCLUDE_DIR");
    println!("cargo:rerun-if-changed=.env");

    dotenv::dotenv().ok();

    // Check Cargo features before loading .env so configuration cannot enable a feature.
    let bundled = env::var("LINK_DUCKDB_BUNDLED").unwrap_or_default().to_lowercase() == "true";

    println!("cargo:rerun-if-env-changed=LINK_DUCKDB_BUNDLED");

    let header_file = Path::new(&env::var("DUCKDB_INCLUDE_DIR").unwrap()).join("duckdb_v2.h");

    println!("cargo:rerun-if-changed={}", header_file.to_string_lossy());

    // Prefer explicit dirs (local build tree); otherwise discover a system install.
    match (env::var("DUCKDB_LIB_DIR"), env::var("DUCKDB_INCLUDE_DIR")) {
        (Ok(lib_dir), Ok(inc_dir)) => link_from_dirs(&lib_dir, &inc_dir, bundled),
        _ => link_from_system(bundled),
    }
}

/// Link against DuckDB found at explicit lib/include dirs (e.g. a local build tree).
fn link_from_dirs(lib_dir: &str, inc_dir: &str, bundled: bool) {
    // Resolve to absolute paths so the embedded rpath (and includes) are valid regardless of cwd
    let lib_dir = resolve_dir(lib_dir);
    let inc_dir = resolve_dir(inc_dir);
    let lib_dir_str = lib_dir.display().to_string();

    // Both libduckdb.dylib and the self-contained libduckdb_static.a live in DUCKDB_LIB_DIR
    println!("cargo:rustc-link-search=native={lib_dir_str}");
    if bundled {
        link_static();
    } else {
        link_lib_dynamic();
        emit_rpaths(&lib_dir_str);
    }

    // Export metadata to dependent crates (exposed as DEP_DUCKDB_*).
    println!("cargo:lib_dir={lib_dir_str}");
    if bundled {
        println!("cargo:bundled=1");
    }

    generate_bindings(&inc_dir.join("duckdb_v2.h").to_string_lossy(), &[]);
}

/// Discover a system-installed DuckDB via pkg-config (Unix) or vcpkg (Windows).
/// Both probes emit the link directives themselves; we return the include paths.
fn link_from_system(bundled: bool) {
    let include_paths = probe_pkg_config(bundled).or_else(probe_vcpkg).unwrap_or_else(|| {
        panic!(
            "DuckDB not found. Set DUCKDB_LIB_DIR and DUCKDB_INCLUDE_DIR, or make it \
                 discoverable via pkg-config (duckdb.pc) or vcpkg."
        )
    });

    if bundled {
        println!("cargo:bundled=1");
    }

    generate_bindings("duckdb_v2.h", &include_paths);
}

/// Probe pkg-config. Returns the include paths on success, `None` if unavailable.
fn probe_pkg_config(bundled: bool) -> Option<Vec<PathBuf>> {
    let library = pkg_config::Config::new().statik(bundled).probe("duckdb").ok()?;

    if !bundled {
        // System libs are usually on the default loader path, but a keg-only install
        // (e.g. Homebrew) is not — add rpaths so dynamic runs resolve without env vars.
        if let Some(first) = library.link_paths.first() {
            println!("cargo:lib_dir={}", first.display());
        }
        for path in &library.link_paths {
            emit_rpaths(&path.display().to_string());
        }
    }
    Some(library.include_paths)
}

/// Probe vcpkg. Returns the include paths on success, `None` if unavailable.
fn probe_vcpkg() -> Option<Vec<PathBuf>> {
    let library = vcpkg::find_package("duckdb").ok()?;
    Some(library.include_paths)
}

/// Link the shared library.
fn link_lib_dynamic() {
    println!("cargo:rustc-link-lib=dylib=duckdb");
}

/// Link the self-contained static archive plus the C++ runtime it needs.
fn link_static() {
    println!("cargo:rustc-link-lib=static=duckdb_static");
    match env::var("CARGO_CFG_TARGET_OS").as_deref() {
        Ok("macos") | Ok("ios") => println!("cargo:rustc-link-lib=dylib=c++"),
        _ => println!("cargo:rustc-link-lib=dylib=stdc++"),
    }
}

/// Emit rpaths so binaries/tests find libduckdb.dylib at runtime.
/// The absolute path works straight from the build tree; the loader-relative
/// entries let a shipped binary find the dylib placed next to it (or in ../lib).
fn emit_rpaths(lib_dir: &str) {
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
        // windows: no rpath concept — the DLL must sit next to the .exe or on PATH.
        _ => {}
    }
}

/// Resolve a user-supplied dir to an absolute path.
/// Build scripts run with cwd = the crate's manifest dir, so a relative path must be
/// resolved against the directory cargo was invoked from (inherited via $PWD), not cwd.
fn resolve_dir(raw: &str) -> PathBuf {
    let path = Path::new(raw);
    let abs = if path.is_absolute() {
        path.to_path_buf()
    } else if let Some(pwd) = env::var_os("PWD") {
        Path::new(&pwd).join(path)
    } else {
        println!(
            "cargo:warning=relative DuckDB dir '{raw}' but $PWD is unset; \
             resolving against the crate dir — prefer an absolute path"
        );
        path.to_path_buf()
    };
    // Clean up ../ and symlinks when the dir exists; fall back to the lexical path otherwise.
    std::fs::canonicalize(&abs).unwrap_or(abs)
}

/// Generate bindings for duckdb_v2.h, adding any discovered include dirs as -I.
fn generate_bindings(header: &str, include_dirs: &[PathBuf]) {
    let mut builder = bindgen::Builder::default()
        .header(header)
        .default_enum_style(bindgen::EnumVariation::Rust { non_exhaustive: true })
        .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()));

    for dir in include_dirs {
        builder = builder.clang_arg(format!("-I{}", dir.display()));
    }

    let bindings = builder.generate().expect("Unable to generate bindings");

    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Couldn't write bindings!");
}

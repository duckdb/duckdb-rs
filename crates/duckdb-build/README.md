# duckdb-build

Shared DuckDB installation discovery and runtime linking for Cargo build scripts.
This crate has no native DuckDB dependency and does not download DuckDB.
Discovery resolves artifacts without emitting linker directives. Linking and
runtime-path configuration are separate operations.

`libduckdb-sys` uses this crate internally. Applications can also use its
runtime-path helper to run against a local installation without setting loader
environment variables. Static builds and loader-visible installations do not
need the application helper.

## Installation discovery

By default, `libduckdb-sys` looks for the release pinned in
[`src/consts.rs`](src/consts.rs):

```text
~/.duckdb/lib/v2.0.0-alpha39998/
    duckdb_v2.h
    libduckdb.dylib | libduckdb.so | libduckdb_static.a
```
Alternatively, set **both** variables to override the entire installation:
```sh
export DUCKDB_INCLUDE_DIR=... # duckdb_v2.h location
export DUCKDB_LIB_DIR=... # libduckdb location
```

Setting only one variable, an empty value, or an invalid installation is an
error; overrides are never mixed with pkg-config or the pinned installation.
Relative overrides are resolved against the invocation directory (`PWD`, when
absolute). If `PWD` is unset or relative, they resolve against the build script's
current directory with a warning. Use absolute paths when possible, and let the
shell expand `~` (for example, `export DUCKDB_LIB_DIR=~/duckdb/lib`).

Static linking is supported using the `static` feature flag on `duckdb` or
`libduckdb-sys`.

### Build-script API

For a native binding crate, discovery and emission can be separate:

```rust,no_run
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let installation = duckdb_build::resolve_library()?;
    // Use installation.header and installation.include_dir() for bindgen.
    installation.emit_rerun_calls();
    installation.emit_linker_flags()?;
    Ok(())
}
```

`resolve_library()` selects static linking when the helper's `static` feature is
enabled, and dynamic linking otherwise.

The returned `ResolvedLibrary` exposes `library`, `header`, and `mode` fields,
plus `lib_dir()` and `include_dir()` accessors.
Directory-based installations emit their link flags directly.
System installations delegate emission to pkg-config with a second probe,
rather than duplicating pkg-config's linker logic. Keep the provider environment
unchanged between discovery and emission.

### Optional system discovery

The `pkg-config` Cargo feature is **disabled by default**. Enable it on
`duckdb`, `libduckdb-sys`, or directly on `duckdb-build`:

```toml
[dependencies]
duckdb = { git = "https://github.com/duckdb/duckdb-rs.git", rev = "<SHA>", features = ["pkg-config"] }

```

The `duckdb` and `libduckdb-sys` features forward to the native build helper.
No application build dependency is needed for system discovery.

- Without a system-discovery feature, discovery tries the pinned installation,
  then `/usr/local/lib` and `/usr/local/include`.
- With the feature, pkg-config is used. A failed probe is an error, not a silent fallback.
- Setting both `DUCKDB_LIB_DIR` and `DUCKDB_INCLUDE_DIR` bypasses system
  discovery entirely. A partial or invalid override is an error.

For pkg-config, install a compatible `duckdb.pc` and use `PKG_CONFIG_PATH` when
it is outside the usual search path. Static linking preserves `Libs.private`
and the package's actual archive names instead of assuming `duckdb_static`.
The pkg-config crate handles linker flags, framework directives, target-qualified
environment variables, and cross-compilation guards. System discovery may emit
Cargo environment rebuild hints, but does not emit linker directives.

System packages must supply a compatible v2 library and `duckdb_v2.h`. Opting
into system discovery does not enforce the native version or SHA in `consts.rs`.

#### In case of missing libaries
You can install them using the official DuckDB install script:
```sh
curl https://install.duckdb.org | DUCKDB_INSTALL=dynamic sh 
```

## Runtime deployment

Build-time discovery does not configure the operating system's loader. The
native build helper emits rpaths and installation metadata, but Cargo does not
propagate a dependency's `rustc-link-arg` directives to applications.
For shared linking, install DuckDB in a loader-visible location or configure
deployment explicitly. `PKG_CONFIG_PATH` and `DUCKDB_LIB_DIR` affect the build,
not runtime library lookup. The default pinned directory under `~/.duckdb` is
not normally searched by the loader.

On Linux, use a system library location, loader configuration, `LD_LIBRARY_PATH`,
or an rpath. On macOS, lookup also depends on the dylib's install name; an
`@rpath` library needs an appropriate rpath. On Windows, place `duckdb.dll`
beside the executable or on `PATH`.

### Application helper

For the pinned installation or a directory override, add the helper to embed
the installed shared library's runtime path:

```toml
[build-dependencies]
duckdb-build = { git = "https://github.com/duckdb/duckdb-rs.git", rev = "<SHA>" }
```

In the application's `build.rs`:

```rust,no_run
fn main() -> Result<(), Box<dyn std::error::Error>> {
    duckdb_build::emit_dynamic_linking_flags()
}
```

`emit_dynamic_linking_flags()` resolves the installation using the same full
environment override, opt-in system provider, or default directories as native
linking. It requires both the library and `duckdb_v2.h`.
Use the same version/source and discovery configuration for
`duckdb` and `duckdb-build`; if using pkg-config, enable it on both dependencies.
The feature remains disabled by default.

This embeds an absolute path, so the installation must remain available at that
location. Skip the helper for static linking. It does not automatically detect
the link mode selected by a transitive dependency.

### Explicit and packaged paths

If the directory is already known, supply an absolute path instead:

```rust,no_run
fn main() {
    duckdb_build::emit_runtime_path(&std::path::PathBuf::from("/opt/duckdb/lib"));
}
```

Explicit paths do not discover installations, inspect artifacts, or require the
`pkg-config` feature. The helper must run from the final application's build script
because Cargo does not propagate a dependency's `rustc-link-arg` directives.
It emits nothing on Windows.

Both helpers also emit runtime paths for the executable's directory and its
`../lib` directory, using `$ORIGIN` on Linux/Android and `@loader_path` on
macOS/iOS. They do not copy the shared library; packaging remains the
application's responsibility. For deployment using only relative paths, emit
the appropriate target-specific linker arguments from the application's build
script instead of embedding an absolute installation path.

Applications that need the exact build-time selection can add `libduckdb-sys`
as a direct dependency using the same version/source as `duckdb`. Their build
scripts then receive `DEP_DUCKDB_LIB_DIR`, `DEP_DUCKDB_INCLUDE_DIR`, and
`DEP_DUCKDB_STATIC` (set for static linking). Metadata is only passed to immediate
dependents, not through `duckdb` or a `duckdb-build` build dependency.

## Static builds

Enable `duckdb`'s `static` feature to link an
existing static DuckDB archive. No DuckDB shared library or runtime-path helper
is needed. This is not a vendored build: it does not compile DuckDB from source,
and any other dynamically linked dependencies still need to be available.

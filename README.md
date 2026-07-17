# duckdb-rs

[![Latest Version](https://img.shields.io/crates/v/duckdb.svg)](https://crates.io/crates/duckdb)
[![Documentation](https://img.shields.io/badge/docs.rs-duckdb-orange)](https://docs.rs/duckdb)
[![MIT License](https://img.shields.io/crates/l/duckdb.svg)](LICENSE)
[![Downloads](https://img.shields.io/crates/d/duckdb.svg)](https://crates.io/crates/duckdb)
[![CI](https://github.com/duckdb/duckdb-rs/workflows/CI/badge.svg)](https://github.com/duckdb/duckdb-rs/actions)

duckdb-rs is an ergonomic Rust wrapper for [DuckDB](https://github.com/duckdb/duckdb).

You can use it to:

- Query DuckDB with type-safe bindings and an API inspired by [rusqlite](https://github.com/rusqlite/rusqlite).
- Read and write Arrow, Parquet, JSON, and CSV formats natively.
- Create DuckDB extensions in Rust with custom scalar and table functions.

## Quickstart

Create a new project and add the `duckdb` crate:

```shell
cargo new quack-in-rust
cd quack-in-rust
cargo add duckdb -F bundled
```

Update `src/main.rs` with the following code:

```rust
use duckdb::{params, Connection, Result};

struct Duck {
    id: i32,
    name: String,
}

fn main() -> Result<()> {
    let conn = Connection::open_in_memory()?;

    conn.execute(
        "CREATE TABLE ducks (id INTEGER PRIMARY KEY, name TEXT)",
        [], // empty list of parameters
    )?;

    conn.execute_batch(
        r#"
        INSERT INTO ducks (id, name) VALUES (1, 'Donald Duck');
        INSERT INTO ducks (id, name) VALUES (2, 'Scrooge McDuck');
        "#,
    )?;

    conn.execute(
        "INSERT INTO ducks (id, name) VALUES (?, ?)",
        params![3, "Darkwing Duck"],
    )?;

    let ducks = conn
        .prepare("FROM ducks")?
        .query_map([], |row| {
            Ok(Duck {
                id: row.get(0)?,
                name: row.get(1)?,
            })
        })?
        .collect::<Result<Vec<_>>>()?;

    for duck in ducks {
        println!("{}) {}", duck.id, duck.name);
    }

    Ok(())
}
```

Execute the program with `cargo run` and watch DuckDB in action!

## Examples

The following [examples](crates/duckdb/examples) demonstrate various features and use cases of duckdb-rs:

- **basic** - Basic usage including creating tables, inserting data, and querying with and without Arrow.
- **appender** - Bulk data insertion using the appender API with transactions.
- **arrow_vtab** - Querying an in-memory Arrow RecordBatch from DuckDB SQL (requires the `vtab-arrow` feature).
- **parquet** - Reading Parquet files directly using DuckDB's Parquet extension.
- **vscalar** - Defining and registering a custom scalar function (requires the `vscalar` feature).
- **vtab** - Defining a custom table function returning primitive, LIST, and STRUCT columns (requires the `vtab` feature).
- **repl** - Interactive SQL REPL.
- **hello-ext** - A DuckDB extension written in Rust (see [Building and loading extensions](#building-and-loading-extensions)).

Run any example with `cargo run --example <name>`.

## Feature flags

The `duckdb` crate provides a number of Cargo features that can be enabled to add functionality:

### Virtual tables and functions

- `vtab` - Base support for creating custom table functions and virtual tables.
- `vtab-arrow` - Apache Arrow integration for virtual tables. Enables conversion between Arrow RecordBatch and DuckDB data chunks.
- `vtab-excel` - Deprecated no-op retained for feature compatibility.
- `vscalar` - Create custom scalar functions that operate on individual values or rows.
- `vscalar-arrow` - Arrow-optimized scalar functions for vectorized operations.

### File formats

- `json` - Enables reading and writing JSON files. Implies `bundled`.
- `parquet` - Enables reading and writing Parquet files. Implies `bundled`.

### Bundled DuckDB extensions

These extensions are only available through the CMake build backend and imply `bundled-cmake`.

- `autocomplete` - DuckDB's autocomplete extension.
- `icu` - DuckDB's ICU extension for locale-aware operations.
- `tpch` - DuckDB's TPC-H benchmark extension.
- `tpcds` - DuckDB's TPC-DS benchmark extension.

### Data integration

- `appender-arrow` - Efficient bulk insertion of Arrow data into DuckDB tables.
- `polars` - Integration with Polars DataFrames.
- `ndarray` - Conversions between DuckDB vector (`ARRAY`) columns and
  `ndarray::Array1<f32>` / `Array1<f64>`. Also see the `Vec<f32>` / `[T; N]`
  `ToSql`/`FromSql` impls, which are available without this feature.
- `serde` - Round-trip `#[derive(Serialize, Deserialize)]` Rust values to and
  from DuckDB `STRUCT` columns through the `types::Struct<T>` wrapper. Rust
  structs and string-keyed maps map to `STRUCT`; `Vec`/`[T; N]` map to `LIST`;
  unit enum variants map to `VARCHAR`. A bare `impl<T: Serialize> ToSql` is
  incoherent with the scalar impls, so values are wrapped (`[Struct(&value)]`
  to bind, `Struct<T>` to read back).
- `rust_decimal` - Compatibility impls for `rust_decimal::Decimal`. DuckDB
  `DECIMAL` values use `duckdb::types::Decimal` as the core full-domain
  carrier; this feature restores `ToSql`/`FromSql` conversions for
  `rust_decimal::Decimal` when values fit that crate's decimal domain,
  including compatibility reads from `FLOAT`, `DOUBLE`, and text columns.

### Convenience features

- `vtab-full` - Enables virtual table features: `vtab-arrow` and `appender-arrow`; retains the deprecated `vtab-excel` compatibility flag.
- `extensions-full` - Enables all major extensions: `json`, `parquet`, and `vtab-full`.
- `modern-full` - Enables modern Rust ecosystem integrations: `chrono`, `serde`, `serde_json`, `url`, `r2d2`, `uuid`, `polars`, `rust_decimal`, and `ndarray`.

### Build configuration

- `bundled` - Uses a bundled version of DuckDB's source code and compiles it during build. This is the simplest way to get started and avoids needing DuckDB system libraries.
- `bundled-cmake` - _Experimental_. Builds DuckDB via its upstream CMake build system instead of `cc`. Requires a duckdb-rs checkout (not available from crates.io). See [step 2](#notes-on-building-duckdb-and-libduckdb-sys) below for details.
- `buildtime_bindgen` - Use bindgen at build time to generate fresh bindings instead of using pre-generated ones.
- `loadable-extension` - _Experimental_ support for creating loadable DuckDB extensions. Includes procedural macros for extension development. Only enable this when building an extension, never for a client application - see [Database client or extension?](#database-client-or-extension)

## Installation

### Versioning

Starting with DuckDB `v1.5.0`, the duckdb-rs version encodes the DuckDB version in its second semver component.
The format is `1.MAJOR_MINOR_PATCH.x`, e.g., DuckDB `v1.5.0` maps to duckdb-rs `1.10500.x`.
Use a tilde requirement to receive duckdb-rs patch releases without automatically moving to a different bundled DuckDB version.

### Using stable releases from crates.io

The recommended way to use duckdb-rs is to add it from crates.io:

```shell
cargo add duckdb -F bundled
```

Or manually add it to your `Cargo.toml`:

```toml
[dependencies]
duckdb = { version = "~1.10504.0", features = ["bundled"] }
```

### Using the development version from git

To use the latest development version from the main branch, you can specify a git dependency in your `Cargo.toml`:

```toml
# Use a specific branch
duckdb = { git = "https://github.com/duckdb/duckdb-rs", branch = "main", features = ["bundled"] }

# Use a specific commit
duckdb = { git = "https://github.com/duckdb/duckdb-rs", rev = "abc123def", features = ["bundled"] }
```

Note: Using the main branch of duckdb-rs means you'll get the latest Rust bindings and features, but you'll still be using whatever version of DuckDB core is bundled with that commit (when using the `bundled` feature).
Use the `v1.4-andium` branch for LTS releases that stay on DuckDB 1.4 Andium.

## Notes on building duckdb and libduckdb-sys

`libduckdb-sys` is a separate crate from `duckdb-rs` that provides the Rust
declarations for DuckDB's C API. By default, `libduckdb-sys` attempts to find a DuckDB library that already exists on your system using pkg-config, or a
[Vcpkg](https://github.com/Microsoft/vcpkg) installation for MSVC ABI builds.

You can adjust this behavior in a number of ways:

1. If you use the `bundled` feature, `libduckdb-sys` will use the
   [cc](https://crates.io/crates/cc) crate to compile DuckDB from source and
   link against that. This source is embedded in the `libduckdb-sys` crate and
   tracks the DuckDB version vendored for that duckdb-rs release.
   This is probably the simplest solution to any build problems. You can enable this by adding the following in your `Cargo.toml` file:

   ```bash
   cargo add duckdb --features bundled
   ```

   `Cargo.toml` will be updated.

   ```toml
   [dependencies]
   duckdb = { version = "~1.10504.0", features = ["bundled"] }
   ```

2. If you use the `bundled-cmake` feature, `libduckdb-sys` will build DuckDB from the local checkout in `crates/libduckdb-sys/duckdb-sources` using upstream CMake. This keeps plain `bundled` unchanged while allowing CMake-only extensions such as `icu`.

   Example:

   ```toml
   [dependencies]
   duckdb = { git = "https://github.com/duckdb/duckdb-rs", branch = "main", features = ["bundled-cmake", "icu"] }
   ```

   Notes:
   - `bundled-cmake` is _experimental_ and requires a git/workspace checkout. Published crates omit the full `duckdb-sources` tree.
   - It implies `bundled` for conditional-compilation gates, but uses CMake instead of the `cc` backend. Any CMake-only extension feature (e.g. `icu`) enables it automatically.
   - It always links DuckDB's default static extensions (`core_functions` and `parquet`) and therefore implies the `parquet` Cargo feature.
   - Plain `bundled` uses DuckDB's standard allocator and skips jemalloc. `bundled-cmake` enables upstream jemalloc on supported 64-bit, non-musl Linux targets. Other targets follow DuckDB's CMake checks. Set `DUCKDB_DISABLE_JEMALLOC=1` to force the standard allocator.
   - Extension autoload/autoinstall are enabled to match `bundled`, despite upstream CMake defaults. Set `DUCKDB_DISABLE_EXTENSION_LOAD=1` or `DISABLE_EXTENSION_LOAD=1` to disable external extension install/load support and force autoload/autoinstall off. Statically linked extensions remain available.
   - If `ninja` is on `PATH`, the build uses Ninja by default. Set `CMAKE_GENERATOR` to override.
   - DuckDB builds in `Release` mode by default, even for Rust debug builds, avoiding DuckDB's much slower debug/sanitizer profile. Override with `DUCKDB_CMAKE_BUILD_TYPE` or `CMAKE_BUILD_TYPE`. `DUCKDB_CMAKE_BUILD_TYPE` takes precedence.
   - `DUCKDB_EXTENSION_CONFIGS` is unsupported. Setting it fails fast.
   - Use `cargo build -vv -F bundled-cmake` for CMake configure/build logs.

3. When linking against a DuckDB library already on the system (so _not_ using any of the `bundled` features), you can set the `DUCKDB_LIB_DIR` environment variable to point to a directory containing the library. You can also set the `DUCKDB_INCLUDE_DIR` variable to point to the directory containing `duckdb.h`.

   Linux example:

   ```shell
   wget https://github.com/duckdb/duckdb/releases/download/v1.5.4/libduckdb-linux-arm64.zip
   unzip libduckdb-linux-arm64.zip -d libduckdb

   export DUCKDB_LIB_DIR=$PWD/libduckdb
   export DUCKDB_INCLUDE_DIR=$DUCKDB_LIB_DIR
   export LD_LIBRARY_PATH=$DUCKDB_LIB_DIR

   cargo build --examples
   ```

   macOS example:

   ```shell
   wget https://github.com/duckdb/duckdb/releases/download/v1.5.4/libduckdb-osx-universal.zip
   unzip libduckdb-osx-universal.zip -d libduckdb

   export DUCKDB_LIB_DIR=$PWD/libduckdb
   export DUCKDB_INCLUDE_DIR=$DUCKDB_LIB_DIR
   export DYLD_FALLBACK_LIBRARY_PATH=$DUCKDB_LIB_DIR

   cargo build --examples
   ```

4. Setting `DUCKDB_DOWNLOAD_LIB=1` makes the build script download pre-built DuckDB binaries from GitHub Releases. This always links against the dynamic library in the archive (setting `DUCKDB_STATIC` has no effect), and it effectively automates the manual steps above. The archives are cached in `target/duckdb-download/<target>/<version>` and that directory is automatically added to the linker search path. The downloaded version always matches the DuckDB version encoded in the `libduckdb-sys` crate version.

   ```shell
   DUCKDB_DOWNLOAD_LIB=1 cargo test
   ```

5. Installing the DuckDB development packages will usually be all that is required, but
   the build helpers for [pkg-config](https://github.com/alexcrichton/pkg-config-rs)
   and [vcpkg](https://github.com/mcgoo/vcpkg-rs) have some additional configuration
   options. The default when using vcpkg is to dynamically link,
   which must be enabled by setting `VCPKGRS_DYNAMIC=1` environment variable before build.

When none of the options above are used, the build script falls back to this discovery path and will emit the appropriate `cargo:rustc-link-lib` directives if DuckDB is found on your system.

Cross-compiling with `bundled` is best-effort (not covered by CI): it compiles DuckDB from C++ source and so needs a working C++ cross-compiler for the target, not just the Rust toolchain. If you can't get one working, prefer one of the non-`bundled` options above (e.g. `DUCKDB_LIB_DIR` or `DUCKDB_DOWNLOAD_LIB=1`) and link a pre-built library for the target instead.

### ICU extension and the bundled features

When using the `bundled` feature, the ICU extension is not included due to crates.io's 10MB package size limit. This means some date/time operations (like `now() - interval '1 day'` or `ts::date` casts) will fail. You can load ICU at runtime:

```rust,ignore
conn.execute_batch("INSTALL icu; LOAD icu;")?;
```

Alternatively, link against a system libduckdb that was compiled with ICU (see build instructions above).

If you are working from a duckdb-rs checkout, you can also use `bundled-cmake,icu` to compile ICU in through DuckDB's CMake build.

### Binding generation

We use [bindgen](https://crates.io/crates/bindgen) to generate the Rust
declarations from DuckDB's C header file. `bindgen`
[recommends](https://github.com/servo/rust-bindgen#library-usage-with-buildrs)
running this as part of the build process for libraries that use it. We tried
this briefly (`duckdb` 0.10.0, specifically), but it had some annoyances:

- The build time for `libduckdb-sys` (and therefore `duckdb`) increased
  dramatically.
- Running `bindgen` requires a relatively recent version of Clang, which many
  systems do not have installed by default.
- Running `bindgen` also requires the DuckDB header file to be present.

So we try to avoid running `bindgen` at build time by shipping
pregenerated bindings for DuckDB.

If you use the `bundled` feature, you will get pregenerated bindings for the
bundled version of DuckDB. If you want to run `bindgen` at build time to
produce your own bindings, use the `buildtime_bindgen` Cargo feature.

## Thread safety

A `Connection` is `Send` but not `Sync`: you may move it between threads, but you cannot share a single connection across threads at the same time. DuckDB does support concurrent access through _multiple_ connections to the same database (see [DuckDB's concurrency docs](https://duckdb.org/docs/current/connect/concurrency).

For multi-threaded applications, give each thread its own connection. The simplest way is a connection pool via the `r2d2` feature:

```rust,ignore
use duckdb::{DuckdbConnectionManager, params};

let manager = DuckdbConnectionManager::file("file.db")?;
let pool = r2d2::Pool::new(manager)?;

// Each worker checks out its own connection from the pool.
let conn = pool.get()?;
conn.execute("INSERT INTO foo (bar) VALUES (?)", params![1])?;
```

## Database client or extension?

duckdb-rs covers two distinct use cases, and you normally pick exactly one:

- **As a database client** (most common): embed DuckDB in your Rust application to run queries, as shown in the [Quickstart](#quickstart).
- **For writing DuckDB extensions**: build a loadable `.duckdb_extension` that DuckDB loads at runtime. Enable the `loadable-extension` feature.

Do **not** enable `loadable-extension` for a regular client application. It replaces DuckDB's normal C API functions with extension wrappers that are only initialized when DuckDB loads your extension. Outside an extension context, calls such as `Connection::open_in_memory()` then panic with `DuckDB API not initialized or DuckDB feature omitted`.

## Building and loading extensions

The [`hello-ext`](crates/duckdb/examples/hello-ext) example shows the extension API in action: it registers a `hello(name)` table function via the `loadable-extension` feature and the `duckdb_entrypoint_c_api` macro.

`cargo build` alone does **not** produce a loadable extension though. DuckDB only loads files ending in `.duckdb_extension` that carry a metadata footer matching the target DuckDB version. A bare shared library fails with _The file is not a DuckDB extension. The metadata at the end of the file is invalid_. Appending that footer requires DuckDB's extension build tooling.

So rather than wiring this up by hand, start from the official [extension-template-rs](https://github.com/duckdb/extension-template-rs) template for a complete, ready-to-build Rust extension. It handles the metadata step, platform/version detection, tests, and CI. Its `make debug` builds the shared library and then appends the footer, producing a `.duckdb_extension` you load with `duckdb -unsigned`.

## Rust version compatibility

duckdb-rs is built and tested with stable Rust, and keeps a rolling MSRV (minimum supported Rust version) that may only be updated when the encoded DuckDB major/minor version changes, on a need basis (e.g. project dependencies bump their MSRV or a particular Rust feature is useful for us). The new MSRV will be at least 6 months old. DuckDB patch updates and crate patch releases (e.g. `1.10500.0` to `1.10500.1`) are guaranteed to keep the same MSRV.

## Contributing

We welcome contributions! Take a look at [CONTRIBUTING.md](CONTRIBUTING.md) for more information.

Join our [Discord](https://discord.gg/tcvwpjfnZx) to chat with the community in the #rust channel.

## License

Copyright (c) Stichting DuckDB Foundation

Licensed under the [MIT license](LICENSE).

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
- **parquet** - Reading Parquet files directly using DuckDB's Parquet extension.
- **repl** - Interactive SQL REPL.
- **hello-ext** - A loadable DuckDB extension.

Run any example with `cargo run --example <name>`.

## Feature flags

The `duckdb` crate provides a number of Cargo features that can be enabled to add functionality:

### Virtual tables and functions

- `vtab` - Base support for creating custom table functions and virtual tables.
- `vtab-arrow` - Apache Arrow integration for virtual tables. Enables conversion between Arrow RecordBatch and DuckDB data chunks.
- `vtab-excel` - Read Excel (.xlsx) files directly in SQL queries with automatic schema detection.
- `vscalar` - Create custom scalar functions that operate on individual values or rows.
- `vscalar-arrow` - Arrow-optimized scalar functions for vectorized operations.

### File formats

- `json` - Enables reading and writing JSON files. Requires `bundled`.
- `parquet` - Enables reading and writing Parquet files. Requires `bundled`. The experimental `bundled-cmake` backend always includes Parquet to match DuckDB's upstream defaults.

### Bundled DuckDB extensions

- `autocomplete` - Enables DuckDB's autocomplete extension. Requires `bundled-cmake`.
- `icu` - Enables DuckDB's ICU extension. Requires `bundled-cmake`.
- `tpch` - Enables DuckDB's TPC-H extension. Requires `bundled-cmake`.
- `tpcds` - Enables DuckDB's TPC-DS extension. Requires `bundled-cmake`.

### Data integration

- `appender-arrow` - Efficient bulk insertion of Arrow data into DuckDB tables.
- `polars` - Integration with Polars DataFrames.

### Convenience features

- `vtab-full` - Enables all virtual table features: `vtab-excel`, `vtab-arrow`, and `appender-arrow`.
- `extensions-full` - Enables all major extensions: `json`, `parquet`, and `vtab-full`.
- `modern-full` - Enables modern Rust ecosystem integrations: `chrono`, `serde_json`, `url`, `r2d2`, `uuid`, and `polars`.

### Build configuration

- `bundled` - Uses a bundled version of DuckDB's source code and compiles it during build. This is the simplest way to get started and avoids needing DuckDB system libraries.
- `bundled-cmake` - *Experimental*. Builds DuckDB through its upstream CMake build system. This currently targets git/workspace checkouts of duckdb-rs where `crates/libduckdb-sys/duckdb-sources` is available locally. This path always links DuckDB's default bundled extensions for this build, currently `core_functions` and `parquet`, and therefore also implies the `parquet` Cargo feature.
- `buildtime_bindgen` - Use bindgen at build time to generate fresh bindings instead of using pre-generated ones.
- `loadable-extension` - _Experimental_ support for creating loadable DuckDB extensions. Includes procedural macros for extension development.

## Installation

### Versioning

Starting with DuckDB `v1.5.0`, the duckdb-rs version encodes the DuckDB version in its second semver component.
The format is `1.MAJOR_MINOR_PATCH.x`, e.g., DuckDB `v1.5.0` maps to duckdb-rs `1.10500.x`.

### Using stable releases from crates.io

The recommended way to use duckdb-rs is to add it from crates.io:

```shell
cargo add duckdb -F bundled
```

Or manually add it to your `Cargo.toml`:

```toml
[dependencies]
duckdb = { version = "=1.10501.0", features = ["bundled"] }
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
   duckdb = { version = "1.10501.0", features = ["bundled"] }
   ```

2. If you use the `bundled-cmake` feature, `libduckdb-sys` will build DuckDB from the local checkout in `crates/libduckdb-sys/duckdb-sources` using upstream CMake. This keeps plain `bundled` unchanged while allowing CMake-only extensions such as `icu`.

   Example:

   ```toml
   [dependencies]
   duckdb = { git = "https://github.com/duckdb/duckdb-rs", branch = "main", features = ["bundled-cmake", "icu"] }
   ```

   Notes:

   - `bundled-cmake` is *experimental*.
   - `bundled-cmake` currently targets git/workspace checkouts. It is not available from crates.io because the full `duckdb-sources` tree is not packaged there.
   - If any CMake-only extension feature is enabled, the bundled build switches to the CMake backend and all bundled extensions for that build are compiled through CMake.
   - `bundled-cmake` always links DuckDB's default bundled extensions for this build, currently `core_functions` and `parquet`. `core_functions` does not have a separate Cargo feature, and `bundled-cmake` therefore also implies `parquet`.
   - When `ninja` is available on `PATH`, the CMake backend prefers the Ninja generator automatically. Set `CMAKE_GENERATOR` to override this.
   - `bundled-cmake` builds DuckDB in `Release` mode by default, even in Rust debug builds, to avoid DuckDB's much slower debug/sanitizer profile. Set `DUCKDB_CMAKE_BUILD_TYPE` to `Debug`, `RelWithDebInfo`, `MinSizeRel`, or `Release` to override this.
   - Set `DUCKDB_DISABLE_EXTENSION_LOAD=1` to disable runtime extension loading and installation in the CMake backend.
   - Set `DUCKDB_EXTENSION_CONFIGS` to a semicolon-separated list of DuckDB extension config files to load additional extensions through DuckDB's native CMake machinery.
   - TODO: extend `bundled-cmake` to support out-of-tree static extensions end-to-end, e.g. `sqlite_scanner`, without requiring manual extra linking steps.
   - Use `cargo build -vv -F bundled-cmake` to surface the underlying CMake configure/build logs in Cargo output.

3. When linking against a DuckDB library already on the system (so _not_ using any of the `bundled` features), you can set the `DUCKDB_LIB_DIR` environment variable to point to a directory containing the library. You can also set the `DUCKDB_INCLUDE_DIR` variable to point to the directory containing `duckdb.h`.

   Linux example:

   ```shell
   wget https://github.com/duckdb/duckdb/releases/download/v1.5.1/libduckdb-linux-arm64.zip
   unzip libduckdb-linux-arm64.zip -d libduckdb

   export DUCKDB_LIB_DIR=$PWD/libduckdb
   export DUCKDB_INCLUDE_DIR=$DUCKDB_LIB_DIR
   export LD_LIBRARY_PATH=$DUCKDB_LIB_DIR

   cargo build --examples
   ```

   macOS example:

   ```shell
   wget https://github.com/duckdb/duckdb/releases/download/v1.5.1/libduckdb-osx-universal.zip
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

### ICU extension and the bundled features

When using the `bundled` feature, the ICU extension is not included due to crates.io's 10MB package size limit. This means some date/time operations (like `now() - interval '1 day'` or `ts::date` casts) will fail. You can load ICU at runtime:

```rust,ignore
conn.execute_batch("INSTALL icu; LOAD icu;")?;
```

Alternatively, link against libduckdb without the `bundled` feature (see build instructions above). The ICU extension will be built-in and pre-loaded in that case.

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

## Rust version compatibility

duckdb-rs is built and tested with stable Rust, and keeps a rolling MSRV (minimum supported Rust version) that may only be updated when the encoded DuckDB major/minor version changes, on a need basis (e.g. project dependencies bump their MSRV or a particular Rust feature is useful for us). The new MSRV will be at least 6 months old. DuckDB patch updates and crate patch releases (e.g. `1.10500.0` to `1.10500.1`) are guaranteed to keep the same MSRV.

## Contributing

We welcome contributions! Take a look at [CONTRIBUTING.md](CONTRIBUTING.md) for more information.

Join our [Discord](https://discord.gg/tcvwpjfnZx) to chat with the community in the #rust channel.

## License

Copyright (c) Stichting DuckDB Foundation

Licensed under the [MIT license](LICENSE).

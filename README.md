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
- **hello-ext** - A loadable DuckDB extension using the legacy extension API.
- **hello-ext-capi** - A loadable DuckDB extension using the modern extension API.

Run any example with `cargo run --example <name>`.

## Feature flags

The `duckdb` crate provides a number of Cargo features that can be enabled to add functionality:

### Virtual tables and functions

- `vtab` - Base support for creating custom table functions and virtual tables.
- `vtab-arrow` - Apache Arrow integration for virtual tables. Enables conversion between Arrow RecordBatch and DuckDB data chunks.
- `vtab-excel` - Read Excel (.xlsx) files directly in SQL queries with automatic schema detection.
- `vtab-loadable` - Support for creating loadable DuckDB extensions. Includes procedural macros for extension development.
- `vscalar` - Create custom scalar functions that operate on individual values or rows.
- `vscalar-arrow` - Arrow-optimized scalar functions for vectorized operations.

### Data integration

- `json` - Enables reading and writing JSON files. Requires `bundled`.
- `parquet` - Enables reading and writing Parquet files. Requires `bundled`.
- `appender-arrow` - Efficient bulk insertion of Arrow data into DuckDB tables.
- `polars` - Integration with Polars DataFrames.

### Convenience features

- `vtab-full` - Enables all virtual table features: `vtab-excel`, `vtab-arrow`, and `appender-arrow`.
- `extensions-full` - Enables all major extensions: `json`, `parquet`, and `vtab-full`.
- `modern-full` - Enables modern Rust ecosystem integrations: `chrono`, `serde_json`, `url`, `r2d2`, `uuid`, and `polars`.

### Build configuration

- `bundled` - Uses a bundled version of DuckDB's source code and compiles it during build. This is the simplest way to get started and avoids needing DuckDB system libraries.
- `buildtime_bindgen` - Use bindgen at build time to generate fresh bindings instead of using pre-generated ones.
- `loadable-extension` - _Experimental_ support for building extensions that can be dynamically loaded into DuckDB.

## Installation

### Using stable releases from crates.io

The recommended way to use duckdb-rs is to add it from crates.io:

```shell
cargo add duckdb -F bundled
```

Or manually add it to your `Cargo.toml`:

```toml
[dependencies]
duckdb = { version = "=1.4.3", features = ["bundled"] }
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
   as we are still in development, we will update it regularly. After we are more stable,
   we will use the stable released version from [duckdb](https://github.com/duckdb/duckdb/releases).
   This is probably the simplest solution to any build problems. You can enable this by adding the following in your `Cargo.toml` file:

   ```bash
   cargo add duckdb --features bundled
   ```

   `Cargo.toml` will be updated.

   ```toml
   [dependencies]
   duckdb = { version = "1.4.1", features = ["bundled"] }
   ```

2. When linking against a DuckDB library already on the system (so _not_ using any of the `bundled` features), you can set the `DUCKDB_LIB_DIR` environment variable to point to a directory containing the library. You can also set the `DUCKDB_INCLUDE_DIR` variable to point to the directory containing `duckdb.h`.

   Linux example:

   ```shell
   wget https://github.com/duckdb/duckdb/releases/download/v1.4.1/libduckdb-linux-arm64.zip
   unzip libduckdb-linux-arm64.zip -d libduckdb

   export DUCKDB_LIB_DIR=$PWD/libduckdb
   export DUCKDB_INCLUDE_DIR=$DUCKDB_LIB_DIR
   export LD_LIBRARY_PATH=$DUCKDB_LIB_DIR

   cargo build --examples
   ```

   macOS example:

   ```shell
   wget https://github.com/duckdb/duckdb/releases/download/v1.4.1/libduckdb-osx-universal.zip
   unzip libduckdb-osx-universal.zip -d libduckdb

   export DUCKDB_LIB_DIR=$PWD/libduckdb
   export DUCKDB_INCLUDE_DIR=$DUCKDB_LIB_DIR
   export DYLD_FALLBACK_LIBRARY_PATH=$DUCKDB_LIB_DIR

   cargo build --examples
   ```

3. _Experimental:_ Setting `DUCKDB_DOWNLOAD_LIB=1` makes the build script download pre-built DuckDB binaries from GitHub Releases. This always links against the dynamic library in the archive (setting `DUCKDB_STATIC` has no effect), and it effectively automates the manual steps above. The archives are cached in `target/duckdb-download/<target>/<version>` and that directory is automatically added to the linker search path. The downloaded version always matches the `libduckdb-sys` crate version.

   ```shell
   DUCKDB_DOWNLOAD_LIB=1 cargo test
   ```

4. Installing the duckdb development packages will usually be all that is required, but
   the build helpers for [pkg-config](https://github.com/alexcrichton/pkg-config-rs)
   and [vcpkg](https://github.com/mcgoo/vcpkg-rs) have some additional configuration
   options. The default when using vcpkg is to dynamically link,
   which must be enabled by setting `VCPKGRS_DYNAMIC=1` environment variable before build.

When none of the options above are used, the build script falls back to this discovery path and will emit the appropriate `cargo:rustc-link-lib` directives if DuckDB is found on your system.

### ICU extension and the bundled feature

When using the `bundled` feature, the ICU extension is not included due to crates.io's 10MB package size limit. This means some date/time operations (like `now() - interval '1 day'` or `ts::date` casts) will fail. You can load ICU at runtime:

```rust,ignore
conn.execute_batch("INSTALL icu; LOAD icu;")?;
```

Alternatively, link against libduckdb without the `bundled` feature (see build instructions above). The ICU extension will be built-in and pre-loaded in that case.

### Binding generation

We use [bindgen](https://crates.io/crates/bindgen) to generate the Rust
declarations from DuckDB's C header file. `bindgen`
[recommends](https://github.com/servo/rust-bindgen#library-usage-with-buildrs)
running this as part of the build process of libraries that used this. We tried
this briefly (`duckdb` 0.10.0, specifically), but it had some annoyances:

- The build time for `libduckdb-sys` (and therefore `duckdb`) increased
  dramatically.
- Running `bindgen` requires a relatively-recent version of Clang, which many
  systems do not have installed by default.
- Running `bindgen` also requires the DuckDB header file to be present.

So we try to avoid running `bindgen` at build-time by shipping
pregenerated bindings for DuckDB.

If you use the `bundled` features, you will get pregenerated bindings for the
bundled version of DuckDB. If you want to run `bindgen` at buildtime to
produce your own bindings, use the `buildtime_bindgen` Cargo feature.

## Rust version compatibility

duckdb-rs is built and tested with stable Rust, and will keep a rolling MSRV (minimum supported Rust version) that can only be updated in major and minor releases on a need by basis (e.g. project dependencies bump their MSRV or a particular Rust feature is useful for us etc.). The new MSRV will be at least 6 months old. Patch releases are guaranteed to have the same MSRV.

## Contributing

We welcome contributions! Take a look at [CONTRIBUTING.md](CONTRIBUTING.md) for more information.

Join our [Discord](https://discord.gg/tcvwpjfnZx) to chat with the community in the #rust channel.

## License

Copyright 2021-2025 Stichting DuckDB Foundation

Licensed under the [MIT license](LICENSE).

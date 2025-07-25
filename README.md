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

## Quick start

```rust
use duckdb::{params, Connection, Result};

// In your project, we need to keep the arrow version same as the version used in duckdb.
// Refer to https://github.com/duckdb/duckdb-rs/issues/92
// You can either:
use duckdb::arrow::record_batch::RecordBatch;
// Or in your Cargo.toml, use * as the version; features can be toggled according to your needs
// arrow = { version = "*", default-features = false, features = ["prettyprint"] }
// Then you can:
// use arrow::record_batch::RecordBatch;

use duckdb::arrow::util::pretty::print_batches;

#[derive(Debug)]
struct Person {
    id: i32,
    name: String,
    data: Option<Vec<u8>>,
}

fn main() -> Result<()> {
    let conn = Connection::open_in_memory()?;

    conn.execute_batch(
        r"CREATE SEQUENCE seq;
          CREATE TABLE person (
                  id              INTEGER PRIMARY KEY DEFAULT NEXTVAL('seq'),
                  name            TEXT NOT NULL,
                  data            BLOB
                  );
        ")?;

    let me = Person {
        id: 0,
        name: "Steven".to_string(),
        data: None,
    };
    conn.execute(
        "INSERT INTO person (name, data) VALUES (?, ?)",
        params![me.name, me.data],
    )?;

    // query table by rows
    let mut stmt = conn.prepare("SELECT id, name, data FROM person")?;
    let person_iter = stmt.query_map([], |row| {
        Ok(Person {
            id: row.get(0)?,
            name: row.get(1)?,
            data: row.get(2)?,
        })
    })?;

    for person in person_iter {
        let p = person.unwrap();
        println!("ID: {}", p.id);
        println!("Found person {:?}", p);
    }

    // query table by arrow
    let rbs: Vec<RecordBatch> = stmt.query_arrow([])?.collect();
    print_batches(&rbs).unwrap();
    Ok(())
}
```

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

## Notes on building duckdb and libduckdb-sys

`libduckdb-sys` is a separate crate from `duckdb-rs` that provides the Rust
declarations for DuckDB's C API. By default, `libduckdb-sys` attempts to find a DuckDB library that already exists on your system using pkg-config, or a
[Vcpkg](https://github.com/Microsoft/vcpkg) installation for MSVC ABI builds.

You can adjust this behavior in a number of ways:

- If you use the `bundled` feature, `libduckdb-sys` will use the
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
  # Assume that version DuckDB version 0.9.2 is used.
  duckdb = { version = "0.9.2", features = ["bundled"] }
  ```

* When linking against a DuckDB library already on the system (so _not_ using any of the `bundled` features), you can set the `DUCKDB_LIB_DIR` environment variable to point to a directory containing the library. You can also set the `DUCKDB_INCLUDE_DIR` variable to point to the directory containing `duckdb.h`.
* Installing the duckdb development packages will usually be all that is required, but
  the build helpers for [pkg-config](https://github.com/alexcrichton/pkg-config-rs)
  and [vcpkg](https://github.com/mcgoo/vcpkg-rs) have some additional configuration
  options. The default when using vcpkg is to dynamically link,
  which must be enabled by setting `VCPKGRS_DYNAMIC=1` environment variable before build.

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

## Contributing

We welcome contributions! Take a look at [CONTRIBUTING.md](CONTRIBUTING.md) for more information.

Join our [Discord](https://discord.gg/tcvwpjfnZx) to chat with the community in the #rust channel.

## License

Copyright 2021-2025 Stichting DuckDB Foundation

Licensed under the [MIT license](LICENSE).

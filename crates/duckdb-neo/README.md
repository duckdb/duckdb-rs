# DuckDB-neo

### ⚠ DuckDB's v2 C API is in rapid development. Consider `duckdb-neo` unstable. Feedback is welcome!


[![Latest Version](https://img.shields.io/crates/v/duckdb-neo.svg)](https://crates.io/crates/duckdb)
[![Documentation](https://img.shields.io/badge/docs.rs-duckdb-orange)](https://docs.rs/duckdb)
[![MIT License](https://img.shields.io/crates/l/duckdb.svg)](LICENSE)
[![Downloads](https://img.shields.io/crates/d/duckdb.svg)](https://crates.io/crates/duckdb)
[![CI](https://github.com/duckdb/duckdb-rs/workflows/CI/badge.svg)](https://github.com/duckdb/duckdb-rs/actions)

duckdb-neo is an safe, ergonomic Rust wrapper for [DuckDB](https://github.com/duckdb/duckdb).

You can use it to:

- Query DuckDB with type-safe bindings.
- Read and write Arrow, Parquet, JSON, and CSV formats natively.
- [Create DuckDB extensions](#extensions) in Rust with various custom functions!

## Quickstart

Create a new project and add the `duckdb-neo` crate:

```shell
cargo new quack-in-rust
cd quack-in-rust
cargo add duckdb-neo
```

Update `src/main.rs` with the following code:

```rust
use duckdb_neo::{
    Parameters, Result,
    environment::{Environment, StorageLocation},
};

struct Duck {
    id: i32,
    name: String,
}

fn main() -> Result<()> {
    let environment = Environment::new()?;
    let database = environment.open(StorageLocation::InMemory)?;
    let conn = database.connect()?;

    conn.execute(
        "CREATE TABLE ducks (id INTEGER PRIMARY KEY, name TEXT)",
        Parameters::None,
    )?;

    // `execute` runs one statement; `parse` splits a script into statements.
    for statement in conn.parse(
        r#"
        INSERT INTO ducks (id, name) VALUES (1, 'Donald Duck');
        INSERT INTO ducks (id, name) VALUES (2, 'Scrooge McDuck');
        "#,
    )? {
        conn.execute(statement?, Parameters::None)?;
    }

    conn.execute(
        "INSERT INTO ducks (id, name) VALUES (?, ?)",
        Parameters::positional(&[&3, &"Darkwing Duck"]),
    )?;

    // Results arrive as chunks of column vectors.
    let mut ducks = Vec::new();
    let mut result = conn.query("FROM ducks ORDER BY id", Parameters::None)?;
    while let Some(chunk) = result.next_chunk()? {
        let ids = chunk.get_vector_at::<i32>(0)?;
        let names = chunk.get_vector_at::<String>(1)?;
        for (id, name) in ids.iter()?.zip(names.iter()?) {
            ducks.push(Duck {
                id: *id.expect("id is a primary key"),
                name: name.unwrap_or_default().to_owned(),
            });
        }
    }

    for duck in ducks {
        println!("{}) {}", duck.id, duck.name);
    }

    Ok(())
}
```

Execute the program with `cargo run` and watch DuckDB in action!

## Feature flags

### Integrations

- `chrono` - Convert DuckDB `DATE`, `TIME`, and `TIMESTAMP` values to [`chrono`](https://docs.rs/chrono) types.
- `r2d2` - `duckdb_neo::r2d2::ConnectionManager`, a connection pool manager for [r2d2](https://github.com/sfackler/r2d2).
- `uuid` - Read and write DuckDB `UUID` values as [`uuid::Uuid`](https://docs.rs/uuid).

### Linking against a prebuilt DuckDB

By default, `duckdb-neo` links against a prebuilt DuckDB library. Set `DUCKDB_LIB_DIR` to use a specific one; otherwise these features are tried in order:

- `buildtime-download` _(default)_ - Download the DuckDB release this crate was built for. Set `DUCKDB_DOWNLOAD_LIB=0` to skip it.
- `buildtime-pkg-config` _(default)_ - Find a DuckDB library installed on the system with pkg-config.

### Bindings

- `buildtime-pregenerated` _(default)_ - Use the v2 C API bindings that ship with the crate.
- `buildtime-bindgen` - Generate the bindings at build time instead. Needs libclang and `duckdb_v2.h`, found through `DUCKDB_INCLUDE_DIR`. Overrides `buildtime-pregenerated` when both are enabled.

### Building DuckDB from source

These features compile DuckDB into your binary, so the linking features above are not needed:

```toml
duckdb-neo = { version = "...", default-features = false, features = ["bundled"] }
```

- `bundled` - Compile DuckDB from the bundled sources.
- `bundled-cmake` - _Experimental_. Compile DuckDB with its upstream CMake build instead. Always includes the Parquet extension. Requires a git dependency on duckdb-rs, since the published crate does not include the full DuckDB sources.

The following DuckDB extensions can be statically linked through `bundled-cmake`. Each one implies `bundled-cmake`:

- `bundled-cmake-autocomplete` - SQL autocompletion.
- `bundled-cmake-httpfs` - HTTP(S) and S3 file systems (Linux and macOS only).
- `bundled-cmake-icu` - Time zones, collations and other locale-aware operations.
- `bundled-cmake-tpch` - TPC-H data and queries.
- `bundled-cmake-tpcds` - TPC-DS data and queries.

## libduckdb-sys

`duckdb-neo` is built on [libduckdb-sys](https://crates.io/crates/libduckdb-sys), which provides the raw FFI bindings to DuckDB's C API and finds or builds the DuckDB library. The `duckdb` crate uses the same crate and version, with the v1 C API. `duckdb-neo` only enables the v2 bindings, exposed as `libduckdb_sys::v2`. You rarely need it directly. For any part of the C API that `duckdb-neo` does not wrap yet, add `libduckdb-sys` with the `capi-v2` feature.

If you encounter any shortcomings in the bindings that force an `libduckdb-sys` include, let us know!

## Extensions

TODO

## Rust version compatibility

`duckdb-neo` shares its MSRV (minimum supported Rust version) and update policy with the `duckdb` crate. It is currently Rust 1.88.

## Contributing

We welcome contributions! Take a look at [CONTRIBUTING.md](https://github.com/duckdb/duckdb-rs/blob/main/CONTRIBUTING.md) for more information.

Join our [Discord](https://discord.gg/tcvwpjfnZx) to chat with the community in the #rust channel.

## License

Copyright (c) Stichting DuckDB Foundation

Licensed under the [MIT license](https://github.com/duckdb/duckdb-rs/blob/main/LICENSE).

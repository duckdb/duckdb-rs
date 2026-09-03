# duckdb-rs

[![Latest Version](https://img.shields.io/crates/v/duckdb.svg)](https://crates.io/crates/duckdb)
[![Documentation](https://img.shields.io/badge/docs.rs-duckdb-orange)](https://docs.rs/duckdb)
[![MIT License](https://img.shields.io/crates/l/duckdb.svg)](LICENSE)
[![Downloads](https://img.shields.io/crates/d/duckdb.svg)](https://crates.io/crates/duckdb)
[![CI](https://github.com/duckdb/duckdb-rs/workflows/CI/badge.svg)](https://github.com/duckdb/duckdb-rs/actions)

duckdb-rs is an ergonomic Rust wrapper for [DuckDB](https://github.com/duckdb/duckdb), with an API inspired by [rusqlite](https://github.com/rusqlite/rusqlite). Use it to:

- Query DuckDB with type-safe bindings.
- Read and write Arrow, Parquet, JSON, and CSV formats natively.
- Build DuckDB extensions in Rust with custom scalar and table functions.

## Documentation

The **[DuckDB Rust client guide](https://duckdb.org/docs/stable/clients/rust)** is the primary documentation:

- [Overview](https://duckdb.org/docs/stable/clients/rust/overview) — installation and the full list of Cargo [feature flags](https://duckdb.org/docs/stable/clients/rust/overview#feature-flags).
- [Connect](https://duckdb.org/docs/stable/clients/rust/connecting) — configuration, connection pooling, and thread safety.
- [Import Data](https://duckdb.org/docs/stable/clients/rust/data_import) — the Appender and file readers.
- [Run Queries](https://duckdb.org/docs/stable/clients/rust/querying) — binding parameters and mapping rows to Rust types.
- [Handle Results](https://duckdb.org/docs/stable/clients/rust/result_handling) — Apache Arrow and Polars interchange.
- [Write User Defined Functions](https://duckdb.org/docs/stable/clients/rust/functions) — scalar and table functions, and loadable extensions.
- [Profile and Monitor](https://duckdb.org/docs/stable/clients/rust/profiling) — query profiling and interrupting long-running queries.
- [Troubleshoot](https://duckdb.org/docs/stable/clients/rust/troubleshoot) — linking against a system library and other build issues.

The complete API reference is on [docs.rs](https://docs.rs/duckdb).

## Quickstart

```shell
cargo add duckdb -F bundled
```

```rust
use duckdb::{Connection, Result};

fn main() -> Result<()> {
    let conn = Connection::open_in_memory()?;

    conn.execute_batch(
        "CREATE TABLE ducks (id INTEGER, name TEXT);
         INSERT INTO ducks VALUES (1, 'Donald Duck'), (2, 'Scrooge McDuck');",
    )?;

    let mut stmt = conn.prepare("SELECT id, name FROM ducks")?;
    for row in stmt.query_map([], |r| Ok((r.get::<_, i32>(0)?, r.get::<_, String>(1)?)))? {
        let (id, name) = row?;
        println!("{id}) {name}");
    }
    Ok(())
}
```

See the [documentation](https://duckdb.org/docs/stable/clients/rust) for everything beyond this.

## Examples

Runnable examples live in [`crates/duckdb/examples`](crates/duckdb/examples), covering basic usage, the Appender, Arrow virtual tables, Parquet, scalar and table functions, a REPL, and a loadable extension. Run one with the appropriate features:

```shell
cargo run --example basic --features bundled
cargo run --example arrow_vtab --features "bundled vtab-arrow"
```

`hello-ext` is a library target, so build it rather than running it:

```shell
cargo build --example hello-ext --features loadable-extension
```

## Building from a Source Checkout

The user-facing build options (`bundled`, linking against a system library, `DUCKDB_DOWNLOAD_LIB`, cross-compiling) are documented under [Troubleshoot](https://duckdb.org/docs/stable/clients/rust/troubleshoot) in the guide. The options below apply only when building from a checkout of this repository.

`DUCKDB_DOWNLOAD_LIB=1` additionally requires the `download-lib` feature, which pulls an HTTP client (`ureq`/`rustls`) into the build script. It is on by default; consumers building with `bundled` can drop it (and the HTTP client) with `default-features = false`.

### `bundled-cmake`

The `bundled-cmake` feature builds DuckDB from `crates/libduckdb-sys/duckdb-sources` using DuckDB's upstream CMake build instead of the `cc` backend. It is required for CMake-only extensions such as `icu`, and is not available from crates.io because published crates omit the full source tree.

```toml
duckdb = { git = "https://github.com/duckdb/duckdb-rs", branch = "main", features = ["bundled-cmake", "icu"] }
```

- It implies `bundled` for conditional-compilation gates and always links DuckDB's default static extensions (`core_functions` and `parquet`), so it also implies the `parquet` feature.
- It enables upstream jemalloc on supported 64-bit, non-musl Linux targets. Set `DUCKDB_DISABLE_JEMALLOC=1` to force the standard allocator.
- Extension autoload/autoinstall are enabled to match `bundled`. Set `DUCKDB_DISABLE_EXTENSION_LOAD=1` to turn them off.
- DuckDB builds in `Release` mode by default, even for Rust debug builds. Override with `DUCKDB_CMAKE_BUILD_TYPE` (which takes precedence) or `CMAKE_BUILD_TYPE`.
- If `ninja` is on `PATH`, the build uses it by default; set `CMAKE_GENERATOR` to override. `DUCKDB_EXTENSION_CONFIGS` is unsupported and fails fast.
- Use `cargo build -vv -F bundled-cmake` for CMake configure and build logs.

### Binding generation

`libduckdb-sys` ships pregenerated bindings for DuckDB's C API rather than running [bindgen](https://crates.io/crates/bindgen) at build time, which keeps build times down and avoids requiring Clang and the DuckDB header on every machine. To regenerate bindings at build time instead, enable the `buildtime_bindgen` feature.

## Rust version compatibility

duckdb-rs is built and tested with stable Rust and keeps a rolling MSRV that trails the current release by at least 6 months. The MSRV may only change when the encoded DuckDB major/minor version changes; patch releases keep the same MSRV.

## Contributing

Contributions are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md), and join the `#rust` channel on our [Discord](https://discord.gg/tcvwpjfnZx).

## License

Copyright (c) Stichting DuckDB Foundation. Licensed under the [MIT license](LICENSE).

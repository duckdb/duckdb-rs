# duckdb-rs

[![Latest Version](https://img.shields.io/crates/v/duckdb.svg)](https://crates.io/crates/duckdb)
[![Documentation](https://img.shields.io/badge/docs.rs-duckdb-orange)](https://docs.rs/duckdb)
[![MIT License](https://img.shields.io/crates/l/duckdb.svg)](LICENSE)
[![Downloads](https://img.shields.io/crates/d/duckdb.svg)](https://crates.io/crates/duckdb)
[![CI](https://github.com/duckdb/duckdb-rs/workflows/CI/badge.svg)](https://github.com/duckdb/duckdb-rs/actions)

> ⚠️ The V2 C API is in rapid development. Consider this branch unstable. Feedback is welcome!

duckdb-rs is an ergonomic Rust wrapper for [DuckDB](https://github.com/duckdb/duckdb), with an API inspired by [rusqlite](https://github.com/rusqlite/rusqlite). Use it to:

- Query DuckDB with type-safe bindings.
- ~~Read and write Arrow, Parquet, JSON, and CSV formats natively.~~
- ~~Build DuckDB extensions in Rust with custom scalar and table functions.~~

## Documentation
> ⚠️ The V2 Documentation is not live yet!

The **[DuckDB V1 Rust client guide](https://duckdb.org/docs/stable/clients/rust)** is the primary documentation:

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

> ⚠️ The V2 API is unstable - no packages are available in the registry for now. 

```shell
cargo add duckdb --git https://github.com/duckdb/duckdb-rs.git --rev <SHA>
```

Install the pinned DuckDB v2 library and `duckdb_v2.h` under
`~/.duckdb/lib/<ARTIFACT_VERSION>`, or set both `DUCKDB_LIB_DIR` and
`DUCKDB_INCLUDE_DIR` to a compatible installation. The optional `pkg-config`
feature enables system discovery instead. `libduckdb-sys` handles native linking
automatically.

For a shared library in the default `~/.duckdb` installation, add the application
helper to embed its runtime path on Linux/macOS, avoiding loader environment
variables:

```shell
cargo add duckdb-build --build --git https://github.com/duckdb/duckdb-rs.git --rev <SHA>
```

#### build.rs
```rust
fn main() -> Result<(), Box<dyn std::error::Error>> {
    duckdb_build::emit_dynamic_linking_flags()
}
```

Use the same revision and discovery configuration for both dependencies. If
using `pkg-config`, enable that feature on both `duckdb` and `duckdb-build`.
Windows still requires `duckdb.dll` beside the executable or on `PATH`;
macOS dylibs need an appropriate `@rpath` install name.

Static builds and loader-visible installations do not need the helper. The
`static` feature links an existing archive; it does not compile or download DuckDB.
See [installation and deployment options](crates/duckdb-build/README.md).

#### main.rs
```rust
use duckdb::{
    Parameters, Result,
    environment::{Environment, StorageLocation},
};


fn main() -> Result<()> {
    let env = Environment::new()?;
    let db = env.open(StorageLocation::InMemory);
    let conn = db?.connect()?;

    conn.execute("CREATE TABLE t1 as select * from range(0, 100) r(i)", Parameters::None)?;

    let query = conn.query("select * from t1", Parameters::None)?;

    for chunk in query {
        let chunk = chunk?;

        let vector = chunk.get_vector_at::<i64>(0)?;

        for row in vector.iter()? {
            println!("{:?}", row);
        }
    }

    Ok(())
}
```

~~See the [documentation](https://duckdb.org/docs/stable/clients/rust) for everything beyond this.~~

## Examples

Runnable examples live in [`crates/duckdb/examples`](crates/duckdb/examples), covering basic usage.

```shell
cargo run --example basic 001-json-to-duckdb-db
```

## Rust version compatibility

duckdb-rs is built and tested with stable Rust and keeps a rolling MSRV that trails the current release by at least 6 months. The MSRV may only change when the encoded DuckDB major/minor version changes; patch releases keep the same MSRV.

## Contributing

Contributions are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md), and join the `#rust` channel on our [Discord](https://discord.gg/tcvwpjfnZx).

## License

Copyright (c) Stichting DuckDB Foundation. Licensed under the [MIT license](LICENSE).

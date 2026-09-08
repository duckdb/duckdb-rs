# duckdb-build

Shared DuckDB installation discovery and runtime linking for Cargo build scripts.
This crate does not depend on, link, or download DuckDB.

## Installation discovery

By default, `libduckdb-sys` looks for the release pinned in
[`src/consts.rs`](src/consts.rs):

```text
~/.duckdb/lib/v2.0.0-alpha39998/
    duckdb_v2.h
    libduckdb.dylib | libduckdb.so | libduckdb_static.a
```
Alternatively, you can set the following variables to override this search:
```
export DUCKDB_INCLUDE_DIR=... # duckdb_v2.h location
export DUCKDB_LIB_DIR=... # libduckdb location
```

Static linking is also supported using the `static` feature flag. or using `LINK_DUCKDB_STATIC=true`

#### In case of missing libaries
You can install them using the official DuckDB install script:
```sh
curl https://install.duckdb.org | DUCKDB_INSTALL=dynamic sh 
```

## Local applications

Cargo does not propagate a dependency's runtime linker arguments into your
application. Add this helper as a **build dependency** of the package producing
the final executable.

In the application's `build.rs`:
```rust
fn main() -> std::io::Result<()> {
    duckdb_build::emit_runtime_path(duckdb_build::RuntimeLocation::Installed)
}
```

On Linux and macOS this embeds the absolute installed library directory. The
application runs outside Cargo without loader environment variables as long as
that installation remains available. `DUCKDB_LIB_DIR` overrides the default.

## Distributed applications

Choose the packaged library's location relative to the executable instead:

```rust
fn main() -> std::io::Result<()> {
    duckdb_build::emit_runtime_path(
        duckdb_build::RuntimeLocation::RelativeToExecutable("."),
    )
}
```

## Static builds
Static builds do not need runtime-path configuration; skip the helper when the
application enables `static`.

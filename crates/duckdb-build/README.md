# duckdb-build

Shared DuckDB installation discovery and runtime linking for Cargo build scripts.
This crate does not depend on, link, or download DuckDB.

## Installation discovery

By default, `libduckdb-sys` looks for the release pinned in
[`src/consts.rs`](src/consts.rs):

```text
~/.duckdb/lib/v2.0.0-alpha39998/
    duckdb_v2.h
    libduckdb.dylib       # macOS
    libduckdb.so          # Linux
    duckdb.dll           # Windows runtime library
    duckdb.lib           # Windows MSVC import library
```

Only your target platform's files are required. Windows GNU targets use
`libduckdb.dll.a` as the import library. Static linking requires
`libduckdb_static.a` (`duckdb_static.lib` for MSVC) instead of the shared library.
The `static` feature selects that installed static archive; it does not compile
DuckDB from source.

Enable it with `--features static`. `LINK_DUCKDB_STATIC=true` also selects static
linking for builds configured through the environment.

No environment variables are needed for the default installation. Override
`DUCKDB_LIB_DIR` and `DUCKDB_INCLUDE_DIR` independently to use other library or
header directories. A leading `~` or `~/` expands to your home directory, including
in quoted values such as `DUCKDB_LIB_DIR="~/duckdb"`. Other relative overrides are
resolved against absolute `PWD`, or the build script's working directory if `PWD`
is unavailable. Resolved directories are canonicalized before use. Prefer
absolute overrides, especially on Windows and when cross-compiling.

The helper validates that the required files exist, not their embedded version
or checksum. The release directory selects the version, and the expected commit
SHA is included in diagnostics. Always install matching headers and libraries.
For cross-compilation, overrides must point to artifacts for Cargo's **target**,
not the machine running the build script.

Missing installations or artifacts report the expected release and this command:

```sh
curl https://install.duckdb.org | sh
```

The installer command is currently a placeholder for the installation workflow;
ensure the pinned alpha's header and libraries are present at the expected path.
Cargo builds do not invoke it automatically. There is no implicit `.env`,
pkg-config, or vcpkg discovery; export overrides into the build environment.

## Local applications

Cargo does not propagate a dependency's runtime linker arguments into your
application. Add this helper as a **build dependency** of the package producing
the final executable:

```toml
[dependencies]
duckdb-rs = "=2.0.0"

[build-dependencies]
duckdb-build = "=2.0.0"
```

Keep the helper version aligned with `libduckdb-sys` so both use the same pin.
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

Use `"."` for a library beside the executable, `"../lib"` for a `bin/` and `lib/`
layout, or another relative directory. This emits `$ORIGIN/<directory>` on
Linux/Android and `@loader_path/<directory>` on macOS/iOS, without embedding the
build machine's installation path. With a shared-library target, those tokens
are relative to that shared library.

Compilation still needs the installed headers and library. Copy the library and
any required shared-library dependencies into the package separately; the helper
does not copy them. Headers are not needed at runtime.

On macOS the DuckDB dylib must have an `@rpath` install name, such as
`@rpath/libduckdb.dylib`. Correct absolute install names before linking and sign
the final package afterward.

Windows has no rpath. The helper emits no runtime arguments there: place
`duckdb.dll` beside the executable or put its directory on `PATH`, for both local
and distributed applications.

Static builds do not need runtime-path configuration; skip the helper when the
application enables `static`.

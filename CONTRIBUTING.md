# Contributing

Thank you for your interest in contributing to duckdb-rs. This guide describes how to set up and develop the project.

## Background

This workspace has three crates:

- `libduckdb-sys` provides native bindings for the [DuckDB C API](https://duckdb.org/docs/current/clients/c/api).
- `duckdb` provides an ergonomic wrapper around `libduckdb-sys`.
- `duckdb-loadable-macros` provides procedural macros for loadable DuckDB extensions.

Most users should use `duckdb`, but development may involve any of these components.

## Prerequisites

### Required for `buildtime_bindgen` feature

When using the `buildtime_bindgen` feature, you need to have `libclang` installed on your system, as it's required by the `bindgen` crate to generate Rust bindings from C headers.

**Linux (Debian/Ubuntu):**
```shell
sudo apt-get update
sudo apt-get install -y libclang-dev
```

**macOS:**
```shell
brew install llvm
```

**Windows:**
Install LLVM from [llvm.org](https://releases.llvm.org/) or use the installer, and ensure the binaries are in your PATH.

### DuckDB Library and Header Setup

When using `DUCKDB_LIB_DIR` and `DUCKDB_INCLUDE_DIR`, put the header and the library files for your platform directly in those directories. For example:

```
~/duckdb-lib/
├── duckdb.h           # All platforms
├── libduckdb.so       # Linux
├── libduckdb.dylib    # macOS
├── duckdb.dll         # Windows runtime library
└── duckdb.lib         # Windows MSVC import library
```

Only the files for your platform are required. The header file `duckdb.h` must be directly in `DUCKDB_INCLUDE_DIR`, not in a `duckdb/` subdirectory. Windows MSVC builds require both `duckdb.dll` and its `duckdb.lib` import library.

## Development

### duckdb-c-api
Some features are still not implemented in the c api, so we may need pull request in the [duckdb repo](https://github.com/duckdb/duckdb).

```shell
# build duckdb
cd ~/github/
git clone git@github.com:duckdb/duckdb.git
cd duckdb
GEN=ninja make debug
```

Related logics:
* header file: https://github.com/duckdb/duckdb/blob/main/src/include/duckdb.h
* impl file: https://github.com/duckdb/duckdb/blob/main/src/main/capi/duckdb-c.cpp
* test file: https://github.com/duckdb/duckdb/blob/main/test/api/capi/test_capi.cpp
* You can refer to one of my previous PR: https://github.com/duckdb/duckdb/pull/1923

After make the change, we can build the repo and use it in `duckdb-rs` by:
```shell
# export library and header file
cd ~/github/duckdb
mkdir ~/duckdb-lib

# Copy header and library to the same directory
# Note: duckdb.h must be directly in ~/duckdb-lib/, not in a subdirectory

# macOS:
cp src/include/duckdb.h build/debug/src/libduckdb.dylib ~/duckdb-lib/

# Linux:
cp src/include/duckdb.h build/debug/src/libduckdb.so ~/duckdb-lib/

# Windows:
cp src/include/duckdb.h build/debug/src/duckdb.dll build/debug/src/duckdb.lib ~/duckdb-lib/

# set lib dir
export DUCKDB_LIB_DIR=~/duckdb-lib

# set header dir (can be the same as LIB_DIR)
export DUCKDB_INCLUDE_DIR=~/duckdb-lib

# Make the dynamic library available when tests run. Use the command for your platform.

# Linux:
export LD_LIBRARY_PATH="$DUCKDB_LIB_DIR${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"

# macOS:
export DYLD_FALLBACK_LIBRARY_PATH="$DUCKDB_LIB_DIR${DYLD_FALLBACK_LIBRARY_PATH:+:$DYLD_FALLBACK_LIBRARY_PATH}"

# Windows (Git Bash):
export PATH="$DUCKDB_LIB_DIR:$PATH"
```

### libduckdb-sys

Use the exported library and header:

```shell
# Ensure environment variables are set
export DUCKDB_LIB_DIR=~/duckdb-lib
export DUCKDB_INCLUDE_DIR=~/duckdb-lib

cd ~/github/duckdb-rs/crates/libduckdb-sys
cargo test --features buildtime_bindgen
```

**Note:** Make sure `libclang` is installed (see Prerequisites section above) when using the `buildtime_bindgen` feature.

Use the bundled DuckDB source and pregenerated bindings:
```shell
cd ~/github/duckdb-rs/crates/libduckdb-sys
cargo test --features bundled
```

CI exercises downloaded DuckDB libraries on Linux and Windows, the `bundled` backend through the Linux feature checks, and the `bundled-cmake` backend in its dedicated job.

To test an upstream DuckDB C API change before it is tagged, run the top-level upgrade script with the full upstream commit SHA:

```shell
cd ~/github/duckdb-rs
./upgrade.sh --sha <COMMIT_SHA>
```

This checks out that commit in the `duckdb-sources` submodule, regenerates `duckdb.tar.gz`, and regenerates both sets of pregenerated bindings. It does not run tests; validate the `bundled` and `bundled-cmake` backends afterward.

### duckdb-rs

Use the exported library and header:

```shell
# Ensure environment variables are set
export DUCKDB_LIB_DIR=~/duckdb-lib
export DUCKDB_INCLUDE_DIR=~/duckdb-lib

cd ~/github/duckdb-rs/
cargo test --features buildtime_bindgen -- --nocapture
```

**Note:** Make sure `libclang` is installed (see Prerequisites section above) when using the `buildtime_bindgen` feature.

Use the bundled DuckDB source and pregenerated bindings:

```shell
cd ~/github/duckdb-rs
cargo test --features bundled -- --nocapture
```

Run AddressSanitizer for Rust code on x86-64 Linux:
```shell
cd ~/github/duckdb-rs
RUSTFLAGS="-Zsanitizer=address -C debuginfo=0" \
RUSTDOCFLAGS="-Zsanitizer=address" \
ASAN_OPTIONS="detect_stack_use_after_return=1:detect_leaks=1:symbolize=1" \
cargo +nightly test --lib --tests --features bundled \
  --target x86_64-unknown-linux-gnu --package duckdb
```

Install a nightly Rust toolchain first if necessary. `RUSTFLAGS` does not instrument the bundled C++ sources compiled by the `cc` crate, so this command covers the Rust side of the integration.

### Update to a new version

When DuckDB releases a new version, duckdb-rs needs a matching release.
Prepare normal releases from `main`. For LTS releases that stay on DuckDB 1.4 Andium, use the `v1.4-andium` branch.

Use the top-level upgrade script for DuckDB version updates:

```shell
./upgrade.sh
```

This updates the workspace crate version, the exact workspace dependency pins
that keep sibling crates in lockstep, README `Cargo.toml` examples, and DuckDB
version references such as workflow tags and README download URLs. It then calls
`./crates/libduckdb-sys/upgrade.sh` to regenerate bindings.

DuckDB's C API may occasionally have breaking changes, so version updates may
also require code fixes.

For a duckdb-rs patch release that does not change the bundled DuckDB version:

```shell
./upgrade.sh --patch
```

Patch releases only update crate versions, exact workspace dependency pins,
README `Cargo.toml` examples, and `Cargo.lock`. They do not update the DuckDB
submodule, generated bindings, or DuckDB download tags.

#### Testing a pre-tag DuckDB commit

To integrate a DuckDB version before it is tagged, pass a commit SHA to `--sha`
(e.g. a release-branch commit). The `bundled` build compiles from source, so no
published release binaries are needed.

```shell
./upgrade.sh --sha <COMMIT_SHA>          # pin a commit, no version bump
./upgrade.sh v1.5.4 --sha <COMMIT_SHA>   # planned release, sources pinned to commit
```

Both regenerate bundled sources and bindings, but do not run tests. Validate
locally, or rely on the nightly workflow and CI to cover the bundled backends.
Download-based CI jobs (`DUCKDB_DOWNLOAD_LIB`, the Windows release zip) stay red
until DuckDB publishes the release binaries. If the planned release command
above already bumped versions and download URLs, finalize the bundled sources
and bindings from the tag with `./crates/libduckdb-sys/upgrade.sh v1.5.4`.

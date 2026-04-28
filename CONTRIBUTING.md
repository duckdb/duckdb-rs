# Contributing
Glad your are interested in this project. Here I'll share how I develop this library, hopefully it will be helpfull for you to get start.

## Background
This repo has two crates: `duckdb` and `libduckdb-sys`, which `libduckdb-sys` is the original binding for [duckdb-c-api](https://github.com/duckdb/duckdb/blob/master/src/include/duckdb.h) and `duckdb` is an ergonomic wrapper on `libduckdb-sys`.

Most user should use `duckdb`, but our development may happen in both of these components.

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

When using `DUCKDB_LIB_DIR` and `DUCKDB_INCLUDE_DIR` environment variables, ensure the following folder structure:

```
~/duckdb-lib/
├── duckdb.h          # Header file (must be directly in the directory, not in a subdirectory)
└── libduckdb.so      # Library file (or .dylib on macOS, .dll on Windows)
```

**Important:** The header file `duckdb.h` must be placed directly in the directory specified by `DUCKDB_INCLUDE_DIR`, not in a `duckdb/` subdirectory. The build system expects the header at `{DUCKDB_INCLUDE_DIR}/duckdb.h`.

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
cp src/include/duckdb.h build/debug/src/duckdb.dll ~/duckdb-lib/

# set lib dir
export DUCKDB_LIB_DIR=~/duckdb-lib

# set header dir (can be the same as LIB_DIR)
export DUCKDB_INCLUDE_DIR=~/duckdb-lib
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

Use the bundled header file:
```shell
cd ~/github/duckdb-rs/crates/libduckdb-sys
cargo test --features bundled
```

Currently in [github actions](https://github.com/duckdb/duckdb-rs/actions), we always use the bundled file for testing. So if you change the header in duckdb-cpp repo, you need to make the PR merged and updated the [`duckdb-sources` submodule](https://github.com/duckdb/duckdb-rs/tree/main/crates/libduckdb-sys).
You can generated the amalgamated file by:

```shell
cd ~/github/duckdb
mkdir -p build/amaldebug
python scripts/amalgamation.py
cp src/amalgamation/duckdb.cpp src/include/duckdb.h src/amalgamation/duckdb.hpp ../duckdb-rs/crates/libduckdb-sys/duckdb-sources/
```

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

Use the bundled header file:

```shell
cd ~/github/duckdb-rs
cargo test --features bundled -- --nocapture
```

Detect memory leaks:
```shell
cd ~/github/duckdb-rs
ASAN_OPTIONS=detect_leaks=1 ASAN_SYMBOLIZER_PATH=/usr/local/opt/llvm/bin/llvm-symbolizer cargo test --features bundled -- --nocapture
```

### Update to new version

Everytime duckdb release to a new version, we also need to release a new version.

We can use the scripts to do the upgrades:
```shell
./upgrade.sh
```
Which use sed to update the version number and then call `./libduckdb-sys/upgrade.sh` to generated new bindings.

We may need to fix any error as duckdb's c-api may have breaking changes occasionally.

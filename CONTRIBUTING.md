# Contributing
Glad your are interested in this project. Here I'll share how I develop this library, hopefully it will be helpfull for you to get start.

## Background
This repo has two crates: `duckdb` and `libduckdb-sys`, which `libduckdb-sys` is the original binding for [duckdb-c-api](https://github.com/duckdb/duckdb/blob/master/src/include/duckdb.h) and `duckdb` is an ergonomic wrapper on `libduckdb-sys`.

Most user should use `duckdb`, but our development may happen in both of these components.

## Development

### duckdb-c-api
Some features are still not implemented in the c api, so we may need pull request in the [duckdb repo]((https://github.com/duckdb/duckdb).

```shell
# build duckdb
cd ~/github/
git clone git@github.com:duckdb/duckdb.git
cd duckdb
GEN=ninja make debug
```

Related logics:
* header file: https://github.com/duckdb/duckdb/blob/master/src/include/duckdb.h
* impl file: https://github.com/duckdb/duckdb/blob/master/src/main/duckdb-c.cpp
* test file: https://github.com/duckdb/duckdb/blob/master/test/api/capi/test_capi.cpp
* You can refer to one of my previous PR: https://github.com/duckdb/duckdb/pull/1923

After make the change, we can build the repo and use it in `duckdb-rs` by:
```shell
# assume in macOS, you may need to change the file in other OS
# export library and header file
cd ~/github/duckdb
mkdir ~/duckdb-lib
cp src/include/duckdb.h build/debug/src/libduckdb.dylib ~/duckdb-lib/
# set lib dir
export DUCKDB_LIB_DIR=~/duckdb-lib
# set header dir
export DUCKDB_INCLUDE_DIR=~/duckdb-lib
```

### libduckdb-sys

Use the exported library and header:

```shell
cd ~/github/duckdb-rs/libduckdb-sys
cargo test --features buildtime_bindgen
```

Use the bundled header file:
```shell
cd ~/github/duckdb-rs/libduckdb-sys
cargo test --features bundled
```

Currently in [github actions](https://github.com/wangfenjin/duckdb-rs/actions), we always use the bundled file for testing. So if you change the header in duckdb-cpp repo, you need to make the PR merged and updated the [bundled-file](https://github.com/wangfenjin/duckdb-rs/tree/main/libduckdb-sys/duckdb).
You can generated the amalgamated file by:

```shell
cd ~/github/duckdb
mkdir -p build/amaldebug
python scripts/amalgamation.py
cp src/amalgamation/duckdb.cpp src/include/duckdb.h src/amalgamation/duckdb.hpp ../duckdb-rs/libduckdb-sys/duckdb/
```

### duckdb-rs

Use the exported library and header:

```shell
cd ~/github/duckdb-rs/
cargo test --features buildtime_bindgen -- --nocapture
```

Use the bundled header file:

```shell
cd ~/github/duckdb-rs
cargo test --features bundled -- --nocapture
```
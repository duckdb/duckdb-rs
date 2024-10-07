#!/bin/bash

set -e

SCRIPT=$(realpath "$0")
SCRIPT_DIR=$(dirname "$SCRIPT")

echo "$SCRIPT_DIR"
cd "$SCRIPT_DIR"
cargo clean
mkdir -p "$SCRIPT_DIR/../../target" "$SCRIPT_DIR/duckdb"
export DUCKDB_LIB_DIR="$SCRIPT_DIR/duckdb"

# Download and extract amalgamation
DUCKDB_VERSION=v1.1.1
git submodule update --init --checkout
cd "$SCRIPT_DIR/duckdb-sources"
git fetch
git checkout "$DUCKDB_VERSION"
cd "$SCRIPT_DIR"
python3 "$SCRIPT_DIR/update_sources.py"

# Regenerate bindgen file for DUCKDB
cd "$SCRIPT_DIR"
rm -f "$SCRIPT_DIR/src/bindgen_bundled_version_loadable.rs"
find "$SCRIPT_DIR/../../target" -type f -name bindgen.rs -exec rm {} \;
cargo build --features "extensions-full buildtime_bindgen loadable-extension"
find "$SCRIPT_DIR/../../target" -type f -name bindgen.rs -exec cp {} "$SCRIPT_DIR/src/bindgen_bundled_version_loadable.rs" \;

# Sanity checks
# FIXME: how to test this here?

# Regenerate bindgen file for DUCKDB
rm -f "$SCRIPT_DIR/src/bindgen_bundled_version.rs"
# Just to make sure there is only one bindgen.rs file in target dir
find "$SCRIPT_DIR/../../target" -type f -name bindgen.rs -exec rm {} \;
cargo build --features "extensions-full buildtime_bindgen"
find "$SCRIPT_DIR/../../target" -type f -name bindgen.rs -exec cp {} "$SCRIPT_DIR/src/bindgen_bundled_version.rs" \;

# Sanity checks
cd "$SCRIPT_DIR/.."
cargo test --features "extensions-full buildtime_bindgen"

printf '    \e[35;1mFinished\e[0m bundled DUCKDB tests\n'

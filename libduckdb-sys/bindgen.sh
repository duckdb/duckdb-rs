#!/bin/bash -e

SCRIPT_DIR=$(cd "$(dirname "$_")" && pwd)
CUR_DIR=$(pwd -P)
echo "$SCRIPT_DIR"
cd "$SCRIPT_DIR" || { echo "fatal error" >&2; exit 1; }
cargo clean
mkdir -p "$SCRIPT_DIR/../target" "$SCRIPT_DIR/duckdb"
export DUCKDB_LIB_DIR="$SCRIPT_DIR/duckdb"
export DU_INCLUDE_DIR="$DUCKDB_LIB_DIR"

# Download and extract amalgamation
# DUCKDB_VERSION=v0.2.6
# wget -T 20 "https://github.com/duckdb/duckdb/releases/download/$DUCKDB_VERSION/libduckdb-src.zip"
# unzip -o libduckdb-src.zip -d duckdb
# rm -f libduckdb-src.zip

# Regenerate bindgen file for DUCKDB
rm -f "$DUCKDB_LIB_DIR/bindgen_bundled_version.rs"
cargo update
# Just to make sure there is only one bindgen.rs file in target dir
find "$SCRIPT_DIR/../target" -type f -name bindgen.rs -exec rm {} \;
env LIBDUCKDB_SYS_BUNDLING=1 cargo test --features "bundled buildtime_bindgen"
find "$SCRIPT_DIR/../target" -type f -name bindgen.rs -exec cp {} "$DUCKDB_LIB_DIR/bindgen_bundled_version.rs" \;

# Sanity checks
cd "$SCRIPT_DIR/.." || { echo "fatal error" >&2; exit 1; }
cargo update
cargo test --features "bundled buildtime_bindgen"
printf '    \e[35;1mFinished\e[0m bundled DUCKDB tests\n'

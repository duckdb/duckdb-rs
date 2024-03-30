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
DUCKDB_VERSION=v0.10.1
git submodule update --init --checkout
cd "$SCRIPT_DIR/duckdb-sources" || { echo "fatal error" >&2; exit 1; }
git fetch
git checkout "$DUCKDB_VERSION"
cd "$SCRIPT_DIR" || { echo "fatal error" >&2; exit 1; }
python3 "$SCRIPT_DIR/update_sources.py"

# Regenerate bindgen file for DUCKDB
rm -f "$SCRIPT_DIR/src/bindgen_bundled_version.rs"
cargo update
# Just to make sure there is only one bindgen.rs file in target dir
find "$SCRIPT_DIR/../target" -type f -name bindgen.rs -exec rm {} \;
env LIBDUCKDB_SYS_BUNDLING=1 cargo test --features "extensions-full buildtime_bindgen"
find "$SCRIPT_DIR/../target" -type f -name bindgen.rs -exec cp {} "$SCRIPT_DIR/src/bindgen_bundled_version.rs" \;

# Sanity checks
cd "$SCRIPT_DIR/.." || { echo "fatal error" >&2; exit 1; }
cargo update
cargo test --features "extensions-full buildtime_bindgen"
printf '    \e[35;1mFinished\e[0m bundled DUCKDB tests\n'

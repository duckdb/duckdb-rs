#!/bin/bash

set -e -o pipefail

SCRIPT=$(realpath "$0")
SCRIPT_DIR=$(dirname "$SCRIPT")
WORKSPACE_DIR=$(realpath "$SCRIPT_DIR/../..")

echo "$SCRIPT_DIR"
cd "$SCRIPT_DIR"

current_workspace_version() {
    grep '^version = "' "$WORKSPACE_DIR/Cargo.toml" | head -n1 | sed -E 's/version = "([^"]+)"/\1/'
}

crate_version_to_duckdb_version() {
    local CRATE_VERSION=$1
    local ENCODED
    ENCODED=$(echo "$CRATE_VERSION" | cut -d. -f2)
    local DUCKDB_MAJOR=$((ENCODED / 10000))
    local DUCKDB_MINOR=$(((ENCODED / 100) % 100))
    local DUCKDB_PATCH=$((ENCODED % 100))
    printf 'v%s.%s.%s' "$DUCKDB_MAJOR" "$DUCKDB_MINOR" "$DUCKDB_PATCH"
}

cargo clean
mkdir -p "$SCRIPT_DIR/../../target" "$SCRIPT_DIR/duckdb"
export DUCKDB_LIB_DIR="$SCRIPT_DIR/duckdb"

# Download and extract amalgamation
DUCKDB_VERSION=${1:-$(crate_version_to_duckdb_version "$(current_workspace_version)")}
DUCKDB_VERSION="v${DUCKDB_VERSION#v}"
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
if [ ! -f "$SCRIPT_DIR/src/bindgen_bundled_version_loadable.rs" ]; then
    echo "ERROR: bindgen_bundled_version_loadable.rs was not regenerated" >&2
    exit 1
fi

# Sanity checks
# FIXME: how to test this here?

# Regenerate bindgen file for DUCKDB
rm -f "$SCRIPT_DIR/src/bindgen_bundled_version.rs"
# Just to make sure there is only one bindgen.rs file in target dir
find "$SCRIPT_DIR/../../target" -type f -name bindgen.rs -exec rm {} \;
cargo build --features "extensions-full buildtime_bindgen"
find "$SCRIPT_DIR/../../target" -type f -name bindgen.rs -exec cp {} "$SCRIPT_DIR/src/bindgen_bundled_version.rs" \;
if [ ! -f "$SCRIPT_DIR/src/bindgen_bundled_version.rs" ]; then
    echo "ERROR: bindgen_bundled_version.rs was not regenerated" >&2
    exit 1
fi

# Sanity checks
cd "$SCRIPT_DIR/.."
cargo test --features "extensions-full buildtime_bindgen"

printf '    \e[35;1mFinished\e[0m bundled DUCKDB tests\n'

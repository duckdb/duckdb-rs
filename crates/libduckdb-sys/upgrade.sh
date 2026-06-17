#!/bin/bash

set -e -o pipefail

usage() {
    cat <<'EOF'
Usage:
  ./crates/libduckdb-sys/upgrade.sh [DUCKDB_VERSION]

Without arguments, regenerate bindings for the version pinned in this script.
With DUCKDB_VERSION, regenerate bindings for that DuckDB release. The version
may be specified with or without a leading "v", e.g. "1.4.5" or "v1.4.5".
EOF
}

valid_duckdb_version() {
    local version=${1#v}
    [[ "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]
}

if [ "${1:-}" = "-h" ] || [ "${1:-}" = "--help" ]; then
    usage
    exit 0
fi

if [ $# -gt 1 ]; then
    usage >&2
    exit 1
fi

# Download and extract amalgamation
DUCKDB_VERSION=${1:-v1.4.4}
if ! valid_duckdb_version "$DUCKDB_VERSION"; then
    echo "Invalid DuckDB version: $DUCKDB_VERSION" >&2
    usage >&2
    exit 1
fi
DUCKDB_VERSION="v${DUCKDB_VERSION#v}"

SCRIPT=$(realpath "$0")
SCRIPT_DIR=$(dirname "$SCRIPT")

echo "$SCRIPT_DIR"
cd "$SCRIPT_DIR"
cargo clean
mkdir -p "$SCRIPT_DIR/../../target" "$SCRIPT_DIR/duckdb"
export DUCKDB_LIB_DIR="$SCRIPT_DIR/duckdb"

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

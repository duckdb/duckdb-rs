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

fetch_duckdb_sha() {
    local SHA=$1
    if ! git fetch origin "$SHA"; then
        echo "Failed to fetch DuckDB commit $SHA from origin." >&2
        echo "Ensure the commit is reachable from an advertised remote ref." >&2
        exit 1
    fi
}

# Parse args before doing expensive regeneration work.
DUCKDB_SHA=""
POSITIONAL=()
while [ $# -gt 0 ]; do
    case "$1" in
        --sha)
            if [ -z "${2:-}" ]; then
                echo "--sha requires a commit SHA" >&2
                exit 1
            fi
            DUCKDB_SHA=$2
            shift 2
            ;;
        *)
            POSITIONAL+=("$1")
            shift
            ;;
    esac
done

if [ -n "$DUCKDB_SHA" ] && ! [[ "$DUCKDB_SHA" =~ ^[0-9a-fA-F]{40}$ ]]; then
    echo "Invalid commit SHA: $DUCKDB_SHA (expected 40 hex characters)" >&2
    exit 1
fi

cargo clean
mkdir -p "$SCRIPT_DIR/../../target" "$SCRIPT_DIR/duckdb"
export DUCKDB_LIB_DIR="$SCRIPT_DIR/duckdb"

DUCKDB_VERSION=${POSITIONAL[0]:-$(crate_version_to_duckdb_version "$(current_workspace_version)")}
DUCKDB_VERSION="v${DUCKDB_VERSION#v}"

if [ -n "$DUCKDB_SHA" ]; then
    echo "Pinning DuckDB sources to commit $DUCKDB_SHA (version $DUCKDB_VERSION)"
fi

git submodule update --init --checkout
cd "$SCRIPT_DIR/duckdb-sources"
if [ -n "$DUCKDB_SHA" ]; then
    fetch_duckdb_sha "$DUCKDB_SHA"
    DUCKDB_TARGET=$DUCKDB_SHA
else
    git fetch --all --tags
    DUCKDB_TARGET=$DUCKDB_VERSION
fi
git checkout "$DUCKDB_TARGET"
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

printf '    \e[35;1mFinished\e[0m regenerating bundled DUCKDB sources and bindings (tests not run)\n'

#!/bin/bash

set -e -o pipefail

sed_inplace() {
    if sed --version 2>/dev/null | grep -q GNU; then
        sed -i "$@"
    else
        sed -i '' "$@"
    fi
}

get_latest_release() {
    local REPO=$1
    gh api "repos/$REPO/releases/latest" --jq '.tag_name'
}

current_workspace_version() {
    grep '^version = "' Cargo.toml | head -n1 | sed -E 's/version = "([^"]+)"/\1/'
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

duckdb_version_to_crate_version() {
    local DUCKDB_VERSION=${1#v}
    local CRATE_MAJOR=$2
    local DUCKDB_MAJOR DUCKDB_MINOR DUCKDB_PATCH
    IFS=. read -r DUCKDB_MAJOR DUCKDB_MINOR DUCKDB_PATCH <<<"$DUCKDB_VERSION"
    printf '%s.%d%02d%02d.0' "$CRATE_MAJOR" "$DUCKDB_MAJOR" "$DUCKDB_MINOR" "$DUCKDB_PATCH"
}

DUCKDB_VERSION=${1:-$(get_latest_release "duckdb/duckdb")}
DUCKDB_VERSION="v${DUCKDB_VERSION#v}"
CURRENT_CRATE_VERSION=$(current_workspace_version)
CURRENT_DUCKDB_VERSION=$(crate_version_to_duckdb_version "$CURRENT_CRATE_VERSION")
CRATE_MAJOR=$(echo "$CURRENT_CRATE_VERSION" | cut -d. -f1)
TARGET_CRATE_VERSION=$(duckdb_version_to_crate_version "$DUCKDB_VERSION" "$CRATE_MAJOR")
CURRENT_CRATE_VERSION_PATTERN=${CURRENT_CRATE_VERSION//./\\.}
CURRENT_DUCKDB_VERSION_PATTERN=${CURRENT_DUCKDB_VERSION//./\\.}

if [ "$DUCKDB_VERSION" = "$CURRENT_DUCKDB_VERSION" ]; then
    echo "Already up to date, latest DuckDB version is $DUCKDB_VERSION and workspace version is $CURRENT_CRATE_VERSION"
    exit 0
fi

echo "Start to upgrade DuckDB from $CURRENT_DUCKDB_VERSION to $DUCKDB_VERSION"
echo "Update crate version from $CURRENT_CRATE_VERSION to $TARGET_CRATE_VERSION"

sed_inplace "s!$CURRENT_CRATE_VERSION_PATTERN!$TARGET_CRATE_VERSION!g" \
    Cargo.toml \
    crates/duckdb/Cargo.toml \
    crates/libduckdb-sys/Cargo.toml \
    crates/duckdb-loadable-macros/Cargo.toml

sed_inplace "s!$CURRENT_DUCKDB_VERSION_PATTERN!$DUCKDB_VERSION!g" \
    .github/workflows/rust.yaml

# Update README: only Cargo.toml examples and download URLs, not prose/history.
sed_inplace "/version = \"/s!$CURRENT_CRATE_VERSION_PATTERN!$TARGET_CRATE_VERSION!g" README.md
sed_inplace "/releases\/download/s!$CURRENT_DUCKDB_VERSION_PATTERN!$DUCKDB_VERSION!g" README.md

# Let Cargo rewrite Cargo.lock from the updated manifests instead of editing it as text.
cargo metadata --format-version 1 >/dev/null

exec ./crates/libduckdb-sys/upgrade.sh "$DUCKDB_VERSION"

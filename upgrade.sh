#!/bin/bash

set -e -o pipefail

sed_inplace() {
    if sed --version 2>/dev/null | grep -q GNU; then
        sed -i "$@"
    else
        sed -i '' "$@"
    fi
}

usage() {
    cat <<'EOF'
Usage:
  ./upgrade.sh [DUCKDB_VERSION]

Without arguments, upgrade to the latest DuckDB release.
With DUCKDB_VERSION, upgrade to that DuckDB release. The version may be
specified with or without a leading "v", e.g. "1.4.5" or "v1.4.5".
EOF
}

# https://gist.github.com/lukechilds/a83e1d7127b78fef38c2914c4ececc3c
# Usage
# $ get_latest_release "duckdb/duckdb"
get_latest_release() {
    curl -fSs "https://api.github.com/repos/$1/releases/latest" | # Get latest release from GitHub api
      grep '"tag_name":' |                                            # Get tag line
      sed -E 's/.*"v([^"]+)".*/\1/'                                   # Pluck JSON value
}

current_workspace_version() {
    grep '^version = "' Cargo.toml | head -n1 | sed -E 's/version = "([^"]+)"/\1/'
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

duckdb_version=${1:-$(get_latest_release "duckdb/duckdb")}
if ! valid_duckdb_version "$duckdb_version"; then
    echo "Invalid DuckDB version: $duckdb_version" >&2
    usage >&2
    exit 1
fi
duckdb_version=${duckdb_version#v}
duckdb_rs_version=$(current_workspace_version)

if [ "$duckdb_version" = "$duckdb_rs_version" ]; then
    echo "Already up to date, current workspace version is $duckdb_rs_version"
    exit 0
fi

echo "Start to upgrade from $duckdb_rs_version to $duckdb_version"

duckdb_rs_version_pattern=${duckdb_rs_version//./\\.}
sed_inplace "s/$duckdb_rs_version_pattern/$duckdb_version/g" \
    Cargo.toml \
    crates/duckdb/Cargo.toml \
    crates/libduckdb-sys/upgrade.sh \
    crates/libduckdb-sys/Cargo.toml \
    .github/workflows/rust.yaml \
    README.md

exec ./crates/libduckdb-sys/upgrade.sh "$duckdb_version"

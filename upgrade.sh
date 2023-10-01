#!/bin/bash

## How to run
##   `./upgrade.sh`

# https://gist.github.com/lukechilds/a83e1d7127b78fef38c2914c4ececc3c
# Usage
# $ get_latest_release "duckdb/duckdb"
get_latest_release() {
    curl --silent "https://api.github.com/repos/$1/releases/latest" | # Get latest release from GitHub api
      grep '"tag_name":' |                                            # Get tag line
      sed -E 's/.*"v([^"]+)".*/\1/'                                   # Pluck JSON value
}

duckdb_version=$(get_latest_release "duckdb/duckdb")
duckdb_rs_version=$(get_latest_release "duckdb/duckdb-rs")

if [ $duckdb_version = $duckdb_rs_version ]; then
    echo "Already update to date, latest version is $duckdb_version"
    exit 0
fi

echo "Start to upgrade from $duckdb_version to $duckdb_rs_version"

sed -i '' "s/$duckdb_rs_version/$duckdb_version/g" Cargo.toml libduckdb-sys/upgrade.sh libduckdb-sys/Cargo.toml .github/workflows/rust.yaml
./libduckdb-sys/upgrade.sh

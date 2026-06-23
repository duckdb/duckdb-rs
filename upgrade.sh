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

usage() {
    cat <<'EOF'
Usage:
  ./upgrade.sh [DUCKDB_VERSION]
  ./upgrade.sh DUCKDB_VERSION --sha COMMIT_SHA
  ./upgrade.sh --sha COMMIT_SHA
  ./upgrade.sh --patch

Without arguments, upgrade to the latest DuckDB release.
With DUCKDB_VERSION, upgrade bundled DuckDB and reset the crate patch to 0.
With DUCKDB_VERSION and --sha, pin the bundled DuckDB sources to COMMIT_SHA
(e.g. a pre-tag release-branch commit) while still bumping to the planned
DUCKDB_VERSION. With --sha alone, pin sources to COMMIT_SHA and regenerate for
the current crate version without bumping versions or download URLs (for
testing arbitrary upstream commits).
Full upgrades also update workflow and README download URLs and regenerate
bindings. With --patch, increment only the duckdb-rs crate patch version and
leave DuckDB sources, generated bindings, and DuckDB download tags unchanged.
EOF
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

valid_duckdb_version() {
    local DUCKDB_VERSION=${1#v}
    [[ "$DUCKDB_VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]
}

next_crate_patch_version() {
    local CRATE_VERSION=$1
    local MAJOR ENCODED PATCH
    IFS=. read -r MAJOR ENCODED PATCH <<<"$CRATE_VERSION"
    if [[ ! "$MAJOR" =~ ^[0-9]+$ || ! "$ENCODED" =~ ^[0-9]+$ || ! "$PATCH" =~ ^[0-9]+$ ]]; then
        echo "Invalid current crate version: $CRATE_VERSION" >&2
        exit 1
    fi
    printf '%s.%s.%s' "$MAJOR" "$ENCODED" "$((10#$PATCH + 1))"
}

update_crate_versions() {
    cargo metadata --format-version 1 >/dev/null

    sed_inplace "s!$CURRENT_CRATE_VERSION_PATTERN!$TARGET_CRATE_VERSION!g" \
        Cargo.toml \
        crates/duckdb/Cargo.toml \
        crates/libduckdb-sys/Cargo.toml \
        crates/duckdb-loadable-macros/Cargo.toml

    # Update README Cargo.toml examples, not prose/history.
    sed_inplace "/version = \"/s!$CURRENT_CRATE_VERSION_PATTERN!$TARGET_CRATE_VERSION!g" README.md

    local UPDATED_CRATE_VERSION
    UPDATED_CRATE_VERSION=$(current_workspace_version)
    if [ "$UPDATED_CRATE_VERSION" != "$TARGET_CRATE_VERSION" ]; then
        echo "Expected workspace version $TARGET_CRATE_VERSION, found $UPDATED_CRATE_VERSION" >&2
        exit 1
    fi

    # Let Cargo rewrite Cargo.lock from the updated manifests instead of editing it as text.
    cargo metadata --format-version 1 >/dev/null
}

PATCH_MODE=0
DUCKDB_SHA=""
POSITIONAL=()

while [ $# -gt 0 ]; do
    case "$1" in
        -h|--help)
            usage
            exit 0
            ;;
        --patch)
            PATCH_MODE=1
            shift
            ;;
        --sha)
            if [ -z "${2:-}" ]; then
                echo "--sha requires a commit SHA" >&2
                usage >&2
                exit 1
            fi
            DUCKDB_SHA=$2
            shift 2
            ;;
        -*)
            usage >&2
            exit 1
            ;;
        *)
            POSITIONAL+=("$1")
            shift
            ;;
    esac
done

if [ "$PATCH_MODE" -eq 1 ] && { [ -n "$DUCKDB_SHA" ] || [ ${#POSITIONAL[@]} -ne 0 ]; }; then
    usage >&2
    exit 1
fi

if [ -n "$DUCKDB_SHA" ] && ! [[ "$DUCKDB_SHA" =~ ^[0-9a-fA-F]{7,40}$ ]]; then
    echo "Invalid commit SHA: $DUCKDB_SHA" >&2
    exit 1
fi

if [ "$PATCH_MODE" -eq 0 ] && [ ${#POSITIONAL[@]} -gt 1 ]; then
    usage >&2
    exit 1
fi

CURRENT_CRATE_VERSION=$(current_workspace_version)
CURRENT_DUCKDB_VERSION=$(crate_version_to_duckdb_version "$CURRENT_CRATE_VERSION")
CRATE_MAJOR=$(echo "$CURRENT_CRATE_VERSION" | cut -d. -f1)
CURRENT_CRATE_VERSION_PATTERN=${CURRENT_CRATE_VERSION//./\\.}
CURRENT_DUCKDB_VERSION_PATTERN=${CURRENT_DUCKDB_VERSION//./\\.}

if [ "$PATCH_MODE" -eq 1 ]; then
    TARGET_CRATE_VERSION=$(next_crate_patch_version "$CURRENT_CRATE_VERSION")
    echo "Start crate patch release for DuckDB $CURRENT_DUCKDB_VERSION"
    echo "Update crate version from $CURRENT_CRATE_VERSION to $TARGET_CRATE_VERSION"
    update_crate_versions
    exit 0
fi

# Pin an arbitrary upstream commit against the current crate version, without
# bumping versions or download URLs.
if [ -n "$DUCKDB_SHA" ] && [ ${#POSITIONAL[@]} -eq 0 ]; then
    echo "Pin DuckDB commit $DUCKDB_SHA against current crate version $CURRENT_CRATE_VERSION (no version bump)"
    exec ./crates/libduckdb-sys/upgrade.sh --sha "$DUCKDB_SHA"
fi

DUCKDB_VERSION=${POSITIONAL[0]:-$(get_latest_release "duckdb/duckdb")}
if ! valid_duckdb_version "$DUCKDB_VERSION"; then
    echo "Invalid DuckDB version: $DUCKDB_VERSION" >&2
    usage >&2
    exit 1
fi
DUCKDB_VERSION="v${DUCKDB_VERSION#v}"
TARGET_CRATE_VERSION=$(duckdb_version_to_crate_version "$DUCKDB_VERSION" "$CRATE_MAJOR")

if [ -z "$DUCKDB_SHA" ] && [ "$DUCKDB_VERSION" = "$CURRENT_DUCKDB_VERSION" ]; then
    echo "Already up to date, latest DuckDB version is $DUCKDB_VERSION and workspace version is $CURRENT_CRATE_VERSION"
    exit 0
fi

SHA_NOTE=""
[ -n "$DUCKDB_SHA" ] && SHA_NOTE=" (source pinned to commit $DUCKDB_SHA)"
echo "Start to upgrade DuckDB from $CURRENT_DUCKDB_VERSION to $DUCKDB_VERSION$SHA_NOTE"
echo "Update crate version from $CURRENT_CRATE_VERSION to $TARGET_CRATE_VERSION"

update_crate_versions

sed_inplace "s!$CURRENT_DUCKDB_VERSION_PATTERN!$DUCKDB_VERSION!g" \
    .github/workflows/rust.yaml

# Update README download URLs, not prose/history.
sed_inplace "/releases\/download/s!$CURRENT_DUCKDB_VERSION_PATTERN!$DUCKDB_VERSION!g" README.md

SYS_ARGS=("$DUCKDB_VERSION")
[ -n "$DUCKDB_SHA" ] && SYS_ARGS+=(--sha "$DUCKDB_SHA")
exec ./crates/libduckdb-sys/upgrade.sh "${SYS_ARGS[@]}"

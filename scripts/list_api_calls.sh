#!/usr/bin/env bash
# List the v2 C API functions from bindgen_v2.rs and those not yet used by duckdb-neo.
#
# Usage: scripts/list_api_calls.sh [-b BINDINGS_RS] [-o OUT_DIR] [-s SEARCH_PATH]... [-- CARGO_BUILD_ARGS...]
#
#   -b  Use this bindgen_v2.rs instead of locating it via `cargo build`.
#   -o  Directory for the lists (default: target/api_calls).
#   -s  Path searched for API usages (repeatable, default: crates/duckdb-neo/src).
#
# Writes all_api_calls.txt and unimplemented_api_calls.txt, one name per line, sorted.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

bindings=""
out_dir="target/api_calls"
search_paths=()

while getopts "b:o:s:h" opt; do
    case "$opt" in
        b) bindings="$OPTARG" ;;
        o) out_dir="$OPTARG" ;;
        s) search_paths+=("$OPTARG") ;;
        h) sed -n '2,10p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) exit 2 ;;
    esac
done
shift $((OPTIND - 1))
[[ "${1:-}" == "--" ]] && shift
[[ ${#search_paths[@]} -eq 0 ]] && search_paths=(crates/duckdb-neo/src)

if [[ -z "$bindings" ]]; then
    # The build-script-executed message carries libduckdb-sys's OUT_DIR.
    # Only libduckdb-sys's build script has to succeed, so a failing duckdb-neo build is tolerated.
    build_json="$(cargo build -p duckdb-neo --message-format=json "$@")" \
        || echo "warning: cargo build failed, using the build script output anyway" >&2
    while IFS= read -r dir; do
        [[ -f "$dir/bindgen_v2.rs" ]] && bindings="$dir/bindgen_v2.rs"
    done < <(printf '%s\n' "$build_json" \
        | rg '"reason":"build-script-executed"' \
        | rg '"package_id":"[^"]*libduckdb-sys' \
        | rg -o '"out_dir":"([^"]+)"' -r '$1')
    if [[ -z "$bindings" ]]; then
        echo "error: no bindgen_v2.rs found in libduckdb-sys OUT_DIR (is capi-v2 enabled?)" >&2
        exit 1
    fi
fi

[[ -f "$bindings" ]] || { echo "error: $bindings not found" >&2; exit 1; }

mkdir -p "$out_dir"
all="$out_dir/all_api_calls.txt"
unimplemented="$out_dir/unimplemented_api_calls.txt"

rg -o --no-filename -N '^\s*pub fn (\w+)\s*\(' -r '$1' "$bindings" | sort -u > "$all"

# Whole comment lines are matched as one token so names only mentioned in comments are dropped.
used="$(rg -o --no-filename -N '^\s*//.*|\bduckdb_v2_\w+\b' "${search_paths[@]}" \
    | rg -v '^\s*//' | sort -u)"
comm -23 "$all" <(printf '%s\n' "$used") > "$unimplemented"

total=$(wc -l < "$all" | tr -d ' ')
missing=$(wc -l < "$unimplemented" | tr -d ' ')
echo "bindings:      $bindings"
echo "all:           $all ($total)"
echo "unimplemented: $unimplemented ($missing)"

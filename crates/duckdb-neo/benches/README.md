# duckdb-neo benchmarks

[Divan](https://github.com/nvzqz/divan) benchmarks of user-defined functions written with duckdb-neo. They
measure end-to-end query execution, including DuckDB engine work and the wrapper's callback dispatch,
typed vector readers and writers, and function state, at 1,000 to 1,000,000 rows. CI uses them to catch
regressions, and [`scripts/neo_bench.py`](../../../scripts/neo_bench.py) compares any two versions of the code.

## Benchmarks

| Benchmark | Function | Exercises |
| --- | --- | --- |
| `scalar::merge_struct` | `merge_struct(i INTEGER, s VARCHAR, m MAP(VARCHAR, INTEGER), u UNION(num INTEGER, str VARCHAR), d DECIMAL(18, 3))` returns a `STRUCT` of its arguments | Scalar callbacks, reading and writing `MAP`, `UNION`, `DECIMAL` and `STRUCT` vectors |
| `table::dash_split` | `dash_split(input VARCHAR, n BIGINT)` splits `input` on `-` into at most `n` parts, like `str::splitn`, returning `(idx BIGINT, part VARCHAR)` rows | Table function scan state, projection pushdown, `VARCHAR` writes |
| `aggregate::row_hash_ungrouped` | `row_hash(s VARCHAR, d DOUBLE, b BOOLEAN)` sums per-row hashes into a `UBIGINT` | Aggregate updates into a single state |
| `aggregate::row_hash_grouped` | `row_hash` with `GROUP BY` over 1,024 groups | Aggregate updates, combine and finalize over many states |
| `copy::base56_to` | `COPY ... TO ... (FORMAT base56)` writes each value's bytes base56-encoded | `COPY TO` batches, `ColumnDataCollection` scans, DuckDB file system writes |
| `copy::base56_from` | `COPY ... FROM ... (FORMAT base56)` decodes such a file into a table | `COPY FROM` execution, writes to the output chunk |

The functions live in [`support/`](support), one module each. The argument of every benchmark is its row
count: input rows, or parts for `dash_split`. Each benchmark builds its input before timing and checks its
function's output against an independent computation, so an optimization that breaks a function fails the
run instead of reporting a speedup. Initial statement preparation is outside the timed region; each
iteration executes and drains the result, including any rebinding and planning DuckDB performs during
execution. These are end-to-end measurements, not isolated measurements of wrapper overhead.

DuckDB runs with one thread, so the numbers reflect per-row cost rather than the machine's core count.

## Running

```sh
# Everything: about a minute on an Apple M-series laptop.
cargo bench -p duckdb-neo --bench functions

# Filter by name; filters match any part of the path.
cargo bench -p duckdb-neo --bench functions -- scalar
cargo bench -p duckdb-neo --bench functions -- copy::base56_from

# Correctness only, in a debug build, without timing.
cargo test -p duckdb-neo --test bench_functions
```

| Variable | Default | Effect |
| --- | --- | --- |
| `NEO_BENCH_ROWS` | `1000,10000,100000,1000000` | Comma-separated row counts. Test runs (`cargo test --benches`, `-- --test`) default to `1000`. |
| `NEO_BENCH_THREADS` | `1` | DuckDB's `threads` setting. |

Divan's own options also apply, e.g. `-- --sample-count 20` or `DIVAN_MAX_TIME=2`.

## Comparing versions

`scripts/neo_bench.py compare` benchmarks a base git ref against the working tree, uncommitted changes
included, or against `--head <ref>`:

```sh
# The working tree against origin/main.
python3 scripts/neo_bench.py compare --base origin/main

# Two refs, one group of benchmarks, the larger sizes only.
python3 scripts/neo_bench.py compare --base main --head my-branch --filter aggregate --rows 100000,1000000

# Only measure the working tree.
python3 scripts/neo_bench.py run --quick --filter scalar
```

The script exports the base with `git archive` into `target/neo-bench/trees/<sha>` (no git worktree; tracked
files stay untouched), builds both versions into the same target directory, and runs them alternately
(base, head, head, base, ...) on the same machine to cancel drift. Each benchmark's median per run is
collected, and the median of those per side is compared:

| Status | Meaning |
| --- | --- |
| `regression` | Head is slower by more than `--threshold` (default 10%), and every head run is slower than every base run. |
| `improvement` | The same, faster. |
| `inconclusive` | The change exceeds the threshold, but the runs overlap: noise. Rerun with more `--repetitions`. |
| `unchanged` | Within the threshold. |
| `new` / `removed` | The benchmark exists on one side only. |

It exits with 0, 1 when it finds a regression (unless `--no-fail`), or 2 on errors such as a failing build or
benchmark. `--json <file>` writes machine-readable results, `--markdown <file>` appends the report (CI uses
`$GITHUB_STEP_SUMMARY`), and `--log-dir <dir>` keeps divan's raw output of every run. Other options:
`--repetitions`, `--sample-count`, `--max-time`, `--threads`, `--quick` (20 samples, one run per side), and
extra divan arguments after `--`. Run `python3 scripts/neo_bench.py compare --help` for the full list.

Each side runs its own benchmark code, so a change to a benchmark itself shows up as a change in its timing.

## Testing a performance change (checklist for agents)

1. Check that the machine is quiet (`uptime`) and that no other build or benchmark runs. Benchmark results
   from a busy machine are not meaningful.
2. Check correctness first: `cargo test -p duckdb-neo --test bench_functions`.
3. Measure the area you changed, e.g.
   `python3 scripts/neo_bench.py compare --base origin/main --filter scalar --rows 100000,1000000 --json /tmp/neo-bench.json`.
   Read `results[].status` and `results[].change` (a fraction: `-0.25` is 25% faster) from the JSON.
4. Before reporting a result, run the full comparison without `--filter`, so that regressions elsewhere show up.
5. Only report `improvement` or `regression` statuses as changes. `inconclusive` is noise: rerun with
   `--repetitions 5`, or report it as no measurable change.
6. To measure on CI's hardware instead, push the branch and dispatch the workflow:
   `gh workflow run neo-benchmarks.yaml --ref <branch> -f base=main -f filter=scalar`, then
   `gh run watch <run-id>` and `gh run download <run-id> -n neo-bench-results` for `results.json` and the raw
   logs. Dispatching needs the workflow on the default branch; `--ref` picks the code it measures.

## CI

[`.github/workflows/neo-benchmarks.yaml`](../../../.github/workflows/neo-benchmarks.yaml) runs on pull requests
that touch duckdb-neo, libduckdb-sys, the macros, the lockfile, or the script. It compares the pull request's
merge commit against its first parent with three runs per side and fails on regressions. The report is in the
job summary, and the `neo-bench-results` artifact holds `results.json` and the raw logs. Hosted runners are
noisy; a regression must exceed the threshold and separate every run, but rerun the job before acting on a
borderline result. The workflow can also be dispatched with a base ref, filters, row counts, repetitions and
threshold.

## Adding a benchmark

1. Add a module to `support/` with the function's `register`, a `create_input` that builds its input, and a
   `verify` that checks its output.
2. Register the function in `Fixture::new` in `support/mod.rs`.
3. Add a `#[divan::bench(args = bench_rows())]` function to `functions.rs` that creates the input, verifies
   it, and times the query with `bench_query`.
4. Call `verify` from a test in `tests/bench_functions.rs`.

Benchmark names (`group/function/rows`) identify results across versions; renaming one shows up as
`removed` and `new`.

## Other languages

[`instructions.md`](instructions.md) specifies the functions, inputs, checks, benchmarks, comparison and
CI precisely enough to rebuild them on another DuckDB API, with golden values to validate against.

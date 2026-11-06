# Rebuilding the function benchmarks in another language

This is the specification of the duckdb-neo `functions` benchmarks, written so that the same framework can
be rebuilt on top of another DuckDB API, such as the C++ API over the v2 C API (`tools/cpp` in DuckDB), Go, or
Python bindings. Following it gives benchmarks whose names, inputs, queries and results match the Rust
ones, so numbers can be compared across languages, and whose correctness checks can be validated against
the golden values at the end of this file.

The Rust implementation is the reference: [`functions.rs`](functions.rs), [`support/`](support),
[`../tests/bench_functions.rs`](../tests/bench_functions.rs),
[`scripts/neo_bench.py`](../../../scripts/neo_bench.py) and
[`.github/workflows/neo-benchmarks.yaml`](../../../.github/workflows/neo-benchmarks.yaml). When this file and
the code disagree, the code wins; fix this file.

**Must match exactly:** function names, signatures and semantics; input SQL; timed queries; benchmark names;
the hash and base56 algorithms; the base56 file format; error conditions. **Language-specific:** the
benchmark harness, how functions are registered, file layout, and the build commands.

## 1. Components

Rebuild five parts:

| Part | Rust | Purpose |
| --- | --- | --- |
| Functions | `benches/support/*.rs` | The four user-defined functions, their input tables, and `verify` checks. |
| Benchmark harness | `benches/functions.rs` (divan) | Times one query per benchmark and row count. |
| Correctness tests | `tests/bench_functions.rs` | Runs every `verify` on 5,000 rows in the normal test suite. |
| Comparison script | `scripts/neo_bench.py` | Builds and runs two versions alternately and classifies changes. |
| CI workflow | `.github/workflows/neo-benchmarks.yaml` | Runs the comparison on pull requests and on demand. |

Keep the functions in a module that both the harness and the tests use, so the tests check exactly the
code that is benchmarked.

## 2. Fixture

Every benchmark, and every test, uses a fresh fixture:

1. Create a DuckDB instance with the option `threads` set to the `NEO_BENCH_THREADS` environment variable,
   default `1`. Attach an in-memory database as the default.
2. Open one connection.
3. Register all four functions on that connection: `merge_struct`, `dash_split`, `row_hash`, `base56`.

### Row counts

Each benchmark is parameterized by a row count. `NEO_BENCH_ROWS` overrides the list (comma separated;
ignore `_` and surrounding spaces). Otherwise use `1000,10000,100000,1000000` in benchmark mode, and only
`1000` when the harness runs benchmarks once as a smoke test (divan: `cargo test --benches` or `--test`).

## 3. Benchmarks

Benchmark names are `group/function/rows`, e.g. `scalar/merge_struct/1000000`. Use exactly these:

| Name | Setup (not timed) | Timed query |
| --- | --- | --- |
| `scalar/merge_struct/<rows>` | `merge_struct` input, `verify` | `SELECT merge_struct(i, s, m, u, d) FROM scalar_input` |
| `table/dash_split/<rows>` | `dash_split` input, `verify` | `SELECT idx, part FROM dash_split(getvariable('split_input'), <rows>)` |
| `aggregate/row_hash_ungrouped/<rows>` | `row_hash` input, `verify` | `SELECT row_hash(s, d, b) FROM agg_input` |
| `aggregate/row_hash_grouped/<rows>` | `row_hash` input, `verify` | `SELECT g, row_hash(s, d, b) FROM agg_input GROUP BY g` |
| `copy/base56_to/<rows>` | base56 input, `verify` | `COPY copy_input TO '<path>' (FORMAT base56)` |
| `copy/base56_from/<rows>` | base56 input, `verify`, write the file once, create `copy_target` | `COPY copy_target FROM '<path>' (FORMAT base56)` |

Rules for every benchmark:

- Create the fixture, build the input and run `verify` once per benchmark and row count, before timing.
  A `verify` failure must fail the benchmark run.
- Prepare the timed statement once, outside the timed region. Each iteration executes it and drains the
  result (consumes it without reading rows). Timing covers end-to-end execution, including any rebinding
  and planning DuckDB performs during execution; it does not isolate wrapper overhead.
- `<path>` is `data.b56` in a fresh temporary directory, deleted afterwards. Quote it as a SQL string
  literal, doubling any `'`.
- `base56_from`: before every iteration, outside the timed region, run `TRUNCATE copy_target`. Time one
  iteration per sample, so that rows never accumulate within a sample.
- Report throughput as `<rows>` items per iteration, if the harness supports it.
- Collect at least 100 samples by default and report each benchmark's **median** time per iteration.

## 4. Functions

General rules:

- Implement every function through the API's typed vector readers and writers and its function
  callbacks. Do not fall back to generic "get/set value" paths or to SQL: the point is to measure the
  binding.
- Read argument vectors in every storage kind: flat, constant, and dictionary (selection vector). Flatten
  vectors of any other kind (FSST, sequence) before reading.
- A scalar callback whose arguments are all constant may receive a row count of 1. Always size the output
  to the input row count.
- Read `VARCHAR` vectors without re-validating UTF-8 (Rust: `UncheckedString`); DuckDB guarantees it. Do
  validate bytes that come from outside DuckDB, such as a `COPY FROM` file.

### 4.1 `merge_struct` (scalar)

```
merge_struct(i INTEGER, s VARCHAR, m MAP(VARCHAR, INTEGER),
             u UNION(num INTEGER, str VARCHAR), d DECIMAL(18, 3))
  -> STRUCT(i INTEGER, s VARCHAR, m MAP(VARCHAR, INTEGER),
            u UNION(num INTEGER, str VARCHAR), d DECIMAL(18, 3))
```

- Returns a struct whose fields are the arguments, unchanged. A NULL argument becomes a NULL field; the
  struct itself is never NULL.
- Register it with **special NULL handling**, so NULL arguments reach the callback. With DuckDB's default
  handling, any NULL argument must produce a NULL result.
- Write the output column by column: size the struct to the row count, mark every struct row valid, then
  write each field's child vector.
  - `m`: size the entries child to the total entry count of the input rows before writing, and trim it to
    the entries actually written afterwards. Fail on a NULL key. Entry order may change (the Rust writer
    takes a `HashMap`); the checks compare entries unordered.
  - `u`: keep the member tag: member 0 (`num`) or member 1 (`str`), with its value or NULL. A NULL union
    row stays NULL.
  - `d`: copy the scaled integer (storage `int64` for width 18).

Input:

```sql
CREATE OR REPLACE TABLE scalar_input AS
SELECT i::INTEGER AS i,
       CASE WHEN i % 97 = 0 THEN NULL ELSE 'value_' || i END AS s,
       MAP {'a': i::INTEGER, 'b': (i * 2)::INTEGER, 'key_' || (i % 7): (i % 7)::INTEGER} AS m,
       CASE WHEN i % 2 = 0 THEN union_value(num := i::INTEGER)::UNION(num INTEGER, str VARCHAR)
            ELSE union_value(str := 'union_string_' || i)::UNION(num INTEGER, str VARCHAR) END AS u,
       CASE WHEN i % 89 = 0 THEN NULL ELSE (i / 1000)::DECIMAL(18, 3) END AS d
FROM range(<rows>) t(i)
```

`verify` passes when this returns 0:

```sql
SELECT count(*) FROM (SELECT merge_struct(i, s, m, u, d) AS r, * FROM scalar_input)
WHERE r.i IS DISTINCT FROM i OR r.s IS DISTINCT FROM s OR r.u IS DISTINCT FROM u
   OR r.d IS DISTINCT FROM d
   OR list_sort(map_entries(r.m)) IS DISTINCT FROM list_sort(map_entries(m))
```

The test also checks that `typeof(merge_struct(i, s, m, u, d))` is
`STRUCT(i INTEGER, s VARCHAR, m MAP(VARCHAR, INTEGER), u UNION(num INTEGER, str VARCHAR), d DECIMAL(18,3))`.

### 4.2 `dash_split` (table function)

```
dash_split(input VARCHAR, n BIGINT) -> TABLE(idx BIGINT, part VARCHAR)
```

- Splits `input` on `-` into at most `n` parts, like Rust's `str::splitn(n, '-')`: the first `n - 1`
  separators split, and the last part holds the rest of the string, separators included. Emits one row per
  part, with `idx` the 0-based part number, in order.
  - `n = 0` gives no rows. An empty `input` gives one empty part. Leading, trailing and repeated `-` give
    empty parts.
  - A NULL `input` or `n` gives no rows. A negative `n` is an error. Non-constant arguments are an error.
- Bind: read both arguments as constants, declare the two columns, and report the exact cardinality
  `min(n, number of '-' + 1)`.
- Scan: one global cursor (byte offset, next part number), single-threaded (maximum 1 thread). Each exec
  call emits up to the output chunk's capacity parts; an empty chunk ends the scan.
- Enable **projection pushdown** and write only the projected columns, mapping each output position to its
  declared column. When the query projects no columns (`count(*)`), DuckDB asks for column 0 (`idx`) and
  takes the row count from it. The batch's row count is the first output vector's size, so size every
  written vector.

Input: the scan reads a string stored in a variable, so the string is built once outside the timed region:

```sql
SET VARIABLE split_input = (
    SELECT string_agg(CASE WHEN i % 3 = 0 THEN 'long_segment_' || i ELSE 'p' || i END, '-' ORDER BY i)
    FROM range(<rows>) t(i))
```

A third of the parts are longer than 12 bytes, so they are not inlined in DuckDB's string representation.

`verify` checks, comparing against the language's own `splitn`:

1. `SELECT idx, part FROM dash_split('<input>', <n>) ORDER BY idx` for
   `('a-b-c', 2)`, `('a-b-c', 3)`, `('a-b-c', 10)`, `('-a--b-', 10)`, `('', 1)`, `('abc', 0)`, `('a-b', 1)`.
2. `SELECT string_agg(part, ',') FROM dash_split('a-b-c', 3)` is `a,b,c` (projects only `part`).
3. `SELECT count(*) FROM dash_split(NULL, 3)` is 0.
4. With `P` the number of parts of `split_input`: the timed query with `n = P` returns exactly the
   `(idx, part)` pairs from enumerating `split_input`'s parts, including indices across chunk boundaries,
   and `SELECT count(*)` over it returns `P`.

### 4.3 `row_hash` (aggregate)

```
row_hash(s VARCHAR, d DOUBLE, b BOOLEAN) -> UBIGINT
```

- Hashes each row independently with `hash_row` below, and sums the row hashes with wrapping (mod 2^64)
  addition. The sum makes the result independent of row order and of how DuckDB splits and combines
  states.
- State: `hash: u64` and `rows: u64`, both starting at 0. Update adds `hash_row` to `hash` and increments
  `rows`. Combine adds both fields (wrapping for `hash`). Finalize writes `hash`, or NULL when `rows` is 0.
- NULL fields are hashed (see below); they do not skip the row.
- Register it as **not order dependent**.

`hash_row(s, d, b)` on 64-bit unsigned integers with wrapping multiplication. `fnv(h, bytes)` is FNV-1a:
for each byte, `h = (h XOR byte) * 0x00000100000001b3`.

```
h = 0xcbf29ce484222325
h = s is NULL ? fnv(h, [0x00]) : fnv(fnv(h, [0x01]), UTF-8 bytes of s)
h = d is NULL ? fnv(h, [0x00]) : fnv(fnv(h, [0x01]), 8 little-endian bytes of d's IEEE 754 bit pattern)
h = fnv(h, [b is NULL ? 0 : b ? 2 : 1])
h = (h XOR (h >> 30)) * 0xbf58476d1ce4e5b9
h = (h XOR (h >> 27)) * 0x94d049bb133111eb
return h XOR (h >> 31)
```

Input (1,024 groups):

```sql
CREATE OR REPLACE TABLE agg_input AS
SELECT (i % 1024)::INTEGER AS g,
       CASE WHEN i % 89 = 0 THEN NULL ELSE 'key_' || (i * 7919 % 100003) END AS s,
       CASE WHEN i % 97 = 0 THEN NULL ELSE i * 0.25 END::DOUBLE AS d,
       CASE WHEN i % 101 = 0 THEN NULL ELSE i % 3 = 0 END AS b
FROM range(<rows>) t(i)
```

`verify`:

1. Compute the expected values in the host language: read `SELECT g, s, d, b FROM agg_input` and wrap-sum
   `hash_row` over all rows, both overall and separately for each group key `g`.
2. The ungrouped query returns the expected overall value.
3. Every grouped row has the expected hash for its key, and every expected key appears exactly once.
   Reject missing, duplicate or unexpected groups, and NULL hashes for nonempty groups.
4. `SELECT row_hash(s, d, b) FROM agg_input WHERE false` returns NULL.

### 4.4 `base56` (COPY format, both directions)

One format named `base56`, registered for both `COPY ... TO` and `COPY ... FROM`, with fallback file
extension `b56`.

**Alphabet** (56 digits, without the look-alikes `0 1 I O l o`), digit value = position:

```
23456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnpqrstuvwxyz
```

**Encoding** of a byte string:

- Split it into 4-byte blocks and a tail of 0 to 3 bytes.
- Each block, read as a little-endian `u32`, becomes 6 digits, most significant digit first (base 56,
  zero-padded with `2`).
- A tail of 1, 2 or 3 bytes is zero-padded to 4 bytes, read as a little-endian `u32`, and becomes 2, 3 or 5
  digits, most significant first.

**Decoding** reverses this. The digit count modulo 6 must be 0, 2, 3 or 5 (giving 0, 1, 2 or 3 tail bytes).
Fail on a digit outside the alphabet, on any other length, and on a block whose value does not fit its byte
count (`>= 2^32` for 6 digits, `>= 2^(8 * tail bytes)` for a tail).

**File format** (UTF-8, `\n` line endings):

- Header line: `#base56 ` followed by the column type names joined by `,`, e.g.
  `#base56 BIGINT,DOUBLE,VARCHAR,BOOLEAN,INTEGER`.
- One line per row: fields joined by `,`. NULL is `~`. Otherwise the field is the base56 encoding of the
  value's bytes:

  | Type | Bytes |
  | --- | --- |
  | `BOOLEAN` | 1 byte: `0x00` false, `0x01` true |
  | `INTEGER` | 4 bytes, little-endian |
  | `BIGINT` | 8 bytes, little-endian |
  | `DOUBLE` | 8 bytes, little-endian IEEE 754 bit pattern |
  | `VARCHAR` | UTF-8 bytes; the empty string is an empty field |

- Any other column type fails binding with an error containing `base56 does not support <TYPE>`, e.g.
  `base56 does not support TINYINT`.

**`COPY ... TO`:**

- Bind: map the input column types; fail on unsupported ones.
- Init (once per output file): open the file through DuckDB's file system, created or truncated, and write
  the header.
- Batch: scan the batch's column data collection and encode its rows into a byte buffer, row by row in
  column order.
- Flush: write the buffer to the file under a lock, retrying partial writes.
- Finalize: close the file.

**`COPY ... FROM`:**

- Bind: map the target table's column types; fail on unsupported ones.
- Init global: read the whole file through DuckDB's file system. The file must start with the header for
  the target columns; otherwise fail with an error containing `does not match the target columns`.
- Exec: claim up to the output chunk's capacity of lines after the last claimed line (with an atomic
  compare-and-swap, so parallel readers are safe), split them into fields, check that each line has as many
  fields as there are columns, decode each column into its output vector, and size every vector to the
  number of claimed lines. Zero lines ends the read. A final line without `\n` is still a row.
- Fail when a decoded fixed-width field has the wrong byte count, a BOOLEAN byte is not 0 or 1, or a
  VARCHAR is not valid UTF-8.

Input:

```sql
CREATE OR REPLACE TABLE copy_input AS
SELECT i::BIGINT AS id,
       CASE WHEN i % 89 = 0 THEN NULL ELSE i * 1.5 END::DOUBLE AS x,
       CASE WHEN i % 97 = 0 THEN NULL ELSE 'name_' || i END AS name,
       CASE WHEN i % 101 = 0 THEN NULL ELSE i % 2 = 0 END AS flag,
       (i % 1000)::INTEGER AS bucket
FROM range(<rows>) t(i)
```

Target tables are created empty with `CREATE OR REPLACE TABLE <name> AS FROM copy_input LIMIT 0`.

`verify`, given a file path:

1. Codec: for every length 0 to 13, round-trip all-`0x00`, all-`0xFF`, and bytes `k * 97 mod 256` for
   `k = 0, 1, ...`; every encoded digit must be in the alphabet. Decoding `2`, `2222`, `222220`, `zzzzzz`
   and `zz` must fail.
2. `COPY copy_input TO <path>`, create `copy_verify`, `COPY copy_verify FROM <path>`; this must return 0:
   ```sql
   SELECT count(*) FROM ((FROM copy_input EXCEPT ALL FROM copy_verify)
                         UNION ALL (FROM copy_verify EXCEPT ALL FROM copy_input))
   ```
   Then drop `copy_verify`.
3. `COPY` the file into `CREATE OR REPLACE TABLE copy_mismatch (id BIGINT)` fails with
   `does not match the target columns`. Drop `copy_mismatch`.
4. `COPY (SELECT 1::TINYINT) TO <path> (FORMAT base56)` fails with `does not support TINYINT`.

## 5. Correctness tests

In the language's normal test suite, one test per function, each on a fresh fixture with 5,000 rows:
create the input and run `verify` (for `base56`, in a temporary directory), plus the `typeof` check for
`merge_struct`. These must run in a debug build within seconds.

Before relying on the checks, confirm that each one catches a bug: break each function once (for example,
change one row's value, flip one state's hash during combine, skip a byte while splitting, perturb decoded
doubles) and check that its test fails.

## 6. Comparison script

Reuse `scripts/neo_bench.py` if possible: only `CARGO_BENCH`, `BENCH_FILE` and `parse_divan` are specific
to Rust. Otherwise port it with this behavior.

**Interface:**

```
compare [--base REF] [--head REF] [--filter F]... [--rows LIST] [--threads N] [--sample-count N]
        [--max-time SECS] [--quick] [--repetitions N] [--threshold PCT] [--no-fail] [--locked]
        [--target-dir DIR] [--log-dir DIR] [--json FILE] [--markdown FILE] [-- harness args]
run     [--ref REF] (same run options)
```

Defaults: `--base origin/main`, the working tree as head (uncommitted changes included), 3 repetitions for
`compare` and 1 for `run`, threshold 10%. `--quick` means 20 samples and 1 repetition. `--rows` and
`--threads` set `NEO_BENCH_ROWS` and `NEO_BENCH_THREADS`. `--filter` selects benchmarks by name.

**Algorithm:**

1. Resolve each ref to a commit, trying `origin/<ref>` when `<ref>` does not resolve (CI checkouts only
   have remote-tracking branches). Export the commit with `git archive` into
   `<target-dir>/neo-bench/trees/<sha>`, reusing a complete earlier export. Do not create git worktrees.
2. If the base has no benchmarks, run only the head and report every benchmark as `new`.
3. Build both versions before running anything, sharing one build directory for dependencies.
4. Run the versions alternately: base, head, then head, base, and so on, `repetitions` times each. Each run
   yields every benchmark's median time.
5. Per benchmark and side, take the median of the per-run medians. `change = head / base - 1`.
6. Classify:

   | Status | Condition |
   | --- | --- |
   | `regression` | `change > threshold` and every head run is slower than every base run |
   | `improvement` | `change < -threshold` and every head run is faster than every base run |
   | `inconclusive` | beyond the threshold, but the runs overlap |
   | `unchanged` | within the threshold |
   | `new` / `removed` | present on one side only |

7. Exit 1 when there is a regression (unless `--no-fail`), 2 on errors (unknown ref, failed build or run,
   no results), otherwise 0.

**Outputs:**

- A markdown report on stdout, also appended to `--markdown`: head and base descriptions, the number of
  runs, the machine (OS, architecture, core count, load average before the runs), the rule above, and a
  table of benchmark, base median ± half the range, head median ± half the range, change, status, and a
  summary count per status.
- `--json`:
  ```json
  {
    "base": {"ref": "...", "sha": "...", "has_benchmarks": true},
    "head": {"ref": null, "sha": "...", "dirty": true},
    "threshold": 0.1, "repetitions": 3, "machine": "...",
    "results": [{"name": "table/dash_split/1000", "status": "unchanged", "change": 0.018,
                 "base_ns": [...], "head_ns": [...], "base_median_ns": 61400.0, "head_median_ns": 62600.0}]
  }
  ```
- `--log-dir`: each run's raw harness output, named `run-<NN>-<label>.txt`.

If the harness has no machine-readable output (divan does not), parse its table, and keep the names in the
`group/function/rows` form.

## 7. CI workflow

- Triggers: pull requests touching the bindings, their native library, build files, lockfile, toolchain
  pin, the script, or the workflow; and manual dispatch with inputs `base` (default `main`), `filter`
  (space separated), `rows`, `repetitions` (default 3) and `threshold` (default 10).
- One job on one runner, with full git history.
- For pull requests, compare the checked-out merge commit against its first parent (`HEAD^1`), the exact
  base it merges into.
- Run the comparison with locked dependencies, writing the report to the job summary, and upload the JSON
  and logs as an artifact named `neo-bench-results`, also when the job fails.
- Cancel superseded runs of the same pull request.

## 8. Golden values

From the Rust implementation; an implementation that matches the rules above reproduces them.

base56 encodings:

| Bytes | Encoding |
| --- | --- |
| (empty) | `` (empty) |
| `00` | `22` |
| `FF` | `6Z` |
| `"duck"` | `5HDDPE` |
| `"DuckDB!"` | `5HDDNe2ER4N` |
| `true` (BOOLEAN) | `23` |
| `7` (INTEGER) | `222229` |
| `42` (BIGINT) | `22222k222222` |
| `1.5` (DOUBLE) | `2222223x9Ays` |

`hash_row`:

| `s`, `d`, `b` | Hash |
| --- | --- |
| `'duck'`, `1.5`, `true` | `17517133033816018756` |
| NULL, NULL, NULL | `16898831758259097429` |
| `''`, `0.0`, `false` | `15861653319075095682` |

`SELECT row_hash(s, d, b) FROM agg_input`: `13117526009623259447` at 1,000 rows,
`15537843542995968811` at 10,000 rows.

`COPY copy_input TO ... (FORMAT base56)` at 1,000 rows writes 48,667 bytes, starting with:

```
#base56 BIGINT,DOUBLE,VARCHAR,BOOLEAN,INTEGER
222222222222,~,~,~,222222
222223222222,2222223x9Ays,573hTy63h,22,222223
222224222222,2222223xF9MS,573hTy68H,23,222224
```

`split_input` at 10 rows:
`long_segment_0-p1-p2-long_segment_3-p4-p5-long_segment_6-p7-p8-long_segment_9`; at 1,000 rows it is 8,897
bytes long.

`merge_struct` rows 0 and 1, with entries sorted
(`{'i': r.i, 's': r.s, 'm': list_sort(map_entries(r.m)), 'u': r.u, 'd': r.d}::VARCHAR`):

```
{'i': 0, 's': NULL, 'm': [{'key': a, 'value': 0}, {'key': b, 'value': 0}, {'key': key_0, 'value': 0}], 'u': 0, 'd': NULL}
{'i': 1, 's': value_1, 'm': [{'key': a, 'value': 1}, {'key': b, 'value': 2}, {'key': key_1, 'value': 1}], 'u': union_string_1, 'd': 0.001}
```

## 9. Done when

- The harness runs all 24 benchmarks (6 benchmarks at 4 row counts) with the names from section 3, and a
  smoke mode runs each once at 1,000 rows.
- Every `verify` passes at every row count, and each fails when its function is broken.
- The golden values in section 8 are reproduced.
- Comparing a version against itself reports every benchmark `unchanged` or `inconclusive`; an artificial
  slowdown of 20% or more in one function is reported as `regression` with exit code 1, and the other
  benchmarks stay `unchanged`.
- The CI workflow passes a linter (for GitHub Actions: `actionlint` with `shellcheck`).

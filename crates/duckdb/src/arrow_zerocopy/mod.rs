//! Zero-copy Arrow registration via DuckDB's native `arrow_scan` table function.
//!
//! Registers a set of held Arrow `RecordBatch`es as a DuckDB view whose scans reference the Arrow
//! buffers in place (no copy into DuckDB native storage), unlike the `Appender`. The factory is
//! replayable (a fresh stream per scan, so self-joins work) and honors both projection and filter
//! pushdown.
//!
//! # Filter pushdown
//!
//! The C++ shim serialises DuckDB's `TableFilterSet` into a compact little-endian wire-format
//! buffer that is decoded and evaluated in Rust before returning each stream. The following filter
//! kinds are handled end-to-end:
//!
//! * `CONSTANT_COMPARISON` (eq / ne / lt / gt / le / ge) over all pushable scalar types: ints,
//!   unsigned, float/double (incl. NaN total-order), bool, Utf8, Blob, Date, Time (µs),
//!   Timestamp (all four DuckDB units incl. TZ), Decimal128.
//! * `IS_NULL` / `IS_NOT_NULL`
//! * `CONJUNCTION_AND` / `CONJUNCTION_OR` (nested trees)
//! * `IN_FILTER`
//! * `STRUCT_EXTRACT` — struct-field (column-path) predicates via recursive descent.
//!
//! The following are safely skipped (the outer query re-checks):
//!
//! * `OPTIONAL_FILTER` — a hint; correctness is guaranteed by the join.
//! * `DYNAMIC_FILTER` / `BLOOM_FILTER` — join-reduction filters; re-checked by the join.
//!
//! On DuckDB v1.5.4 the optimizer routes every pushable arrow-scan filter to one of these
//! structured kinds (it constant-folds expressions and keeps genuinely-complex predicates above
//! the scan). The catch-all `EXPRESSION_FILTER` kind is not emitted; should one ever appear it
//! produces a clean error stream (fail-loud, never silently wrong).

pub(crate) mod filter;

use std::ffi::{CString, c_char, c_void};
use std::os::raw::c_int;

use arrow::datatypes::SchemaRef;
use arrow::ffi::FFI_ArrowSchema;
use arrow::ffi_stream::FFI_ArrowArrayStream;
use arrow::record_batch::{RecordBatch, RecordBatchIterator};

use crate::error::Error;
use crate::inner_connection::InnerConnection;
use crate::{Connection, Result};

type ProduceFn = extern "C" fn(*mut c_void, *const *const c_char, usize, *const u8, usize, *mut FFI_ArrowArrayStream);
type GetSchemaFn = extern "C" fn(*mut c_void, *mut FFI_ArrowSchema);

// SAFETY: layout matched by the C++ `RustCallbacks` prefix; the two fn pointers MUST stay first.
#[repr(C)]
struct ArrowFactory {
    produce: ProduceFn,
    get_schema: GetSchemaFn,
    batches: Vec<RecordBatch>,
    schema: SchemaRef,
}

unsafe extern "C" {
    fn ddb_rs_arrow_register(conn: crate::ffi::duckdb_connection, name: *const c_char, factory: *mut c_void) -> c_int;
}

/// Handle to a live zero-copy Arrow registration created by [`Connection::register_arrow`].
///
/// Dropping this handle automatically unregisters the view from DuckDB (via `DROP VIEW IF EXISTS`)
/// **before** freeing the factory memory it points at, so a late scan errors cleanly ("view not
/// found") rather than dereferencing freed memory.
///
/// The `'conn` lifetime borrow guarantees the connection outlives the view.
///
/// # Thread safety
///
/// `ArrowView` is `!Send` and `!Sync`: it holds a raw pointer to the factory and is tied to the
/// single-threaded [`Connection`] model of this crate.
pub struct ArrowView<'conn> {
    conn: &'conn Connection,
    name: String,
    factory: *mut ArrowFactory,
}

impl Drop for ArrowView<'_> {
    fn drop(&mut self) {
        // Unregister the view BEFORE freeing the factory it points at, so a later scan can't
        // dereference freed memory (it errors cleanly with "view not found" instead).
        let escaped = self.name.replace('"', "\"\"");
        let _ = self.conn.execute_batch(&format!("DROP VIEW IF EXISTS \"{escaped}\""));
        // SAFETY: factory came from Box::into_raw in register_arrow; freed exactly once.
        unsafe { drop(Box::from_raw(self.factory)) };
    }
}

/// Build a non-null-callback error stream so that DuckDB's `get_next` call returns an Arrow error
/// rather than dereferencing a null function pointer. A null `get_next` (from `empty()`) would
/// segfault inside `ArrowArrayStreamWrapper::GetNextChunk`, which has no null guard.
fn error_stream(schema: SchemaRef, msg: String) -> FFI_ArrowArrayStream {
    let iter = std::iter::once::<arrow::error::Result<RecordBatch>>(Err(arrow::error::ArrowError::ComputeError(msg)));
    FFI_ArrowArrayStream::new(Box::new(RecordBatchIterator::new(iter, schema)))
}

/// Resolve column name pointers to schema indices.
fn resolve_indices(f: &ArrowFactory, names: *const *const c_char, n: usize) -> Result<Vec<usize>, String> {
    if n == 0 {
        return Ok((0..f.schema.fields().len()).collect());
    }
    let name_ptrs = unsafe { std::slice::from_raw_parts(names, n) };
    name_ptrs
        .iter()
        .map(|&p| {
            let name = unsafe { std::ffi::CStr::from_ptr(p) }
                .to_str()
                .map_err(|e| format!("column name is not valid UTF-8: {e}"))?;
            f.schema
                .index_of(name)
                .map_err(|_| format!("projected column '{name}' not found in schema"))
        })
        .collect::<Result<Vec<usize>, String>>()
}

/// Build the projected and filter-applied [`FFI_ArrowArrayStream`], returning an error message on failure.
/// Also returns the projected [`SchemaRef`] so callers can use it for error streams.
fn build_stream_filtered(
    f: &ArrowFactory,
    names: *const *const c_char,
    n: usize,
    filter_ptr: *const u8,
    filter_len: usize,
) -> Result<(FFI_ArrowArrayStream, SchemaRef), String> {
    // Resolve projected indices and build the projected schema ONCE up front.
    let indices = resolve_indices(f, names, n)?;
    let proj_schema: SchemaRef =
        std::sync::Arc::new(f.schema.project(&indices).map_err(|e| format!("project schema: {e}"))?);

    // Project batches using the already-resolved indices.
    let projected: Vec<RecordBatch> = f
        .batches
        .iter()
        .map(|b| b.project(&indices).map_err(|e| e.to_string()))
        .collect::<Result<Vec<RecordBatch>, String>>()?;

    // Decode + apply filter
    let filter_buf = if filter_ptr.is_null() || filter_len == 0 {
        &[][..]
    } else {
        unsafe { std::slice::from_raw_parts(filter_ptr, filter_len) }
    };
    let node = filter::decode(filter_buf);

    let filtered: Vec<RecordBatch> = projected
        .into_iter()
        .map(|b| {
            let mask = filter::evaluate(&node, &b).map_err(|e| e.to_string())?;
            arrow::compute::filter_record_batch(&b, &mask).map_err(|e| e.to_string())
        })
        .collect::<Result<Vec<RecordBatch>, String>>()?;

    // `proj_schema` already computed above — no second resolve_indices call needed even when
    // all rows are filtered out or batches are empty.
    let reader = RecordBatchIterator::new(
        filtered.into_iter().map(Ok::<RecordBatch, arrow::error::ArrowError>),
        proj_schema.clone(),
    );
    Ok((FFI_ArrowArrayStream::new(Box::new(reader)), proj_schema))
}

extern "C" fn rust_produce(
    factory: *mut c_void,
    names: *const *const c_char,
    n: usize,
    filter_ptr: *const u8,
    filter_len: usize,
    out: *mut FFI_ArrowArrayStream,
) {
    // Read the factory pointer BEFORE the fallible work so it is available on the recovery path.
    // SAFETY: factory is valid for the lifetime of the ArrowView, which must outlive
    // any scan callback (documented on ArrowView).
    let f = unsafe { &*(factory as *const ArrowFactory) };

    // Compute the projected schema up front so error streams carry the right schema.
    // Fall back to the full schema only when projection itself fails (e.g. unknown column name).
    let projected_schema: SchemaRef = resolve_indices(f, names, n)
        .and_then(|indices| {
            f.schema
                .project(&indices)
                .map(std::sync::Arc::new)
                .map_err(|e| e.to_string())
        })
        .unwrap_or_else(|_| f.schema.clone());

    let stream = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        build_stream_filtered(f, names, n, filter_ptr, filter_len)
    })) {
        Ok(Ok((s, _proj_schema))) => s,
        Ok(Err(msg)) => error_stream(projected_schema, msg),
        Err(_) => error_stream(
            projected_schema,
            "arrow zero-copy factory panicked in produce".to_string(),
        ),
    };
    // SAFETY: `out` is a valid, uninitialized output slot provided by DuckDB's shim.
    unsafe { std::ptr::write(out, stream) };
}

extern "C" fn rust_get_schema(factory: *mut c_void, out: *mut FFI_ArrowSchema) {
    let result = std::panic::catch_unwind(|| unsafe {
        let f = &*(factory as *const ArrowFactory);
        let schema = FFI_ArrowSchema::try_from(f.schema.as_ref()).expect("export schema");
        std::ptr::write(out, schema);
    });
    if result.is_err() {
        // empty() yields a less-actionable downstream error than the produce-path error stream; acceptable at bind time.
        unsafe { std::ptr::write(out, FFI_ArrowSchema::empty()) };
    }
}

/// Build a generic DuckDB-failure error with a message (the crate's `Error` has no plain
/// string variant; this mirrors `error.rs`'s `Error::DuckDBFailure(ffi::Error::new(code), msg)`).
fn duckdb_err(msg: impl Into<String>) -> Error {
    Error::DuckDBFailure(
        crate::ffi::Error::new(crate::ffi::duckdb_state_DuckDBError),
        Some(msg.into()),
    )
}

impl Connection {
    /// Register `batches` as a zero-copy Arrow view named `name` on this connection.
    ///
    /// Scans reference the held Arrow buffers in place — no copy into DuckDB storage. The
    /// factory is replayable (a fresh stream per scan, so self-joins work) and honors both
    /// **projection pushdown** (DuckDB selects only needed columns) and **filter pushdown**:
    /// constant comparisons, IS [NOT] NULL, IN, AND/OR, and STRUCT_EXTRACT
    /// (struct-field) predicates are evaluated in Rust over scalars of all pushable types (ints,
    /// unsigned, float/double incl. NaN total-order, bool, Utf8, Blob, Date, Time (µs), Timestamp,
    /// Decimal128). OPTIONAL/DYNAMIC/BLOOM filters are safely skipped. On DuckDB v1.5.4 the
    /// optimizer routes every pushable arrow-scan filter to one of these structured kinds (it
    /// constant-folds expressions and keeps genuinely-complex predicates above the scan), so the
    /// catch-all EXPRESSION_FILTER kind is not emitted here; should one ever appear it produces a
    /// clean error stream (fail-loud, never silently wrong).
    ///
    /// Dropping the returned [`ArrowView`] automatically unregisters the view from DuckDB
    /// (via `DROP VIEW IF EXISTS`) before freeing the factory memory, so a late scan errors
    /// cleanly rather than dereferencing freed memory.
    ///
    /// # Errors
    ///
    /// Returns an error if `batches` is empty, if any batch has a schema that does not match
    /// the first batch's schema, or if DuckDB fails to create the view.
    ///
    /// # Thread safety
    ///
    /// [`ArrowView`] is `!Send` and `!Sync` — it is tied to the single-threaded [`Connection`]
    /// model of this crate.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use duckdb::Connection;
    /// # use duckdb::arrow::array::Int64Array;
    /// # use duckdb::arrow::datatypes::{DataType, Field, Schema};
    /// # use duckdb::arrow::record_batch::RecordBatch;
    /// # use std::sync::Arc;
    /// let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
    /// let arr = Arc::new(Int64Array::from(vec![1i64, 2, 3, 4, 5]));
    /// let batch = RecordBatch::try_new(schema, vec![arr]).unwrap();
    ///
    /// let conn = Connection::open_in_memory().unwrap();
    /// let _view = conn.register_arrow("nums", vec![batch]).unwrap();
    ///
    /// // Filtered + projected query — filter is pushed into the Rust evaluator:
    /// let sum: i64 = conn
    ///     .query_row("SELECT sum(v) FROM nums WHERE v > 2", [], |r| r.get(0))
    ///     .unwrap();
    /// assert_eq!(sum, 12); // 3 + 4 + 5
    /// ```
    pub fn register_arrow(&self, name: &str, batches: Vec<RecordBatch>) -> Result<ArrowView<'_>> {
        // Fail (without allocating the factory) before Box::into_raw so nothing can leak.
        let cname = CString::new(name).map_err(Error::NulError)?;
        let schema = batches
            .first()
            .map(|b| b.schema())
            .ok_or_else(|| duckdb_err("register_arrow: empty batches"))?;
        for b in &batches {
            if b.schema() != schema {
                return Err(duckdb_err("register_arrow: all batches must share the same schema"));
            }
        }
        let factory_ptr = Box::into_raw(Box::new(ArrowFactory {
            produce: rust_produce,
            get_schema: rust_get_schema,
            batches,
            schema,
        }));
        match self
            .db
            .borrow_mut()
            .register_arrow_inner(cname.as_ptr(), factory_ptr.cast::<c_void>())
        {
            Ok(()) => Ok(ArrowView {
                conn: self,
                name: name.to_owned(),
                factory: factory_ptr,
            }),
            Err(e) => {
                // SAFETY: registration failed, reclaim the box we just leaked.
                unsafe { drop(Box::from_raw(factory_ptr)) };
                Err(e)
            }
        }
    }
}

impl InnerConnection {
    fn register_arrow_inner(&mut self, name: *const c_char, factory: *mut c_void) -> Result<()> {
        // SAFETY: `self.con` is a live duckdb_connection; the shim returns 0 on success.
        let rc = unsafe { ddb_rs_arrow_register(self.con, name, factory) };
        if rc != 0 {
            return Err(duckdb_err(
                "ddb_rs_arrow_register failed (C++ exception in arrow_scan/CreateView)",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn sample() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("region", DataType::Utf8, true),
        ]));
        let k = Int64Array::from(vec![1, 2, 3, 4]);
        let region = StringArray::from(vec![Some("us"), Some("eu"), None, Some("us")]);
        RecordBatch::try_new(schema, vec![Arc::new(k), Arc::new(region)]).unwrap()
    }

    #[test]
    fn round_trip_select_star() {
        let conn = Connection::open_in_memory().unwrap();
        let batch = sample();
        let reg = conn.register_arrow("v", vec![batch.clone()]).unwrap();
        // count
        let n: i64 = conn.query_row("SELECT count(*) FROM v", [], |r| r.get(0)).unwrap();
        assert_eq!(n, 4);
        // sum of k
        let sum_k: i64 = conn.query_row("SELECT sum(k) FROM v", [], |r| r.get(0)).unwrap();
        assert_eq!(sum_k, 10);
        drop(reg); // handle dropped after queries (lifetime rule)
    }

    #[test]
    fn projection_subset() {
        let conn = Connection::open_in_memory().unwrap();
        let reg = conn.register_arrow("v", vec![sample()]).unwrap();
        // Select only `region` — DuckDB pushes a single-column projection into the factory.
        let count_us: i64 = conn
            .query_row(
                "SELECT count(*) FROM (SELECT region FROM v) WHERE region = 'us'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count_us, 2);
        drop(reg);
    }

    #[test]
    fn self_join_is_replayable() {
        // A self-join scans the view twice → the factory's produce is called more than once.
        // Each call must yield fresh data (the one-shot deprecated path returns empty on the 2nd).
        let conn = Connection::open_in_memory().unwrap();
        let reg = conn.register_arrow("v", vec![sample()]).unwrap();
        let pairs: i64 = conn
            .query_row("SELECT count(*) FROM v a JOIN v b ON a.k = b.k", [], |r| r.get(0))
            .unwrap();
        assert_eq!(pairs, 4); // each of 4 distinct k matches itself exactly once
        drop(reg);
    }

    #[test]
    fn filter_correct_with_pushdown_enabled() {
        // With filter pushdown ON (default), the C++ shim now serialises filters into the wire
        // buffer and Rust applies them — correct results without needing disabled_optimizers.
        let conn = Connection::open_in_memory().unwrap();
        let reg = conn.register_arrow("v", vec![sample()]).unwrap();
        // k > 2 keeps k in {3,4} → sum = 7
        let sum_hi: i64 = conn
            .query_row("SELECT sum(k) FROM v WHERE k > 2", [], |r| r.get(0))
            .unwrap();
        assert_eq!(sum_hi, 7);
        drop(reg);
    }

    #[test]
    fn constant_comparison_filter_applied_no_global_setting() {
        // No `SET disabled_optimizers='filter_pushdown'` — the factory must honor the pushed filter.
        let conn = Connection::open_in_memory().unwrap();
        let reg = conn.register_arrow("v", vec![sample()]).unwrap();
        let sum_gt2: i64 = conn
            .query_row("SELECT sum(k) FROM v WHERE k > 2", [], |r| r.get(0))
            .unwrap();
        assert_eq!(sum_gt2, 7); // 3 + 4
        let cnt_eq: i64 = conn
            .query_row("SELECT count(*) FROM v WHERE k = 2", [], |r| r.get(0))
            .unwrap();
        assert_eq!(cnt_eq, 1);
        drop(reg);
    }

    #[test]
    fn unhandled_filter_kind_errors_not_silently_wrong() {
        use super::filter::{FilterNode, evaluate};
        let err = evaluate(&FilterNode::Unhandled(9), &sample());
        assert!(err.is_err(), "unhandled filter kind must error, got {err:?}");
    }

    // Direct unit tests of the evaluator: DuckDB applies IS_NULL/IS_NOT_NULL/OR above the scan for
    // simple queries, so the SQL tests don't reliably reach these arms. These exercise them
    // deterministically (hand-constructed FilterNodes), bypassing the optimizer.
    #[test]
    fn evaluate_is_null_and_is_not_null_direct() {
        use super::filter::{FilterNode, evaluate};
        // sample(): region (col 1) = [us, eu, NULL, us]
        let batch = sample();
        let null_mask = evaluate(&FilterNode::IsNull { path: vec![1] }, &batch).unwrap();
        assert_eq!(
            null_mask.iter().map(|v| v.unwrap_or(false)).collect::<Vec<_>>(),
            vec![false, false, true, false]
        );
        let not_null_mask = evaluate(&FilterNode::IsNotNull { path: vec![1] }, &batch).unwrap();
        assert_eq!(
            not_null_mask.iter().map(|v| v.unwrap_or(false)).collect::<Vec<_>>(),
            vec![true, true, false, true]
        );
    }

    #[test]
    fn evaluate_or_direct() {
        use super::filter::{CmpOp, FilterNode, ScalarVal, evaluate};
        // sample(): k (col 0) = [1, 2, 3, 4]; k = 1 OR k = 4
        let batch = sample();
        let node = FilterNode::Or(vec![
            FilterNode::Compare {
                path: vec![0],
                op: CmpOp::Eq,
                val: ScalarVal::I64(1),
            },
            FilterNode::Compare {
                path: vec![0],
                op: CmpOp::Eq,
                val: ScalarVal::I64(4),
            },
        ]);
        let mask = evaluate(&node, &batch).unwrap();
        assert_eq!(
            mask.iter().map(|v| v.unwrap_or(false)).collect::<Vec<_>>(),
            vec![true, false, false, true]
        );
    }

    #[test]
    fn evaluate_in_does_not_match_null_column() {
        use super::filter::{FilterNode, ScalarVal, evaluate};
        // sample(): region (col 1) = [us, eu, NULL, us]; region IN ('us') must NOT match the NULL row
        let batch = sample();
        let node = FilterNode::In {
            path: vec![1],
            vals: vec![ScalarVal::Utf8("us".into())],
        };
        let mask = evaluate(&node, &batch).unwrap();
        assert_eq!(
            mask.iter().map(|v| v.unwrap_or(false)).collect::<Vec<_>>(),
            vec![true, false, false, true]
        );
    }

    #[test]
    fn null_and_conjunction_and_in_filters() {
        let conn = Connection::open_in_memory().unwrap();
        let reg = conn.register_arrow("v", vec![sample()]).unwrap();
        let is_null: i64 = conn
            .query_row("SELECT count(*) FROM v WHERE region IS NULL", [], |r| r.get(0))
            .unwrap();
        assert_eq!(is_null, 1);
        let not_null: i64 = conn
            .query_row("SELECT count(*) FROM v WHERE region IS NOT NULL", [], |r| r.get(0))
            .unwrap();
        assert_eq!(not_null, 3);
        let conj: i64 = conn
            .query_row("SELECT sum(k) FROM v WHERE k > 1 AND k < 4", [], |r| r.get(0))
            .unwrap();
        assert_eq!(conj, 5); // 2 + 3
        let in_list: i64 = conn
            .query_row("SELECT sum(k) FROM v WHERE k IN (1, 3)", [], |r| r.get(0))
            .unwrap();
        assert_eq!(in_list, 4); // 1 + 3
        drop(reg);
    }

    #[test]
    fn inner_join_with_filter_matches_appender() {
        // A join can push a dynamic/bloom filter to the arrow scan; skipping those must stay correct
        // because the join re-checks. Compare the zero-copy result to the Appender's.
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE t AS SELECT * FROM (VALUES (2),(3),(9)) AS x(k)")
            .unwrap();
        let reg = conn.register_arrow("v", vec![sample()]).unwrap();
        let joined: i64 = conn
            .query_row("SELECT sum(v.k) FROM v JOIN t ON v.k = t.k WHERE v.k > 1", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(joined, 5); // v.k in {2,3} match t, >1 → 2+3
        drop(reg);
    }

    fn sample_types() -> RecordBatch {
        use arrow::array::{BooleanArray, Float64Array, Int32Array, StringArray, UInt32Array};
        let schema = Arc::new(Schema::new(vec![
            Field::new("i32", DataType::Int32, false),
            Field::new("u32", DataType::UInt32, false),
            Field::new("f64", DataType::Float64, false),
            Field::new("b", DataType::Boolean, false),
            Field::new("s", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![-1, 0, 1, 2, 3])),
                Arc::new(UInt32Array::from(vec![10u32, 50, 99, 100, 200])),
                Arc::new(Float64Array::from(vec![0.5, 1.0, 1.5, 2.0, 2.5])),
                Arc::new(BooleanArray::from(vec![true, false, true, false, true])),
                Arc::new(StringArray::from(vec!["x", "y", "x", "z", "x"])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn filter_pushdown_across_scalar_types() {
        let conn = Connection::open_in_memory().unwrap();
        let reg = conn.register_arrow("v", vec![sample_types()]).unwrap();
        let q = |sql: &str| conn.query_row::<i64, _, _>(sql, [], |r| r.get(0)).unwrap();
        assert_eq!(q("SELECT count(*) FROM v WHERE i32 >= 0"), 4); // 0,1,2,3
        assert_eq!(q("SELECT count(*) FROM v WHERE u32 < 100"), 3); // 10,50,99
        assert_eq!(q("SELECT count(*) FROM v WHERE f64 > 1.5"), 2); // 2.0,2.5
        assert_eq!(q("SELECT count(*) FROM v WHERE b = true"), 3); // 3 trues
        assert_eq!(q("SELECT count(*) FROM v WHERE s = 'x'"), 3); // 3 x's
        drop(reg);
    }

    // Direct unit tests for new compare arms — DuckDB may apply these above the scan
    // rather than pushing them, so SQL tests alone don't prove the Rust arms ran.
    #[test]
    fn evaluate_compare_int32_direct() {
        use super::filter::{CmpOp, FilterNode, ScalarVal, evaluate};
        // sample_types(): i32 (col 0) = [-1, 0, 1, 2, 3]; i32 >= 0 → [false, true, true, true, true]
        let batch = sample_types();
        let node = FilterNode::Compare {
            path: vec![0],
            op: CmpOp::Ge,
            val: ScalarVal::I64(0),
        };
        let mask = evaluate(&node, &batch).unwrap();
        assert_eq!(
            mask.iter().map(|v| v.unwrap_or(false)).collect::<Vec<_>>(),
            vec![false, true, true, true, true]
        );
    }

    #[test]
    fn evaluate_compare_uint32_direct() {
        use super::filter::{CmpOp, FilterNode, ScalarVal, evaluate};
        // sample_types(): u32 (col 1) = [10, 50, 99, 100, 200]; u32 < 100 → [true, true, true, false, false]
        let batch = sample_types();
        let node = FilterNode::Compare {
            path: vec![1],
            op: CmpOp::Lt,
            val: ScalarVal::U64(100),
        };
        let mask = evaluate(&node, &batch).unwrap();
        assert_eq!(
            mask.iter().map(|v| v.unwrap_or(false)).collect::<Vec<_>>(),
            vec![true, true, true, false, false]
        );
    }

    #[test]
    fn evaluate_compare_float64_direct() {
        use super::filter::{CmpOp, FilterNode, ScalarVal, evaluate};
        // sample_types(): f64 (col 2) = [0.5, 1.0, 1.5, 2.0, 2.5]; f64 > 1.5 → [false, false, false, true, true]
        let batch = sample_types();
        let node = FilterNode::Compare {
            path: vec![2],
            op: CmpOp::Gt,
            val: ScalarVal::F64(1.5),
        };
        let mask = evaluate(&node, &batch).unwrap();
        assert_eq!(
            mask.iter().map(|v| v.unwrap_or(false)).collect::<Vec<_>>(),
            vec![false, false, false, true, true]
        );
    }

    #[test]
    fn evaluate_compare_boolean_direct() {
        use super::filter::{CmpOp, FilterNode, ScalarVal, evaluate};
        // sample_types(): b (col 3) = [true, false, true, false, true]; b = true → [true, false, true, false, true]
        let batch = sample_types();
        let node = FilterNode::Compare {
            path: vec![3],
            op: CmpOp::Eq,
            val: ScalarVal::Bool(true),
        };
        let mask = evaluate(&node, &batch).unwrap();
        assert_eq!(
            mask.iter().map(|v| v.unwrap_or(false)).collect::<Vec<_>>(),
            vec![true, false, true, false, true]
        );
    }

    #[test]
    fn evaluate_compare_unsupported_combo_errors() {
        use super::filter::{CmpOp, FilterNode, ScalarVal, evaluate};
        // Int32 column (col 0) with an F64 scalar — no arm covers this combo; must fail loud.
        let node = FilterNode::Compare {
            path: vec![0],
            op: CmpOp::Eq,
            val: ScalarVal::F64(1.0),
        };
        assert!(
            evaluate(&node, &sample_types()).is_err(),
            "mismatched scalar/column type must error, not silently pass"
        );
    }

    #[test]
    fn evaluate_compare_out_of_range_scalar_errors() {
        use super::filter::{CmpOp, FilterNode, ScalarVal, evaluate};
        use arrow::array::Int8Array;
        use arrow::datatypes::{DataType, Field, Schema};
        // Int8 column; scalar 300 (> i8::MAX) must fail loud, NOT wrap to 44 and filter wrongly.
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int8, false)]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(Int8Array::from(vec![1i8, 2, 3]))]).unwrap();
        let node = FilterNode::Compare {
            path: vec![0],
            op: CmpOp::Ge,
            val: ScalarVal::I64(300),
        };
        assert!(
            evaluate(&node, &batch).is_err(),
            "out-of-range scalar must error, not wrap"
        );
    }

    #[test]
    fn drop_unregisters_view() {
        let conn = Connection::open_in_memory().unwrap();
        let reg = conn.register_arrow("v", vec![sample()]).unwrap();
        let n: i64 = conn.query_row("SELECT count(*) FROM v", [], |r| r.get(0)).unwrap();
        assert_eq!(n, 4);
        drop(reg);
        // After drop the view must be gone — query must error.
        assert!(
            conn.query_row::<i64, _, _>("SELECT count(*) FROM v", [], |r| r.get(0))
                .is_err(),
            "view must be unregistered after the ArrowView is dropped"
        );
    }

    #[test]
    fn mismatched_schemas_error() {
        use arrow::datatypes::{DataType, Field, Schema};
        let schema1 = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let schema2 = Arc::new(Schema::new(vec![Field::new("b", DataType::Utf8, false)]));
        let arr1: Arc<dyn arrow::array::Array> = Arc::new(arrow::array::Int64Array::from(vec![1i64]));
        let arr2: Arc<dyn arrow::array::Array> = Arc::new(arrow::array::StringArray::from(vec!["x"]));
        let batch1 = RecordBatch::try_new(schema1, vec![arr1]).unwrap();
        let batch2 = RecordBatch::try_new(schema2, vec![arr2]).unwrap();
        let conn = Connection::open_in_memory().unwrap();
        let result = conn.register_arrow("v", vec![batch1, batch2]);
        assert!(result.is_err(), "mismatched schemas must return Err");
    }

    #[test]
    fn struct_extract_filter_matches_appender() {
        use arrow::array::{ArrayRef, Int64Array, StructArray};
        use arrow::datatypes::{DataType, Field, Fields};
        // Column `s STRUCT(id BIGINT, score BIGINT)` with all-valid structs. (A null-struct row is
        // deliberately NOT used here: DuckDB's native Appender ingestion and its Arrow-scan path
        // diverge on struct-null propagation — extracting a field from a NULL struct is NULL in the
        // Arrow path but the Appender stores the child value — so a null-struct row is not a sound
        // oracle comparison. The null-struct masking semantics are asserted directly in
        // `struct_path_null_struct_masking_direct`.)
        let id = Arc::new(Int64Array::from(vec![1i64, 2, 3, 4])) as ArrayRef;
        let score = Arc::new(Int64Array::from(vec![10i64, 20, 30, 40])) as ArrayRef;
        let fields: Fields = vec![
            Field::new("id", DataType::Int64, false),
            Field::new("score", DataType::Int64, false),
        ]
        .into();
        let s = Arc::new(StructArray::new(fields.clone(), vec![id, score], None)) as ArrayRef;
        let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Struct(fields), true)]));
        let batch = RecordBatch::try_new(schema, vec![s]).unwrap();

        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE t (s STRUCT(id BIGINT, score BIGINT))").unwrap();
        {
            let mut app = conn.appender("t").unwrap();
            app.append_record_batch(batch.clone()).unwrap();
        }
        let reg = conn.register_arrow("v", vec![batch]).unwrap();

        let q = |sql: String| conn.query_row::<i64, _, _>(&sql, [], |r| r.get(0)).unwrap();
        for where_clause in ["WHERE s.id > 2", "WHERE s.score = 20", "WHERE s.id IS NOT NULL"] {
            assert_eq!(
                q(format!("SELECT count(*) FROM v {where_clause}")),
                q(format!("SELECT count(*) FROM t {where_clause}")),
                "struct-field filter must match the Appender oracle: {where_clause}"
            );
        }
        drop(reg);
    }

    #[test]
    fn struct_path_null_struct_masking_direct() {
        // Directly asserts the SQL-correct semantics for a NULL struct: extracting a field from a
        // null struct yields NULL, so `s.id > 2` must EXCLUDE a null-struct row (even when the
        // child slot holds a value), and `s.id IS NULL` must be TRUE for it. This is what
        // resolve_path's ancestor-valid masking provides; it cannot be checked against the Appender
        // oracle because DuckDB's native ingestion mishandles null-struct child values.
        use super::filter::{CmpOp, FilterNode, ScalarVal, evaluate};
        use arrow::array::{Array, ArrayRef, Int64Array, StructArray};
        use arrow::buffer::NullBuffer;
        use arrow::datatypes::{DataType, Field, Fields};
        let id = Arc::new(Int64Array::from(vec![1i64, 2, 3, 4])) as ArrayRef;
        let fields: Fields = vec![Field::new("id", DataType::Int64, false)].into();
        let nulls = NullBuffer::from(vec![true, true, false, true]); // row 2 = NULL struct
        let s = Arc::new(StructArray::new(fields.clone(), vec![id], Some(nulls))) as ArrayRef;
        let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Struct(fields), true)]));
        let batch = RecordBatch::try_new(schema, vec![s]).unwrap();

        let bits = |node: FilterNode| -> Vec<bool> {
            let m = evaluate(&node, &batch).unwrap();
            (0..m.len()).map(|i| m.is_valid(i) && m.value(i)).collect()
        };
        // s.id > 2 : row 2 (null struct) excluded despite child id=3; only row 3 (id=4) kept.
        assert_eq!(
            bits(FilterNode::Compare { path: vec![0, 0], op: CmpOp::Gt, val: ScalarVal::I64(2) }),
            vec![false, false, false, true],
            "null-struct row must be excluded from s.id > 2"
        );
        // s.id IS NULL : only the null-struct row (row 2) reads as NULL.
        assert_eq!(
            bits(FilterNode::IsNull { path: vec![0, 0] }),
            vec![false, false, true, false],
            "field of a null struct must read as NULL"
        );
    }

    #[test]
    fn timestamp_unknown_unit_byte_errors_not_panics() {
        // An out-of-range timestamp unit byte (from a corrupt/forward-incompatible wire buffer)
        // must fail loud (FilterError), NOT panic on an out-of-bounds factor-table index.
        use super::filter::{CmpOp, FilterNode, ScalarVal, evaluate};
        use arrow::array::TimestampMicrosecondArray;
        use arrow::datatypes::{DataType, Field, TimeUnit};
        let schema = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        )]));
        let ts = TimestampMicrosecondArray::from(vec![1_000_000i64, 2_000_000, 3_000_000]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(ts)]).unwrap();
        let node = FilterNode::Compare {
            path: vec![0],
            op: CmpOp::Gt,
            val: ScalarVal::Timestamp { unit: 9, v: 1_000_000 }, // 9 = invalid unit byte
        };
        assert!(
            evaluate(&node, &batch).is_err(),
            "unknown timestamp unit byte must error, not panic"
        );
    }

    #[test]
    fn typed_scalar_filters_match_appender() {
        use arrow::array::{ArrayRef, BinaryArray, Date32Array, TimestampMicrosecondArray};
        use arrow::datatypes::{DataType, Field, TimeUnit};
        // date d (Date32), ts (Timestamp µs, no tz), b (Binary)
        let d = Arc::new(Date32Array::from(vec![19000i32, 19001, 19002, 19003])) as ArrayRef; // days since epoch
        let ts = Arc::new(TimestampMicrosecondArray::from(vec![
            1_000_000i64, 2_000_000, 3_000_000, 4_000_000,
        ])) as ArrayRef;
        let b = Arc::new(BinaryArray::from(vec![
            b"aa".as_ref(), b"bb".as_ref(), b"cc".as_ref(), b"dd".as_ref(),
        ])) as ArrayRef;
        let schema = Arc::new(Schema::new(vec![
            Field::new("d", DataType::Date32, false),
            Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, None), false),
            Field::new("b", DataType::Binary, false),
        ]));
        let batch = RecordBatch::try_new(schema, vec![d, ts, b]).unwrap();

        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE t (d DATE, ts TIMESTAMP, b BLOB)").unwrap();
        {
            let mut app = conn.appender("t").unwrap();
            app.append_record_batch(batch.clone()).unwrap();
        }
        let reg = conn.register_arrow("v", vec![batch]).unwrap();

        let q = |sql: String| conn.query_row::<i64, _, _>(&sql, [], |r| r.get(0)).unwrap();
        for where_clause in [
            "WHERE d > DATE '2022-01-02'",
            "WHERE ts >= TIMESTAMP '1970-01-01 00:00:03'",
            "WHERE b = '\\x63\\x63'::BLOB",
        ] {
            assert_eq!(
                q(format!("SELECT count(*) FROM v {where_clause}")),
                q(format!("SELECT count(*) FROM t {where_clause}")),
                "typed-scalar filter must match the Appender oracle: {where_clause}"
            );
        }
        drop(reg);
    }

    #[test]
    fn decimal128_filter_matches_appender() {
        use arrow::array::{ArrayRef, Decimal128Array};
        use arrow::datatypes::{DataType, Field};
        // DECIMAL(38,2): values 1.00, 2.50, 3.00, 4.25 → unscaled i128 *100
        let dec = Arc::new(
            Decimal128Array::from(vec![100i128, 250, 300, 425])
                .with_precision_and_scale(38, 2)
                .unwrap(),
        ) as ArrayRef;
        let schema = Arc::new(Schema::new(vec![Field::new("p", DataType::Decimal128(38, 2), false)]));
        let batch = RecordBatch::try_new(schema, vec![dec]).unwrap();

        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE t (p DECIMAL(38,2))").unwrap();
        {
            let mut app = conn.appender("t").unwrap();
            app.append_record_batch(batch.clone()).unwrap();
        }
        let reg = conn.register_arrow("v", vec![batch]).unwrap();
        let q = |sql: String| conn.query_row::<i64, _, _>(&sql, [], |r| r.get(0)).unwrap();
        for where_clause in ["WHERE p > 2.50", "WHERE p = 3.00", "WHERE p <= 2.50"] {
            assert_eq!(
                q(format!("SELECT count(*) FROM v {where_clause}")),
                q(format!("SELECT count(*) FROM t {where_clause}")),
                "decimal128 filter must match the Appender oracle: {where_clause}"
            );
        }
        drop(reg);
    }

    #[test]
    fn nan_compare_total_order_direct() {
        use super::filter::{CmpOp, FilterNode, ScalarVal, evaluate};
        use arrow::array::{Array, Float64Array};
        use arrow::datatypes::{DataType, Field};
        // values: 1.0, NaN, 3.0, NULL
        let schema = Arc::new(Schema::new(vec![Field::new("f", DataType::Float64, true)]));
        let arr = Float64Array::from(vec![Some(1.0), Some(f64::NAN), Some(3.0), None]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();
        let eval = |op| {
            let node = FilterNode::Compare { path: vec![0], op, val: ScalarVal::F64(f64::NAN) };
            let m = evaluate(&node, &batch).unwrap();
            (0..m.len()).map(|i| m.is_valid(i) && m.value(i)).collect::<Vec<bool>>()
        };
        // DuckDB total order: NaN is the largest; NaN == NaN; NULL never matches.
        assert_eq!(eval(CmpOp::Eq), vec![false, true, false, false]);   // = NaN → only NaN
        assert_eq!(eval(CmpOp::Ne), vec![true, false, true, false]);    // <> NaN → non-NaN, non-null
        assert_eq!(eval(CmpOp::Lt), vec![true, false, true, false]);    // < NaN → everything below NaN
        assert_eq!(eval(CmpOp::Le), vec![true, true, true, false]);     // <= NaN → all non-null
        assert_eq!(eval(CmpOp::Gt), vec![false, false, false, false]);  // > NaN → nothing
        assert_eq!(eval(CmpOp::Ge), vec![false, true, false, false]);   // >= NaN → only NaN
    }

    #[test]
    fn multi_column_projection_and_filter_matches_appender() {
        use arrow::array::{ArrayRef, Int64Array};
        use arrow::datatypes::{DataType, Field, Schema};
        // 6 Int64 columns; filter columns that are NOT first in projection order. A filter<->column
        // mapping bug silently returns wrong rows here — regression guard for the reverse-map bug.
        let n = 20i64;
        let fields: Vec<Field> = (0..6)
            .map(|c| Field::new(format!("c{c}"), DataType::Int64, false))
            .collect();
        let schema = Arc::new(Schema::new(fields));
        let cols: Vec<ArrayRef> = (0..6)
            .map(|c| Arc::new(Int64Array::from_iter_values((0..n).map(|i| i * 10 + c as i64))) as ArrayRef)
            .collect();
        let batch = RecordBatch::try_new(schema, cols).unwrap();

        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE t (c0 BIGINT, c1 BIGINT, c2 BIGINT, c3 BIGINT, c4 BIGINT, c5 BIGINT)")
            .unwrap();
        {
            let mut app = conn.appender("t").unwrap();
            app.append_record_batch(batch.clone()).unwrap();
        }
        let reg = conn.register_arrow("v", vec![batch]).unwrap();

        // Projects {c1,c3,c5} (a non-prefix subset) and filters non-first columns. The Appender
        // table `t` is the oracle; the zero-copy view `v` must agree.
        let where_clause = "WHERE c5 > 50 AND c1 > 30 AND c3 > 20";
        let q = |sql: String| conn.query_row::<i64, _, _>(&sql, [], |r| r.get(0)).unwrap();
        assert_eq!(
            q(format!("SELECT count(*) FROM v {where_clause}")),
            q(format!("SELECT count(*) FROM t {where_clause}")),
            "row count must match the Appender oracle"
        );
        let v_sum = q(format!("SELECT COALESCE(sum(c1), 0) FROM v {where_clause}"));
        let t_sum = q(format!("SELECT COALESCE(sum(c1), 0) FROM t {where_clause}"));
        assert_eq!(v_sum, t_sum, "projected/filtered value must match the Appender oracle");
        assert!(v_sum > 0, "the test query should select some rows");
        drop(reg);
    }
}

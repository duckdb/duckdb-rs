//! Benchmark: zero-copy `register_arrow` vs the Appender (copy), cold per-task
//! (open connection + ingest + query). Two queries are measured:
//!   1. Unfiltered projection: `SELECT sum(c0) FROM …`
//!   2. Filtered + projected:  `SELECT sum(c0) FROM … WHERE c0 > n/2`
//!
//! The table is wide (8 int64 columns) with light compute and single-column projection,
//! so both the skipped ingest copy AND projection+filter pushdown show clearly.
//!
//! Run:
//!   cargo run -p duckdb --features "bundled appender-arrow" --release --example arrow_zerocopy_bench

use std::sync::Arc;
use std::time::Instant;

use duckdb::Connection;
use duckdb::arrow::array::Int64Array;
use duckdb::arrow::datatypes::{DataType, Field, Schema};
use duckdb::arrow::record_batch::RecordBatch;

const COLS: usize = 8;

fn make_batch(n: i64) -> RecordBatch {
    let fields: Vec<Field> = (0..COLS)
        .map(|c| Field::new(format!("c{c}"), DataType::Int64, false))
        .collect();
    let schema = Arc::new(Schema::new(fields));
    let cols: Vec<Arc<dyn duckdb::arrow::array::Array>> = (0..COLS)
        .map(|c| Arc::new(Int64Array::from_iter_values((0..n).map(|i| i + c as i64))) as _)
        .collect();
    RecordBatch::try_new(schema, cols).unwrap()
}

fn ddl() -> String {
    let cols: Vec<String> = (0..COLS).map(|c| format!("c{c} BIGINT")).collect();
    format!("CREATE TABLE t ({})", cols.join(", "))
}

fn appender_cold(batch: &RecordBatch, query: &str) -> i64 {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(&ddl()).unwrap();
    {
        let mut app = conn.appender("t").unwrap();
        app.append_record_batch(batch.clone()).unwrap();
    }
    conn.query_row(query, [], |r| r.get(0)).unwrap()
}

fn zerocopy_cold(batch: &RecordBatch, query: &str) -> i64 {
    let conn = Connection::open_in_memory().unwrap();
    let reg = conn.register_arrow("v", vec![batch.clone()]).unwrap();
    let out: i64 = conn.query_row(query, [], |r| r.get(0)).unwrap();
    drop(reg);
    out
}

fn main() {
    let sizes = [100_000i64, 1_000_000, 10_000_000];
    let k = 5;

    // Warm up (first connection + first arrow_scan pay one-time inits).
    let warm = make_batch(1000);
    assert_eq!(
        appender_cold(&warm, "SELECT sum(c0) FROM t"),
        zerocopy_cold(&warm, "SELECT sum(c0) FROM v"),
        "warm-up results must agree (unfiltered)"
    );

    println!("Copy-dominated cold per-task: zero-copy arrow_scan vs Appender (wide table, {COLS} cols)");

    // ── Unfiltered query ────────────────────────────────────────────────────────
    println!("\n[Unfiltered] SELECT sum(c0) FROM …");
    println!(
        "{:>12} {:>14} {:>15} {:>9}",
        "rows", "appender ms", "zero-copy ms", "speedup"
    );
    for &n in &sizes {
        let batch = make_batch(n);
        let q_app = "SELECT sum(c0) FROM t";
        let q_zc = "SELECT sum(c0) FROM v";
        assert_eq!(
            appender_cold(&batch, q_app),
            zerocopy_cold(&batch, q_zc),
            "results must agree (unfiltered) at n={n}"
        );

        let t0 = Instant::now();
        for _ in 0..k {
            let _ = appender_cold(&batch, q_app);
        }
        let app_ms = t0.elapsed().as_secs_f64() * 1000.0 / k as f64;

        let t1 = Instant::now();
        for _ in 0..k {
            let _ = zerocopy_cold(&batch, q_zc);
        }
        let zc_ms = t1.elapsed().as_secs_f64() * 1000.0 / k as f64;

        println!("{:>12} {:>14.1} {:>15.1} {:>8.2}x", n, app_ms, zc_ms, app_ms / zc_ms);
    }

    // ── Filtered + projected query ──────────────────────────────────────────────
    println!("\n[Filtered] SELECT sum(c0) FROM … WHERE c0 > n/2");
    println!(
        "{:>12} {:>14} {:>15} {:>9}",
        "rows", "appender ms", "zero-copy ms", "speedup"
    );
    for &n in &sizes {
        let batch = make_batch(n);
        let q_app = format!("SELECT sum(c0) FROM t WHERE c0 > {}", n / 2);
        let q_zc = format!("SELECT sum(c0) FROM v WHERE c0 > {}", n / 2);
        assert_eq!(
            appender_cold(&batch, &q_app),
            zerocopy_cold(&batch, &q_zc),
            "results must agree (filtered) at n={n}"
        );

        let t0 = Instant::now();
        for _ in 0..k {
            let _ = appender_cold(&batch, &q_app);
        }
        let app_ms = t0.elapsed().as_secs_f64() * 1000.0 / k as f64;

        let t1 = Instant::now();
        for _ in 0..k {
            let _ = zerocopy_cold(&batch, &q_zc);
        }
        let zc_ms = t1.elapsed().as_secs_f64() * 1000.0 / k as f64;

        println!("{:>12} {:>14.1} {:>15.1} {:>8.2}x", n, app_ms, zc_ms, app_ms / zc_ms);
    }
}

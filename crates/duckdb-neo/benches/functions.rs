//! Benchmarks for user-defined functions built on duckdb-neo.
//!
//! Each benchmark registers its function on a fresh in-memory database, creates its input
//! once, checks the function's output, and then times the query. See `README.md` in this
//! directory for running, filtering and comparing them.

mod support;

use divan::{Bencher, counter::ItemsCount};
use duckdb_neo::{Parameters, connection::Connection};
use support::{Fixture, base56, bench_rows, dash_split, merge_struct, row_hash};

fn main() {
    divan::main();
}

fn bench_query(bencher: Bencher<'_, '_>, conn: &Connection, sql: &str, rows: usize) {
    // Non-cacheable plans rebind during execution, which is part of the measurement.
    let statement = conn.prepare(sql, false).unwrap();
    bencher
        .counter(ItemsCount::new(rows))
        .bench_local(|| statement.execute(Parameters::None).unwrap().drain().unwrap());
}

mod scalar {
    use super::*;

    #[divan::bench(args = bench_rows())]
    fn merge_struct(bencher: Bencher<'_, '_>, rows: usize) {
        let fixture = Fixture::new().unwrap();
        merge_struct::create_input(&fixture.conn, rows).unwrap();
        merge_struct::verify(&fixture.conn).unwrap();
        bench_query(bencher, &fixture.conn, merge_struct::QUERY, rows);
    }
}

mod table {
    use super::*;

    #[divan::bench(args = bench_rows())]
    fn dash_split(bencher: Bencher<'_, '_>, rows: usize) {
        let fixture = Fixture::new().unwrap();
        dash_split::create_input(&fixture.conn, rows).unwrap();
        dash_split::verify(&fixture.conn).unwrap();
        bench_query(bencher, &fixture.conn, &dash_split::query(rows), rows);
    }
}

mod aggregate {
    use super::*;

    #[divan::bench(args = bench_rows())]
    fn row_hash_ungrouped(bencher: Bencher<'_, '_>, rows: usize) {
        let fixture = Fixture::new().unwrap();
        row_hash::create_input(&fixture.conn, rows).unwrap();
        row_hash::verify(&fixture.conn).unwrap();
        bench_query(bencher, &fixture.conn, row_hash::UNGROUPED_QUERY, rows);
    }

    #[divan::bench(args = bench_rows())]
    fn row_hash_grouped(bencher: Bencher<'_, '_>, rows: usize) {
        let fixture = Fixture::new().unwrap();
        row_hash::create_input(&fixture.conn, rows).unwrap();
        row_hash::verify(&fixture.conn).unwrap();
        bench_query(bencher, &fixture.conn, row_hash::GROUPED_QUERY, rows);
    }
}

mod copy {
    use super::*;

    fn fixture(rows: usize) -> (Fixture, tempfile::TempDir, String) {
        let fixture = Fixture::new().unwrap();
        base56::create_input(&fixture.conn, rows).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("data.b56").to_str().unwrap().to_owned();
        base56::verify(&fixture.conn, &path).unwrap();
        (fixture, dir, path)
    }

    #[divan::bench(args = bench_rows())]
    fn base56_to(bencher: Bencher<'_, '_>, rows: usize) {
        let (fixture, _dir, path) = fixture(rows);
        bench_query(bencher, &fixture.conn, &base56::copy_to_sql(&path), rows);
    }

    #[divan::bench(args = bench_rows(), sample_size = 1)]
    fn base56_from(bencher: Bencher<'_, '_>, rows: usize) {
        let (fixture, _dir, path) = fixture(rows);
        let conn = &fixture.conn;
        fixture.drain(&base56::copy_to_sql(&path)).unwrap();
        base56::create_target(conn, "copy_target").unwrap();
        let copy = conn
            .prepare(base56::copy_from_sql("copy_target", &path).as_str(), false)
            .unwrap();
        let truncate = conn.prepare("TRUNCATE copy_target", false).unwrap();
        bencher
            .counter(ItemsCount::new(rows))
            .with_inputs(|| truncate.execute(Parameters::None).unwrap().drain().unwrap())
            .bench_local_values(|_| copy.execute(Parameters::None).unwrap().drain().unwrap());
    }
}

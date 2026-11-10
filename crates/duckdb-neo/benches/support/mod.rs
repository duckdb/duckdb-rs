//! The functions under benchmark, their input tables, and correctness checks.
//!
//! Shared by the `functions` benchmark and the `bench_functions` test, which runs every
//! [`verify`](merge_struct::verify) check against small inputs.
//!
//! `VARCHAR` vectors are read as [`UncheckedString`]: DuckDB guarantees they hold valid UTF-8,
//! so the readers skip validating every string again.

pub mod base56;
pub mod dash_split;
pub mod merge_struct;
pub mod row_hash;

use duckdb_neo::{
    Error, Parameters, Result,
    connection::Connection,
    environment::{Environment, StorageLocation},
    instance::{AttachBuilder, Instance, InstanceBuilder},
    types::UncheckedString,
    vector::{StorageKind, Unknown, Vector, VectorElement},
};

/// Row counts benchmarked when `NEO_BENCH_ROWS` is unset.
pub const DEFAULT_ROWS: [usize; 4] = [1_000, 10_000, 100_000, 1_000_000];

/// Row counts from `NEO_BENCH_ROWS` (comma separated), else [`DEFAULT_ROWS`].
///
/// Test runs (`cargo test --benches`, or `--test`) only use the smallest count.
pub fn bench_rows() -> Vec<usize> {
    if let Ok(rows) = std::env::var("NEO_BENCH_ROWS") {
        return rows
            .split(',')
            .map(|rows| {
                rows.trim()
                    .replace('_', "")
                    .parse()
                    .expect("NEO_BENCH_ROWS holds row counts")
            })
            .collect();
    }
    let has_flag = |flag: &str| std::env::args().any(|arg| arg == flag);
    if has_flag("--bench") && !has_flag("--test") {
        DEFAULT_ROWS.to_vec()
    } else {
        DEFAULT_ROWS[..1].to_vec()
    }
}

/// An in-memory database with every benchmark function registered.
///
/// DuckDB runs with `NEO_BENCH_THREADS` threads, default 1, so results reflect
/// per-row cost rather than the runner's core count.
pub struct Fixture {
    pub conn: Connection,
    _instance: Instance,
    _environment: Environment,
}

impl Fixture {
    pub fn new() -> Result<Self> {
        let environment = Environment::new()?;
        let threads = std::env::var("NEO_BENCH_THREADS").unwrap_or_else(|_| "1".to_owned());
        let instance = InstanceBuilder::new().option("threads", threads).build(&environment)?;
        instance.attach(AttachBuilder::new(StorageLocation::InMemory).make_default())?;
        let conn = instance.connect()?;
        merge_struct::register(&conn)?;
        dash_split::register(&conn)?;
        row_hash::register(&conn)?;
        base56::register(&conn)?;
        Ok(Self {
            conn,
            _instance: instance,
            _environment: environment,
        })
    }

    /// Run `sql` and discard its rows.
    pub fn drain(&self, sql: &str) -> Result<()> {
        self.conn.query(sql, Parameters::None)?.drain()?;
        Ok(())
    }
}

/// Narrow an argument vector, flattening storage that readers cannot read directly.
pub fn readable<T: VectorElement>(vector: Vector<'_, Unknown>) -> Result<Vector<'_, T>> {
    let mut vector = vector.cast::<T>()?;
    if vector.storage_kind() == StorageKind::Other {
        vector.flatten()?;
    }
    Ok(vector)
}

pub fn ensure(condition: bool, message: impl FnOnce() -> String) -> Result<()> {
    if condition {
        Ok(())
    } else {
        Err(Error::invalid_input(message()))
    }
}

/// Return the first value of a single-row query.
pub fn query_u64(conn: &Connection, sql: &str) -> Result<Option<u64>> {
    let chunk = conn.query(sql, Parameters::None)?.next_chunk()?;
    let chunk = chunk.ok_or_else(|| Error::invalid_input(format!("{sql} returned no rows")))?;
    Ok(chunk.get_vector_at::<u64>(0)?.reader()?.get(0)?.copied())
}

/// Return the first value of a single-row query that must not be NULL.
pub fn query_i64(conn: &Connection, sql: &str) -> Result<i64> {
    let chunk = conn.query(sql, Parameters::None)?.next_chunk()?;
    let chunk = chunk.ok_or_else(|| Error::invalid_input(format!("{sql} returned no rows")))?;
    let value = chunk.get_vector_at::<i64>(0)?.reader()?.get(0)?.copied();
    value.ok_or_else(|| Error::invalid_input(format!("{sql} returned NULL")))
}

/// Return the first value of a single-row query that must not be NULL.
pub fn query_string(conn: &Connection, sql: &str) -> Result<String> {
    let chunk = conn.query(sql, Parameters::None)?.next_chunk()?;
    let chunk = chunk.ok_or_else(|| Error::invalid_input(format!("{sql} returned no rows")))?;
    let value = chunk
        .get_vector_at::<UncheckedString>(0)?
        .reader()?
        .get(0)?
        .map(str::to_owned);
    value.ok_or_else(|| Error::invalid_input(format!("{sql} returned NULL")))
}

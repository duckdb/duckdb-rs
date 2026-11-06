//! Correctness checks for the functions in `benches/support`, on inputs small enough for debug builds.

#[allow(dead_code)]
#[path = "../benches/support/mod.rs"]
mod support;

use duckdb_neo::Result;
use support::{Fixture, base56, dash_split, merge_struct, row_hash};

const ROWS: usize = 5_000;

#[test]
fn merge_struct_packs_arguments() -> Result<()> {
    let fixture = Fixture::new()?;
    merge_struct::create_input(&fixture.conn, ROWS)?;
    merge_struct::verify(&fixture.conn)?;
    let output_type = support::query_string(
        &fixture.conn,
        "SELECT typeof(merge_struct(i, s, m, u, d)) FROM scalar_input LIMIT 1",
    )?;
    assert_eq!(
        output_type,
        "STRUCT(i INTEGER, s VARCHAR, m MAP(VARCHAR, INTEGER), u UNION(num INTEGER, str VARCHAR), d DECIMAL(18,3))"
    );
    Ok(())
}

#[test]
fn dash_split_splits_into_n_parts() -> Result<()> {
    let fixture = Fixture::new()?;
    dash_split::create_input(&fixture.conn, ROWS)?;
    dash_split::verify(&fixture.conn)
}

#[test]
fn row_hash_is_order_independent() -> Result<()> {
    let fixture = Fixture::new()?;
    row_hash::create_input(&fixture.conn, ROWS)?;
    row_hash::verify(&fixture.conn)
}

#[test]
fn base56_round_trips() -> Result<()> {
    let fixture = Fixture::new()?;
    base56::create_input(&fixture.conn, ROWS)?;
    let dir = tempfile::tempdir().unwrap();
    base56::verify(&fixture.conn, dir.path().join("data.b56").to_str().unwrap())
}

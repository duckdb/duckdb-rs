//! Correctness checks for the functions in `benches/support`, on inputs small enough for debug builds.

#[allow(dead_code)]
#[path = "../benches/support/mod.rs"]
mod support;

use duckdb_neo::{Parameters, Result, connection::Connection};
use support::{Fixture, base56, dash_split, merge_struct, nulls::Nulls, row_hash};

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
fn merge_struct_verification_rejects_null_structs() -> Result<()> {
    let fixture = Fixture::new()?;
    merge_struct::create_input_with_nulls(&fixture.conn, ROWS, Nulls::new(100)?)?;
    fixture.conn.execute(
        "CREATE OR REPLACE MACRO merge_struct(i, s, m, u, d) AS
         NULL::STRUCT(i INTEGER, s VARCHAR, m MAP(VARCHAR, INTEGER),
                      u UNION(num INTEGER, str VARCHAR), d DECIMAL(18, 3))",
        Parameters::None,
    )?;
    assert!(merge_struct::verify(&fixture.conn).is_err());
    Ok(())
}

#[test]
fn merge_struct_preserves_null_and_empty_maps() -> Result<()> {
    let fixture = Fixture::new()?;
    merge_struct::create_input_with_nulls(&fixture.conn, ROWS, Nulls::new(0)?)?;
    fixture.conn.execute(
        "UPDATE scalar_input SET m = CASE i % 5
            WHEN 0 THEN NULL
            WHEN 1 THEN MAP {}::MAP(VARCHAR, INTEGER)
            WHEN 2 THEN MAP {'nullable': NULL::INTEGER, 'value': i}
            ELSE m END",
        Parameters::None,
    )?;
    merge_struct::verify(&fixture.conn)
}

#[test]
fn merge_struct_preserves_varying_maps_and_long_keys() -> Result<()> {
    let fixture = Fixture::new()?;
    merge_struct::create_input_with_nulls(&fixture.conn, ROWS, Nulls::new(0)?)?;
    fixture.conn.execute(
        "UPDATE scalar_input SET m = CASE i % 11
            WHEN 0 THEN NULL
            WHEN 1 THEN MAP {}::MAP(VARCHAR, INTEGER)
            ELSE map_from_entries(list_transform(range(i % 17), lambda j: {
                'key': 'long map key for row ' || i || ' entry ' || j,
                'value': CASE WHEN j % 3 = 0 THEN NULL ELSE (i + j)::INTEGER END
            })) END",
        Parameters::None,
    )?;
    merge_struct::verify(&fixture.conn)
}

#[test]
fn merge_struct_preserves_null_unions_and_members() -> Result<()> {
    let fixture = Fixture::new()?;
    merge_struct::create_input_with_nulls(&fixture.conn, ROWS, Nulls::new(0)?)?;
    fixture.conn.execute(
        "UPDATE scalar_input SET u = CASE i % 5
            WHEN 0 THEN NULL
            WHEN 1 THEN union_value(num := NULL::INTEGER)
            WHEN 2 THEN union_value(str := NULL::VARCHAR)
            ELSE u END",
        Parameters::None,
    )?;
    merge_struct::verify(&fixture.conn)?;
    assert_eq!(
        support::query_i64(
            &fixture.conn,
            "SELECT count(*) FROM (SELECT merge_struct(i, s, m, u, d) AS r, u FROM scalar_input)
             WHERE union_tag(r.u) IS DISTINCT FROM union_tag(u)",
        )?,
        0
    );
    Ok(())
}

#[test]
fn merge_struct_preserves_selected_and_constant_unions() -> Result<()> {
    let fixture = Fixture::new()?;
    merge_struct::create_input(&fixture.conn, ROWS)?;
    fixture
        .conn
        .execute("ALTER TABLE scalar_input RENAME TO scalar_source", Parameters::None)?;
    for source in [
        "SELECT * FROM scalar_source WHERE i % 5 <> 0",
        "SELECT i, s, m, union_value(str := 'a long constant string')::UNION(num INTEGER, str VARCHAR) AS u, d
         FROM scalar_source",
        "SELECT i, s, m, union_value(num := NULL::INTEGER)::UNION(num INTEGER, str VARCHAR) AS u, d
         FROM scalar_source",
        "SELECT i, s, m, NULL::UNION(num INTEGER, str VARCHAR) AS u, d FROM scalar_source",
    ] {
        fixture.conn.execute(
            format!("CREATE OR REPLACE VIEW scalar_input AS {source}").as_str(),
            Parameters::None,
        )?;
        merge_struct::verify(&fixture.conn)?;
    }
    Ok(())
}

#[test]
fn merge_struct_preserves_selected_and_constant_maps() -> Result<()> {
    let fixture = Fixture::new()?;
    merge_struct::create_input_with_nulls(&fixture.conn, ROWS, Nulls::new(0)?)?;
    fixture
        .conn
        .execute("ALTER TABLE scalar_input RENAME TO scalar_source", Parameters::None)?;
    for source in [
        "SELECT * FROM scalar_source WHERE i % 5 <> 0",
        "SELECT i, s, MAP {'a long constant key': 42::INTEGER, 'nullable': NULL::INTEGER} AS m, u, d
         FROM scalar_source",
        "SELECT i, s, MAP {}::MAP(VARCHAR, INTEGER) AS m, u, d FROM scalar_source",
        "SELECT i, s, NULL::MAP(VARCHAR, INTEGER) AS m, u, d FROM scalar_source",
    ] {
        fixture.conn.execute(
            format!("CREATE OR REPLACE VIEW scalar_input AS {source}").as_str(),
            Parameters::None,
        )?;
        merge_struct::verify(&fixture.conn)?;
    }
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
#[ignore = "requires NEO_BENCH_CPP pointing to the C++ benchmark executable"]
fn base56_cpp_interoperability() -> Result<()> {
    let executable = std::env::var("NEO_BENCH_CPP").expect("set NEO_BENCH_CPP to neo_cpp_bench");
    let fixture = Fixture::new()?;
    for percent in [0, 10, 100] {
        base56::create_input_with_nulls(&fixture.conn, 1_000, Nulls::new(percent)?)?;
        let dir = tempfile::tempdir().unwrap();
        let rust = dir.path().join("rust.b56");
        let cpp = dir.path().join("cpp.b56");
        fixture.drain(&base56::copy_to_sql(rust.to_str().unwrap()))?;
        let output = std::process::Command::new(&executable)
            .env("NEO_BENCH_NULL_PERCENT", percent.to_string())
            .args(["--import", rust.to_str().unwrap(), "--export", cpp.to_str().unwrap()])
            .output()
            .unwrap();
        assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
        assert_eq!(std::fs::read(&rust).unwrap(), std::fs::read(&cpp).unwrap());
        base56::create_target(&fixture.conn, "copy_verify")?;
        fixture.drain(&base56::copy_from_sql("copy_verify", cpp.to_str().unwrap()))?;
        assert_eq!(
            support::query_i64(
                &fixture.conn,
                "SELECT count(*) FROM ((FROM copy_input EXCEPT ALL FROM copy_verify)
                 UNION ALL (FROM copy_verify EXCEPT ALL FROM copy_input))",
            )?,
            0
        );
    }
    Ok(())
}

fn assert_null_rate(conn: &Connection, source: &str, column: &str, percent: u8) -> Result<()> {
    let rows = support::query_i64(conn, &format!("SELECT count(*) FROM {source}"))?;
    let nulls = support::query_i64(conn, &format!("SELECT count(*) FROM {source} WHERE {column} IS NULL"))?;
    match percent {
        0 => assert_eq!(nulls, 0, "{source}.{column}"),
        100 => assert_eq!(nulls, rows, "{source}.{column}"),
        _ => {
            assert!(rows > 0, "{source}.{column}");
            let observed = nulls as f64 / rows as f64;
            assert!(
                (observed - f64::from(percent) / 100.0).abs() < 0.02,
                "{source}.{column}: {nulls}/{rows} NULL, expected about {percent}%"
            );
        }
    }
    Ok(())
}

fn assert_null_pattern(conn: &Connection, source: &str, column: &str, seed: u8, percent: u8) -> Result<()> {
    assert_null_rate(conn, source, column, percent)?;
    assert_eq!(
        support::query_i64(
            conn,
            &format!(
                "SELECT count(*) FROM {source} WHERE ({column} IS NULL) IS DISTINCT FROM
         (hash(rowid::UBIGINT, {seed}::UBIGINT) % 100 < {percent})"
            )
        )?,
        0,
        "{source}.{column}: NULL pattern differs"
    );
    Ok(())
}

#[test]
fn fixtures_respect_null_percentages() -> Result<()> {
    let fixture = Fixture::new()?;
    let conn = &fixture.conn;
    for percent in [0, 10, 100] {
        let nulls = Nulls::new(percent)?;
        merge_struct::create_input_with_nulls(conn, 10_000, nulls)?;
        assert_eq!(support::query_i64(conn, "SELECT count(*) FROM scalar_input")?, 10_000);
        merge_struct::verify(conn)?;
        for (column, seed) in [("i", 0), ("s", 1), ("m", 2), ("u", 3), ("d", 4)] {
            assert_null_pattern(conn, "scalar_input", column, seed, percent)?;
        }
        for (key, seed) in [("a", 5), ("b", 6), ("key_%", 7)] {
            let source = format!(
                "(SELECT rowid, entry.value AS v FROM scalar_input, UNNEST(map_entries(m)) t(entry)
                  WHERE entry.key LIKE '{key}')"
            );
            assert_null_pattern(conn, &source, "v", seed, percent)?;
        }
        for (member, seed) in [("num", 8), ("str", 9)] {
            let source = format!(
                "(SELECT rowid, union_extract(u, '{member}') AS v FROM scalar_input
                  WHERE u IS NOT NULL AND union_tag(u) = '{member}')"
            );
            assert_null_pattern(conn, &source, "v", seed, percent)?;
        }
        if percent == 10 {
            assert!(
                support::query_i64(
                    conn,
                    "SELECT count(*) FROM scalar_input WHERE (s IS NULL) <> (d IS NULL)"
                )? > 0
            );
        }
        row_hash::create_input_with_nulls(conn, 10_000, nulls)?;
        assert_eq!(support::query_i64(conn, "SELECT count(*) FROM agg_input")?, 10_000);
        row_hash::verify(conn)?;
        for (column, seed) in [("s", 10), ("d", 11), ("b", 12)] {
            assert_null_pattern(conn, "agg_input", column, seed, percent)?;
        }
        assert_null_rate(conn, "agg_input", "g", 0)?;
        base56::create_input_with_nulls(conn, 10_000, nulls)?;
        assert_eq!(support::query_i64(conn, "SELECT count(*) FROM copy_input")?, 10_000);
        for (column, seed) in [("id", 20), ("x", 21), ("name", 22), ("flag", 23), ("bucket", 24)] {
            assert_null_pattern(conn, "copy_input", column, seed, percent)?;
        }
        let dir = tempfile::tempdir().unwrap();
        base56::verify(conn, dir.path().join("data.b56").to_str().unwrap())?;
    }
    Ok(())
}

#[test]
fn base56_round_trips() -> Result<()> {
    let fixture = Fixture::new()?;
    base56::create_input(&fixture.conn, ROWS)?;
    let dir = tempfile::tempdir().unwrap();
    base56::verify(&fixture.conn, dir.path().join("data.b56").to_str().unwrap())
}

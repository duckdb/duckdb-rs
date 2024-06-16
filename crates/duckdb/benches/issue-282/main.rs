//! ensure that the databases are generated using the `generate-database.sh` utility
use arrow_convert::{ArrowDeserialize, ArrowField, ArrowSerialize};
use criterion::{criterion_group, criterion_main, Criterion};

pub fn sqlite_db() -> rusqlite::Connection {
    let db_path = concat!(env!("CARGO_MANIFEST_DIR"), "/benches/issue-282/data/db.sqlite");
    rusqlite::Connection::open(db_path).unwrap()
}

pub fn duck_db() -> duckdb::Connection {
    let db_path = concat!(env!("CARGO_MANIFEST_DIR"), "/benches/issue-282/data/db.duckdb");
    duckdb::Connection::open(db_path).unwrap()
}

#[derive(Debug, ArrowField, ArrowSerialize, ArrowDeserialize)]
struct Income {
    created_at: Option<i32>,
    amount: Option<f32>,
    category_id: i32,
    wallet_id: Option<i32>,
    meta: Option<String>,
}

impl Income {
    fn select_duckdb_arrow(
        conn: &duckdb::Connection,
        start: u32,
        end: u32,
    ) -> Result<Vec<Self>, Box<dyn std::error::Error>> {
        let sql = format!(
            "SELECT created_at, amount, category_id, wallet_id, meta \
          FROM 'income' \
          WHERE created_at >= {} AND created_at <= {}",
            start, end
        );
        let mut stmt = conn.prepare_cached(&sql)?;
        let result = stmt.query_arrow_deserialized::<Income>([])?;
        Ok(result)
    }

    fn select_duckdb(conn: &duckdb::Connection, start: u32, end: u32) -> Result<Vec<Self>, Box<dyn std::error::Error>> {
        let mut arr = Vec::new();
        let sql = format!(
            "SELECT created_at, amount, category_id, wallet_id, meta \
          FROM 'income' \
          WHERE created_at >= {} AND created_at <= {}",
            start, end
        );
        let mut stmt = conn.prepare_cached(&sql)?;
        let result_iter = stmt.query_map([], |row| {
            Ok(Self {
                created_at: row.get(0)?,
                amount: row.get(1)?,
                category_id: row.get(2)?,
                wallet_id: row.get(3)?,
                meta: row.get(4)?,
            })
        })?;
        for result in result_iter {
            arr.push(result?);
        }
        Ok(arr)
    }

    fn select_sqlite(
        conn: &rusqlite::Connection,
        start: u32,
        end: u32,
    ) -> Result<Vec<Self>, Box<dyn std::error::Error>> {
        let mut arr = Vec::new();
        let sql = format!(
            "SELECT created_at, amount, category_id, wallet_id, meta \
          FROM 'income' \
          WHERE created_at >= {} AND created_at <= {}",
            start, end
        );
        let mut stmt = conn.prepare(&sql)?;
        let result_iter = stmt.query_map([], |row| {
            Ok(Self {
                created_at: row.get(0)?,
                amount: row.get(1)?,
                category_id: row.get(2)?,
                wallet_id: row.get(3)?,
                meta: row.get(4)?,
            })
        })?;
        for result in result_iter {
            arr.push(result?);
        }
        Ok(arr)
    }
}

fn bench_sqlite(c: &mut Criterion) {
    let sqlite_conn = sqlite_db();
    c.bench_function("sqlite_test", |b| {
        b.iter(|| {
            let out = Income::select_sqlite(&sqlite_conn, 1709292049, 1711375239).unwrap();
            out.len()
        })
    });
}

fn bench_duckdb(c: &mut Criterion) {
    let duckdb_conn = duck_db();
    c.bench_function("duckdb_test", |b| {
        b.iter(|| {
            let out = Income::select_duckdb(&duckdb_conn, 1709292049, 1711375239).unwrap();
            out.len()
        })
    });
}

fn bench_duckdb_arrow(c: &mut Criterion) {
    let duckdb_conn = duck_db();
    c.bench_function("duckdb_test_arrow", |b| {
        b.iter(|| {
            let out = Income::select_duckdb_arrow(&duckdb_conn, 1709292049, 1711375239).unwrap();
            out.len()
        })
    });
}

// criterion_group!(benches, bench_duckdb_arrow);
criterion_group!(benches, bench_sqlite, bench_duckdb, bench_duckdb_arrow);
criterion_main!(benches);

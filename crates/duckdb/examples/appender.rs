use duckdb::{Connection, DropBehavior, Result, params};

fn main() -> Result<()> {
    //let mut db = Connection::open("10m.db")?;
    let mut db = Connection::open_in_memory()?;

    let create_table_sql = "
        create table IF NOT EXISTS test
        (
            id INTEGER not null, -- primary key,
            area CHAR(6),
            age TINYINT not null,
            active TINYINT not null
        );";
    db.execute_batch(create_table_sql)?;

    let row_count = 10_000_000;
    {
        let mut tx = db.transaction()?;
        tx.set_drop_behavior(DropBehavior::Commit);
        let mut app = tx.appender("test")?;
        for i in 0..row_count {
            app.append_row(params![
                i,
                get_random_area_code(),
                get_random_age(),
                get_random_active(),
            ])?;
        }
    }

    let val = db.query_row("SELECT count(1) FROM test", [], |row| <(u32,)>::try_from(row))?;
    assert_eq!(val, (row_count,));
    Ok(())
}

// Modified from https://github.com/avinassh/fast-sqlite3-inserts/blob/master/src/bin/common.rs
use rand::{Rng, prelude::IndexedRandom};

#[inline]
fn get_random_age() -> i8 {
    let vs: Vec<i8> = vec![5, 10, 15];
    *vs.choose(&mut rand::rng()).unwrap()
}

#[inline]
fn get_random_active() -> i8 {
    if rand::random() {
        return 1;
    }
    0
}

#[inline]
fn get_random_bool() -> bool {
    rand::random()
}

#[inline]
fn get_random_area_code() -> Option<String> {
    if !get_random_bool() {
        return None;
    }
    let mut rng = rand::rng();
    Some(format!("{:06}", rng.random_range(0..999999)))
}

//! DuckDB aligns 16-byte values to 8 bytes only: uncompressed column segments are scanned in place,
//! after a block header, so these reads and writes must not assume 16-byte alignment.

use duckdb_neo::{
    Parameters, Result,
    connection::Connection,
    environment::{Environment, StorageLocation},
    scalar::MicroScalar,
    types::{Decimal, UuidValueRaw},
    vector::{ReadableNativeElement, Vector, WritableNativeElement},
};

fn execute(conn: &Connection, sql: &str) -> Result<Vec<i64>> {
    let mut result = conn.query(sql, Parameters::None)?;
    let mut rows = Vec::new();
    while let Some(chunk) = result.next_chunk()? {
        if chunk.col_count() > 0 {
            rows.extend(
                chunk
                    .get_vector_at::<i64>(0)?
                    .reader()?
                    .iter()
                    .map(|value| *value.unwrap()),
            );
        }
    }
    Ok(rows)
}

fn register_copy<T>(conn: &Connection, name: &str) -> Result<()>
where
    T: duckdb_neo::DuckDBType + ReadableNativeElement + WritableNativeElement<Write = T> + 'static,
    for<'a> T::Ref<'a>: std::ops::Deref<Target = T>,
{
    MicroScalar::create(name, |_ctx, input: Vector<'_, T>, mut output: Vector<'_, T>| {
        let reader = input.reader()?;
        let mut writer = output.writer()?;
        for (row, value) in reader.iter().enumerate() {
            writer.set_option(row, value.as_deref())?;
        }
        Ok(())
    })
    .register(conn)
}

/// Copies wide decimals through their physical slots, which DuckDB aligns to 8 bytes only.
fn register_slot_copy(conn: &Connection) -> Result<()> {
    MicroScalar::create(
        "copy_decimal_slots",
        |_ctx, input: Vector<'_, Decimal<38, 2>>, mut output: Vector<'_, Decimal<38, 2>>| {
            let reader = input.reader()?;
            // SAFETY: the scanned column has no NULLs, so every slot is initialized.
            let slots: &[Decimal<38, 2>] = unsafe { reader.as_slice() }.expect("DECIMAL exposes its slots");
            let selection = reader.selection();
            let mut writer = output.writer()?;
            for row in 0..reader.len() {
                writer.set(row, &slots[selection.index(row)])?;
            }
            Ok(())
        },
    )
    .register(conn)
}

#[test]
fn scans_of_uncompressed_wide_values_round_trip() -> Result<()> {
    let dir = std::env::temp_dir().join(format!("neo-unaligned-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("db.duckdb").to_str().unwrap().to_owned();
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::OnDisk(path))?.connect()?;
    execute(&conn, "PRAGMA force_compression='uncompressed'")?;
    execute(
        &conn,
        "CREATE TABLE t AS SELECT uuid() AS u, (i * 1000000007)::DECIMAL(38, 2) AS d FROM range(100000) t(i)",
    )?;
    execute(&conn, "CHECKPOINT")?;
    register_copy::<UuidValueRaw>(&conn, "copy_uuid")?;
    register_copy::<Decimal<38, 2>>(&conn, "copy_decimal")?;
    register_slot_copy(&conn)?;
    let counts = execute(
        &conn,
        "SELECT count(*) FILTER (copy_uuid(u) = u AND copy_decimal(d) = d AND copy_decimal_slots(d) = d)::BIGINT
         FROM t",
    )?;
    assert_eq!(counts, [100_000]);
    std::fs::remove_dir_all(&dir).ok();
    Ok(())
}

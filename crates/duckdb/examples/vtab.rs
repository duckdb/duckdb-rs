// Custom table function example.
//
// Defines a table function `numbers(BIGINT)` by implementing the `VTab` trait,
// registers it on a connection, and queries it from SQL. It returns one row per
// integer in `0..count`, across columns of increasing complexity, showing how
// to populate each vector kind:
//
//   - BIGINT / DOUBLE -- write physical values per row
//   - VARCHAR         -- insert per row; mark rows NULL with `set_null`
//   - LIST<BIGINT>    -- flatten children into one child vector, point each
//                        row's entry at its slice
//   - STRUCT(..)      -- one child vector per field
//
// It also shows DuckDB's per-chunk execution model: `func` is called repeatedly,
// emitting at most a vector's worth of rows each time until the scan is done.
//
// Run with: cargo run --example vtab --features vtab

use duckdb::{
    Connection, Result,
    arrow::util::pretty::print_batches,
    core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId},
    vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab},
};
use std::{
    error::Error,
    sync::atomic::{AtomicUsize, Ordering},
};

struct Numbers;

// Bind data is computed once from the call's arguments and is read-only during
// execution.
struct NumbersBind {
    count: usize,
}

// Init data tracks scan progress. `func` is called once per output chunk, so we
// remember how many rows we've already emitted.
struct NumbersInit {
    cursor: AtomicUsize,
}

impl VTab for Numbers {
    type BindData = NumbersBind;
    type InitData = NumbersInit;

    /// Declares the result columns and reads the call's arguments. Runs once.
    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        bind.add_result_column("n", LogicalTypeId::Bigint.into());
        bind.add_result_column("sqrt_n", LogicalTypeId::Double.into());
        bind.add_result_column("label", LogicalTypeId::Varchar.into());

        // LIST<BIGINT>: each row holds the divisors of `n`.
        let bigint: LogicalTypeHandle = LogicalTypeId::Bigint.into();
        bind.add_result_column("divisors", LogicalTypeHandle::list(&bigint));

        // STRUCT(is_even BOOLEAN, name VARCHAR): the parity of `n`.
        let parity = LogicalTypeHandle::struct_type(&[
            ("is_even", LogicalTypeId::Boolean.into()),
            ("name", LogicalTypeId::Varchar.into()),
        ]);
        bind.add_result_column("parity", parity);

        let count = bind.get_parameter(0).to_int64().max(0) as usize;
        Ok(NumbersBind { count })
    }

    /// Sets up per-scan state. Runs once after `bind`.
    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
        Ok(NumbersInit {
            cursor: AtomicUsize::new(0),
        })
    }

    /// Produces one chunk of rows per call. Populate `output` for the next batch
    /// of rows and set its length; set the length to 0 to end the scan.
    fn func(func: &TableFunctionInfo<Self>, output: &mut DataChunkHandle) -> Result<(), Box<dyn Error>> {
        let count = func.get_bind_data().count;
        let cursor = &func.get_init_data().cursor;

        // A chunk holds at most a vector's worth of rows (STANDARD_VECTOR_SIZE),
        // so a large result is delivered over several `func` calls.
        let capacity = output.flat_vector(0).capacity();

        // Claim the next chunk atomically. `fetch_add` hands each caller a
        // disjoint `start`, so this stays correct even if DuckDB scans on
        // multiple threads. The final chunk over-advances the cursor past
        // `count`, which is fine: the next call sees `start >= count` and ends.
        let start = cursor.fetch_add(capacity, Ordering::Relaxed);
        if start >= count {
            output.set_len(0); // no more rows: signal end of scan
            return Ok(());
        }
        let rows = capacity.min(count - start);

        // BIGINT: write one initialized value per row.
        {
            let mut v = output.flat_vector(0);
            for i in 0..rows {
                unsafe { v.write(i, (start + i) as i64) };
            }
        }

        // DOUBLE: same pattern, different physical type.
        {
            let mut v = output.flat_vector(1);
            for i in 0..rows {
                unsafe { v.write(i, ((start + i) as f64).sqrt()) };
            }
        }

        // VARCHAR: insert per row. Multiples of 3 are emitted as NULL. The same
        // `set_null` works on list and struct vectors too.
        {
            let mut v = output.flat_vector(2);
            for i in 0..rows {
                let n = start + i;
                if n % 3 == 0 {
                    v.set_null(i);
                } else {
                    v.insert(i, format!("number {n}").as_str());
                }
            }
        }

        // LIST<BIGINT>: buffer every row's child values and entry range, commit
        // the contiguous child storage, then point the entries into it. An
        // empty list (e.g. for n = 0) is distinct from a NULL list, which you'd
        // produce with `list.set_null(i)`.
        {
            let mut list = output.list_vector(3);
            let mut children: Vec<i64> = Vec::new();
            let mut entries = Vec::with_capacity(rows);
            for i in 0..rows {
                let divisors = divisors((start + i) as i64);
                entries.push((children.len(), divisors.len()));
                children.extend_from_slice(&divisors);
            }
            // Commit child storage before entries can point into it.
            unsafe { list.set_child(&children) };
            for (row, (offset, len)) in entries.into_iter().enumerate() {
                list.set_entry(row, offset, len);
            }
        }

        // STRUCT(is_even BOOLEAN, name VARCHAR): one child vector per field,
        // each sized like the chunk.
        {
            let mut parity = output.struct_vector(4);
            {
                let mut is_even = parity.child(0, rows);
                for i in 0..rows {
                    unsafe { is_even.write(i, (start + i) % 2 == 0) };
                }
            }
            {
                let mut name = parity.child(1, rows);
                for i in 0..rows {
                    name.insert(i, if (start + i) % 2 == 0 { "even" } else { "odd" });
                }
            }
        }

        output.set_len(rows);
        Ok(())
    }

    /// One parameter: the number of rows to produce.
    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![LogicalTypeId::Bigint.into()])
    }
}

/// The positive divisors of `n` (empty for n <= 0).
fn divisors(n: i64) -> Vec<i64> {
    (1..=n).filter(|d| n % d == 0).collect()
}

fn main() -> Result<()> {
    let conn = Connection::open_in_memory()?;
    conn.register_table_function::<Numbers>("numbers")?;

    // Query the function like any other table.
    let batches: Vec<_> = conn.prepare("SELECT * FROM numbers(6)")?.query_arrow([])?.collect();
    print_batches(&batches).unwrap();

    // A result larger than a single chunk is produced over several `func` calls;
    // the row count proves every chunk was emitted.
    let total: i64 = conn.query_row("SELECT count(*) FROM numbers(5000)", [], |row| row.get(0))?;
    assert_eq!(total, 5000);

    Ok(())
}

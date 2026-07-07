use super::{
    ArrowInitData, ArrowVTab, RowSlice, arrow_arraydata_to_query_params, arrow_ffi_to_query_params,
    arrow_recordbatch_to_query_params,
};
use crate::arrow_interop::test_support::{uuid_array, uuid_field};
use crate::{Connection, Result};
use arrow::{
    array::{
        Array, ArrayRef, BooleanArray, Decimal128Array, Int32Array, ListArray, MapArray, StringArray, StructArray,
    },
    buffer::{OffsetBuffer, ScalarBuffer},
    datatypes::{DataType, Field, Fields, Schema},
    ffi::{FFI_ArrowArray, FFI_ArrowSchema},
    record_batch::RecordBatch,
};
use std::{
    error::Error,
    sync::{Arc, Barrier, atomic::AtomicUsize},
};
use uuid::Uuid;

fn example_record_batch() -> RecordBatch {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("is_odd", DataType::Boolean, true),
    ]);
    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4])) as ArrayRef,
            Arc::new(StringArray::from(vec!["apple", "banana", "cherry", "date"])) as ArrayRef,
            Arc::new(BooleanArray::from(vec![true, false, true, false])) as ArrayRef,
        ],
    )
    .expect("failed to create record batch")
}

#[test]
fn test_arrow_uuid_extension_roundtrip() -> Result<(), Box<dyn Error>> {
    let db = Connection::open_in_memory()?;
    db.register_table_function::<ArrowVTab>("arrow")?;

    let uuids = vec![
        Uuid::from_u128(0xa1a2a3a4b1b2c1c2d1d2d3d4d5d6d7d8),
        Uuid::from_u128(0xb1a2a3a4b1b2c1c2d1d2d3d4d5d6d7d8),
        Uuid::from_u128(0),
        Uuid::from_u128(0x42),
    ];
    let schema = Schema::new(vec![uuid_field("id")]);
    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![Arc::new(uuid_array(&uuids, Some(vec![true, true, false, true]))) as ArrayRef],
    )?;
    let param = arrow_recordbatch_to_query_params(batch);

    let mut stmt = db.prepare("SELECT id::VARCHAR, typeof(id) FROM arrow(?, ?)")?;
    let rows = stmt.query_map(param, |row| {
        Ok((row.get::<_, Option<String>>(0)?, row.get::<_, String>(1)?))
    })?;
    let observed: std::result::Result<Vec<_>, _> = rows.collect();

    assert_eq!(
        observed?,
        vec![
            (Some(uuids[0].to_string()), "UUID".to_string()),
            (Some(uuids[1].to_string()), "UUID".to_string()),
            (None, "UUID".to_string()),
            (Some(uuids[3].to_string()), "UUID".to_string()),
        ]
    );

    Ok(())
}

#[test]
fn test_arrow_uuid_extension_roundtrip_in_struct() -> Result<(), Box<dyn Error>> {
    let db = Connection::open_in_memory()?;
    db.register_table_function::<ArrowVTab>("arrow")?;

    let uuids = vec![
        Uuid::from_u128(0xa1a2a3a4b1b2c1c2d1d2d3d4d5d6d7d8),
        Uuid::from_u128(0x42),
    ];
    let struct_array = StructArray::from(vec![
        (
            Arc::new(uuid_field("id")),
            Arc::new(uuid_array(&uuids, None)) as ArrayRef,
        ),
        (
            Arc::new(Field::new("label", DataType::Utf8, true)),
            Arc::new(StringArray::from(vec!["alpha", "beta"])) as ArrayRef,
        ),
    ]);
    let schema = Schema::new(vec![Field::new(
        "payload",
        DataType::Struct(Fields::from(vec![
            uuid_field("id"),
            Field::new("label", DataType::Utf8, true),
        ])),
        true,
    )]);
    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(struct_array) as ArrayRef])?;
    let param = arrow_recordbatch_to_query_params(batch);

    let mut stmt = db.prepare("SELECT payload.id::VARCHAR, typeof(payload.id) FROM arrow(?, ?)")?;
    let rows = stmt.query_map(param, |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)))?;
    let observed: std::result::Result<Vec<_>, _> = rows.collect();

    assert_eq!(
        observed?,
        vec![
            (uuids[0].to_string(), "UUID".to_string()),
            (uuids[1].to_string(), "UUID".to_string()),
        ]
    );

    Ok(())
}

#[test]
fn test_arrow_uuid_extension_roundtrip_in_list() -> Result<(), Box<dyn Error>> {
    let db = Connection::open_in_memory()?;
    db.register_table_function::<ArrowVTab>("arrow")?;

    let uuids = vec![
        Uuid::from_u128(0xa1a2a3a4b1b2c1c2d1d2d3d4d5d6d7d8),
        Uuid::from_u128(0xb1a2a3a4b1b2c1c2d1d2d3d4d5d6d7d8),
        Uuid::from_u128(0x42),
    ];
    let list_array = ListArray::new(
        Arc::new(uuid_field("item")),
        OffsetBuffer::new(ScalarBuffer::from(vec![0, 2, 3])),
        Arc::new(uuid_array(&uuids, None)),
        None,
    );
    let schema = Schema::new(vec![Field::new("ids", list_array.data_type().clone(), true)]);
    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(list_array) as ArrayRef])?;
    let param = arrow_recordbatch_to_query_params(batch);

    let mut stmt = db.prepare("SELECT ids[1]::VARCHAR, typeof(ids[1]) FROM arrow(?, ?)")?;
    let rows = stmt.query_map(param, |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)))?;
    let observed: std::result::Result<Vec<_>, _> = rows.collect();

    assert_eq!(
        observed?,
        vec![
            (uuids[0].to_string(), "UUID".to_string()),
            (uuids[2].to_string(), "UUID".to_string()),
        ]
    );

    Ok(())
}

#[test]
fn test_arrow_uuid_extension_roundtrip_in_map() -> Result<(), Box<dyn Error>> {
    let db = Connection::open_in_memory()?;
    db.register_table_function::<ArrowVTab>("arrow")?;

    let uuids = vec![
        Uuid::from_u128(0xa1a2a3a4b1b2c1c2d1d2d3d4d5d6d7d8),
        Uuid::from_u128(0xb1a2a3a4b1b2c1c2d1d2d3d4d5d6d7d8),
    ];
    let entries = StructArray::from(vec![
        (
            Arc::new(Field::new("keys", DataType::Utf8, false)),
            Arc::new(StringArray::from(vec!["alpha", "beta"])) as ArrayRef,
        ),
        (
            Arc::new(uuid_field("values")),
            Arc::new(uuid_array(&uuids, None)) as ArrayRef,
        ),
    ]);
    let entries_field = Arc::new(Field::new("entries", entries.data_type().clone(), false));
    let map_array = MapArray::new(
        entries_field,
        OffsetBuffer::new(ScalarBuffer::from(vec![0, 1, 2])),
        entries,
        None,
        false,
    );
    let schema = Schema::new(vec![Field::new("lookup", map_array.data_type().clone(), true)]);
    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(map_array) as ArrayRef])?;
    let param = arrow_recordbatch_to_query_params(batch);

    let mut stmt = db.prepare("SELECT lookup['alpha']::VARCHAR, lookup['beta']::VARCHAR FROM arrow(?, ?)")?;
    let rows = stmt.query_map(param, |row| {
        Ok((row.get::<_, Option<String>>(0)?, row.get::<_, Option<String>>(1)?))
    })?;
    let observed: std::result::Result<Vec<_>, _> = rows.collect();

    assert_eq!(
        observed?,
        vec![(Some(uuids[0].to_string()), None), (None, Some(uuids[1].to_string())),]
    );

    Ok(())
}

#[test]
fn test_query_record_batch_with_arrow_vtab() -> Result<(), Box<dyn Error>> {
    let db = Connection::open_in_memory()?;
    db.register_table_function::<ArrowVTab>("arrow")?;

    let params = arrow_recordbatch_to_query_params(example_record_batch());
    let batches: Vec<RecordBatch> = db
        .prepare(
            "
                SELECT id, upper(name) AS name, is_odd
                FROM arrow(?, ?)
                WHERE is_odd
                ORDER BY id
                ",
        )?
        .query_arrow(params)?
        .collect();

    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 2);

    let ids = batch.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
    let names = batch.column(1).as_any().downcast_ref::<StringArray>().unwrap();
    let predicates = batch.column(2).as_any().downcast_ref::<BooleanArray>().unwrap();

    assert_eq!(ids.value(0), 1);
    assert_eq!(ids.value(1), 3);
    assert_eq!(names.value(0), "APPLE");
    assert_eq!(names.value(1), "CHERRY");
    assert!(predicates.value(0));
    assert!(predicates.value(1));

    Ok(())
}

// Mark rows straddling vector-size slice boundaries as NULL so bitmap offsets
// are exercised when the source batch is sliced.
fn boundary_null(i: usize, vector_size: usize) -> bool {
    i == vector_size.saturating_sub(1) || i == vector_size || i == vector_size.saturating_add(1)
}

fn large_record_batch(n: usize, vector_size: usize) -> RecordBatch {
    let ids: Vec<i32> = (0..n as i32).collect();
    let vals: Vec<Option<String>> = (0..n)
        .map(|i| {
            if boundary_null(i, vector_size) {
                None
            } else {
                Some(format!("val-{i:05}"))
            }
        })
        .collect();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("val", DataType::Utf8, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(ids)) as ArrayRef,
            Arc::new(StringArray::from(vals)) as ArrayRef,
        ],
    )
    .expect("failed to create record batch")
}

#[test]
fn test_arrow_init_data_take_slice_partitions_rows_concurrently() {
    let num_rows = 10_000usize;
    let thread_count = 16;
    let init = Arc::new(ArrowInitData {
        offset: AtomicUsize::new(0),
        vector_size: 1,
    });
    let start = Arc::new(Barrier::new(thread_count));

    let mut handles = Vec::new();
    for _ in 0..thread_count {
        let init = Arc::clone(&init);
        let start = Arc::clone(&start);
        handles.push(std::thread::spawn(move || {
            start.wait();

            let mut slices = Vec::new();
            while let Some(slice) = init.take_slice(num_rows) {
                assert!(slice.len > 0);
                assert!(slice.offset < num_rows);
                assert!(slice.offset + slice.len <= num_rows);
                slices.push(slice);
            }
            slices
        }));
    }

    let mut slices = handles
        .into_iter()
        .flat_map(|handle| handle.join().expect("slice worker panicked"))
        .collect::<Vec<_>>();
    slices.sort_unstable_by_key(|slice| slice.offset);

    assert_eq!(slices.len(), num_rows);
    let mut next_offset = 0;
    for slice in slices {
        assert_eq!(slice.offset, next_offset);
        next_offset += slice.len;
    }
    assert_eq!(next_offset, num_rows);
    assert!(init.take_slice(num_rows).is_none());
}

#[test]
fn test_arrow_init_data_take_slice_handles_partial_tail() {
    let init = ArrowInitData {
        offset: AtomicUsize::new(0),
        vector_size: 4,
    };

    assert_eq!(init.take_slice(9), Some(RowSlice { offset: 0, len: 4 }));
    assert_eq!(init.take_slice(9), Some(RowSlice { offset: 4, len: 4 }));
    assert_eq!(init.take_slice(9), Some(RowSlice { offset: 8, len: 1 }));
    assert_eq!(init.take_slice(9), None);
    assert_eq!(init.take_slice(9), None);

    let empty = ArrowInitData {
        offset: AtomicUsize::new(0),
        vector_size: 4,
    };
    assert_eq!(empty.take_slice(0), None);
    assert_eq!(empty.take_slice(0), None);
}

#[test]
fn test_vtab_arrow() -> Result<(), Box<dyn Error>> {
    let db = Connection::open_in_memory()?;
    db.register_table_function::<ArrowVTab>("arrow")?;

    let rbs: Vec<RecordBatch> = db
        .prepare("SELECT * FROM read_parquet('./examples/int32_decimal.parquet');")?
        .query_arrow([])?
        .collect();
    let param = arrow_recordbatch_to_query_params(rbs.into_iter().next().unwrap());
    let mut stmt = db.prepare("select sum(value) from arrow(?, ?)")?;
    let mut arr = stmt.query_arrow(param)?;
    let rb = arr.next().expect("no record batch");
    assert_eq!(rb.num_columns(), 1);
    let column = rb.column(0).as_any().downcast_ref::<Decimal128Array>().unwrap();
    assert_eq!(column.len(), 1);
    assert_eq!(column.value(0), i128::from(30000));
    Ok(())
}

#[test]
fn test_vtab_arrow_large_record_batch() -> Result<(), Box<dyn Error>> {
    let db = Connection::open_in_memory()?;
    db.register_table_function::<ArrowVTab>("arrow")?;
    let vector_size = unsafe { crate::ffi::duckdb_vector_size() } as usize;
    assert!(vector_size > 0);

    let two_vectors = vector_size.checked_mul(2).expect("vector size overflow");
    let one_over = vector_size.checked_add(1).expect("vector size overflow");
    let partial_tail = vector_size / 2;
    let with_tail = two_vectors.checked_add(partial_tail).expect("vector size overflow");

    // Cover empty input, small input, vector-size boundary cases, and a tail
    // after multiple full vectors.
    for n in [
        0usize,
        1,
        vector_size.saturating_sub(1),
        vector_size,
        one_over,
        two_vectors,
        with_tail,
    ] {
        let param = arrow_recordbatch_to_query_params(large_record_batch(n, vector_size));
        let rbs: Vec<RecordBatch> = db
            .prepare("SELECT id, val FROM arrow(?, ?) ORDER BY id")?
            .query_arrow(param)?
            .collect();

        let total: usize = rbs.iter().map(|rb| rb.num_rows()).sum();
        assert_eq!(total, n, "row count mismatch for n={n}");

        // Verify every row survives the slicing in order, across chunk boundaries.
        let ids: Vec<i32> = rbs
            .iter()
            .flat_map(|rb| {
                rb.column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect();
        assert_eq!(
            ids,
            (0..n as i32).collect::<Vec<_>>(),
            "row order/content mismatch for n={n}"
        );

        let vals: Vec<Option<String>> = rbs
            .iter()
            .flat_map(|rb| {
                let val = rb.column(1).as_any().downcast_ref::<StringArray>().unwrap();
                (0..val.len())
                    .map(|i| {
                        if val.is_null(i) {
                            None
                        } else {
                            Some(val.value(i).to_string())
                        }
                    })
                    .collect::<Vec<_>>()
            })
            .collect();
        let expected_vals: Vec<Option<String>> = (0..n)
            .map(|i| {
                if boundary_null(i, vector_size) {
                    None
                } else {
                    Some(format!("val-{i:05}"))
                }
            })
            .collect();
        assert_eq!(vals, expected_vals, "null/value mismatch for n={n}");
    }
    Ok(())
}

#[test]
fn test_vtab_arrow_rust_array() -> Result<(), Box<dyn Error>> {
    let db = Connection::open_in_memory()?;
    db.register_table_function::<ArrowVTab>("arrow")?;

    // This is a show case that it's easy for you to build an in-memory data
    // and pass into DuckDB
    let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
    let array = Int32Array::from(vec![1, 2, 3, 4, 5]);
    let rb = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array)]).expect("failed to create record batch");
    let param = arrow_recordbatch_to_query_params(rb);
    let mut stmt = db.prepare("select sum(a)::int32 from arrow(?, ?)")?;
    let mut arr = stmt.query_arrow(param)?;
    let rb = arr.next().expect("no record batch");
    assert_eq!(rb.num_columns(), 1);
    let column = rb.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(column.len(), 1);
    assert_eq!(column.value(0), 15);
    Ok(())
}

#[test]
fn test_vtab_arrow_view_can_rebind_record_batch() -> Result<(), Box<dyn Error>> {
    let db = Connection::open_in_memory()?;
    db.register_table_function::<ArrowVTab>("arrow")?;

    let batch = example_record_batch();
    let param = arrow_recordbatch_to_query_params(batch.clone());
    db.execute(
        &format!(
            "CREATE VIEW arrow_view AS SELECT * FROM arrow({}::UBIGINT, {}::UBIGINT)",
            param[0], param[1]
        ),
        [],
    )?;

    for _ in 0..2 {
        let rbs: Vec<RecordBatch> = db.prepare("SELECT * FROM arrow_view")?.query_arrow([])?.collect();
        assert_eq!(vec![batch.clone()], rbs);
    }

    Ok(())
}

#[test]
fn test_vtab_arrow_arraydata_query_params() -> Result<(), Box<dyn Error>> {
    let batch = example_record_batch();
    let struct_array = StructArray::from(batch);
    let param = arrow_arraydata_to_query_params(struct_array.to_data());

    let db = Connection::open_in_memory()?;
    db.register_table_function::<ArrowVTab>("arrow")?;
    let mut stmt = db.prepare("select sum(id)::int32 from arrow(?, ?)")?;
    let rb = stmt.query_arrow(param)?.next().expect("no record batch");
    let column = rb.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(column.value(0), 10);
    Ok(())
}

#[test]
fn test_vtab_arrow_ffi_query_params() -> Result<(), Box<dyn Error>> {
    let batch = example_record_batch();
    let struct_array = StructArray::from(batch);
    let array = FFI_ArrowArray::new(&struct_array.to_data());
    let schema = FFI_ArrowSchema::try_from(struct_array.data_type())?;
    let param = arrow_ffi_to_query_params(array, schema);

    let db = Connection::open_in_memory()?;
    db.register_table_function::<ArrowVTab>("arrow")?;
    let mut stmt = db.prepare("select sum(id)::int32 from arrow(?, ?)")?;
    let rb = stmt.query_arrow(param)?.next().expect("no record batch");
    let column = rb.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(column.value(0), 10);
    Ok(())
}

#[test]
fn test_arrow_null_query_params_error() {
    let db = Connection::open_in_memory().unwrap();
    db.register_table_function::<ArrowVTab>("arrow").unwrap();

    let err = db.prepare("SELECT * FROM arrow(NULL, NULL)").err().unwrap();
    assert!(
        err.to_string()
            .contains("ArrowVTab record batch address parameter must not be NULL"),
        "unexpected error: {err}"
    );
}

#[test]
fn test_arrow_zero_address_query_params_error() {
    let db = Connection::open_in_memory().unwrap();
    db.register_table_function::<ArrowVTab>("arrow").unwrap();

    let valid = arrow_recordbatch_to_query_params(example_record_batch());
    let sql = format!("SELECT * FROM arrow(0::UBIGINT, {}::UBIGINT)", valid[1]);
    let err = db.prepare(&sql).err().unwrap();
    assert!(
        err.to_string().contains("invalid ArrowVTab record batch address"),
        "unexpected error: {err}"
    );
}

#[test]
fn test_arrow_marker_mismatch_query_params_error() {
    let db = Connection::open_in_memory().unwrap();
    db.register_table_function::<ArrowVTab>("arrow").unwrap();

    let err = db.prepare("SELECT * FROM arrow(1::UBIGINT, 2::UBIGINT)").err().unwrap();
    assert!(
        err.to_string().contains("query parameter marker mismatch"),
        "unexpected error: {err}"
    );
}

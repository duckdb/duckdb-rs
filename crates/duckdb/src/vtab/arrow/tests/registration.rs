//! Lifetime and binding behaviour of [`ArrowBatchRegistration`].

use super::*;

// Constructors.

#[test]
#[should_panic(expected = "ArrowVTab registration requires a struct array")]
fn test_from_array_data_rejects_non_struct_array() {
    let _ = ArrowBatchRegistration::from_array_data(Int32Array::from(vec![1, 2, 3]).to_data());
}

#[test]
fn test_vtab_arrow_arraydata_registration() -> Result<(), Box<dyn Error>> {
    let batch = example_record_batch();
    let struct_array = StructArray::from(batch);
    let reg = ArrowBatchRegistration::from_array_data(struct_array.to_data());

    let db = Connection::open_in_memory()?;
    db.register_table_function::<ArrowVTab>("arrow")?;
    let mut stmt = db.prepare("select sum(id)::int32 from arrow(?)")?;
    let rb = stmt.query_arrow([&reg])?.next().expect("no record batch");
    let column = rb.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(column.value(0), 10);
    Ok(())
}

#[test]
fn test_vtab_arrow_ffi_registration() -> Result<(), Box<dyn Error>> {
    let batch = example_record_batch();
    let struct_array = StructArray::from(batch);
    let array = FFI_ArrowArray::new(&struct_array.to_data());
    let schema = FFI_ArrowSchema::try_from(struct_array.data_type())?;
    drop(struct_array);
    // SAFETY: Both values were exported from the same valid struct array, and
    // the FFI array owns the backing buffers through its release callback.
    let reg = unsafe { ArrowBatchRegistration::from_ffi(array, schema) };

    let db = Connection::open_in_memory()?;
    db.register_table_function::<ArrowVTab>("arrow")?;
    let mut stmt = db.prepare("select sum(id)::int32 from arrow(?)")?;
    let rb = stmt.query_arrow([&reg])?.next().expect("no record batch");
    let column = rb.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(column.value(0), 10);
    Ok(())
}

// Token parameter validation.

#[test]
fn test_arrow_null_query_params_error() {
    let db = Connection::open_in_memory().unwrap();
    db.register_table_function::<ArrowVTab>("arrow").unwrap();

    let err = db.prepare("SELECT * FROM arrow(NULL)").err().unwrap();
    assert!(
        err.to_string()
            .contains("ArrowVTab record batch token parameter must not be NULL"),
        "unexpected error: {err}"
    );
}

#[test]
fn test_arrow_unknown_token_query_params_error() {
    let db = Connection::open_in_memory().unwrap();
    db.register_table_function::<ArrowVTab>("arrow").unwrap();

    let err = db.prepare("SELECT * FROM arrow(0::UBIGINT)").err().unwrap();
    assert!(
        err.to_string().contains("record batch token 0 is not registered"),
        "unexpected error: {err}"
    );
}

#[test]
fn test_small_literal_does_not_select_registered_batch() -> Result<(), Box<dyn Error>> {
    let db = Connection::open_in_memory()?;
    db.register_table_function::<ArrowVTab>("arrow")?;
    let reg = ArrowBatchRegistration::new(example_record_batch());

    let err = db
        .query_row("SELECT name FROM arrow(1::UBIGINT)", [], |row| row.get::<_, String>(0))
        .expect_err("a small literal selected a registered batch");
    assert!(err.to_string().contains("is not registered"), "unexpected error: {err}");

    let observed: String = db.query_row("SELECT name FROM arrow(?)", [&reg], |row| row.get(0))?;
    assert_eq!(observed, "apple");
    Ok(())
}

// Reuse across scans and executions.

#[test]
fn test_registration_can_be_reused_across_executions() -> Result<(), Box<dyn Error>> {
    let db = Connection::open_in_memory()?;
    db.register_table_function::<ArrowVTab>("arrow")?;

    let reg = ArrowBatchRegistration::new(example_record_batch());
    let mut stmt = db.prepare("SELECT * FROM arrow(?)")?;
    let batches: Vec<RecordBatch> = stmt.query_arrow([&reg])?.collect();
    assert_eq!(batches, vec![example_record_batch()]);

    let batches: Vec<RecordBatch> = stmt.query_arrow([&reg])?.collect();
    assert_eq!(batches, vec![example_record_batch()]);

    Ok(())
}

#[test]
fn test_registration_can_back_two_scans() -> Result<(), Box<dyn Error>> {
    let db = Connection::open_in_memory()?;
    db.register_table_function::<ArrowVTab>("arrow")?;
    let reg = ArrowBatchRegistration::new(example_record_batch());
    let mut stmt = db.prepare(
        "SELECT lhs.name || rhs.name
         FROM arrow($1) lhs
         CROSS JOIN arrow($1) rhs",
    )?;

    let observed = stmt.query_row([&reg], |row| row.get::<_, String>(0))?;
    assert_eq!(observed, "appleapple");
    Ok(())
}

#[test]
fn test_registration_parameter_composes_with_other_parameters() -> Result<(), Box<dyn Error>> {
    let db = Connection::open_in_memory()?;
    db.register_table_function::<ArrowVTab>("arrow")?;
    let left = ArrowBatchRegistration::new(RecordBatch::try_from_iter(vec![(
        "value",
        Arc::new(StringArray::from(vec!["left"])) as ArrayRef,
    )])?);
    let right = ArrowBatchRegistration::new(RecordBatch::try_from_iter(vec![(
        "value",
        Arc::new(StringArray::from(vec!["right"])) as ArrayRef,
    )])?);
    let mut stmt = db.prepare(
        "SELECT lhs.value || ':' || rhs.value
         FROM arrow($1) lhs
         CROSS JOIN arrow($2) rhs
         WHERE $3 = 42",
    )?;

    let observed: String = stmt.query_row((&left, &right, 42), |row| row.get(0))?;

    assert_eq!(observed, "left:right");
    Ok(())
}

#[test]
fn test_prepared_statement_accepts_a_new_registration() -> Result<(), Box<dyn Error>> {
    let db = Connection::open_in_memory()?;
    db.register_table_function::<ArrowVTab>("arrow")?;
    let mut stmt = db.prepare("SELECT name FROM arrow(?)")?;

    for expected in ["first", "second"] {
        let batch =
            RecordBatch::try_from_iter(vec![("name", Arc::new(StringArray::from(vec![expected])) as ArrayRef)])?;
        let reg = ArrowBatchRegistration::new(batch);
        let observed: String = stmt.query_row([&reg], |row| row.get(0))?;
        assert_eq!(observed, expected);
    }

    Ok(())
}

// Registry lifetime.

#[test]
fn test_unbound_registration_releases_batch() -> Result<(), Box<dyn Error>> {
    let array = Arc::new(StringArray::from(vec!["unbound allocation"]));
    let weak_array = Arc::downgrade(&array);
    let batch = RecordBatch::try_from_iter(vec![("value", array as ArrayRef)])?;
    let reg = ArrowBatchRegistration::new(batch);

    assert!(weak_array.upgrade().is_some());
    drop(reg);
    assert!(weak_array.upgrade().is_none(), "the unbound batch is still retained");
    Ok(())
}

#[test]
fn test_temporary_registration_releases_completed_insert_batches() -> Result<(), Box<dyn Error>> {
    let db = Connection::open_in_memory()?;
    db.register_table_function::<ArrowVTab>("arrow")?;
    db.execute("CREATE TABLE target(value VARCHAR)", [])?;

    for value in ["first allocation", "second allocation", "third allocation"] {
        let array = Arc::new(StringArray::from(vec![value]));
        let weak_array = Arc::downgrade(&array);
        let batch = RecordBatch::try_from_iter(vec![("value", array as ArrayRef)])?;

        db.execute(
            "INSERT INTO target SELECT * FROM arrow(?)",
            [&ArrowBatchRegistration::new(batch)],
        )?;

        assert!(
            weak_array.upgrade().is_none(),
            "the completed query still retains {value}"
        );
    }

    Ok(())
}

#[test]
fn test_temporary_registration_releases_batch_after_parse_error() -> Result<(), Box<dyn Error>> {
    let db = Connection::open_in_memory()?;
    db.register_table_function::<ArrowVTab>("arrow")?;
    let array = Arc::new(StringArray::from(vec!["failed query allocation"]));
    let weak_array = Arc::downgrade(&array);
    let batch = RecordBatch::try_from_iter(vec![("value", array as ArrayRef)])?;

    let err = db
        .execute("SELECT * FROM arrow(?) WHERE", [&ArrowBatchRegistration::new(batch)])
        .unwrap_err();

    assert!(err.to_string().contains("Parser Error"), "unexpected error: {err}");
    assert!(
        weak_array.upgrade().is_none(),
        "the failed query still retains its batch"
    );
    Ok(())
}

#[test]
fn test_active_stream_outlives_registration() -> Result<(), Box<dyn Error>> {
    let db = Connection::open_in_memory()?;
    db.register_table_function::<ArrowVTab>("arrow")?;
    let reg = ArrowBatchRegistration::new(example_record_batch());
    let mut stmt = db.prepare("SELECT * FROM arrow(?)")?;
    let stream = stmt.stream_arrow([&reg])?;

    drop(reg);

    assert_eq!(stream.collect::<Vec<_>>(), vec![example_record_batch()]);
    Ok(())
}

#[test]
fn test_bound_statement_outlives_registration() -> Result<(), Box<dyn Error>> {
    let db = Connection::open_in_memory()?;
    db.register_table_function::<ArrowVTab>("arrow")?;
    let reg = ArrowBatchRegistration::new(example_record_batch());
    let mut stmt = db.prepare(&format!("SELECT * FROM arrow({}::UBIGINT)", reg.token))?;

    drop(reg);

    let batches: Vec<RecordBatch> = stmt.query_arrow([])?.collect();
    assert_eq!(batches, vec![example_record_batch()]);
    Ok(())
}

#[test]
fn test_stored_view_follows_registration_lifetime() -> Result<(), Box<dyn Error>> {
    let db = Connection::open_in_memory()?;
    db.register_table_function::<ArrowVTab>("arrow")?;
    let reg = ArrowBatchRegistration::new(example_record_batch());
    db.execute(
        &format!("CREATE VIEW arrow_view AS SELECT * FROM arrow({}::UBIGINT)", reg.token),
        [],
    )?;

    let count: i64 = db.query_row("SELECT count(*) FROM arrow_view", [], |row| row.get(0))?;
    assert_eq!(count, 4);

    drop(reg);

    let err = db
        .prepare("SELECT * FROM arrow_view")
        .expect_err("stale view was rebound");
    assert!(err.to_string().contains("is not registered"), "unexpected error: {err}");
    Ok(())
}

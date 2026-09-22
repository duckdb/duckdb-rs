use crate::{
    DuckDBType, Parameters, Result, ToValue,
    column_data_collection::ColumnDataCollection,
    connection::Context,
    data_chunk::DataChunk,
    environment::{Environment, StorageLocation},
    logical_type::LogicalTypeID,
    qualified_name::QualifiedName,
    replacement_scan::{ReplacementHandle, ReplacementScanBuilder, ReplacementScanCallbacks, ReplacementType},
};

struct CustomReplacementScan {
    count: i32,
}

impl ReplacementScanCallbacks for CustomReplacementScan {
    fn scan<'a>(&'a self, context: Context, name: &QualifiedName, replacement: ReplacementHandle<'a>) -> Result<()> {
        let view = name.get_view()?;

        if let Some(table) = view.table
            && table.starts_with("num")
        {
            assert!(view.catalog == Some("test".to_string()));
            assert!(view.schema == Some("main".to_string()));

            let split = table
                .replace("num_", "")
                .replace('\'', "")
                .split('_')
                .map(|x| x.parse::<i32>().unwrap())
                .collect::<Vec<_>>();

            dbg!(&split);

            replacement.set_reference(ReplacementType::Table("range".try_into()?))?;
            replacement.add_parameter(split[0].value(&context)?)?;
            replacement.add_parameter((split[1] + self.count).value(&context)?)?;
        }

        Ok(())
    }
}

struct CustomNamedParameters {}

impl ReplacementScanCallbacks for CustomNamedParameters {
    fn scan<'a>(&'a self, context: Context, name: &QualifiedName, replacement: ReplacementHandle<'a>) -> Result<()> {
        let view = name.get_view()?;

        if view.table.is_some_and(|t| t.starts_with("alltypes")) {
            assert!(view.catalog.is_none());
            assert!(view.schema.is_none());

            replacement.set_reference(ReplacementType::Table("test_all_types".try_into()?))?;
            replacement.add_parameter_with_name("use_large_bignum", true.value(&context)?)?;
            replacement.add_parameter_with_name("use_large_enum", false.value(&context)?)?;
        }

        Ok(())
    }
}

struct CustomCdcScan {
    collection: ColumnDataCollection,
}

impl ReplacementScanCallbacks for CustomCdcScan {
    fn scan<'a>(&'a self, _context: Context, name: &QualifiedName, replacement: ReplacementHandle<'a>) -> Result<()> {
        let view = name.get_view()?;

        if view.table.is_some_and(|t| t.starts_with("cdc")) {
            replacement.set_reference(ReplacementType::NamedColumnDataCollection((
                &self.collection,
                vec!["id".to_string(), "is_active".to_string()],
            )))?;
        }

        Ok(())
    }
}

#[test]
fn test_replacement_scan() -> crate::Result<()> {
    let env = Environment::new()?;
    let db = env.open(StorageLocation::InMemory)?;
    let conn = db.connect()?;

    ReplacementScanBuilder::new(CustomReplacementScan { count: 42 }).register(&db)?;

    ReplacementScanBuilder::new(CustomNamedParameters {}).register(&conn)?;

    let mut query = conn.query("SELECT * FROM test.main.num_10_20", Parameters::None)?;

    let chunk = query.next().unwrap()?;

    assert!(chunk.get_vector_at::<i64>(0)?.logical_type().type_id() == LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT);

    assert_eq!(chunk.row_count()?, 10 + 42);

    assert!(query.next().is_none());

    let mut query = conn.query("SELECT * FROM alltypes", Parameters::None)?;

    let chunk = query.next().unwrap()?;

    assert!(
        chunk.get_vector_at::<bool>(0)?.logical_type().type_id() == LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_BOOLEAN
    );

    assert_eq!(chunk.vectors_count()?, 59);

    Ok(())
}

#[test]
fn test_replacement_scan_cdc() -> crate::Result<()> {
    let env = Environment::new()?;
    let db = env.open(StorageLocation::InMemory)?;
    let conn = db.connect()?;

    let logical_types = [i32::logical_type(&conn)?, bool::logical_type(&conn)?];

    let collection = ColumnDataCollection::new(&conn, &logical_types)?;

    let chunk = DataChunk::create(&logical_types, true)?;
    let mut id = chunk.get_vector_at::<i32>(0)?;
    let mut is_active = chunk.get_vector_at::<bool>(1)?;

    id.set_size(2)?;
    is_active.set_size(2)?;

    id.write(0, Some(10))?;
    id.write(1, Some(20))?;
    is_active.write(0, Some(true))?;
    is_active.write(1, Some(false))?;

    let mut appender = collection.to_append()?;
    appender.append(&chunk)?;
    let collection = appender.to_normal();

    ReplacementScanBuilder::new(CustomCdcScan { collection }).register(&conn)?;

    let mut query = conn.query("SELECT * FROM cdc_scan", Parameters::None)?;

    let chunk = query.next().unwrap()?;

    assert!(
        chunk.get_vector_at::<i32>(0)?.logical_type().type_id() == LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER
    );
    assert!(
        chunk.get_vector_at::<bool>(1)?.logical_type().type_id() == LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_BOOLEAN
    );

    assert_eq!(chunk.row_count()?, 2);

    let id = chunk.get_vector_at::<i32>(0)?;
    let is_active = chunk.get_vector_at::<bool>(1)?;

    assert_eq!(id.get(0)?, Some(&10));
    assert_eq!(id.get(1)?, Some(&20));
    assert_eq!(is_active.get(0)?, Some(&true));
    assert_eq!(is_active.get(1)?, Some(&false));

    assert!(query.next().is_none());

    Ok(())
}

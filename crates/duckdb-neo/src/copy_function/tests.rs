use std::{fs, sync::Mutex};

use crate::{
    DuckDBType, Parameters,
    column_data_collection::ColumnDataCollection,
    connection::Context,
    copy_function::{CopyFromCardinality, CopyFromFunctionCallbacks, CopyFunctionBuilder, CopyToFunctionCallbacks},
    data_chunk::{DataChunk, DataChunkRef},
    environment::{Environment, StorageLocation},
    file::{File, FileBuilder, FileSystem},
    logical_type::LogicalTypeID,
};

struct RapidCopy {
    multiplier: f64,
}

impl CopyToFunctionCallbacks for RapidCopy {
    type BindData = i32;
    type InitData = Mutex<File>;
    type BatchData = Vec<i32>;

    fn bind(&self, _context: &Context, column_info: &super::CopyToBindInfo) -> crate::Result<Self::BindData> {
        assert_eq!(column_info.len()?, 1);
        assert_eq!(column_info.get_column(0)?.0, "i");
        assert_eq!(
            column_info.get_column(0)?.1.type_id(),
            LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT
        );

        Ok(10)
    }

    fn batch_size(&self, _context: &Context, _bind_data: &Self::BindData) -> Option<usize> {
        Some(1)
    }

    fn init(&self, context: &Context, _bind_data: &Self::BindData, file_path: &str) -> crate::Result<Self::InitData> {
        let fs = FileSystem::new(context)?;

        let file = FileBuilder::new(&fs, file_path)?.write()?.create()?.open()?;

        Ok(Mutex::new(file))
    }

    fn batch(
        &self,
        context: &Context,
        _bind_data: &Self::BindData,
        _init_data: &Self::InitData,
        input: crate::column_data_collection::ColumnDataCollection,
    ) -> crate::Result<Self::BatchData> {
        let scanner = input.to_scan()?;
        let mut result = Vec::new();
        let mut to_append = ColumnDataCollection::new(context, [i64::logical_type(context)?])?.to_append()?;

        let data_chunk = DataChunk::create(&[i64::logical_type(context)?], true)?;
        let mut vec = data_chunk.get_vector_at::<i64>(0)?;
        vec.set_size(1)?;
        vec.write(0, Some(10))?;

        to_append.append(&data_chunk)?;

        let mut scanner = scanner.to_append()?;
        scanner.combine(to_append.to_normal())?;

        let mut scanner = scanner.to_scan()?;

        while let Some(chunk) = scanner.next_chunk()? {
            let in_vector = chunk.get_vector_at::<i64>(0)?;

            for item in in_vector.iter()?.flatten() {
                let mapped_value = ((*item) as f64 * self.multiplier) as i32;
                result.push(mapped_value);
            }
        }

        assert_eq!(result.len(), 10 + 1, "Expected 11 rows in batch data");

        Ok(result)
    }

    fn flush(
        &self,
        _context: &Context,
        _bind_data: &Self::BindData,
        init_data: &Self::InitData,
        batch_data: &Self::BatchData,
    ) -> crate::Result<()> {
        assert_eq!(batch_data.len(), 10 + 1, "Expected 11 rows in batch data");

        let file = init_data.lock().unwrap();

        for &value in batch_data {
            let buffer = (value.to_string() + ",").into_bytes();
            let written = file.write(&buffer)?;

            assert_eq!(written, buffer.len(), "Failed to write all bytes to file");
        }

        file.sync()?;

        Ok(())
    }

    fn finalize(
        &self,
        _context: &Context,
        _bind_data: &Self::BindData,
        init_data: &Self::InitData,
    ) -> crate::Result<()> {
        init_data.lock().unwrap().close()?;
        Ok(())
    }
}

#[test]
pub fn test_copy_function() -> crate::Result<()> {
    let env = Environment::new().expect("Failed to create environment");
    let db = env
        .open(StorageLocation::InMemory)
        .expect("Failed to open in-memory database");
    let conn = db.connect().expect("Failed to connect to database");

    CopyFunctionBuilder::new("rapidcopy", RapidCopy { multiplier: 1.5 })
        .register(&conn)
        .expect("Failed to register copy function");

    conn.execute(
        "COPY (SELECT i FROM range(0, $1) t(i)) TO 'out.txt' (FORMAT rapidcopy, USE_TMP_FILE FALSE)",
        Parameters::positional(&[&10]),
    )
    .expect("Failed to execute COPY query");

    let output = fs::read_to_string("out.txt").expect("Failed to read COPY output");
    assert_eq!(output, "0,1,3,4,6,7,9,10,12,13,15,");
    fs::remove_file("out.txt").expect("Failed to remove COPY output");

    Ok(())
}

struct RangeSource {
    total_rows: usize,
}

impl CopyFromFunctionCallbacks for RangeSource {
    type BindData = ();
    type LocalState = usize;
    type GlobalState = ();

    fn bind(
        &self,
        _context: &Context,
        _file_path: &str,
        bind_info: &super::CopyFromBindInfo,
    ) -> crate::Result<(Self::BindData, Option<CopyFromCardinality>)> {
        assert_eq!(bind_info.column_count()?, 1);
        assert_eq!(bind_info.get_column(0)?.0, "i");
        assert_eq!(
            bind_info.get_column(0)?.1.type_id(),
            LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT
        );

        Ok((
            (),
            Some(CopyFromCardinality {
                cardinality: self.total_rows,
                is_exact: true,
            }),
        ))
    }

    fn init_local_state(
        &self,
        _bind_data: Option<&Self::BindData>,
        _context: &Context,
        _global_state: Option<&Self::GlobalState>,
    ) -> crate::Result<Option<Self::LocalState>> {
        Ok(Some(0))
    }

    fn exec(
        &self,
        _bind_data: Option<&Self::BindData>,
        _global_state: Option<&Self::GlobalState>,
        local_state: Option<&mut Self::LocalState>,
        _context: &Context,
        output: DataChunkRef<'_>,
    ) -> crate::Result<()> {
        let produced = local_state.expect("local state must be set");
        let remaining = self.total_rows.saturating_sub(*produced);
        let batch = remaining.min(3);

        let mut vec = output.get_vector_at::<i64>(0)?;
        vec.set_size(batch)?;
        for row in 0..batch {
            vec.write(row, Some((*produced + row) as i64))?;
        }

        *produced += batch;

        Ok(())
    }
}

#[test]
pub fn test_copy_from_function() -> crate::Result<()> {
    let env = Environment::new().expect("Failed to create environment");
    let db = env
        .open(StorageLocation::InMemory)
        .expect("Failed to open in-memory database");
    let conn = db.connect().expect("Failed to connect to database");

    CopyFunctionBuilder::new("rangesource", RangeSource { total_rows: 7 })
        .register_from(&conn)
        .expect("Failed to register copy-from function");

    conn.execute("CREATE TABLE t(i BIGINT)", Parameters::None)
        .expect("Failed to create table");

    conn.execute("COPY t FROM 'unused.txt' (FORMAT rangesource)", Parameters::None)
        .expect("Failed to execute COPY FROM query");

    let mut statements = conn.parse("SELECT i FROM t ORDER BY i").expect("Failed to parse query");
    let statement = statements
        .next()
        .expect("expected a statement")
        .expect("valid statement");

    let mut values = Vec::new();
    for chunk in conn.query(statement, Parameters::None).expect("Failed to query") {
        let chunk = chunk.expect("Failed to fetch chunk");
        let column = chunk.get_vector_at::<i64>(0).expect("Failed to get column");
        for item in column.iter().expect("Failed to iterate").flatten() {
            values.push(*item);
        }
    }

    assert_eq!(values, vec![0, 1, 2, 3, 4, 5, 6]);

    Ok(())
}

/// Implements both `COPY ... TO` and `COPY ... FROM` for the same format name,
/// proving [`CopyFunctionBuilder::register_to_and_from`]
/// compiles and registers both sides on one handle.
struct EchoFormat;

impl CopyToFunctionCallbacks for EchoFormat {
    type InitData = Mutex<File>;
    type BindData = ();
    type BatchData = Vec<i64>;

    fn bind(&self, _context: &Context, _column_info: &super::CopyToBindInfo) -> crate::Result<Self::BindData> {
        Ok(())
    }

    fn batch_size(&self, _context: &Context, _bind_data: &Self::BindData) -> Option<usize> {
        Some(1)
    }

    fn init(&self, context: &Context, _bind_data: &Self::BindData, file_path: &str) -> crate::Result<Self::InitData> {
        let fs = FileSystem::new(context)?;
        Ok(Mutex::new(FileBuilder::new(&fs, file_path)?.write()?.create()?.open()?))
    }

    fn batch(
        &self,
        _context: &Context,
        _bind_data: &Self::BindData,
        _init_data: &Self::InitData,
        input: ColumnDataCollection,
    ) -> crate::Result<Self::BatchData> {
        let mut result = Vec::new();
        let mut scan = input.to_scan()?;
        while let Some(chunk) = scan.next_chunk()? {
            let in_vector = chunk.get_vector_at::<i64>(0)?;
            result.extend(in_vector.iter()?.flatten().copied());
        }
        Ok(result)
    }

    fn flush(
        &self,
        _context: &Context,
        _bind_data: &Self::BindData,
        init_data: &Self::InitData,
        batch_data: &Self::BatchData,
    ) -> crate::Result<()> {
        let file = init_data.lock().unwrap();
        for value in batch_data {
            let buffer = (value.to_string() + ",").into_bytes();
            file.write(&buffer)?;
        }
        Ok(())
    }

    fn finalize(
        &self,
        _context: &Context,
        _bind_data: &Self::BindData,
        init_data: &Self::InitData,
    ) -> crate::Result<()> {
        init_data.lock().unwrap().close()
    }
}

impl CopyFromFunctionCallbacks for EchoFormat {
    type BindData = ();
    type LocalState = usize;
    type GlobalState = ();

    fn bind(
        &self,
        _context: &Context,
        _file_path: &str,
        _bind_info: &super::CopyFromBindInfo,
    ) -> crate::Result<(Self::BindData, Option<CopyFromCardinality>)> {
        Ok(((), None))
    }

    fn init_local_state(
        &self,
        _bind_data: Option<&Self::BindData>,
        _context: &Context,
        _global_state: Option<&Self::GlobalState>,
    ) -> crate::Result<Option<Self::LocalState>> {
        Ok(Some(0))
    }

    fn exec(
        &self,
        _bind_data: Option<&Self::BindData>,
        _global_state: Option<&Self::GlobalState>,
        local_state: Option<&mut Self::LocalState>,
        _context: &Context,
        output: DataChunkRef<'_>,
    ) -> crate::Result<()> {
        let produced = local_state.expect("local state must be set");
        let batch = if *produced == 0 { 2 } else { 0 };

        let mut vec = output.get_vector_at::<i64>(0)?;
        vec.set_size(batch)?;
        for row in 0..batch {
            vec.write(row, Some((*produced + row) as i64))?;
        }

        *produced += batch;

        Ok(())
    }
}

#[test]
pub fn test_copy_to_and_from_function() -> crate::Result<()> {
    let env = Environment::new().expect("Failed to create environment");
    let db = env
        .open(StorageLocation::InMemory)
        .expect("Failed to open in-memory database");
    let conn = db.connect().expect("Failed to connect to database");

    CopyFunctionBuilder::new("echoformat", EchoFormat)
        .register_to_and_from(&conn)
        .expect("Failed to register combined copy function");

    conn.execute("CREATE TABLE t(i BIGINT)", Parameters::None)
        .expect("Failed to create table");

    conn.execute("COPY t FROM 'echo_in.txt' (FORMAT echoformat)", Parameters::None)
        .expect("Failed to execute COPY FROM query");

    conn.execute(
        "COPY (SELECT i FROM t) TO 'echo_out.txt' (FORMAT echoformat, USE_TMP_FILE FALSE)",
        Parameters::None,
    )
    .expect("Failed to execute COPY TO query");

    let output = fs::read_to_string("echo_out.txt").expect("Failed to read COPY output");
    assert_eq!(output, "0,1,");
    fs::remove_file("echo_out.txt").expect("Failed to remove COPY output");

    Ok(())
}

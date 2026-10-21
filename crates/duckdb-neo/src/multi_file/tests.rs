use std::{
    fs,
    ops::Range,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use crate::{
    DuckDBType, Parameters, Result, ToValue,
    bind_arguments::BindArgument,
    connection::{Connection, Context, SettingScope},
    data_chunk::DataChunkRef,
    environment::{Environment, StorageLocation},
    error::Error,
    file::{FileBuilder, FileSystem},
    multi_file::MultiFileFunctionBuilder,
    signature::{FunctionParameter, SignatureBuilder},
    table_function::{
        BindFunctionHandle, BindInfoApi, ExecColumnInfo, InitColumnData, TableClaimBatchCallbacks,
        TableFunctionBuilder, TableFunctionCallbacks, TableGetBindInfoCallbacks,
    },
};

const ROWS_PER_BATCH: usize = 100;

/// Reads a `.kv` file: a `name:field_id,...` header line, then one line of
/// comma-separated BIGINTs per row.
#[derive(Default)]
struct KvReader {
    bind_info_calls: Arc<AtomicUsize>,
    claimed_batches: Arc<AtomicUsize>,
    extra_identifier: Option<(usize, &'static [u64])>,
}

struct KvFile {
    columns: Vec<(String, i32)>,
    rows: Vec<Vec<i64>>,
}

impl KvFile {
    fn batch_count(&self) -> usize {
        self.rows.len().div_ceil(ROWS_PER_BATCH)
    }
}

impl TableFunctionCallbacks for KvReader {
    type BindData = KvFile;
    /// The next batch to hand out.
    type GlobalState = AtomicUsize;
    /// The rows of the batch this thread claimed, until exec produced them.
    type LocalState = Option<Range<usize>>;

    fn bind(
        &self,
        context: &Context,
        arguments: Vec<BindArgument>,
        bind_handle: BindFunctionHandle<'_>,
    ) -> Result<Self::BindData> {
        let path = arguments[0]
            .value
            .as_ref()
            .and_then(|value| value.get::<String>().transpose())
            .ok_or_else(|| Error::invalid_input("expected a file path"))??;

        let fs = FileSystem::new(context)?;
        let file = FileBuilder::new(&fs, &path)?.read().open()?;
        let content = String::from_utf8(file.read(file.size()? as usize)?).unwrap();
        file.close()?;

        let mut lines = content.lines();
        let columns: Vec<(String, i32)> = lines
            .next()
            .unwrap()
            .split(',')
            .map(|column| {
                let (name, field_id) = column.split_once(':').unwrap();
                (name.to_string(), field_id.parse().unwrap())
            })
            .collect();
        let rows = lines
            .map(|line| line.split(',').map(|value| value.parse().unwrap()).collect())
            .collect();

        for (name, _) in &columns {
            bind_handle.add_result_column(name, &i64::logical_type(context)?)?;
        }

        Ok(KvFile { columns, rows })
    }

    fn init_global_state(
        &self,
        _bind_data: &Self::BindData,
        _context: &Context,
        _column_data: InitColumnData<'_>,
    ) -> Result<(Option<Self::GlobalState>, Option<usize>)> {
        Ok((Some(AtomicUsize::new(0)), None))
    }

    fn init_local_state(
        &self,
        _bind_data: &Self::BindData,
        _global_state: Option<&Self::GlobalState>,
        _context: &Context,
        _column_data: InitColumnData<'_>,
    ) -> Result<Option<Self::LocalState>> {
        Ok(Some(None))
    }

    fn exec(
        &self,
        bind_data: &Self::BindData,
        _global_state: Option<&Self::GlobalState>,
        local_state: Option<&mut Self::LocalState>,
        _context: &Context,
        mut output: DataChunkRef<'_>,
        column_info: ExecColumnInfo<'_>,
    ) -> Result<()> {
        // A batch is produced in one go; the empty chunk after it ends the batch.
        let Some(batch) = local_state.unwrap().take() else {
            return Ok(());
        };

        for index in 0..column_info.count()? {
            let column = column_info.get_column_index(index)?;
            let mut vector = output.get_vector_at_mut::<i64>(index)?;
            vector.set_size(batch.len())?;
            for (row, values) in bind_data.rows[batch.clone()].iter().enumerate() {
                vector.write(row, Some(values[column]))?;
            }
        }

        Ok(())
    }
}

impl TableGetBindInfoCallbacks for KvReader {
    fn bind_info(&self, bind_data: &Self::BindData, context: &Context, api: BindInfoApi<'_>) -> Result<()> {
        self.bind_info_calls.fetch_add(1, Ordering::SeqCst);

        for (index, (_, field_id)) in bind_data.columns.iter().enumerate() {
            api.set_column_identifier(index, &[], &field_id.value(context)?)?;
        }
        if let Some((index, path)) = self.extra_identifier {
            api.set_column_identifier(index, path, &"extra".value(context)?)?;
        }
        api.set_option("row_count", &(bind_data.rows.len() as i64).value(context)?)
    }
}

impl TableClaimBatchCallbacks for KvReader {
    fn claim_batch(
        &self,
        bind_data: &Self::BindData,
        global_state: Option<&Self::GlobalState>,
        local_state: Option<&mut Self::LocalState>,
        _context: &Context,
    ) -> Result<bool> {
        let batch = global_state.unwrap().fetch_add(1, Ordering::SeqCst);
        if batch >= bind_data.batch_count() {
            return Ok(false);
        }

        self.claimed_batches.fetch_add(1, Ordering::SeqCst);
        let start = batch * ROWS_PER_BATCH;
        *local_state.unwrap() = Some(start..(start + ROWS_PER_BATCH).min(bind_data.rows.len()));
        Ok(true)
    }
}

/// A fresh directory under the system temp dir, removed on drop.
struct TempDir(PathBuf);

impl TempDir {
    fn new(name: &str) -> Self {
        let path = std::env::temp_dir().join(format!("duckdb_neo_{name}_{}", std::process::id()));
        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path).expect("Failed to create temp dir");
        TempDir(path)
    }

    fn join(&self, path: &str) -> String {
        self.0.join(path).to_str().unwrap().to_string()
    }

    /// Write a `.kv` file whose rows are `(row, offset + row)`, in `header`'s column order.
    fn write_kv(&self, name: &str, header: &str, rows: Range<i64>, offset: i64, swapped: bool) -> String {
        let mut content = format!("{header}\n");
        for row in rows {
            let (first, second) = if swapped {
                (offset + row, row)
            } else {
                (row, offset + row)
            };
            content.push_str(&format!("{first},{second}\n"));
        }
        let path = self.join(name);
        fs::write(&path, content).expect("Failed to write kv file");
        path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn query_pairs(conn: &Connection, sql: &str) -> Result<Vec<(i64, i64)>> {
    let mut result = conn.query(sql, Parameters::None)?;
    let mut rows = Vec::new();
    while let Some(chunk) = result.next_chunk()? {
        let first = chunk.get_vector_at::<i64>(0)?;
        let second = chunk.get_vector_at::<i64>(1)?;
        for row in 0..first.len() {
            rows.push((*first.get(row)?.unwrap(), *second.get(row)?.unwrap()));
        }
    }
    Ok(rows)
}

fn register_kv(conn: &Connection, reader: KvReader) -> Result<()> {
    TableFunctionBuilder::new(
        "read_kv_file",
        SignatureBuilder::new().param(FunctionParameter::new::<String>("file")),
        reader,
    )
    // The multi-file scan asks each file for its columns in the order it maps them.
    .with_projection_pushdown(true)
    .with_get_bind_info()
    .with_claim_batch()
    .register(conn)?;

    MultiFileFunctionBuilder::new("read_kv", "read_kv_file")
        .reader_type("KV")
        .file_extension("kv")
        .register(conn)
}

#[test]
fn test_multi_file_function() -> Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    // Several threads scan the batches of each file.
    conn.config()?.set_text("threads", "4", SettingScope::Default)?;

    let reader = KvReader::default();
    let bind_info_calls = reader.bind_info_calls.clone();
    let claimed_batches = reader.claimed_batches.clone();
    register_kv(&conn, reader)?;

    let dir = TempDir::new("multi_file_function");
    let first = dir.write_kv("first.kv", "a:1,b:2", 0..1_000, 10_000, false);
    // Renamed and reordered columns, still identified by the same field ids.
    dir.write_kv("second.kv", "bee:2,aye:1", 1_000..1_550, 10_000, true);

    let expected: Vec<(i64, i64)> = (0..1_550).map(|row| (row, 10_000 + row)).collect();

    // Batches are claimed for the multi-file scan, which keeps the rows of a file in order.
    let rows = query_pairs(&conn, &format!("SELECT a, b FROM read_kv('{first}')"))?;
    assert_eq!(rows, expected[..1_000]);
    assert_eq!(claimed_batches.swap(0, Ordering::SeqCst), 10);

    // The schema maps the files' columns by the field ids get_bind_info reported.
    let schema = "schema=map {
        1: {name: 'x', type: 'BIGINT', default_value: NULL},
        2: {name: 'y', type: 'BIGINT', default_value: NULL}
    }";
    let rows = query_pairs(
        &conn,
        &format!("SELECT x, y FROM read_kv('{}', {schema})", dir.join("*.kv")),
    )?;
    assert_eq!(rows, expected);
    assert_eq!(claimed_batches.swap(0, Ordering::SeqCst), 10 + 6);
    assert!(bind_info_calls.load(Ordering::SeqCst) >= 2);

    // Projected and row-only scans read the mapped columns too.
    let rows = query_pairs(
        &conn,
        &format!(
            "SELECT y, x FROM read_kv('{}', {schema}) WHERE x % 500 = 0",
            dir.join("*.kv")
        ),
    )?;
    assert_eq!(rows, [(10_000, 0), (10_500, 500), (11_000, 1_000), (11_500, 1_500)]);
    let rows = query_pairs(
        &conn,
        &format!(
            "SELECT count(*), sum(y)::BIGINT FROM read_kv('{}', {schema})",
            dir.join("*.kv")
        ),
    )?;
    assert_eq!(rows, [(1_550, expected.iter().map(|(_, y)| y).sum())]);
    claimed_batches.store(0, Ordering::SeqCst);

    // A directory reads the files with the registered extension.
    let rows = query_pairs(
        &conn,
        &format!("SELECT x, y FROM read_kv('{}', {schema})", dir.0.to_str().unwrap()),
    )?;
    assert_eq!(rows, expected);
    assert_eq!(claimed_batches.swap(0, Ordering::SeqCst), 10 + 6);

    // Without the multi-file wrapper the function claims its own batches.
    let rows = query_pairs(&conn, &format!("SELECT a, b FROM read_kv_file('{first}')"))?;
    assert_eq!(rows, expected[..1_000]);
    assert_eq!(claimed_batches.swap(0, Ordering::SeqCst), 10);

    Ok(())
}

#[test]
fn test_multi_file_function_by_name() -> Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    register_kv(&conn, KvReader::default())?;

    let dir = TempDir::new("multi_file_function_by_name");
    dir.write_kv("first.kv", "a:1,b:2", 0..3, 10, false);
    dir.write_kv("second.kv", "b:2,a:1", 3..5, 10, true);

    // Without a schema, the files are mapped by column name.
    let rows = query_pairs(
        &conn,
        &format!("SELECT a, b FROM read_kv('{}') ORDER BY a", dir.join("*.kv")),
    )?;
    assert_eq!(rows, [(0, 10), (1, 11), (2, 12), (3, 13), (4, 14)]);

    let error = conn
        .execute(
            format!("SELECT * FROM read_kv('{}')", dir.join("missing_*.kv")).as_str(),
            Parameters::None,
        )
        .unwrap_err();
    assert!(error.message.contains("No files found"), "{error}");

    Ok(())
}

#[test]
fn test_multi_file_function_errors() -> Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;

    // Wrapping requires a registered function with a supported file parameter.
    let error = MultiFileFunctionBuilder::new("read_missing", "no_such_function")
        .register(&conn)
        .unwrap_err();
    assert!(error.message.contains("no_such_function"), "{error}");
    TableFunctionBuilder::new(
        "read_kv_count",
        SignatureBuilder::new().param(FunctionParameter::new::<i32>("count")),
        KvReader::default(),
    )
    .register(&conn)?;
    let error = MultiFileFunctionBuilder::new("read_kv_counts", "read_kv_count")
        .register(&conn)
        .unwrap_err();
    assert!(error.message.contains("read_kv_count"), "{error}");

    let dir = TempDir::new("multi_file_function_errors");
    let file = dir.write_kv("first.kv", "a:1,b:2", 0..3, 10, false);
    // Column identifiers must refer to declared columns and valid nested fields.
    for (name, extra_identifier, message) in [
        ("read_kv_column", (2, &[][..]), "result column 2 has not been declared"),
        ("read_kv_path", (0, &[0][..]), "out of range for a field of type BIGINT"),
    ] {
        TableFunctionBuilder::new(
            &format!("{name}_file"),
            SignatureBuilder::new().param(FunctionParameter::new::<String>("file")),
            KvReader {
                extra_identifier: Some(extra_identifier),
                ..KvReader::default()
            },
        )
        .with_get_bind_info()
        .register(&conn)?;
        MultiFileFunctionBuilder::new(name, format!("{name}_file")).register(&conn)?;

        let error = conn
            .execute(format!("SELECT * FROM {name}('{file}')").as_str(), Parameters::None)
            .unwrap_err();
        assert!(error.message.contains(message), "{error}");
    }
    Ok(())
}

use std::{fs, num::NonZeroUsize, sync::Mutex};

use crate::{
    DuckDBType, Parameters,
    column_data_collection::ColumnDataCollection,
    connection::Context,
    copy_function::{
        CopyFromCardinality, CopyFromFunctionCallbacks, CopyFunctionBuilder, CopyToFunctionCallbacks, CopyToStatistics,
        CopyToStatisticsCallbacks,
    },
    data_chunk::{DataChunk, DataChunkRef},
    environment::{Environment, StorageLocation},
    file::{File, FileBuilder, FileSystem},
    logical_type::LogicalTypeID,
    types::FromValue,
};

struct RapidCopy {
    multiplier: f64,
    batch_size: crate::Result<Option<NonZeroUsize>>,
}

struct RapidCopyFile {
    file: File,
    rows_written: usize,
    bytes_written: usize,
}

impl CopyToFunctionCallbacks for RapidCopy {
    type BindData = i32;
    type InitData = Mutex<RapidCopyFile>;
    type BatchData = Vec<i32>;

    fn bind(&self, _context: &Context, column_info: &super::CopyToBindInfo) -> crate::Result<Self::BindData> {
        assert_eq!(column_info.len()?, 1);
        assert_eq!(column_info.get_column(0)?.0, "i");
        assert_eq!(
            column_info.get_column(0)?.1.id()?,
            LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT
        );

        Ok(10)
    }

    fn batch_size(&self, _context: &Context, _bind_data: &Self::BindData) -> crate::Result<Option<NonZeroUsize>> {
        self.batch_size.clone()
    }

    fn init(&self, context: &Context, _bind_data: &Self::BindData, file_path: &str) -> crate::Result<Self::InitData> {
        let fs = FileSystem::new(context)?;

        let file = FileBuilder::new(&fs, file_path)?.write().create().open()?;

        Ok(Mutex::new(RapidCopyFile {
            file,
            rows_written: 0,
            bytes_written: 0,
        }))
    }

    fn batch(
        &self,
        context: &Context,
        _bind_data: &Self::BindData,
        _init_data: &Self::InitData,
        input: &ColumnDataCollection<'_>,
    ) -> crate::Result<Self::BatchData> {
        let mut scanner = input.to_scan(context)?;
        let mut result = Vec::new();
        let mut to_append = ColumnDataCollection::new(context, [i64::logical_type(context)?])?.to_append()?;
        while let Some(mut chunk) = scanner.next_chunk()? {
            to_append.append(&mut chunk)?;
        }

        let mut data_chunk = DataChunk::create(&context, &[i64::logical_type(context)?], true)?;
        let mut vec = data_chunk.get_vector_at_mut::<i64>(0)?;
        vec.set_size(1)?;
        vec.writer()?.set(0, Some(10))?;

        to_append.append(&mut data_chunk)?;

        let mut scanner = to_append.to_scan(context)?;

        while let Some(chunk) = scanner.next_chunk()? {
            let in_vector = chunk.get_vector_at::<i64>(0)?;

            for item in in_vector.reader()?.iter().flatten() {
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
        _input: &ColumnDataCollection<'_>,
        batch_data: &mut Self::BatchData,
    ) -> crate::Result<()> {
        assert_eq!(batch_data.len(), 10 + 1, "Expected 11 rows in batch data");

        let mut state = init_data.lock().unwrap();

        for value in batch_data.drain(..) {
            let buffer = (value.to_string() + ",").into_bytes();
            let written = state.file.write(&buffer)?;

            assert_eq!(written, buffer.len(), "Failed to write all bytes to file");
            state.rows_written += 1;
            state.bytes_written += written;
        }

        state.file.sync()?;

        Ok(())
    }

    fn finalize(
        &self,
        _context: &Context,
        _bind_data: &Self::BindData,
        init_data: &Self::InitData,
    ) -> crate::Result<()> {
        init_data.lock().unwrap().file.close()?;
        Ok(())
    }
}

impl CopyToStatisticsCallbacks for RapidCopy {
    // Called after finalize has closed the file, so report what flush counted.
    fn statistics(
        &self,
        _context: &Context,
        _bind_data: &Self::BindData,
        init_data: &Self::InitData,
    ) -> crate::Result<CopyToStatistics> {
        let state = init_data.lock().unwrap();

        Ok(CopyToStatistics {
            row_count: Some(state.rows_written),
            file_size: Some(state.bytes_written),
        })
    }
}

#[test]
pub fn test_copy_function() -> crate::Result<()> {
    let env = Environment::new().expect("Failed to create environment");
    let instance = env
        .open(StorageLocation::InMemory)
        .expect("Failed to open in-memory database");
    let conn = instance.connect().expect("Failed to connect to database");

    CopyFunctionBuilder::new(
        "rapidcopy",
        RapidCopy {
            multiplier: 1.5,
            batch_size: Ok(NonZeroUsize::new(1)),
        },
    )
    .with_statistics()
    .register(&conn)
    .expect("Failed to register copy function");

    let mut result = conn
        .query(
            "COPY (SELECT i FROM range(0, $1) t(i)) TO 'out.txt' (FORMAT rapidcopy, USE_TMP_FILE FALSE, RETURN_STATS)",
            Parameters::positional(&[&10]),
        )
        .expect("Failed to execute COPY query");

    let mut stats = Vec::new();
    while let Some(chunk) = result.next_chunk()? {
        let filenames = chunk.get_vector_at::<String>(0)?.reader()?;
        let counts = chunk.get_vector_at::<u64>(1)?.reader()?;
        let file_sizes = chunk.get_vector_at::<u64>(2)?.reader()?;
        for row in 0..chunk.row_count()? {
            stats.push((
                filenames.get(row)?.unwrap().to_string(),
                *counts.get(row)?.unwrap(),
                *file_sizes.get(row)?.unwrap(),
            ));
        }
    }
    drop(result);

    let output = fs::read_to_string("out.txt").expect("Failed to read COPY output");
    assert_eq!(output, "0,1,3,4,6,7,9,10,12,13,15,");
    // The batch callback appends one row to the ten it receives.
    assert_eq!(stats, [("out.txt".to_string(), 11, output.len() as u64)]);
    fs::remove_file("out.txt").expect("Failed to remove COPY output");

    Ok(())
}

#[test]
fn test_copy_deferred_input() -> crate::Result<()> {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use crate::raw::RawExt;

    struct DeferredCopy {
        rows: Arc<Mutex<Vec<i64>>>,
        batches: Arc<AtomicUsize>,
        drops: Arc<AtomicUsize>,
    }

    struct DeferredBatch {
        handle: usize,
        rows: usize,
        drops: Arc<AtomicUsize>,
    }

    impl Drop for DeferredBatch {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    impl CopyToFunctionCallbacks for DeferredCopy {
        type BindData = ();
        type InitData = ();
        type BatchData = DeferredBatch;

        fn bind(&self, _context: &Context, _info: &super::CopyToBindInfo) -> crate::Result<()> {
            Ok(())
        }

        fn init(&self, _context: &Context, _bind_data: &(), _file_path: &str) -> crate::Result<()> {
            Ok(())
        }

        fn batch(
            &self,
            _context: &Context,
            _bind_data: &(),
            _init_data: &(),
            input: &ColumnDataCollection<'_>,
        ) -> crate::Result<Self::BatchData> {
            self.batches.fetch_add(1, Ordering::SeqCst);
            Ok(DeferredBatch {
                handle: input.raw() as usize,
                rows: input.len()?,
                drops: self.drops.clone(),
            })
        }

        fn flush(
            &self,
            context: &Context,
            _bind_data: &(),
            _init_data: &(),
            input: &ColumnDataCollection<'_>,
            batch_data: &mut Self::BatchData,
        ) -> crate::Result<()> {
            assert_eq!(input.raw() as usize, batch_data.handle);
            assert_eq!(input.len()?, batch_data.rows);
            let mut scan = input.to_scan(context)?;
            let mut rows = self.rows.lock().unwrap();
            while let Some(chunk) = scan.next_chunk()? {
                rows.extend(chunk.get_vector_at::<i64>(0)?.reader()?.iter().flatten().copied());
            }
            Ok(())
        }

        fn finalize(&self, _context: &Context, _bind_data: &(), _init_data: &()) -> crate::Result<()> {
            Ok(())
        }
    }

    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    conn.execute("SET threads = 4", Parameters::None)?;
    let rows = Arc::new(Mutex::new(Vec::new()));
    let batches = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    CopyFunctionBuilder::new(
        "deferred_copy",
        DeferredCopy {
            rows: rows.clone(),
            batches: batches.clone(),
            drops: drops.clone(),
        },
    )
    .register(&conn)?;
    let dir = TempDir::new("copy_deferred_input");
    conn.execute(
        format!(
            "COPY (SELECT i FROM range(4097) t(i) ORDER BY i) TO '{}' \
             (FORMAT deferred_copy, BATCH_SIZE 128, USE_TMP_FILE FALSE)",
            dir.join("out")
        )
        .as_str(),
        Parameters::None,
    )?;
    assert_eq!(*rows.lock().unwrap(), (0..4097).collect::<Vec<i64>>());
    assert!(batches.load(Ordering::SeqCst) > 1);
    assert_eq!(drops.load(Ordering::SeqCst), batches.load(Ordering::SeqCst));
    Ok(())
}

#[test]
fn test_copy_batch_size_error() -> crate::Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    CopyFunctionBuilder::new(
        "batch_size_error",
        RapidCopy {
            multiplier: 1.0,
            batch_size: Err(crate::Error::invalid_input("batch size unavailable")),
        },
    )
    .register(&conn)?;

    let dir = TempDir::new("copy_batch_size_error");
    let error = conn
        .execute(
            format!(
                "COPY (SELECT i FROM range(10) t(i)) TO '{}' (FORMAT batch_size_error)",
                dir.join("out.txt")
            )
            .as_str(),
            Parameters::None,
        )
        .unwrap_err();
    assert!(error.message.contains("batch size unavailable"), "{error}");
    Ok(())
}

#[test]
fn test_copy_return_stats_requires_statistics() -> crate::Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    CopyFunctionBuilder::new(
        "no_statistics",
        RapidCopy {
            multiplier: 1.0,
            batch_size: Ok(None),
        },
    )
    .register(&conn)?;

    let dir = TempDir::new("copy_return_stats_requires_statistics");
    let error = conn
        .execute(
            format!(
                "COPY (SELECT i FROM range(10) t(i)) TO '{}' (FORMAT no_statistics, RETURN_STATS)",
                dir.join("out.txt")
            )
            .as_str(),
            Parameters::None,
        )
        .unwrap_err();
    assert!(error.message.contains("RETURN_STATS is not supported"), "{error}");
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
            bind_info.get_column(0)?.1.id()?,
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

    fn init_global_state(
        &self,
        _bind_data: &Self::BindData,
        _context: &Context,
    ) -> crate::Result<(Option<Self::GlobalState>, Option<NonZeroUsize>)> {
        Ok((None, NonZeroUsize::new(1)))
    }

    fn init_local_state(
        &self,
        _bind_data: &Self::BindData,
        _context: &Context,
        _global_state: Option<&Self::GlobalState>,
    ) -> crate::Result<Option<Self::LocalState>> {
        Ok(Some(0))
    }

    fn exec(
        &self,
        _bind_data: &Self::BindData,
        _global_state: Option<&Self::GlobalState>,
        local_state: Option<&mut Self::LocalState>,
        _context: &Context,
        mut output: DataChunkRef<'_>,
    ) -> crate::Result<()> {
        let produced = local_state.expect("local state must be set");
        let remaining = self.total_rows.saturating_sub(*produced);
        let batch = remaining.min(3);

        let mut vec = output.get_vector_at_mut::<i64>(0)?;
        vec.set_size(batch)?;
        for row in 0..batch {
            vec.writer()?.set(row, Some((*produced + row) as i64))?;
        }

        *produced += batch;

        Ok(())
    }
}

#[test]
pub fn test_copy_from_function() -> crate::Result<()> {
    let env = Environment::new().expect("Failed to create environment");
    let instance = env
        .open(StorageLocation::InMemory)
        .expect("Failed to open in-memory database");
    let conn = instance.connect().expect("Failed to connect to database");

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
    let mut result = conn.query(statement, Parameters::None).expect("Failed to query");
    while let Some(chunk) = result.next_chunk().expect("Failed to fetch chunk") {
        let column = chunk.get_vector_at::<i64>(0).expect("Failed to get column");
        for item in column.reader()?.iter().flatten() {
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

    fn bind(&self, _context: &Context, column_info: &super::CopyToBindInfo) -> crate::Result<Self::BindData> {
        assert_eq!(column_info.column_count()?, column_info.len()?);
        assert_eq!(column_info.get_column(0)?.0, "i");
        assert!(!column_info.file_path()?.is_empty());
        assert_eq!(column_info.option_count()?, column_info.options()?.len());
        if column_info.option_count()? > 0 {
            let (name, value) = column_info.get_option(0)?;
            assert_eq!(name, "custom_option");
            assert_eq!(String::from_value(&value)?, Some("roundtrip".to_string()));
        }
        Ok(())
    }

    fn batch_size(&self, _context: &Context, _bind_data: &Self::BindData) -> crate::Result<Option<NonZeroUsize>> {
        Ok(NonZeroUsize::new(1))
    }

    fn init(&self, context: &Context, _bind_data: &Self::BindData, file_path: &str) -> crate::Result<Self::InitData> {
        let fs = FileSystem::new(context)?;
        Ok(Mutex::new(FileBuilder::new(&fs, file_path)?.write().create().open()?))
    }

    fn batch(
        &self,
        context: &Context,
        _bind_data: &Self::BindData,
        _init_data: &Self::InitData,
        input: &ColumnDataCollection<'_>,
    ) -> crate::Result<Self::BatchData> {
        let mut result = Vec::new();
        let mut scan = input.to_scan(context)?;
        while let Some(chunk) = scan.next_chunk()? {
            let in_vector = chunk.get_vector_at::<i64>(0)?;
            result.extend(in_vector.reader()?.iter().flatten().copied());
        }
        Ok(result)
    }

    fn flush(
        &self,
        _context: &Context,
        _bind_data: &Self::BindData,
        init_data: &Self::InitData,
        _input: &ColumnDataCollection<'_>,
        batch_data: &mut Self::BatchData,
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
        file_path: &str,
        bind_info: &super::CopyFromBindInfo,
    ) -> crate::Result<(Self::BindData, Option<CopyFromCardinality>)> {
        assert_eq!(bind_info.file_path()?, file_path);
        assert_eq!(bind_info.column_count()?, 1);
        assert_eq!(bind_info.get_column(0)?.0, "i");
        assert_eq!(bind_info.option_count()?, 1);
        let (name, value) = bind_info.get_option(0)?;
        assert_eq!(name, "custom_option");
        assert_eq!(String::from_value(&value)?, Some("roundtrip".to_string()));
        Ok(((), None))
    }

    fn init_local_state(
        &self,
        _bind_data: &Self::BindData,
        _context: &Context,
        _global_state: Option<&Self::GlobalState>,
    ) -> crate::Result<Option<Self::LocalState>> {
        Ok(Some(0))
    }

    fn exec(
        &self,
        _bind_data: &Self::BindData,
        _global_state: Option<&Self::GlobalState>,
        local_state: Option<&mut Self::LocalState>,
        _context: &Context,
        mut output: DataChunkRef<'_>,
    ) -> crate::Result<()> {
        let produced = local_state.expect("local state must be set");
        let batch = if *produced == 0 { 2 } else { 0 };

        let mut vec = output.get_vector_at_mut::<i64>(0)?;
        vec.set_size(batch)?;
        for row in 0..batch {
            vec.writer()?.set(row, Some((*produced + row) as i64))?;
        }

        *produced += batch;

        Ok(())
    }
}

#[test]
pub fn test_copy_to_and_from_function() -> crate::Result<()> {
    let env = Environment::new().expect("Failed to create environment");
    let instance = env
        .open(StorageLocation::InMemory)
        .expect("Failed to open in-memory database");
    let conn = instance.connect().expect("Failed to connect to database");

    CopyFunctionBuilder::new("echoformat", EchoFormat)
        .register_to_and_from(&conn)
        .expect("Failed to register combined copy function");

    conn.execute("CREATE TABLE t(i BIGINT)", Parameters::None)
        .expect("Failed to create table");

    conn.execute(
        "COPY t FROM 'echo_in.txt' (FORMAT echoformat, custom_option 'roundtrip')",
        Parameters::None,
    )
    .expect("Failed to execute COPY FROM query");

    conn.execute(
        "COPY (SELECT i FROM t) TO 'echo_out.txt' (FORMAT echoformat, USE_TMP_FILE FALSE, custom_option 'roundtrip')",
        Parameters::None,
    )
    .expect("Failed to execute COPY TO query");

    let output = fs::read_to_string("echo_out.txt").expect("Failed to read COPY output");
    assert_eq!(output, "0,1,");
    fs::remove_file("echo_out.txt").expect("Failed to remove COPY output");

    Ok(())
}

/// A fresh directory under the system temp dir, removed on drop.
struct TempDir(std::path::PathBuf);

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

    /// The files under `dir`, relative to it.
    fn files(&self, dir: &str) -> Vec<String> {
        fn walk(root: &std::path::Path, path: &std::path::Path, out: &mut Vec<String>) {
            for entry in fs::read_dir(path).expect("Failed to read dir").flatten() {
                let path = entry.path();
                if path.is_dir() {
                    walk(root, &path, out);
                } else {
                    let relative = path.strip_prefix(root).unwrap().components();
                    out.push(
                        relative
                            .map(|c| c.as_os_str().to_str().unwrap())
                            .collect::<Vec<_>>()
                            .join("/"),
                    );
                }
            }
        }
        let root = self.0.join(dir);
        let mut out = Vec::new();
        walk(&root, &root, &mut out);
        out.sort();
        out
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

/// DuckDB only uses the file extension when it names the files itself, as with
/// `PER_THREAD_OUTPUT` or `PARTITION_BY`.
#[test]
pub fn test_copy_file_extension() -> crate::Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;
    // One file per directory or partition.
    conn.execute("SET threads = 1", Parameters::None)?;

    CopyFunctionBuilder::new("echo_ext", EchoFormat)
        .with_fallback_extension("echo")
        .register(&conn)?;
    CopyFunctionBuilder::new("echo_no_ext", EchoFormat).register(&conn)?;

    let dir = TempDir::new("copy_file_extension");
    let copy = |options: &str, target: &str| {
        conn.execute(
            format!(
                "COPY (SELECT i FROM range(4) t(i)) TO '{}' ({options})",
                dir.join(target)
            )
            .as_str(),
            Parameters::None,
        )
    };

    copy("FORMAT echo_ext, PER_THREAD_OUTPUT", "per_thread")?;
    assert_eq!(dir.files("per_thread"), ["data_0.echo"]);
    assert_eq!(
        fs::read_to_string(dir.join("per_thread/data_0.echo")).unwrap(),
        "0,1,2,3,"
    );

    // The FILE_EXTENSION option overrides it.
    copy(
        "FORMAT echo_ext, PER_THREAD_OUTPUT, FILE_EXTENSION 'other'",
        "overridden",
    )?;
    assert_eq!(dir.files("overridden"), ["data_0.other"]);

    // Without one, DuckDB names the files with a bare dot, which Windows strips.
    copy("FORMAT echo_no_ext, PER_THREAD_OUTPUT", "no_extension")?;
    let expected = if cfg!(windows) { "data_0" } else { "data_0." };
    assert_eq!(dir.files("no_extension"), [expected]);

    conn.execute(
        format!(
            "COPY (SELECT i % 2 AS p, i FROM range(4) t(i)) TO '{}' (FORMAT echo_ext, PARTITION_BY (p))",
            dir.join("partitioned")
        )
        .as_str(),
        Parameters::None,
    )?;
    assert_eq!(dir.files("partitioned"), ["p=0/data_0.echo", "p=1/data_0.echo"]);
    assert_eq!(
        fs::read_to_string(dir.join("partitioned/p=0/data_0.echo")).unwrap(),
        "0,2,"
    );

    Ok(())
}

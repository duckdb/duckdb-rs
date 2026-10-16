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

    fn batch<'ctx>(
        &self,
        context: &'ctx Context,
        _bind_data: &Self::BindData,
        _init_data: &Self::InitData,
        input: ColumnDataCollection<'ctx>,
    ) -> crate::Result<Self::BatchData> {
        let scanner = input.to_scan()?;
        let mut result = Vec::new();
        let mut to_append = ColumnDataCollection::new(context, [i64::logical_type(context)?])?.to_append()?;

        let mut data_chunk = DataChunk::create(&context, &[i64::logical_type(context)?], true)?;
        let mut vec = data_chunk.get_vector_at_mut::<i64>(0)?;
        vec.set_size(1)?;
        vec.write(0, Some(10))?;

        to_append.append(&mut data_chunk)?;

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
        mut output: DataChunkRef<'_>,
    ) -> crate::Result<()> {
        let produced = local_state.expect("local state must be set");
        let remaining = self.total_rows.saturating_sub(*produced);
        let batch = remaining.min(3);

        let mut vec = output.get_vector_at_mut::<i64>(0)?;
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
    let mut result = conn.query(statement, Parameters::None).expect("Failed to query");
    while let Some(chunk) = result.next_chunk().expect("Failed to fetch chunk") {
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

    fn batch<'ctx>(
        &self,
        _context: &'ctx Context,
        _bind_data: &Self::BindData,
        _init_data: &Self::InitData,
        input: ColumnDataCollection<'ctx>,
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
        mut output: DataChunkRef<'_>,
    ) -> crate::Result<()> {
        let produced = local_state.expect("local state must be set");
        let batch = if *produced == 0 { 2 } else { 0 };

        let mut vec = output.get_vector_at_mut::<i64>(0)?;
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

//! File access through DuckDB's file system.

use std::marker::PhantomData;

use duckdb_loadable_macros::{AsRaw, DropHandle, FromRaw};

use crate::{
    Result,
    builder_helpers::handle_unwind,
    check_api_call,
    error::Error,
    ffi,
    ffi_str::DuckDBStr,
    links::{DatabaseKeepAlive, FileSystemLink, KeepAlive},
    raw::RawExt,
    value::Value,
};

pub(crate) unsafe extern "C" fn copy_glob_callback(
    text: *const ffi::duckdb_v2_str,
    user_data: *mut std::os::raw::c_void,
    err: *mut ffi::duckdb_v2_error_info_handle,
) {
    handle_unwind(
        || {
            if text.is_null() || user_data.is_null() {
                return Err(Error::api_error("NULL value in copy_render_box"));
            }

            let items = unsafe { &mut *(user_data as *mut Vec<String>) };

            items.push(
                DuckDBStr::from_raw(unsafe { *text })
                    .to_string_lossy()
                    .unwrap_or_default()
                    .into_owned(),
            );

            Ok(())
        },
        err,
    );
}

/// Append an expanded path, which comes from the OS and may not be valid UTF-8.
unsafe extern "C" fn copy_path_callback(
    text: *const ffi::duckdb_v2_str,
    user_data: *mut std::os::raw::c_void,
    err: *mut ffi::duckdb_v2_error_info_handle,
) {
    handle_unwind(
        || {
            if text.is_null() || user_data.is_null() {
                return Err(Error::api_error("NULL value in copy_path_callback"));
            }

            let string = unsafe { &mut *(user_data as *mut String) };
            string.push_str(
                &DuckDBStr::from_raw(unsafe { *text })
                    .to_string_lossy()
                    .unwrap_or_default(),
            );

            Ok(())
        },
        err,
    );
}
/// A borrowed handle to DuckDB's file system.
#[derive(Debug, AsRaw, FromRaw)]
pub struct FileSystem<'link> {
    handle: ffi::duckdb_v2_file_system_handle,
    database: Option<KeepAlive>,
    _marker: PhantomData<&'link ()>,
}

impl<'link> FileSystem<'link> {
    /// Borrow the file system associated with a connection or callback context.
    ///
    /// Files opened through a connection's file system keep that connection and its
    /// database alive. Files opened through a context's file system must not outlive
    /// the connection that ran the callback, for example by being stored in a `static`
    /// or sent to code outside DuckDB.
    #[allow(private_bounds)]
    pub fn new<C: FileSystemLink + DatabaseKeepAlive>(link: &'link C) -> Result<Self> {
        let handle = link.get_file_system()?;

        Ok(FileSystem {
            handle,
            database: link.keep_alive(),
            _marker: PhantomData,
        })
    }

    pub fn exists(&self, path: &str) -> Result<bool> {
        check_api_call!(ffi::duckdb_v2_file_system_path_exists, self.raw(), &path.into(), RET)
    }

    pub fn is_remote(&self, path: &str) -> Result<bool> {
        check_api_call!(ffi::duckdb_v2_file_system_path_is_remote, self.raw(), &path.into(), RET)
    }

    pub fn move_item(&self, from: &str, to: &str) -> Result<()> {
        check_api_call!(
            ffi::duckdb_v2_file_system_path_move,
            self.raw(),
            &from.into(),
            &to.into(),
        )
    }

    pub fn remove(&self, path: &str) -> Result<bool> {
        check_api_call!(ffi::duckdb_v2_file_system_path_remove, self.raw(), &path.into(), RET)
    }

    pub fn expand(&self, path: &str) -> Result<String> {
        let mut string = String::new();

        check_api_call!(
            ffi::duckdb_v2_file_system_path_expand,
            self.raw(),
            &path.into(),
            Some(copy_path_callback),
            &mut string as *mut String as *mut std::os::raw::c_void
        )?;

        Ok(string)
    }

    pub fn glob(&self, path: &str) -> Result<Vec<String>> {
        let mut items = Vec::new();

        check_api_call!(
            ffi::duckdb_v2_file_system_path_glob,
            self.raw(),
            &path.into(),
            Some(copy_glob_callback),
            &mut items as *mut Vec<String> as *mut std::os::raw::c_void
        )?;

        Ok(items)
    }
}
/// Configures how a path is opened through a DuckDB [`FileSystem`].
///
/// Access and creation flags are disabled by default and can be composed with
/// the builder methods before calling [`FileBuilder::open`].
#[derive(Debug, DropHandle)]
#[drop_handle(ffi::duckdb_v2_file_open_options_destroy)]
pub struct FileBuilder<'link> {
    fs: &'link FileSystem<'link>,
    handle: ffi::duckdb_v2_file_open_options_handle,
    path: String,
}

impl<'link> FileBuilder<'link> {
    /// Create a builder for `path` with no flags enabled.
    pub fn new(fs: &'link FileSystem<'link>, path: &str) -> Result<FileBuilder<'link>> {
        check_api_call!(ffi::duckdb_v2_file_open_options_create, fs.handle, RET).map(|handle| FileBuilder {
            fs,
            handle,
            path: path.to_string(),
        })
    }

    /// Enable write access.
    pub fn write(self) -> Result<Self> {
        check_api_call!(
            ffi::duckdb_v2_file_open_options_set_flag,
            self.handle,
            ffi::DUCKDB_V2_FILE_FLAG::DUCKDB_V2_FILE_FLAG_WRITE
        )?;
        Ok(self)
    }

    /// Enable read access.
    pub fn read(self) -> Result<Self> {
        check_api_call!(
            ffi::duckdb_v2_file_open_options_set_flag,
            self.handle,
            ffi::DUCKDB_V2_FILE_FLAG::DUCKDB_V2_FILE_FLAG_READ
        )?;
        Ok(self)
    }

    /// Create the file when it does not exist.
    pub fn create(self) -> Result<Self> {
        check_api_call!(
            ffi::duckdb_v2_file_open_options_set_flag,
            self.handle,
            ffi::DUCKDB_V2_FILE_FLAG::DUCKDB_V2_FILE_FLAG_CREATE
        )?;
        Ok(self)
    }

    /// Create the file if missing, or truncate it to empty if it exists.
    pub fn create_new(self) -> Result<Self> {
        check_api_call!(
            ffi::duckdb_v2_file_open_options_set_flag,
            self.handle,
            ffi::DUCKDB_V2_FILE_FLAG::DUCKDB_V2_FILE_FLAG_CREATE_NEW
        )?;
        Ok(self)
    }

    /// Create the file only if it does not exist, failing if it already does.
    pub fn exclusive_create(self) -> Result<Self> {
        let new = self.create()?;
        check_api_call!(
            ffi::duckdb_v2_file_open_options_set_flag,
            new.handle,
            ffi::DUCKDB_V2_FILE_FLAG::DUCKDB_V2_FILE_FLAG_EXCLUSIVE_CREATE
        )?;
        Ok(new)
    }

    /// Enable append mode.
    pub fn append(self) -> Result<Self> {
        check_api_call!(
            ffi::duckdb_v2_file_open_options_set_flag,
            self.handle,
            ffi::DUCKDB_V2_FILE_FLAG::DUCKDB_V2_FILE_FLAG_APPEND
        )?;
        Ok(self)
    }

    /// Permit concurrent reads and writes at explicit offsets.
    ///
    /// Set this when [`File::read_at`] or [`File::write_at`] are used from several threads.
    pub fn parallel_access(self) -> Result<Self> {
        check_api_call!(
            ffi::duckdb_v2_file_open_options_set_flag,
            self.handle,
            ffi::DUCKDB_V2_FILE_FLAG::DUCKDB_V2_FILE_FLAG_PARALLEL_ACCESS
        )?;
        Ok(self)
    }

    /// Attach a named hint for the file system that handles the path.
    ///
    /// Unrecognized names are ignored.
    pub fn set_value(self, name: &str, value: &Value) -> Result<Self> {
        check_api_call!(
            ffi::duckdb_v2_file_open_options_set_value,
            self.handle,
            &name.into(),
            value.raw()
        )?;
        Ok(self)
    }

    /// Open the path with the configured flags.
    pub fn open(self) -> Result<File> {
        File::open(self.fs, &self.path, self.handle)
    }
}

/// An open file owned by the caller.
///
/// Reads and writes advance one shared byte position.
///
/// # Example
/// ```
/// use duckdb_neo::environment::{Environment, StorageLocation};
/// use duckdb_neo::file::{File, FileSystem, FileBuilder};
///
/// # fn main() -> duckdb_neo::Result<()> {
/// let env = Environment::new()?;
/// let db = env.open(StorageLocation::InMemory)?;
/// let conn = db.connect()?;
/// let fs = FileSystem::new(&conn)?;
/// let path = std::env::temp_dir().join("duckdb-rs-file-example.txt");
/// let file = FileBuilder::new(&fs, path.to_str().unwrap())?
///         .write()?
///         .read()?
///         .create()?
///         .open()?;
/// file.write(b"DuckDB")?;
/// file.seek(0)?;
/// assert_eq!(file.read(6)?, b"DuckDB");
/// file.close()?;
/// std::fs::remove_file(path).expect("failed to remove example file");
/// # Ok(())
/// # }
/// ```
#[derive(Debug, DropHandle)]
#[drop_handle(ffi::duckdb_v2_file_destroy)]
pub struct File {
    /// The owned DuckDB file handle.
    handle: ffi::duckdb_v2_file_handle,
    /// Dropped after the handle is destroyed.
    _database: Option<KeepAlive>,
}

// SAFETY: a `FileHandle` has no thread affinity. Not `Sync`: concurrent reads are only safe for
// files opened with `FILE_FLAG_PARALLEL_ACCESS`.
unsafe impl Send for File {}

impl File {
    /// Close the underlying file without destroying its handle.
    ///
    /// The handle is still destroyed on drop but cannot be read, written,
    /// sought, or synchronized after this call.
    pub fn close(&self) -> crate::Result<()> {
        check_api_call!(ffi::duckdb_v2_file_close, self.handle)
    }

    /// Open `path` with a bitwise combination of DuckDB file flags.
    pub(crate) fn open(
        fs: &FileSystem<'_>,
        path: &str,
        flags: ffi::duckdb_v2_file_open_options_handle,
    ) -> crate::Result<Self> {
        check_api_call!(ffi::duckdb_v2_file_system_open, fs.handle, &path.into(), flags, RET).map(|handle| File {
            handle,
            _database: fs.database.clone(),
        })
    }

    /// Read up to `len` bytes from the current position.
    pub fn read(&self, len: usize) -> Result<Vec<u8>> {
        let mut buffer = vec![0u8; len];
        let mut bytes_read: u64 = 0;

        check_api_call!(
            ffi::duckdb_v2_file_read,
            self.handle,
            buffer.as_mut_ptr() as *mut std::ffi::c_void,
            buffer.len() as u64,
            &mut bytes_read,
        )?;

        buffer.truncate(bytes_read as usize);

        Ok(buffer)
    }

    /// Set the current position to an absolute byte offset.
    pub fn seek(&self, position: usize) -> Result<()> {
        check_api_call!(ffi::duckdb_v2_file_seek, self.handle, position as u64)
    }

    /// Return the file size in bytes.
    pub fn size(&self) -> Result<u64> {
        check_api_call!(ffi::duckdb_v2_file_size, self.handle, RET)
    }

    /// Flush buffered writes to persistent storage.
    pub fn sync(&self) -> Result<()> {
        check_api_call!(ffi::duckdb_v2_file_sync, self.handle)
    }

    /// Return the current byte position.
    pub fn tell(&self) -> Result<u64> {
        check_api_call!(ffi::duckdb_v2_file_tell, self.handle, RET)
    }

    /// Write bytes at the current position and return the number written.
    pub fn write(&self, buffer: &[u8]) -> Result<usize> {
        let mut bytes_written: u64 = 0;

        check_api_call!(
            ffi::duckdb_v2_file_write,
            self.handle,
            buffer.as_ptr() as *const std::ffi::c_void,
            buffer.len() as u64,
            &mut bytes_written,
        )?;

        Ok(bytes_written as usize)
    }

    /// Read exactly `len` bytes from `position`; returns an error when not enough bytes could be read.
    pub fn read_at(&self, position: usize, len: usize) -> Result<Vec<u8>> {
        let mut buffer = vec![0u8; len];

        check_api_call!(
            ffi::duckdb_v2_file_read_at,
            self.handle,
            buffer.as_mut_ptr() as *mut std::ffi::c_void,
            len as u64,
            position as u64
        )?;
        Ok(buffer)
    }
    /// Write bytes at position. Increases file-size when overflowing.
    pub fn write_at(&self, position: usize, buffer: &[u8]) -> Result<()> {
        check_api_call!(
            ffi::duckdb_v2_file_write_at,
            self.handle,
            buffer.as_ptr() as *const std::ffi::c_void,
            buffer.len() as u64,
            position as u64,
        )
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use crate::{
        Parameters,
        environment::{Environment, StorageLocation},
        file::{FileBuilder, FileSystem},
    };

    #[test]
    fn test_file_read() -> crate::Result<()> {
        let env = Environment::new()?;
        let db = env.open(StorageLocation::InMemory)?;
        let conn = db.connect()?;

        let fs = FileSystem::new(&conn)?;

        let file = FileBuilder::new(&fs, "test_file.txt")?
            .write()?
            .create_new()?
            .read()?
            .open()?;

        file.write("HELLO RUST CLIENT!".as_bytes())?;
        file.write_at(11, "DUCKDB".as_bytes())?;

        file.sync()?;

        file.seek(6)?;

        let res = file.read(4)?;

        let res = String::from_utf8(res).unwrap();

        assert_eq!(res, "RUST");

        assert_eq!(file.tell()?, 10);

        assert_eq!(file.size()?, 18);

        assert_eq!(String::from_utf8(file.read_at(11, 6)?).unwrap(), "DUCKDB");

        file.close()?;

        std::fs::remove_file("test_file.txt").unwrap();

        Ok(())
    }

    #[test]
    fn test_connection_file_keeps_connection_alive() -> crate::Result<()> {
        let env = Environment::new()?;
        let db = env.open(StorageLocation::InMemory)?;
        let conn = db.connect()?;
        let path = std::env::temp_dir().join("duckdb-rs-file-keep-alive.txt");

        let file = {
            let fs = FileSystem::new(&conn)?;
            FileBuilder::new(&fs, path.to_str().unwrap())?
                .read()?
                .write()?
                .create()?
                .open()?
        };

        drop(conn);
        drop(db);
        assert_eq!(env.get_database_count()?, 1);

        file.write(b"DuckDB")?;
        file.seek(0)?;
        assert_eq!(file.read(6)?, b"DuckDB");
        drop(file);
        assert_eq!(env.get_database_count()?, 0);

        std::fs::remove_file(path).expect("failed to remove test file");
        Ok(())
    }

    #[test]
    fn test_file_system_paths() -> crate::Result<()> {
        let env = Environment::new()?;
        let db = env.open(StorageLocation::InMemory)?;
        let fs = FileSystem::new(&db)?;

        let dir = std::env::temp_dir().join(format!("duckdb-rs-file-system-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("failed to create test dir");
        let path = |name: &str| dir.join(name).to_str().unwrap().to_string();

        for name in ["a.txt", "b.txt", "c.csv"] {
            FileBuilder::new(&fs, &path(name))?.write()?.create()?.open()?;
        }
        assert!(fs.exists(&path("a.txt"))?);
        assert!(!fs.exists(&path("missing.txt"))?);
        // A directory is not a file.
        assert!(!fs.exists(dir.to_str().unwrap())?);

        let mut matched = fs.glob(&path("*.txt"))?;
        matched.sort();
        assert_eq!(matched, [path("a.txt"), path("b.txt")]);
        assert!(fs.glob(&path("*.json"))?.is_empty());

        fs.move_item(&path("a.txt"), &path("moved.txt"))?;
        assert!(!fs.exists(&path("a.txt"))?);
        assert!(fs.exists(&path("moved.txt"))?);

        assert!(fs.remove(&path("moved.txt"))?);
        // Removing a missing file is not an error.
        assert!(!fs.remove(&path("moved.txt"))?);

        assert!(!fs.is_remote(&path("b.txt"))?);
        assert!(fs.is_remote("s3://bucket/data.parquet")?);

        std::fs::remove_dir_all(&dir).expect("failed to remove test dir");
        Ok(())
    }

    #[test]
    fn test_file_system_expand() -> crate::Result<()> {
        let env = Environment::new()?;
        let db = env.open(StorageLocation::InMemory)?;
        let conn = db.connect()?;
        conn.execute("SET home_directory = '/duck/home'", Parameters::None)?;
        let fs = FileSystem::new(&conn)?;

        assert_eq!(fs.expand("~/data.csv")?, "/duck/home/data.csv");
        assert_eq!(fs.expand("/plain/data.csv")?, "/plain/data.csv");
        Ok(())
    }

    // macOS file systems reject names that are not valid UTF-8.
    #[cfg(target_os = "linux")]
    #[test]
    fn test_glob_non_utf8_name() -> crate::Result<()> {
        use std::os::unix::ffi::OsStrExt;

        let env = Environment::new()?;
        let db = env.open(StorageLocation::InMemory)?;
        let fs = FileSystem::new(&db)?;

        let dir = tempfile::tempdir().expect("failed to create test dir");
        std::fs::write(dir.path().join(std::ffi::OsStr::from_bytes(b"\xff.txt")), b"").expect("failed to write file");
        let dir = dir.path().to_str().unwrap();

        assert_eq!(fs.glob(&format!("{dir}/*.txt"))?, [format!("{dir}/\u{fffd}.txt")]);
        Ok(())
    }
}

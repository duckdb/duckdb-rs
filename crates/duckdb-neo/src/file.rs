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
    links::{FileSystemLink, InstanceKeepAlive, KeepAlive},
    raw::RawExt,
    value::Value,
};

unsafe extern "C" fn copy_glob_callback(
    text: *const ffi::duckdb_v2_str,
    user_data: *mut std::os::raw::c_void,
    err: *mut ffi::duckdb_v2_error_info_handle,
) {
    handle_unwind(
        || {
            if text.is_null() || user_data.is_null() {
                return Err(Error::api_error("NULL value in copy_glob_callback"));
            }

            // SAFETY: glob passes its live, exclusively borrowed output Vec as user_data.
            let items = unsafe { &mut *(user_data as *mut Vec<String>) };
            // SAFETY: DuckDB passes a live string descriptor for the duration of the callback.
            let text = unsafe { *text };

            items.push(
                DuckDBStr::from_raw(text)
                    .to_string_lossy()
                    .unwrap_or_default()
                    .into_owned(),
            );

            Ok(())
        },
        err,
    );
}

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
    instance: Option<KeepAlive>,
    _marker: PhantomData<&'link ()>,
}

impl<'link> FileSystem<'link> {
    /// Borrow the file system of a [`Connection`](crate::connection::Connection), [`Instance`](crate::instance::Instance), or [`Context`](crate::connection::Context).
    /// Connection/instance files keep their owner alive; context files may outlive the callback but not its connection.
    #[allow(private_bounds)]
    pub fn new<C: FileSystemLink + InstanceKeepAlive>(link: &'link C) -> Result<Self> {
        let handle = link.get_file_system()?;

        Ok(FileSystem {
            handle,
            instance: link.keep_alive(),
            _marker: PhantomData,
        })
    }

    /// Whether a file exists at `path`; directories return false.
    pub fn exists(&self, path: &str) -> Result<bool> {
        check_api_call!(ffi::duckdb_v2_file_system_path_exists, self.raw(), &path.into(), RET)
    }

    /// Whether `path` names a remote file, determined without opening it or loading extensions.
    pub fn is_remote(&self, path: &str) -> Result<bool> {
        check_api_call!(ffi::duckdb_v2_file_system_path_is_remote, self.raw(), &path.into(), RET)
    }

    /// Move a file within one file system; replacement and atomicity depend on that file system.
    pub fn move_item(&self, from: &str, to: &str) -> Result<()> {
        check_api_call!(
            ffi::duckdb_v2_file_system_path_move,
            self.raw(),
            &from.into(),
            &to.into(),
        )
    }

    /// Remove a file, returning false if it does not exist.
    pub fn remove(&self, path: &str) -> Result<bool> {
        check_api_call!(ffi::duckdb_v2_file_system_path_remove, self.raw(), &path.into(), RET)
    }

    /// Expand a path, replacing a leading `~` with the configured home directory.
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

    /// List matching file paths in unspecified order; a literal path matches only if it exists.
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
#[derive(Debug, AsRaw, DropHandle)]
#[drop_handle(ffi::duckdb_v2_file_open_options_destroy)]
pub struct FileBuilder<'link> {
    fs: &'link FileSystem<'link>,
    handle: ffi::duckdb_v2_file_open_options_handle,
    path: String,
    flags: Vec<ffi::DUCKDB_V2_FILE_FLAG>,
}

impl<'link> FileBuilder<'link> {
    /// Create a builder for `path` with no flags enabled.
    pub fn new(fs: &'link FileSystem<'link>, path: &str) -> Result<FileBuilder<'link>> {
        check_api_call!(ffi::duckdb_v2_file_open_options_create, fs.handle, RET).map(|handle| FileBuilder {
            fs,
            handle,
            path: path.to_string(),
            flags: Vec::new(),
        })
    }

    /// Enable write access.
    pub fn write(mut self) -> Self {
        self.flags.push(ffi::DUCKDB_V2_FILE_FLAG::DUCKDB_V2_FILE_FLAG_WRITE);
        self
    }

    /// Enable read access.
    pub fn read(mut self) -> Self {
        self.flags.push(ffi::DUCKDB_V2_FILE_FLAG::DUCKDB_V2_FILE_FLAG_READ);
        self
    }

    /// Create the file when it does not exist.
    pub fn create(mut self) -> Self {
        self.flags.push(ffi::DUCKDB_V2_FILE_FLAG::DUCKDB_V2_FILE_FLAG_CREATE);
        self
    }

    /// Create the file if missing, or truncate it to empty if it exists.
    pub fn create_new(mut self) -> Self {
        self.flags
            .push(ffi::DUCKDB_V2_FILE_FLAG::DUCKDB_V2_FILE_FLAG_CREATE_NEW);
        self
    }

    /// Create the file only if it does not exist, failing if it already does.
    pub fn exclusive_create(self) -> Self {
        let mut builder = self.create();
        builder
            .flags
            .push(ffi::DUCKDB_V2_FILE_FLAG::DUCKDB_V2_FILE_FLAG_EXCLUSIVE_CREATE);
        builder
    }

    /// Enable append mode.
    pub fn append(mut self) -> Self {
        self.flags.push(ffi::DUCKDB_V2_FILE_FLAG::DUCKDB_V2_FILE_FLAG_APPEND);
        self
    }

    /// Hint that the file system should expect access at explicit offsets.
    /// This does not make [`File`] shareable between threads.
    pub fn parallel_access(mut self) -> Self {
        self.flags
            .push(ffi::DUCKDB_V2_FILE_FLAG::DUCKDB_V2_FILE_FLAG_PARALLEL_ACCESS);
        self
    }

    /// Read through DuckDB's external file cache; do not combine with write or append.
    pub fn external_file_cache(mut self) -> Self {
        self.flags
            .push(ffi::DUCKDB_V2_FILE_FLAG::DUCKDB_V2_FILE_FLAG_EXTERNAL_FILE_CACHE);
        self
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
        for flag in &self.flags {
            check_api_call!(ffi::duckdb_v2_file_open_options_set_flag, self.handle, *flag)?;
        }
        File::open(self.fs, &self.path, self.handle)
    }
}

/// An open file owned by the caller.
///
/// Reads and writes advance one shared byte position. A file opened through a
/// [`Context`](crate::connection::Context) may outlive the callback but not its connection.
///
/// # Example
/// ```
/// use duckdb_neo::environment::{Environment, StorageLocation};
/// use duckdb_neo::file::{FileSystem, FileBuilder};
///
/// # fn main() -> duckdb_neo::Result<()> {
/// let env = Environment::new()?;
/// let instance = env.open(StorageLocation::InMemory)?;
/// let conn = instance.connect()?;
/// let fs = FileSystem::new(&conn)?;
/// let dir = tempfile::tempdir().expect("failed to create example directory");
/// let path = dir.path().join("example.txt");
/// let file = FileBuilder::new(&fs, path.to_str().unwrap())?
///         .write()
///         .read()
///         .create()
///         .open()?;
/// file.write(b"DuckDB")?;
/// file.seek(0)?;
/// assert_eq!(file.read(6)?, b"DuckDB");
/// file.close()?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, AsRaw, DropHandle)]
#[drop_handle(ffi::duckdb_v2_file_destroy)]
pub struct File {
    handle: ffi::duckdb_v2_file_handle,
    // Keep the instance alive until the file handle is destroyed.
    _instance: Option<KeepAlive>,
}

// SAFETY: a `FileHandle` has no thread affinity. Not `Sync`: concurrent reads are only safe for
// files opened with `FILE_FLAG_PARALLEL_ACCESS`.
unsafe impl Send for File {}

impl File {
    /// Close the underlying file without destroying its handle.
    ///
    /// The handle is still destroyed on drop but cannot be read, written,
    /// or synchronized after this call.
    pub fn close(&self) -> crate::Result<()> {
        check_api_call!(ffi::duckdb_v2_file_close, self.handle)
    }

    pub(crate) fn open(
        fs: &FileSystem<'_>,
        path: &str,
        options: ffi::duckdb_v2_file_open_options_handle,
    ) -> crate::Result<Self> {
        check_api_call!(ffi::duckdb_v2_file_system_open, fs.handle, &path.into(), options, RET).map(|handle| File {
            handle,
            _instance: fs.instance.clone(),
        })
    }

    /// Read up to `len` bytes from the current position.
    pub fn read(&self, len: usize) -> Result<Vec<u8>> {
        let mut buffer = vec![0u8; len];
        let bytes_read = self.read_into(&mut buffer)?;
        buffer.truncate(bytes_read);
        Ok(buffer)
    }

    /// Read into `buffer` from the current position, returning the number of bytes read.
    pub fn read_into(&self, buffer: &mut [u8]) -> Result<usize> {
        let mut bytes_read: u64 = 0;

        check_api_call!(
            ffi::duckdb_v2_file_read,
            self.handle,
            buffer.as_mut_ptr() as *mut std::ffi::c_void,
            buffer.len() as u64,
            &mut bytes_read,
        )?;

        Ok(bytes_read as usize)
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

        self.read_exact_at(position, &mut buffer)?;
        Ok(buffer)
    }

    /// Fill `buffer` from `position` without moving the cursor; a short read is an error.
    pub fn read_exact_at(&self, position: usize, buffer: &mut [u8]) -> Result<()> {
        check_api_call!(
            ffi::duckdb_v2_file_read_at,
            self.handle,
            buffer.as_mut_ptr() as *mut std::ffi::c_void,
            buffer.len() as u64,
            position as u64
        )
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
    fn test_file_builder_flags() -> crate::Result<()> {
        let env = Environment::new()?;
        let instance = env.open(StorageLocation::InMemory)?;
        let fs = FileSystem::new(&instance)?;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("flags.txt");
        let path = path.to_str().unwrap();
        let builder = || FileBuilder::new(&fs, path);

        assert!(builder()?.open().is_err());
        assert!(!std::path::Path::new(path).exists());

        let file = builder()?.write().read().exclusive_create().open()?;
        assert_eq!(file.write(b"ok")?, 2);
        file.seek(0)?;
        assert_eq!(file.read(2)?, b"ok");
        Ok(())
    }

    #[test]
    fn test_file_external_cache() -> crate::Result<()> {
        let env = Environment::new()?;
        let instance = env.open(StorageLocation::InMemory)?;
        let fs = FileSystem::new(&instance)?;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cached.txt");
        std::fs::write(&path, b"cached bytes").unwrap();

        let file = FileBuilder::new(&fs, path.to_str().unwrap())?
            .read()
            .external_file_cache()
            .open()?;
        assert_eq!(file.read(12)?, b"cached bytes");
        Ok(())
    }

    #[test]
    fn test_file_read_into() -> crate::Result<()> {
        let env = Environment::new()?;
        let instance = env.open(StorageLocation::InMemory)?;
        let fs = FileSystem::new(&instance)?;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("read-into.txt");
        std::fs::write(&path, b"abcdef").unwrap();
        let file = FileBuilder::new(&fs, path.to_str().unwrap())?.read().open()?;

        let mut buffer = [0; 4];
        assert_eq!(file.read_into(&mut buffer)?, 4);
        assert_eq!(&buffer, b"abcd");
        assert_eq!(file.read_into(&mut buffer)?, 2);
        assert_eq!(&buffer, b"efcd");
        assert_eq!(file.read_into(&mut buffer)?, 0);
        assert_eq!(file.read_into(&mut [])?, 0);
        assert_eq!(file.tell()?, 6);

        let position: usize = 1;
        file.read_exact_at(position, &mut buffer)?;
        assert_eq!(&buffer, b"bcde");
        assert_eq!(file.tell()?, 6);
        assert!(file.read_exact_at(4, &mut buffer).is_err());
        Ok(())
    }

    #[test]
    fn test_file_read() -> crate::Result<()> {
        let env = Environment::new()?;
        let instance = env.open(StorageLocation::InMemory)?;
        let conn = instance.connect()?;

        let fs = FileSystem::new(&conn)?;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("read.txt");

        let file = FileBuilder::new(&fs, path.to_str().unwrap())?
            .write()
            .create_new()
            .read()
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

        Ok(())
    }

    #[test]
    fn test_connection_file_keeps_connection_alive() -> crate::Result<()> {
        let env = Environment::new()?;
        let instance = env.open(StorageLocation::InMemory)?;
        let conn = instance.connect()?;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("keep-alive.txt");

        let file = {
            let fs = FileSystem::new(&conn)?;
            FileBuilder::new(&fs, path.to_str().unwrap())?
                .read()
                .write()
                .create()
                .open()?
        };

        drop(conn);
        drop(instance);
        assert_eq!(env.instance_count()?, 1);

        file.write(b"DuckDB")?;
        file.seek(0)?;
        assert_eq!(file.read(6)?, b"DuckDB");
        drop(file);
        assert_eq!(env.instance_count()?, 0);

        Ok(())
    }

    #[test]
    fn test_instance_file_keeps_instance_alive() -> crate::Result<()> {
        let env = Environment::new()?;
        let instance = env.open(StorageLocation::InMemory)?;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("instance-keep-alive.txt");
        let file = {
            let fs = FileSystem::new(&instance)?;
            FileBuilder::new(&fs, path.to_str().unwrap())?
                .read()
                .write()
                .create()
                .open()?
        };

        drop(instance);
        assert_eq!(env.instance_count()?, 1);
        file.write(b"alive")?;
        assert_eq!(file.read_at(0, 5)?, b"alive");
        drop(file);
        assert_eq!(env.instance_count()?, 0);
        Ok(())
    }

    #[test]
    fn test_connection_chunk_copy_keeps_instance_alive() -> crate::Result<()> {
        use crate::{DuckDBType, data_chunk::DataChunk};

        let env = Environment::new()?;
        let instance = env.open(StorageLocation::InMemory)?;
        let conn = instance.connect()?;
        let copy = {
            let mut chunk = DataChunk::create(&conn, [i32::logical_type(&conn)?], true)?;
            let mut vector = chunk.get_vector_at_mut::<i32>(0)?;
            vector.set_size(1)?;
            vector.writer()?.set(0, Some(42))?;
            chunk.copy(&conn)?
        };

        drop(conn);
        drop(instance);
        assert_eq!(env.instance_count()?, 1);
        assert_eq!(copy.row_count()?, 1);
        assert_eq!(copy.get_vector_at::<i32>(0)?.reader()?.get(0)?, Some(&42));
        drop(copy);
        assert_eq!(env.instance_count()?, 0);
        Ok(())
    }

    #[test]
    fn test_file_system_paths() -> crate::Result<()> {
        let env = Environment::new()?;
        let instance = env.open(StorageLocation::InMemory)?;
        let fs = FileSystem::new(&instance)?;

        let dir = tempfile::tempdir().unwrap();
        let path = |name: &str| dir.path().join(name).to_str().unwrap().to_string();

        for name in ["a.txt", "b.txt", "c.csv"] {
            FileBuilder::new(&fs, &path(name))?.write().create().open()?;
        }
        assert!(fs.exists(&path("a.txt"))?);
        assert!(!fs.exists(&path("missing.txt"))?);
        // A directory is not a file.
        assert!(!fs.exists(dir.path().to_str().unwrap())?);

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

        Ok(())
    }

    #[test]
    fn test_file_open_modes_and_hints() -> crate::Result<()> {
        use crate::ToValue;

        let env = Environment::new()?;
        let instance = env.open(StorageLocation::InMemory)?;
        let conn = instance.connect()?;
        let fs = FileSystem::new(&conn)?;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("modes.txt");
        let path = path.to_str().unwrap();
        assert!(FileBuilder::new(&fs, path)?.read().open().is_err());

        let file = FileBuilder::new(&fs, path)?.write().exclusive_create().open()?;
        file.write(b"abc")?;
        drop(file);
        assert!(FileBuilder::new(&fs, path)?.write().exclusive_create().open().is_err());

        let file = FileBuilder::new(&fs, path)?
            .write()
            .append()
            .set_value("unknown_hint", &42_i32.value(&conn)?)?
            .open()?;
        file.write(b"def")?;
        drop(file);
        assert_eq!(std::fs::read(path).unwrap(), b"abcdef");

        let file = FileBuilder::new(&fs, path)?.read().write().parallel_access().open()?;
        file.write_at(2, b"XY")?;
        assert_eq!(file.read_at(1, 4)?, b"bXYe");
        assert_eq!(file.tell()?, 0);
        assert!(file.read_at(6, 1).is_err());
        file.seek(6)?;
        assert!(file.read(1)?.is_empty());
        Ok(())
    }

    #[test]
    fn test_file_operations_after_close() -> crate::Result<()> {
        let env = Environment::new()?;
        let instance = env.open(StorageLocation::InMemory)?;
        let fs = FileSystem::new(&instance)?;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("closed.txt");
        let file = FileBuilder::new(&fs, path.to_str().unwrap())?
            .read()
            .write()
            .create()
            .open()?;
        file.close()?;

        assert!(file.read(1).is_err());
        assert!(file.read_at(0, 1).is_err());
        assert!(file.write(b"x").is_err());
        assert!(file.write_at(0, b"x").is_err());
        // Windows only records the seek position, so it succeeds after close.
        #[cfg(not(windows))]
        assert!(file.seek(0).is_err());
        #[cfg(windows)]
        assert!(file.seek(0).is_ok());
        assert!(file.sync().is_err());
        Ok(())
    }

    #[test]
    fn test_glob_callback_error() -> crate::Result<()> {
        use crate::{
            error::{Error, check_api_call_no_err},
            ffi,
            raw::RawExt,
        };

        let env = Environment::new()?;
        let instance = env.open(StorageLocation::InMemory)?;
        let fs = FileSystem::new(&instance)?;
        let builder = FileBuilder::new(&fs, "unused")?;
        let mut error = std::ptr::null_mut();
        // SAFETY: the options are live; an invalid flag creates an owned error handle.
        let code = unsafe {
            ffi::duckdb_v2_file_open_options_set_flag(
                builder.raw(),
                ffi::DUCKDB_V2_FILE_FLAG::DUCKDB_V2_FILE_FLAG_INVALID,
                &mut error,
            )
        };
        assert_ne!(code, ffi::DUCKDB_V2_ERROR::DUCKDB_V2_ERROR_NONE);
        assert!(!error.is_null());
        // SAFETY: the callback rejects null inputs before dereferencing them; error is live.
        unsafe { super::copy_glob_callback(std::ptr::null(), std::ptr::null_mut(), &mut error) };
        let mut code = ffi::DUCKDB_V2_ERROR::DUCKDB_V2_ERROR_NONE;
        check_api_call_no_err!(ffi::duckdb_v2_error_info_get_code, error, &mut code)?;
        assert_eq!(code, ffi::DUCKDB_V2_ERROR::DUCKDB_V2_ERROR_API);
        // SAFETY: this test transfers ownership of the live error handle.
        let result = unsafe { Error::from_code_and_handle(code, &mut error) };
        assert!(result.message.contains("NULL value in copy_glob_callback"));
        Ok(())
    }

    #[test]
    fn test_file_system_expand() -> crate::Result<()> {
        let env = Environment::new()?;
        let instance = env.open(StorageLocation::InMemory)?;
        let conn = instance.connect()?;
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
        let instance = env.open(StorageLocation::InMemory)?;
        let fs = FileSystem::new(&instance)?;

        let dir = tempfile::tempdir().expect("failed to create test dir");
        std::fs::write(dir.path().join(std::ffi::OsStr::from_bytes(b"\xff.txt")), b"").expect("failed to write file");
        let dir = dir.path().to_str().unwrap();

        assert_eq!(fs.glob(&format!("{dir}/*.txt"))?, [format!("{dir}/\u{fffd}.txt")]);
        Ok(())
    }
}

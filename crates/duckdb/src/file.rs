//! File access through DuckDB's file system.

use crate::{Result, builder_helpers::context_and_connection_fn, check_api_call, check_api_call_no_err, ffi};

/// A borrowed handle to DuckDB's file system.
pub struct FileSystem {
    /// The borrowed DuckDB file-system handle.
    pub handle: ffi::duckdb_v2_file_system_handle,
}

impl FileSystem {
    context_and_connection_fn! {
        /// Borrow the file system associated with a connection or callback context.
        pub fn from_[context, connection]() -> Result<Self>
        {
            context_fn: ffi::duckdb_v2_file_system_get_from_context,
            connection_fn: ffi::duckdb_v2_file_system_get_from_connection,
        }
        let handle = check_api_call!(api_fn!(), **api_arg!(), RET)?;

        Ok(FileSystem { handle })
    }
}

/// Configures how a path is opened through a DuckDB [`FileSystem`].
///
/// Access and creation flags are disabled by default and can be composed with
/// the builder methods before calling [`FileBuilder::open`].
pub struct FileBuilder<'a> {
    fs: &'a FileSystem,
    path: String,
    flags: u64,
}

macro_rules! set_flag {
    ($flags:expr, $flag:expr, $enable:expr) => {
        if $enable {
            $flags |= $flag as u64;
        } else {
            $flags &= !($flag as u64);
        }
    };
}

impl<'a> FileBuilder<'a> {
    /// Create a builder for `path` with no flags enabled.
    pub fn new(fs: &'a FileSystem, path: &str) -> FileBuilder<'a> {
        FileBuilder {
            fs,
            path: path.to_string(),
            flags: 0,
        }
    }

    /// Enable or disable write access.
    pub fn write(mut self, write: bool) -> Self {
        set_flag!(self.flags, ffi::DUCKDB_V2_FILE_FLAG::DUCKDB_V2_FILE_FLAG_WRITE, write);
        self
    }

    /// Enable or disable read access.
    pub fn read(mut self, read: bool) -> Self {
        set_flag!(self.flags, ffi::DUCKDB_V2_FILE_FLAG::DUCKDB_V2_FILE_FLAG_READ, read);
        self
    }

    /// Enable or disable creating the file when it does not exist.
    pub fn create(mut self, create: bool) -> Self {
        set_flag!(self.flags, ffi::DUCKDB_V2_FILE_FLAG::DUCKDB_V2_FILE_FLAG_CREATE, create);
        self
    }

    /// Enable or disable exclusive creation that fails if the file exists.
    pub fn create_new(mut self, create_new: bool) -> Self {
        set_flag!(
            self.flags,
            ffi::DUCKDB_V2_FILE_FLAG::DUCKDB_V2_FILE_FLAG_CREATE_NEW,
            create_new
        );
        self
    }

    /// Enable or disable append mode.
    pub fn append(mut self, append: bool) -> Self {
        set_flag!(self.flags, ffi::DUCKDB_V2_FILE_FLAG::DUCKDB_V2_FILE_FLAG_APPEND, append);
        self
    }

    /// Open the path with the configured flags.
    pub fn open(self) -> Result<File> {
        File::open(self.fs, &self.path, self.flags)
    }
}

/// An open file owned by the caller.
///
/// Reads and writes advance one shared byte position.
///
/// # Example
/// ```
/// use duckdb_rs::{Environment, StorageLocation};
/// use duckdb_rs::file::{File, FileSystem, FileBuilder};
///
/// # fn main() -> duckdb_rs::Result<()> {
/// let env = Environment::new()?;
/// let db = env.open(StorageLocation::InMemory)?;
/// let conn = db.connect()?;
/// let fs = FileSystem::from_connection(&conn)?;
/// let path = std::env::temp_dir().join("duckdb-rs-file-example.txt");
/// let file = FileBuilder::new(&fs, path.to_str().unwrap())
///         .write(true)
///         .read(true)
///         .create(true)
///         .open()?;
/// file.write(b"DuckDB")?;
/// file.seek(0)?;
/// assert_eq!(file.read(6)?, b"DuckDB");
/// file.close()?;
/// std::fs::remove_file(path).expect("failed to remove example file");
/// # Ok(())
/// # }
/// ```
pub struct File {
    /// The owned DuckDB file handle.
    pub handle: ffi::duckdb_v2_file_handle_handle,
}

impl File {
    /// Close the underlying file without destroying its handle.
    ///
    /// The handle is still destroyed on drop but cannot be read, written,
    /// sought, or synchronized after this call.
    pub fn close(&self) -> crate::Result<()> {
        check_api_call!(ffi::duckdb_v2_file_handle_close, self.handle)
    }

    /// Open `path` with a bitwise combination of DuckDB file flags.
    pub(crate) fn open(fs: &FileSystem, path: &str, flags: u64) -> crate::Result<Self> {
        Ok(File {
            handle: check_api_call!(ffi::duckdb_v2_file_system_open, fs.handle, path.into(), flags, RET)?,
        })
    }

    /// Read up to `len` bytes from the current position.
    pub fn read(&self, len: usize) -> Result<Vec<u8>> {
        let mut buffer = vec![0u8; len];
        let mut bytes_read: u64 = 0;

        check_api_call!(
            ffi::duckdb_v2_file_handle_read,
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
        check_api_call!(ffi::duckdb_v2_file_handle_seek, self.handle, position as u64)
    }

    /// Return the file size in bytes.
    pub fn size(&self) -> Result<u64> {
        check_api_call!(ffi::duckdb_v2_file_handle_size, self.handle, RET)
    }

    /// Flush buffered writes to persistent storage.
    pub fn sync(&self) -> Result<()> {
        check_api_call!(ffi::duckdb_v2_file_handle_sync, self.handle)
    }

    /// Return the current byte position.
    pub fn tell(&self) -> Result<u64> {
        check_api_call!(ffi::duckdb_v2_file_handle_tell, self.handle, RET)
    }

    /// Write bytes at the current position and return the number written.
    pub fn write(&self, buffer: &[u8]) -> Result<usize> {
        let mut bytes_written: u64 = 0;

        check_api_call!(
            ffi::duckdb_v2_file_handle_write,
            self.handle,
            buffer.as_ptr() as *const std::ffi::c_void,
            buffer.len() as u64,
            &mut bytes_written,
        )?;

        Ok(bytes_written as usize)
    }
}

impl Drop for File {
    fn drop(&mut self) {
        check_api_call_no_err!(ffi::duckdb_v2_file_handle_destroy, &mut self.handle)
            .expect("Failed to destroy file handle");
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use crate::{
        Environment, StorageLocation,
        file::{FileBuilder, FileSystem},
    };

    #[test]
    fn test_file_read() -> crate::Result<()> {
        let env = Environment::new()?;
        let db = env.open(StorageLocation::InMemory)?;
        let conn = db.connect()?;

        let fs = FileSystem::from_connection(&conn)?;

        let file = FileBuilder::new(&fs, "test_file.txt")
            .write(true)
            .create(true)
            .create_new(true)
            .read(true)
            .open()?;

        file.write("HELLO RUST CLIENT!".as_bytes())?;

        file.sync()?;

        file.seek(6)?;

        let res = file.read(4)?;

        let res = String::from_utf8(res).unwrap();

        assert_eq!(res, "RUST");

        assert_eq!(file.tell()?, 10);

        assert_eq!(file.size()?, 18);

        file.close()?;

        std::fs::remove_file("test_file.txt").unwrap();

        Ok(())
    }
}

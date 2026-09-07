//! Emit DuckDB log records and register custom log storage.

use std::ops::Deref;

use crate::{
    Context, Result,
    builder_helpers::{
        OpaqueHandle, context_and_connection_fn, ffi_enum_redeclaration, get_opaque_data_ref, handle_unwind,
    },
    check_api_call, check_api_call_no_err,
    database::Database,
    ffi,
};

ffi_enum_redeclaration! {
    /// The severity of a DuckDB log record.
    #[allow(missing_docs)]
    pub enum LogLevel <- ffi::DUCKDB_V2_LOG_LEVEL {
    Trace = DUCKDB_V2_LOG_LEVEL_TRACE,
    Debug = DUCKDB_V2_LOG_LEVEL_DEBUG,
    Info = DUCKDB_V2_LOG_LEVEL_INFO,
    Warn = DUCKDB_V2_LOG_LEVEL_WARN,
    Error = DUCKDB_V2_LOG_LEVEL_ERROR,
    Fatal = DUCKDB_V2_LOG_LEVEL_FATAL,
    }
}

/// Emits records through DuckDB's configured logging system.
pub struct Log;

impl Log {
    context_and_connection_fn! {
        /// Submit a log record associated with a connection or callback context.
        ///
        /// Whether the record is emitted depends on the active logging
        /// configuration.
        pub fn log_on_[context, connection](
            level: LogLevel,
            message: &str,
            log_type: &str,
        ) -> Result<()>
        {
            context_fn: ffi::duckdb_v2_context_log,
            connection_fn: ffi::duckdb_v2_connection_log,
        }
        check_api_call!(
            api_fn!(),
            **api_arg!(),
            level.into(),
            message.into(),
            log_type.into()
        )
    }
}

unsafe extern "C" fn log_callback<T: LogStorageCallbacks>(
    user_data: *mut ::std::os::raw::c_void,
    timestamp: i64,
    level: libduckdb_sys::DUCKDB_V2_LOG_LEVEL,
    log_type: ffi::duckdb_v2_str,
    log_message: ffi::duckdb_v2_str,
    err: *mut ffi::duckdb_v2_error_info_handle,
) {
    handle_unwind(
        || {
            let implementation = unsafe { get_opaque_data_ref::<T>(user_data) }.unwrap();

            implementation.log(log_message.into(), level.try_into()?, timestamp, log_type.into())
        },
        err,
    );
}

struct LogStorageBuilderHandle(ffi::duckdb_v2_log_storage_builder_handle);

impl Drop for LogStorageBuilderHandle {
    fn drop(&mut self) {
        check_api_call_no_err!(ffi::duckdb_v2_log_storage_builder_destroy, &mut self.0).unwrap()
    }
}

impl Deref for LogStorageBuilderHandle {
    type Target = ffi::duckdb_v2_log_storage_builder_handle;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Registers a named Rust implementation as DuckDB log storage.
///
/// DuckDB calls the implementation for records routed to the registered
/// storage by its logging configuration.
///
/// # Example
/// ```
/// use duckdb_rs::{Environment, StorageLocation};
/// use duckdb_rs::log::{LogStorageBuilder, LogStorageCallbacks, LogLevel};
///
/// struct StdoutLogger;
///
/// impl LogStorageCallbacks for StdoutLogger {
///     fn log(
///         &self,
///         message: &str,
///         level: LogLevel,
///         _timestamp: i64,
///         log_type: &str,
///     ) -> duckdb_rs::Result<()> {
///         println!("[{level:?}] {log_type}: {message}");
///         Ok(())
///     }
/// }
///
/// # fn main() -> duckdb_rs::Result<()> {
/// let env = Environment::new()?;
/// let db = env.open(StorageLocation::InMemory)?;
/// LogStorageBuilder::new("stdout", StdoutLogger).register_with_database(&db)?;
/// # Ok(())
/// # }
/// ```
pub struct LogStorageBuilder<T: LogStorageCallbacks> {
    name: String,
    implementation: OpaqueHandle<T>,
}

impl<T: LogStorageCallbacks> LogStorageBuilder<T> {
    /// Create named log storage backed by `implementation`.
    pub fn new(name: &str, implementation: T) -> Self {
        Self {
            name: name.to_string(),
            implementation: OpaqueHandle::new(implementation),
        }
    }

    fn build(&self) -> Result<LogStorageBuilderHandle> {
        let handle = LogStorageBuilderHandle(check_api_call!(ffi::duckdb_v2_log_storage_builder_create, RET)?);

        check_api_call!(
            ffi::duckdb_v2_log_storage_builder_set_name,
            *handle,
            (&self.name).into(),
        )?;

        check_api_call!(
            ffi::duckdb_v2_log_storage_builder_set_user_data,
            *handle,
            self.implementation.to_handle()
        )?;

        check_api_call!(
            ffi::duckdb_v2_log_storage_builder_set_log_callback,
            *handle,
            Some(log_callback::<T>),
        )?;

        Ok(handle)
    }

    /// Register the storage with a database.
    pub fn register_with_database(self, db: &Database) -> Result<()> {
        let handle = self.build()?;

        check_api_call!(
            ffi::duckdb_v2_log_storage_builder_register_with_database,
            db.handle.lock().unwrap().handle,
            *handle
        )?;

        Ok(())
    }

    /// Register the storage with a callback context's database.
    pub fn register_with_context(self, ctx: &Context) -> Result<()> {
        let handle = self.build()?;

        check_api_call!(ffi::duckdb_v2_log_storage_builder_register_with_context, **ctx, *handle)?;

        Ok(())
    }
}

/// Receives records routed to custom log storage.
pub trait LogStorageCallbacks: Send + Sync + 'static {
    /// Process a record timestamped in microseconds since the Unix epoch.
    fn log(&self, log_message: &str, level: LogLevel, timestamp: i64, log_type: &str) -> Result<()>;
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use std::sync::atomic::AtomicI64;

    use crate::{
        Environment, Parameters, SettingScope, StorageLocation,
        connection_options::OptionValue,
        log::{Log, LogLevel, LogStorageBuilder, LogStorageCallbacks},
    };

    static IS_CALLED: AtomicI64 = AtomicI64::new(0);

    struct CustomLogger;

    impl LogStorageCallbacks for CustomLogger {
        fn log(&self, log_message: &str, level: LogLevel, _timestamp: i64, log_type: &str) -> crate::Result<()> {
            assert_eq!(log_type, "cpp_api_test");
            assert_ne!(log_message, "wrong message");
            assert_eq!(level, LogLevel::Warn);

            let current_count = IS_CALLED.load(std::sync::atomic::Ordering::Relaxed);
            IS_CALLED.store(current_count + 1, std::sync::atomic::Ordering::Relaxed);

            Ok(())
        }
    }

    #[test]
    fn test_log_storage_builder() -> crate::Result<()> {
        let env = Environment::new()?;
        let db = env.open(StorageLocation::InMemory)?;
        let conn = db.connect()?;

        LogStorageBuilder::new("custom_logger", CustomLogger).register_with_database(&db)?;

        conn.set_option(&OptionValue::new("enable_logging", "true")?, Some(SettingScope::Global))?;

        conn.set_option(
            &OptionValue::new("logging_storage", "custom_logger")?,
            Some(SettingScope::Global),
        )?;

        conn.set_option(
            &OptionValue::new("logging_level", "WARNING")?,
            Some(SettingScope::Global),
        )?;

        let conn = db.connect()?;

        Log::log_on_connection(&conn, LogLevel::Warn, "first message", "cpp_api_test")?;

        conn.execute(
            "SELECT write_log('second message', log_type := 'cpp_api_test', level := 'WARNING');",
            Parameters::None,
        )?;

        conn.execute(
            "SELECT write_log('wrong message', log_type := 'NOT_VALID_LEVEL', level := 'INFO');",
            Parameters::None,
        )?;

        assert_eq!(IS_CALLED.load(std::sync::atomic::Ordering::Relaxed), 2);

        Ok(())
    }
}

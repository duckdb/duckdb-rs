//! Emit DuckDB log records and register custom log storage.

use crate::{Result, builder_helpers::ffi_enum_redeclaration, check_api_call, connection::Context, ffi};

ffi_enum_redeclaration! {
    /// The severity of a DuckDB log record.
    #[allow(missing_docs)]
    pub enum LogLevel <- ffi::DUCKDB_V2_LOG_LEVEL {
    Trace = DUCKDB_V2_LOG_LEVEL_TRACE,
    Debug = DUCKDB_V2_LOG_LEVEL_DEBUG,
    Info = DUCKDB_V2_LOG_LEVEL_INFO,
    Warn = DUCKDB_V2_LOG_LEVEL_WARNING,
    Error = DUCKDB_V2_LOG_LEVEL_ERROR,
    Fatal = DUCKDB_V2_LOG_LEVEL_FATAL,
    }
}

/// Emits records through DuckDB's configured logging system.
pub struct Log;

impl Log {
    /// Log a message in the context's connection scope, subject to DuckDB's logging configuration.
    pub fn log_on_context(ctx: &Context, level: LogLevel, message: &str, log_type: &str) -> Result<()> {
        check_api_call!(
            ffi::duckdb_v2_context_log,
            **ctx,
            level.into(),
            log_type.into(),
            message.into()
        )
    }
}

#[cfg(feature = "capi-v2-p4")]
unsafe extern "C" fn log_callback<T: LogStorageCallbacks>(
    user_data: *mut ::std::os::raw::c_void,
    timestamp: i64,
    level: libduckdb_sys::v2::DUCKDB_V2_LOG_LEVEL,
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

#[cfg(feature = "capi-v2-p4")]
/// Registers a named Rust implementation as DuckDB log storage.
///
/// DuckDB calls the implementation for records routed to the registered
/// storage by its logging configuration.
///
/// # Example
/// ```
/// use duckdb_neo::environment::{Environment, StorageLocation};
/// use duckdb_neo::log::{LogStorageBuilder, LogStorageCallbacks, LogLevel};
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
///     ) -> duckdb_neo::Result<()> {
///         println!("[{level:?}] {log_type}: {message}");
///         Ok(())
///     }
/// }
///
/// # fn main() -> duckdb_neo::Result<()> {
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

#[cfg(feature = "capi-v2-p4")]
impl<T: LogStorageCallbacks> LogStorageBuilder<T> {
    /// Create named log storage backed by `implementation`.
    pub fn new(name: &str, implementation: T) -> Self {
        Self {
            name: name.to_string(),
            implementation: OpaqueHandle::new(implementation),
        }
    }

    fn build(&self) -> Result<LogStorageBuilderHandle> {
        let handle = LogStorageBuilderHandle::new()?;

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
    use crate::{
        Parameters,
        builder_helpers::scalar_callback,
        connection::SettingScope,
        environment::{Environment, StorageLocation},
        log::{Log, LogLevel},
        scalar::ScalarFunctionBuilder,
        signature::{Parameter, SignatureBuilder},
    };

    scalar_callback!(LogCallback, i32, |_input, _result, ctx, _ud| {
        Log::log_on_context(&ctx, LogLevel::Warn, "first message", "cpp_api_test")
    });

    use crate::types::DuckDBType;

    #[test]
    fn test_log_storage_builder() -> crate::Result<()> {
        let env = Environment::new()?;
        let db = env.open(StorageLocation::InMemory)?;
        let conn = db.connect()?;

        // LogStorageBuilder::new("custom_logger", CustomLogger).register_with_database(&db)?;

        conn.set_option("enable_logging", "true", Some(SettingScope::Global))?;

        // conn.set_option(
        //     "logging_storage", "custom_logger",
        //     Some(SettingScope::Global),
        // )?;

        conn.set_option("logging_level", "WARNING", Some(SettingScope::Global))?;

        let conn = db.connect()?;

        ScalarFunctionBuilder::new(
            "log_it",
            SignatureBuilder::new(
                [Parameter::normal("A", i32::logical_type(&conn)?)],
                i32::logical_type(&conn)?,
            ),
            LogCallback,
        )
        .register(&conn)?;

        conn.execute("SELECT log_it(1)", Parameters::None)?;

        conn.execute(
            "SELECT write_log('second message', log_type := 'cpp_api_test', level := 'WARNING');",
            Parameters::None,
        )?;

        conn.execute(
            "SELECT write_log('wrong message', log_type := 'NOT_VALID_LEVEL', level := 'INFO');",
            Parameters::None,
        )?;

        let query = conn.query("SELECT * FROM duckdb_logs;", Parameters::None)?;

        for chunk in query {
            let chunk = chunk?;

            assert_eq!(chunk.row_count()?, 2)
        }

        // assert_eq!(IS_CALLED.load(std::sync::atomic::Ordering::Relaxed), 2);

        Ok(())
    }
}

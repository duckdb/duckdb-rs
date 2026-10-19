//! Emit DuckDB log records.

use crate::{
    Result,
    builder_helpers::ffi_enum_redeclaration,
    check_api_call,
    connection::{Connection, Context},
    ffi,
    instance::Instance,
    raw::RawExt,
};

ffi_enum_redeclaration! {
    /// The severity of a DuckDB log record.
    #[allow(missing_docs)]
    #[derive(PartialOrd, Ord)]
    pub enum LogLevel <- ffi::DUCKDB_V2_LOG_LEVEL {
    Trace = DUCKDB_V2_LOG_LEVEL_TRACE,
    Debug = DUCKDB_V2_LOG_LEVEL_DEBUG,
    Info = DUCKDB_V2_LOG_LEVEL_INFO,
    Warn = DUCKDB_V2_LOG_LEVEL_WARNING,
    Error = DUCKDB_V2_LOG_LEVEL_ERROR,
    Fatal = DUCKDB_V2_LOG_LEVEL_FATAL,
    }
}

ffi_enum_redeclaration! {
    #[derive(PartialOrd, Ord)]
    pub enum LogScope <- ffi::DUCKDB_V2_LOG_SCOPE {
        Database = DUCKDB_V2_LOG_SCOPE_DATABASE,
        Connection = DUCKDB_V2_LOG_SCOPE_CONNECTION,
        Thread = DUCKDB_V2_LOG_SCOPE_THREAD
    }
}

/// Emits records through DuckDB's configured logging system.
#[derive(Debug)]
pub struct Log;

impl Log {
    /// Logs in the context's connection scope.
    pub fn log_on_context(ctx: &Context, level: LogLevel, message: &str, log_type: &str) -> Result<()> {
        check_api_call!(
            ffi::duckdb_v2_context_log,
            ctx.raw(),
            level.into(),
            &log_type.into(),
            &message.into()
        )
    }

    /// Logs in connection scope.
    pub fn log_on_connection(conn: &Connection, level: LogLevel, message: &str, log_type: &str) -> Result<()> {
        check_api_call!(
            ffi::duckdb_v2_connection_log,
            conn.raw(),
            level.into(),
            &log_type.into(),
            &message.into()
        )
    }

    /// Logs in database scope (no connection or query ids).
    pub fn log_on_instance(instance: &Instance, level: LogLevel, message: &str, log_type: &str) -> Result<()> {
        check_api_call!(
            ffi::duckdb_v2_instance_log,
            instance.raw(),
            level.into(),
            &log_type.into(),
            &message.into()
        )
    }
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
        signature::{FunctionParameter, SignatureBuilder},
    };

    scalar_callback!(LogCallback, i32, |_input, _result, ctx, _ud| {
        Log::log_on_context(ctx, LogLevel::Warn, "first message", "cpp_api_test")
    });

    use crate::types::DuckDBType;

    #[test]
    fn test_instance_log_releases_handle_lock() -> crate::Result<()> {
        use crate::{
            instance::InstanceHandle,
            log_sink::{LogMetadata, LogSinkBuilder, LogSinkCallbacks, LogWriteData},
        };
        use std::sync::{
            Arc, Mutex, Weak,
            atomic::{AtomicBool, Ordering},
        };

        struct LockProbe(Weak<Mutex<InstanceHandle>>, Arc<AtomicBool>);
        impl LogSinkCallbacks for LockProbe {
            fn write(&self, data: LogWriteData<'_>, _: LogMetadata<'_>) -> crate::Result<()> {
                if data.log_type == "rs_lock_probe" {
                    let instance = self.0.upgrade().unwrap();
                    self.1.store(instance.try_lock().is_ok(), Ordering::SeqCst);
                }
                Ok(())
            }
        }

        let env = Environment::new()?;
        let instance = env.open(StorageLocation::InMemory)?;
        let conn = instance.connect()?;
        let unlocked = Arc::new(AtomicBool::new(false));
        LogSinkBuilder::new(
            "rs_lock_probe",
            LockProbe(Arc::downgrade(&instance.handle), unlocked.clone()),
        )
        .register(&instance)?;
        conn.execute("CALL enable_logging()", Parameters::None)?;
        conn.execute("SET logging_storage = 'rs_lock_probe'", Parameters::None)?;
        Log::log_on_instance(&instance, LogLevel::Info, "probe", "rs_lock_probe")?;
        assert!(unlocked.load(Ordering::SeqCst));
        Ok(())
    }

    #[test]
    fn test_log_on_context() -> crate::Result<()> {
        let env = Environment::new()?;
        let instance = env.open(StorageLocation::InMemory)?;
        let conn = instance.connect()?;

        conn.config()?
            .set_text("enable_logging", "true", SettingScope::Global)?;
        conn.config()?
            .set_text("logging_level", "WARNING", SettingScope::Global)?;

        let conn = instance.connect()?;

        ScalarFunctionBuilder::new(
            "log_it",
            SignatureBuilder::new()
                .param(FunctionParameter::new::<i32>("A"))
                .return_type(i32::logical_type(&conn)?),
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

        let mut query = conn.query("SELECT * FROM duckdb_logs;", Parameters::None)?;

        while let Some(chunk) = query.next_chunk()? {
            assert_eq!(chunk.row_count()?, 2)
        }

        Ok(())
    }
}

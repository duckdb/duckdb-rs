//! Emit DuckDB log records.

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
        Log::log_on_context(ctx, LogLevel::Warn, "first message", "cpp_api_test")
    });

    use crate::types::DuckDBType;

    #[test]
    fn test_log_on_context() -> crate::Result<()> {
        let env = Environment::new()?;
        let db = env.open(StorageLocation::InMemory)?;
        let mut conn = db.connect()?;

        conn.set_option("enable_logging", "true", Some(SettingScope::Global))?;
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

        Ok(())
    }
}

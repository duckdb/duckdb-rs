use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};

use crate::{
    Parameters,
    environment::{Environment, StorageLocation},
    log::Log,
};

use super::*;

const LOG_TYPE: &str = "RsLogSinkTest";

/// One record seen by [`GlobalSink`].
#[derive(Debug, Clone, PartialEq)]
struct Collected {
    message: Option<String>,
    level: LogLevel,
    scope: LogScope,
    has_connection_id: bool,
    timestamp: i64,
}

// Filled by the sink while DuckDB logs and read back by the test afterwards.
static COLLECTED: Mutex<Vec<Collected>> = Mutex::new(Vec::new());
static FLUSHES: AtomicUsize = AtomicUsize::new(0);

/// A sink without state of its own that records into [`COLLECTED`].
struct GlobalSink;

impl LogSinkCallbacks for GlobalSink {
    fn flush(&self) -> Result<()> {
        FLUSHES.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn write(&self, data: LogWriteData, metadata: LogMetadata<'_>) -> Result<()> {
        // DuckDB logs records of its own too.
        if data.log_type.as_deref() != Some(LOG_TYPE) {
            return Ok(());
        }
        COLLECTED.lock().unwrap().push(Collected {
            message: data.message,
            level: data.log_level,
            scope: data.log_scope,
            has_connection_id: metadata.connection_id()?.is_some(),
            timestamp: data.timestamp,
        });
        Ok(())
    }
}

#[test]
fn test_log_sink_writes_to_global() -> Result<()> {
    let env = Environment::new()?;
    let db = env.open(StorageLocation::InMemory)?;
    let conn = db.connect()?;

    LogSinkBuilder::new("rs_global_store", GlobalSink).register(&db)?;

    conn.execute("CALL enable_logging()", Parameters::None)?;
    // Sink names are case-insensitive.
    conn.execute("SET logging_storage = 'RS_GLOBAL_STORE'", Parameters::None)?;
    conn.execute(
        "SELECT write_log('first', level := 'warning', log_type := 'RsLogSinkTest')",
        Parameters::None,
    )?;
    Log::log_on_connection(&conn, LogLevel::Error, "second", LOG_TYPE)?;
    Log::log_on_instance(&db, LogLevel::Info, "third", LOG_TYPE)?;
    conn.execute("SET logging_storage = 'memory'", Parameters::None)?;

    let collected = COLLECTED.lock().unwrap().clone();
    let summary = collected
        .iter()
        .map(|c| {
            (
                c.message.as_deref(),
                c.level.clone(),
                c.scope.clone(),
                c.has_connection_id,
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        summary,
        [
            (Some("first"), LogLevel::Warn, LogScope::Connection, true),
            (Some("second"), LogLevel::Error, LogScope::Connection, true),
            // Instance records belong to no connection.
            (Some("third"), LogLevel::Info, LogScope::Database, false),
        ]
    );
    assert!(collected.iter().all(|c| c.timestamp > 0));
    // The end of every query flushes the log.
    assert!(FLUSHES.load(Ordering::SeqCst) > 0);

    Ok(())
}

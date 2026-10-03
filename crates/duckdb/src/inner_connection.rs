use std::{
    ffi::{CStr, CString},
    mem,
    os::raw::c_char,
    ptr, str,
    sync::{Arc, Mutex},
};

use super::{Appender, Config, Connection, Result, ffi};
use crate::{
    error::{
        Error, result_from_duckdb_appender, result_from_duckdb_extract, result_from_duckdb_prepare,
        result_from_duckdb_result,
    },
    raw_statement::RawStatement,
    statement::Statement,
};

#[derive(Debug)]
struct DatabaseHandle {
    db: ffi::duckdb_database,
    close_on_drop: bool,
}

// `duckdb_database` is an opaque C pointer. We share it via `Arc` so the database
// is closed exactly once when the last connection referencing it is dropped.
// This means the handle may be dropped on any thread that owns a `Connection`.
unsafe impl Send for DatabaseHandle {}

impl DatabaseHandle {
    #[inline]
    pub fn new(db: ffi::duckdb_database, close_on_drop: bool) -> Self {
        Self { db, close_on_drop }
    }

    #[inline]
    pub fn raw(&self) -> ffi::duckdb_database {
        self.db
    }
}

impl Drop for DatabaseHandle {
    fn drop(&mut self) {
        if !self.close_on_drop {
            return;
        }
        if self.db.is_null() {
            return;
        }
        unsafe {
            ffi::duckdb_close(&mut self.db);
            self.db = ptr::null_mut();
        }
    }
}

pub struct InnerConnection {
    // Mutex makes the Send-only handle `Sync` so the `Arc` can be shared across threads;
    // it is not used for concurrent access to the database itself.
    database: Arc<Mutex<DatabaseHandle>>,
    pub con: ffi::duckdb_connection,
    pub(crate) cell: Arc<ConnectionCell>,
    interrupt: Arc<InterruptHandle>,
}

impl InnerConnection {
    #[inline]
    unsafe fn new(database: Arc<Mutex<DatabaseHandle>>) -> Result<Self> {
        unsafe {
            let mut con: ffi::duckdb_connection = ptr::null_mut();
            let db_raw = database.lock().expect("database handle mutex poisoned").raw();
            let r = ffi::duckdb_connect(db_raw, &mut con);
            if r != ffi::DuckDBSuccess {
                ffi::duckdb_disconnect(&mut con);
                return Err(Error::DuckDBFailure(
                    ffi::Error::new(r),
                    Some("connect error".to_owned()),
                ));
            }
            let cell = Arc::new(ConnectionCell::new(con));
            let interrupt = Arc::new(InterruptHandle::new(cell.clone()));

            Ok(Self {
                database,
                con,
                cell,
                interrupt,
            })
        }
    }

    pub fn open_with_flags(c_path: &CStr, config: Config) -> Result<Self> {
        unsafe {
            let mut db: ffi::duckdb_database = ptr::null_mut();
            let mut c_err = std::ptr::null_mut();
            let r = ffi::duckdb_open_ext(c_path.as_ptr(), &mut db, config.duckdb_config(), &mut c_err);
            if r != ffi::DuckDBSuccess {
                let msg = ffi::DuckDbString::from_nullable_ptr(c_err).map(|c_err| c_err.to_string_lossy().to_string());
                return Err(Error::DuckDBFailure(ffi::Error::new(r), msg));
            }
            Self::new_from_raw_db(db, true)
        }
    }

    #[inline]
    pub(crate) unsafe fn new_from_raw_db(raw: ffi::duckdb_database, close_on_drop: bool) -> Result<Self> {
        unsafe { Self::new(Arc::new(Mutex::new(DatabaseHandle::new(raw, close_on_drop)))) }
    }

    pub fn close(&mut self) -> Result<()> {
        if self.con.is_null() {
            return Ok(());
        }
        // Disconnecting inside the cell's lock is what stops a handle on another
        // thread from passing its null check and then calling into a connection
        // this thread is freeing.
        let cell = self.cell.clone();
        let con = &mut self.con;
        cell.close(|| unsafe {
            ffi::duckdb_disconnect(con);
            *con = ptr::null_mut();
        });
        Ok(())
    }

    /// Creates a new connection to the already-opened database.
    pub fn try_clone(&self) -> Result<Self> {
        unsafe { Self::new(self.database.clone()) }
    }

    pub fn execute(&mut self, sql: &str) -> Result<()> {
        let c_str = CString::new(sql)?;
        unsafe {
            let mut out = mem::zeroed();
            let r = ffi::duckdb_query(self.con, c_str.as_ptr() as *const c_char, &mut out);
            result_from_duckdb_result(r, &mut out)?;
            ffi::duckdb_destroy_result(&mut out);
            Ok(())
        }
    }

    pub fn prepare<'a>(&mut self, conn: &'a Connection, sql: &str) -> Result<Statement<'a>> {
        let c_str = CString::new(sql)?;

        // Extract statements (handles both single and multi-statement queries)
        let mut extracted = ptr::null_mut();
        let num_stmts =
            unsafe { ffi::duckdb_extract_statements(self.con, c_str.as_ptr() as *const c_char, &mut extracted) };
        result_from_duckdb_extract(num_stmts, extracted)?;

        // Auto-cleanup on drop
        let _guard = ExtractedStatementsGuard(extracted);

        // Execute all intermediate statements
        for i in 0..num_stmts - 1 {
            self.execute_extracted_statement(extracted, i)?;
        }

        // Prepare and return final statement
        let final_stmt = self.prepare_extracted_statement(extracted, num_stmts - 1)?;
        Ok(Statement::new(conn, unsafe { RawStatement::new(final_stmt) }))
    }

    fn prepare_extracted_statement(
        &self,
        extracted: ffi::duckdb_extracted_statements,
        index: ffi::idx_t,
    ) -> Result<ffi::duckdb_prepared_statement> {
        let mut stmt = ptr::null_mut();
        let res = unsafe { ffi::duckdb_prepare_extracted_statement(self.con, extracted, index, &mut stmt) };
        result_from_duckdb_prepare(res, stmt)?;
        Ok(stmt)
    }

    fn execute_extracted_statement(
        &self,
        extracted: ffi::duckdb_extracted_statements,
        index: ffi::idx_t,
    ) -> Result<()> {
        let mut stmt = self.prepare_extracted_statement(extracted, index)?;

        let mut result = unsafe { mem::zeroed() };
        let rc = unsafe { ffi::duckdb_execute_prepared(stmt, &mut result) };

        let error = if rc != ffi::DuckDBSuccess {
            unsafe {
                let c_err = ffi::duckdb_result_error(&mut result as *mut _);
                let msg = if c_err.is_null() {
                    None
                } else {
                    Some(CStr::from_ptr(c_err).to_string_lossy().to_string())
                };
                Some(Error::DuckDBFailure(ffi::Error::new(rc), msg))
            }
        } else {
            None
        };

        unsafe {
            ffi::duckdb_destroy_prepare(&mut stmt);
            ffi::duckdb_destroy_result(&mut result);
        }

        error.map_or(Ok(()), Err)
    }

    pub fn appender<'a>(
        &mut self,
        conn: &'a Connection,
        catalog: Option<&str>,
        schema: &str,
        table: &str,
    ) -> Result<Appender<'a>> {
        let c_app = self.create_table_appender(catalog, schema, table)?;
        Ok(Appender::new(conn, c_app))
    }

    fn create_table_appender(
        &mut self,
        catalog: Option<&str>,
        schema: &str,
        table: &str,
    ) -> Result<ffi::duckdb_appender> {
        let mut c_app: ffi::duckdb_appender = ptr::null_mut();
        let c_catalog = catalog.map(CString::new).transpose()?;
        let c_table = CString::new(table)?;
        let c_schema = CString::new(schema)?;
        let r = unsafe {
            ffi::duckdb_appender_create_ext(
                self.con,
                c_catalog.as_ref().map_or(ptr::null(), |c| c.as_ptr() as *const c_char),
                c_schema.as_ptr() as *const c_char,
                c_table.as_ptr() as *const c_char,
                &mut c_app,
            )
        };
        result_from_duckdb_appender(r, &mut c_app)?;
        Ok(c_app)
    }

    pub fn appender_with_columns<'a>(
        &mut self,
        conn: &'a Connection,
        catalog: Option<&str>,
        schema: &str,
        table: &str,
        columns: &[&str],
    ) -> Result<Appender<'a>> {
        // The C API only supports narrowing columns after the appender is created.
        // Create the appender first, then activate the requested column subset.
        let c_app = self.create_table_appender(catalog, schema, table)?;
        let mut appender = Appender::new(conn, c_app);
        for column in columns {
            appender.add_column(column)?;
        }
        Ok(appender)
    }

    pub fn get_interrupt_handle(&self) -> Arc<InterruptHandle> {
        self.interrupt.clone()
    }

    #[inline]
    pub fn is_autocommit(&self) -> bool {
        true
    }
}

struct ExtractedStatementsGuard(ffi::duckdb_extracted_statements);

impl Drop for ExtractedStatementsGuard {
    fn drop(&mut self) {
        unsafe { ffi::duckdb_destroy_extracted(&mut self.0) }
    }
}

impl Drop for InnerConnection {
    #[inline]
    fn drop(&mut self) {
        use std::thread::panicking;
        if let Err(e) = self.close() {
            if panicking() {
                eprintln!("Error while closing DuckDB connection: {e:?}");
            } else {
                panic!("Error while closing DuckDB connection: {e:?}");
            }
        }
    }
}

/// The raw connection pointer, shared between an [`InnerConnection`] and every
/// handle handed out from it ([`InterruptHandle`], [`crate::progress::ProgressHandle`]).
///
/// The pointer is nulled on close, so a handle outliving its connection is a
/// no-op rather than a use-after-free; the mutex is what serializes those
/// handle calls against the close. It does *not* guard use of the connection
/// itself — DuckDB permits `duckdb_interrupt` and `duckdb_query_progress`
/// while a query is in flight on another thread.
pub(crate) struct ConnectionCell {
    conn: Mutex<ffi::duckdb_connection>,
}

unsafe impl Send for ConnectionCell {}
unsafe impl Sync for ConnectionCell {}

impl ConnectionCell {
    fn new(conn: ffi::duckdb_connection) -> Self {
        Self { conn: Mutex::new(conn) }
    }

    /// Run `f` against the raw connection, or return `None` if it is closed.
    ///
    /// The lock is held for the duration of `f`, so `f` must return promptly —
    /// an interrupt or a progress read, not a query — and must not re-enter
    /// this cell, which would deadlock on the non-reentrant mutex.
    pub(crate) fn with<T>(&self, f: impl FnOnce(ffi::duckdb_connection) -> T) -> Option<T> {
        let conn = self.lock();
        (!conn.is_null()).then(|| f(*conn))
    }

    /// Null the stored pointer, then run `on_close` still holding the lock.
    pub(crate) fn close(&self, on_close: impl FnOnce()) {
        let mut conn = self.lock();
        *conn = ptr::null_mut();
        on_close();
    }

    /// Poisoning is recovered from rather than propagated: the pointer is
    /// either the connection or null whatever happens, and closing runs from
    /// `Drop`, where a second panic would abort.
    fn lock(&self) -> std::sync::MutexGuard<'_, ffi::duckdb_connection> {
        self.conn.lock().unwrap_or_else(|e| e.into_inner())
    }
}

/// A handle that allows interrupting long-running queries.
pub struct InterruptHandle {
    cell: Arc<ConnectionCell>,
}

impl InterruptHandle {
    fn new(cell: Arc<ConnectionCell>) -> Self {
        Self { cell }
    }

    /// Interrupt the query currently running on the connection this handle was
    /// obtained from. The interrupt will cause that query to fail with
    /// `Error::DuckDBFailure`. If the connection was dropped after obtaining
    /// this interrupt handle, calling this method results in a noop.
    ///
    /// See [`crate::Connection::interrupt_handle`] for an example.
    pub fn interrupt(&self) {
        self.cell.with(|conn| unsafe { ffi::duckdb_interrupt(conn) });
    }
}

use std::{
    ffi::{c_void, CStr, CString},
    mem,
    os::raw::c_char,
    ptr, str,
    sync::{Arc, Mutex},
};

use arrow::{
    datatypes::{Schema, SchemaRef},
    ffi::FFI_ArrowSchema,
};

use super::{ffi, Appender, Config, Connection, Result};
use crate::{
    error::{
        result_from_duckdb_appender, result_from_duckdb_arrow, result_from_duckdb_extract, result_from_duckdb_prepare,
        Error,
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
    interrupt: Arc<InterruptHandle>,
}

impl InnerConnection {
    #[inline]
    unsafe fn new(database: Arc<Mutex<DatabaseHandle>>) -> Result<Self> {
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
        let interrupt = Arc::new(InterruptHandle::new(con));

        Ok(Self {
            database,
            con,
            interrupt,
        })
    }

    pub fn open_with_flags(c_path: &CStr, config: Config) -> Result<Self> {
        unsafe {
            let mut db: ffi::duckdb_database = ptr::null_mut();
            let mut c_err = std::ptr::null_mut();
            let r = ffi::duckdb_open_ext(c_path.as_ptr(), &mut db, config.duckdb_config(), &mut c_err);
            if r != ffi::DuckDBSuccess {
                let msg = Some(CStr::from_ptr(c_err).to_string_lossy().to_string());
                ffi::duckdb_free(c_err as *mut c_void);
                return Err(Error::DuckDBFailure(ffi::Error::new(r), msg));
            }
            Self::new_from_raw_db(db, true)
        }
    }

    #[inline]
    pub(crate) unsafe fn new_from_raw_db(raw: ffi::duckdb_database, close_on_drop: bool) -> Result<Self> {
        Self::new(Arc::new(Mutex::new(DatabaseHandle::new(raw, close_on_drop))))
    }

    pub fn close(&mut self) -> Result<()> {
        if self.con.is_null() {
            return Ok(());
        }
        unsafe {
            ffi::duckdb_disconnect(&mut self.con);
            self.con = ptr::null_mut();
            self.interrupt.clear();
        }
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
            let r = ffi::duckdb_query_arrow(self.con, c_str.as_ptr() as *const c_char, &mut out);
            result_from_duckdb_arrow(r, out)?;
            ffi::duckdb_destroy_arrow(&mut out);
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

    pub fn appender<'a>(&mut self, conn: &'a Connection, table: &str, schema: &str) -> Result<Appender<'a>> {
        let mut c_app: ffi::duckdb_appender = ptr::null_mut();
        let c_table = CString::new(table)?;
        let c_schema = CString::new(schema)?;
        let r = unsafe {
            ffi::duckdb_appender_create(
                self.con,
                c_schema.as_ptr() as *const c_char,
                c_table.as_ptr() as *const c_char,
                &mut c_app,
            )
        };
        result_from_duckdb_appender(r, &mut c_app)?;
        Ok(Appender::new(conn, c_app))
    }

    pub fn appender_to_catalog_and_db<'a>(
        &mut self,
        conn: &'a Connection,
        table: &str,
        catalog: &str,
        schema: &str,
    ) -> Result<Appender<'a>> {
        let mut c_app: ffi::duckdb_appender = ptr::null_mut();
        let c_table = CString::new(table)?;
        let c_catalog = CString::new(catalog)?;
        let c_schema = CString::new(schema)?;

        let r = unsafe {
            ffi::duckdb_appender_create_ext(
                self.con,
                c_catalog.as_ptr() as *const c_char,
                c_schema.as_ptr() as *const c_char,
                c_table.as_ptr() as *const c_char,
                &mut c_app,
            )
        };
        result_from_duckdb_appender(r, &mut c_app)?;
        Ok(Appender::new(conn, c_app))
    }

    pub fn appender_with_columns<'a>(
        &mut self,
        conn: &'a Connection,
        table: &str,
        schema: &str,
        catalog: Option<&str>,
        columns: &[&str],
    ) -> Result<Appender<'a>> {
        // The C API only supports narrowing columns after the appender is created.
        // Create the appender first, then activate the requested column subset.
        let mut appender = match catalog {
            Some(catalog) => self.appender_to_catalog_and_db(conn, table, catalog, schema)?,
            None => self.appender(conn, table, schema)?,
        };
        for column in columns {
            appender.add_column(column)?;
        }
        Ok(appender)
    }

    pub fn get_interrupt_handle(&self) -> Arc<InterruptHandle> {
        self.interrupt.clone()
    }

    /// Extracts the Arrow schema from a prepared statement without executing it.
    pub fn prepared_schema(&self, stmt: ffi::duckdb_prepared_statement) -> Result<Option<SchemaRef>> {
        let ncols = unsafe { ffi::duckdb_prepared_statement_column_count(stmt) as usize };
        if ncols == 0 {
            return Ok(None);
        }

        let mut names: Vec<CString> = Vec::with_capacity(ncols);
        let mut logical_types = LogicalTypesGuard::with_capacity(ncols);

        for i in 0..ncols {
            let name_ptr = unsafe { ffi::duckdb_prepared_statement_column_name(stmt, i as ffi::idx_t) };
            if name_ptr.is_null() {
                return Err(Error::DuckDBFailure(
                    ffi::Error::new(ffi::DuckDBError),
                    Some(format!("Failed to get column name for column {i}")),
                ));
            }
            let name = unsafe { CStr::from_ptr(name_ptr).to_owned() };
            unsafe { ffi::duckdb_free(name_ptr as *mut c_void) };
            names.push(name);

            let logical_type = unsafe { ffi::duckdb_prepared_statement_column_logical_type(stmt, i as ffi::idx_t) };
            if logical_type.is_null() {
                return Err(Error::DuckDBFailure(
                    ffi::Error::new(ffi::DuckDBError),
                    Some(format!("Failed to get logical type for column {i}")),
                ));
            }
            logical_types.push(logical_type);
        }

        let mut arrow_options = ArrowOptionsGuard::from_connection(self.con)?;
        let name_ptrs: Vec<*const i8> = names.iter().map(|n| n.as_ptr()).collect();
        let mut arrow_schema = FFI_ArrowSchema::empty();

        let error_data = unsafe {
            ffi::duckdb_to_arrow_schema(
                arrow_options.as_mut_ptr(),
                logical_types.as_mut_ptr(),
                name_ptrs.as_ptr() as *mut *const i8,
                ncols as ffi::idx_t,
                &mut arrow_schema as *mut FFI_ArrowSchema as *mut ffi::ArrowSchema,
            )
        };

        if !error_data.is_null() {
            let error_msg = unsafe {
                CStr::from_ptr(ffi::duckdb_error_data_message(error_data))
                    .to_string_lossy()
                    .into_owned()
            };
            unsafe {
                let mut error_data = error_data;
                ffi::duckdb_destroy_error_data(&mut error_data);
            }
            return Err(Error::DuckDBFailure(
                ffi::Error::new(ffi::DuckDBError),
                Some(format!("Failed to convert to Arrow schema: {error_msg}")),
            ));
        }

        let schema = Schema::try_from(&arrow_schema).map_err(|e| {
            Error::DuckDBFailure(
                ffi::Error::new(ffi::DuckDBError),
                Some(format!("Failed to convert FFI Arrow schema: {e}")),
            )
        })?;

        Ok(Some(Arc::new(schema)))
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

/// RAII guard for a collection of DuckDB logical types.
struct LogicalTypesGuard(Vec<ffi::duckdb_logical_type>);

impl LogicalTypesGuard {
    fn with_capacity(cap: usize) -> Self {
        Self(Vec::with_capacity(cap))
    }

    fn push(&mut self, lt: ffi::duckdb_logical_type) {
        self.0.push(lt);
    }

    fn as_mut_ptr(&mut self) -> *mut ffi::duckdb_logical_type {
        self.0.as_mut_ptr()
    }
}

impl Drop for LogicalTypesGuard {
    fn drop(&mut self) {
        for lt in &mut self.0 {
            unsafe { ffi::duckdb_destroy_logical_type(lt) }
        }
    }
}

/// RAII guard for DuckDB arrow options.
struct ArrowOptionsGuard(ffi::duckdb_arrow_options);

impl ArrowOptionsGuard {
    fn from_connection(con: ffi::duckdb_connection) -> Result<Self> {
        let mut opts: ffi::duckdb_arrow_options = ptr::null_mut();
        unsafe { ffi::duckdb_connection_get_arrow_options(con, &mut opts) };
        if opts.is_null() {
            return Err(Error::DuckDBFailure(
                ffi::Error::new(ffi::DuckDBError),
                Some("Failed to get arrow options from connection".to_string()),
            ));
        }
        Ok(Self(opts))
    }

    fn as_mut_ptr(&mut self) -> ffi::duckdb_arrow_options {
        self.0
    }
}

impl Drop for ArrowOptionsGuard {
    fn drop(&mut self) {
        unsafe { ffi::duckdb_destroy_arrow_options(&mut self.0) }
    }
}

impl Drop for InnerConnection {
    #[allow(unused_must_use)]
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

/// A handle that allows interrupting long-running queries.
pub struct InterruptHandle {
    conn: Mutex<ffi::duckdb_connection>,
}

unsafe impl Send for InterruptHandle {}
unsafe impl Sync for InterruptHandle {}

impl InterruptHandle {
    fn new(conn: ffi::duckdb_connection) -> Self {
        Self { conn: Mutex::new(conn) }
    }

    fn clear(&self) {
        *(self.conn.lock().unwrap()) = ptr::null_mut();
    }

    /// Interrupt the query currently running on the connection this handle was
    /// obtained from. The interrupt will cause that query to fail with
    /// `Error::DuckDBFailure`. If the connection was dropped after obtaining
    /// this interrupt handle, calling this method results in a noop.
    ///
    /// See [`crate::Connection::interrupt_handle`] for an example.
    pub fn interrupt(&self) {
        let db_handle = self.conn.lock().unwrap();

        if !db_handle.is_null() {
            unsafe {
                ffi::duckdb_interrupt(*db_handle);
            }
        }
    }
}

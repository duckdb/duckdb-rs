use std::{
    ffi::{c_void, CStr, CString},
    mem,
    os::raw::c_char,
    ptr, str,
};

use super::{ffi, Appender, Config, Connection, Result};
use crate::{
    error::{result_from_duckdb_appender, result_from_duckdb_arrow, result_from_duckdb_prepare, Error},
    raw_statement::RawStatement,
    statement::Statement,
};

pub struct InnerConnection {
    pub db: ffi::duckdb_database,
    pub con: ffi::duckdb_connection,
    owned: bool,
}

impl InnerConnection {
    #[inline]
    pub unsafe fn new(db: ffi::duckdb_database, owned: bool) -> Result<InnerConnection> {
        let mut con: ffi::duckdb_connection = ptr::null_mut();
        let r = ffi::duckdb_connect(db, &mut con);
        if r != ffi::DuckDBSuccess {
            ffi::duckdb_disconnect(&mut con);
            return Err(Error::DuckDBFailure(
                ffi::Error::new(r),
                Some("connect error".to_owned()),
            ));
        }
        Ok(InnerConnection { db, con, owned })
    }

    pub fn open_with_flags(c_path: &CStr, config: Config) -> Result<InnerConnection> {
        unsafe {
            let mut db: ffi::duckdb_database = ptr::null_mut();
            let mut c_err = std::ptr::null_mut();
            let r = ffi::duckdb_open_ext(c_path.as_ptr(), &mut db, config.duckdb_config(), &mut c_err);
            if r != ffi::DuckDBSuccess {
                let msg = Some(CStr::from_ptr(c_err).to_string_lossy().to_string());
                ffi::duckdb_free(c_err as *mut c_void);
                return Err(Error::DuckDBFailure(ffi::Error::new(r), msg));
            }
            InnerConnection::new(db, true)
        }
    }

    pub fn close(&mut self) -> Result<()> {
        if self.db.is_null() {
            return Ok(());
        }
        if self.con.is_null() {
            return Ok(());
        }
        unsafe {
            ffi::duckdb_disconnect(&mut self.con);
            self.con = ptr::null_mut();

            if self.owned {
                ffi::duckdb_close(&mut self.db);
                self.db = ptr::null_mut();
            }
        }
        Ok(())
    }

    /// Creates a new connection to the already-opened database.
    pub fn try_clone(&self) -> Result<Self> {
        unsafe { InnerConnection::new(self.db, false) }
    }

    pub fn execute(&mut self, sql: &str) -> Result<()> {
        let c_str = CString::new(sql).unwrap();
        unsafe {
            let mut out = mem::zeroed();
            let r = ffi::duckdb_query_arrow(self.con, c_str.as_ptr() as *const c_char, &mut out);
            result_from_duckdb_arrow(r, out)?;
            ffi::duckdb_destroy_arrow(&mut out);
            Ok(())
        }
    }

    pub fn prepare<'a>(&mut self, conn: &'a Connection, sql: &str) -> Result<Statement<'a>> {
        let mut c_stmt: ffi::duckdb_prepared_statement = ptr::null_mut();
        let c_str = CString::new(sql).unwrap();
        let r = unsafe { ffi::duckdb_prepare(self.con, c_str.as_ptr() as *const c_char, &mut c_stmt) };
        result_from_duckdb_prepare(r, c_stmt)?;
        Ok(Statement::new(conn, unsafe { RawStatement::new(c_stmt) }))
    }

    pub fn appender<'a>(&mut self, conn: &'a Connection, table: &str, schema: &str) -> Result<Appender<'a>> {
        let mut c_app: ffi::duckdb_appender = ptr::null_mut();
        let c_table = CString::new(table).unwrap();
        let c_schema = CString::new(schema).unwrap();
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

    #[inline]
    pub fn is_autocommit(&self) -> bool {
        true
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

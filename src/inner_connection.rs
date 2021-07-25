use std::ffi::{CStr, CString};
use std::mem;
use std::os::raw::c_char;
use std::ptr;
use std::str;

use super::ffi;
use super::{Connection, OpenFlags, Result};
use crate::error::{result_from_duckdb_arrow, result_from_duckdb_prepare, Error};
use crate::raw_statement::RawStatement;
use crate::statement::Statement;

pub struct InnerConnection {
    pub db: ffi::duckdb_database,
    pub con: ffi::duckdb_connection,
    // pub result: ffi::duckdb_result,
    owned: bool,
}

impl Clone for InnerConnection {
    fn clone(&self) -> Self {
        unsafe { InnerConnection::new(self.db, false) }
    }
}

impl InnerConnection {
    #[allow(clippy::mutex_atomic)]
    #[inline]
    pub unsafe fn new(db: ffi::duckdb_database, owned: bool) -> InnerConnection {
        let mut con: ffi::duckdb_connection = ptr::null_mut();
        let r = ffi::duckdb_connect(db, &mut con);
        if r != ffi::DuckDBSuccess {
            ffi::duckdb_disconnect(&mut con);
            let e = Error::DuckDBFailure(ffi::Error::new(r), Some("connect error".to_owned()));
            // TODO: fix this
            panic!("error {:?}", e);
        }
        InnerConnection {
            db,
            con,
            // result: mem::zeroed(),
            owned,
        }
    }

    pub fn open_with_flags(c_path: &CStr, _: OpenFlags, _: Option<&CStr>) -> Result<InnerConnection> {
        unsafe {
            let mut db: ffi::duckdb_database = ptr::null_mut();
            let r = ffi::duckdb_open(c_path.as_ptr(), &mut db);
            if r != ffi::DuckDBSuccess {
                ffi::duckdb_close(&mut db);
                let e = Error::DuckDBFailure(
                    ffi::Error::new(r),
                    Some(format!("{}: {}", "duckdb_open error", c_path.to_string_lossy())),
                );
                return Err(e);
            }
            Ok(InnerConnection::new(db, true))
        }
    }

    #[allow(clippy::mutex_atomic)]
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

    #[inline]
    #[allow(dead_code)]
    pub fn changes(&mut self) -> usize {
        panic!("changes: not supported")
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
                eprintln!("Error while closing DuckDB connection: {:?}", e);
            } else {
                panic!("Error while closing DuckDB connection: {:?}", e);
            }
        }
    }
}

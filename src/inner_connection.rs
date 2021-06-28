use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int, c_uint};
use std::ptr;
// use std::mem;
use std::str;

use super::ffi;
use super::{Connection, OpenFlags, Result};
use crate::error::{error_from_handle, Error};
use crate::raw_statement::RawStatement;
use crate::statement::Statement;

pub struct InnerConnection {
    pub db: ffi::duckdb_database,
    pub con: ffi::duckdb_connection,
    // pub result: ffi::duckdb_result,
    owned: bool,
}

impl InnerConnection {
    #[allow(clippy::mutex_atomic)]
    #[inline]
    pub unsafe fn new(db: ffi::duckdb_database, owned: bool) -> InnerConnection {
        let mut con: ffi::duckdb_connection = ptr::null_mut();
        let r = ffi::duckdb_connect(db, &mut con);
        if r != ffi::DuckDBSuccess {
            ffi::duckdb_disconnect(&mut con);
            let e = Error::SqliteFailure(
                ffi::Error::new(r),
                Some("connect error".to_owned()),
            );
            // TODO: fix this
            panic!("error {:?}", e);
        }
        InnerConnection {
            db: db,
            con: con,
            // result: mem::zeroed(),
            //interrupt_lock: Arc::new(Mutex::new(&mut con)),
            owned: owned,
        }
    }

    pub fn open_with_flags(
        c_path: &CStr,
        _: OpenFlags,
        _: Option<&CStr>,
    ) -> Result<InnerConnection> {
        unsafe {
            let mut db: ffi::duckdb_database = ptr::null_mut();
            let r = if c_path.to_str().unwrap() == ":memory:" {
                ffi::duckdb_open(ptr::null_mut(), &mut db)
            } else {
                ffi::duckdb_open(c_path.as_ptr(), &mut db)
            };
            if r != ffi::DuckDBSuccess {
                ffi::duckdb_close(&mut db);
                let e = Error::SqliteFailure(
                    ffi::Error::new(r),
                    Some(format!("{}: {}", "duckdb_open error", c_path.to_string_lossy())),
                );
                return Err(e);
            }
            let mut con: ffi::duckdb_connection = ptr::null_mut();
            let r = ffi::duckdb_connect(db, &mut con);
            if r != ffi::DuckDBSuccess {
                ffi::duckdb_disconnect(&mut con);
                ffi::duckdb_close(&mut db);
                let e = Error::SqliteFailure(
                    ffi::Error::new(r),
                    Some("connect error".to_owned()),
                );
                // TODO: fix this
                panic!("error {:?}", e);
            }
            Ok(InnerConnection {
                db: db,
                con: con,
                // result: mem::zeroed(),
                owned: true,
            })
            // Ok(InnerConnection::new(&mut db, true))
        }
    }

    #[inline]
    pub fn db(&self) -> ffi::duckdb_database {
        self.db
    }

    #[inline]
    pub fn decode_result(&mut self, code: c_int) -> Result<()> {
        unsafe { InnerConnection::decode_result_raw(&mut self.db, code as c_uint) }
    }

    #[inline]
    unsafe fn decode_result_raw(db: *mut ffi::duckdb_connection, code: c_uint) -> Result<()> {
        if code == ffi::DuckDBSuccess {
            Ok(())
        } else {
            Err(error_from_handle(db, code as c_int))
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
//        let mut shared_handle = self.interrupt_lock.lock().unwrap();
//        assert!(
//            !shared_handle.is_null(),
//            "Bug: Somehow interrupt_lock was cleared before the DB was closed"
//        );
        unsafe {
            ffi::duckdb_disconnect(&mut self.con);
            // Need to use _raw because _guard has a reference out, and
            // decode_result takes &mut self.
 //           *shared_handle = ptr::null_mut();
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
        let r = unsafe {
            ffi::duckdb_query(
                self.con,
                c_str.as_ptr() as *const c_char,
                // &mut self.result,
                ptr::null_mut(),
            )
        };
        self.decode_result(r as c_int)
    }

    pub fn prepare<'a>(&mut self, conn: &'a Connection, sql: &str) -> Result<Statement<'a>> {
        let mut c_stmt: ffi::duckdb_prepared_statement = ptr::null_mut();
        let c_str = CString::new(sql).unwrap();
        let r = unsafe {
            ffi::duckdb_prepare(
                self.con,
                c_str.as_ptr() as *const c_char,
                &mut c_stmt,
            )
        };
        self.decode_result(r as c_int)?;
        Ok(Statement::new(conn, unsafe {
            RawStatement::new(c_stmt)
        }))
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

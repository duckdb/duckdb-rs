use super::ffi;
use super::{AppenderParams, Connection, Result, ValueRef};
use std::ffi::c_void;
use std::fmt;
use std::iter::IntoIterator;
use std::os::raw::c_char;

use crate::error::result_from_duckdb_code;
use crate::types::{ToSql, ToSqlOutput};

/// Appender for fast import data
pub struct Appender<'conn> {
    conn: &'conn Connection,
    app: ffi::duckdb_appender,
}

impl Appender<'_> {
    /// Append multiple rows from Iterator
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// # use duckdb::{Connection, Result, params};
    /// fn insert_rows(conn: &Connection) -> Result<()> {
    ///     let mut app = conn.appender("foo")?;
    ///     app.append_rows([[1, 2], [3, 4], [5, 6], [7, 8], [9, 10]])?;
    ///     Ok(())
    /// }
    /// ```
    ///
    /// # Failure
    ///
    /// Will return `Err` if append column count not the same with the table schema
    #[inline]
    pub fn append_rows<P, I>(&mut self, rows: I) -> Result<()>
    where
        I: IntoIterator<Item = P>,
        P: AppenderParams,
    {
        for row in rows {
            self.append_row(row)?;
        }
        Ok(())
    }

    /// Append one row
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// # use duckdb::{Connection, Result, params};
    /// fn insert_row(conn: &Connection) -> Result<()> {
    ///     let mut app = conn.appender("foo")?;
    ///     app.append_row([1, 2])?;
    ///     Ok(())
    /// }
    /// ```
    ///
    /// # Failure
    ///
    /// Will return `Err` if append column count not the same with the table schema
    #[inline]
    pub fn append_row<P: AppenderParams>(&mut self, params: P) -> Result<()> {
        let rc = unsafe { ffi::duckdb_appender_begin_row(self.app) };
        result_from_duckdb_code(rc, None)?;
        params.__bind_in(self)?;
        let rc = unsafe { ffi::duckdb_appender_end_row(self.app) };
        result_from_duckdb_code(rc, None)
    }

    #[inline]
    pub(crate) fn bind_parameters<P>(&mut self, params: P) -> Result<()>
    where
        P: IntoIterator,
        P::Item: ToSql,
    {
        for p in params.into_iter() {
            self.bind_parameter(&p)?;
        }
        Ok(())
    }

    fn bind_parameter<P: ?Sized + ToSql>(&self, param: &P) -> Result<()> {
        let value = param.to_sql()?;

        let ptr = self.app;
        let value = match value {
            ToSqlOutput::Borrowed(v) => v,
            ToSqlOutput::Owned(ref v) => ValueRef::from(v),
        };
        // TODO: append more
        let rc = match value {
            ValueRef::Null => unsafe { ffi::duckdb_append_null(ptr) },
            ValueRef::Boolean(i) => unsafe { ffi::duckdb_append_bool(ptr, i) },
            ValueRef::TinyInt(i) => unsafe { ffi::duckdb_append_int8(ptr, i) },
            ValueRef::SmallInt(i) => unsafe { ffi::duckdb_append_int16(ptr, i) },
            ValueRef::Int(i) => unsafe { ffi::duckdb_append_int32(ptr, i) },
            ValueRef::BigInt(i) => unsafe { ffi::duckdb_append_int64(ptr, i) },

            // FIXME
            ValueRef::HugeInt(i) => unsafe { ffi::duckdb_append_int64(ptr, i as i64) },

            ValueRef::Float(r) => unsafe { ffi::duckdb_append_float(ptr, r) },
            ValueRef::Double(r) => unsafe { ffi::duckdb_append_double(ptr, r) },
            ValueRef::Text(s) => unsafe {
                ffi::duckdb_append_varchar_length(ptr, s.as_ptr() as *const c_char, s.len() as u64)
            },
            ValueRef::Blob(b) => unsafe { ffi::duckdb_append_blob(ptr, b.as_ptr() as *const c_void, b.len() as u64) },
            _ => unreachable!("not supported"),
        };
        result_from_duckdb_code(rc, Some("append error".to_owned()))?;
        Ok(())
    }

    #[inline]
    pub(super) fn new(conn: &Connection, app: ffi::duckdb_appender) -> Appender<'_> {
        Appender { conn, app }
    }

    /// Flush data into DB
    #[inline]
    pub fn flush(&mut self) {
        unsafe {
            ffi::duckdb_appender_flush(self.app);
        }
    }
}

impl Drop for Appender<'_> {
    fn drop(&mut self) {
        self.flush();
        unsafe {
            ffi::duckdb_appender_close(self.app);
            ffi::duckdb_appender_destroy(&mut self.app);
        }
    }
}

impl fmt::Debug for Appender<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Appender").field("conn", self.conn).finish()
    }
}

#[cfg(test)]
mod test {
    use crate::{Connection, Result};
    use std::convert::TryFrom;

    #[test]
    fn test_append_one_row() -> Result<()> {
        let db = Connection::open_in_memory()?;
        db.execute_batch("CREATE TABLE foo(x INTEGER)")?;

        {
            let mut app = db.appender("foo")?;
            app.append_row([42])?;
        }

        let val = db.query_row("SELECT x FROM foo", [], |row| <(i32,)>::try_from(row))?;
        assert_eq!(val, (42,));
        Ok(())
    }

    #[test]
    fn test_append_rows() -> Result<()> {
        let db = Connection::open_in_memory()?;
        db.execute_batch("CREATE TABLE foo(x INTEGER, y INTEGER)")?;

        {
            let mut app = db.appender("foo")?;
            app.append_rows([[1, 2], [3, 4], [5, 6], [7, 8], [9, 10]])?;
        }

        let val = db.query_row("SELECT sum(x), sum(y) FROM foo", [], |row| <(i32, i32)>::try_from(row))?;
        assert_eq!(val, (25, 30));
        Ok(())
    }
}

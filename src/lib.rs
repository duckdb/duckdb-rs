//! duckdb-rs is an ergonomic wrapper for using DuckDB from Rust. It attempts to
//! expose an interface similar to [rusqlite](https://github.com/rusqlite/rusqlite).
//!
//! ```rust
//! use duckdb::{params, Connection, Result};
//! use duckdb::arrow::record_batch::RecordBatch;
//! use duckdb::arrow::util::pretty::print_batches;
//!
//! #[derive(Debug)]
//! struct Person {
//!     id: i32,
//!     name: String,
//!     data: Option<Vec<u8>>,
//! }
//!
//! fn main() -> Result<()> {
//!     let conn = Connection::open_in_memory()?;
//!
//!     conn.execute_batch(
//!         r"CREATE SEQUENCE seq;
//!           CREATE TABLE person (
//!                   id              INTEGER PRIMARY KEY DEFAULT NEXTVAL('seq'),
//!                   name            TEXT NOT NULL,
//!                   data            BLOB
//!                   );
//!          ")?;
//!     let me = Person {
//!         id: 0,
//!         name: "Steven".to_string(),
//!         data: None,
//!     };
//!     conn.execute(
//!         "INSERT INTO person (name, data) VALUES (?, ?)",
//!         params![me.name, me.data],
//!     )?;
//!
//!     let mut stmt = conn.prepare("SELECT id, name, data FROM person")?;
//!     let person_iter = stmt.query_map([], |row| {
//!         Ok(Person {
//!             id: row.get(0)?,
//!             name: row.get(1)?,
//!             data: row.get(2)?,
//!         })
//!     })?;
//!
//!     for person in person_iter {
//!         println!("Found person {:?}", person.unwrap());
//!     }
//!
//!     // query table by arrow
//!     let rbs: Vec<RecordBatch> = stmt.query_arrow([])?.collect();
//!     print_batches(&rbs);
//!     Ok(())
//! }
//! ```
#![warn(missing_docs)]

pub use libduckdb_sys as ffi;

use std::cell::RefCell;
use std::convert;
use std::default::Default;
use std::ffi::CString;
use std::fmt;
use std::path::{Path, PathBuf};
use std::result;
use std::str;

use crate::cache::StatementCache;
use crate::inner_connection::InnerConnection;
use crate::raw_statement::RawStatement;
use crate::types::ValueRef;

pub use crate::appender::Appender;
pub use crate::appender_params::{appender_params_from_iter, AppenderParams, AppenderParamsFromIter};
pub use crate::arrow_batch::Arrow;
pub use crate::cache::CachedStatement;
pub use crate::column::Column;
pub use crate::config::{AccessMode, Config, DefaultNullOrder, DefaultOrder};
pub use crate::error::Error;
pub use crate::ffi::ErrorCode;
pub use crate::params::{params_from_iter, Params, ParamsFromIter};
#[cfg(feature = "r2d2")]
pub use crate::r2d2::DuckdbConnectionManager;
pub use crate::row::{AndThenRows, Map, MappedRows, Row, RowIndex, Rows};
pub use crate::statement::Statement;
pub use crate::transaction::{DropBehavior, Savepoint, Transaction, TransactionBehavior};
pub use crate::types::ToSql;

// re-export dependencies from arrow-rs to minimise version maintenance for crate users
pub use arrow;

#[macro_use]
mod error;
mod appender;
mod appender_params;
mod arrow_batch;
mod cache;
mod column;
mod config;
mod inner_connection;
mod params;
mod pragma;
#[cfg(feature = "r2d2")]
mod r2d2;
mod raw_statement;
mod row;
mod statement;
mod transaction;

#[cfg(feature = "extensions-full")]
mod extension;

pub mod types;

pub(crate) mod util;

// Number of cached prepared statements we'll hold on to.
const STATEMENT_CACHE_DEFAULT_CAPACITY: usize = 16;

/// A macro making it more convenient to pass heterogeneous or long lists of
/// parameters as a `&[&dyn ToSql]`.
///
/// # Example
///
/// ```rust,no_run
/// # use duckdb::{Result, Connection, params};
///
/// struct Person {
///     name: String,
///     age_in_years: u8,
///     data: Option<Vec<u8>>,
/// }
///
/// fn add_person(conn: &Connection, person: &Person) -> Result<()> {
///     conn.execute("INSERT INTO person (name, age_in_years, data)
///                   VALUES (?1, ?2, ?3)",
///                  params![person.name, person.age_in_years, person.data])?;
///     Ok(())
/// }
/// ```
#[macro_export]
macro_rules! params {
    () => {
        &[] as &[&dyn $crate::ToSql]
    };
    ($($param:expr),+ $(,)?) => {
        &[$(&$param as &dyn $crate::ToSql),+] as &[&dyn $crate::ToSql]
    };
}

/// A typedef of the result returned by many methods.
pub type Result<T, E = Error> = result::Result<T, E>;

/// See the [method documentation](#tymethod.optional).
pub trait OptionalExt<T> {
    /// Converts a `Result<T>` into a `Result<Option<T>>`.
    ///
    /// By default, duckdb-rs treats 0 rows being returned from a query that is
    /// expected to return 1 row as an error. This method will
    /// handle that error, and give you back an `Option<T>` instead.
    fn optional(self) -> Result<Option<T>>;
}

impl<T> OptionalExt<T> for Result<T> {
    fn optional(self) -> Result<Option<T>> {
        match self {
            Ok(value) => Ok(Some(value)),
            Err(Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e),
        }
    }
}

/// Name for a database within a DuckDB connection.
#[derive(Copy, Clone, Debug)]
pub enum DatabaseName<'a> {
    /// The main database.
    Main,

    /// The temporary database (e.g., any "CREATE TEMPORARY TABLE" tables).
    Temp,

    /// A database that has been attached via "ATTACH DATABASE ...".
    Attached(&'a str),
}

impl<'a> fmt::Display for DatabaseName<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            DatabaseName::Main => write!(f, "main"),
            DatabaseName::Temp => write!(f, "temp"),
            DatabaseName::Attached(s) => write!(f, "{s}"),
        }
    }
}

/// Shorthand for [`DatabaseName::Main`].
pub const MAIN_DB: DatabaseName<'static> = DatabaseName::Main;

/// Shorthand for [`DatabaseName::Temp`].
pub const TEMP_DB: DatabaseName<'static> = DatabaseName::Temp;

/// A connection to a DuckDB database.
pub struct Connection {
    db: RefCell<InnerConnection>,
    cache: StatementCache,
    path: Option<PathBuf>,
}

unsafe impl Send for Connection {}

impl Connection {
    /// Open a new connection to a DuckDB database.
    ///
    /// `Connection::open(path)` is equivalent to
    /// `Connection::open_with_flags(path,
    /// Config::default())`.
    ///
    /// ```rust,no_run
    /// # use duckdb::{Connection, Result};
    /// fn open_my_db() -> Result<()> {
    ///     let path = "./my_db.db3";
    ///     let db = Connection::open(&path)?;
    ///     println!("{}", db.is_autocommit());
    ///     Ok(())
    /// }
    /// ```
    ///
    /// # Failure
    ///
    /// Will return `Err` if `path` cannot be converted to a C-compatible
    /// string or if the underlying DuckDB open call fails.
    #[inline]
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Connection> {
        Connection::open_with_flags(path, Config::default())
    }

    /// Open a new connection to an in-memory DuckDB database.
    ///
    /// # Failure
    ///
    /// Will return `Err` if the underlying DuckDB open call fails.
    #[inline]
    pub fn open_in_memory() -> Result<Connection> {
        Connection::open_in_memory_with_flags(Config::default())
    }

    /// Open a new connection to a DuckDB database.
    ///
    /// # Failure
    ///
    /// Will return `Err` if `path` cannot be converted to a C-compatible
    /// string or if the underlying DuckDB open call fails.
    #[inline]
    pub fn open_with_flags<P: AsRef<Path>>(path: P, config: Config) -> Result<Connection> {
        #[cfg(unix)]
        fn path_to_cstring(p: &Path) -> Result<CString> {
            use std::os::unix::ffi::OsStrExt;
            Ok(CString::new(p.as_os_str().as_bytes())?)
        }

        #[cfg(not(unix))]
        fn path_to_cstring(p: &Path) -> Result<CString> {
            let s = p.to_str().ok_or_else(|| Error::InvalidPath(p.to_owned()))?;
            Ok(CString::new(s)?)
        }

        let c_path = path_to_cstring(path.as_ref())?;
        InnerConnection::open_with_flags(&c_path, config).map(|db| Connection {
            db: RefCell::new(db),
            cache: StatementCache::with_capacity(STATEMENT_CACHE_DEFAULT_CAPACITY),
            path: Some(path.as_ref().to_path_buf()),
        })
    }

    /// Open a new connection to an in-memory DuckDB database.
    ///
    /// # Failure
    ///
    /// Will return `Err` if the underlying DuckDB open call fails.
    #[inline]
    pub fn open_in_memory_with_flags(config: Config) -> Result<Connection> {
        Connection::open_with_flags(":memory:", config)
    }

    /// Convenience method to run multiple SQL statements (that cannot take any
    /// parameters).
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// # use duckdb::{Connection, Result};
    /// fn create_tables(conn: &Connection) -> Result<()> {
    ///     conn.execute_batch("BEGIN;
    ///                         CREATE TABLE foo(x INTEGER);
    ///                         CREATE TABLE bar(y TEXT);
    ///                         COMMIT;",
    ///     )
    /// }
    /// ```
    ///
    /// # Failure
    ///
    /// Will return `Err` if `sql` cannot be converted to a C-compatible string
    /// or if the underlying DuckDB call fails.
    pub fn execute_batch(&self, sql: &str) -> Result<()> {
        self.db.borrow_mut().execute(sql)
    }

    /// Convenience method to prepare and execute a single SQL statement.
    ///
    /// On success, returns the number of rows that were changed or inserted or
    /// deleted.
    ///
    /// ## Example
    ///
    /// ### With params
    ///
    /// ```rust,no_run
    /// # use duckdb::{Connection};
    /// fn update_rows(conn: &Connection) {
    ///     match conn.execute("UPDATE foo SET bar = 'baz' WHERE qux = ?", [1i32]) {
    ///         Ok(updated) => println!("{} rows were updated", updated),
    ///         Err(err) => println!("update failed: {}", err),
    ///     }
    /// }
    /// ```
    ///
    /// ### With params of varying types
    ///
    /// ```rust,no_run
    /// # use duckdb::{Connection, params};
    /// fn update_rows(conn: &Connection) {
    ///     match conn.execute("UPDATE foo SET bar = ? WHERE qux = ?", params![&"baz", 1i32]) {
    ///         Ok(updated) => println!("{} rows were updated", updated),
    ///         Err(err) => println!("update failed: {}", err),
    ///     }
    /// }
    /// ```
    ///
    /// # Failure
    ///
    /// Will return `Err` if `sql` cannot be converted to a C-compatible string
    /// or if the underlying DuckDB call fails.
    #[inline]
    pub fn execute<P: Params>(&self, sql: &str, params: P) -> Result<usize> {
        self.prepare(sql).and_then(|mut stmt| stmt.execute(params))
    }

    /// Returns the path to the database file, if one exists and is known.
    #[inline]
    pub fn path(&self) -> Option<&Path> {
        self.path.as_deref()
    }

    /// Convenience method to execute a query that is expected to return a
    /// single row.
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// # use duckdb::{Result, Connection};
    /// fn preferred_locale(conn: &Connection) -> Result<String> {
    ///     conn.query_row(
    ///         "SELECT value FROM preferences WHERE name='locale'",
    ///         [],
    ///         |row| row.get(0),
    ///     )
    /// }
    /// ```
    ///
    /// If the query returns more than one row, all rows except the first are
    /// ignored.
    ///
    /// Returns `Err(QueryReturnedNoRows)` if no results are returned. If the
    /// query truly is optional, you can call `.optional()` on the result of
    /// this to get a `Result<Option<T>>`.
    ///
    /// # Failure
    ///
    /// Will return `Err` if `sql` cannot be converted to a C-compatible string
    /// or if the underlying DuckDB call fails.
    #[inline]
    pub fn query_row<T, P, F>(&self, sql: &str, params: P, f: F) -> Result<T>
    where
        P: Params,
        F: FnOnce(&Row<'_>) -> Result<T>,
    {
        self.prepare(sql)?.query_row(params, f)
    }

    /// Convenience method to execute a query that is expected to return a
    /// single row, and execute a mapping via `f` on that returned row with
    /// the possibility of failure. The `Result` type of `f` must implement
    /// `std::convert::From<Error>`.
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// # use duckdb::{Result, Connection};
    /// fn preferred_locale(conn: &Connection) -> Result<String> {
    ///     conn.query_row_and_then(
    ///         "SELECT value FROM preferences WHERE name='locale'",
    ///         [],
    ///         |row| row.get(0),
    ///     )
    /// }
    /// ```
    ///
    /// If the query returns more than one row, all rows except the first are
    /// ignored.
    ///
    /// # Failure
    ///
    /// Will return `Err` if `sql` cannot be converted to a C-compatible string
    /// or if the underlying DuckDB call fails.
    #[inline]
    pub fn query_row_and_then<T, E, P, F>(&self, sql: &str, params: P, f: F) -> Result<T, E>
    where
        P: Params,
        F: FnOnce(&Row<'_>) -> Result<T, E>,
        E: convert::From<Error>,
    {
        self.prepare(sql)?
            .query(params)?
            .get_expected_row()
            .map_err(E::from)
            .and_then(f)
    }

    /// Prepare a SQL statement for execution.
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// # use duckdb::{Connection, Result};
    /// fn insert_new_people(conn: &Connection) -> Result<()> {
    ///     let mut stmt = conn.prepare("INSERT INTO People (name) VALUES (?)")?;
    ///     stmt.execute(["Joe Smith"])?;
    ///     stmt.execute(["Bob Jones"])?;
    ///     Ok(())
    /// }
    /// ```
    ///
    /// # Failure
    ///
    /// Will return `Err` if `sql` cannot be converted to a C-compatible string
    /// or if the underlying DuckDB call fails.
    #[inline]
    pub fn prepare(&self, sql: &str) -> Result<Statement<'_>> {
        self.db.borrow_mut().prepare(self, sql)
    }

    /// Create an Appender for fast import data
    /// default to use `DatabaseName::Main`
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
    /// Will return `Err` if `table` not exists
    pub fn appender(&self, table: &str) -> Result<Appender<'_>> {
        self.appender_to_db(table, &DatabaseName::Main.to_string())
    }

    /// Create an Appender for fast import data
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// # use duckdb::{Connection, Result, params, DatabaseName};
    /// fn insert_rows(conn: &Connection) -> Result<()> {
    ///     let mut app = conn.appender_to_db("foo", &DatabaseName::Main.to_string())?;
    ///     app.append_rows([[1, 2], [3, 4], [5, 6], [7, 8], [9, 10]])?;
    ///     Ok(())
    /// }
    /// ```
    ///
    /// # Failure
    ///
    /// Will return `Err` if `table` not exists
    pub fn appender_to_db(&self, table: &str, schema: &str) -> Result<Appender<'_>> {
        self.db.borrow_mut().appender(self, table, schema)
    }

    /// Close the DuckDB connection.
    ///
    /// This is functionally equivalent to the `Drop` implementation for
    /// `Connection` except that on failure, it returns an error and the
    /// connection itself (presumably so closing can be attempted again).
    ///
    /// # Failure
    ///
    /// Will return `Err` if the underlying DuckDB call fails.
    #[inline]
    pub fn close(self) -> Result<(), (Connection, Error)> {
        let r = self.db.borrow_mut().close();
        r.map_err(move |err| (self, err))
    }

    /// Test for auto-commit mode.
    /// Autocommit mode is on by default.
    #[inline]
    pub fn is_autocommit(&self) -> bool {
        self.db.borrow().is_autocommit()
    }

    /// Creates a new connection to the already-opened database.
    pub fn try_clone(&self) -> Result<Self> {
        let inner = self.db.borrow().try_clone()?;
        Ok(Connection {
            db: RefCell::new(inner),
            cache: StatementCache::with_capacity(STATEMENT_CACHE_DEFAULT_CAPACITY),
            path: self.path.clone(),
        })
    }

    /// Returns the version of the DuckDB library
    pub fn version(&self) -> Result<String> {
        self.query_row("PRAGMA version", [], |row| row.get(0))
    }
}

impl fmt::Debug for Connection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Connection").field("path", &self.path).finish()
    }
}

#[cfg(doctest)]
doc_comment::doctest!("../README.md");

#[cfg(test)]
mod test {
    use super::*;
    use std::error::Error as StdError;
    use std::fmt;

    use arrow::array::Int32Array;
    use arrow::datatypes::DataType;
    use arrow::record_batch::RecordBatch;
    use fallible_iterator::FallibleIterator;

    // this function is never called, but is still type checked; in
    // particular, calls with specific instantiations will require
    // that those types are `Send`.
    #[allow(dead_code, unconditional_recursion)]
    fn ensure_send<T: Send>() {
        ensure_send::<Connection>();
    }

    pub fn checked_memory_handle() -> Connection {
        Connection::open_in_memory().unwrap()
    }

    #[test]
    fn test_params_of_vary_types() -> Result<()> {
        let db = checked_memory_handle();
        let sql = "BEGIN;
                   CREATE TABLE foo(bar TEXT, qux INTEGER);
                   INSERT INTO foo VALUES ('baz', 1), ('baz', 2), ('baz', 3);
                   END;";
        db.execute_batch(sql)?;

        let changed = db.execute("UPDATE foo SET qux = ? WHERE bar = ?", params![1i32, &"baz"])?;
        assert_eq!(changed, 3);
        Ok(())
    }

    #[test]
    #[cfg_attr(windows, ignore = "Windows doesn't allow concurrent writes to a file")]
    fn test_concurrent_transactions_busy_commit() -> Result<()> {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("transactions.db3");

        Connection::open(&path)?.execute_batch(
            "
            BEGIN;
            CREATE TABLE foo(x INTEGER);
            INSERT INTO foo VALUES(42);
            END;",
        )?;

        let mut db1 =
            Connection::open_with_flags(&path, Config::default().access_mode(config::AccessMode::ReadWrite)?)?;
        let mut db2 =
            Connection::open_with_flags(&path, Config::default().access_mode(config::AccessMode::ReadWrite)?)?;

        {
            let tx1 = db1.transaction()?;
            let tx2 = db2.transaction()?;

            // SELECT first makes sqlite lock with a shared lock
            tx1.query_row("SELECT x FROM foo LIMIT 1", [], |_| Ok(()))?;
            tx2.query_row("SELECT x FROM foo LIMIT 1", [], |_| Ok(()))?;

            tx1.execute("INSERT INTO foo VALUES(?1)", [1])?;
            let _ = tx2.execute("INSERT INTO foo VALUES(?1)", [2]);

            let _ = tx1.commit();
            let _ = tx2.commit();
        }

        let _ = db1.transaction().expect("commit should have closed transaction");
        let _ = db2.transaction().expect("commit should have closed transaction");
        Ok(())
    }

    #[test]
    fn test_persistence() -> Result<()> {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("test.db3");

        {
            let db = Connection::open(&path)?;
            let sql = "BEGIN;
                   CREATE TABLE foo(x INTEGER);
                   INSERT INTO foo VALUES(42);
                   END;";
            db.execute_batch(sql)?;
        }

        let path_string = path.to_str().unwrap();
        let db = Connection::open(path_string)?;
        let the_answer: Result<i64> = db.query_row("SELECT x FROM foo", [], |r| r.get(0));

        assert_eq!(42i64, the_answer?);
        Ok(())
    }

    #[test]
    fn test_open() {
        let con = Connection::open_in_memory();
        if con.is_err() {
            panic!("open error {}", con.unwrap_err());
        }
        assert!(Connection::open_in_memory().is_ok());
        let db = checked_memory_handle();
        assert!(db.close().is_ok());
        let _ = checked_memory_handle();
        let _ = checked_memory_handle();
    }

    #[test]
    fn test_open_failure() -> Result<()> {
        let filename = "no_such_file.db";
        let result =
            Connection::open_with_flags(filename, Config::default().access_mode(config::AccessMode::ReadOnly)?);
        assert!(result.is_err());
        let err = result.err().unwrap();
        if let Error::DuckDBFailure(_e, Some(msg)) = err {
            // TODO: update error code
            // assert_eq!(ErrorCode::CannotOpen, e.code);
            assert!(
                msg.contains(filename),
                "error message '{msg}' does not contain '{filename}'"
            );
        } else {
            panic!("DuckDBFailure expected");
        }
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn test_invalid_unicode_file_names() -> Result<()> {
        use std::ffi::OsStr;
        use std::fs::File;
        use std::os::unix::ffi::OsStrExt;
        let temp_dir = tempfile::tempdir().unwrap();

        let path = temp_dir.path();
        if File::create(path.join(OsStr::from_bytes(&[0xFE]))).is_err() {
            // Skip test, filesystem doesn't support invalid Unicode
            return Ok(());
        }
        let db_path = path.join(OsStr::from_bytes(&[0xFF]));
        {
            let db = Connection::open(&db_path)?;
            let sql = "BEGIN;
                   CREATE TABLE foo(x INTEGER);
                   INSERT INTO foo VALUES(42);
                   END;";
            db.execute_batch(sql)?;
        }

        let db = Connection::open(&db_path)?;
        let the_answer: Result<i64> = db.query_row("SELECT x FROM foo", [], |r| r.get(0));

        assert_eq!(42i64, the_answer?);
        Ok(())
    }

    #[test]
    fn test_close_always_ok() -> Result<()> {
        let db = checked_memory_handle();

        // TODO: prepare a query but not execute it

        db.close().unwrap();
        Ok(())
    }

    #[test]
    fn test_execute_batch() -> Result<()> {
        let db = checked_memory_handle();
        let sql = "BEGIN;
                   CREATE TABLE foo(x INTEGER);
                   INSERT INTO foo VALUES(1);
                   INSERT INTO foo VALUES(2);
                   INSERT INTO foo VALUES(3);
                   INSERT INTO foo VALUES(4);
                   END;";
        db.execute_batch(sql)?;

        db.execute_batch("UPDATE foo SET x = 3 WHERE x < 3")?;

        assert!(db.execute_batch("INVALID SQL").is_err());
        Ok(())
    }

    #[test]
    fn test_execute_single() -> Result<()> {
        let db = checked_memory_handle();
        db.execute_batch("CREATE TABLE foo(x INTEGER)")?;

        assert_eq!(
            3,
            db.execute("INSERT INTO foo(x) VALUES (?), (?), (?)", [1i32, 2i32, 3i32])?
        );
        assert_eq!(1, db.execute("INSERT INTO foo(x) VALUES (?)", [4i32])?);

        assert_eq!(
            10i32,
            db.query_row::<i32, _, _>("SELECT SUM(x) FROM foo", [], |r| r.get(0))?
        );
        Ok(())
    }

    #[test]
    fn test_prepare_column_names() -> Result<()> {
        let db = checked_memory_handle();
        db.execute_batch("CREATE TABLE foo(x INTEGER);")?;

        let mut stmt = db.prepare("SELECT * FROM foo")?;
        stmt.execute([])?;
        assert_eq!(stmt.column_count(), 1);
        assert_eq!(stmt.column_names(), vec!["x"]);

        let mut stmt = db.prepare("SELECT x AS a, x AS b FROM foo")?;
        stmt.execute([])?;
        assert_eq!(stmt.column_count(), 2);
        assert_eq!(stmt.column_names(), vec!["a", "b"]);
        Ok(())
    }

    #[test]
    fn test_prepare_execute() -> Result<()> {
        let db = checked_memory_handle();
        db.execute_batch("CREATE TABLE foo(x INTEGER);")?;

        let mut insert_stmt = db.prepare("INSERT INTO foo(x) VALUES(?)")?;
        assert_eq!(insert_stmt.execute([1i32])?, 1);
        assert_eq!(insert_stmt.execute([2i32])?, 1);
        assert_eq!(insert_stmt.execute([3i32])?, 1);

        assert!(insert_stmt.execute(["hello"]).is_err());
        // NOTE: can't execute on errored stmt
        // assert!(insert_stmt.execute(["goodbye"]).is_err());
        // assert!(insert_stmt.execute([types::Null]).is_err());

        let mut update_stmt = db.prepare("UPDATE foo SET x=? WHERE x<?")?;
        assert_eq!(update_stmt.execute([3i32, 3i32])?, 2);
        assert_eq!(update_stmt.execute([3i32, 3i32])?, 0);
        assert_eq!(update_stmt.execute([8i32, 8i32])?, 3);
        Ok(())
    }

    #[test]
    fn test_prepare_query() -> Result<()> {
        let db = checked_memory_handle();
        db.execute_batch("CREATE TABLE foo(x INTEGER);")?;

        let mut insert_stmt = db.prepare("INSERT INTO foo(x) VALUES(?)")?;
        assert_eq!(insert_stmt.execute([1i32])?, 1);
        assert_eq!(insert_stmt.execute([2i32])?, 1);
        assert_eq!(insert_stmt.execute([3i32])?, 1);

        let mut query = db.prepare("SELECT x FROM foo WHERE x < ? ORDER BY x DESC")?;
        {
            let mut rows = query.query([4i32])?;
            let mut v = Vec::<i32>::new();

            while let Some(row) = rows.next()? {
                v.push(row.get(0)?);
            }

            assert_eq!(v, [3i32, 2, 1]);
        }

        {
            let mut rows = query.query([3i32])?;
            let mut v = Vec::<i32>::new();

            while let Some(row) = rows.next()? {
                v.push(row.get(0)?);
            }

            assert_eq!(v, [2i32, 1]);
        }
        Ok(())
    }

    #[test]
    fn test_query_map() -> Result<()> {
        let db = checked_memory_handle();
        let sql = "BEGIN;
                   CREATE TABLE foo(x INTEGER, y TEXT);
                   INSERT INTO foo VALUES(4, 'hello');
                   INSERT INTO foo VALUES(3, ', ');
                   INSERT INTO foo VALUES(2, 'world');
                   INSERT INTO foo VALUES(1, '!');
                   END;";
        db.execute_batch(sql)?;

        let mut query = db.prepare("SELECT x, y FROM foo ORDER BY x DESC")?;
        let results: Result<Vec<String>> = query.query([])?.map(|row| row.get(1)).collect();

        assert_eq!(results?.concat(), "hello, world!");
        Ok(())
    }

    #[test]
    fn test_query_row() -> Result<()> {
        let db = checked_memory_handle();
        let sql = "BEGIN;
                   CREATE TABLE foo(x INTEGER);
                   INSERT INTO foo VALUES(1);
                   INSERT INTO foo VALUES(2);
                   INSERT INTO foo VALUES(3);
                   INSERT INTO foo VALUES(4);
                   END;";
        db.execute_batch(sql)?;

        assert_eq!(
            10i64,
            db.query_row::<i64, _, _>("SELECT SUM(x) FROM foo", [], |r| r.get(0))?
        );

        let result: Result<i64> = db.query_row("SELECT x FROM foo WHERE x > 5", [], |r| r.get(0));
        match result.unwrap_err() {
            Error::QueryReturnedNoRows => (),
            err => panic!("Unexpected error {err}"),
        }

        let bad_query_result = db.query_row("NOT A PROPER QUERY; test123", [], |_| Ok(()));

        assert!(bad_query_result.is_err());
        Ok(())
    }

    #[test]
    fn test_optional() -> Result<()> {
        let db = checked_memory_handle();

        let result: Result<i64> = db.query_row("SELECT 1 WHERE 0 <> 0", [], |r| r.get(0));
        let result = result.optional();
        match result? {
            None => (),
            _ => panic!("Unexpected result"),
        }

        let result: Result<i64> = db.query_row("SELECT 1 WHERE 0 == 0", [], |r| r.get(0));
        let result = result.optional();
        match result? {
            Some(1) => (),
            _ => panic!("Unexpected result"),
        }

        let bad_query_result: Result<i64> = db.query_row("NOT A PROPER QUERY", [], |r| r.get(0));
        let bad_query_result = bad_query_result.optional();
        assert!(bad_query_result.is_err());
        Ok(())
    }

    #[test]
    fn test_prepare_failures() -> Result<()> {
        let db = checked_memory_handle();
        db.execute_batch("CREATE TABLE foo(x INTEGER);")?;

        let _ = db.prepare("SELECT * FROM does_not_exist").unwrap_err();
        // assert!(format!("{}", err).contains("does_not_exist"));
        Ok(())
    }

    #[test]
    fn test_is_autocommit() {
        let db = checked_memory_handle();
        assert!(db.is_autocommit(), "autocommit expected to be active by default");
    }

    #[test]
    #[ignore = "not supported"]
    fn test_statement_debugging() -> Result<()> {
        let db = checked_memory_handle();
        let query = "SELECT 12345";
        let stmt = db.prepare(query)?;

        assert!(format!("{stmt:?}").contains(query));
        Ok(())
    }

    #[test]
    fn test_notnull_constraint_error() -> Result<()> {
        let db = checked_memory_handle();
        db.execute_batch("CREATE TABLE foo(x TEXT NOT NULL)")?;

        let result = db.execute("INSERT INTO foo (x) VALUES (NULL)", []);
        assert!(result.is_err());

        match result.unwrap_err() {
            Error::DuckDBFailure(err, _) => {
                // TODO(wangfenjin): Update errorcode
                assert_eq!(err.code, ErrorCode::Unknown);
            }
            err => panic!("Unexpected error {err}"),
        }
        Ok(())
    }

    #[test]
    fn test_clone() -> Result<()> {
        // 1. Drop the cloned connection first. The original connection should still be able to run queries.
        {
            let owned_con = checked_memory_handle();
            {
                let cloned_con = owned_con.try_clone().unwrap();
                cloned_con.execute_batch("create table test (c1 bigint)")?;
                cloned_con.close().unwrap();
            }
            owned_con.execute_batch("create table test2 (c1 bigint)")?;
            owned_con.close().unwrap();
        }

        // 2. Close the original connection first. The cloned connection should still be able to run queries.
        {
            let cloned_con = {
                let owned_con = checked_memory_handle();
                let clone = owned_con.try_clone().unwrap();
                owned_con.execute_batch("create table test (c1 bigint)")?;
                owned_con.close().unwrap();
                clone
            };
            cloned_con.execute_batch("create table test2 (c1 bigint)")?;
            cloned_con.close().unwrap();
        }
        Ok(())
    }

    mod query_and_then_tests {

        use super::*;

        #[derive(Debug)]
        enum CustomError {
            SomeError,
            Sqlite(Error),
        }

        impl fmt::Display for CustomError {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
                match *self {
                    CustomError::SomeError => write!(f, "my custom error"),
                    CustomError::Sqlite(ref se) => write!(f, "my custom error: {se}"),
                }
            }
        }

        impl StdError for CustomError {
            fn description(&self) -> &str {
                "my custom error"
            }

            fn cause(&self) -> Option<&dyn StdError> {
                match *self {
                    CustomError::SomeError => None,
                    CustomError::Sqlite(ref se) => Some(se),
                }
            }
        }

        impl From<Error> for CustomError {
            fn from(se: Error) -> CustomError {
                CustomError::Sqlite(se)
            }
        }

        type CustomResult<T> = Result<T, CustomError>;

        #[test]
        fn test_query_and_then() -> Result<()> {
            let db = checked_memory_handle();
            let sql = "BEGIN;
                       CREATE TABLE foo(x INTEGER, y TEXT);
                       INSERT INTO foo VALUES(4, 'hello');
                       INSERT INTO foo VALUES(3, ', ');
                       INSERT INTO foo VALUES(2, 'world');
                       INSERT INTO foo VALUES(1, '!');
                       END;";
            db.execute_batch(sql)?;

            let mut query = db.prepare("SELECT x, y FROM foo ORDER BY x DESC")?;
            let results: Result<Vec<String>> = query.query_and_then([], |row| row.get(1))?.collect();

            assert_eq!(results?.concat(), "hello, world!");
            Ok(())
        }

        #[test]
        fn test_query_and_then_fails() -> Result<()> {
            let db = checked_memory_handle();
            let sql = "BEGIN;
                       CREATE TABLE foo(x INTEGER, y TEXT);
                       INSERT INTO foo VALUES(4, 'hello');
                       INSERT INTO foo VALUES(3, ', ');
                       INSERT INTO foo VALUES(2, 'world');
                       INSERT INTO foo VALUES(1, '!');
                       END;";
            db.execute_batch(sql)?;

            let mut query = db.prepare("SELECT x, y FROM foo ORDER BY x DESC")?;
            let bad_type: Result<Vec<f64>> = query.query_and_then([], |row| row.get(1))?.collect();

            match bad_type.unwrap_err() {
                Error::InvalidColumnType(..) => (),
                err => panic!("Unexpected error {err}"),
            }

            let bad_idx: Result<Vec<String>> = query.query_and_then([], |row| row.get(3))?.collect();

            match bad_idx.unwrap_err() {
                Error::InvalidColumnIndex(_) => (),
                err => panic!("Unexpected error {err}"),
            }
            Ok(())
        }

        #[test]
        fn test_query_and_then_custom_error() -> CustomResult<()> {
            let db = checked_memory_handle();
            let sql = "BEGIN;
                       CREATE TABLE foo(x INTEGER, y TEXT);
                       INSERT INTO foo VALUES(4, 'hello');
                       INSERT INTO foo VALUES(3, ', ');
                       INSERT INTO foo VALUES(2, 'world');
                       INSERT INTO foo VALUES(1, '!');
                       END;";
            db.execute_batch(sql)?;

            let mut query = db.prepare("SELECT x, y FROM foo ORDER BY x DESC")?;
            let results: CustomResult<Vec<String>> = query
                .query_and_then([], |row| row.get(1).map_err(CustomError::Sqlite))?
                .collect();

            assert_eq!(results?.concat(), "hello, world!");
            Ok(())
        }

        #[test]
        fn test_query_and_then_custom_error_fails() -> Result<()> {
            let db = checked_memory_handle();
            let sql = "BEGIN;
                       CREATE TABLE foo(x INTEGER, y TEXT);
                       INSERT INTO foo VALUES(4, 'hello');
                       INSERT INTO foo VALUES(3, ', ');
                       INSERT INTO foo VALUES(2, 'world');
                       INSERT INTO foo VALUES(1, '!');
                       END;";
            db.execute_batch(sql)?;

            let mut query = db.prepare("SELECT x, y FROM foo ORDER BY x DESC")?;
            let bad_type: CustomResult<Vec<f64>> = query
                .query_and_then([], |row| row.get(1).map_err(CustomError::Sqlite))?
                .collect();

            match bad_type.unwrap_err() {
                CustomError::Sqlite(Error::InvalidColumnType(..)) => (),
                err => panic!("Unexpected error {err}"),
            }

            let bad_idx: CustomResult<Vec<String>> = query
                .query_and_then([], |row| row.get(3).map_err(CustomError::Sqlite))?
                .collect();

            match bad_idx.unwrap_err() {
                CustomError::Sqlite(Error::InvalidColumnIndex(_)) => (),
                err => panic!("Unexpected error {err}"),
            }

            let non_sqlite_err: CustomResult<Vec<String>> =
                query.query_and_then([], |_| Err(CustomError::SomeError))?.collect();

            match non_sqlite_err.unwrap_err() {
                CustomError::SomeError => (),
                err => panic!("Unexpected error {err}"),
            }
            Ok(())
        }

        #[test]
        fn test_query_row_and_then_custom_error() -> CustomResult<()> {
            let db = checked_memory_handle();
            let sql = "BEGIN;
                       CREATE TABLE foo(x INTEGER, y TEXT);
                       INSERT INTO foo VALUES(4, 'hello');
                       END;";
            db.execute_batch(sql)?;

            let query = "SELECT x, y FROM foo ORDER BY x DESC";
            let results: CustomResult<String> =
                db.query_row_and_then(query, [], |row| row.get(1).map_err(CustomError::Sqlite));

            assert_eq!(results?, "hello");
            Ok(())
        }

        #[test]
        fn test_query_row_and_then_custom_error_fails() -> Result<()> {
            let db = checked_memory_handle();
            let sql = "BEGIN;
                       CREATE TABLE foo(x INTEGER, y TEXT);
                       INSERT INTO foo VALUES(4, 'hello');
                       END;";
            db.execute_batch(sql)?;

            let query = "SELECT x, y FROM foo ORDER BY x DESC";
            let bad_type: CustomResult<f64> =
                db.query_row_and_then(query, [], |row| row.get(1).map_err(CustomError::Sqlite));

            match bad_type.unwrap_err() {
                CustomError::Sqlite(Error::InvalidColumnType(..)) => (),
                err => panic!("Unexpected error {err}"),
            }

            let bad_idx: CustomResult<String> =
                db.query_row_and_then(query, [], |row| row.get(3).map_err(CustomError::Sqlite));

            match bad_idx.unwrap_err() {
                CustomError::Sqlite(Error::InvalidColumnIndex(_)) => (),
                err => panic!("Unexpected error {err}"),
            }

            let non_sqlite_err: CustomResult<String> =
                db.query_row_and_then(query, [], |_| Err(CustomError::SomeError));

            match non_sqlite_err.unwrap_err() {
                CustomError::SomeError => (),
                err => panic!("Unexpected error {err}"),
            }
            Ok(())
        }
    }

    #[test]
    fn test_dynamic() -> Result<()> {
        let db = checked_memory_handle();
        let sql = "BEGIN;
                       CREATE TABLE foo(x INTEGER, y TEXT);
                       INSERT INTO foo VALUES(4, 'hello');
                       END;";
        db.execute_batch(sql)?;

        db.query_row("SELECT * FROM foo", [], |r| {
            assert_eq!(2, r.as_ref().column_count());
            Ok(())
        })
    }
    #[test]
    fn test_dyn_box() -> Result<()> {
        let db = checked_memory_handle();
        db.execute_batch("CREATE TABLE foo(x INTEGER);")?;
        let b: Box<dyn ToSql> = Box::new(5);
        db.execute("INSERT INTO foo VALUES(?)", [b])?;
        db.query_row("SELECT x FROM foo", [], |r| {
            assert_eq!(5, r.get_unwrap::<_, i32>(0));
            Ok(())
        })
    }

    #[test]
    fn test_alter_table() -> Result<()> {
        let db = checked_memory_handle();
        db.execute_batch("CREATE TABLE x(t INTEGER);")?;
        // `execute_batch` should be used but `execute` should also work
        db.execute("ALTER TABLE x RENAME TO y;", [])?;
        Ok(())
    }

    #[test]
    fn test_query_arrow_record_batch_small() -> Result<()> {
        let db = checked_memory_handle();
        let sql = "BEGIN TRANSACTION;
                   CREATE TABLE test(t INTEGER);
                   INSERT INTO test VALUES (1); INSERT INTO test VALUES (2); INSERT INTO test VALUES (3); INSERT INTO test VALUES (4); INSERT INTO test VALUES (5);
                   END TRANSACTION;";
        db.execute_batch(sql)?;
        let mut stmt = db.prepare("select t from test order by t desc")?;
        let mut arr = stmt.query_arrow([])?;

        let schema = arr.get_schema();
        assert_eq!(schema.fields().len(), 1);
        assert_eq!(schema.field(0).name(), "t");
        assert_eq!(schema.field(0).data_type(), &DataType::Int32);

        let rb = arr.next().unwrap();
        let column = rb.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(column.len(), 5);
        assert_eq!(column.value(0), 5);
        assert_eq!(column.value(1), 4);
        assert_eq!(column.value(2), 3);
        assert_eq!(column.value(3), 2);
        assert_eq!(column.value(4), 1);

        assert!(arr.next().is_none());
        Ok(())
    }

    #[test]
    fn test_query_arrow_record_batch_large() -> Result<()> {
        let db = checked_memory_handle();
        db.execute_batch("BEGIN TRANSACTION")?;
        db.execute_batch("CREATE TABLE test(t INTEGER);")?;
        for _ in 0..600 {
            db.execute_batch("INSERT INTO test VALUES (1); INSERT INTO test VALUES (2); INSERT INTO test VALUES (3); INSERT INTO test VALUES (4); INSERT INTO test VALUES (5);")?;
        }
        db.execute_batch("END TRANSACTION")?;
        let rbs: Vec<RecordBatch> = db.prepare("select t from test order by t")?.query_arrow([])?.collect();
        // batch size is not stable
        // assert_eq!(rbs.len(), 3);
        assert_eq!(rbs.iter().map(|rb| rb.num_rows()).sum::<usize>(), 3000);
        assert_eq!(
            rbs.iter()
                .map(|rb| rb
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .iter()
                    .map(|i| i.unwrap())
                    .sum::<i32>())
                .sum::<i32>(),
            9000
        );
        Ok(())
    }

    #[test]
    fn test_database_name_to_string() -> Result<()> {
        assert_eq!(DatabaseName::Main.to_string(), "main");
        assert_eq!(DatabaseName::Temp.to_string(), "temp");
        assert_eq!(DatabaseName::Attached("abc").to_string(), "abc");
        Ok(())
    }

    #[cfg(feature = "bundled")]
    #[test]
    fn test_version() -> Result<()> {
        let db = checked_memory_handle();
        const VERSION: &str = env!("CARGO_PKG_VERSION");
        let version = db.version()?;
        assert_eq!(version, VERSION);
        Ok(())
    }
}

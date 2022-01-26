#![deny(warnings)]
//! # Duckdb-rs support for the `r2d2` connection pool.
//!
//!
//! Integrated with: [r2d2](https://crates.io/crates/r2d2)
//!
//!
//! ## Example
//!
//! ```rust,no_run
//! extern crate r2d2;
//! extern crate duckdb;
//! 
//!
//! use std::thread;
//! use duckdb::{DuckdbConnectionManager, params};
//! 
//!
//! fn main() {
//!     let manager = DuckdbConnectionManager::file("file.db").unwrap();
//!     let pool = r2d2::Pool::new(manager).unwrap();
//!     pool.get()
//!         .unwrap()
//!         .execute("CREATE TABLE IF NOT EXISTS foo (bar INTEGER)", params![])
//!         .unwrap();
//!
//!     (0..10)
//!         .map(|i| {
//!             let pool = pool.clone();
//!             thread::spawn(move || {
//!                 let conn = pool.get().unwrap();
//!                 conn.execute("INSERT INTO foo (bar) VALUES (?)", &[&i])
//!                     .unwrap();
//!             })
//!         })
//!         .collect::<Vec<_>>()
//!         .into_iter()
//!         .map(thread::JoinHandle::join)
//!         .collect::<Result<_, _>>()
//!         .unwrap()
//! }
//! ```
use std::{path::Path,  sync::{Mutex, Arc}};
use crate::{Result, Connection, Error, Config};

/// An `r2d2::ManageConnection` for `duckdb::Connection`s.
pub struct DuckdbConnectionManager {
    connection: Arc<Mutex<Connection>>,
}

impl DuckdbConnectionManager {

    /// Creates a new `DuckdbConnectionManager` from file.
    pub fn file<P: AsRef<Path>>(path: P) -> Result<Self> {
        Ok(Self {
            connection: Arc::new(Mutex::new(Connection::open(path)?)),
        })
    }
    /// Creates a new `DuckdbConnectionManager` from file with flags.
    pub fn file_with_flags<P: AsRef<Path>>(path: P, config: Config) -> Result<Self> {
        Ok(Self {
            connection: Arc::new(Mutex::new(Connection::open_with_flags(path, config)?)),
        })
    }

    /// Creates a new `DuckdbConnectionManager` from memory.
    pub fn memory() -> Result<Self> {
        Ok(Self {
            connection: Arc::new(Mutex::new(Connection::open_in_memory()?)),
        })
    }

    /// Creates a new `DuckdbConnectionManager` from memory with flags.
    pub fn memory_with_flags(config: Config) -> Result<Self> {
        Ok(Self {
            connection: Arc::new(Mutex::new(Connection::open_in_memory_with_flags(config)?)),
        })
    }
}

impl r2d2::ManageConnection for DuckdbConnectionManager {
    type Connection = Connection;
    type Error = Error;

    fn connect(&self) -> Result<Self::Connection, Self::Error> {
        let conn = self.connection.lock().unwrap();
        Ok(conn.clone())
    }

    fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        conn.execute_batch("").map_err(Into::into)
    }

    fn has_broken(&self, _: &mut Self::Connection) -> bool {
        false
    }
}


#[cfg(test)]
mod test {
    extern crate r2d2;
    use super::*;
    use crate::Result;
    use crate::types::Value;
    use std::{sync::mpsc, thread};

    use tempdir::TempDir;

   
    #[test]
    fn test_basic() -> Result<()>{
        let manager = DuckdbConnectionManager::file("file.db")?;
        let pool = r2d2::Pool::builder().max_size(2).build(manager).unwrap();
    
        let (s1, r1) = mpsc::channel();
        let (s2, r2) = mpsc::channel();
    
        let pool1 = pool.clone();
        let t1 = thread::spawn(move || {
            let conn = pool1.get().unwrap();
            s1.send(()).unwrap();
            r2.recv().unwrap();
            drop(conn);
        });
    
        let pool2 = pool.clone();
        let t2 = thread::spawn(move || {
            let conn = pool2.get().unwrap();
            s2.send(()).unwrap();
            r1.recv().unwrap();
            drop(conn);
        });
    
        t1.join().unwrap();
        t2.join().unwrap();
    
        pool.get().unwrap();
        Ok(())
    }
    
    #[test]
    fn test_file() -> Result<()>{
        let manager = DuckdbConnectionManager::file("file.db")?;
        let pool = r2d2::Pool::builder().max_size(2).build(manager).unwrap();
    
        let (s1, r1) = mpsc::channel();
        let (s2, r2) = mpsc::channel();
    
        let pool1 = pool.clone();
        let t1 = thread::spawn(move || {
            let conn = pool1.get().unwrap();
            let conn1: &Connection = &*conn;
            s1.send(()).unwrap();
            r2.recv().unwrap();
            drop(conn1);
        });
    
        let pool2 = pool.clone();
        let t2 = thread::spawn(move || {
            let conn = pool2.get().unwrap();
            s2.send(()).unwrap();
            r1.recv().unwrap();
            drop(conn);
        });
    
        t1.join().unwrap();
        t2.join().unwrap();
    
        pool.get().unwrap();
        Ok(())
    }
    
    #[test]
    fn test_is_valid() -> Result<()>{
        let manager = DuckdbConnectionManager::file("file.db")?;
        let pool = r2d2::Pool::builder()
            .max_size(1)
            .test_on_check_out(true)
            .build(manager)
            .unwrap();
    
        pool.get().unwrap();
        Ok(())
    }
    
    #[test]
    fn test_error_handling() -> Result<()> {
        //! We specify a directory as a database. This is bound to fail.
        let dir = TempDir::new("r2d2-duckdb").expect("Could not create temporary directory");
        let dirpath = dir.path().to_str().unwrap();
        assert!(DuckdbConnectionManager::file(dirpath).is_err());
        Ok(())
    }
    
    #[test]
    fn test_with_flags() -> Result<()> {
        let config = Config::default()
        .access_mode(crate::AccessMode::ReadWrite)?
        .default_null_order(crate::DefaultNullOrder::NullsLast)?
        .default_order(crate::DefaultOrder::Desc)?
        .enable_external_access(true)?
        .enable_object_cache(false)?
        .max_memory("2GB")?
        .threads(4)?;
        let manager = DuckdbConnectionManager::file_with_flags("file.db", config)?;
        let pool = r2d2::Pool::builder().max_size(2).build(manager).unwrap();
        let conn = pool.get().unwrap();
        conn.execute_batch("CREATE TABLE foo(x Text)")?;

        let mut stmt = conn.prepare("INSERT INTO foo(x) VALUES (?)")?;
        stmt.execute(&[&"a"])?;
        stmt.execute(&[&"b"])?;
        stmt.execute(&[&"c"])?;
        stmt.execute([Value::Null])?;

        let val: Result<Vec<Option<String>>> = conn
            .prepare("SELECT x FROM foo ORDER BY x")?
            .query_and_then([], |row| row.get(0))?
            .collect();
        let val = val?;
        let mut iter = val.iter();
        assert_eq!(iter.next().unwrap().as_ref().unwrap(), "c");
        assert_eq!(iter.next().unwrap().as_ref().unwrap(), "b");
        assert_eq!(iter.next().unwrap().as_ref().unwrap(), "a");
        assert!(iter.next().unwrap().is_none());
        assert_eq!(iter.next(), None);

        Ok(())
    }
}
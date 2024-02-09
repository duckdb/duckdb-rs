use std::sync::{Arc, Mutex};

pub use libduckdb_sys as ffi;

use crate::Connection;

/// A wrapper to the bindgen database to implement the sent trait, from which new connections can be created across threads.
#[derive(Debug, Copy, Clone)]
pub struct Database(pub ffi::duckdb_database);
unsafe impl Send for Database {}

#[derive(Debug, Clone)]
pub struct DatabaseHandle(Arc<Mutex<Database>>);
impl DatabaseHandle {
    pub fn new(db: Database) -> Self {
        Self(Arc::new(Mutex::new(db)))
    }
    /// get a new connection to the database
    pub fn connection(&self) -> crate::Result<Connection> {
        unsafe { Connection::open_from_raw(self.0.lock().unwrap().0) }
    }
}

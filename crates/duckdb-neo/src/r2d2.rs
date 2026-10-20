//! Connection pooling through [`r2d2`]; the manager keeps its instance and environment alive.
//!
//! Connections retain transactions, settings, and temporary tables across checkouts.
//! Use [`r2d2::CustomizeConnection`] to initialize new connections; clean up before returning them.
//!
//! ```
//! use duckdb_neo::{environment::{Environment, StorageLocation}, r2d2::ConnectionManager, Parameters};
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let env = Environment::new()?;
//! let instance = env.open(StorageLocation::InMemory)?;
//! let pool = r2d2::Pool::builder().build(ConnectionManager::new(&instance))?;
//! let conn = pool.get()?;
//! conn.execute("SELECT 1", Parameters::None)?;
//! # Ok(())
//! # }
//! ```

use std::sync::OnceLock;

use crate::{Parameters, Result, connection::Connection, error::Error, instance::Instance};

/// An [`r2d2::ManageConnection`] that opens connections to a shared [`Instance`].
#[derive(Debug)]
pub struct ConnectionManager {
    instance: Instance,
}

impl ConnectionManager {
    /// Create a manager that opens connections to `instance` and keeps it alive.
    /// Checked-out connections retain their session state when returned to the pool.
    pub fn new(instance: &Instance) -> Self {
        Self {
            instance: Instance {
                handle: instance.handle.clone(),
                factory: OnceLock::new(),
            },
        }
    }
}

impl r2d2::ManageConnection for ConnectionManager {
    type Connection = Connection;
    type Error = Error;

    fn connect(&self) -> Result<Connection> {
        Connection::new(&self.instance)
    }

    fn is_valid(&self, conn: &mut Self::Connection) -> Result<()> {
        conn.execute("SELECT 1", Parameters::None).map(drop)
    }

    fn has_broken(&self, _conn: &mut Self::Connection) -> bool {
        // Always false since duckdb lives in memory.
        false
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        Parameters,
        environment::{Environment, StorageLocation},
        r2d2::ConnectionManager,
    };

    #[test]
    fn test_is_valid_preserves_error() -> crate::Result<()> {
        use r2d2::ManageConnection;

        let env = Environment::new()?;
        let instance = env.open(StorageLocation::InMemory)?;
        let manager = ConnectionManager::new(&instance);
        let mut conn = manager.connect()?;
        conn.execute("BEGIN", Parameters::None)?;
        assert!(
            conn.execute("SELECT CAST('invalid' AS INTEGER)", Parameters::None)
                .is_err()
        );
        let expected = conn.execute("SELECT 1", Parameters::None).unwrap_err();
        let actual = manager.is_valid(&mut conn).unwrap_err();
        assert_eq!(actual.code, expected.code);
        assert_eq!(actual.message, expected.message);
        Ok(())
    }

    #[test]
    fn test_r2d2_valid_connection() -> crate::Result<()> {
        let env = Environment::new()?;
        let instance = env.open(StorageLocation::InMemory)?;
        {
            dbg!(instance.handle.lock().unwrap().handle.is_null());
        }

        let pool = r2d2::Pool::builder()
            .max_size(2)
            .build(ConnectionManager::new(&instance))
            .expect("Failed to create pool");

        let conn = pool.get().expect("Failed to get connection");
        assert!(conn.execute("SELECT 42", Parameters::None).is_ok());

        Ok(())
    }

    use std::{sync::mpsc, thread};

    #[test]
    fn test_r2d2_mpsc() -> crate::Result<()> {
        let env = Environment::new()?;
        let instance = env.open(StorageLocation::InMemory)?;

        let pool = r2d2::Pool::builder()
            .max_size(2)
            .build(ConnectionManager::new(&instance))
            .expect("Failed to create pool");

        {
            let conn = pool.get().unwrap();
            conn.execute("CREATE TABLE test (num INTEGER)", Parameters::None)?;
        }

        let (s1, r1) = mpsc::channel();
        let (s2, _r2) = mpsc::channel();

        let pool1 = pool.clone();
        let t1 = thread::spawn(move || {
            let conn = pool1.get().unwrap();

            let result = conn
                .execute("INSERT INTO test (num) VALUES (42)", Parameters::None)
                .unwrap();

            assert_eq!(result, 1);

            s1.send(()).unwrap();
            drop(conn);
        });

        let pool2 = pool.clone();
        let t2 = thread::spawn(move || {
            let conn = pool2.get().unwrap();
            r1.recv().unwrap();
            {
                let mut result = conn.query("SELECT * FROM test", Parameters::None).unwrap();

                let chunk = result.next_chunk().unwrap().unwrap();

                let vec = chunk.get_vector_at::<i32>(0).unwrap();

                assert_eq!(vec.get(0).unwrap(), Some(&42));
            }

            s2.send(()).unwrap();
            drop(conn);
        });

        t1.join().unwrap();
        t2.join().unwrap();

        pool.get().unwrap();

        Ok(())
    }
}

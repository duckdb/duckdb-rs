use crate::{Parameters, Result, connection::Connection, database::Instance, error::Error};

/// An [`r2d2::ManageConnection`] that opens connections to a shared [`Instance`].
pub struct ConnectionManager {
    database: Instance,
}

impl ConnectionManager {
    /// Create a manager that opens connections to `database`.
    pub fn new(database: &Instance) -> Self {
        Self {
            database: Instance {
                handle: database.handle.clone(),
            },
        }
    }
}

impl r2d2::ManageConnection for ConnectionManager {
    type Connection = Connection;
    type Error = Error;

    fn connect(&self) -> Result<Connection> {
        Connection::new(&self.database)
    }

    fn is_valid(&self, conn: &mut Self::Connection) -> std::result::Result<(), Self::Error> {
        let res = conn.execute("SELECT 1", Parameters::None);

        if !res.is_ok() {
            return Err(Error::api_error("Connection is not valid".to_string()));
        }

        Ok(())
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
    fn test_r2d2_valid_connection() -> crate::Result<()> {
        let env = Environment::new()?;
        let db = env.open(StorageLocation::InMemory)?;
        {
            dbg!(db.handle.lock().unwrap().handle.is_null());
        }

        let pool = r2d2::Pool::builder()
            .max_size(2)
            .build(ConnectionManager::new(&db))
            .expect("Failed to create pool");

        let conn = pool.get().expect("Failed to get connection");
        assert!(conn.execute("SELECT 42", Parameters::None).is_ok());

        Ok(())
    }

    use std::{sync::mpsc, thread};

    #[test]
    fn test_r2d2_mpsc() -> crate::Result<()> {
        let env = Environment::new()?;
        let db = env.open(StorageLocation::InMemory)?;

        let pool = r2d2::Pool::builder()
            .max_size(2)
            .build(ConnectionManager::new(&db))
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

                let chunk = result.next().unwrap().unwrap();

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

use crate::{Connection, Result};
use std::ops::Deref;

/// Options for how a Transaction should behave when it is dropped.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum DropBehavior {
    /// Roll back the changes. This is the default.
    Rollback,

    /// Commit the changes.
    Commit,

    /// Do not commit or roll back changes - this will leave the transaction open, so should be used with care.
    Ignore,

    /// Panic. Used to enforce intentional behavior during development.
    Panic,
}

/// Represents a transaction on a database connection.
///
/// ## Note
///
/// Transactions will roll back by default. Use `commit` method to explicitly
/// commit the transaction, or use `set_drop_behavior` to change what happens
/// when the transaction is dropped.
///
/// ## Example
///
/// ```rust,no_run
/// # use duckdb::{Connection, Result};
/// # fn do_queries_part_1(_conn: &Connection) -> Result<()> { Ok(()) }
/// # fn do_queries_part_2(_conn: &Connection) -> Result<()> { Ok(()) }
/// fn perform_queries(conn: &mut Connection) -> Result<()> {
///     let tx = conn.transaction()?;
///
///     do_queries_part_1(&tx)?; // tx causes rollback if this fails
///     do_queries_part_2(&tx)?; // tx causes rollback if this fails
///
///     tx.commit()
/// }
/// ```
#[derive(Debug)]
pub struct Transaction<'conn> {
    conn: &'conn Connection,
    drop_behavior: DropBehavior,
}

impl Transaction<'_> {
    /// Begin a new transaction. Cannot be nested.
    ///
    /// Even though we don't mutate the connection, we take a `&mut Connection`
    /// so as to prevent nested transactions on the same connection. For cases
    /// where this is unacceptable, [`Transaction::new_unchecked`] is available.
    #[inline]
    pub fn new(conn: &mut Connection) -> Result<Transaction<'_>> {
        Self::new_unchecked(conn)
    }

    /// Begin a new transaction, failing if a transaction is open.
    ///
    /// If a transaction is already open, this will return an error. Where
    /// possible, [`Transaction::new`] should be preferred, as it provides a
    /// compile-time guarantee that transactions are not nested.
    #[inline]
    pub fn new_unchecked(conn: &Connection) -> Result<Transaction<'_>> {
        let query = "BEGIN TRANSACTION";
        conn.execute_batch(query).map(move |_| Transaction {
            conn,
            drop_behavior: DropBehavior::Rollback,
        })
    }

    /// Get the current setting for what happens to the transaction when it is
    /// dropped.
    #[inline]
    pub fn drop_behavior(&self) -> DropBehavior {
        self.drop_behavior
    }

    /// Configure the transaction to perform the specified action when it is
    /// dropped.
    #[inline]
    pub fn set_drop_behavior(&mut self, drop_behavior: DropBehavior) {
        self.drop_behavior = drop_behavior
    }

    /// A convenience method which consumes and commits a transaction.
    #[inline]
    pub fn commit(mut self) -> Result<()> {
        self.commit_()
    }

    #[inline]
    fn commit_(&mut self) -> Result<()> {
        self.conn.execute_batch("COMMIT")
    }

    /// A convenience method which consumes and rolls back a transaction.
    #[inline]
    pub fn rollback(mut self) -> Result<()> {
        self.rollback_()
    }

    #[inline]
    fn rollback_(&mut self) -> Result<()> {
        self.conn.execute_batch("ROLLBACK")
    }

    /// Consumes the transaction, committing or rolling back according to the
    /// current setting (see `drop_behavior`).
    ///
    /// Functionally equivalent to the `Drop` implementation, but allows
    /// callers to see any errors that occur.
    #[inline]
    pub fn finish(mut self) -> Result<()> {
        self.finish_()
    }

    #[inline]
    fn finish_(&mut self) -> Result<()> {
        // if self.conn.is_autocommit() {
        //     println!("is autocommit");
        //     return Ok(());
        // }
        match self.drop_behavior() {
            DropBehavior::Commit => self.commit_().or_else(|_| self.rollback_()),
            DropBehavior::Rollback => self.rollback_(),
            DropBehavior::Ignore => Ok(()),
            DropBehavior::Panic => panic!("Transaction dropped unexpectedly."),
        }
    }
}

impl Deref for Transaction<'_> {
    type Target = Connection;

    #[inline]
    fn deref(&self) -> &Connection {
        self.conn
    }
}

#[allow(unused_must_use)]
impl Drop for Transaction<'_> {
    #[inline]
    fn drop(&mut self) {
        self.finish_();
    }
}

impl Connection {
    /// Begin a new transaction with the default behavior (DEFERRED).
    ///
    /// The transaction defaults to rolling back when it is dropped. If you
    /// want the transaction to commit, you must call
    /// [`commit`](Transaction::commit) or [`set_drop_behavior(DropBehavior:
    /// :Commit)`](Transaction::set_drop_behavior).
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// # use duckdb::{Connection, Result};
    /// # fn do_queries_part_1(_conn: &Connection) -> Result<()> { Ok(()) }
    /// # fn do_queries_part_2(_conn: &Connection) -> Result<()> { Ok(()) }
    /// fn perform_queries(conn: &mut Connection) -> Result<()> {
    ///     let tx = conn.transaction()?;
    ///
    ///     do_queries_part_1(&tx)?; // tx causes rollback if this fails
    ///     do_queries_part_2(&tx)?; // tx causes rollback if this fails
    ///
    ///     tx.commit()
    /// }
    /// ```
    ///
    /// # Failure
    ///
    /// Will return `Err` if the underlying DuckDB call fails.
    #[inline]
    pub fn transaction(&mut self) -> Result<Transaction<'_>> {
        Transaction::new(self)
    }

    /// Begin a new transaction with the default behavior (DEFERRED).
    ///
    /// Attempt to open a nested transaction will result in a DuckDB error.
    /// `Connection::transaction` prevents this at compile time by taking `&mut
    /// self`, but `Connection::unchecked_transaction()` may be used to defer
    /// the checking until runtime.
    ///
    /// See [`Connection::transaction`] and [`Transaction::new_unchecked`]
    /// (which can be used if the default transaction behavior is undesirable).
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// # use duckdb::{Connection, Result};
    /// # use std::rc::Rc;
    /// # fn do_queries_part_1(_conn: &Connection) -> Result<()> { Ok(()) }
    /// # fn do_queries_part_2(_conn: &Connection) -> Result<()> { Ok(()) }
    /// fn perform_queries(conn: Rc<Connection>) -> Result<()> {
    ///     let tx = conn.unchecked_transaction()?;
    ///
    ///     do_queries_part_1(&tx)?; // tx causes rollback if this fails
    ///     do_queries_part_2(&tx)?; // tx causes rollback if this fails
    ///
    ///     tx.commit()
    /// }
    /// ```
    ///
    /// # Failure
    ///
    /// Will return `Err` if the underlying DuckDB call fails. The specific
    /// error returned if transactions are nested is currently unspecified.
    pub fn unchecked_transaction(&self) -> Result<Transaction<'_>> {
        Transaction::new_unchecked(self)
    }
}

#[cfg(test)]
mod test {
    use super::DropBehavior;
    use crate::{Connection, Result};

    fn checked_no_autocommit_memory_handle() -> Result<Connection> {
        let db = Connection::open_in_memory()?;
        db.execute_batch(
            r"
            CREATE TABLE foo (x INTEGER);
            -- SET AUTOCOMMIT TO OFF;
        ",
        )?;
        Ok(db)
    }

    #[test]
    fn test_drop() -> Result<()> {
        let mut db = checked_no_autocommit_memory_handle()?;
        {
            let tx = db.transaction()?;
            assert!(tx.execute_batch("INSERT INTO foo VALUES(1)").is_ok());
            // default: rollback
        }
        {
            let mut tx = db.transaction()?;
            tx.execute_batch("INSERT INTO foo VALUES(2)")?;
            tx.set_drop_behavior(DropBehavior::Commit);
        }
        {
            let tx = db.transaction()?;
            assert_eq!(
                2i32,
                tx.query_row::<i32, _, _>("SELECT SUM(x) FROM foo", [], |r| r.get(0))?
            );
        }
        Ok(())
    }

    #[test]
    fn test_unchecked_nesting() -> Result<()> {
        let db = checked_no_autocommit_memory_handle()?;

        {
            db.unchecked_transaction()?;
            // default: rollback
        }
        {
            let tx = db.unchecked_transaction()?;
            tx.execute_batch("INSERT INTO foo VALUES(1)")?;
            // Ensure this doesn't interfere with ongoing transaction
            // let e = tx.unchecked_transaction().unwrap_err();
            // assert_nested_tx_error(e);
            tx.execute_batch("INSERT INTO foo VALUES(1)")?;
            tx.commit()?;
        }

        assert_eq!(
            2i32,
            db.query_row::<i32, _, _>("SELECT SUM(x) FROM foo", [], |r| r.get(0))?
        );
        Ok(())
    }

    #[test]
    fn test_explicit_rollback_commit() -> Result<()> {
        let mut db = checked_no_autocommit_memory_handle()?;
        {
            let tx = db.transaction()?;
            tx.execute_batch("INSERT INTO foo VALUES(1)")?;
            tx.rollback()?;
        }
        {
            let tx = db.transaction()?;
            tx.execute_batch("INSERT INTO foo VALUES(4)")?;
            tx.commit()?;
        }
        {
            let tx = db.transaction()?;
            assert_eq!(
                4i32,
                tx.query_row::<i32, _, _>("SELECT SUM(x) FROM foo", [], |r| r.get(0))?
            );
        }
        Ok(())
    }

    #[test]
    fn test_rc() -> Result<()> {
        use std::rc::Rc;
        let mut conn = Connection::open_in_memory()?;
        let rc_txn = Rc::new(conn.transaction()?);

        // This will compile only if Transaction is Debug
        Rc::try_unwrap(rc_txn).unwrap();
        Ok(())
    }
}

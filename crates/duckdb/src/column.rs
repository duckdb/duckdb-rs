use std::str;

use arrow::datatypes::DataType;

use crate::{core::LogicalTypeHandle, Error, Result, Statement};

/// Information about a column of a DuckDB query.
#[derive(Debug)]
pub struct Column<'stmt> {
    name: &'stmt str,
    decl_type: Option<&'stmt str>,
}

impl Column<'_> {
    /// Returns the name of the column.
    #[inline]
    pub fn name(&self) -> &str {
        self.name
    }

    /// Returns the type of the column (`None` for expression).
    #[inline]
    pub fn decl_type(&self) -> Option<&str> {
        self.decl_type
    }
}

impl Statement<'_> {
    /// Get all the column names in the result set of the prepared statement.
    ///
    /// If associated DB schema can be altered concurrently, you should make
    /// sure that current statement has already been stepped once before
    /// calling this method.
    ///
    /// # Caveats
    /// Panics if the query has not been [`execute`](Statement::execute)d yet.
    pub fn column_names(&self) -> Vec<String> {
        self.stmt
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().to_owned())
            .collect()
    }

    /// Return the number of columns in the result set returned by the prepared
    /// statement.
    ///
    /// If associated DB schema can be altered concurrently, you should make
    /// sure that current statement has already been stepped once before
    /// calling this method.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use duckdb::{Connection, Result};
    /// fn get_column_count(conn: &Connection) -> Result<usize> {
    ///     let mut stmt = conn.prepare("SELECT id, name FROM people")?;
    ///
    ///     // Option 1: Execute first, then get column count
    ///     stmt.execute([])?;
    ///     let count = stmt.column_count();
    ///
    ///     // Option 2: Get column count from rows (avoids borrowing issues)
    ///     let mut stmt2 = conn.prepare("SELECT id, name FROM people")?;
    ///     let rows = stmt2.query([])?;
    ///     let count2 = rows.as_ref().unwrap().column_count();
    ///
    ///     Ok(count)
    /// }
    /// ```
    ///
    /// # Caveats
    /// Panics if the query has not been [`execute`](Statement::execute)d yet.
    #[inline]
    pub fn column_count(&self) -> usize {
        self.stmt.column_count()
    }

    /// Check that column name reference lifetime is limited:
    /// https://www.sqlite.org/c3ref/column_name.html
    /// > The returned string pointer is valid...
    ///
    /// `column_name` reference can become invalid if `stmt` is reprepared
    /// (because of schema change) when `query_row` is called. So we assert
    /// that a compilation error happens if this reference is kept alive:
    /// ```compile_fail
    /// use duckdb::{Connection, Result};
    /// fn main() -> Result<()> {
    ///     let db = Connection::open_in_memory()?;
    ///     let mut stmt = db.prepare("SELECT 1 as x")?;
    ///     let column_name = stmt.column_name(0)?;
    ///     let x = stmt.query_row([], |r| r.get::<_, i64>(0))?; // E0502
    ///     assert_eq!(1, x);
    ///     assert_eq!("x", column_name);
    ///     Ok(())
    /// }
    /// ```
    #[inline]
    pub(super) fn column_name_unwrap(&self, col: usize) -> &String {
        // Just panic if the bounds are wrong for now, we never call this
        // without checking first.
        self.column_name(col).expect("Column out of bounds")
    }

    /// Returns the name assigned to a particular column in the result set
    /// returned by the prepared statement.
    ///
    /// If associated DB schema can be altered concurrently, you should make
    /// sure that current statement has already been stepped once before
    /// calling this method.
    ///
    /// ## Failure
    ///
    /// Returns an `Error::InvalidColumnIndex` if `idx` is outside the valid
    /// column range for this row.
    ///
    /// # Caveats
    /// Panics if the query has not been [`execute`](Statement::execute)d yet
    /// or when column name is not valid UTF-8.
    #[inline]
    pub fn column_name(&self, col: usize) -> Result<&String> {
        self.stmt.column_name(col).ok_or(Error::InvalidColumnIndex(col))
    }

    /// Returns the column index in the result set for a given column name.
    ///
    /// If there is no AS clause then the name of the column is unspecified and
    /// may change from one release of DuckDB to the next.
    ///
    /// If associated DB schema can be altered concurrently, you should make
    /// sure that current statement has already been stepped once before
    /// calling this method.
    ///
    /// # Failure
    ///
    /// Will return an `Error::InvalidColumnName` when there is no column with
    /// the specified `name`.
    ///
    /// # Caveats
    /// Panics if the query has not been [`execute`](Statement::execute)d yet.
    #[inline]
    pub fn column_index(&self, name: &str) -> Result<usize> {
        let n = self.column_count();
        for i in 0..n {
            // Note: `column_name` is only fallible if `i` is out of bounds,
            // which we've already checked.
            if name.eq_ignore_ascii_case(self.stmt.column_name(i).unwrap()) {
                return Ok(i);
            }
        }
        Err(Error::InvalidColumnName(String::from(name)))
    }

    /// Returns the declared data type of the column.
    ///
    /// # Caveats
    /// Panics if the query has not been [`execute`](Statement::execute)d yet.
    #[inline]
    pub fn column_type(&self, idx: usize) -> DataType {
        self.stmt.column_type(idx)
    }

    /// Returns the declared logical data type of the column.
    pub fn column_logical_type(&self, idx: usize) -> LogicalTypeHandle {
        self.stmt.column_logical_type(idx)
    }
}

#[cfg(test)]
mod test {
    use crate::{Connection, Result};

    #[test]
    fn test_column_name_in_error() -> Result<()> {
        use crate::{types::Type, Error};
        let db = Connection::open_in_memory()?;
        db.execute_batch(
            "BEGIN;
             CREATE TABLE foo(x INTEGER, y TEXT);
             INSERT INTO foo VALUES(4, NULL);
             END;",
        )?;
        let mut stmt = db.prepare("SELECT x as renamed, y FROM foo")?;
        let mut rows = stmt.query([])?;
        let row = rows.next()?.unwrap();
        match row.get::<_, String>(0).unwrap_err() {
            Error::InvalidColumnType(idx, name, ty) => {
                assert_eq!(idx, 0);
                assert_eq!(name, "renamed");
                assert_eq!(ty, Type::Int);
            }
            e => {
                panic!("Unexpected error type: {e:?}");
            }
        }
        match row.get::<_, String>("y").unwrap_err() {
            Error::InvalidColumnType(idx, name, ty) => {
                assert_eq!(idx, 1);
                assert_eq!(name, "y");
                assert_eq!(ty, Type::Null);
            }
            e => {
                panic!("Unexpected error type: {e:?}");
            }
        }
        Ok(())
    }
}

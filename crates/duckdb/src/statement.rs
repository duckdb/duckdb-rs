use std::{convert, ffi::c_void, fmt, mem, os::raw::c_char, ptr, str};

use arrow::{
    array::StructArray,
    datatypes::{DataType, SchemaRef},
};

use super::{AndThenRows, Connection, Error, MappedRows, Params, RawStatement, Result, Row, Rows, ValueRef, ffi};
#[cfg(feature = "polars")]
use crate::polars_dataframe::Polars;
use crate::{
    arrow_batch::{Arrow, ArrowStream},
    error::result_from_duckdb_prepare,
    types::{ToSql, Value},
};
#[cfg(feature = "polars")]
use polars_core::utils::arrow as polars_arrow;

/// A prepared statement.
///
/// # Thread Safety
///
/// `Statement` is neither `Send` nor `Sync`:
/// - Not `Send` because it holds a reference to `Connection`, which is `!Sync`
/// - Not `Sync` because DuckDB prepared statements don't support concurrent access
///
/// See the [DuckDB concurrency documentation](https://duckdb.org/docs/stable/connect/concurrency.html) for more details.
pub struct Statement<'conn> {
    conn: &'conn Connection,
    pub(crate) stmt: RawStatement,
}

impl Statement<'_> {
    /// Execute the prepared statement.
    ///
    /// On success, returns the number of rows that were changed or inserted or
    /// deleted.
    ///
    /// ## Example
    ///
    /// ### Use with positional parameters
    ///
    /// ```rust,no_run
    /// # use duckdb::{Connection, Result, params};
    /// fn update_rows(conn: &Connection) -> Result<()> {
    ///     let mut stmt = conn.prepare("UPDATE foo SET bar = 'baz' WHERE qux = ?")?;
    ///     // The `duckdb::params!` macro is mostly useful when the parameters do not
    ///     // all have the same type, or if there are more than 32 parameters
    ///     // at once.
    ///     stmt.execute(params![1i32])?;
    ///     // However, it's not required, many cases are fine as:
    ///     stmt.execute(&[&2i32])?;
    ///     // Or even:
    ///     stmt.execute([2i32])?;
    ///     Ok(())
    /// }
    /// ```
    ///
    /// ### Use without parameters
    ///
    /// ```rust,no_run
    /// # use duckdb::{Connection, Result, params};
    /// fn delete_all(conn: &Connection) -> Result<()> {
    ///     let mut stmt = conn.prepare("DELETE FROM users")?;
    ///     stmt.execute([])?;
    ///     Ok(())
    /// }
    /// ```
    ///
    /// # Failure
    ///
    /// Will return `Err` if binding parameters fails, the executed statement
    /// returns rows (in which case `query` should be used instead), or the
    /// underlying DuckDB call fails.
    #[inline]
    pub fn execute<P: Params>(&mut self, params: P) -> Result<usize> {
        params.__bind_in(self)?;
        self.execute_with_bound_parameters()
    }

    /// Execute an INSERT.
    ///
    /// # Note
    ///
    /// This function is a convenience wrapper around
    /// [`execute()`](Statement::execute) intended for queries that insert a
    /// single item. It is possible to misuse this function in a way that it
    /// cannot detect, such as by calling it on a statement which _updates_
    /// a single item rather than inserting one. Please don't do that.
    ///
    /// # Failure
    ///
    /// Will return `Err` if no row is inserted or many rows are inserted.
    #[inline]
    pub fn insert<P: Params>(&mut self, params: P) -> Result<()> {
        let changes = self.execute(params)?;
        match changes {
            1 => Ok(()),
            _ => Err(Error::StatementChangedRows(changes)),
        }
    }

    /// Execute the prepared statement, returning a handle to the resulting
    /// vector of arrow RecordBatch
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// # use duckdb::{Result, Connection};
    /// # use arrow::record_batch::RecordBatch;
    /// fn get_arrow_data(conn: &Connection) -> Result<Vec<RecordBatch>> {
    ///     Ok(conn.prepare("SELECT * FROM test")?.query_arrow([])?.collect())
    /// }
    /// ```
    ///
    /// # Failure
    ///
    /// Will return `Err` if binding parameters fails.
    #[inline]
    pub fn query_arrow<P: Params>(&mut self, params: P) -> Result<Arrow<'_>> {
        self.execute(params)?;
        Ok(Arrow::new(self))
    }

    /// Execute the prepared statement, returning a handle to the resulting
    /// vector of arrow RecordBatch in streaming way
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// # use duckdb::{Result, Connection};
    /// # use arrow::record_batch::RecordBatch;
    /// # use arrow::datatypes::SchemaRef;
    /// fn get_arrow_data(conn: &Connection, schema: SchemaRef) -> Result<Vec<RecordBatch>> {
    ///     Ok(conn.prepare("SELECT * FROM test")?.stream_arrow([], schema)?.collect())
    /// }
    /// ```
    ///
    /// # Failure
    ///
    /// Will return `Err` if binding parameters fails.
    #[inline]
    pub fn stream_arrow<P: Params>(&mut self, params: P, schema: SchemaRef) -> Result<ArrowStream<'_>> {
        params.__bind_in(self)?;
        self.stmt.execute_streaming()?;
        Ok(ArrowStream::new(self, schema))
    }

    /// Execute the prepared statement, returning a handle to the resulting
    /// vector of polars DataFrame.
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// # use duckdb::{Result, Connection};
    /// # use polars::prelude::DataFrame;
    ///
    /// fn get_polars_dfs(conn: &Connection) -> Result<Vec<DataFrame>> {
    ///     let dfs: Vec<DataFrame> = conn
    ///         .prepare("SELECT * FROM test")?
    ///         .query_polars([])?
    ///         .collect();
    ///
    ///     Ok(dfs)
    /// }
    /// ```
    ///
    /// To derive a DataFrame from Vec\<DataFrame>, we can use function
    /// [polars_core::utils::accumulate_dataframes_vertical_unchecked](https://docs.rs/polars-core/latest/polars_core/utils/fn.accumulate_dataframes_vertical_unchecked.html).
    ///
    /// ```rust,no_run
    /// # use duckdb::{Result, Connection};
    /// # use polars::prelude::DataFrame;
    /// # use polars_core::utils::accumulate_dataframes_vertical_unchecked;
    ///
    /// fn get_polars_df(conn: &Connection) -> Result<DataFrame> {
    ///     let mut stmt = conn.prepare("SELECT * FROM test")?;
    ///     let pl = stmt.query_polars([])?;
    ///     let df = accumulate_dataframes_vertical_unchecked(pl);
    ///
    ///    Ok(df)
    /// }
    /// ```
    ///
    ///
    #[cfg(feature = "polars")]
    #[inline]
    pub fn query_polars<P: Params>(&mut self, params: P) -> Result<Polars<'_>> {
        self.execute(params)?;
        Ok(Polars::new(self))
    }

    /// Execute the prepared statement, returning a handle to the resulting
    /// rows.
    ///
    /// Due to lifetime restricts, the rows handle returned by `query` does not
    /// implement the `Iterator` trait. Consider using
    /// [`query_map`](Statement::query_map) or
    /// [`query_and_then`](Statement::query_and_then) instead, which do.
    ///
    /// ## Example
    ///
    /// ### Use without parameters
    ///
    /// ```rust,no_run
    /// # use duckdb::{Connection, Result};
    /// fn get_names(conn: &Connection) -> Result<Vec<String>> {
    ///     let mut stmt = conn.prepare("SELECT name FROM people")?;
    ///     let mut rows = stmt.query([])?;
    ///
    ///     let mut names = Vec::new();
    ///     while let Some(row) = rows.next()? {
    ///         names.push(row.get(0)?);
    ///     }
    ///
    ///     Ok(names)
    /// }
    /// ```
    ///
    /// ### Use with positional parameters
    ///
    /// ```rust,no_run
    /// # use duckdb::{Connection, Result};
    /// fn query(conn: &Connection, name: &str) -> Result<()> {
    ///     let mut stmt = conn.prepare("SELECT * FROM test where name = ?")?;
    ///     let mut rows = stmt.query(duckdb::params![name])?;
    ///     while let Some(row) = rows.next()? {
    ///         // ...
    ///     }
    ///     Ok(())
    /// }
    /// ```
    ///
    /// Or, equivalently (but without the [`params!`] macro).
    ///
    /// ```rust,no_run
    /// # use duckdb::{Connection, Result};
    /// fn query(conn: &Connection, name: &str) -> Result<()> {
    ///     let mut stmt = conn.prepare("SELECT * FROM test where name = ?")?;
    ///     let mut rows = stmt.query([name])?;
    ///     while let Some(row) = rows.next()? {
    ///         // ...
    ///     }
    ///     Ok(())
    /// }
    /// ```
    ///
    /// ## Failure
    ///
    /// Will return `Err` if binding parameters fails.
    #[inline]
    pub fn query<P: Params>(&mut self, params: P) -> Result<Rows<'_>> {
        self.execute(params)?;
        Ok(Rows::new(self))
    }

    /// Executes the prepared statement and maps a function over the resulting
    /// rows, returning an iterator over the mapped function results.
    ///
    /// `f` is used to transform the _streaming_ iterator into a _standard_
    /// iterator.
    ///
    /// This is equivalent to `stmt.query(params)?.mapped(f)`.
    ///
    /// ## Example
    ///
    /// ### Use with positional params
    ///
    /// ```rust,no_run
    /// # use duckdb::{Connection, Result};
    /// fn get_names(conn: &Connection) -> Result<Vec<String>> {
    ///     let mut stmt = conn.prepare("SELECT name FROM people")?;
    ///     let rows = stmt.query_map([], |row| row.get(0))?;
    ///
    ///     let mut names = Vec::new();
    ///     for name_result in rows {
    ///         names.push(name_result?);
    ///     }
    ///
    ///     Ok(names)
    /// }
    /// ```
    ///
    /// ## Failure
    ///
    /// Will return `Err` if binding parameters fails.
    pub fn query_map<T, P, F>(&mut self, params: P, f: F) -> Result<MappedRows<'_, F>>
    where
        P: Params,
        F: FnMut(&Row<'_>) -> Result<T>,
    {
        self.query(params).map(|rows| rows.mapped(f))
    }

    /// Executes the prepared statement and maps a function over the resulting
    /// rows, where the function returns a `Result` with `Error` type
    /// implementing `std::convert::From<Error>` (so errors can be unified).
    ///
    /// This is equivalent to `stmt.query(params)?.and_then(f)`.
    ///
    /// ## Example
    ///
    /// ### Use with positional params
    ///
    /// ```no_run
    /// # use duckdb::{Connection, Result};
    /// fn get_names(conn: &Connection) -> Result<Vec<String>> {
    ///     let mut stmt = conn.prepare("SELECT name FROM people WHERE id = ?")?;
    ///     let rows = stmt.query_and_then(["one"], |row| row.get::<_, String>(0))?;
    ///
    ///     let mut persons = Vec::new();
    ///     for person_result in rows {
    ///         persons.push(person_result?);
    ///     }
    ///
    ///     Ok(persons)
    /// }
    /// ```
    ///
    /// # Failure
    ///
    /// Will return `Err` if binding parameters fails.
    #[inline]
    pub fn query_and_then<T, E, P, F>(&mut self, params: P, f: F) -> Result<AndThenRows<'_, F>>
    where
        P: Params,
        E: convert::From<Error>,
        F: FnMut(&Row<'_>) -> Result<T, E>,
    {
        self.query(params).map(|rows| rows.and_then(f))
    }

    /// Return `true` if a query in the SQL statement it executes returns one
    /// or more rows and `false` if the SQL returns an empty set.
    #[inline]
    pub fn exists<P: Params>(&mut self, params: P) -> Result<bool> {
        let mut rows = self.query(params)?;
        let exists = rows.next()?.is_some();
        Ok(exists)
    }

    /// Convenience method to execute a query that is expected to return a
    /// single row.
    ///
    /// If the query returns more than one row, all rows except the first are
    /// ignored.
    ///
    /// Returns `Err(QueryReturnedNoRows)` if no results are returned. If the
    /// query truly is optional, you can call
    /// [`.optional()`](crate::OptionalExt::optional) on the result of
    /// this to get a `Result<Option<T>>` (requires that the trait
    /// `duckdb::OptionalExt` is imported).
    ///
    /// # Failure
    ///
    /// Will return `Err` if the underlying DuckDB call fails.
    pub fn query_row<T, P, F>(&mut self, params: P, f: F) -> Result<T>
    where
        P: Params,
        F: FnOnce(&Row<'_>) -> Result<T>,
    {
        self.query(params)?.get_expected_row().and_then(f)
    }

    /// Convenience method to execute a query that is expected to return exactly
    /// one row.
    ///
    /// Returns `Err(QueryReturnedMoreThanOneRow)` if the query returns more than one row.
    ///
    /// Returns `Err(QueryReturnedNoRows)` if no results are returned. If the
    /// query truly is optional, you can call
    /// [`.optional()`](crate::OptionalExt::optional) on the result of
    /// this to get a `Result<Option<T>>` (requires that the trait
    /// `duckdb::OptionalExt` is imported).
    ///
    /// # Failure
    ///
    /// Will return `Err` if the underlying DuckDB call fails.
    pub fn query_one<T, P, F>(&mut self, params: P, f: F) -> Result<T>
    where
        P: Params,
        F: FnOnce(&Row<'_>) -> Result<T>,
    {
        let mut rows = self.query(params)?;
        let row = rows.get_expected_row().and_then(f)?;
        if rows.next()?.is_some() {
            return Err(Error::QueryReturnedMoreThanOneRow);
        }
        Ok(row)
    }

    /// Return the row count
    #[inline]
    pub fn row_count(&self) -> usize {
        self.stmt.row_count()
    }

    /// Get next batch records in arrow-rs
    #[inline]
    pub fn step(&self) -> Option<StructArray> {
        self.stmt.step()
    }

    /// Get next batch records in arrow-rs in a streaming way
    #[inline]
    pub fn stream_step(&self, schema: SchemaRef) -> Option<StructArray> {
        self.stmt.streaming_step(schema)
    }

    #[cfg(feature = "polars")]
    #[inline]
    pub(crate) fn step_polars(&self) -> Option<polars_arrow::array::StructArray> {
        self.stmt.step_polars()
    }

    #[inline]
    pub(crate) fn bind_parameters<P>(&mut self, params: P) -> Result<()>
    where
        P: IntoIterator,
        P::Item: ToSql,
    {
        let result = self.try_bind_parameters(params);
        if result.is_err() {
            let _ = self.stmt.clear_bindings();
        }
        result
    }

    fn try_bind_parameters<P>(&mut self, params: P) -> Result<()>
    where
        P: IntoIterator,
        P::Item: ToSql,
    {
        let expected = self.stmt.bind_parameter_count();
        let mut index = 0;
        for p in params.into_iter() {
            index += 1; // The leftmost SQL parameter has an index of 1.
            if index > expected {
                break;
            }
            self.bind_parameter(&p, index)?;
        }
        if index != expected {
            Err(Error::InvalidParameterCount(index, expected))
        } else {
            Ok(())
        }
    }

    /// Return the number of parameters that can be bound to this statement.
    #[inline]
    pub fn parameter_count(&self) -> usize {
        self.stmt.bind_parameter_count()
    }

    /// Returns the name of the parameter at the given index.
    ///
    /// This can be used to query the names of named parameters (e.g., `$param_name`)
    /// in a prepared statement.
    ///
    /// # Arguments
    ///
    /// * `one_based_col_index` - One-based parameter index (1 to [`Statement::parameter_count`])
    ///
    /// # Returns
    ///
    /// * `Ok(String)` - The parameter name (without the `$` prefix for named params, or the numeric index for positional params)
    /// * `Err(InvalidParameterIndex)` - If the index is out of range
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use duckdb::{Connection, Result};
    /// fn query_parameter_names(conn: &Connection) -> Result<()> {
    ///     let stmt = conn.prepare("SELECT $foo, $bar")?;
    ///
    ///     assert_eq!(stmt.parameter_count(), 2);
    ///     assert_eq!(stmt.parameter_name(1)?, "foo");
    ///     assert_eq!(stmt.parameter_name(2)?, "bar");
    ///
    ///     Ok(())
    /// }
    /// ```
    #[inline]
    pub fn parameter_name(&self, idx: usize) -> Result<String> {
        self.stmt.parameter_name(idx)
    }

    /// Low level API to directly bind a parameter to a given index.
    ///
    /// Note that the index is one-based, that is, the first parameter index is
    /// 1 and not 0. This is consistent with the DuckDB API and the values given
    /// to parameters bound as `?NNN`.
    ///
    /// The valid values for `one_based_col_index` begin at `1`, and end at
    /// [`Statement::parameter_count`], inclusive.
    ///
    /// # Caveats
    ///
    /// This should not generally be used, but is available for special cases
    /// such as:
    ///
    /// - binding parameters where a gap exists.
    /// - binding named and positional parameters in the same query.
    /// - separating parameter binding from query execution.
    ///
    /// Statements that have had their parameters bound this way should be
    /// queried or executed by [`Statement::raw_query`] or
    /// [`Statement::raw_execute`]. Other functions are not guaranteed to work.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use duckdb::{Connection, Result};
    /// fn query(conn: &Connection) -> Result<()> {
    ///     let mut stmt = conn.prepare("SELECT * FROM test WHERE name = ? AND value > ?2")?;
    ///     stmt.raw_bind_parameter(1, "foo")?;
    ///     stmt.raw_bind_parameter(2, 100)?;
    ///     let mut rows = stmt.raw_query();
    ///     while let Some(row) = rows.next()? {
    ///         // ...
    ///     }
    ///     Ok(())
    /// }
    /// ```
    #[inline]
    pub fn raw_bind_parameter<T: ToSql>(&mut self, one_based_col_index: usize, param: T) -> Result<()> {
        // This is the same as `bind_parameter` but slightly more ergonomic and
        // correctly takes `&mut self`.
        self.bind_parameter(&param, one_based_col_index)
    }

    /// Low level API to execute a statement given that all parameters were
    /// bound explicitly with the [`Statement::raw_bind_parameter`] API.
    ///
    /// # Caveats
    ///
    /// Any unbound parameters will have `NULL` as their value.
    ///
    /// This should not generally be used outside of special cases, and
    /// functions in the [`Statement::execute`] family should be preferred.
    ///
    /// # Failure
    ///
    /// Will return `Err` if the executed statement returns rows (in which case
    /// `query` should be used instead), or the underlying DuckDB call fails.
    #[inline]
    pub fn raw_execute(&mut self) -> Result<usize> {
        self.execute_with_bound_parameters()
    }

    /// Low level API to get `Rows` for this query given that all parameters
    /// were bound explicitly with the [`Statement::raw_bind_parameter`] API.
    ///
    /// # Caveats
    ///
    /// Any unbound parameters will have `NULL` as their value.
    ///
    /// This should not generally be used outside of special cases, and
    /// functions in the [`Statement::query`] family should be preferred.
    ///
    /// Note that if the SQL does not return results, [`Statement::raw_execute`]
    /// should be used instead.
    #[inline]
    pub fn raw_query(&self) -> Rows<'_> {
        Rows::new(self)
    }

    /// Returns the underlying schema of the prepared statement.
    ///
    /// # Caveats
    /// Panics if the query has not been [`execute`](Statement::execute)d yet.
    #[inline]
    pub fn schema(&self) -> SchemaRef {
        self.stmt.schema()
    }

    // generic because many of these branches can constant fold away.
    fn bind_parameter<P: ?Sized + ToSql>(&self, param: &P, col: usize) -> Result<()> {
        let value = param.to_sql()?;

        let ptr = unsafe { self.stmt.ptr() };

        let value_ref = match value.as_value_ref() {
            Ok(r) => r,
            Err(value) => unsafe {
                // Container types (List, Struct, etc.) are not convertible to ValueRef.
                // Instead, bind them via duckdb_value C API.
                let duckdb_val = value_to_duckdb(value)?;
                let rc = ffi::duckdb_bind_value(ptr, col as u64, duckdb_val);
                ffi::duckdb_destroy_value(&mut { duckdb_val });
                return result_from_duckdb_prepare(rc, ptr);
            },
        };
        // TODO: bind more
        let rc = match value_ref {
            ValueRef::Null => unsafe { ffi::duckdb_bind_null(ptr, col as u64) },
            ValueRef::Boolean(i) => unsafe { ffi::duckdb_bind_boolean(ptr, col as u64, i) },
            ValueRef::TinyInt(i) => unsafe { ffi::duckdb_bind_int8(ptr, col as u64, i) },
            ValueRef::SmallInt(i) => unsafe { ffi::duckdb_bind_int16(ptr, col as u64, i) },
            ValueRef::Int(i) => unsafe { ffi::duckdb_bind_int32(ptr, col as u64, i) },
            ValueRef::BigInt(i) => unsafe { ffi::duckdb_bind_int64(ptr, col as u64, i) },
            ValueRef::HugeInt(i) => unsafe {
                let hi = ffi::duckdb_hugeint {
                    lower: i as u64,
                    upper: (i >> 64) as i64,
                };
                ffi::duckdb_bind_hugeint(ptr, col as u64, hi)
            },
            ValueRef::UTinyInt(i) => unsafe { ffi::duckdb_bind_uint8(ptr, col as u64, i) },
            ValueRef::USmallInt(i) => unsafe { ffi::duckdb_bind_uint16(ptr, col as u64, i) },
            ValueRef::UInt(i) => unsafe { ffi::duckdb_bind_uint32(ptr, col as u64, i) },
            ValueRef::UBigInt(i) => unsafe { ffi::duckdb_bind_uint64(ptr, col as u64, i) },
            ValueRef::Float(r) => unsafe { ffi::duckdb_bind_float(ptr, col as u64, r) },
            ValueRef::Double(r) => unsafe { ffi::duckdb_bind_double(ptr, col as u64, r) },
            ValueRef::Text(s) => unsafe {
                ffi::duckdb_bind_varchar_length(ptr, col as u64, s.as_ptr() as *const c_char, s.len() as u64)
            },
            ValueRef::Blob(b) => unsafe {
                ffi::duckdb_bind_blob(ptr, col as u64, b.as_ptr() as *const c_void, b.len() as u64)
            },
            ValueRef::Timestamp(u, i) => unsafe {
                ffi::duckdb_bind_timestamp(ptr, col as u64, ffi::duckdb_timestamp { micros: u.to_micros(i) })
            },
            ValueRef::Interval { months, days, nanos } => unsafe {
                let micros = nanos / 1_000;
                ffi::duckdb_bind_interval(ptr, col as u64, ffi::duckdb_interval { months, days, micros })
            },
            ValueRef::Date32(days) => unsafe { ffi::duckdb_bind_date(ptr, col as u64, ffi::duckdb_date { days }) },
            ValueRef::Time64(u, i) => unsafe {
                ffi::duckdb_bind_time(ptr, col as u64, ffi::duckdb_time { micros: u.to_micros(i) })
            },
            ValueRef::Decimal(d) => unsafe {
                let decimal = crate::types::to_duckdb_decimal(d);
                ffi::duckdb_bind_decimal(ptr, col as u64, decimal)
            },
            ValueRef::List(..)
            | ValueRef::Struct(..)
            | ValueRef::Map(..)
            | ValueRef::Array(..)
            | ValueRef::Union(..)
            | ValueRef::Enum(..) => unsafe {
                let mut duckdb_val = value_ref_to_duckdb(value_ref)?;
                let rc = ffi::duckdb_bind_value(ptr, col as u64, duckdb_val);
                ffi::duckdb_destroy_value(&mut duckdb_val);
                return result_from_duckdb_prepare(rc, ptr);
            },
        };
        result_from_duckdb_prepare(rc, ptr)
    }

    #[inline]
    fn execute_with_bound_parameters(&mut self) -> Result<usize> {
        self.stmt.execute()
    }

    /// Safety: This is unsafe, because using `sqlite3_stmt` after the
    /// connection has closed is illegal, but `RawStatement` does not enforce
    /// this, as it loses our protective `'conn` lifetime bound.
    #[inline]
    pub(crate) unsafe fn into_raw(mut self) -> RawStatement {
        let mut stmt = unsafe { RawStatement::new(ptr::null_mut()) };
        mem::swap(&mut stmt, &mut self.stmt);
        stmt
    }
}

fn binding_unsupported_value(type_name: &'static str) -> Error {
    Error::ToSqlConversionFailure(format!("binding {type_name} parameters is not yet supported").into())
}

impl fmt::Debug for Statement<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let sql = if self.stmt.is_null() {
            Ok("")
        } else {
            str::from_utf8(self.stmt.sql().unwrap().to_bytes())
        };
        f.debug_struct("Statement")
            .field("conn", self.conn)
            .field("stmt", &self.stmt)
            .field("sql", &sql)
            .finish()
    }
}

impl Statement<'_> {
    #[inline]
    pub(super) fn new(conn: &Connection, stmt: RawStatement) -> Statement<'_> {
        Statement { conn, stmt }
    }
}

/// Convert a `Value` to a `duckdb_value`. The caller must destroy the returned value.
pub(crate) fn value_to_duckdb(value: &Value) -> Result<ffi::duckdb_value> {
    Ok(match value {
        Value::Null => unsafe { ffi::duckdb_create_null_value() },
        Value::Boolean(b) => unsafe { ffi::duckdb_create_bool(*b) },
        Value::TinyInt(i) => unsafe { ffi::duckdb_create_int8(*i) },
        Value::SmallInt(i) => unsafe { ffi::duckdb_create_int16(*i) },
        Value::Int(i) => unsafe { ffi::duckdb_create_int32(*i) },
        Value::BigInt(i) => unsafe { ffi::duckdb_create_int64(*i) },
        Value::HugeInt(i) => unsafe {
            ffi::duckdb_create_hugeint(ffi::duckdb_hugeint {
                lower: *i as u64,
                upper: (*i >> 64) as i64,
            })
        },
        Value::UTinyInt(i) => unsafe { ffi::duckdb_create_uint8(*i) },
        Value::USmallInt(i) => unsafe { ffi::duckdb_create_uint16(*i) },
        Value::UInt(i) => unsafe { ffi::duckdb_create_uint32(*i) },
        Value::UBigInt(i) => unsafe { ffi::duckdb_create_uint64(*i) },
        Value::Float(f) => unsafe { ffi::duckdb_create_float(*f) },
        Value::Double(f) => unsafe { ffi::duckdb_create_double(*f) },
        Value::Decimal(d) => unsafe {
            let decimal = crate::types::to_duckdb_decimal(*d);
            ffi::duckdb_create_decimal(decimal)
        },
        Value::Timestamp(u, i) => unsafe {
            ffi::duckdb_create_timestamp(ffi::duckdb_timestamp {
                micros: u.to_micros(*i),
            })
        },
        Value::Text(s) => unsafe { ffi::duckdb_create_varchar_length(s.as_ptr() as *const c_char, s.len() as u64) },
        Value::Blob(b) => unsafe { ffi::duckdb_create_blob(b.as_ptr(), b.len() as u64) },
        Value::Date32(days) => unsafe { ffi::duckdb_create_date(ffi::duckdb_date { days: *days }) },
        Value::Time64(u, i) => unsafe {
            ffi::duckdb_create_time(ffi::duckdb_time {
                micros: u.to_micros(*i),
            })
        },
        Value::Interval { months, days, nanos } => unsafe {
            ffi::duckdb_create_interval(ffi::duckdb_interval {
                months: *months,
                days: *days,
                micros: *nanos / 1_000,
            })
        },
        Value::List(items) => unsafe {
            let child_type = infer_list_child_logical_type(items)?;
            let mut duckdb_values: Vec<ffi::duckdb_value> = Vec::with_capacity(items.len());
            // Track how many we've created for cleanup on error
            let result = (|| -> Result<ffi::duckdb_value> {
                for item in items {
                    duckdb_values.push(value_to_duckdb(item)?);
                }
                let list_val =
                    ffi::duckdb_create_list_value(child_type, duckdb_values.as_mut_ptr(), items.len() as u64);
                Ok(list_val)
            })();
            // Cleanup: destroy child values and logical type
            for mut v in duckdb_values {
                ffi::duckdb_destroy_value(&mut v);
            }
            ffi::duckdb_destroy_logical_type(&mut { child_type });
            return result;
        },
        Value::Enum(_) | Value::Struct(_) | Value::Map(_) | Value::Array(_) | Value::Union(_) => {
            return Err(binding_unsupported_value(value.data_type_name()));
        }
    })
}

/// Infer the child logical type of a list from its elements.
/// Uses the first non-null element. Errors on empty or all-null lists.
fn infer_list_child_logical_type(items: &[Value]) -> Result<ffi::duckdb_logical_type> {
    for item in items {
        if !matches!(item, Value::Null) {
            return value_to_logical_type(item);
        }
    }
    Err(Error::ToSqlConversionFailure(
        "cannot infer element type of empty or all-null list".into(),
    ))
}

/// Convert a `Value` to its DuckDB logical type.
fn value_to_logical_type(value: &Value) -> Result<ffi::duckdb_logical_type> {
    use crate::core::LogicalTypeId;
    let id = match value {
        Value::Null => {
            return Err(Error::ToSqlConversionFailure(
                "cannot infer logical type from Value::Null".into(),
            ));
        }
        Value::Boolean(_) => LogicalTypeId::Boolean,
        Value::TinyInt(_) => LogicalTypeId::Tinyint,
        Value::SmallInt(_) => LogicalTypeId::Smallint,
        Value::Int(_) => LogicalTypeId::Integer,
        Value::BigInt(_) => LogicalTypeId::Bigint,
        Value::HugeInt(_) => LogicalTypeId::Hugeint,
        Value::UTinyInt(_) => LogicalTypeId::UTinyint,
        Value::USmallInt(_) => LogicalTypeId::USmallint,
        Value::UInt(_) => LogicalTypeId::UInteger,
        Value::UBigInt(_) => LogicalTypeId::UBigint,
        Value::Float(_) => LogicalTypeId::Float,
        Value::Double(_) => LogicalTypeId::Double,
        Value::Decimal(_) => LogicalTypeId::Decimal,
        Value::Timestamp(_, _) => LogicalTypeId::Timestamp,
        Value::Text(_) => LogicalTypeId::Varchar,
        Value::Blob(_) => LogicalTypeId::Blob,
        Value::Date32(_) => LogicalTypeId::Date,
        Value::Time64(_, _) => LogicalTypeId::Time,
        Value::Interval { .. } => LogicalTypeId::Interval,
        Value::Enum(_) => LogicalTypeId::Enum,
        Value::List(items) => unsafe {
            let child_type = infer_list_child_logical_type(items)?;
            let list_type = ffi::duckdb_create_list_type(child_type);
            ffi::duckdb_destroy_logical_type(&mut { child_type });
            return Ok(list_type);
        },
        Value::Struct(_) | Value::Map(_) | Value::Array(_) | Value::Union(_) => {
            return Err(binding_unsupported_value(value.data_type_name()));
        }
    };
    Ok(unsafe { ffi::duckdb_create_logical_type(id as u32) })
}

/// Convert a `ValueRef` to a `duckdb_value`. The caller must destroy the returned value.
pub(crate) fn value_ref_to_duckdb(value: ValueRef<'_>) -> Result<ffi::duckdb_value> {
    use crate::types::ListType;
    match value {
        ValueRef::Null => Ok(unsafe { ffi::duckdb_create_null_value() }),
        ValueRef::Boolean(b) => Ok(unsafe { ffi::duckdb_create_bool(b) }),
        ValueRef::TinyInt(i) => Ok(unsafe { ffi::duckdb_create_int8(i) }),
        ValueRef::SmallInt(i) => Ok(unsafe { ffi::duckdb_create_int16(i) }),
        ValueRef::Int(i) => Ok(unsafe { ffi::duckdb_create_int32(i) }),
        ValueRef::BigInt(i) => Ok(unsafe { ffi::duckdb_create_int64(i) }),
        ValueRef::HugeInt(i) => Ok(unsafe {
            ffi::duckdb_create_hugeint(ffi::duckdb_hugeint {
                lower: i as u64,
                upper: (i >> 64) as i64,
            })
        }),
        ValueRef::UTinyInt(i) => Ok(unsafe { ffi::duckdb_create_uint8(i) }),
        ValueRef::USmallInt(i) => Ok(unsafe { ffi::duckdb_create_uint16(i) }),
        ValueRef::UInt(i) => Ok(unsafe { ffi::duckdb_create_uint32(i) }),
        ValueRef::UBigInt(i) => Ok(unsafe { ffi::duckdb_create_uint64(i) }),
        ValueRef::Float(f) => Ok(unsafe { ffi::duckdb_create_float(f) }),
        ValueRef::Double(f) => Ok(unsafe { ffi::duckdb_create_double(f) }),
        ValueRef::Decimal(d) => Ok(unsafe { ffi::duckdb_create_decimal(crate::types::to_duckdb_decimal(d)) }),
        ValueRef::Timestamp(u, i) => {
            Ok(unsafe { ffi::duckdb_create_timestamp(ffi::duckdb_timestamp { micros: u.to_micros(i) }) })
        }
        ValueRef::Text(s) => {
            Ok(unsafe { ffi::duckdb_create_varchar_length(s.as_ptr() as *const c_char, s.len() as u64) })
        }
        ValueRef::Blob(b) => Ok(unsafe { ffi::duckdb_create_blob(b.as_ptr(), b.len() as u64) }),
        ValueRef::Date32(days) => Ok(unsafe { ffi::duckdb_create_date(ffi::duckdb_date { days }) }),
        ValueRef::Time64(u, i) => Ok(unsafe { ffi::duckdb_create_time(ffi::duckdb_time { micros: u.to_micros(i) }) }),
        ValueRef::Interval { months, days, nanos } => Ok(unsafe {
            ffi::duckdb_create_interval(ffi::duckdb_interval {
                months,
                days,
                micros: nanos / 1_000,
            })
        }),
        ValueRef::List(list_type, idx) => unsafe {
            let (start, end, values, child_dt) = match list_type {
                ListType::Regular(arr) => {
                    let offsets = arr.offsets();
                    let s: usize = offsets[idx].try_into().unwrap();
                    let e: usize = offsets[idx + 1].try_into().unwrap();
                    (s, e, arr.values(), arr.value_type())
                }
                ListType::Large(arr) => {
                    let offsets = arr.offsets();
                    let s: usize = offsets[idx].try_into().unwrap();
                    let e: usize = offsets[idx + 1].try_into().unwrap();
                    (s, e, arr.values(), arr.value_type())
                }
            };
            let child_logical = arrow_datatype_to_logical_type(&child_dt)?;
            let mut duckdb_values: Vec<ffi::duckdb_value> = Vec::with_capacity(end - start);
            let result = (|| -> Result<ffi::duckdb_value> {
                for row in start..end {
                    let elem = Row::value_ref_internal(row, idx, values);
                    duckdb_values.push(value_ref_to_duckdb(elem)?);
                }
                Ok(ffi::duckdb_create_list_value(
                    child_logical,
                    duckdb_values.as_mut_ptr(),
                    duckdb_values.len() as u64,
                ))
            })();
            for mut v in duckdb_values {
                ffi::duckdb_destroy_value(&mut v);
            }
            ffi::duckdb_destroy_logical_type(&mut { child_logical });
            result
        },
        _ => Err(binding_unsupported_value(value.data_type().name())),
    }
}

/// Convert an Arrow `DataType` to a DuckDB logical type.
fn arrow_datatype_to_logical_type(dt: &DataType) -> Result<ffi::duckdb_logical_type> {
    use crate::core::LogicalTypeId;
    let id = match dt {
        DataType::Boolean => LogicalTypeId::Boolean,
        DataType::Int8 => LogicalTypeId::Tinyint,
        DataType::Int16 => LogicalTypeId::Smallint,
        DataType::Int32 => LogicalTypeId::Integer,
        DataType::Int64 => LogicalTypeId::Bigint,
        DataType::UInt8 => LogicalTypeId::UTinyint,
        DataType::UInt16 => LogicalTypeId::USmallint,
        DataType::UInt32 => LogicalTypeId::UInteger,
        DataType::UInt64 => LogicalTypeId::UBigint,
        DataType::Float32 => LogicalTypeId::Float,
        DataType::Float64 => LogicalTypeId::Double,
        DataType::Utf8 | DataType::LargeUtf8 => LogicalTypeId::Varchar,
        DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_) => LogicalTypeId::Blob,
        DataType::Date32 => LogicalTypeId::Date,
        DataType::Timestamp(_, _) => LogicalTypeId::Timestamp,
        DataType::Time64(_) => LogicalTypeId::Time,
        DataType::Interval(_) => LogicalTypeId::Interval,
        DataType::Decimal128(_, _) => LogicalTypeId::Decimal,
        DataType::List(field) | DataType::LargeList(field) => unsafe {
            let child = arrow_datatype_to_logical_type(field.data_type())?;
            let list_type = ffi::duckdb_create_list_type(child);
            ffi::duckdb_destroy_logical_type(&mut { child });
            return Ok(list_type);
        },
        _ => {
            return Err(Error::ToSqlConversionFailure(
                format!("unsupported Arrow DataType for binding: {dt}").into(),
            ));
        }
    };
    Ok(unsafe { ffi::duckdb_create_logical_type(id as u32) })
}

#[cfg(test)]
mod test {
    use arrow::{array::ListArray, datatypes::Int32Type};

    use crate::{
        Connection, Error, Result, params_from_iter,
        types::{ListType, ToSql, ToSqlOutput, ValueRef},
    };
    use rust_decimal::Decimal;

    struct BorrowedList(ListArray);
    impl BorrowedList {
        fn new(value: Vec<Option<i32>>) -> Self {
            Self(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(value)]))
        }
    }
    impl ToSql for BorrowedList {
        fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
            Ok(ToSqlOutput::Borrowed(ValueRef::List(ListType::Regular(&self.0), 0)))
        }
    }

    #[test]
    fn test_execute() -> Result<()> {
        let db = Connection::open_in_memory()?;
        db.execute_batch("CREATE TABLE foo(x INTEGER)")?;

        assert_eq!(db.execute("INSERT INTO foo(x) VALUES (?)", [&2i32])?, 1);
        assert_eq!(db.execute("INSERT INTO foo(x) VALUES (?)", [&3i32])?, 1);

        // TODO(wangfenjin): No column type for SUM(x)?
        assert_eq!(
            5i32,
            db.query_row::<i32, _, _>("SELECT SUM(x) FROM foo WHERE x > ?", [&0i32], |r| r.get(0))?
        );
        assert_eq!(
            3i32,
            db.query_row::<i32, _, _>("SELECT SUM(x) FROM foo WHERE x > ?", [&2i32], |r| r.get(0))?
        );
        Ok(())
    }

    #[test]
    fn test_stmt_execute() -> Result<()> {
        let db = Connection::open_in_memory()?;
        let sql = r#"
        CREATE SEQUENCE seq;
        CREATE TABLE test (id INTEGER DEFAULT NEXTVAL('seq'), name TEXT NOT NULL, flag INTEGER);
        "#;
        db.execute_batch(sql)?;

        let mut stmt = db.prepare("INSERT INTO test (name) VALUES (?)")?;
        stmt.execute([&"one"])?;

        let mut stmt = db.prepare("SELECT COUNT(*) FROM test WHERE name = ?")?;
        assert_eq!(1i32, stmt.query_row::<i32, _, _>([&"one"], |r| r.get(0))?);
        Ok(())
    }

    #[test]
    fn test_query() -> Result<()> {
        let db = Connection::open_in_memory()?;
        let sql = r#"
        CREATE TABLE test (id INTEGER PRIMARY KEY NOT NULL, name TEXT NOT NULL, flag INTEGER);
        INSERT INTO test(id, name) VALUES (1, 'one');
        "#;
        db.execute_batch(sql)?;

        let mut stmt = db.prepare("SELECT id FROM test where name = ?")?;
        {
            let id: i32 = stmt.query_one([&"one"], |r| r.get(0))?;
            assert_eq!(id, 1);
        }
        Ok(())
    }

    #[test]
    fn test_query_and_then() -> Result<()> {
        let db = Connection::open_in_memory()?;
        let sql = r#"
        CREATE TABLE test (id INTEGER PRIMARY KEY NOT NULL, name TEXT NOT NULL, flag INTEGER);
        INSERT INTO test(id, name) VALUES (1, 'one');
        INSERT INTO test(id, name) VALUES (2, 'one');
        "#;
        db.execute_batch(sql)?;

        let mut stmt = db.prepare("SELECT id FROM test where name = ? ORDER BY id ASC")?;
        let mut rows = stmt.query_and_then([&"one"], |row| {
            let id: i32 = row.get(0)?;
            if id == 1 {
                Ok(id)
            } else {
                Err(Error::ExecuteReturnedResults)
            }
        })?;

        // first row should be Ok
        let doubled_id: i32 = rows.next().unwrap()?;
        assert_eq!(1, doubled_id);

        // second row should be Err
        #[allow(clippy::match_wild_err_arm)]
        match rows.next().unwrap() {
            Ok(_) => panic!("invalid Ok"),
            Err(Error::ExecuteReturnedResults) => (),
            Err(_) => panic!("invalid Err"),
        }
        Ok(())
    }

    #[test]
    fn test_unbound_parameters_are_error() -> Result<()> {
        let db = Connection::open_in_memory()?;
        let sql = "CREATE TABLE test (x TEXT, y TEXT)";
        db.execute_batch(sql)?;

        let mut stmt = db.prepare("INSERT INTO test (x, y) VALUES (?, ?)")?;
        assert!(stmt.execute([&"one"]).is_err());
        Ok(())
    }

    #[test]
    fn test_insert_empty_text_is_none() -> Result<()> {
        let db = Connection::open_in_memory()?;
        let sql = "CREATE TABLE test (x TEXT, y TEXT)";
        db.execute_batch(sql)?;

        let mut stmt = db.prepare("INSERT INTO test (x) VALUES (?)")?;
        stmt.execute([&"one"])?;

        let result: Option<String> = db.query_row("SELECT y FROM test WHERE x = 'one'", [], |row| row.get(0))?;
        assert!(result.is_none());
        Ok(())
    }

    #[test]
    fn test_raw_binding() -> Result<()> {
        let db = Connection::open_in_memory()?;
        db.execute_batch("CREATE TABLE test (name TEXT, value INTEGER)")?;
        {
            let mut stmt = db.prepare("INSERT INTO test (name, value) VALUES (?, ?)")?;

            stmt.raw_bind_parameter(2, 50i32)?;
            stmt.raw_bind_parameter(1, "example")?;
            let n = stmt.raw_execute()?;
            assert_eq!(n, 1);
        }

        {
            let mut stmt = db.prepare("SELECT name, value FROM test WHERE value = ?")?;
            stmt.raw_bind_parameter(1, 50)?;
            stmt.raw_execute()?;
            let mut rows = stmt.raw_query();
            {
                let row = rows.next()?.unwrap();
                let name: String = row.get(0)?;
                assert_eq!(name, "example");
                let value: i32 = row.get(1)?;
                assert_eq!(value, 50);
            }
            assert!(rows.next()?.is_none());
        }

        {
            let db = Connection::open_in_memory()?;
            db.execute_batch("CREATE TABLE test (name TEXT, value UINTEGER)")?;
            let mut stmt = db.prepare("INSERT INTO test(name, value) VALUES (?, ?)")?;
            stmt.raw_bind_parameter(1, "negative")?;
            stmt.raw_bind_parameter(2, u32::MAX)?;
            let n = stmt.raw_execute()?;
            assert_eq!(n, 1);
            assert_eq!(
                u32::MAX,
                db.query_row::<u32, _, _>("SELECT value FROM test", [], |r| r.get(0))?
            );
        }

        {
            let db = Connection::open_in_memory()?;
            db.execute_batch("CREATE TABLE test (name TEXT, value UBIGINT)")?;
            let mut stmt = db.prepare("INSERT INTO test(name, value) VALUES (?, ?)")?;
            stmt.raw_bind_parameter(1, "negative")?;
            stmt.raw_bind_parameter(2, u64::MAX)?;
            let n = stmt.raw_execute()?;
            assert_eq!(n, 1);
            assert_eq!(
                u64::MAX,
                db.query_row::<u64, _, _>("SELECT value FROM test", [], |r| r.get(0))?
            );
        }

        Ok(())
    }

    #[test]
    #[cfg_attr(windows, ignore = "Windows doesn't allow concurrent writes to a file")]
    fn test_insert_duplicate() -> Result<()> {
        let db = Connection::open_in_memory()?;
        db.execute_batch("CREATE TABLE foo(x INTEGER UNIQUE)")?;
        let mut stmt = db.prepare("INSERT INTO foo (x) VALUES (?)")?;
        // TODO(wangfenjin): currently always 1
        stmt.insert([1i32])?;
        stmt.insert([2i32])?;
        assert!(stmt.insert([1i32]).is_err());
        let mut multi = db.prepare("INSERT INTO foo (x) SELECT 3 UNION ALL SELECT 4")?;
        match multi.insert([]).unwrap_err() {
            Error::StatementChangedRows(2) => (),
            err => panic!("Unexpected error {err}"),
        }
        Ok(())
    }

    #[test]
    fn test_insert_different_tables() -> Result<()> {
        // Test for https://github.com/duckdb/duckdb/issues/171
        let db = Connection::open_in_memory()?;
        db.execute_batch(
            r"
            CREATE TABLE foo(x INTEGER);
            CREATE TABLE bar(x INTEGER);
        ",
        )?;

        db.prepare("INSERT INTO foo VALUES (10)")?.insert([])?;
        db.prepare("INSERT INTO bar VALUES (10)")?.insert([])?;
        Ok(())
    }

    // When using RETURNING clauses, DuckDB core treats the statement as a query result instead of a modification
    // statement. This causes execute() to return 0 changed rows and insert() to fail with an error.
    // This test demonstrates current behavior and proper usage patterns for RETURNING clauses.
    #[test]
    fn test_insert_with_returning_clause() -> Result<()> {
        let db = Connection::open_in_memory()?;
        db.execute_batch(
            "CREATE SEQUENCE location_id_seq START WITH 1 INCREMENT BY 1;
             CREATE TABLE location (
                 id INTEGER PRIMARY KEY DEFAULT nextval('location_id_seq'),
                 name TEXT NOT NULL
             )",
        )?;

        // INSERT without RETURNING using execute
        let changes = db.execute("INSERT INTO location (name) VALUES (?)", ["test1"])?;
        assert_eq!(changes, 1);

        // INSERT with RETURNING using execute - returns 0 (known limitation)
        let changes = db.execute("INSERT INTO location (name) VALUES (?) RETURNING id", ["test2"])?;
        assert_eq!(changes, 0);

        // Verify the row was actually inserted despite returning 0
        let count: i64 = db.query_row("SELECT COUNT(*) FROM location", [], |r| r.get(0))?;
        assert_eq!(count, 2);

        // INSERT without RETURNING using insert
        let mut stmt = db.prepare("INSERT INTO location (name) VALUES (?)")?;
        stmt.insert(["test3"])?;

        // INSERT with RETURNING using insert - fails (known limitation)
        let mut stmt = db.prepare("INSERT INTO location (name) VALUES (?) RETURNING id")?;
        let result = stmt.insert(["test4"]);
        assert!(matches!(result, Err(Error::StatementChangedRows(0))));

        // Verify the row was still inserted despite the error
        let count: i64 = db.query_row("SELECT COUNT(*) FROM location", [], |r| r.get(0))?;
        assert_eq!(count, 4);

        // Proper way to use RETURNING - with query_row
        let id: i64 = db.query_row("INSERT INTO location (name) VALUES (?) RETURNING id", ["test5"], |r| {
            r.get(0)
        })?;
        assert_eq!(id, 5);

        // Proper way to use RETURNING - with query_map
        let mut stmt = db.prepare("INSERT INTO location (name) VALUES (?) RETURNING id")?;
        let ids: Vec<i64> = stmt
            .query_map(["test6"], |row| row.get(0))?
            .collect::<Result<Vec<_>>>()?;
        assert_eq!(ids.len(), 1);
        assert_eq!(ids[0], 6);

        // Proper way to use RETURNING - with query_one
        let id: i64 = db
            .prepare("INSERT INTO location (name) VALUES (?) RETURNING id")?
            .query_one(["test7"], |r| r.get(0))?;
        assert_eq!(id, 7);

        // Multiple RETURNING columns
        let (id, name): (i64, String) = db.query_row(
            "INSERT INTO location (name) VALUES (?) RETURNING id, name",
            ["test8"],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        assert_eq!(id, 8);
        assert_eq!(name, "test8");

        Ok(())
    }

    #[test]
    fn test_exists() -> Result<()> {
        let db = Connection::open_in_memory()?;
        let sql = "BEGIN;
                   CREATE TABLE foo(x INTEGER);
                   INSERT INTO foo VALUES(1);
                   INSERT INTO foo VALUES(2);
                   END;";
        db.execute_batch(sql)?;
        let mut stmt = db.prepare("SELECT 1 FROM foo WHERE x = ?")?;
        assert!(stmt.exists([1i32])?);
        assert!(stmt.exists([2i32])?);
        assert!(!stmt.exists([0i32])?);
        Ok(())
    }

    #[test]
    fn test_query_row() -> Result<()> {
        let db = Connection::open_in_memory()?;
        let sql = "BEGIN;
                   CREATE TABLE foo(x INTEGER, y INTEGER);
                   INSERT INTO foo VALUES(1, 3);
                   INSERT INTO foo VALUES(2, 4);
                   END;";
        db.execute_batch(sql)?;
        let mut stmt = db.prepare("SELECT y FROM foo WHERE x = ?")?;
        let y: Result<i32> = stmt.query_row([1i32], |r| r.get(0));
        assert_eq!(3i32, y?);
        Ok(())
    }

    #[test]
    fn test_query_one() -> Result<()> {
        let db = Connection::open_in_memory()?;
        let sql = "BEGIN;
                   CREATE TABLE foo(x INTEGER, y INTEGER);
                   INSERT INTO foo VALUES(1, 3);
                   INSERT INTO foo VALUES(2, 4);
                   END;";
        db.execute_batch(sql)?;

        // Exactly one row
        let y: i32 = db
            .prepare("SELECT y FROM foo WHERE x = ?")?
            .query_one([1], |r| r.get(0))?;
        assert_eq!(y, 3);

        // No rows
        let res: Result<i32> = db
            .prepare("SELECT y FROM foo WHERE x = ?")?
            .query_one([99], |r| r.get(0));
        assert_eq!(res.unwrap_err(), Error::QueryReturnedNoRows);

        // Multiple rows
        let res: Result<i32> = db.prepare("SELECT y FROM foo")?.query_one([], |r| r.get(0));
        assert_eq!(res.unwrap_err(), Error::QueryReturnedMoreThanOneRow);

        Ok(())
    }

    #[test]
    fn test_query_one_optional() -> Result<()> {
        use crate::OptionalExt;

        let db = Connection::open_in_memory()?;
        let sql = "BEGIN;
                   CREATE TABLE foo(x INTEGER, y INTEGER);
                   INSERT INTO foo VALUES(1, 3);
                   INSERT INTO foo VALUES(2, 4);
                   END;";
        db.execute_batch(sql)?;

        // Exactly one row
        let y: Option<i32> = db
            .prepare("SELECT y FROM foo WHERE x = ?")?
            .query_one([1], |r| r.get(0))
            .optional()?;
        assert_eq!(y, Some(3));

        // No rows
        let y: Option<i32> = db
            .prepare("SELECT y FROM foo WHERE x = ?")?
            .query_one([99], |r| r.get(0))
            .optional()?;
        assert_eq!(y, None);

        // Multiple rows - should still return error (not converted by optional)
        let res = db
            .prepare("SELECT y FROM foo")?
            .query_one([], |r| r.get::<_, i32>(0))
            .optional();
        assert_eq!(res.unwrap_err(), Error::QueryReturnedMoreThanOneRow);

        Ok(())
    }

    #[test]
    fn test_query_by_column_name() -> Result<()> {
        let db = Connection::open_in_memory()?;
        let sql = "BEGIN;
                   CREATE TABLE foo(x INTEGER, y INTEGER);
                   INSERT INTO foo VALUES(1, 3);
                   END;";
        db.execute_batch(sql)?;
        let mut stmt = db.prepare("SELECT y FROM foo")?;
        let y: Result<i64> = stmt.query_row([], |r| r.get("y"));
        assert_eq!(3i64, y?);
        Ok(())
    }

    #[test]
    fn test_get_schema_of_executed_result() -> Result<()> {
        use arrow::datatypes::{DataType, Field, Schema};
        let db = Connection::open_in_memory()?;
        let sql = "BEGIN;
                   CREATE TABLE foo(x STRING, y INTEGER);
                   INSERT INTO foo VALUES('hello', 3);
                   END;";
        db.execute_batch(sql)?;
        let mut stmt = db.prepare("SELECT x, y FROM foo")?;
        let _ = stmt.execute([]);
        let schema = stmt.schema();
        assert_eq!(
            *schema,
            Schema::new(vec![
                Field::new("x", DataType::Utf8, true),
                Field::new("y", DataType::Int32, true)
            ])
        );
        Ok(())
    }

    #[test]
    #[should_panic(expected = "called `Option::unwrap()` on a `None` value")]
    fn test_unexecuted_schema_panics() {
        let db = Connection::open_in_memory().unwrap();
        let sql = "BEGIN;
                   CREATE TABLE foo(x STRING, y INTEGER);
                   INSERT INTO foo VALUES('hello', 3);
                   END;";
        db.execute_batch(sql).unwrap();
        let stmt = db.prepare("SELECT x, y FROM foo").unwrap();
        let _ = stmt.schema();
    }

    #[test]
    fn test_query_by_column_name_ignore_case() -> Result<()> {
        let db = Connection::open_in_memory()?;
        let sql = "BEGIN;
                   CREATE TABLE foo(x INTEGER, y INTEGER);
                   INSERT INTO foo VALUES(1, 3);
                   END;";
        db.execute_batch(sql)?;
        let mut stmt = db.prepare("SELECT y as Y FROM foo")?;
        let y: Result<i64> = stmt.query_row([], |r| r.get("y"));
        assert_eq!(3i64, y?);
        Ok(())
    }

    #[test]
    fn test_bind_parameters() -> Result<()> {
        let db = Connection::open_in_memory()?;
        // dynamic slice:
        db.query_row("SELECT ?1, ?2, ?3", [&1u8 as &dyn ToSql, &"one", &Some("one")], |row| {
            row.get::<_, u8>(0)
        })?;
        // existing collection:
        let data = vec![1, 2, 3];
        db.query_row("SELECT ?1, ?2, ?3", params_from_iter(&data), |row| row.get::<_, u8>(0))?;
        db.query_row("SELECT ?1, ?2, ?3", params_from_iter(data.as_slice()), |row| {
            row.get::<_, u8>(0)
        })?;
        db.query_row("SELECT ?1, ?2, ?3", params_from_iter(data), |row| row.get::<_, u8>(0))?;

        let data: std::collections::BTreeSet<String> =
            ["one", "two", "three"].iter().map(|s| (*s).to_string()).collect();
        db.query_row("SELECT ?1, ?2, ?3", params_from_iter(&data), |row| {
            row.get::<_, String>(0)
        })?;

        let data = [0; 3];
        db.query_row("SELECT ?1, ?2, ?3", params_from_iter(&data), |row| row.get::<_, u8>(0))?;
        db.query_row("SELECT ?1, ?2, ?3", params_from_iter(data.iter()), |row| {
            row.get::<_, u8>(0)
        })?;
        Ok(())
    }

    #[test]
    fn test_named_parameters() -> Result<()> {
        use std::collections::HashMap;

        let named_params = HashMap::from([("foo", 42), ("bar", 23)]);

        let db = Connection::open_in_memory()?;
        let sql = r#"SELECT $foo > $bar"#;
        let result: bool = db.query_row(sql, &named_params, |row| row.get(0))?;
        assert!(result);
        Ok(())
    }

    #[test]
    fn test_named_parameters_macro() -> Result<()> {
        let db = Connection::open_in_memory()?;
        let name = "alice";
        let params = crate::named_params! {
            "foo": 42,
            "name": name,
        };
        let result: bool = db.query_row("SELECT $foo > 40 AND $name = 'alice'", params, |row| row.get(0))?;
        assert!(result);
        Ok(())
    }

    #[test]
    fn test_empty_named_parameters_macro() -> Result<()> {
        let db = Connection::open_in_memory()?;
        let result: i32 = db.query_row("SELECT 1", crate::named_params! {}, |row| row.get(0))?;
        assert_eq!(result, 1);
        Ok(())
    }

    #[test]
    fn test_named_parameters_repeated_placeholder() -> Result<()> {
        use std::collections::HashMap;

        let db = Connection::open_in_memory()?;
        let stmt = db.prepare("SELECT $foo + $foo")?;
        assert_eq!(stmt.parameter_count(), 1);
        assert_eq!(stmt.parameter_name(1)?, "foo");

        let slice_result: i32 = db.query_row(
            "SELECT $foo + $foo",
            crate::named_params! {
                "foo": 21,
            },
            |row| row.get(0),
        )?;
        assert_eq!(slice_result, 42);

        let named_params = HashMap::from([("foo", 21)]);
        let hashmap_result: i32 = db.query_row("SELECT $foo + $foo", &named_params, |row| row.get(0))?;
        assert_eq!(hashmap_result, 42);

        Ok(())
    }

    #[test]
    fn test_named_parameters_reject_extra_keys() -> Result<()> {
        use std::collections::HashMap;

        let named_params = HashMap::from([("foo", 42), ("bar", 23), ("extra", 1)]);

        let db = Connection::open_in_memory()?;
        let err = db
            .query_row("SELECT $foo > $bar", &named_params, |row| row.get::<_, bool>(0))
            .unwrap_err();
        assert_eq!(err, Error::InvalidParameterName("extra".to_string()));
        Ok(())
    }

    #[test]
    fn test_named_parameters_reject_extra_hashmap_key_deterministically() -> Result<()> {
        use std::collections::HashMap;

        let named_params = HashMap::from([("foo", 42), ("z_extra", 1), ("a_extra", 2)]);

        let db = Connection::open_in_memory()?;
        let err = db
            .query_row("SELECT $foo", &named_params, |row| row.get::<_, i32>(0))
            .unwrap_err();
        assert_eq!(err, Error::InvalidParameterName("a_extra".to_string()));
        Ok(())
    }

    #[test]
    fn test_named_parameters_reject_extra_slice_key() -> Result<()> {
        let db = Connection::open_in_memory()?;
        let bar = 23;
        let params = crate::named_params! {
            "foo": 42,
            "middle_extra": 0,
            "bar": bar,
            "first_extra": 1,
            "second_extra": 2,
        };

        let err = db
            .query_row("SELECT $foo + $bar", params, |row| row.get::<_, i32>(0))
            .unwrap_err();
        assert_eq!(err, Error::InvalidParameterName("middle_extra".to_string()));
        Ok(())
    }

    #[test]
    fn test_named_parameters_reject_missing_hashmap_key() -> Result<()> {
        use std::collections::HashMap;

        let named_params = HashMap::from([("foo", 42)]);

        let db = Connection::open_in_memory()?;
        let err = db
            .query_row("SELECT $foo > $bar", &named_params, |row| row.get::<_, bool>(0))
            .unwrap_err();
        assert_eq!(err, Error::InvalidParameterName("bar".to_string()));
        Ok(())
    }

    #[test]
    fn test_named_parameters_reject_missing_slice_key() -> Result<()> {
        let db = Connection::open_in_memory()?;
        let params = crate::named_params! {
            "foo": 42,
        };

        let err = db
            .query_row("SELECT $foo > $bar", params, |row| row.get::<_, bool>(0))
            .unwrap_err();
        assert_eq!(err, Error::InvalidParameterName("bar".to_string()));
        Ok(())
    }

    #[test]
    fn test_named_parameters_reject_duplicate_slice_keys() -> Result<()> {
        let db = Connection::open_in_memory()?;
        let first = 42;
        let second = 23;
        let params = &[("foo", &first as &dyn ToSql), ("foo", &second as &dyn ToSql)] as &[(&str, &dyn ToSql)];

        let err = db
            .query_row("SELECT $foo", params, |row| row.get::<_, i32>(0))
            .unwrap_err();
        assert_eq!(
            err,
            Error::InvalidParameterName("duplicate parameter name: foo".to_string())
        );
        Ok(())
    }

    #[test]
    fn test_named_parameters_reject_positional_placeholders() -> Result<()> {
        use std::collections::HashMap;

        let named_params = HashMap::from([("1", 42), ("2", 23)]);

        let db = Connection::open_in_memory()?;
        let err = db
            .query_row("SELECT ? > ?", &named_params, |row| row.get::<_, bool>(0))
            .unwrap_err();
        assert_eq!(
            err,
            Error::InvalidParameterName("positional parameter 1 cannot be used with named parameters".to_string())
        );
        Ok(())
    }

    #[test]
    fn test_named_parameters_reject_dollar_number_placeholders() -> Result<()> {
        use std::collections::HashMap;

        let named_params = HashMap::from([("1", 42), ("2", 23)]);
        let db = Connection::open_in_memory()?;

        let hashmap_err = db
            .query_row("SELECT $1 + $2", &named_params, |row| row.get::<_, i32>(0))
            .unwrap_err();
        assert_eq!(
            hashmap_err,
            Error::InvalidParameterName("positional parameter 1 cannot be used with named parameters".to_string())
        );

        let slice_err = db
            .query_row(
                "SELECT $1 + $2",
                crate::named_params! {
                    "1": 42,
                    "2": 23,
                },
                |row| row.get::<_, i32>(0),
            )
            .unwrap_err();
        assert_eq!(
            slice_err,
            Error::InvalidParameterName("positional parameter 1 cannot be used with named parameters".to_string())
        );
        Ok(())
    }

    #[test]
    fn test_named_parameters_reject_mixed_placeholders() -> Result<()> {
        let db = Connection::open_in_memory()?;
        let err = db.prepare("SELECT $foo + ?").unwrap_err();
        assert!(err.to_string().contains("Mixing named and positional parameters"));
        Ok(())
    }

    #[test]
    fn test_named_parameters_bind_null_values() -> Result<()> {
        use std::collections::HashMap;

        let db = Connection::open_in_memory()?;
        let named_params = HashMap::from([("x", None::<i32>)]);

        let hashmap_result: bool = db.query_row("SELECT $x IS NULL", &named_params, |row| row.get(0))?;
        assert!(hashmap_result);

        let slice_result: bool = db.query_row(
            "SELECT $x IS NULL",
            crate::named_params! {
                "x": Option::<i32>::None,
            },
            |row| row.get(0),
        )?;
        assert!(slice_result);

        Ok(())
    }

    #[test]
    fn test_named_parameters_string_keys_query_map() -> Result<()> {
        use std::collections::HashMap;

        let db = Connection::open_in_memory()?;
        let params = HashMap::from([("min".to_string(), 2i64), ("max".to_string(), 3i64)]);
        let mut stmt = db.prepare("SELECT i FROM range(5) tbl(i) WHERE i BETWEEN $min AND $max ORDER BY i")?;
        let rows = stmt.query_map(&params, |row| row.get::<_, i64>(0))?;
        let values = rows.collect::<Result<Vec<_>>>()?;

        assert_eq!(values, [2, 3]);
        Ok(())
    }

    #[test]
    fn test_named_parameters_cow_keys_custom_hasher() -> Result<()> {
        use std::{
            borrow::Cow,
            collections::{HashMap, hash_map::DefaultHasher},
            hash::BuildHasherDefault,
        };

        let db = Connection::open_in_memory()?;
        let mut params: HashMap<Cow<'static, str>, i64, BuildHasherDefault<DefaultHasher>> = HashMap::default();
        params.insert(Cow::Borrowed("min"), 2);
        params.insert(Cow::Owned("max".to_string()), 3);

        let mut stmt = db.prepare("SELECT i FROM range(5) tbl(i) WHERE i BETWEEN $min AND $max ORDER BY i")?;
        let rows = stmt.query_map(&params, |row| row.get::<_, i64>(0))?;
        let values = rows.collect::<Result<Vec<_>>>()?;

        assert_eq!(values, [2, 3]);
        Ok(())
    }

    #[test]
    fn test_empty_stmt() -> Result<()> {
        let conn = Connection::open_in_memory()?;
        let stmt = conn.prepare("");
        assert!(stmt.is_err());

        Ok(())
    }

    #[test]
    fn test_comment_empty_stmt() -> Result<()> {
        let conn = Connection::open_in_memory()?;
        assert!(conn.prepare("/*SELECT 1;*/").is_err());
        Ok(())
    }

    #[test]
    fn test_comment_and_sql_stmt() -> Result<()> {
        let conn = Connection::open_in_memory()?;
        let mut stmt = conn.prepare("/*...*/ SELECT 1;")?;
        stmt.execute([])?;
        assert_eq!(1, stmt.column_count());
        Ok(())
    }

    #[test]
    fn test_nul_byte() -> Result<()> {
        let db = Connection::open_in_memory()?;
        let expected = "a\x00b";
        let actual: String = db.query_row("SELECT CAST(? AS VARCHAR)", [expected], |row| row.get(0))?;
        assert_eq!(expected, actual);
        Ok(())
    }

    #[test]
    fn test_parameter_name() -> Result<()> {
        let db = Connection::open_in_memory()?;

        {
            let stmt = db.prepare("SELECT $foo, $bar")?;

            assert_eq!(stmt.parameter_count(), 2);
            assert_eq!(stmt.parameter_name(1)?, "foo");
            assert_eq!(stmt.parameter_name(2)?, "bar");

            assert!(matches!(stmt.parameter_name(0), Err(Error::InvalidParameterIndex(0))));
            assert!(matches!(
                stmt.parameter_name(100),
                Err(Error::InvalidParameterIndex(100))
            ));
        }

        // Positional parameters return their index number as the name
        {
            let stmt = db.prepare("SELECT ?, ?")?;
            assert_eq!(stmt.parameter_count(), 2);
            assert_eq!(stmt.parameter_name(1)?, "1");
            assert_eq!(stmt.parameter_name(2)?, "2");
        }

        // Numbered positional parameters also return their number
        {
            let stmt = db.prepare("SELECT ?1, ?2")?;
            assert_eq!(stmt.parameter_count(), 2);
            assert_eq!(stmt.parameter_name(1)?, "1");
            assert_eq!(stmt.parameter_name(2)?, "2");
        }

        Ok(())
    }

    #[test]
    fn test_bind_named_parameters_manually() -> Result<()> {
        use std::collections::HashMap;

        let db = Connection::open_in_memory()?;
        let mut stmt = db.prepare("SELECT $foo > $bar")?;

        let mut params: HashMap<String, i32> = HashMap::new();
        params.insert("foo".to_string(), 42);
        params.insert("bar".to_string(), 23);

        for idx in 1..=stmt.parameter_count() {
            let name = stmt.parameter_name(idx)?;
            if let Some(value) = params.get(&name) {
                stmt.raw_bind_parameter(idx, value)?;
            }
        }

        stmt.raw_execute()?;

        let mut rows = stmt.raw_query();
        let row = rows.next()?.unwrap();
        let result: bool = row.get(0)?;
        assert!(result);

        Ok(())
    }

    #[test]
    fn test_execute_streaming_error_message() -> Result<()> {
        let db = Connection::open_in_memory()?;

        // Trigger a conversion error - should fail with a descriptive message
        let mut stmt = db.prepare("SELECT CAST('not-a-number' AS INTEGER)")?;
        let result = stmt.stmt.execute_streaming();

        assert!(result.is_err());
        let err = result.unwrap_err();

        let error_string = format!("{}", err);
        assert!(
            error_string.contains("Conversion Error"),
            "Expected descriptive error, got: {}",
            error_string
        );

        Ok(())
    }

    #[test]
    fn test_bind_date32() -> Result<()> {
        use crate::types::Value;

        let db = Connection::open_in_memory()?;
        // 19130 days since epoch = 2022-05-18
        let result: bool = db.query_row("SELECT ? = DATE '2022-05-18'", [Value::Date32(19130)], |row| row.get(0))?;
        assert!(result);
        Ok(())
    }

    #[test]
    fn test_bind_time64() -> Result<()> {
        use crate::types::{TimeUnit, Value};

        let db = Connection::open_in_memory()?;
        // 45_045_123_456 micros = 12:30:45.123456
        let micros = 45_045_123_456i64;
        let result: bool = db.query_row(
            "SELECT ? = TIME '12:30:45.123456'",
            [Value::Time64(TimeUnit::Microsecond, micros)],
            |row| row.get(0),
        )?;
        assert!(result);
        Ok(())
    }

    #[test]
    fn test_execute_tuple() -> Result<()> {
        let db = Connection::open_in_memory()?;
        db.execute_batch("CREATE TABLE test (id INTEGER, name TEXT, score DOUBLE)")?;

        // Heterogeneous tuple
        let mut stmt = db.prepare("INSERT INTO test VALUES (?, ?, ?)")?;
        stmt.execute((1i32, "alice", 95.5f64))?;
        stmt.execute((2i32, "bob", 87.0f64))?;

        let mut stmt = db.prepare("SELECT id, name, score FROM test ORDER BY id")?;
        let mut rows = stmt.query([])?;

        let row = rows.next()?.unwrap();
        assert_eq!(row.get::<_, i32>(0)?, 1);
        assert_eq!(row.get::<_, String>(1)?, "alice");
        assert_eq!(row.get::<_, f64>(2)?, 95.5);

        let row = rows.next()?.unwrap();
        assert_eq!(row.get::<_, i32>(0)?, 2);
        assert_eq!(row.get::<_, String>(1)?, "bob");
        assert_eq!(row.get::<_, f64>(2)?, 87.0);

        Ok(())
    }

    #[test]
    fn test_query_row_tuple() -> Result<()> {
        let db = Connection::open_in_memory()?;
        db.execute_batch("CREATE TABLE test (id INTEGER, name TEXT)")?;
        db.execute("INSERT INTO test VALUES (1, 'alice')", [])?;

        let name: String = db.query_row("SELECT name FROM test WHERE id = ?", (1i32,), |r| r.get(0))?;
        assert_eq!(name, "alice");

        Ok(())
    }

    #[test]
    fn test_execute_tuple_single_element() -> Result<()> {
        let db = Connection::open_in_memory()?;
        db.execute_batch("CREATE TABLE test (id INTEGER)")?;

        db.execute("INSERT INTO test VALUES (?)", (42i32,))?;

        let val: i32 = db.query_row("SELECT id FROM test", [], |r| r.get(0))?;
        assert_eq!(val, 42);
        Ok(())
    }

    #[test]
    fn test_execute_tuple_many_columns() -> Result<()> {
        let db = Connection::open_in_memory()?;
        db.execute_batch(
            "CREATE TABLE test (a INT, b TEXT, c DOUBLE, d INT, e TEXT, f DOUBLE, g INT, h TEXT, i DOUBLE, j INT, k TEXT, l DOUBLE, m INT, n TEXT, o DOUBLE, p INT)",
        )?;

        // Use arity 16 with heterogeneous types to exercise the max tuple impl
        db.execute(
            "INSERT INTO test VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                1i32, "a", 1.0f64, 2i32, "b", 2.0f64, 3i32, "c", 3.0f64, 4i32, "d", 4.0f64, 5i32, "e", 5.0f64, 6i32,
            ),
        )?;

        let (a, p): (i32, i32) = db.query_row("SELECT a, p FROM test", [], |r| Ok((r.get(0)?, r.get(1)?)))?;
        assert_eq!(a, 1);
        assert_eq!(p, 6);
        Ok(())
    }

    #[test]
    fn test_execute_tuple_with_option() -> Result<()> {
        let db = Connection::open_in_memory()?;
        db.execute_batch("CREATE TABLE test (id INTEGER, name TEXT)")?;

        db.execute("INSERT INTO test VALUES (?, ?)", (1i32, None::<String>))?;

        let name: Option<String> = db.query_row("SELECT name FROM test WHERE id = ?", (1i32,), |r| r.get(0))?;
        assert_eq!(name, None);
        Ok(())
    }

    #[test]
    fn test_execute_empty_tuple() -> Result<()> {
        let db = Connection::open_in_memory()?;
        db.execute_batch("CREATE TABLE test (id INTEGER DEFAULT 1)")?;

        db.execute("INSERT INTO test DEFAULT VALUES", ())?;

        let val: i32 = db.query_row("SELECT id FROM test", (), |r| r.get(0))?;
        assert_eq!(val, 1);
        Ok(())
    }

    #[test]
    fn test_with_decimal() -> Result<()> {
        let db = Connection::open_in_memory()?;
        db.execute_batch(
            "BEGIN; \
            CREATE TABLE foo(x DECIMAL(18, 4)); \
            CREATE TABLE bar(y DECIMAL(18, 2)); \
            COMMIT;",
        )?;

        // If duckdb's scale is larger than rust_decimal's scale, value should not be truncated.
        let value = Decimal::from_i128_with_scale(12345, 4);
        db.execute("INSERT INTO foo(x) VALUES (?)", [&value])?;
        let row: Decimal = db.query_row("SELECT x FROM foo", [], |r| r.get::<_, Decimal>(0))?;
        assert_eq!(row, value);

        // If duckdb's scale is smaller than rust_decimal's scale, value should be truncated (1.2345 -> 1.23).
        let value = Decimal::from_i128_with_scale(12345, 4);
        db.execute("INSERT INTO bar(y) VALUES (?)", [&value])?;
        let row: Decimal = db.query_row("SELECT y FROM bar", [], |r| r.get::<_, Decimal>(0))?;
        assert_eq!(row, Decimal::from_i128_with_scale(123, 2));

        Ok(())
    }

    #[test]
    fn test_decimal_from_sql_integer_and_float() -> Result<()> {
        let db = Connection::open_in_memory()?;

        // FromSql: INTEGER -> Decimal
        let row: Decimal = db.query_row("SELECT 42", [], |r| r.get(0))?;
        assert_eq!(row, Decimal::from(42));

        // FromSql: BIGINT -> Decimal
        let row: Decimal = db.query_row("SELECT 9999999999::BIGINT", [], |r| r.get(0))?;
        assert_eq!(row, Decimal::from(9999999999_i64));

        // FromSql: DOUBLE -> Decimal (inherits float imprecision)
        let row: Decimal = db.query_row("SELECT 3.14::DOUBLE", [], |r| r.get(0))?;
        let diff = (row - Decimal::from_str_exact("3.14").unwrap()).abs();
        assert!(diff < Decimal::from_str_exact("0.0001").unwrap());

        // FromSql: VARCHAR -> Decimal
        let row: Decimal = db.query_row("SELECT '123.456'::VARCHAR", [], |r| r.get(0))?;
        assert_eq!(row, Decimal::from_str_exact("123.456").unwrap());

        // FromSql: HUGEINT -> Decimal
        let row: Decimal = db.query_row("SELECT 12345678901234567890::HUGEINT", [], |r| r.get(0))?;
        assert_eq!(row, Decimal::from_i128_with_scale(12345678901234567890, 0));

        Ok(())
    }

    #[test]
    fn test_bind_error_clears_partial_parameter_state() -> Result<()> {
        let db = Connection::open_in_memory()?;
        db.execute("CREATE TABLE t (id INTEGER NOT NULL, name TEXT)", [])?;

        let mut stmt = db.prepare("INSERT INTO t VALUES (?, ?)")?;
        stmt.execute(crate::params![1]).unwrap_err();

        stmt.raw_bind_parameter(2, "ok")?;
        assert!(stmt.raw_execute().is_err());

        let count: i32 = db.query_row("SELECT COUNT(*) FROM t", [], |row| row.get(0))?;
        assert_eq!(count, 0);

        stmt.raw_bind_parameter(1, 7)?;
        stmt.raw_bind_parameter(2, "ok")?;
        assert_eq!(stmt.raw_execute()?, 1);

        let row = db.query_row("SELECT id, name FROM t", [], |row| {
            Ok((row.get::<_, i32>(0)?, row.get::<_, String>(1)?))
        })?;
        assert_eq!(row, (7, "ok".to_string()));
        Ok(())
    }

    #[test]
    fn test_bind_list() -> Result<()> {
        use crate::types::Value;

        let db = Connection::open_in_memory()?;
        db.execute("CREATE TABLE test (numbers INTEGER[])", [])?;

        let list_value = Value::List(vec![Value::Int(1), Value::Int(2), Value::Int(3)]);
        db.execute("INSERT INTO test VALUES (?)", crate::params![&list_value])?;

        let result = db.query_row("SELECT numbers FROM test", [], |row| row.get::<_, Value>(0))?;
        assert_eq!(result, Value::List(vec![Value::Int(1), Value::Int(2), Value::Int(3)]));
        Ok(())
    }

    #[test]
    fn test_bind_list_with_nulls() -> Result<()> {
        use crate::types::Value;

        let db = Connection::open_in_memory()?;
        db.execute("CREATE TABLE test (numbers INTEGER[])", [])?;

        let list_value = Value::List(vec![Value::Int(1), Value::Null, Value::Int(3)]);
        db.execute("INSERT INTO test VALUES (?)", crate::params![&list_value])?;

        let result = db.query_row("SELECT numbers FROM test", [], |row| row.get::<_, Value>(0))?;
        assert_eq!(result, Value::List(vec![Value::Int(1), Value::Null, Value::Int(3)]));
        Ok(())
    }

    #[test]
    fn test_bind_empty_list_returns_error() {
        use crate::types::Value;

        let db = Connection::open_in_memory().unwrap();
        db.execute("CREATE TABLE test (numbers INTEGER[])", []).unwrap();

        let list_value = Value::List(vec![]);
        let err = db
            .execute("INSERT INTO test VALUES (?)", crate::params![&list_value])
            .unwrap_err();

        match err {
            Error::ToSqlConversionFailure(e) => {
                assert!(e.to_string().contains("empty or all-null"), "unexpected message: {e}");
            }
            other => panic!("expected ToSqlConversionFailure, got {other:?}"),
        }
    }

    #[test]
    fn test_bind_nested_list() -> Result<()> {
        use crate::types::Value;

        let db = Connection::open_in_memory()?;
        db.execute("CREATE TABLE test (matrix INTEGER[][])", [])?;

        let inner1 = Value::List(vec![Value::Int(1), Value::Int(2)]);
        let inner2 = Value::List(vec![Value::Int(3), Value::Int(4)]);
        let list_value = Value::List(vec![inner1.clone(), inner2.clone()]);
        db.execute("INSERT INTO test VALUES (?)", crate::params![&list_value])?;

        let result: Value = db.query_row("SELECT matrix FROM test", [], |row| row.get::<_, Value>(0))?;

        assert_eq!(result, Value::List(vec![inner1, inner2]));
        Ok(())
    }

    #[test]
    fn test_bind_list_ref() -> Result<()> {
        use crate::types::Value;

        let db = Connection::open_in_memory()?;
        db.execute("CREATE TABLE test (numbers INTEGER[])", [])?;

        let list_array = BorrowedList::new(vec![Some(1), Some(2), Some(3)]);
        db.execute("INSERT INTO test VALUES (?)", crate::params![list_array])?;

        let result = db.query_row("SELECT numbers FROM test", [], |row| row.get::<_, Value>(0))?;
        assert_eq!(result, Value::List(vec![Value::Int(1), Value::Int(2), Value::Int(3)]));
        Ok(())
    }

    #[test]
    fn test_bind_list_ref_nulls() -> Result<()> {
        use crate::types::Value;

        let db = Connection::open_in_memory()?;
        db.execute("CREATE TABLE test (numbers INTEGER[])", [])?;

        let list_array = BorrowedList::new(vec![Some(3), None, Some(5)]);
        db.execute("INSERT INTO test VALUES (?)", crate::params![list_array])?;

        let result = db.query_row("SELECT numbers FROM test", [], |r| r.get::<_, Value>(0))?;
        assert_eq!(result, Value::List(vec![Value::Int(3), Value::Null, Value::Int(5)]));
        Ok(())
    }

    #[test]
    fn test_bind_list_ref_empty() -> Result<()> {
        use crate::types::Value;

        let db = Connection::open_in_memory()?;
        db.execute("CREATE TABLE test (numbers INTEGER[])", [])?;

        let list_array = BorrowedList::new(vec![]);
        db.execute("INSERT INTO test VALUES (?)", crate::params![list_array])?;

        let result = db.query_row("SELECT numbers FROM test", [], |r| r.get::<_, Value>(0))?;
        assert_eq!(result, Value::List(vec![]));
        Ok(())
    }
}

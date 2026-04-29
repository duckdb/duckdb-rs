use super::{AppenderParams, Connection, Result, ValueRef, ffi};
use std::{ffi::c_void, fmt, os::raw::c_char};

use crate::{
    Error,
    error::result_from_duckdb_appender,
    types::{ToSql, ToSqlOutput, value_ref_from_value},
};

/// Appender for fast import data
///
/// # Thread Safety
///
/// `Appender` is neither `Send` nor `Sync`:
/// - Not `Send` because it holds a reference to `Connection`, which is `!Sync`
/// - Not `Sync` because DuckDB appenders don't support concurrent access
///
/// To use an appender in another thread, move the `Connection` to that thread
/// and create the appender there.
///
/// If you need to share an `Appender` across threads, wrap it in a `Mutex`.
///
/// See [DuckDB concurrency documentation](https://duckdb.org/docs/stable/connect/concurrency.html) for more details.
///
/// # Wide Tables (Many Columns)
///
/// Array literals `[value; N]` are supported for tables with up to 32 columns.
///
/// ```rust,ignore
/// appender.append_row([0; 32])?;
/// appender.append_row([1, 2, 3, 4, 5])?;
/// ```
///
/// For tables with more than 32 columns, use one of these alternatives:
///
/// ## 1. Slice approach - convert values to `&dyn ToSql`
///
/// ```rust,ignore
/// let values: Vec<i32> = vec![0; 100];
/// let params: Vec<&dyn ToSql> = values.iter().map(|v| v as &dyn ToSql).collect();
/// appender.append_row(params.as_slice())?;
/// ```
///
/// ## 2. `params!` macro - write values explicitly
///
/// ```rust,ignore
/// appender.append_row(params![v1, v2, v3, ..., v50])?;
/// ```
///
/// ## 3. `appender_params_from_iter` - pass an iterator directly
///
/// ```rust,ignore
/// use duckdb::appender_params_from_iter;
/// let values: Vec<i32> = vec![0; 100];
/// appender.append_row(appender_params_from_iter(values))?;
/// ```
///
/// All three methods can be used interchangeably and mixed in the same appender.
pub struct Appender<'conn> {
    conn: &'conn Connection,
    app: ffi::duckdb_appender,
}

#[cfg(feature = "appender-arrow")]
mod arrow;

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
    /// Will return `Err` if append column count not the same with the table
    /// schema, or if a value cannot be converted for appending.
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
    /// Will return `Err` if append column count not the same with the table
    /// schema, or if a value cannot be converted for appending.
    #[inline]
    pub fn append_row<P: AppenderParams>(&mut self, params: P) -> Result<()> {
        params.__bind_in(self)
    }

    #[inline]
    pub(crate) fn append_parameter_row<P>(&mut self, params: P) -> Result<()>
    where
        P: IntoIterator,
        P::Item: ToSql,
    {
        let params = params.into_iter().collect::<Vec<_>>();
        let values = params
            .iter()
            .map(ToSql::to_sql)
            .collect::<Result<Vec<ToSqlOutput<'_>>>>()?;

        self.validate_parameter_values(&values)?;

        let _ = unsafe { ffi::duckdb_appender_begin_row(self.app) };
        self.bind_parameter_values(&values)?;
        // NOTE: we only check end_row return value
        let rc = unsafe { ffi::duckdb_appender_end_row(self.app) };
        result_from_duckdb_appender(rc, &mut self.app)
    }

    fn validate_parameter_values(&self, values: &[ToSqlOutput<'_>]) -> Result<()> {
        for value in values {
            let value = to_value_ref(value)?;
            validate_appender_value_ref(value)?;
        }
        Ok(())
    }

    fn bind_parameter_values(&self, values: &[ToSqlOutput<'_>]) -> Result<()> {
        for value in values {
            self.bind_parameter(value)?;
        }
        Ok(())
    }

    fn bind_parameter(&self, value: &ToSqlOutput<'_>) -> Result<()> {
        let ptr = self.app;
        let value = to_value_ref(value)?;
        // NOTE: we ignore the return value here
        //       because if anything failed, end_row will fail
        // TODO: append more
        let rc = match value {
            ValueRef::Null => unsafe { ffi::duckdb_append_null(ptr) },
            ValueRef::Boolean(i) => unsafe { ffi::duckdb_append_bool(ptr, i) },
            ValueRef::TinyInt(i) => unsafe { ffi::duckdb_append_int8(ptr, i) },
            ValueRef::SmallInt(i) => unsafe { ffi::duckdb_append_int16(ptr, i) },
            ValueRef::Int(i) => unsafe { ffi::duckdb_append_int32(ptr, i) },
            ValueRef::BigInt(i) => unsafe { ffi::duckdb_append_int64(ptr, i) },
            ValueRef::UTinyInt(i) => unsafe { ffi::duckdb_append_uint8(ptr, i) },
            ValueRef::USmallInt(i) => unsafe { ffi::duckdb_append_uint16(ptr, i) },
            ValueRef::UInt(i) => unsafe { ffi::duckdb_append_uint32(ptr, i) },
            ValueRef::UBigInt(i) => unsafe { ffi::duckdb_append_uint64(ptr, i) },
            ValueRef::HugeInt(i) => unsafe {
                let hi = ffi::duckdb_hugeint {
                    lower: i as u64,
                    upper: (i >> 64) as i64,
                };
                ffi::duckdb_append_hugeint(ptr, hi)
            },

            ValueRef::Float(r) => unsafe { ffi::duckdb_append_float(ptr, r) },
            ValueRef::Double(r) => unsafe { ffi::duckdb_append_double(ptr, r) },
            ValueRef::Text(s) => unsafe {
                ffi::duckdb_append_varchar_length(ptr, s.as_ptr() as *const c_char, s.len() as u64)
            },
            ValueRef::Timestamp(u, i) => unsafe {
                ffi::duckdb_append_timestamp(ptr, ffi::duckdb_timestamp { micros: u.to_micros(i) })
            },
            ValueRef::Blob(b) => unsafe { ffi::duckdb_append_blob(ptr, b.as_ptr() as *const c_void, b.len() as u64) },
            ValueRef::Date32(d) => unsafe { ffi::duckdb_append_date(ptr, ffi::duckdb_date { days: d }) },
            ValueRef::Time64(u, v) => unsafe {
                ffi::duckdb_append_time(ptr, ffi::duckdb_time { micros: u.to_micros(v) })
            },
            ValueRef::Interval { months, days, nanos } => unsafe {
                ffi::duckdb_append_interval(
                    ptr,
                    ffi::duckdb_interval {
                        months,
                        days,
                        micros: nanos / 1000,
                    },
                )
            },
            ValueRef::Decimal(d) => unsafe {
                let decimal = crate::types::to_duckdb_decimal(d);
                let mut value = ffi::duckdb_create_decimal(decimal);
                if value.is_null() {
                    return Err(Error::AppendError);
                }
                let res = ffi::duckdb_append_value(ptr, value);
                ffi::duckdb_destroy_value(&mut value);
                res
            },
            _ => {
                return Err(Error::ToSqlConversionFailure(
                    format!("appending value of type {} is not yet supported", value.data_type()).into(),
                ));
            }
        };
        if rc != 0 {
            return Err(Error::AppendError);
        }
        Ok(())
    }

    #[inline]
    pub(super) fn new(conn: &Connection, app: ffi::duckdb_appender) -> Appender<'_> {
        Appender { conn, app }
    }

    /// Flush data into DB
    #[inline]
    pub fn flush(&mut self) -> Result<()> {
        unsafe {
            let res = ffi::duckdb_appender_flush(self.app);
            result_from_duckdb_appender(res, &mut self.app)
        }
    }

    /// Add a column to the appender's active column list.
    ///
    /// When columns are added, only those columns need values during append.
    /// Other columns will use their DEFAULT value (or NULL if no default).
    ///
    /// This flushes any pending data before modifying the column list.
    #[inline]
    pub fn add_column(&mut self, name: &str) -> Result<()> {
        let c_name = std::ffi::CString::new(name)?;
        let rc = unsafe { ffi::duckdb_appender_add_column(self.app, c_name.as_ptr() as *const c_char) };
        result_from_duckdb_appender(rc, &mut self.app)
    }

    /// Clear the appender's active column list.
    ///
    /// After clearing, all columns become active again and values must be
    /// provided for every column during append.
    ///
    /// This flushes any pending data before clearing.
    #[inline]
    pub fn clear_columns(&mut self) -> Result<()> {
        let rc = unsafe { ffi::duckdb_appender_clear_columns(self.app) };
        result_from_duckdb_appender(rc, &mut self.app)
    }
}

impl Drop for Appender<'_> {
    fn drop(&mut self) {
        if !self.app.is_null() {
            unsafe {
                ffi::duckdb_appender_destroy(&mut self.app);
            }
        }
    }
}

impl fmt::Debug for Appender<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Appender").field("conn", self.conn).finish()
    }
}

fn appending_unsupported_value(value_type: impl fmt::Display) -> String {
    format!("appending {value_type} values is not yet supported")
}

fn to_value_ref<'value, 'output>(value: &'value ToSqlOutput<'output>) -> Result<ValueRef<'value>>
where
    'output: 'value,
{
    match *value {
        ToSqlOutput::Borrowed(v) => Ok(v),
        ToSqlOutput::Owned(ref v) => value_ref_from_value(v, appending_unsupported_value),
    }
}

fn validate_appender_value_ref(value: ValueRef<'_>) -> Result<()> {
    match value {
        ValueRef::Null
        | ValueRef::Boolean(_)
        | ValueRef::TinyInt(_)
        | ValueRef::SmallInt(_)
        | ValueRef::Int(_)
        | ValueRef::BigInt(_)
        | ValueRef::HugeInt(_)
        | ValueRef::UTinyInt(_)
        | ValueRef::USmallInt(_)
        | ValueRef::UInt(_)
        | ValueRef::UBigInt(_)
        | ValueRef::Float(_)
        | ValueRef::Double(_)
        | ValueRef::Text(_)
        | ValueRef::Timestamp(_, _)
        | ValueRef::Blob(_)
        | ValueRef::Date32(_)
        | ValueRef::Time64(_, _)
        | ValueRef::Interval { .. }
        | ValueRef::Decimal(_) => Ok(()),
        _ => Err(Error::ToSqlConversionFailure(
            appending_unsupported_value(value.data_type()).into(),
        )),
    }
}

#[cfg(test)]
mod test {
    use rust_decimal::Decimal;

    use crate::{Connection, Error, Result, params};

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
    fn test_append_unsupported_container_type_returns_error() -> Result<()> {
        use crate::{
            ToSql,
            types::{ToSqlOutput, Value},
        };

        struct OwnedList;
        impl ToSql for OwnedList {
            fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
                Ok(ToSqlOutput::Owned(Value::List(vec![Value::Int(1), Value::Int(2)])))
            }
        }

        let db = Connection::open_in_memory()?;
        db.execute_batch("CREATE TABLE foo(id INTEGER, name TEXT)")?;

        let list = OwnedList;
        let mut app = db.appender("foo")?;
        app.append_row(params![10, "before"])?;
        app.append_row(params![11, "also before"])?;
        let err = app.append_row(params![1, list]).unwrap_err();

        match err {
            Error::ToSqlConversionFailure(e) => {
                assert!(
                    e.to_string().contains("appending List values is not yet supported"),
                    "unexpected message: {e}"
                );
            }
            other => panic!("expected ToSqlConversionFailure, got {other:?}"),
        }
        app.append_row(params![2, "ok"])?;
        app.flush()?;

        let rows = db
            .prepare("SELECT id, name FROM foo ORDER BY id")?
            .query_map([], |row| Ok((row.get::<_, i32>(0)?, row.get::<_, String>(1)?)))?
            .collect::<Result<Vec<_>>>()?;
        assert_eq!(
            rows,
            vec![
                (2, "ok".to_string()),
                (10, "before".to_string()),
                (11, "also before".to_string())
            ]
        );
        let count: i32 = db.query_row("SELECT COUNT(*) FROM foo", [], |row| row.get(0))?;
        assert_eq!(count, 3);
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

    #[cfg(feature = "uuid")]
    #[test]
    fn test_append_uuid() -> Result<()> {
        use uuid::Uuid;

        let db = Connection::open_in_memory()?;
        db.execute_batch("CREATE TABLE foo(x UUID)")?;

        let id = Uuid::new_v4();
        {
            let mut app = db.appender("foo")?;
            app.append_row([id])?;
        }

        let val = db.query_row("SELECT x FROM foo", [], |row| <(Uuid,)>::try_from(row))?;
        assert_eq!(val, (id,));
        Ok(())
    }

    #[test]
    fn test_append_string_as_ts_row() -> Result<()> {
        let db = Connection::open_in_memory()?;
        db.execute_batch("CREATE TABLE foo(x TIMESTAMP)")?;

        {
            let mut app = db.appender("foo")?;
            app.append_row(["2022-04-09 15:56:37.544"])?;
        }

        let val = db.query_row("SELECT x FROM foo", [], |row| <(i64,)>::try_from(row))?;
        assert_eq!(val, (1649519797544000,));
        Ok(())
    }

    #[test]
    fn test_append_timestamp() -> Result<()> {
        use std::time::Duration;
        let db = Connection::open_in_memory()?;
        db.execute_batch("CREATE TABLE foo(x TIMESTAMP)")?;

        let d = Duration::from_secs(1);
        {
            let mut app = db.appender("foo")?;
            app.append_row([d])?;
        }

        let val = db.query_row("SELECT x FROM foo where x=?", [d], |row| <(i32,)>::try_from(row))?;
        assert_eq!(val, (d.as_micros() as i32,));
        Ok(())
    }

    #[test]
    #[cfg(feature = "chrono")]
    fn test_append_datetime() -> Result<()> {
        use chrono::{NaiveDate, NaiveDateTime};

        let db = Connection::open_in_memory()?;
        db.execute_batch("CREATE TABLE foo(x DATE, y TIMESTAMP)")?;

        let date = NaiveDate::from_ymd_opt(2024, 6, 5).unwrap();
        let timestamp = date.and_hms_opt(18, 26, 53).unwrap();
        {
            let mut app = db.appender("foo")?;
            app.append_row(params![date, timestamp])?;
        }
        let (date2, timestamp2) = db.query_row("SELECT x, y FROM foo", [], |row| {
            Ok((row.get::<_, NaiveDate>(0)?, row.get::<_, NaiveDateTime>(1)?))
        })?;
        assert_eq!(date, date2);
        assert_eq!(timestamp, timestamp2);
        Ok(())
    }

    #[test]
    #[cfg(feature = "chrono")]
    fn test_append_struct_with_params() -> Result<()> {
        use chrono::NaiveDate;

        struct Person {
            first_name: String,
            last_name: String,
            dob: NaiveDate,
        }

        let db = Connection::open_in_memory()?;

        db.execute_batch("CREATE TABLE foo(first_name VARCHAR, last_name VARCHAR, dob DATE);")?;

        let person1 = Person {
            first_name: String::from("John"),
            last_name: String::from("Smith"),
            dob: NaiveDate::from_ymd_opt(1970, 1, 1).unwrap(),
        };

        let person2 = Person {
            first_name: String::from("Jane"),
            last_name: String::from("Smith"),
            dob: NaiveDate::from_ymd_opt(1975, 1, 1).unwrap(),
        };

        // Use params! to extract struct fields
        {
            let persons = vec![&person1, &person2];
            let mut app = db.appender("foo")?;
            for p in &persons {
                app.append_row(params![&p.first_name, &p.last_name, p.dob])?;
            }
        }

        let count: i64 = db.query_row("SELECT count(*) FROM foo", [], |row| row.get(0))?;
        assert_eq!(count, 2);

        Ok(())
    }

    #[test]
    fn test_appender_error() -> Result<()> {
        let conn = Connection::open_in_memory()?;
        conn.execute(
            r"CREATE TABLE foo (
            foobar TEXT,
            foobar_int INT,
            foobar_split TEXT[] AS (split(trim(foobar), ','))
            );",
            [],
        )?;
        let mut appender = conn.appender("foo")?;
        match appender.append_row(params!["foo"]) {
            Err(Error::DuckDBFailure(.., Some(msg))) => {
                assert_eq!(msg, "Call to EndRow before all columns have been appended to!")
            }
            Err(err) => panic!("unexpected error: {err:?}"),
            Ok(_) => panic!("expected an error but got Ok"),
        }
        Ok(())
    }

    #[test]
    fn test_appender_foreign_key_constraint() -> Result<()> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch(
            r"
            CREATE TABLE parent (id INTEGER PRIMARY KEY);
            CREATE TABLE child (
                id INTEGER,
                parent_id INTEGER,
                FOREIGN KEY (parent_id) REFERENCES parent(id)
            );",
        )?;
        conn.execute("INSERT INTO parent VALUES (1)", [])?;

        let mut appender = conn.appender("child")?;
        appender.append_row(params![1, 999])?; // Invalid parent_id

        // Foreign key constraint should be checked during flush
        match appender.flush() {
            Err(Error::DuckDBFailure(_, Some(msg))) => {
                assert_eq!(
                    msg,
                    "Failed to append: Violates foreign key constraint because key \"id: 999\" does not exist in the referenced table"
                );
            }
            Err(e) => panic!("Expected foreign key constraint error, got: {e:?}"),
            Ok(_) => panic!("Expected foreign key constraint error, but flush succeeded"),
        }

        Ok(())
    }

    #[test]
    fn test_appender_defaults_and_column_switching() -> Result<()> {
        let db = Connection::open_in_memory()?;
        db.execute_batch("CREATE TABLE foo(a INT DEFAULT 99, b INT, c INT DEFAULT 7)")?;

        // Only provide column b; a and c should use their defaults
        {
            let mut app = db.appender_with_columns("foo", &["b"])?;
            app.append_row([Some(1)])?;
            app.append_row([Option::<i32>::None])?;
        }

        // Switch to a different active column set, then back to full width
        {
            let mut app = db.appender("foo")?;
            app.add_column("c")?;
            app.add_column("a")?;
            app.append_row([10, 1])?; // set c and a; b gets NULL

            app.clear_columns()?; // revert to all columns
            app.append_row([2, 3, 4])?;
        }

        let rows: Vec<(i32, Option<i32>, i32)> = db
            .prepare("SELECT a, b, c FROM foo ORDER BY a, b NULLS LAST")?
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
            .collect::<Result<_>>()?;

        assert_eq!(
            rows,
            vec![
                (1, None, 10),    // add_column path; b NULL, c set
                (2, Some(3), 4),  // clear_columns path; all provided
                (99, Some(1), 7), // defaults applied for a and c
                (99, None, 7)     // default + NULL
            ]
        );

        Ok(())
    }

    #[test]
    fn test_appender_with_columns_sequence_default() -> Result<()> {
        let db = Connection::open_in_memory()?;
        db.execute_batch(
            "CREATE SEQUENCE seq START 1;
             CREATE TABLE foo(id INTEGER DEFAULT nextval('seq'), name TEXT)",
        )?;

        {
            let mut app = db.appender_with_columns("foo", &["name"])?;
            app.append_row(["Alice"])?;
            app.append_row(["Bob"])?;
            app.append_row(["Charlie"])?;
        }

        let rows: Vec<(i32, String)> = db
            .prepare("SELECT id, name FROM foo ORDER BY id")?
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
            .collect::<Result<_>>()?;

        assert_eq!(
            rows,
            vec![(1, "Alice".into()), (2, "Bob".into()), (3, "Charlie".into())]
        );

        Ok(())
    }

    #[test]
    fn test_appender_with_columns_to_db_schema() -> Result<()> {
        let db = Connection::open_in_memory()?;
        db.execute_batch(
            "CREATE SCHEMA s;
             CREATE TABLE s.foo(a INTEGER DEFAULT 5, b INTEGER)",
        )?;

        {
            let mut app = db.appender_with_columns_to_db("foo", "s", &["b"])?;
            app.append_row([7])?;
        }

        let (a, b): (i32, i32) = db.query_row("SELECT a, b FROM s.foo", [], |row| Ok((row.get(0)?, row.get(1)?)))?;
        assert_eq!((a, b), (5, 7));
        Ok(())
    }

    #[test]
    fn test_appender_with_columns_to_catalog_and_db() -> Result<()> {
        let db = Connection::open_in_memory()?;
        db.execute_batch(
            "CREATE SCHEMA s;
             CREATE TABLE s.bar(a INTEGER DEFAULT 11, b INTEGER)",
        )?;

        {
            // Default in-memory catalog is "memory"
            let mut app = db.appender_with_columns_to_catalog_and_db("bar", "memory", "s", &["b"])?;
            app.append_row([9])?;
        }

        let (a, b): (i32, i32) = db.query_row("SELECT a, b FROM s.bar", [], |row| Ok((row.get(0)?, row.get(1)?)))?;
        assert_eq!((a, b), (11, 9));
        Ok(())
    }

    #[test]
    fn test_appender_decimal() -> Result<()> {
        let d1 = Decimal::from_i128_with_scale(11344, 4);
        let d2 = Decimal::from_i128_with_scale(12312, 3);
        let d3 = Decimal::from_i128_with_scale(-98765, 5);

        let conn = Connection::open_in_memory()?;
        conn.execute_batch("CREATE TABLE decimals (value DECIMAL(20, 10));")?;

        let mut appender = conn.appender("decimals")?;
        appender.append_row(params![d1])?;
        appender.append_row(params![d2])?;
        appender.append_row(params![d3])?;
        appender.flush()?;

        let results: Vec<Decimal> = conn
            .prepare("SELECT value FROM decimals ORDER BY value ASC")?
            .query_map([], |row| row.get(0))?
            .collect::<Result<Vec<Decimal>>>()?;

        assert_eq!(results, vec![d3, d1, d2]);

        Ok(())
    }

    #[test]
    fn test_appender_decimal_hugeint_upper_bits() -> Result<()> {
        let negative = Decimal::from_i128_with_scale(-7922816251426433759354395033_i128, 10);
        let positive = Decimal::from_i128_with_scale(7922816251426433759354395033_i128, 10);

        let conn = Connection::open_in_memory()?;
        conn.execute_batch("CREATE TABLE decimals (value DECIMAL(28, 10));")?;

        let mut appender = conn.appender("decimals")?;
        appender.append_row(params![negative])?;
        appender.append_row(params![positive])?;
        appender.flush()?;

        let results: Vec<Decimal> = conn
            .prepare("SELECT value FROM decimals ORDER BY value ASC")?
            .query_map([], |row| row.get(0))?
            .collect::<Result<Vec<Decimal>>>()?;

        assert_eq!(results, vec![negative, positive]);
        Ok(())
    }

    #[test]
    fn test_appender_decimal_boundary_values() -> Result<()> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch("CREATE TABLE decimals (value DECIMAL(29, 0));")?;

        let mut appender = conn.appender("decimals")?;
        appender.append_row(params![Decimal::ZERO])?;
        appender.append_row(params![Decimal::MAX])?;
        appender.flush()?;

        let results: Vec<Decimal> = conn
            .prepare("SELECT value FROM decimals ORDER BY value ASC")?
            .query_map([], |row| row.get(0))?
            .collect::<Result<Vec<Decimal>>>()?;

        assert_eq!(results, vec![Decimal::ZERO, Decimal::MAX]);
        Ok(())
    }
}

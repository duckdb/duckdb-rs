use std::{
    ffi::{c_char, c_void},
    fmt,
};

use ::arrow::array::RecordBatch;
use arrow_convert::{
    field::ArrowField,
    serialize::{ArrowSerialize, FlattenRecordBatch, TryIntoArrow},
};
use ffi::{duckdb_append_data_chunk, duckdb_appender_flush};
use itertools::Itertools;

use super::{ffi, Connection, Result};
#[cfg(feature = "appender-arrow")]
use crate::vtab::arrow::record_batch_to_duckdb_data_chunk;
use crate::{
    core::{ColumnInfo, DataChunk, DataChunkHandle, LogicalType, LogicalTypeHandle},
    error::result_from_duckdb_appender,
    types::{ToSqlOutput, ValueRef},
    AppenderParams, Error, ToSql,
};

/// Appender for fast import data
pub struct Appender<'conn> {
    conn: &'conn Connection,
    app: ffi::duckdb_appender,

    /// column layout stored as tree, for fast access and to create data chunks
    columns: Vec<ColumnInfo>,

    /// the type of the columns this table stores
    column_types: Vec<LogicalType>,

    /// chunks that have not been flushed
    chunks: Vec<DataChunk>,
}

impl Appender<'_> {
    #[inline]
    pub(super) fn new(conn: &Connection, app: ffi::duckdb_appender) -> Appender<'_> {
        let column_count = unsafe { ffi::duckdb_appender_column_count(app) };

        // initialize column_types
        let column_types = (0..column_count)
            .map(|i| {
                let handle = unsafe { LogicalTypeHandle::new(ffi::duckdb_appender_column_type(app, i)) };
                LogicalType::from(handle)
            })
            .collect::<Vec<_>>();

        // initialize columns
        let columns: Vec<ColumnInfo> = (0..column_count)
            .map(|i| ColumnInfo::new(&column_types[i as usize]))
            .collect();

        let chunks = vec![];

        Appender {
            conn,
            app,
            columns,
            column_types,
            chunks,
        }
    }

    #[cfg(feature = "appender-arrow")]
    fn append_record_batch_inner(&mut self, record_batch: Vec<RecordBatch>) -> Result<()> {
        for batch in record_batch {
            let mut data_chunk = DataChunk::new(batch.num_rows(), &self.column_types, &self.columns);
            record_batch_to_duckdb_data_chunk(&batch, &mut data_chunk).map_err(|_op| Error::AppendError)?;
            self.chunks.push(data_chunk);
        }
        Ok(())
    }

    /// Append one record_batch
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// # use duckdb::{Connection, Result, params};
    ///   use arrow::record_batch::RecordBatch;
    /// fn insert_record_batch(conn: &Connection,record_batch:RecordBatch) -> Result<()> {
    ///     let mut app = conn.appender("foo")?;
    ///     app.append_record_batch(record_batch)?;
    ///     Ok(())
    /// }
    /// ```
    ///
    /// # Failure
    ///
    /// Will return `Err` if append column count not the same with the table schema
    #[cfg(feature = "appender-arrow")]
    pub fn append_record_batch(&mut self, record_batch: RecordBatch) -> Result<()> {
        let batches = split_batch_for_data_chunk_capacity(record_batch, DataChunkHandle::max_capacity());
        self.append_record_batch_inner(batches)
    }

    /// Append rows to the appender.
    ///
    /// # Arguments
    ///
    /// * `records` - An iterator over the records to be appended.
    /// * `expand_struct` - A boolean indicating whether to expand the struct.
    #[cfg(feature = "appender-arrow")]
    pub fn append_rows_arrow<'a, T>(
        &mut self,
        records: impl IntoIterator<Item = &'a T>,
        expand_struct: bool,
    ) -> Result<()>
    where
        T: ArrowSerialize + ArrowField<Type = T> + 'static,
    {
        let batches = records
            .into_iter()
            .chunks(DataChunkHandle::max_capacity())
            .into_iter()
            .map(|chunk| {
                let batch: RecordBatch = chunk.try_into_arrow()?;
                if expand_struct {
                    batch.flatten()
                } else {
                    Ok(batch)
                }
            })
            .collect::<Result<Vec<_>, _>>()?;

        self.append_record_batch_inner(batches)
    }

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
        let _ = unsafe { ffi::duckdb_appender_begin_row(self.app) };
        params.__bind_in(self)?;
        // NOTE: we only check end_row return value
        let rc = unsafe { ffi::duckdb_appender_end_row(self.app) };
        result_from_duckdb_appender(rc, &mut self.app)
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
            _ => unreachable!("not supported"),
        };
        if rc != 0 {
            return Err(Error::AppendError);
        }
        Ok(())
    }

    /// Flush data into DB
    #[inline]
    pub fn flush(&mut self) -> Result<()> {
        // append data chunks
        for chunk in self.chunks.drain(..) {
            let rc = unsafe { duckdb_append_data_chunk(self.app, chunk.get_handle().ptr) };
            result_from_duckdb_appender(rc, &mut self.app)?;
        }

        let rc = unsafe { duckdb_appender_flush(self.app) };
        result_from_duckdb_appender(rc, &mut self.app)
    }
}

impl Drop for Appender<'_> {
    fn drop(&mut self) {
        if !self.app.is_null() {
            self.flush().expect("Failed to flush appender");
            unsafe {
                ffi::duckdb_appender_close(self.app);
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

/// Splits a given `RecordBatch` into multiple smaller batches based on the maximum number of rows per batch.
fn split_batch_for_data_chunk_capacity(batch: RecordBatch, data_chunk_max_rows: usize) -> Vec<RecordBatch> {
    let rows_per_batch = data_chunk_max_rows.max(1);
    let n_batches = (batch.num_rows() / rows_per_batch).max(1);
    let mut out = Vec::with_capacity(n_batches + 1);

    let mut offset = 0;
    while offset < batch.num_rows() {
        let length = (rows_per_batch).min(batch.num_rows() - offset);
        out.push(batch.slice(offset, length));

        offset += length;
    }

    out
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow::{
        array::{Int8Array, StringArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };

    use crate::{Connection, Result};

    #[cfg(feature = "appender-arrow")]
    #[test]
    fn test_append_record_batch() -> Result<()> {
        let db = Connection::open_in_memory()?;
        db.execute_batch("CREATE TABLE foo(id TINYINT not null,area TINYINT not null,name Varchar)")?;
        {
            let id_array = Int8Array::from(vec![1, 2, 3, 4, 5]);
            let area_array = Int8Array::from(vec![11, 22, 33, 44, 55]);
            let name_array = StringArray::from(vec![Some("11"), None, None, Some("44"), None]);
            let schema = Schema::new(vec![
                Field::new("id", DataType::Int8, true),
                Field::new("area", DataType::Int8, true),
                Field::new("area", DataType::Utf8, true),
            ]);
            let record_batch = RecordBatch::try_new(
                Arc::new(schema),
                vec![Arc::new(id_array), Arc::new(area_array), Arc::new(name_array)],
            )
            .unwrap();
            let mut app = db.appender("foo")?;
            app.append_record_batch(record_batch)?;
        }
        let mut stmt = db.prepare("SELECT id, area,name  FROM foo")?;
        let rbs: Vec<RecordBatch> = stmt.query_arrow([])?.collect();
        assert_eq!(rbs.iter().map(|op| op.num_rows()).sum::<usize>(), 5);
        Ok(())
    }

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

    // Waiting https://github.com/duckdb/duckdb/pull/3405
    #[cfg(feature = "uuid")]
    #[test]
    #[ignore = "not supported for now"]
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

        use crate::params;

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
    fn test_appender_error() -> Result<(), crate::Error> {
        let conn = Connection::open_in_memory()?;
        conn.execute(
            r"CREATE TABLE foo (
            foobar TEXT,
            foobar_split TEXT[] AS (split(trim(foobar), ','))
            );",
            [],
        )?;
        let mut appender = conn.appender("foo")?;
        match appender.append_row(["foo"]) {
            Err(crate::Error::DuckDBFailure(.., Some(msg))) => {
                assert_eq!(msg, "Call to EndRow before all rows have been appended to!")
            }
            _ => panic!("expected error"),
        }
        Ok(())
    }
}

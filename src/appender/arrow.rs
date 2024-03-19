use super::{ffi, Appender, Result};
use crate::{
    error::result_from_duckdb_appender,
    vtab::{record_batch_to_duckdb_data_chunk, to_duckdb_logical_type, DataChunk, LogicalType},
    Error,
};
use arrow::record_batch::RecordBatch;
use ffi::duckdb_append_data_chunk;

impl Appender<'_> {
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
    #[inline]
    pub fn append_record_batch(&mut self, record_batch: RecordBatch) -> Result<()> {
        let schema = record_batch.schema();
        let mut logical_type: Vec<LogicalType> = vec![];
        for field in schema.fields() {
            let logical_t = to_duckdb_logical_type(field.data_type())
                .map_err(|_op| Error::ArrowTypeToDuckdbType(field.to_string(), field.data_type().clone()))?;
            logical_type.push(logical_t);
        }

        let mut data_chunk = DataChunk::new(&logical_type);
        record_batch_to_duckdb_data_chunk(&record_batch, &mut data_chunk).map_err(|_op| Error::AppendError)?;

        let rc = unsafe { duckdb_append_data_chunk(self.app, data_chunk.get_ptr()) };
        result_from_duckdb_appender(rc, self.app)
    }
}

#[cfg(test)]
mod test {
    use crate::{Connection, Result};
    use arrow::{
        array::{Int8Array, StringArray, TimestampMicrosecondArray},
        datatypes::{DataType, Field, Schema, TimeUnit},
        record_batch::RecordBatch,
    };
    use std::sync::Arc;

    #[test]
    fn test_append_record_batch() -> Result<()> {
        let db = Connection::open_in_memory()?;
        db.execute_batch(
            "CREATE TABLE foo(id TINYINT not null, area TINYINT not null, name Varchar, ts Timestamp NOT NULL)",
        )?;
        {
            let id_array = Int8Array::from(vec![1, 2, 3, 4, 5]);
            let area_array = Int8Array::from(vec![11, 22, 33, 44, 55]);
            let name_array = StringArray::from(vec![Some("11"), None, None, Some("44"), None]);
            let ts_array = TimestampMicrosecondArray::from(vec![1, 200, 3200, 4500, 54000]);
            let schema = Schema::new(vec![
                Field::new("id", DataType::Int8, true),
                Field::new("area", DataType::Int8, true),
                Field::new("name", DataType::Utf8, true),
                Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, None), true),
            ]);
            let record_batch = RecordBatch::try_new(
                Arc::new(schema),
                vec![
                    Arc::new(id_array),
                    Arc::new(area_array),
                    Arc::new(name_array),
                    Arc::new(ts_array),
                ],
            )
            .unwrap();
            let mut app = db.appender("foo")?;
            app.append_record_batch(record_batch)?;
        }
        let mut stmt = db.prepare("SELECT * FROM foo")?;
        let rbs: Vec<RecordBatch> = stmt.query_arrow([])?.collect();
        assert_eq!(rbs.iter().map(|op| op.num_rows()).sum::<usize>(), 5);
        Ok(())
    }
}

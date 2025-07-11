use std::ffi::CString;

use arrow::ffi_stream::FFI_ArrowArrayStream;

use crate::{error::error_from_duckdb_code, ffi, Connection, Error, Result};

impl Connection {
    /// Registers a temporary view in DuckDB based on an Arrow stream.
    ///
    /// Note the underlying `duckdb_arrow_scan` C API is marked for deprecation.
    /// However, similar functionality will be preserved in a new yet-to-be-determined API.
    ///
    /// # Arguments
    ///
    /// * `view_name`: The name of the view to register
    /// * `arrow_scan`: The Arrow stream to register
    pub fn register_arrow_scan_view(&self, view_name: &str, arrow_scan: &FFI_ArrowArrayStream) -> Result<()> {
        let conn = self.db.borrow_mut().con;
        let c_str = CString::new(view_name).map_err(Error::NulError)?;
        let transmuted_arrow_scan = arrow_scan as *const _ as ffi::duckdb_arrow_stream;
        let r = unsafe { ffi::duckdb_arrow_scan(conn, c_str.as_ptr(), transmuted_arrow_scan) };
        if r != ffi::DuckDBSuccess {
            return error_from_duckdb_code(r, Some("duckdb_arrow_scan failed to register view".to_string()));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{
        array::{Int32Array, StringArray},
        datatypes::{DataType, Field, Schema, SchemaRef},
        error::ArrowError,
        record_batch::RecordBatch,
    };
    use std::sync::Arc;

    /// A simple RecordBatchReader implementation for testing
    struct TestRecordBatchReader {
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
        index: usize,
    }

    impl TestRecordBatchReader {
        fn new(batches: Vec<RecordBatch>) -> Self {
            // All batches should have the same schema, so we can use the first one
            let schema = batches[0].schema();
            TestRecordBatchReader {
                schema,
                batches,
                index: 0,
            }
        }
    }

    impl Iterator for TestRecordBatchReader {
        type Item = std::result::Result<RecordBatch, ArrowError>;

        fn next(&mut self) -> Option<Self::Item> {
            if self.index < self.batches.len() {
                let batch = self.batches[self.index].clone();
                self.index += 1;
                Some(Ok(batch))
            } else {
                None
            }
        }
    }

    impl arrow::record_batch::RecordBatchReader for TestRecordBatchReader {
        fn schema(&self) -> SchemaRef {
            self.schema.clone()
        }
    }

    #[test]
    fn test_register_arrow_scan_view() -> Result<()> {
        // Create a test database connection
        let db = Connection::open_in_memory()?;

        // Create Arrow arrays for test data
        let id_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let name_array = StringArray::from(vec!["Alice", "Bob", "Charlie", "Dave", "Eve"]);

        // Create a schema and record batch
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let record_batch = RecordBatch::try_new(schema, vec![Arc::new(id_array), Arc::new(name_array)])
            .expect("Failed to create record batch");

        // Create a RecordBatchReader
        let reader = TestRecordBatchReader::new(vec![record_batch]);

        // Convert to FFI_ArrowArrayStream - this needs to live longer than any queries to the view
        let stream = arrow::ffi_stream::FFI_ArrowArrayStream::new(
            Box::new(reader) as Box<dyn arrow::record_batch::RecordBatchReader + Send>
        );

        // Register the view
        db.register_arrow_scan_view("test_view", &stream)?;

        // Query the view to verify it works
        let rows = db
            .prepare("SELECT id, name FROM test_view ORDER BY id")?
            .query_map([], |row| Ok((row.get::<_, i32>(0)?, row.get::<_, String>(1)?)))?
            .collect::<Result<Vec<_>>>()?;

        // Verify results
        assert_eq!(rows.len(), 5);
        assert_eq!(rows[0], (1, "Alice".to_string()));
        assert_eq!(rows[1], (2, "Bob".to_string()));
        assert_eq!(rows[2], (3, "Charlie".to_string()));
        assert_eq!(rows[3], (4, "Dave".to_string()));
        assert_eq!(rows[4], (5, "Eve".to_string()));

        Ok(())
    }

    #[test]
    fn test_register_arrow_scan_view_with_nulls() -> Result<()> {
        // Create a test database connection
        let db = Connection::open_in_memory()?;

        // Create Arrow arrays with null values
        let id_array = Int32Array::from(vec![Some(1), Some(2), None, Some(4), Some(5)]);
        let name_array = StringArray::from(vec![Some("Alice"), None, Some("Charlie"), Some("Dave"), Some("Eve")]);

        // Create a schema and record batch
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("name", DataType::Utf8, true),
        ]));

        let record_batch = RecordBatch::try_new(schema, vec![Arc::new(id_array), Arc::new(name_array)])
            .expect("Failed to create record batch");

        // Create a RecordBatchReader
        let reader = TestRecordBatchReader::new(vec![record_batch]);

        // Convert to FFI_ArrowArrayStream
        let stream = arrow::ffi_stream::FFI_ArrowArrayStream::new(
            Box::new(reader) as Box<dyn arrow::record_batch::RecordBatchReader + Send>
        );

        // Register the view
        db.register_arrow_scan_view("test_view_nulls", &stream)?;

        // Query the view to verify it works, including handling of nulls
        let rows = db
            .prepare("SELECT id, name FROM test_view_nulls ORDER BY id NULLS LAST")?
            .query_map([], |row| {
                let id: Option<i32> = row.get(0)?;
                let name: Option<String> = row.get(1)?;
                Ok((id, name))
            })?
            .collect::<Result<Vec<_>>>()?;

        // Verify results
        assert_eq!(rows.len(), 5);
        assert_eq!(rows[0], (Some(1), Some("Alice".to_string())));
        assert_eq!(rows[1], (Some(2), None));
        assert_eq!(rows[2], (Some(4), Some("Dave".to_string())));
        assert_eq!(rows[3], (Some(5), Some("Eve".to_string())));
        assert_eq!(rows[4], (None, Some("Charlie".to_string())));

        Ok(())
    }

    #[test]
    fn test_register_arrow_scan_view_multiple_batches() -> Result<()> {
        // Create a test database connection
        let db = Connection::open_in_memory()?;

        // Create schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        // Create first batch
        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
            ],
        )
        .expect("Failed to create record batch");

        // Create second batch
        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![4, 5])),
                Arc::new(StringArray::from(vec!["Dave", "Eve"])),
            ],
        )
        .expect("Failed to create record batch");

        // Create a RecordBatchReader with multiple batches
        let reader = TestRecordBatchReader::new(vec![batch1, batch2]);

        // Convert to FFI_ArrowArrayStream
        let stream = arrow::ffi_stream::FFI_ArrowArrayStream::new(
            Box::new(reader) as Box<dyn arrow::record_batch::RecordBatchReader + Send>
        );

        // Register the view
        db.register_arrow_scan_view("test_view_multi", &stream)?;

        // Query all data to verify correct ordering
        let rows = db
            .prepare("SELECT id, name FROM test_view_multi ORDER BY id")?
            .query_map([], |row| Ok((row.get::<_, i32>(0)?, row.get::<_, String>(1)?)))?
            .collect::<Result<Vec<_>>>()?;

        // Verify results
        assert_eq!(rows.len(), 5);
        assert_eq!(rows[0], (1, "Alice".to_string()));
        assert_eq!(rows[1], (2, "Bob".to_string()));
        assert_eq!(rows[2], (3, "Charlie".to_string()));
        assert_eq!(rows[3], (4, "Dave".to_string()));
        assert_eq!(rows[4], (5, "Eve".to_string()));

        Ok(())
    }
}

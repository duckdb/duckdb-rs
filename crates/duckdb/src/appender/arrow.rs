use super::{Appender, Result, ffi};
use crate::{
    Error,
    arrow_interop::{record_batch_to_duckdb_data_chunk, to_duckdb_logical_type_for_field},
    core::DataChunkHandle,
    error::{arrow_conversion_failure, result_from_duckdb_appender},
};
use arrow::record_batch::RecordBatch;
use ffi::{duckdb_append_data_chunk, duckdb_vector_size};

impl Appender<'_> {
    /// Append one record batch
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
        let fields = schema.fields();
        let capacity = fields.len();
        let mut logical_types = Vec::with_capacity(capacity);
        for field in fields.iter() {
            logical_types.push(to_duckdb_logical_type_for_field(field).map_err(|err| {
                Error::ArrowTypeToDuckdbType(format!("{}: {err}", field.name()), field.data_type().clone())
            })?);
        }

        let vector_size = unsafe { duckdb_vector_size() } as usize;
        let num_rows = record_batch.num_rows();

        // Process record batch in chunks that fit within DuckDB's vector size
        let mut offset = 0;
        while offset < num_rows {
            let slice_len = std::cmp::min(vector_size, num_rows - offset);
            let slice = record_batch.slice(offset, slice_len);

            let mut data_chunk = DataChunkHandle::new(&logical_types);
            record_batch_to_duckdb_data_chunk(&slice, &mut data_chunk).map_err(|err| {
                arrow_conversion_failure("Could not convert Arrow record batch to DuckDB data chunk", err)
            })?;

            let rc = unsafe { duckdb_append_data_chunk(self.app, data_chunk.get_ptr()) };
            result_from_duckdb_appender(rc, &mut self.app)?;

            offset += slice_len;
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::{
        Connection, Error, Result,
        arrow_interop::{
            UUID_BYTE_WIDTH, UUID_EXTENSION_NAME,
            test_support::{uuid_array, uuid_field, uuid_metadata},
        },
    };
    use arrow::{
        array::{ArrayRef, FixedSizeBinaryArray, Int8Array, Int32Array, StringArray},
        buffer::Buffer,
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use std::sync::Arc;
    use uuid::Uuid;

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
                Field::new("name", DataType::Utf8, true),
            ]);
            let record_batch = RecordBatch::try_new(
                Arc::new(schema),
                vec![Arc::new(id_array), Arc::new(area_array), Arc::new(name_array)],
            )
            .unwrap();
            let mut app = db.appender("foo")?;
            app.append_record_batch(record_batch)?;
        }
        let mut stmt = db.prepare("SELECT id, area, name FROM foo")?;
        let rbs: Vec<RecordBatch> = stmt.query_arrow([])?.collect();
        assert_eq!(rbs.iter().map(|op| op.num_rows()).sum::<usize>(), 5);
        Ok(())
    }

    #[test]
    fn test_append_record_batch_uuid_extension() -> Result<()> {
        let db = Connection::open_in_memory()?;
        db.execute_batch("CREATE TABLE foo(pos INTEGER, id UUID)")?;

        let vector_size = unsafe { crate::ffi::duckdb_vector_size() } as usize;
        let record_count = vector_size + 3;
        let uuids = (0..record_count)
            .map(|i| Uuid::from_u128(0xa1a2a3a4b1b2c1c2d1d2d3d4d5d60000 + i as u128))
            .collect::<Vec<_>>();
        let nulls = (0..record_count)
            .map(|i| i != 1 && i != vector_size)
            .collect::<Vec<_>>();
        let schema = Schema::new(vec![Field::new("pos", DataType::Int32, false), uuid_field("id")]);
        let record_batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int32Array::from((0..record_count as i32).collect::<Vec<_>>())) as ArrayRef,
                Arc::new(uuid_array(&uuids, Some(nulls))) as ArrayRef,
            ],
        )
        .unwrap();

        {
            let mut app = db.appender("foo")?;
            app.append_record_batch(record_batch)?;
        }

        let count: usize = db.query_row("SELECT COUNT(*) FROM foo", [], |row| row.get(0))?;
        assert_eq!(count, record_count);

        let non_null_count: usize = db.query_row("SELECT COUNT(id) FROM foo", [], |row| row.get(0))?;
        assert_eq!(non_null_count, record_count - 2);

        let first: String = db.query_row("SELECT id::VARCHAR FROM foo WHERE pos = 0", [], |row| row.get(0))?;
        assert_eq!(first, uuids[0].to_string());

        let null_id: Option<String> =
            db.query_row("SELECT id::VARCHAR FROM foo WHERE pos = 1", [], |row| row.get(0))?;
        assert_eq!(null_id, None);

        let after_boundary_pos = (vector_size + 1) as i32;
        let after_boundary: String = db.query_row(
            "SELECT id::VARCHAR FROM foo WHERE pos = ?",
            [after_boundary_pos],
            |row| row.get(0),
        )?;
        assert_eq!(after_boundary, uuids[after_boundary_pos as usize].to_string());

        Ok(())
    }

    #[test]
    fn test_append_record_batch_uuid_extension_rejects_invalid_storage() -> Result<()> {
        let db = Connection::open_in_memory()?;
        db.execute_batch("CREATE TABLE foo(id UUID)")?;

        let schema = Schema::new(vec![
            Field::new("id", DataType::FixedSizeBinary(8), true).with_metadata(uuid_metadata()),
        ]);
        let record_batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(FixedSizeBinaryArray::new(
                8,
                Buffer::from_vec(vec![0; 8]),
                None,
            ))],
        )
        .unwrap();

        let mut app = db.appender("foo")?;
        let err = app.append_record_batch(record_batch).unwrap_err();
        assert!(matches!(&err, Error::ArrowTypeToDuckdbType(..)));
        let expected = format!("{UUID_EXTENSION_NAME} requires FixedSizeBinary({UUID_BYTE_WIDTH})");
        assert!(
            err.to_string().contains(&expected) && err.to_string().contains("FixedSizeBinary(8)"),
            "unexpected error: {err}"
        );

        Ok(())
    }

    #[test]
    fn test_append_record_batch_large() -> Result<()> {
        let record_count = usize::pow(2, 16) + 1;
        let db = Connection::open_in_memory()?;
        db.execute_batch("CREATE TABLE foo(id INT)")?;
        {
            let id_array = Int32Array::from((0..record_count as i32).collect::<Vec<_>>());
            let schema = Schema::new(vec![Field::new("id", DataType::Int32, true)]);
            let record_batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(id_array)]).unwrap();
            let mut app = db.appender("foo")?;
            app.append_record_batch(record_batch)?;
        }
        let count: usize = db.query_row("SELECT COUNT(*) FROM foo", [], |row| row.get(0))?;
        assert_eq!(count, record_count);

        // Verify the data is correct
        let sum: i64 = db.query_row("SELECT SUM(id) FROM foo", [], |row| row.get(0))?;
        let expected_sum: i64 = (0..record_count as i64).sum();
        assert_eq!(sum, expected_sum);

        Ok(())
    }
}

use std::ptr;

use super::{Appender, Result, ffi};
use crate::{
    Connection, Error,
    arrow_interop::to_duckdb_logical_type_for_field,
    core::DataChunkHandle,
    error::{arrow_conversion_failure, result_from_duckdb_appender, result_from_duckdb_error_data},
};
use arrow::{
    array::{Array, StructArray},
    datatypes::Schema,
    ffi::{FFI_ArrowArray, FFI_ArrowSchema},
    record_batch::RecordBatch,
};
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
        // DuckDB's importer does not check `arrow.uuid` storage width. This
        // also rejects types the Rust type mapping does not support.
        for field in schema.fields() {
            to_duckdb_logical_type_for_field(field).map_err(|err| {
                Error::ArrowTypeToDuckdbType(format!("{}: {err}", field.name()), field.data_type().clone())
            })?;
        }

        let converted_schema = ConvertedArrowSchema::new(self.conn, &schema)?;
        let vector_size = unsafe { duckdb_vector_size() } as usize;
        let num_rows = record_batch.num_rows();

        // Process record batch in chunks that fit within DuckDB's vector size
        let mut offset = 0;
        while offset < num_rows {
            let slice_len = std::cmp::min(vector_size, num_rows - offset);
            let data_chunk = converted_schema.import(record_batch.slice(offset, slice_len))?;

            let rc = unsafe { duckdb_append_data_chunk(self.app, data_chunk.get_ptr()) };
            result_from_duckdb_appender(rc, &mut self.app)?;

            offset += slice_len;
        }

        Ok(())
    }
}

/// An Arrow schema converted by DuckDB, used to import record batches with
/// DuckDB's own Arrow conversion.
struct ConvertedArrowSchema {
    connection: ffi::duckdb_connection,
    ptr: ffi::duckdb_arrow_converted_schema,
}

impl ConvertedArrowSchema {
    fn new(conn: &Connection, schema: &Schema) -> Result<Self> {
        let mut ffi_schema = FFI_ArrowSchema::try_from(schema)
            .map_err(|err| arrow_conversion_failure("Could not export Arrow schema", err))?;
        let connection = conn.db.borrow().con;
        let mut converted = Self {
            connection,
            ptr: ptr::null_mut(),
        };
        let error = unsafe {
            ffi::duckdb_schema_from_arrow(
                connection,
                &mut ffi_schema as *mut FFI_ArrowSchema as *mut ffi::ArrowSchema,
                &mut converted.ptr,
            )
        };
        unsafe { result_from_duckdb_error_data(error) }?;
        Ok(converted)
    }

    /// DuckDB takes ownership of the batch's Arrow data; the returned chunk
    /// keeps it alive.
    fn import(&self, batch: RecordBatch) -> Result<DataChunkHandle> {
        let mut ffi_array = FFI_ArrowArray::new(&StructArray::from(batch).into_data());
        let mut chunk: ffi::duckdb_data_chunk = ptr::null_mut();
        let error = unsafe {
            ffi::duckdb_data_chunk_from_arrow(
                self.connection,
                &mut ffi_array as *mut FFI_ArrowArray as *mut ffi::ArrowArray,
                self.ptr,
                &mut chunk,
            )
        };
        let chunk = unsafe { DataChunkHandle::new_owned(chunk) };
        unsafe { result_from_duckdb_error_data(error) }?;
        Ok(chunk)
    }
}

impl Drop for ConvertedArrowSchema {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            unsafe { ffi::duckdb_destroy_arrow_converted_schema(&mut self.ptr) };
        }
    }
}

#[cfg(test)]
mod test {
    use crate::{
        Connection, Error, Result,
        arrow_interop::test_support::{uuid_array, uuid_field, uuid_metadata},
        types::extension::{UUID_BYTE_WIDTH, UUID_EXTENSION_NAME},
    };
    use arrow::{
        array::{Array, ArrayRef, AsArray, FixedSizeBinaryArray, Int8Array, Int32Array, StringArray, StructArray},
        buffer::Buffer,
        compute::cast,
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

    #[test]
    fn test_append_record_batch_parquet_variant_roundtrip() -> Result<()> {
        let db = Connection::open_in_memory()?;
        db.execute_batch(
            "CREATE TABLE src(v VARIANT);
             INSERT INTO src VALUES ({'a': 42}::VARIANT), (NULL);
             CREATE TABLE dst(v VARIANT);",
        )?;

        let batches: Vec<RecordBatch> = {
            let mut stmt = db.prepare("SELECT v FROM src")?;
            stmt.query_arrow([])?.collect()
        };
        {
            let mut app = db.appender("dst")?;
            for batch in batches {
                app.append_record_batch(batch)?;
            }
            app.flush()?;
        }

        let rendered: String = db.query_row("SELECT v::VARCHAR FROM dst WHERE v IS NOT NULL", [], |row| row.get(0))?;
        assert_eq!(rendered, "{'a': 42}");
        let nulls: i64 = db.query_row("SELECT COUNT(*) FROM dst WHERE v IS NULL", [], |row| row.get(0))?;
        assert_eq!(nulls, 1);

        Ok(())
    }

    #[test]
    fn test_append_record_batch_parquet_variant_dictionary_encoded_value() -> Result<()> {
        let db = Connection::open_in_memory()?;
        db.execute_batch("CREATE TABLE dst(v VARIANT)")?;
        let batches: Vec<RecordBatch> = db.prepare("SELECT {'a': 42}::VARIANT AS v")?.query_arrow([])?.collect();
        let exported = batches[0].column(0).as_struct();
        let binary = |name: &str| cast(exported.column_by_name(name).unwrap(), &DataType::Binary).unwrap();
        let metadata = binary("metadata");
        let value = cast(
            &binary("value"),
            &DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Binary)),
        )
        .unwrap();
        let storage = StructArray::from(vec![
            (
                Arc::new(Field::new("metadata", metadata.data_type().clone(), true)),
                metadata,
            ),
            (Arc::new(Field::new("value", value.data_type().clone(), true)), value),
        ]);
        let field = Field::new("v", storage.data_type().clone(), true)
            .with_metadata(batches[0].schema().field(0).metadata().clone());
        let batch = RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![Arc::new(storage)]).unwrap();

        let mut app = db.appender("dst")?;
        app.append_record_batch(batch)?;
        app.flush()?;
        drop(app);

        let rendered: String = db.query_row("SELECT v::VARCHAR FROM dst", [], |row| row.get(0))?;
        assert_eq!(rendered, "{'a': 42}");
        Ok(())
    }
}

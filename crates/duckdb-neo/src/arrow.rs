//! Arrow C Data Interface conversion.
//!
//! Exported `ArrowSchema` and `ArrowArray` values follow Arrow's `release`
//! callback ownership convention. Importing copies array data into DuckDB
//! chunks; the caller retains ownership of the input array.

use libduckdb_sys::v2::DuckDBStr;

use crate::{
    Result, check_api_call, check_api_call_no_err,
    connection::Context,
    data_chunk::{DataChunk, DataChunkRef},
    ffi,
    logical_type::LogicalType,
    schema::Schema,
};

/// Converts DuckDB chunks into Arrow arrays using a fixed schema.
///
/// Feed chunks with [`Self::append`] and drain completed arrays with [`Self::next_array`].
///
/// The context's Arrow settings are copied at creation, so the exporter does
/// not borrow the context.
pub struct ArrowExporter {
    handle: ffi::duckdb_v2_arrow_exporter_handle,
}

impl ArrowExporter {
    /// Capture the column schema and Arrow settings from an active transaction.
    ///
    /// # Panics
    /// Panics if `logical_types` and `names` differ in length.
    ///
    /// `None` or `Some(0)` imposes no batch-size limit.
    pub fn new(
        context: &Context,
        logical_types: &[LogicalType],
        names: &[String],
        batch_size: Option<usize>,
    ) -> Result<Self> {
        let logical_type_handles = logical_types.iter().map(|x| x.handle).collect::<Vec<_>>();
        let name_ptrs = names.iter().map(|x| x.into()).collect::<Vec<DuckDBStr<'_>>>();

        assert_eq!(logical_types.len(), names.len());

        let handle = check_api_call!(
            ffi::duckdb_v2_arrow_exporter_create,
            **context,
            logical_type_handles.as_ptr(),
            name_ptrs.as_ptr(),
            logical_type_handles.len() as u64,
            batch_size.unwrap_or(0) as u64,
            RET
        )?;

        Ok(ArrowExporter { handle })
    }

    /// Copy a chunk into Arrow buffers, flushing any partial batch when `flush` is true.
    ///
    /// Accepts an owned [`DataChunk`] or a chunk borrowed from a callback; the
    /// chunk is only read.
    pub fn append(&mut self, chunk: &DataChunkRef<'_>, flush: bool) -> Result<()> {
        // With `consume` false DuckDB leaves the handle untouched, so a local copy suffices.
        let mut handle = chunk.handle;
        check_api_call!(
            ffi::duckdb_v2_arrow_exporter_append,
            self.handle,
            &mut handle,
            false,
            flush
        )
    }

    /// Return the Arrow schema; the caller must invoke its `release` callback when done.
    pub fn schema(&self) -> Result<ffi::ArrowSchema> {
        check_api_call!(ffi::duckdb_v2_arrow_exporter_get_schema, self.handle, RET)
    }

    /// Take the next completed array, or an array with no `release` callback when none is ready.
    ///
    /// The caller must invoke each returned array's `release` callback when present.
    pub fn next_array(&self) -> Result<ffi::ArrowArray> {
        check_api_call!(ffi::duckdb_v2_arrow_exporter_next_array, self.handle, RET)
    }
}

impl Drop for ArrowExporter {
    fn drop(&mut self) {
        check_api_call_no_err!(ffi::duckdb_v2_arrow_exporter_destroy, &mut self.handle).unwrap();
    }
}

/// Converts Arrow arrays of a fixed schema into DuckDB chunks.
///
/// The importer runs every conversion under the context it was created with,
/// so it borrows that context and cannot outlive it.
pub struct ArrowImporter<'ctx> {
    handle: ffi::duckdb_v2_arrow_importer_handle,
    _context: std::marker::PhantomData<&'ctx Context>,
}

impl<'ctx> ArrowImporter<'ctx> {
    /// Resolve an Arrow schema using the context's active transaction.
    ///
    /// The schema is only read: the caller keeps ownership and must still
    /// release it. `None` or `Some(0)` imposes no chunk-size limit.
    pub fn new(context: &'ctx Context, schema: &mut ffi::ArrowSchema, batch_size: Option<usize>) -> Result<Self> {
        let handle = check_api_call!(
            ffi::duckdb_v2_arrow_importer_create,
            **context,
            schema,
            batch_size.unwrap_or(0) as u64,
            RET
        )?;
        Ok(ArrowImporter {
            handle,
            _context: std::marker::PhantomData,
        })
    }

    /// Queue an array for copying into chunks; `flush` releases any final partial batch.
    ///
    /// The caller keeps ownership of the array, and every produced chunk is a
    /// copy that does not depend on it. Prefer [`Self::append_consumed`], which
    /// avoids the copy and has no safety requirements.
    ///
    /// # Safety
    ///
    /// The importer keeps a pointer to `array` until it is drained, meaning
    /// [`Self::chunk`] has returned `Ok(None)`. Until then the array must not be
    /// released, mutated, or moved. If draining fails, those requirements hold
    /// until the importer is dropped.
    pub unsafe fn append_referenced(&self, array: &mut ffi::ArrowArray, flush: bool) -> Result<()> {
        check_api_call!(ffi::duckdb_v2_arrow_importer_append, self.handle, array, false, flush)
    }

    /// Hand an array over for zero-copy conversion; `flush` releases any final partial batch.
    ///
    /// Produced chunks reference the array's buffers and keep them alive, so
    /// they stay valid after the importer is dropped. On error the array is
    /// released.
    pub fn append_consumed(&self, mut array: ffi::ArrowArray, flush: bool) -> Result<()> {
        if let Err(err) = check_api_call!(
            ffi::duckdb_v2_arrow_importer_append,
            self.handle,
            &mut array,
            true,
            flush
        ) {
            if let Some(release) = array.release {
                unsafe { release(&mut array) };
            }
            return Err(err);
        }
        Ok(())
    }

    /// Return the owned DuckDB schema resolved from the Arrow schema.
    pub fn schema(&self) -> Result<Schema> {
        Ok(Schema {
            handle: check_api_call!(ffi::duckdb_v2_arrow_importer_get_schema, self.handle, RET)?,
        })
    }

    /// Take the next chunk, or return `None` when the current array is drained.
    pub fn chunk(&self) -> Result<Option<DataChunk<'static>>> {
        let handle = check_api_call!(ffi::duckdb_v2_arrow_importer_next_chunk, self.handle, RET)?;

        if handle.is_null() {
            Ok(None)
        } else {
            Ok(Some(DataChunk::new(handle, true)))
        }
    }
}

impl Drop for ArrowImporter<'_> {
    fn drop(&mut self) {
        check_api_call_no_err!(ffi::duckdb_v2_arrow_importer_destroy, &mut self.handle).unwrap();
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        Parameters,
        arrow::{ArrowExporter, ArrowImporter},
        builder_helpers::scalar_callback,
        environment::{Environment, StorageLocation},
        scalar::ScalarFunctionBuilder,
        signature::{Parameter, SignatureBuilder},
        types::DuckDBType,
    };

    scalar_callback!(ToArrowTest, i64, |input, result, ctx, user_data| {
        let logical_types = input
            .vectors()?
            .iter()
            .map(|v| v.logical_type().clone())
            .collect::<Vec<_>>();

        let mut result = result;

        let data_chunk = input.to_data_chunk()?;

        let mut arrow_exporter = ArrowExporter::new(
            &ctx,
            &logical_types,
            &["val1".to_string(), "val2".to_string(), "val3".to_string()],
            None,
        )?;

        let mut schema = arrow_exporter.schema()?;

        arrow_exporter.append(&data_chunk, true)?;

        let importer = ArrowImporter::new(&ctx, &mut schema, None)?;
        unsafe { schema.release.unwrap()(&mut schema) };

        importer.append_consumed(arrow_exporter.next_array()?, true)?;

        let chunk = importer.chunk()?;

        assert!(chunk.is_some());
        let chunk = chunk.unwrap();

        assert_eq!(chunk.row_count()?, 2);

        let val1 = chunk.get_vector_at::<i64>(0)?;
        let val2 = chunk.get_vector_at::<bool>(1)?;
        let val3 = chunk.get_vector_at::<String>(2)?;

        result.set_size(chunk.row_count()?)?;

        for row in 0..chunk.row_count()? {
            let v1 = val1.get(row)?;
            let v2 = val2.get(row)?;
            let v3 = val3.get(row)?;

            println!("Row {}: val1={:?}, val2={:?}, val3={:?}", row, v1, v2, v3);

            let sum = *v1.unwrap_or(&0)
                + v2.map_or(0, |x| if *x { 2 } else { 1 }) as i64
                + v3.unwrap_or_default().len() as i64;

            result.write(row, Some(sum))?;
        }

        Ok(())
    });

    #[test]
    fn test_arrow_conversion() -> crate::Result<()> {
        let env = Environment::new()?;
        let db = env.open(StorageLocation::InMemory)?;
        let conn = db.connect()?;

        ScalarFunctionBuilder::new(
            "to_arrow",
            SignatureBuilder::new(
                [
                    Parameter::normal("val1", i64::logical_type(&conn)?),
                    Parameter::normal("val2", bool::logical_type(&conn)?),
                    Parameter::normal("val3", String::logical_type(&conn)?),
                ],
                i64::logical_type(&conn)?,
            ),
            ToArrowTest,
        )
        .register(&conn)?;

        let result = conn.query(
            "SELECT to_arrow(a, b, c) FROM (VALUES (2, true, 'hello'), (1, false, 'world')) AS t(a, b, c)",
            Parameters::None,
        )?;

        for chunk in result {
            let chunk = chunk?;

            let res = chunk.get_vector_at::<i64>(0)?;

            assert_eq!(*res.get(0)?.unwrap_or(&0), 2 + 2 + 5);
            assert_eq!(*res.get(1)?.unwrap_or(&0), 1 + 1 + 5);
        }

        Ok(())
    }
}

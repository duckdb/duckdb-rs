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
pub struct ArrowExporter {
    handle: ffi::duckdb_v2_arrow_exporter_handle,
}

impl ArrowExporter {
    /// Capture the column schema and Arrow settings from an active transaction.
    ///
    /// `count` must match both slices' lengths. `None` or `Some(0)` imposes no batch-size limit.
    pub fn new(
        context: &Context,
        logical_types: &[LogicalType],
        names: &[String],
        count: usize,
        batch_size: Option<usize>,
    ) -> Result<Self> {
        let logical_type_handles = logical_types.iter().map(|x| x.handle).collect::<Vec<_>>();
        let name_ptrs = names.iter().map(|x| x.into()).collect::<Vec<DuckDBStr<'_>>>();

        let handle = check_api_call!(
            ffi::duckdb_v2_arrow_exporter_create,
            **context,
            logical_type_handles.as_ptr(),
            name_ptrs.as_ptr(),
            count as u64,
            batch_size.unwrap_or(0) as u64,
            RET
        )?;

        Ok(ArrowExporter { handle })
    }

    /// Copy a chunk into Arrow buffers, flushing any partial batch when `flush` is true.
    pub fn append(&mut self, mut chunk: DataChunkRef<'_>, flush: bool) -> Result<()> {
        check_api_call!(
            ffi::duckdb_v2_arrow_exporter_append,
            self.handle,
            &mut chunk.handle,
            false,
            flush
        )
    }

    /// Return the Arrow schema; the caller must invoke its `release` callback when done.
    pub fn get_schema(&self) -> Result<ffi::ArrowSchema> {
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
/// The connection supplying its context must outlive the importer.
pub struct ArrowImporter {
    handle: ffi::duckdb_v2_arrow_importer_handle,
}

impl ArrowImporter {
    /// Resolve an Arrow schema using the context's active transaction.
    ///
    /// The caller retains the schema. `None` or `Some(0)` imposes no chunk-size limit.
    pub fn new(context: Context, schema: &mut ffi::ArrowSchema, batch_size: Option<usize>) -> Result<Self> {
        let handle = check_api_call!(
            ffi::duckdb_v2_arrow_importer_create,
            *context,
            schema,
            batch_size.unwrap_or(0) as u64,
            RET
        )?;
        Ok(ArrowImporter { handle })
    }

    /// Queue an array for copying into chunks; `flush` releases any final partial batch.
    ///
    /// Keep the array valid and drain its chunks before appending another array.
    pub fn append(&self, array: &mut ffi::ArrowArray, flush: bool) -> Result<()> {
        check_api_call!(ffi::duckdb_v2_arrow_importer_append, self.handle, array, false, flush)
    }

    /// Return the owned DuckDB schema resolved from the Arrow schema.
    pub fn schema(&self) -> Result<Schema> {
        Ok(Schema {
            handle: check_api_call!(ffi::duckdb_v2_arrow_importer_get_schema, self.handle, RET)?,
        })
    }

    /// Take the next chunk, or return `None` when the current array is drained.
    pub fn chunk(&self) -> Result<Option<DataChunk>> {
        let handle = check_api_call!(ffi::duckdb_v2_arrow_importer_next_chunk, self.handle, RET)?;

        if handle.is_null() {
            Ok(None)
        } else {
            Ok(Some(DataChunk::new(handle, true)))
        }
    }
}

impl Drop for ArrowImporter {
    fn drop(&mut self) {
        check_api_call_no_err!(ffi::duckdb_v2_arrow_importer_destroy, &mut self.handle).unwrap();
    }
}

#[cfg(test)]
#[cfg(false)]
mod tests {
    use crate::{
        DuckDBType, Environment, Parameters, StorageLocation,
        arrow::{ConversionPlan, logical_types_to_arrow_schema},
        builder_helpers::scalar_callback,
        scalar::ScalarFunctionBuilder,
        signature::{Parameter, SignatureBuilder},
    };

    scalar_callback!(ToArrowTest, i64, |input, result, ctx, user_data| {
        let logical_types = input
            .vectors()?
            .iter()
            .map(|v| v.logical_type().clone())
            .collect::<Vec<_>>();

        let mut result = result;

        let mut arrow_schema = logical_types_to_arrow_schema(&ctx, &logical_types)?;

        let conversion_plan = ConversionPlan::new(&ctx, &mut arrow_schema)?;

        unsafe {
            arrow_schema.release.unwrap()(&mut arrow_schema);
        }

        let schema = conversion_plan.schema()?;

        println!("Schema: {:?}", schema.get_all()?);

        let mut arrow_array = input.to_arrow_array(&ctx)?;

        dbg!(arrow_array);

        let data_chunk = conversion_plan.to_data_chunk(&ctx, &mut arrow_array)?;

        assert_eq!(arrow_array.release.is_none(), true);

        assert_eq!(data_chunk.row_count()?, input.row_count()?);
        assert_eq!(data_chunk.vectors_count()?, input.vectors_count()?);

        result.set_size(data_chunk.row_count()?)?;

        let vec1 = data_chunk.get_vector_at::<i64>(0)?;
        let vec2 = data_chunk.get_vector_at::<bool>(1)?;
        let vec3 = data_chunk.get_vector_at::<String>(2)?;

        for i in 0..data_chunk.row_count()? {
            let val1 = vec1.get(i)?;
            let val2 = vec2.get(i)?;
            let val3 = vec3.get(i)?;

            println!("Row {}: val1={:?}, val2={:?}, val3={:?}", i, val1, val2, val3);

            result.write(
                i,
                Some(*val1.unwrap_or(&0) + *val2.unwrap_or(&false) as i64 + val3.unwrap_or_default().len() as i64),
            )?;
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

            assert_eq!(*res.get(0)?.unwrap_or(&0), 2 + 1 + 5);
            assert_eq!(*res.get(1)?.unwrap_or(&0), 1 + 0 + 5);
        }

        Ok(())
    }
}

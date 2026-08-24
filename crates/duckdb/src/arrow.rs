//! Arrow C Data Interface conversion.
//!
//! Exported `ArrowSchema` and `ArrowArray` values follow Arrow's `release`
//! callback ownership convention. Importing an array transfers its buffers to
//! the resulting [`crate::data_chunk::DataChunk`].

use crate::{
    Context, Result, check_api_call, check_api_call_no_err, data_chunk::DataChunk, ffi,
    logical_type::LogicalType, schema::Schema,
};

/// Convert logical types into an owned Arrow C schema.
///
/// Each Arrow field is named with the corresponding logical type's DuckDB
/// name.
///
/// The caller must invoke the returned schema's `release` callback.
pub fn logical_types_to_arrow_schema(
    context: &Context,
    logical_types: &[LogicalType],
) -> Result<ffi::ArrowSchema> {
    let names = logical_types
        .iter()
        .map(|v| v.name().unwrap().into())
        .collect::<Vec<ffi::duckdb_v2_str>>();

    let types = logical_types.iter().map(|v| v.handle).collect::<Vec<_>>();

    check_api_call!(
        ffi::duckdb_v2_logical_types_to_arrow_schema,
        **context,
        types.as_ptr(),
        names.as_ptr(),
        logical_types.len() as u64,
        RET
    )
}

/// A reusable mapping from an Arrow schema to DuckDB logical types.
///
/// Build a plan once and reuse it for arrays with the same schema. Each
/// imported array transfers ownership of its buffers to the returned
/// [`DataChunk`].
pub struct ConversionPlan {
    /// The owned DuckDB Arrow-conversion-plan handle.
    pub handle: ffi::duckdb_v2_arrow_conversion_plan_handle,
}

impl ConversionPlan {
    /// Resolve an Arrow schema using a context's type configuration.
    ///
    /// The schema remains caller-owned and may be released after this call.
    pub fn new(context: &Context, schema: &mut ffi::ArrowSchema) -> Result<Self> {
        Ok(Self {
            handle: check_api_call!(
                ffi::duckdb_v2_arrow_conversion_plan_create,
                **context,
                schema,
                RET
            )?,
        })
    }

    /// Import an Arrow array as an owned data chunk.
    ///
    /// Ownership transfers to the chunk and the array's `release` callback is
    /// cleared; do not release the array afterward.
    pub fn to_data_chunk(
        &self,
        context: &Context,
        array: &mut ffi::ArrowArray,
    ) -> Result<DataChunk> {
        Ok(DataChunk {
            handle: check_api_call!(
                ffi::duckdb_v2_arrow_array_to_data_chunk,
                **context,
                array,
                self.handle,
                RET
            )?,
            is_owned: true,
            is_writable: false,
        })
    }

    /// Return the resolved DuckDB field schema.
    pub fn schema(&self) -> Result<Schema> {
        Ok(Schema {
            handle: check_api_call!(
                ffi::duckdb_v2_arrow_conversion_plan_get_schema,
                self.handle,
                RET
            )?,
        })
    }
}

impl Drop for ConversionPlan {
    fn drop(&mut self) {
        check_api_call_no_err!(
            ffi::duckdb_v2_arrow_conversion_plan_destroy,
            &mut self.handle
        )
        .unwrap();
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
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

            println!(
                "Row {}: val1={:?}, val2={:?}, val3={:?}",
                i, val1, val2, val3
            );

            result.write(
                i,
                Some(
                    *val1.unwrap_or(&0)
                        + *val2.unwrap_or(&false) as i64
                        + val3.unwrap_or_default().len() as i64,
                ),
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
        .register_with_connection(&conn)?;

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

//! User-defined casts between logical types.

use libduckdb_sys::v2::{self as ffi};

use crate::{
    Result,
    builder_helpers::{OpaqueHandle, ffi_enum_redeclaration, get_user_data, handle_unwind},
    check_api_call,
    handles::{CastFunctionHandle, CastFunctionLink},
    logical_type::LogicalType,
    vector::{Vector, VectorElement},
};

ffi_enum_redeclaration! {
    /// How a cast callback should handle per-row conversion failures.
    pub enum CastMode <- ffi::DUCKDB_V2_CAST_MODE {
        /// Abort the query by returning a conversion error.
        Normal = DUCKDB_V2_CAST_MODE_NORMAL,
        /// Write `NULL` for values that cannot be converted.
        Try = DUCKDB_V2_CAST_MODE_TRY
    }
}

unsafe extern "C" fn exec_callback<T: CastFunctionCallbacks>(
    info: ffi::duckdb_v2_cast_function_exec_info_handle,
    _ctx: ffi::duckdb_v2_context_handle,
    err: *mut ffi::duckdb_v2_error_info_handle,
) {
    handle_unwind(
        || {
            let user_data = get_user_data!(ffi::duckdb_v2_cast_function_exec_get_user_data, info);

            let input = Vector::from_handle(
                &check_api_call!(ffi::duckdb_v2_cast_function_exec_get_input, info, RET)?,
                false,
            )?;
            let input = input.cast::<T::InputType>()?;

            let mode = check_api_call!(ffi::duckdb_v2_cast_function_exec_get_mode, info, RET)?;
            let output = Vector::from_handle(
                &check_api_call!(ffi::duckdb_v2_cast_function_exec_get_output, info, RET)?,
                true,
            )?;
            let output = output.cast::<T::OutputType>()?;

            T::exec(user_data, mode.try_into()?, input, output)
        },
        err,
    );
}

/// Builds and registers a user-defined cast.
///
/// A negative `implicit_cast_cost` makes the cast explicit-only. Non-negative
/// costs allow implicit selection, with lower costs preferred when DuckDB
/// resolves alternatives.
pub struct CastFunctionBuilder<T: CastFunctionCallbacks> {
    source_type: LogicalType,
    target_type: LogicalType,
    implicit_cast_cost: i64,
    user_data: OpaqueHandle<T>,
}

impl<T: CastFunctionCallbacks> CastFunctionBuilder<T> {
    /// Create a cast builder for a source and target type.
    pub fn new(source_type: LogicalType, target_type: LogicalType, implicit_cast_cost: i64, implementation: T) -> Self {
        Self {
            source_type,
            target_type,
            implicit_cast_cost,
            user_data: OpaqueHandle::new(implementation),
        }
    }

    fn build(&self, handle: CastFunctionHandle) -> Result<CastFunctionHandle> {
        check_api_call!(
            ffi::duckdb_v2_cast_function_set_source_type,
            *handle,
            self.source_type.handle
        )?;

        check_api_call!(
            ffi::duckdb_v2_cast_function_set_target_type,
            *handle,
            self.target_type.handle
        )?;

        check_api_call!(
            ffi::duckdb_v2_cast_function_set_implicit_cast_cost,
            *handle,
            self.implicit_cast_cost
        )?;

        check_api_call!(
            ffi::duckdb_v2_cast_function_set_user_data,
            *handle,
            &mut self.user_data.to_handle()
        )?;

        check_api_call!(
            ffi::duckdb_v2_cast_function_set_exec_callback,
            *handle,
            Some(exec_callback::<T>)
        )?;

        Ok(handle)
    }

    /// Register through a connection or extension, consuming the builder.
    #[allow(private_bounds)]
    pub fn register<C: CastFunctionLink>(self, link: &C) -> Result<()> {
        let handle = link.create_cast_function_handle()?;
        let handle = self.build(handle)?;

        check_api_call!(ffi::duckdb_v2_cast_function_register, *handle)
    }
}

/// Execution callback for a user-defined cast.
pub trait CastFunctionCallbacks: Send + Sync + 'static {
    /// The typed input-vector element.
    type InputType: VectorElement;
    /// The typed output-vector element.
    type OutputType: VectorElement;

    /// **Execute:** convert an input batch into the output vector.
    ///
    /// Normal casts should return conversion errors. Try casts should write
    /// `NULL` for values that cannot be converted.
    fn exec(
        &self,
        mode: CastMode,
        input: Vector<'_, Self::InputType>,
        output: Vector<'_, Self::OutputType>,
    ) -> Result<()>;
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use crate::{
        DuckDBType, Parameters,
        cast::{CastFunctionBuilder, CastFunctionCallbacks, CastMode},
        custom_type,
        environment::Environment,
        environment::StorageLocation,
    };

    struct CastToFloat {
        offset: i32,
    }

    impl CastFunctionCallbacks for CastToFloat {
        type InputType = String;
        type OutputType = f32;

        fn exec(
            &self,
            _mode: CastMode,
            input: crate::vector::Vector<'_, Self::InputType>,
            mut output: crate::vector::Vector<'_, Self::OutputType>,
        ) -> crate::Result<()> {
            println!(
                "EXEC CALLBACK: input len = {}, output len = {}",
                input.len(),
                output.len()
            );

            output.set_size(input.len())?;

            for i in 0..input.len() {
                let input_value = input.get(i).unwrap();
                if input_value.is_none() {
                    output.write(i, None)?;
                    continue;
                }
                let input_value = input_value.unwrap();
                let output_value = input_value.parse::<f32>().unwrap() + self.offset as f32;

                output.write(i, Some(output_value))?;
            }
            Ok(())
        }
    }

    #[test]
    fn test_cast_function() -> crate::Result<()> {
        let env = Environment::new()?;
        let db = env.open(StorageLocation::InMemory)?;
        let conn = db.connect()?;

        let custom_type = custom_type::CustomType::new("TEMPERATURE", f32::logical_type(&conn)?)?;
        custom_type.register(&conn)?;
        let logical_type = f32::logical_type(&conn)?;
        let temperature_type = logical_type.to_alias(&conn, "TEMPERATURE")?;

        CastFunctionBuilder::new(
            String::logical_type(&conn)?,
            temperature_type,
            0,
            CastToFloat { offset: 10 },
        )
        .register(&conn)?;

        let result = conn.query(
            "SELECT CAST(x as TEMPERATURE) FROM VALUES ('32'), (NULL)  as t(x)",
            Parameters::None,
        )?;

        for chunk in result {
            let chunk = chunk?;

            let vector = chunk.get_vector_at::<f32>(0)?;

            assert_eq!(vector.get(0)?, Some(&42.0));
            assert_eq!(vector.get(1)?, None);
        }

        Ok(())
    }
}

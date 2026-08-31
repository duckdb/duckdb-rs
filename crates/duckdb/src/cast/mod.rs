//! User-defined casts between logical types.

use std::ops::Deref;

use libduckdb_sys::{self as ffi};

use crate::{
    Context, Result,
    builder_helpers::{OpaqueHandle, context_and_connection_fn, ffi_enum_redeclaration, get_user_data, handle_unwind},
    check_api_call, check_api_call_no_err,
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

/// An owned cast-function builder handle.
pub struct CastFunctionHandle(ffi::duckdb_v2_cast_function_builder_handle);

impl Drop for CastFunctionHandle {
    fn drop(&mut self) {
        check_api_call_no_err!(ffi::duckdb_v2_cast_function_builder_destroy, &mut self.0).unwrap();
    }
}

impl Deref for CastFunctionHandle {
    type Target = ffi::duckdb_v2_cast_function_builder_handle;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

unsafe extern "C" fn exec_callback<T: CastFunctionCallbacks>(
    info: ffi::duckdb_v2_cast_function_exec_info_handle,
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

    fn build(&self) -> Result<CastFunctionHandle> {
        let handle = CastFunctionHandle(check_api_call!(ffi::duckdb_v2_cast_function_builder_create, RET)?);

        check_api_call!(
            ffi::duckdb_v2_cast_function_builder_set_source_type,
            *handle,
            self.source_type.handle
        )?;

        check_api_call!(
            ffi::duckdb_v2_cast_function_builder_set_target_type,
            *handle,
            self.target_type.handle
        )?;

        check_api_call!(
            ffi::duckdb_v2_cast_function_builder_set_implicit_cast_cost,
            *handle,
            self.implicit_cast_cost
        )?;

        check_api_call!(
            ffi::duckdb_v2_cast_function_builder_set_user_data,
            *handle,
            self.user_data.to_handle()
        )?;

        check_api_call!(
            ffi::duckdb_v2_cast_function_builder_set_exec_callback,
            *handle,
            Some(exec_callback::<T>)
        )?;

        Ok(handle)
    }

    context_and_connection_fn! {
        /// Register the cast through a connection or callback context.
        pub fn register_with_[context, connection](self) -> Result<()>
        {
            context_fn: ffi::duckdb_v2_cast_function_builder_register_with_context,
            connection_fn: ffi::duckdb_v2_cast_function_builder_register_with_connection,
        }
        let handle = self.build()?;

        check_api_call!(
            api_fn!(),
            **api_arg!(),
            *handle
        )
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
    fn exec(&self, mode: CastMode, input: Vector<Self::InputType>, output: Vector<Self::OutputType>) -> Result<()>;
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use crate::{
        DuckDBType, Environment, Parameters, StorageLocation,
        cast::{CastFunctionBuilder, CastFunctionCallbacks, CastMode},
        custom_type,
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
            input: crate::vector::Vector<Self::InputType>,
            mut output: crate::vector::Vector<Self::OutputType>,
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
        custom_type.register_with_connection(&conn)?;
        let logical_type = f32::logical_type(&conn)?;
        let temperature_type = logical_type.to_alias("TEMPERATURE")?;

        CastFunctionBuilder::new(
            String::logical_type(&conn)?,
            temperature_type,
            0,
            CastToFloat { offset: 10 },
        )
        .register_with_connection(&conn)?;

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

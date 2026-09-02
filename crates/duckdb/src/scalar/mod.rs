//! User-defined scalar functions.
//!
//! Implement [`ScalarCallbacks`] to bind each call site, optionally initialize
//! worker-local state, and evaluate input chunks into an output vector. Register
//! the implementation with [`ScalarFunctionBuilder`]. Bind callbacks can use
//! [`ResultTypeHandle`] when the concrete result type depends on the arguments.

use std::any::Any;
use std::collections::HashMap;
use std::ops::Deref;

use libduckdb_sys::{self as ffi};

use crate::bind_arguments::BindMetadata;
use crate::builder_helpers::{
    OpaqueHandle, context_and_connection_fn, get_bind_data, get_init_data, get_user_data, handle_unwind, into_opaque,
};
use crate::data_chunk::DataChunk;
use crate::enums::FunctionProperty;
use crate::logical_type::LogicalType;
use crate::signature::SignatureBuilder;
use crate::vector::{Unknown, Vector, VectorElement};
use crate::{Context, Result, check_api_call, check_api_call_no_err};

struct ScalarFunctionBuilderHandle(ffi::duckdb_v2_scalar_function_builder_handle);

impl Drop for ScalarFunctionBuilderHandle {
    fn drop(&mut self) {
        check_api_call_no_err!(ffi::duckdb_v2_scalar_function_builder_destroy, &mut self.0).unwrap();
    }
}

impl Deref for ScalarFunctionBuilderHandle {
    type Target = ffi::duckdb_v2_scalar_function_builder_handle;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

unsafe extern "C" fn bind_callback<T: ScalarCallbacks>(
    info: ffi::duckdb_v2_scalar_function_bind_info_handle,
    context: ffi::duckdb_v2_context_handle,
    err: *mut ffi::duckdb_v2_error_info_handle,
) {
    handle_unwind(
        || {
            let user_data = get_user_data!(ffi::duckdb_v2_scalar_function_bind_get_user_data, info);

            let metadata = BindMetadata::from_scalar(&info)?;

            let result = T::bind(
                user_data,
                Context(context),
                metadata,
                ResultTypeHandle { handle: &info },
            )?;

            check_api_call!(
                ffi::duckdb_v2_scalar_function_bind_set_bind_data,
                info,
                into_opaque(result)
            )?;

            Ok(())
        },
        err,
    );
}

unsafe extern "C" fn init_callback<T: ScalarCallbacks>(
    info: ffi::duckdb_v2_scalar_function_init_info_handle,
    context: ffi::duckdb_v2_context_handle,
    err: *mut ffi::duckdb_v2_error_info_handle,
) {
    handle_unwind(
        || {
            let user_data = get_user_data!(ffi::duckdb_v2_scalar_function_init_get_user_data, info);

            let bind_data = get_bind_data!(ffi::duckdb_v2_scalar_function_init_get_bind_data, info);

            let result = T::init(user_data, bind_data, Context(context))?;

            check_api_call!(
                ffi::duckdb_v2_scalar_function_init_set_init_data,
                info,
                into_opaque(result)
            )?;

            Ok(())
        },
        err,
    );
}

unsafe extern "C" fn exec_callback<T: ScalarCallbacks>(
    info: ffi::duckdb_v2_scalar_function_exec_info_handle,
    context: ffi::duckdb_v2_context_handle,
    err: *mut ffi::duckdb_v2_error_info_handle,
) {
    handle_unwind(
        || {
            let user_data = get_user_data!(ffi::duckdb_v2_scalar_function_exec_get_user_data, info);

            let bind_data = get_bind_data!(ffi::duckdb_v2_scalar_function_exec_get_bind_data, info);

            let init_data = get_init_data!(ffi::duckdb_v2_scalar_function_exec_get_init_data, info);

            let result_handle = check_api_call!(ffi::duckdb_v2_scalar_function_exec_get_result, info, RET)?;

            let result_vec = Vector::from_handle(&result_handle, true)?;

            let input_handle = check_api_call!(ffi::duckdb_v2_scalar_function_exec_get_input, info, RET)?;

            let data_chunk = DataChunk {
                handle: input_handle,
                is_owned: false,
                is_writable: false,
            };

            T::exec(
                user_data,
                bind_data,
                init_data,
                Context(context),
                &data_chunk,
                result_vec,
            )?;

            Ok(())
        },
        err,
    );
}

/// Callback-scoped control over a scalar function's resolved result type.
pub struct ResultTypeHandle<'a> {
    handle: &'a ffi::duckdb_v2_scalar_function_bind_info_handle,
}

impl<'a> ResultTypeHandle<'a> {
    /// Override the result type for the current bound call site.
    ///
    /// For example, a function declared to return `ANY` can derive a concrete
    /// type from its bound arguments. DuckDB copies `result_type`, and the
    /// override is valid only during binding.
    pub fn override_result_type(&self, result_type: LogicalType) -> Result<()> {
        check_api_call!(
            ffi::duckdb_v2_scalar_function_bind_set_return_type,
            *self.handle,
            result_type.handle
        )
    }
}

/// Builds and registers a user-defined scalar function.
pub struct ScalarFunctionBuilder<T: ScalarCallbacks> {
    name: String,
    signature: SignatureBuilder,
    properties: HashMap<ffi::DUCKDB_V2_FUNCTION_PROPERTY_KEY, ffi::DUCKDB_V2_FUNCTION_PROPERTY_VALUE>,
    user_data: OpaqueHandle<T>,
}

impl<T: ScalarCallbacks> ScalarFunctionBuilder<T> {
    /// Create a builder from a name, signature, and callback implementation.
    pub fn new(name: impl Into<String>, signature: SignatureBuilder, implementation: T) -> Self {
        Self {
            name: name.into(),
            signature,
            properties: HashMap::new(),
            user_data: OpaqueHandle::new(implementation),
        }
    }

    /// Set a DuckDB function property.
    pub fn set_property(mut self, item: FunctionProperty) -> Self {
        let (key, value) = item.into();
        self.properties.insert(key, value);
        self
    }

    fn build(&self) -> Result<ScalarFunctionBuilderHandle> {
        let handle = ScalarFunctionBuilderHandle(check_api_call!(ffi::duckdb_v2_scalar_function_builder_create, RET)?);

        check_api_call!(
            ffi::duckdb_v2_scalar_function_builder_set_signature,
            *handle,
            *self.signature.build()?
        )?;

        check_api_call!(
            ffi::duckdb_v2_scalar_function_builder_set_name,
            *handle,
            (&self.name).into()
        )?;

        for (key, value) in &self.properties {
            check_api_call!(
                ffi::duckdb_v2_scalar_function_builder_set_property,
                *handle,
                *key,
                *value
            )?;
        }

        check_api_call!(
            ffi::duckdb_v2_scalar_function_builder_set_user_data,
            *handle,
            self.user_data.to_handle()
        )?;

        check_api_call!(
            ffi::duckdb_v2_scalar_function_builder_set_bind_callback,
            *handle,
            Some(bind_callback::<T>)
        )?;
        check_api_call!(
            ffi::duckdb_v2_scalar_function_builder_set_init_callback,
            *handle,
            Some(init_callback::<T>)
        )?;

        check_api_call!(
            ffi::duckdb_v2_scalar_function_builder_set_exec_callback,
            *handle,
            Some(exec_callback::<T>)
        )?;

        Ok(handle)
    }

    context_and_connection_fn! {
        /// Register the function through a connection or callback context.
        pub fn register_with_[context, connection](self) -> Result<()>
        {
            context_fn: ffi::duckdb_v2_scalar_function_builder_register_with_context,
            connection_fn: ffi::duckdb_v2_scalar_function_builder_register_with_connection,
        }
        let builder_handle = self.build()?;

        check_api_call!(
            api_fn!(),
            **api_arg!(),
            *builder_handle
        )
    }
}

/// Callback lifecycle for a user-defined scalar function.
///
/// DuckDB binds each call site, initializes state for each executing worker,
/// and invokes [`Self::exec`] for batches of rows.
pub trait ScalarCallbacks: Send + Sync + 'static {
    /// Immutable data shared from binding through execution.
    type BindData: Any + Send + Sync + Default;
    /// Worker-local data shared across execution batches.
    type InitData: Any + Send + Sync + Default;
    /// The typed element written to the result vector.
    type ResultType: VectorElement;

    /// **Bind:** validate a call site and create data shared by later phases.
    fn bind(
        &self,
        _context: Context,
        _metadata: BindMetadata,
        _result_type_handle: ResultTypeHandle,
    ) -> Result<Self::BindData> {
        Ok(Self::BindData::default())
    }

    /// **Initialize:** create worker-local execution data.
    fn init(&self, _bind_data: Option<&Self::BindData>, _context: Context) -> Result<Self::InitData> {
        Ok(Self::InitData::default())
    }

    /// **Execute:** evaluate one input batch and fill the output vector.
    fn exec(
        &self,
        bind_data: Option<&Self::BindData>,
        init_data: Option<&Self::InitData>,
        context: Context,
        input: &DataChunk,
        output: Vector<'_, Unknown>,
    ) -> Result<()>;
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests;

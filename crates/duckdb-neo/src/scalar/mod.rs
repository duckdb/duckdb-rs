//! User-defined scalar functions.
//!
//! Implement [`ScalarCallbacks`] to bind each call site, optionally initialize
//! worker-local state, and evaluate input chunks into an output vector. Register
//! the implementation with [`ScalarFunctionBuilder`]. Bind callbacks can use
//! [`ReturnTypeHandle`] when the concrete result type depends on the arguments.

use std::any::Any;
use std::collections::HashMap;

use crate::ffi;

use crate::bind_arguments::{BindArgument, BindMetadata, BindType};
use crate::builder_helpers::{OpaqueHandle, get_bind_data, get_init_data, get_user_data, handle_unwind, into_opaque};
use crate::data_chunk::VectorCollection;
use crate::enums::FunctionProperty;
use crate::handles::{ScalarFunctionBuilderHandle, ScalarFunctionBuilderLink};
use crate::logical_type::LogicalType;
use crate::signature::SignatureBuilder;
use crate::vector::{Unknown, Vector};
use crate::{Result, check_api_call, connection::Context};

unsafe extern "C" fn bind_callback<T: ScalarCallbacks>(
    info: ffi::duckdb_v2_scalar_function_bind_info_handle,
    context: ffi::duckdb_v2_context_handle,
    err: *mut ffi::duckdb_v2_error_info_handle,
) {
    handle_unwind(
        || {
            let user_data = get_user_data!(ffi::duckdb_v2_scalar_function_bind_get_user_data, info);

            let arguments = BindMetadata {
                bind_type: BindType::Scalar(&info),
            }
            .get_arguments()?;

            let result = T::bind(
                user_data,
                Context(context),
                arguments,
                ReturnTypeHandle {
                    handle: FunctionBindHandles::Scalar(&info),
                },
            )?;

            check_api_call!(
                ffi::duckdb_v2_scalar_function_bind_set_bind_data,
                info,
                &mut into_opaque(result)
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
                &mut into_opaque(result)
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

            let arg_count = check_api_call!(ffi::duckdb_v2_scalar_function_exec_get_arg_count, info, RET)? as usize;
            let row_count = check_api_call!(ffi::duckdb_v2_scalar_function_exec_get_row_count, info, RET)? as usize;

            let mut handles = Vec::with_capacity(arg_count);
            for i in 0..arg_count {
                let handle = check_api_call!(ffi::duckdb_v2_scalar_function_exec_get_arg, info, i as u32, RET)?;
                handles.push(handle);
            }

            let collection = VectorCollection {
                handles,
                is_writable: false,
                row_count,
            };

            T::exec(
                user_data,
                bind_data,
                init_data,
                Context(context),
                &collection,
                result_vec,
            )?;

            Ok(())
        },
        err,
    );
}

pub(crate) enum FunctionBindHandles<'a> {
    Scalar(&'a ffi::duckdb_v2_scalar_function_bind_info_handle),
    Aggregate(&'a ffi::duckdb_v2_aggregate_function_bind_info_handle),
}

/// Callback-scoped control over a scalar function's resolved result type.
pub struct ReturnTypeHandle<'a> {
    pub(crate) handle: FunctionBindHandles<'a>,
}

impl<'a> ReturnTypeHandle<'a> {
    /// Override the result type for the current bound call site.
    ///
    /// For example, a function declared to return `ANY` can derive a concrete
    /// type from its bound arguments. DuckDB copies `return`, and the
    /// override is valid only during binding.
    pub fn override_return(&self, return_type: LogicalType) -> Result<()> {
        match self.handle {
            FunctionBindHandles::Scalar(handle) => check_api_call!(
                ffi::duckdb_v2_scalar_function_bind_set_return_type,
                *handle,
                *return_type
            ),
            FunctionBindHandles::Aggregate(handle) => check_api_call!(
                ffi::duckdb_v2_aggregate_function_bind_set_return_type,
                *handle,
                *return_type
            ),
        }
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

    fn build(&self, handle: &ScalarFunctionBuilderHandle) -> Result<()> {
        let signature = check_api_call!(ffi::duckdb_v2_scalar_function_get_signature, **handle, RET)?;

        self.signature.build(&signature)?;

        check_api_call!(
            ffi::duckdb_v2_scalar_function_set_name,
            **handle,
            &mut (&self.name).into()
        )?;

        for (key, value) in &self.properties {
            check_api_call!(ffi::duckdb_v2_scalar_function_set_property, **handle, *key, *value)?;
        }

        check_api_call!(
            ffi::duckdb_v2_scalar_function_set_user_data,
            **handle,
            &mut self.user_data.to_handle()
        )?;

        check_api_call!(
            ffi::duckdb_v2_scalar_function_set_bind_callback,
            **handle,
            Some(bind_callback::<T>)
        )?;
        check_api_call!(
            ffi::duckdb_v2_scalar_function_set_init_callback,
            **handle,
            Some(init_callback::<T>)
        )?;

        check_api_call!(
            ffi::duckdb_v2_scalar_function_set_exec_callback,
            **handle,
            Some(exec_callback::<T>)
        )?;

        Ok(())
    }

    /// Register through a [`Connection`](crate::connection::Connection) or
    /// [`Extension`](crate::connection::Extension), consuming the builder and
    /// transferring ownership of its callback implementation to DuckDB.
    ///
    /// A builder cannot be registered more than once.
    #[allow(private_bounds)] // Keep the handle factory private while accepting both supported link types.
    pub fn register<C: ScalarFunctionBuilderLink>(self, link: &C) -> Result<()> {
        let handle = link.create_scalar_function_handle()?;
        self.build(&handle)?;

        check_api_call!(ffi::duckdb_v2_scalar_function_register, *handle)
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

    /// **Bind:** validate a call site and create data shared by later phases.
    fn bind(
        &self,
        _context: Context,
        _metadata: Vec<BindArgument>,
        _result_type_handle: ReturnTypeHandle<'_>,
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
        vectors: &VectorCollection,
        output: Vector<'_, Unknown>,
    ) -> Result<()>;
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests;

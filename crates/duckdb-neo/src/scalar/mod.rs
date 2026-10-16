//! User-defined scalar functions.
//!
//! Implement [`ScalarCallbacks`] to bind each call site, optionally initialize
//! worker-local state, and evaluate input chunks into an output vector. Register
//! the implementation with [`ScalarFunctionBuilder`]. Bind callbacks can use
//! [`ScalarCallbacks::resolve_types`] when the concrete result type depends on the arguments.

use std::any::Any;
use std::collections::HashMap;

use crate::ffi;

use crate::bind_arguments::BindArguments;
use crate::builder_helpers::{
    ArgCount, OpaqueHandle, get_bind_data, get_opaque_data_ref_mut, get_user_data, handle_unwind, into_opaque,
    into_opaque_eq,
};
use crate::data_chunk::{RowCount, VectorCollection};
use crate::enums::FunctionProperty;
use crate::handles::{ScalarFunctionBuilderHandle, ScalarFunctionBuilderLink};
use crate::logical_type::LogicalType;
use crate::signature::SignatureBuilder;
use crate::vector::{Access, Unknown, Vector};
use crate::{
    Result, check_api_call,
    connection::{Context, FFILink},
    raw::{FromRaw, RawExt},
};

unsafe extern "C" fn resolve_types_callback<T: ScalarCallbacks>(
    info: ffi::duckdb_v2_function_bind_info_handle,
    result: ffi::duckdb_v2_scalar_function_resolve_types_info_handle,
    context: ffi::duckdb_v2_context_handle,
    err: *mut ffi::duckdb_v2_error_info_handle,
) {
    handle_unwind(
        || {
            let arguments = BindArguments { handle: &info };

            let return_type = T::resolve_types(&unsafe { Context::from_raw(context) }, arguments)?;

            if let Some(return_type) = return_type {
                check_api_call!(
                    ffi::duckdb_v2_scalar_function_resolve_types_set_return_type,
                    result,
                    return_type.handle
                )?;
            }

            Ok(())
        },
        err,
    );
}

unsafe extern "C" fn bind_callback<T: ScalarCallbacks>(
    info: ffi::duckdb_v2_function_bind_info_handle,
    result: ffi::duckdb_v2_scalar_function_bind_info_handle,
    context: ffi::duckdb_v2_context_handle,
    err: *mut ffi::duckdb_v2_error_info_handle,
) {
    handle_unwind(
        || {
            let user_data = get_user_data!(ffi::duckdb_v2_function_bind_get_user_data, info);

            let arguments = BindArguments { handle: &info };

            let data = T::bind(user_data, &unsafe { Context::from_raw(context) }, arguments)?;

            check_api_call!(
                ffi::duckdb_v2_scalar_function_bind_set_bind_data,
                result,
                &mut into_opaque_eq(data)
            )
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

            let result = T::init(user_data, bind_data, &unsafe { Context::from_raw(context) })?;

            check_api_call!(
                ffi::duckdb_v2_scalar_function_init_set_init_data,
                info,
                &mut into_opaque(result)
            )
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
            let init_data = check_api_call!(ffi::duckdb_v2_scalar_function_exec_get_init_data, info, RET)?;
            let init_data = unsafe { get_opaque_data_ref_mut::<T::InitData>(init_data) };

            let result_handle = check_api_call!(ffi::duckdb_v2_scalar_function_exec_get_result, info, RET)?;
            let result_vec = Vector::from_handle(&result_handle, Access::Writable)?;

            let arg_count = ArgCount::from_func(|pf, pv, nf, nv| {
                check_api_call!(ffi::duckdb_v2_scalar_function_exec_get_arg_count, info, pf, pv, nf, nv)
            })?;
            let row_count = check_api_call!(ffi::duckdb_v2_scalar_function_exec_get_row_count, info, RET)? as usize;

            let mut handles = Vec::with_capacity(arg_count.total());
            for i in 0..arg_count.total() {
                let handle = check_api_call!(ffi::duckdb_v2_scalar_function_exec_get_arg, info, i as u64, RET)?;
                handles.push(handle);
            }

            let mut collection = VectorCollection::new(handles, Access::Exclusive, RowCount::Fixed(row_count));

            T::exec(
                user_data,
                bind_data,
                init_data,
                &unsafe { Context::from_raw(context) },
                &mut collection,
                result_vec,
            )?;

            Ok(())
        },
        err,
    );
}

/// Builds and registers a user-defined scalar function.
#[derive(Debug)]
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

    fn build(&mut self, handle: &ScalarFunctionBuilderHandle, link: &dyn FFILink) -> Result<()> {
        let signature = check_api_call!(ffi::duckdb_v2_scalar_function_get_signature, handle.raw(), RET)?;

        let docs = check_api_call!(ffi::duckdb_v2_scalar_function_get_docs, handle.raw(), RET)?;

        std::mem::take(&mut self.signature).build(link, &signature, &docs)?;

        check_api_call!(
            ffi::duckdb_v2_scalar_function_set_name,
            handle.raw(),
            &mut (&self.name).into()
        )?;

        for (key, value) in &self.properties {
            check_api_call!(ffi::duckdb_v2_scalar_function_set_property, handle.raw(), *key, *value)?;
        }

        check_api_call!(
            ffi::duckdb_v2_scalar_function_set_user_data,
            handle.raw(),
            &mut self.user_data.to_handle()
        )?;

        check_api_call!(
            ffi::duckdb_v2_scalar_function_set_bind_callback,
            handle.raw(),
            Some(bind_callback::<T>)
        )?;
        check_api_call!(
            ffi::duckdb_v2_scalar_function_set_init_callback,
            handle.raw(),
            Some(init_callback::<T>)
        )?;

        check_api_call!(
            ffi::duckdb_v2_scalar_function_set_exec_callback,
            handle.raw(),
            Some(exec_callback::<T>)
        )?;

        check_api_call!(
            ffi::duckdb_v2_scalar_function_set_resolve_types_callback,
            handle.raw(),
            Some(resolve_types_callback::<T>)
        )
    }

    /// Register through a [`Connection`](crate::connection::Connection) or
    /// [`Extension`](crate::connection::Extension), consuming the builder and
    /// transferring ownership of its callback implementation to DuckDB.
    ///
    /// A builder cannot be registered more than once.
    #[allow(private_bounds)] // Keep the handle factory private while accepting both supported link types.
    pub fn register<C: ScalarFunctionBuilderLink + FFILink>(mut self, link: &C) -> Result<()> {
        let handle = link.create_scalar_function_handle()?;
        self.build(&handle, link)?;

        link.register_scalar_function(handle)
    }
}

/// Callback lifecycle for a user-defined scalar function.
///
/// DuckDB binds each call site, initializes state for each executing worker,
/// and invokes [`Self::exec`] for batches of rows.
pub trait ScalarCallbacks: Send + Sync + 'static {
    /// Immutable data shared from binding through execution.
    type BindData: Any + Send + Sync + Default + PartialEq;
    /// Worker-local data shared across execution batches.
    type InitData: Any + Send + Default;

    fn resolve_types(_context: &Context, _arguments: BindArguments<'_>) -> Result<Option<LogicalType>> {
        Ok(None)
    }

    /// **Bind:** validate a call site and create data shared by later phases.
    fn bind(&self, _context: &Context, _arguments: BindArguments<'_>) -> Result<Self::BindData> {
        Ok(Self::BindData::default())
    }

    /// **Initialize:** create worker-local execution data.
    fn init(&self, _bind_data: Option<&Self::BindData>, _context: &Context) -> Result<Self::InitData> {
        Ok(Self::InitData::default())
    }

    /// **Execute:** evaluate one input batch and fill the output vector.
    fn exec(
        &self,
        bind_data: Option<&Self::BindData>,
        init_data: Option<&mut Self::InitData>,
        context: &Context,
        vectors: &mut VectorCollection,
        output: Vector<'_, Unknown>,
    ) -> Result<()>;
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests;

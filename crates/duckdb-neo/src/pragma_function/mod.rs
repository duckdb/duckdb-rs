//! User-defined pragmas, called with the `PRAGMA` statement.
//!
//! A pragma is one of two kinds. A query pragma implements
//! [`PragmaQueryCallbacks`] and returns SQL that runs in place of the `PRAGMA`
//! statement. An exec pragma implements [`PragmaExecCallbacks`] and runs for its
//! side effects, producing no rows. Register either kind with
//! [`PragmaFunctionBuilder`].
//!
//! A pragma without parameters is called as `PRAGMA name`, one with parameters
//! as `PRAGMA name(...)`. Its arguments are constants, so
//! [`BindArguments::value`] always folds them. Its signature cannot declare a
//! return type.

use crate::ffi;

use crate::{
    Result,
    bind_arguments::BindArguments,
    builder_helpers::{OpaqueHandle, get_user_data, handle_unwind},
    check_api_call,
    connection::{Context, FFILink},
    handles::{PragmaFunctionBuilderHandle, PragmaFunctionBuilderLink},
    raw::{FromRaw, RawExt},
    signature::SignatureBuilder,
};

unsafe extern "C" fn query_callback<T: PragmaQueryCallbacks>(
    info: ffi::duckdb_v2_function_bind_info_handle,
    result: ffi::duckdb_v2_pragma_function_query_info_handle,
    context: ffi::duckdb_v2_context_handle,
    err: *mut ffi::duckdb_v2_error_info_handle,
) {
    handle_unwind(
        || {
            let user_data = get_user_data!(ffi::duckdb_v2_function_bind_get_user_data, info);

            let query = T::query(
                user_data,
                &unsafe { Context::from_raw(context) },
                BindArguments { handle: &info },
            )?;

            check_api_call!(ffi::duckdb_v2_pragma_function_query_set_query, result, &(&query).into())
        },
        err,
    );
}

unsafe extern "C" fn exec_callback<T: PragmaExecCallbacks>(
    info: ffi::duckdb_v2_function_bind_info_handle,
    context: ffi::duckdb_v2_context_handle,
    err: *mut ffi::duckdb_v2_error_info_handle,
) {
    handle_unwind(
        || {
            let user_data = get_user_data!(ffi::duckdb_v2_function_bind_get_user_data, info);

            T::exec(
                user_data,
                &unsafe { Context::from_raw(context) },
                BindArguments { handle: &info },
            )
        },
        err,
    );
}

#[derive(Debug)]
enum PragmaCallback {
    Query(ffi::duckdb_v2_pragma_function_query_callback_fn),
    Exec(ffi::duckdb_v2_pragma_function_exec_callback_fn),
}

/// Builds and registers a user-defined pragma.
///
/// Create a query pragma with [`Self::query`] or an exec pragma with
/// [`Self::exec`].
#[derive(Debug)]
pub struct PragmaFunctionBuilder<T: Send + Sync + 'static> {
    name: String,
    signature: SignatureBuilder,
    callback: PragmaCallback,
    user_data: OpaqueHandle<T>,
}

impl<T: PragmaQueryCallbacks> PragmaFunctionBuilder<T> {
    /// Create a query pragma from a name, signature, and callback implementation.
    pub fn query(name: impl Into<String>, signature: SignatureBuilder, implementation: T) -> Self {
        Self::new(
            name,
            signature,
            PragmaCallback::Query(Some(query_callback::<T>)),
            implementation,
        )
    }
}

impl<T: PragmaExecCallbacks> PragmaFunctionBuilder<T> {
    /// Create an exec pragma from a name, signature, and callback implementation.
    pub fn exec(name: impl Into<String>, signature: SignatureBuilder, implementation: T) -> Self {
        Self::new(
            name,
            signature,
            PragmaCallback::Exec(Some(exec_callback::<T>)),
            implementation,
        )
    }
}

impl<T: Send + Sync + 'static> PragmaFunctionBuilder<T> {
    fn new(name: impl Into<String>, signature: SignatureBuilder, callback: PragmaCallback, implementation: T) -> Self {
        Self {
            name: name.into(),
            signature,
            callback,
            user_data: OpaqueHandle::new(implementation),
        }
    }

    fn build(&mut self, handle: &PragmaFunctionBuilderHandle, link: &dyn FFILink) -> Result<()> {
        check_api_call!(
            ffi::duckdb_v2_pragma_function_set_name,
            handle.raw(),
            &mut (&self.name).into()
        )?;

        let signature = check_api_call!(ffi::duckdb_v2_pragma_function_get_signature, handle.raw(), RET)?;

        let docs = check_api_call!(ffi::duckdb_v2_pragma_function_get_docs, handle.raw(), RET)?;

        std::mem::take(&mut self.signature).build(link, &signature, &docs)?;

        check_api_call!(
            ffi::duckdb_v2_pragma_function_set_user_data,
            handle.raw(),
            &mut self.user_data.to_handle()
        )?;

        match self.callback {
            PragmaCallback::Query(callback) => {
                check_api_call!(
                    ffi::duckdb_v2_pragma_function_set_query_callback,
                    handle.raw(),
                    callback
                )
            }
            PragmaCallback::Exec(callback) => {
                check_api_call!(ffi::duckdb_v2_pragma_function_set_exec_callback, handle.raw(), callback)
            }
        }
    }

    /// Register through a [`Connection`](crate::connection::Connection),
    /// consuming the builder and transferring ownership of its callback
    /// implementation to DuckDB.
    ///
    /// Fails when the signature declares a return type or a pragma of the same
    /// name already exists.
    #[allow(private_bounds)] // Keep the handle factory private.
    pub fn register<C: PragmaFunctionBuilderLink + FFILink>(mut self, link: &C) -> Result<()> {
        let handle = link.create_pragma_function_handle()?;
        self.build(&handle, link)?;

        link.register_pragma_function_handle(handle)
    }
}

/// Callback of a query pragma.
pub trait PragmaQueryCallbacks: Send + Sync + 'static {
    /// Return the SQL to run in place of the `PRAGMA` statement.
    ///
    /// Called before the statement is bound. The SQL may hold several
    /// statements separated by semicolons.
    fn query(&self, context: &Context, arguments: BindArguments<'_>) -> Result<String>;
}

/// Callback of an exec pragma.
pub trait PragmaExecCallbacks: Send + Sync + 'static {
    /// Run the pragma for its side effects when the `PRAGMA` statement executes.
    fn exec(&self, context: &Context, arguments: BindArguments<'_>) -> Result<()>;
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests;

//! User-defined table functions and their scan lifecycle.
//!
//! Implement [`TableFunctionCallbacks`] to bind arguments and declare output
//! columns, initialize shared and worker-local scan state, and produce chunks.
//! [`TableFunctionBuilder`] registers the implementation and configures
//! projection pushdown. Optional cardinality estimates help DuckDB optimize
//! query plans, while progress and complex-filter callbacks expose additional
//! execution and pushdown behavior.

use std::{any::Any, ops::Deref};

use libduckdb_sys as ffi;

use crate::{
    Context, Result,
    bind_arguments::{BindArguments, BindMetadata},
    builder_helpers::{
        OpaqueHandle, context_and_connection_fn, get_bind_data, get_global_state, get_local_state, get_opaque_data_ref,
        get_user_data, handle_unwind, into_opaque,
    },
    check_api_call, check_api_call_no_err,
    data_chunk::DataChunk,
    expression::Expression,
    logical_type::LogicalType,
    signature::SignatureBuilder,
};

/// An owned table-function builder handle.
pub struct TableFunctionBuilderHandle(ffi::duckdb_v2_table_function_builder_handle);

impl Drop for TableFunctionBuilderHandle {
    fn drop(&mut self) {
        check_api_call_no_err!(ffi::duckdb_v2_table_function_builder_destroy, &mut self.0).unwrap();
    }
}

impl Deref for TableFunctionBuilderHandle {
    type Target = ffi::duckdb_v2_table_function_builder_handle;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Callback-scoped output-schema builder.
pub struct BindFunctionHandle<'a>(&'a ffi::duckdb_v2_table_function_bind_info_handle);

impl<'a> BindFunctionHandle<'a> {
    /// Append a result column in declaration order.
    pub fn add_result_column(&self, name: &str, logical_type: LogicalType) -> Result<()> {
        check_api_call!(
            ffi::duckdb_v2_table_function_bind_add_result_column,
            *self.0,
            (name).into(),
            *logical_type
        )?;

        Ok(())
    }
}

unsafe extern "C" fn bind_callback<T: TableFunctionCallbacks>(
    info: ffi::duckdb_v2_table_function_bind_info_handle,
    context: ffi::duckdb_v2_context_handle,
    err: *mut ffi::duckdb_v2_error_info_handle,
) {
    handle_unwind(
        || {
            let user_data = get_user_data!(ffi::duckdb_v2_table_function_bind_get_user_data, info);

            let metadata = BindMetadata::from_table_function(&info)?;

            let (bind_data, cardinality) = T::bind(user_data, Context(context), metadata, BindFunctionHandle(&info))?;

            check_api_call!(
                ffi::duckdb_v2_table_function_bind_set_bind_data,
                info,
                into_opaque(bind_data)
            )?;

            if let Some(cardinality) = cardinality {
                check_api_call!(
                    ffi::duckdb_v2_table_function_bind_set_cardinality,
                    info,
                    cardinality.cardinality as u64,
                    cardinality.is_exact
                )?;
            }

            Ok(())
        },
        err,
    );
}

unsafe extern "C" fn exec_callback<T: TableFunctionCallbacks>(
    info: ffi::duckdb_v2_table_function_exec_info_handle,
    context: ffi::duckdb_v2_context_handle,
    err: *mut ffi::duckdb_v2_error_info_handle,
) {
    handle_unwind(
        || {
            let user_data = get_user_data!(ffi::duckdb_v2_table_function_exec_get_user_data, info);

            let bind_data = get_bind_data!(ffi::duckdb_v2_table_function_exec_get_bind_data, info);

            let global_state = get_global_state!(ffi::duckdb_v2_table_function_exec_get_global_state, info);
            let local_state = get_local_state!(ffi::duckdb_v2_table_function_exec_get_local_state, info);

            let output_chunk = DataChunk {
                handle: check_api_call!(ffi::duckdb_v2_table_function_exec_get_output_chunk, info, RET)?,
                is_owned: false,
                is_writable: true,
            };

            T::exec(
                user_data,
                bind_data,
                global_state,
                local_state,
                Context(context),
                output_chunk,
            )?;

            Ok(())
        },
        err,
    );
}

/// Projected-column metadata supplied during table-function initialization.
pub struct InitColumnData<'a> {
    handle: &'a ffi::duckdb_v2_table_function_init_info_handle,
}

impl<'a> InitColumnData<'a> {
    /// Return the number of columns requested by the query.
    pub fn get_column_count(&self) -> Result<usize> {
        let column_count = check_api_call!(ffi::duckdb_v2_table_function_init_get_column_count, *self.handle, RET)?;

        Ok(column_count as usize)
    }

    /// Map a projected position to its bind-declared result-column index.
    pub fn get_column_index(&self, projected_index: usize) -> Result<usize> {
        let original_index = check_api_call!(
            ffi::duckdb_v2_table_function_init_get_column_index,
            *self.handle,
            projected_index as u64,
            RET
        )?;

        Ok(original_index as usize)
    }
}

/// Candidate filters and column mappings offered for pushdown.
pub struct FilterColumnData<'a> {
    handle: &'a ffi::duckdb_v2_table_function_filter_info_handle,
}

impl<'a> FilterColumnData<'a> {
    /// Return the number of columns in the pushdown-time column list.
    pub fn get_column_count(&self) -> Result<usize> {
        let column_count = check_api_call!(ffi::duckdb_v2_table_function_filter_get_column_count, *self.handle, RET)?;

        Ok(column_count as usize)
    }

    /// Map a pushdown-time position to its bind-declared column index.
    pub fn get_column_index(&self, projected_index: usize) -> Result<usize> {
        let original_index = check_api_call!(
            ffi::duckdb_v2_table_function_filter_get_column_index,
            *self.handle,
            projected_index as u64,
            RET
        )?;

        Ok(original_index as usize)
    }

    /// Borrow a candidate filter expression.
    pub fn get_expression(&self, index: usize) -> Result<Expression<'a>> {
        let expression_handle = check_api_call!(
            ffi::duckdb_v2_table_function_filter_get_expression,
            *self.handle,
            index as u64,
            RET
        )?;

        Ok(Expression {
            handle: expression_handle,
            _marker: std::marker::PhantomData,
        })
    }

    /// Mark a filter that the table function will apply itself.
    ///
    /// DuckDB removes marked filters from the plan above the scan. Leave a
    /// filter unmarked unless the function will enforce it completely.
    pub fn mark_handled(&self, index: usize) -> Result<()> {
        check_api_call!(
            ffi::duckdb_v2_table_function_filter_mark_handled,
            *self.handle,
            index as u64
        )?;

        Ok(())
    }
}

unsafe extern "C" fn init_global_callback<T: TableFunctionCallbacks>(
    info: ffi::duckdb_v2_table_function_init_info_handle,
    context: ffi::duckdb_v2_context_handle,
    err: *mut ffi::duckdb_v2_error_info_handle,
) {
    handle_unwind(
        || {
            let user_data = get_user_data!(ffi::duckdb_v2_table_function_init_get_user_data, info);
            let bind_data = get_bind_data!(ffi::duckdb_v2_table_function_init_get_bind_data, info);

            let (global_state, max_threads) =
                T::init_global_state(user_data, bind_data, Context(context), InitColumnData { handle: &info })?;

            if let Some(global_state) = global_state {
                check_api_call!(
                    ffi::duckdb_v2_table_function_init_set_global_state,
                    info,
                    into_opaque(global_state)
                )?;
            }

            if let Some(max_threads) = max_threads {
                dbg!(max_threads);
                check_api_call!(
                    ffi::duckdb_v2_table_function_init_set_max_threads,
                    info,
                    max_threads as u64
                )?;
            }

            Ok(())
        },
        err,
    );
}

unsafe extern "C" fn init_local_callback<T: TableFunctionCallbacks>(
    info: ffi::duckdb_v2_table_function_init_info_handle,
    context: ffi::duckdb_v2_context_handle,
    err: *mut ffi::duckdb_v2_error_info_handle,
) {
    handle_unwind(
        || {
            let user_data = get_user_data!(ffi::duckdb_v2_table_function_init_get_user_data, info);

            let bind_data = get_bind_data!(ffi::duckdb_v2_table_function_init_get_bind_data, info);

            let global_state = get_global_state!(ffi::duckdb_v2_table_function_init_get_global_state, info);

            let local_state = T::init_local_state(
                user_data,
                bind_data,
                Context(context),
                global_state,
                InitColumnData { handle: &info },
            )?;

            if let Some(local_state) = local_state {
                check_api_call!(
                    ffi::duckdb_v2_table_function_init_set_local_state,
                    info,
                    into_opaque(local_state)
                )?;
            }

            Ok(())
        },
        err,
    );
}

unsafe extern "C" fn progress_callback<T: TableFunctionCallbacks>(
    bind_data: *mut ::std::os::raw::c_void,
    global_state: *mut ::std::os::raw::c_void,
    out_progress: *mut f64,
    context: ffi::duckdb_v2_context_handle,
    err: *mut ffi::duckdb_v2_error_info_handle,
) {
    handle_unwind(
        || {
            let bind_data = unsafe { get_opaque_data_ref::<T::BindData>(bind_data) };
            let global_state = unsafe { get_opaque_data_ref::<T::GlobalState>(global_state) };

            if let Some(progress) = T::progress(bind_data, global_state, Context(context))? {
                unsafe {
                    *out_progress = progress;
                }
            }

            Ok(())
        },
        err,
    );
}

unsafe extern "C" fn cardinality_callback<T: TableFunctionCallbacks>(
    bind_data: *mut ::std::os::raw::c_void,
    out_estimated: *mut ffi::idx_t,
    out_is_exact: *mut bool,
    context: ffi::duckdb_v2_context_handle,
    err: *mut ffi::duckdb_v2_error_info_handle,
) {
    handle_unwind(
        || {
            let bind_data = unsafe { get_opaque_data_ref::<T::BindData>(bind_data) };

            if let Some(cardinality) = T::cardinality(bind_data, Context(context))? {
                unsafe {
                    *out_estimated = cardinality.cardinality as u64;
                    *out_is_exact = cardinality.is_exact;
                }
            }

            Ok(())
        },
        err,
    );
}

/// Candidate filter expressions offered during optimization.
pub struct TableFunctionFilterHandle {
    handle: ffi::duckdb_v2_table_function_filter_info_handle,
}

impl TableFunctionFilterHandle {
    /// Return the number of candidate filters.
    pub fn count(&self) -> Result<usize> {
        let count = check_api_call!(ffi::duckdb_v2_table_function_filter_get_count, self.handle, RET)?;

        Ok(count as usize)
    }

    /// Borrow a candidate filter expression.
    pub fn expression<'a>(&'a self, index: usize) -> Result<Expression<'a>> {
        let handle = check_api_call!(
            ffi::duckdb_v2_table_function_filter_get_expression,
            self.handle,
            index as u64,
            RET
        )?;

        Ok(Expression {
            handle,
            _marker: std::marker::PhantomData,
        })
    }
}

unsafe extern "C" fn pushdown_complex_filter_callback<T: TableFunctionCallbacks>(
    bind_data: *mut ::std::os::raw::c_void,
    info: ffi::duckdb_v2_table_function_filter_info_handle,
    context: ffi::duckdb_v2_context_handle,
    err: *mut ffi::duckdb_v2_error_info_handle,
) {
    handle_unwind(
        || {
            let user_data = get_user_data!(ffi::duckdb_v2_table_function_filter_get_user_data, info);
            let bind_data = unsafe { get_opaque_data_ref::<T::BindData>(bind_data) };

            T::pushdown_complex_filter(
                user_data,
                bind_data,
                Context(context),
                FilterColumnData { handle: &info },
            )
        },
        err,
    );
}

/// Builds and registers a user-defined table function.
pub struct TableFunctionBuilder<T: TableFunctionCallbacks> {
    name: String,
    signature: SignatureBuilder,
    user_data: OpaqueHandle<T>,
    projection_pushdown: bool,
}

impl<T: TableFunctionCallbacks> TableFunctionBuilder<T> {
    /// Create a builder from a name, signature, and callback implementation.
    pub fn new(name: &str, signature: SignatureBuilder, implementation: T) -> Self {
        TableFunctionBuilder {
            name: name.to_string(),
            signature,
            user_data: OpaqueHandle::new(implementation),
            projection_pushdown: false,
        }
    }

    /// Enable or disable projection pushdown.
    ///
    /// When enabled, [`InitColumnData`] reports only requested columns and maps
    /// them back to the bind-declared schema. Execution output contains those
    /// projected columns in the reported order.
    pub fn set_projection_pushdown(mut self, projection_pushdown: bool) -> Self {
        self.projection_pushdown = projection_pushdown;
        self
    }

    /// Build an owned table-function builder handle.
    pub fn build(&self) -> Result<TableFunctionBuilderHandle> {
        let handle = TableFunctionBuilderHandle(check_api_call!(ffi::duckdb_v2_table_function_builder_create, RET)?);

        check_api_call!(
            ffi::duckdb_v2_table_function_builder_set_name,
            *handle,
            (&self.name).into()
        )?;

        check_api_call!(
            ffi::duckdb_v2_table_function_builder_set_signature,
            *handle,
            *self.signature.build()?
        )?;

        check_api_call!(
            ffi::duckdb_v2_table_function_builder_set_projection_pushdown,
            *handle,
            self.projection_pushdown
        )?;

        check_api_call!(
            ffi::duckdb_v2_table_function_builder_set_user_data,
            *handle,
            self.user_data.to_handle()
        )?;

        check_api_call!(
            ffi::duckdb_v2_table_function_builder_set_init_local_callback,
            *handle,
            Some(init_local_callback::<T>)
        )?;

        check_api_call!(
            ffi::duckdb_v2_table_function_builder_set_init_global_callback,
            *handle,
            Some(init_global_callback::<T>)
        )?;

        check_api_call!(
            ffi::duckdb_v2_table_function_builder_set_progress_callback,
            *handle,
            Some(progress_callback::<T>)
        )?;

        check_api_call!(
            ffi::duckdb_v2_table_function_builder_set_cardinality_callback,
            *handle,
            Some(cardinality_callback::<T>)
        )?;

        check_api_call!(
            ffi::duckdb_v2_table_function_builder_set_pushdown_complex_filter_callback,
            *handle,
            Some(pushdown_complex_filter_callback::<T>)
        )?;

        // required
        check_api_call!(
            ffi::duckdb_v2_table_function_builder_set_bind_callback,
            *handle,
            Some(bind_callback::<T>)
        )?;

        check_api_call!(
            ffi::duckdb_v2_table_function_builder_set_exec_callback,
            *handle,
            Some(exec_callback::<T>)
        )?;

        Ok(handle)
    }

    context_and_connection_fn! {
        /// Register the function through a connection or callback context.
        pub fn register_with_[context, connection](self) -> Result<()>
        {
            context_fn: ffi::duckdb_v2_table_function_builder_register_with_context,
            connection_fn: ffi::duckdb_v2_table_function_builder_register_with_connection,
        }
        let handle = self.build()?;

        check_api_call!(
            api_fn!(),
            **api_arg!(),
            *handle
        )?;

        Ok(())
    }
}

/// A table function's estimated or exact output row count.
pub struct TableFunctionCardinality {
    /// The reported output row count.
    pub cardinality: usize,
    /// Whether the row count is exact rather than estimated.
    pub is_exact: bool,
}

/// Callback lifecycle for a user-defined table function.
///
/// DuckDB binds the call and output schema, initializes shared and worker-local
/// scan state, then repeatedly requests output chunks. Optional callbacks
/// expose cardinality, progress, projections, and filter pushdown.
pub trait TableFunctionCallbacks: Send + Sync + 'static {
    /// Data shared from binding through optimization and execution.
    type BindData: Any + Send + Sync;
    /// Mutable state local to one execution worker.
    type LocalState: Any + Send + 'static;
    /// State shared by all workers scanning one function instance.
    type GlobalState: Any + Send + Sync;

    /// **Execute:** produce one output chunk.
    ///
    /// Set the output cardinality to zero when the scan is exhausted.
    fn exec(
        &self,
        bind_data: Option<&Self::BindData>,
        global_state: Option<&Self::GlobalState>,
        local_state: Option<&mut Self::LocalState>,
        context: Context,
        output: DataChunk,
    ) -> Result<()>;

    /// **Bind:** validate arguments, declare columns, and create shared data.
    fn bind(
        &self,
        context: Context,
        metadata: BindArguments,
        bind_handle: BindFunctionHandle,
    ) -> Result<(Self::BindData, Option<TableFunctionCardinality>)>;

    /// **Estimate:** report an output row count for optimization.
    ///
    /// This callback may run multiple times and should be cheap and
    /// side-effect-free.
    fn cardinality(_bind_data: Option<&Self::BindData>, _context: Context) -> Result<Option<TableFunctionCardinality>> {
        Ok(None)
    }

    /// **Progress:** report execution progress from `0.0` to `1.0`.
    fn progress(
        _bind_data: Option<&Self::BindData>,
        _global_state: Option<&Self::GlobalState>,
        _context: Context,
    ) -> Result<Option<f64>> {
        Ok(None)
    }

    /// **Initialize local:** create state for one execution worker.
    fn init_local_state(
        &self,
        _bind_data: Option<&Self::BindData>,
        _context: Context,
        _global_state: Option<&Self::GlobalState>,
        _column_data: InitColumnData,
    ) -> Result<Option<Self::LocalState>> {
        Ok(None)
    }

    /// **Initialize global:** create shared scan state and an optional thread limit.
    fn init_global_state(
        &self,
        _bind_data: Option<&Self::BindData>,
        _context: Context,
        _column_data: InitColumnData,
    ) -> Result<(Option<Self::GlobalState>, Option<usize>)> {
        Ok((None, None))
    }

    /// **Push down filters:** inspect and claim filters applied by the scan.
    fn pushdown_complex_filter(
        &self,
        _bind_data: Option<&Self::BindData>,
        _context: Context,
        _column_data: FilterColumnData,
    ) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests;

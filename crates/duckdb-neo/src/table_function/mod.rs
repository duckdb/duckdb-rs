//! User-defined table functions and their scan lifecycle.
//!
//! Implement [`TableFunctionCallbacks`] to bind arguments and declare output
//! columns, initialize shared and worker-local scan state, and produce chunks.
//! [`TableFunctionBuilder`] registers the implementation and configures
//! projection pushdown. Optional cardinality estimates help DuckDB optimize
//! query plans, while progress and complex-filter callbacks expose additional
//! execution and pushdown behavior.

use std::any::Any;

use libduckdb_sys::v2 as ffi;

use crate::{
    Result,
    bind_arguments::{BindArgument, BindMetadata, BindType},
    builder_helpers::{
        OpaqueHandle, get_bind_data, get_global_state, get_local_state, get_user_data, handle_unwind, into_opaque,
    },
    check_api_call,
    connection::Context,
    data_chunk::DataChunkRef,
    expression::Expression,
    handles::{TableFunctionBuilderHandle, TableFunctionBuilderLink},
    logical_type::LogicalType,
    signature::SignatureBuilder,
    table_function::InitColumnHandle::{Global, Local},
};

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

            let metadata = BindMetadata {
                bind_type: BindType::Table(&info),
            };

            let (bind_data, cardinality) = T::bind(
                user_data,
                Context(context),
                metadata.get_arguments()?,
                BindFunctionHandle(&info),
            )?;

            check_api_call!(
                ffi::duckdb_v2_table_function_bind_set_bind_data,
                info,
                &mut into_opaque(bind_data)
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

pub struct ExecColumnInfo<'a> {
    handle: &'a ffi::duckdb_v2_table_function_exec_info_handle,
}

impl<'a> ExecColumnInfo<'a> {
    pub fn count(&self) -> Result<usize> {
        check_api_call!(ffi::duckdb_v2_table_function_exec_get_column_count, *self.handle, RET).map(|x| x as usize)
    }

    pub fn get_column_index(&self, index: usize) -> Result<usize> {
        check_api_call!(
            ffi::duckdb_v2_table_function_exec_get_column_index,
            *self.handle,
            index as u64,
            RET
        )
        .map(|x| x as usize)
    }
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

            let output_chunk = DataChunkRef::new(
                check_api_call!(ffi::duckdb_v2_table_function_exec_get_output_chunk, info, RET)?,
                true,
            );

            T::exec(
                user_data,
                bind_data,
                global_state,
                local_state,
                Context(context),
                output_chunk,
                ExecColumnInfo { handle: &info },
            )?;

            Ok(())
        },
        err,
    );
}

enum InitColumnHandle<'a> {
    Local(&'a ffi::duckdb_v2_table_function_init_local_info_handle),
    Global(&'a ffi::duckdb_v2_table_function_init_global_info_handle),
}

/// Projected-column metadata supplied during table-function initialization.
pub struct InitColumnData<'a> {
    handle: InitColumnHandle<'a>,
}

impl<'a> InitColumnData<'a> {
    /// Return the number of columns requested by the query.
    pub fn get_column_count(&self) -> Result<usize> {
        let column_count = match self.handle {
            Local(handle) => check_api_call!(ffi::duckdb_v2_table_function_init_local_get_column_count, *handle, RET),
            Global(handle) => check_api_call!(ffi::duckdb_v2_table_function_init_global_get_column_count, *handle, RET),
        }?;

        Ok(column_count as usize)
    }

    /// Map a projected position to its bind-declared result-column index.
    pub fn get_column_index(&self, projected_index: usize) -> Result<usize> {
        let original_index = match self.handle {
            Local(handle) => check_api_call!(
                ffi::duckdb_v2_table_function_init_local_get_column_index,
                *handle,
                projected_index as u64,
                RET
            ),
            Global(handle) => check_api_call!(
                ffi::duckdb_v2_table_function_init_global_get_column_index,
                *handle,
                projected_index as u64,
                RET
            ),
        }?;

        Ok(original_index as usize)
    }
}

/// Candidate filters and column mappings offered for pushdown.
pub struct PushdownData<'a> {
    handle: &'a ffi::duckdb_v2_table_function_filter_pushdown_info_handle,
}

impl<'a> PushdownData<'a> {
    /// Return the number of columns in the pushdown-time column list.
    pub fn get_column_count(&self) -> Result<usize> {
        let column_count = check_api_call!(
            ffi::duckdb_v2_table_function_filter_pushdown_get_column_count,
            *self.handle,
            RET
        )?;

        Ok(column_count as usize)
    }

    /// Map a pushdown-time position to its bind-declared column index.
    pub fn get_column_index(&self, projected_index: usize) -> Result<usize> {
        let original_index = check_api_call!(
            ffi::duckdb_v2_table_function_filter_pushdown_get_column_index,
            *self.handle,
            projected_index as u64,
            RET
        )?;

        Ok(original_index as usize)
    }

    /// Return the number of filter predicates offered for pushdown, combined with `AND`.
    pub fn filter_count(&self) -> Result<usize> {
        let value = check_api_call!(
            ffi::duckdb_v2_table_function_filter_pushdown_get_filter_count,
            *self.handle,
            RET
        )?;

        Ok(value as usize)
    }

    /// Borrow the bound boolean filter expression at `index` for the duration of the callback.
    pub fn filter(&self, index: usize) -> Result<Expression<'a>> {
        let expression_handle = check_api_call!(
            ffi::duckdb_v2_table_function_filter_pushdown_get_filter,
            *self.handle,
            index as u64,
            RET
        )?;

        Ok(Expression {
            handle: expression_handle,
            _marker: std::marker::PhantomData,
        })
    }

    /// Accept responsibility for applying the filter at `index` in the table function.
    ///
    /// DuckDB stops applying accepted filters, so every emitted row must satisfy
    /// them. Unaccepted filters remain enforced by DuckDB.
    pub fn accept_pushdown(&self, index: usize) -> Result<()> {
        check_api_call!(
            ffi::duckdb_v2_table_function_filter_pushdown_accept,
            *self.handle,
            index as u64
        )
    }
}

unsafe extern "C" fn init_global_callback<T: TableFunctionCallbacks>(
    info: ffi::duckdb_v2_table_function_init_global_info_handle,
    context: ffi::duckdb_v2_context_handle,
    err: *mut ffi::duckdb_v2_error_info_handle,
) {
    handle_unwind(
        || {
            let user_data = get_user_data!(ffi::duckdb_v2_table_function_init_global_get_user_data, info);
            let bind_data = get_bind_data!(ffi::duckdb_v2_table_function_init_global_get_bind_data, info);

            let (global_state, max_threads) = T::init_global_state(
                user_data,
                bind_data,
                Context(context),
                InitColumnData { handle: Global(&info) },
            )?;

            if let Some(global_state) = global_state {
                check_api_call!(
                    ffi::duckdb_v2_table_function_init_global_set_global_state,
                    info,
                    &mut into_opaque(global_state)
                )?;
            }

            if let Some(max_threads) = max_threads {
                dbg!(max_threads);
                check_api_call!(
                    ffi::duckdb_v2_table_function_init_global_set_max_threads,
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
    info: ffi::duckdb_v2_table_function_init_local_info_handle,
    context: ffi::duckdb_v2_context_handle,
    err: *mut ffi::duckdb_v2_error_info_handle,
) {
    handle_unwind(
        || {
            let user_data = get_user_data!(ffi::duckdb_v2_table_function_init_local_get_user_data, info);
            let bind_data = get_bind_data!(ffi::duckdb_v2_table_function_init_local_get_bind_data, info);
            let global_state = get_global_state!(ffi::duckdb_v2_table_function_init_local_get_global_state, info);

            let local_state = T::init_local_state(
                user_data,
                bind_data,
                Context(context),
                global_state,
                InitColumnData { handle: Local(&info) },
            )?;

            if let Some(local_state) = local_state {
                check_api_call!(
                    ffi::duckdb_v2_table_function_init_local_set_local_state,
                    info,
                    &mut into_opaque(local_state)
                )?;
            }

            Ok(())
        },
        err,
    );
}

unsafe extern "C" fn progress_callback<T: TableFunctionCallbacks>(
    info: ffi::duckdb_v2_table_function_progress_info_handle,
    ctx: ffi::duckdb_v2_context_handle,
    err: *mut ffi::duckdb_v2_error_info_handle,
) {
    handle_unwind(
        || {
            let user_data = get_user_data!(ffi::duckdb_v2_table_function_progress_get_user_data, info);
            let bind_data = get_bind_data!(ffi::duckdb_v2_table_function_progress_get_bind_data, info);
            let global_state = get_global_state!(ffi::duckdb_v2_table_function_progress_get_global_state, info);

            if let Some(progress) = T::progress(user_data, bind_data, global_state, Context(ctx))? {
                check_api_call!(ffi::duckdb_v2_table_function_progress_set_progress, info, progress)?
            }

            Ok(())
        },
        err,
    );
}

unsafe extern "C" fn filter_pushdown_callback<T: TableFunctionCallbacks>(
    info: ffi::duckdb_v2_table_function_filter_pushdown_info_handle,
    ctx: ffi::duckdb_v2_context_handle,
    err: *mut ffi::duckdb_v2_error_info_handle,
) {
    handle_unwind(
        || {
            let user_data = get_user_data!(ffi::duckdb_v2_table_function_filter_pushdown_get_user_data, info);
            let bind_data = get_bind_data!(ffi::duckdb_v2_table_function_filter_pushdown_get_bind_data, info);

            T::pushdown_filter(user_data, bind_data, Context(ctx), PushdownData { handle: &info })
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
    fn build(&self, handle: &TableFunctionBuilderHandle) -> Result<()> {
        check_api_call!(
            ffi::duckdb_v2_table_function_set_name,
            **handle,
            &mut (&self.name).into()
        )?;

        let signature = check_api_call!(ffi::duckdb_v2_table_function_get_signature, **handle, RET)?;

        self.signature.build(&signature)?;

        check_api_call!(
            ffi::duckdb_v2_table_function_set_projection_pushdown,
            **handle,
            self.projection_pushdown
        )?;

        check_api_call!(
            ffi::duckdb_v2_table_function_set_user_data,
            **handle,
            &mut self.user_data.to_handle()
        )?;

        check_api_call!(
            ffi::duckdb_v2_table_function_set_init_local_callback,
            **handle,
            Some(init_local_callback::<T>)
        )?;

        check_api_call!(
            ffi::duckdb_v2_table_function_set_init_global_callback,
            **handle,
            Some(init_global_callback::<T>)
        )?;

        check_api_call!(
            ffi::duckdb_v2_table_function_set_progress_callback,
            **handle,
            Some(progress_callback::<T>)
        )?;

        check_api_call!(
            ffi::duckdb_v2_table_function_set_filter_pushdown_callback,
            **handle,
            Some(filter_pushdown_callback::<T>)
        )?;

        // required
        check_api_call!(
            ffi::duckdb_v2_table_function_set_bind_callback,
            **handle,
            Some(bind_callback::<T>)
        )?;

        check_api_call!(
            ffi::duckdb_v2_table_function_set_exec_callback,
            **handle,
            Some(exec_callback::<T>)
        )?;

        Ok(())
    }

    /// Register through a connection or extension, consuming the builder.
    #[allow(private_bounds)]
    pub fn register<C: TableFunctionBuilderLink>(self, link: &C) -> Result<()> {
        let handle = link.create_table_function_handle()?;
        self.build(&handle)?;

        check_api_call!(ffi::duckdb_v2_table_function_register, *handle)
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
        output: DataChunkRef<'_>,
        column_info: ExecColumnInfo<'_>,
    ) -> Result<()>;

    /// **Bind:** validate arguments, declare columns, and create shared data.
    fn bind(
        &self,
        context: Context,
        arguments: Vec<BindArgument>,
        bind_handle: BindFunctionHandle<'_>,
    ) -> Result<(Self::BindData, Option<TableFunctionCardinality>)>;

    /// **Progress:** report execution progress from `0.0` to `1.0`.
    fn progress(
        &self,
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
        _column_data: InitColumnData<'_>,
    ) -> Result<Option<Self::LocalState>> {
        Ok(None)
    }

    /// **Initialize global:** create shared scan state and an optional thread limit.
    fn init_global_state(
        &self,
        _bind_data: Option<&Self::BindData>,
        _context: Context,
        _column_data: InitColumnData<'_>,
    ) -> Result<(Option<Self::GlobalState>, Option<usize>)> {
        Ok((None, None))
    }

    /// **Push down filters:** inspect and claim filters applied by the scan.
    fn pushdown_filter(
        &self,
        _bind_data: Option<&Self::BindData>,
        _context: Context,
        _column_data: PushdownData<'_>,
    ) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests;

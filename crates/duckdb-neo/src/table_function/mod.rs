//! User-defined table functions and their scan lifecycle.
//!
//! Implement [`TableFunctionCallbacks`] to bind arguments and declare output
//! columns, initialize shared and worker-local scan state, and produce chunks.
//! [`TableFunctionBuilder`] registers the implementation and configures
//! projection pushdown. Optional cardinality estimates help DuckDB optimize
//! query plans, while progress and complex-filter callbacks expose additional
//! execution and pushdown behavior. Functions that can describe the batches
//! they produce implement [`TablePartitioningCallbacks`] on top.

use std::any::Any;

use libduckdb_sys::v2 as ffi;

use crate::{
    Result,
    bind_arguments::{BindArgument, BindMetadata, BindType},
    builder_helpers::{
        OpaqueHandle, ffi_enum_redeclaration, get_bind_data, get_global_state, get_local_state, get_user_data,
        handle_unwind, into_opaque,
    },
    check_api_call,
    connection::Context,
    data_chunk::DataChunkRef,
    expression::Expression,
    handles::{TableFunctionBuilderHandle, TableFunctionBuilderLink},
    logical_type::LogicalType,
    signature::SignatureBuilder,
    table_function::InitColumnHandle::{Global, Local},
    value::Value,
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

ffi_enum_redeclaration! {
    /// Whether, and how, a scan's partitions line up with a set of columns.
    ///
    /// Reported by [`TablePartitioningCallbacks::partitioning`].
    pub enum TablePartitionInfo <- ffi::DUCKDB_V2_TABLE_PARTITION_INFO {
        /// The scan is not known to be partitioned by the requested columns.
        NotPartitioned = DUCKDB_V2_TABLE_PARTITION_INFO_NOT_PARTITIONED,
        /// Each produced partition carries exactly one distinct value for the requested columns.
        ///
        /// The only variant that unlocks the partitioned aggregate optimization.
        SingleValuePartitions = DUCKDB_V2_TABLE_PARTITION_INFO_SINGLE_VALUE_PARTITIONS,
        /// The produced partitions overlap only at their boundaries.
        OverlappingPartitions = DUCKDB_V2_TABLE_PARTITION_INFO_OVERLAPPING_PARTITIONS,
        /// The produced partitions are disjoint ranges.
        DisjointPartitions = DUCKDB_V2_TABLE_PARTITION_INFO_DISJOINT_PARTITIONS,
    }
}

/// What a downstream operator wants to know about the batch just produced.
pub struct PartitionData<'a> {
    handle: &'a ffi::duckdb_v2_table_function_partition_data_info_handle,
}

impl<'a> PartitionData<'a> {
    /// Return whether a downstream operator needs this batch's ordering position.
    pub fn requires_batch_index(&self) -> Result<bool> {
        check_api_call!(
            ffi::duckdb_v2_table_function_partition_data_requires_batch_index,
            *self.handle,
            RET
        )
    }

    /// Return whether a downstream operator needs this batch's partitioning-column values.
    ///
    /// When true, call [`set_partition_value`](Self::set_partition_value) once
    /// for every index below [`get_column_count`](Self::get_column_count).
    pub fn requires_partition_columns(&self) -> Result<bool> {
        check_api_call!(
            ffi::duckdb_v2_table_function_partition_data_requires_partition_columns,
            *self.handle,
            RET
        )
    }

    /// Return the number of partitioning columns values are requested for.
    ///
    /// Zero when [`requires_partition_columns`](Self::requires_partition_columns) is false.
    pub fn get_column_count(&self) -> Result<usize> {
        let column_count = check_api_call!(
            ffi::duckdb_v2_table_function_partition_data_get_partition_column_count,
            *self.handle,
            RET
        )?;

        Ok(column_count as usize)
    }

    /// Map a requested partitioning column to its bind-declared column index.
    pub fn get_column_index(&self, partition_index: usize) -> Result<usize> {
        let original_index = check_api_call!(
            ffi::duckdb_v2_table_function_partition_data_get_partition_column_index,
            *self.handle,
            partition_index as u64,
            RET
        )?;

        Ok(original_index as usize)
    }

    /// Report the single value the batch carries for the partitioning column at `partition_index`.
    ///
    /// The value must have the declared type of the corresponding result
    /// column. Calling this again for the same index overwrites the previous
    /// value. Reported values only take effect together with a changed batch
    /// index, so [`TablePartitioningCallbacks::partition_data`] must return a new
    /// index whenever these values change.
    pub fn set_partition_value(&self, partition_index: usize, value: &Value) -> Result<()> {
        check_api_call!(
            ffi::duckdb_v2_table_function_partition_data_set_partition_value,
            *self.handle,
            partition_index as u64,
            value.handle
        )
    }
}

/// The candidate `GROUP BY` column set the optimizer is asking about.
pub struct PartitioningData<'a> {
    handle: &'a ffi::duckdb_v2_table_function_partitioning_info_handle,
}

impl<'a> PartitioningData<'a> {
    /// Return the number of columns in the candidate column set.
    pub fn get_column_count(&self) -> Result<usize> {
        let column_count = check_api_call!(
            ffi::duckdb_v2_table_function_partitioning_get_partition_column_count,
            *self.handle,
            RET
        )?;

        Ok(column_count as usize)
    }

    /// Map a candidate column to its bind-declared column index.
    pub fn get_column_index(&self, partition_index: usize) -> Result<usize> {
        let original_index = check_api_call!(
            ffi::duckdb_v2_table_function_partitioning_get_partition_column_index,
            *self.handle,
            partition_index as u64,
            RET
        )?;

        Ok(original_index as usize)
    }
}

unsafe extern "C" fn partition_data_callback<T: TablePartitioningCallbacks>(
    info: ffi::duckdb_v2_table_function_partition_data_info_handle,
    ctx: ffi::duckdb_v2_context_handle,
    err: *mut ffi::duckdb_v2_error_info_handle,
) {
    handle_unwind(
        || {
            let user_data = get_user_data!(ffi::duckdb_v2_table_function_partition_data_get_user_data, info);
            let bind_data = get_bind_data!(ffi::duckdb_v2_table_function_partition_data_get_bind_data, info);
            let global_state = get_global_state!(ffi::duckdb_v2_table_function_partition_data_get_global_state, info);
            let local_state = get_local_state!(ffi::duckdb_v2_table_function_partition_data_get_local_state, info);

            let batch_index = T::partition_data(
                user_data,
                bind_data,
                global_state,
                local_state,
                Context(ctx),
                PartitionData { handle: &info },
            )?;

            // DuckDB validates the batch index on every call, whether or not a
            // downstream operator ends up using it.
            check_api_call!(
                ffi::duckdb_v2_table_function_partition_data_set_batch_index,
                info,
                batch_index as u64
            )?;

            Ok(())
        },
        err,
    );
}

unsafe extern "C" fn partitioning_callback<T: TablePartitioningCallbacks>(
    info: ffi::duckdb_v2_table_function_partitioning_info_handle,
    ctx: ffi::duckdb_v2_context_handle,
    err: *mut ffi::duckdb_v2_error_info_handle,
) {
    handle_unwind(
        || {
            let user_data = get_user_data!(ffi::duckdb_v2_table_function_partitioning_get_user_data, info);
            let bind_data = get_bind_data!(ffi::duckdb_v2_table_function_partitioning_get_bind_data, info);

            let partition_info =
                T::partitioning(user_data, bind_data, Context(ctx), PartitioningData { handle: &info })?;

            check_api_call!(
                ffi::duckdb_v2_table_function_partitioning_set_partition_info,
                info,
                partition_info.into()
            )?;

            Ok(())
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
    partition_data_callback: ffi::duckdb_v2_table_function_partition_data_callback_fn,
    partitioning_callback: ffi::duckdb_v2_table_function_partitioning_callback_fn,
}

impl<T: TablePartitioningCallbacks> TableFunctionBuilder<T> {
    /// Report partition data to DuckDB, enabling [`TablePartitioningCallbacks`].
    ///
    /// DuckDB only accepts the two partitioning callbacks together, so this
    /// registers both. It tells the optimizer the scan can describe the batches
    /// it produces: DuckDB then relies on the batch index reported by
    /// [`TablePartitioningCallbacks::partition_data`] instead of the order in
    /// which batches arrive to preserve insertion order, and can turn a
    /// matching hash aggregate into a partitioned one.
    pub fn with_partitioning(mut self) -> Self {
        self.partition_data_callback = Some(partition_data_callback::<T>);
        self.partitioning_callback = Some(partitioning_callback::<T>);
        self
    }
}

impl<T: TableFunctionCallbacks> TableFunctionBuilder<T> {
    /// Create a builder from a name, signature, and callback implementation.
    pub fn new(name: &str, signature: SignatureBuilder, implementation: T) -> Self {
        TableFunctionBuilder {
            name: name.to_string(),
            signature,
            user_data: OpaqueHandle::new(implementation),
            projection_pushdown: false,
            partition_data_callback: None,
            partitioning_callback: None,
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

        // Registering the partition data callback makes DuckDB order batches by
        // the index it reports, so it is only registered through
        // `with_partitioning`. DuckDB rejects the partitioning callback unless
        // both are set.
        if self.partition_data_callback.is_some() {
            check_api_call!(
                ffi::duckdb_v2_table_function_set_partition_data_callback,
                **handle,
                self.partition_data_callback
            )?;

            check_api_call!(
                ffi::duckdb_v2_table_function_set_partitioning_callback,
                **handle,
                self.partitioning_callback
            )?;
        }

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

/// Describes the batches a table function produces to DuckDB.
///
/// Implement this for table functions that can report where the batch they
/// just produced sits in the scan's order, and optionally that their partitions
/// carry a single distinct value for some columns. Enable the callbacks with
/// [`TableFunctionBuilder::with_partitioning`]; they are ignored, and never
/// registered, without it.
///
/// Reporting batch indices makes DuckDB order the scan's output by them instead
/// of by the order batches arrive in, which is what lets an insertion-order
/// preserving query stay parallel. Claiming single-value partitions lets the
/// optimizer replace a hash aggregate with a partitioned one.
pub trait TablePartitioningCallbacks: TableFunctionCallbacks {
    /// **Partition data:** describe the batch [`exec`](TableFunctionCallbacks::exec) just produced.
    ///
    /// Runs on the worker thread that produced the batch, and only when a
    /// downstream operator needs the batch's ordering position, the values of a
    /// set of partitioning columns, or both; [`PartitionData`] reports which of
    /// those were requested and receives the column values.
    ///
    /// The returned batch index is the batch's ordering position. It must not
    /// decrease across successive calls on the same thread, must be unique
    /// across threads for the ordering to be meaningful, must stay below
    /// roughly `10^13`, and must change whenever the reported partitioning
    /// column values change. It is reported on every call, since DuckDB
    /// validates it even when nothing consumes it.
    fn partition_data(
        &self,
        bind_data: Option<&Self::BindData>,
        global_state: Option<&Self::GlobalState>,
        local_state: Option<&mut Self::LocalState>,
        context: Context,
        partition_data: PartitionData<'_>,
    ) -> Result<usize>;

    /// **Partitioning:** report how the scan partitions a candidate `GROUP BY` column set.
    ///
    /// Runs on the planning thread, once per candidate column set, and must be
    /// deterministic for a given set because the optimizer may discard the plan
    /// it was called for. Only [`TablePartitionInfo::SingleValuePartitions`]
    /// unlocks the partitioned aggregate optimization; every other variant
    /// keeps the regular hash aggregate.
    ///
    /// Claiming single-value partitions commits
    /// [`partition_data`](Self::partition_data) to reporting the values of
    /// those columns for every batch.
    fn partitioning(
        &self,
        _bind_data: Option<&Self::BindData>,
        _context: Context,
        _partitioning_data: PartitioningData<'_>,
    ) -> Result<TablePartitionInfo> {
        Ok(TablePartitionInfo::NotPartitioned)
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests;

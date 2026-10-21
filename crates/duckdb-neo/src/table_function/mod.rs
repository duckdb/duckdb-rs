//! User-defined table functions and their scan lifecycle.
//!
//! Implement [`TableFunctionCallbacks`] to bind arguments and declare output
//! columns, initialize shared and worker-local scan state, and produce chunks.
//! [`TableFunctionBuilder`] registers the implementation and configures
//! projection pushdown. Optional cardinality estimates and progress reports
//! help DuckDB optimize query plans and track execution. Functions that can
//! apply the query's filters themselves implement
//! [`TableFilterPushdownCallbacks`] on top, and functions that can describe the
//! batches they produce implement [`TablePartitioningCallbacks`]. Readers wrapped
//! by a [`MultiFileFunctionBuilder`](crate::multi_file::MultiFileFunctionBuilder)
//! can also implement [`TableGetBindInfoCallbacks`] and [`TableClaimBatchCallbacks`].

use std::any::Any;

use crate::ffi;

use crate::{
    Error, Result,
    bind_arguments::{BindArgument, BindArguments},
    builder_helpers::{
        OpaqueHandle, ffi_enum_redeclaration, get_bind_data, get_global_state, get_local_state, get_user_data,
        handle_unwind, into_opaque,
    },
    check_api_call,
    connection::{Context, FFILink},
    data_chunk::DataChunkRef,
    expression::Expression,
    handles::{TableFunctionBuilderHandle, TableFunctionBuilderLink},
    logical_type::LogicalType,
    raw::{FromRaw, RawExt},
    signature::SignatureBuilder,
    table_function::InitColumnHandle::{Global, Local},
    value::Value,
};

ffi_enum_redeclaration! {
    pub enum OrderPreservation <- ffi::DUCKDB_V2_ORDER_PRESERVATION {
        None = DUCKDB_V2_ORDER_PRESERVATION_NO_ORDER,
        Insertion = DUCKDB_V2_ORDER_PRESERVATION_INSERTION_ORDER,
        Fixes = DUCKDB_V2_ORDER_PRESERVATION_FIXED_ORDER
    }
}
/// Callback-scoped output-schema builder.
#[derive(Debug)]
pub struct BindFunctionHandle<'a>(&'a ffi::duckdb_v2_table_function_bind_info_handle);

impl<'a> BindFunctionHandle<'a> {
    /// Append a result column in declaration order.
    pub fn add_result_column(&self, name: &str, logical_type: &LogicalType) -> Result<()> {
        check_api_call!(
            ffi::duckdb_v2_table_function_bind_add_result_column,
            *self.0,
            &name.into(),
            logical_type.raw()
        )
    }

    /// Hint the output row count to the optimizer; a different actual count is not an error.
    /// Setting `is_exact` also makes the estimate an upper bound.
    pub fn set_cardinality(&self, cardinality: usize, is_exact: bool) -> Result<()> {
        check_api_call!(
            ffi::duckdb_v2_table_function_bind_set_cardinality,
            *self.0,
            cardinality as u64,
            is_exact
        )
    }

    /// Set output ordering; defaults to [`OrderPreservation::Insertion`].
    /// [`OrderPreservation::None`] allows parallel output without a partition-data callback.
    pub fn set_order(&self, order: OrderPreservation) -> Result<()> {
        check_api_call!(
            ffi::duckdb_v2_table_function_bind_set_order_preservation,
            *self.0,
            order.into()
        )
    }
}

unsafe extern "C" fn bind_callback<T: TableFunctionCallbacks>(
    info: ffi::duckdb_v2_function_bind_info_handle,
    result: ffi::duckdb_v2_table_function_bind_info_handle,
    context: ffi::duckdb_v2_context_handle,
    err: *mut ffi::duckdb_v2_error_info_handle,
) {
    handle_unwind(
        || {
            let user_data = get_user_data!(ffi::duckdb_v2_function_bind_get_user_data, info);

            let arguments = BindArguments { handle: &info };

            let bind_data = T::bind(
                user_data,
                &unsafe { Context::from_raw(context) },
                arguments.collect()?,
                BindFunctionHandle(&result),
            )?;

            check_api_call!(
                ffi::duckdb_v2_function_bind_set_bind_data,
                info,
                &mut into_opaque(bind_data)
            )
        },
        err,
    );
}

/// Column layout of the scan's output during the exec callback.
///
/// With projection pushdown enabled, the columns here are the subset of
/// declared columns the query actually uses.
#[derive(Debug)]
pub struct ExecColumnInfo<'a> {
    handle: &'a ffi::duckdb_v2_table_function_exec_info_handle,
}

impl<'a> ExecColumnInfo<'a> {
    /// Return the number of columns the scan produces.
    pub fn count(&self) -> Result<usize> {
        check_api_call!(ffi::duckdb_v2_table_function_exec_get_column_count, *self.handle, RET).map(|x| x as usize)
    }

    /// Return the index of the declared column that the scan's column at `index` stands for.
    ///
    /// Fails if `index` is out of bounds.
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
            let bind_data = get_bind_data!(ffi::duckdb_v2_table_function_exec_get_bind_data, info)
                .ok_or_else(|| Error::api_error("Table function bind data not set"))?;

            let global_state = get_global_state!(ffi::duckdb_v2_table_function_exec_get_global_state, info);
            let local_state = get_local_state!(ffi::duckdb_v2_table_function_exec_get_local_state, info);

            let output_chunk = DataChunkRef::new(
                check_api_call!(ffi::duckdb_v2_table_function_exec_get_output_chunk, info, RET)?,
                true,
            )?;

            T::exec(
                user_data,
                bind_data,
                global_state,
                local_state,
                &unsafe { Context::from_raw(context) },
                output_chunk,
                ExecColumnInfo { handle: &info },
            )?;

            Ok(())
        },
        err,
    );
}

#[derive(Debug)]
enum InitColumnHandle<'a> {
    Local(&'a ffi::duckdb_v2_table_function_init_local_info_handle),
    Global(&'a ffi::duckdb_v2_table_function_init_global_info_handle),
}

/// Projected-column metadata supplied during table-function initialization.
#[derive(Debug)]
pub struct InitColumnData<'a> {
    handle: InitColumnHandle<'a>,
}

impl<'a> InitColumnData<'a> {
    /// Return the number of columns the scan produces.
    pub fn count(&self) -> Result<usize> {
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
#[derive(Debug)]
pub struct PushdownData<'a> {
    handle: &'a ffi::duckdb_v2_table_function_filter_pushdown_info_handle,
}

impl<'a> PushdownData<'a> {
    /// Return the number of columns in the pushdown-time column list.
    pub fn count(&self) -> Result<usize> {
        check_api_call!(
            ffi::duckdb_v2_table_function_filter_pushdown_get_column_count,
            *self.handle,
            RET
        )
        .map(|column_count| column_count as usize)
    }

    /// Map a pushdown-time position to its bind-declared column index.
    pub fn get_column_index(&self, projected_index: usize) -> Result<usize> {
        check_api_call!(
            ffi::duckdb_v2_table_function_filter_pushdown_get_column_index,
            *self.handle,
            projected_index as u64,
            RET
        )
        .map(|original_index| original_index as usize)
    }

    /// Return the number of filter predicates offered for pushdown, combined with `AND`.
    pub fn filter_count(&self) -> Result<usize> {
        check_api_call!(
            ffi::duckdb_v2_table_function_filter_pushdown_get_filter_count,
            *self.handle,
            RET
        )
        .map(|value| value as usize)
    }

    /// Borrow the bound boolean filter expression at `index` for the duration of the callback.
    pub fn filter(&self, index: usize) -> Result<Expression<'a>> {
        check_api_call!(
            ffi::duckdb_v2_table_function_filter_pushdown_get_filter,
            *self.handle,
            index as u64,
            RET
        )
        .map(|expression_handle| Expression {
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
            let bind_data = get_bind_data!(ffi::duckdb_v2_table_function_init_global_get_bind_data, info)
                .ok_or_else(|| Error::api_error("Table function bind data not set"))?;

            let (global_state, max_threads) = T::init_global_state(
                user_data,
                bind_data,
                &unsafe { Context::from_raw(context) },
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
            let bind_data = get_bind_data!(ffi::duckdb_v2_table_function_init_local_get_bind_data, info)
                .ok_or_else(|| Error::api_error("Table function bind data not set"))?;
            let global_state = get_global_state!(ffi::duckdb_v2_table_function_init_local_get_global_state, info);

            let local_state = T::init_local_state(
                user_data,
                bind_data,
                global_state,
                &unsafe { Context::from_raw(context) },
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
    context: ffi::duckdb_v2_context_handle,
    err: *mut ffi::duckdb_v2_error_info_handle,
) {
    handle_unwind(
        || {
            let user_data = get_user_data!(ffi::duckdb_v2_table_function_progress_get_user_data, info);
            let bind_data = get_bind_data!(ffi::duckdb_v2_table_function_progress_get_bind_data, info)
                .ok_or_else(|| Error::api_error("Table function bind data not set"))?;
            let global_state = get_global_state!(ffi::duckdb_v2_table_function_progress_get_global_state, info);

            if let Some(progress) = T::progress(user_data, bind_data, global_state, &unsafe {
                Context::from_raw(context)
            })? {
                check_api_call!(ffi::duckdb_v2_table_function_progress_set_progress, info, progress)?
            }

            Ok(())
        },
        err,
    );
}

unsafe extern "C" fn filter_pushdown_callback<T: TableFilterPushdownCallbacks>(
    info: ffi::duckdb_v2_table_function_filter_pushdown_info_handle,
    context: ffi::duckdb_v2_context_handle,
    err: *mut ffi::duckdb_v2_error_info_handle,
) {
    handle_unwind(
        || {
            let user_data = get_user_data!(ffi::duckdb_v2_table_function_filter_pushdown_get_user_data, info);
            let bind_data = get_bind_data!(ffi::duckdb_v2_table_function_filter_pushdown_get_bind_data, info)
                .ok_or_else(|| Error::api_error("Table function bind data not set"))?;

            T::pushdown_filter(
                user_data,
                bind_data,
                &unsafe { Context::from_raw(context) },
                PushdownData { handle: &info },
            )
        },
        err,
    );
}

unsafe extern "C" fn get_bind_info_callback<T: TableGetBindInfoCallbacks>(
    info: ffi::duckdb_v2_table_function_get_bind_info_info_handle,
    context: ffi::duckdb_v2_context_handle,
    err: *mut ffi::duckdb_v2_error_info_handle,
) {
    handle_unwind(
        || {
            let user_data = get_user_data!(ffi::duckdb_v2_table_function_get_bind_info_get_user_data, info);
            let bind_data = get_bind_data!(ffi::duckdb_v2_table_function_get_bind_info_get_bind_data, info)
                .ok_or_else(|| Error::api_error("Table function bind data not set"))?;

            T::bind_info(
                user_data,
                bind_data,
                &unsafe { Context::from_raw(context) },
                BindInfoApi { handle: &info },
            )
        },
        err,
    );
}

unsafe extern "C" fn claim_batch_callback<T: TableClaimBatchCallbacks>(
    info: ffi::duckdb_v2_table_function_claim_batch_info_handle,
    context: ffi::duckdb_v2_context_handle,
    err: *mut ffi::duckdb_v2_error_info_handle,
) {
    handle_unwind(
        || {
            let user_data = get_user_data!(ffi::duckdb_v2_table_function_claim_batch_get_user_data, info);
            let bind_data = get_bind_data!(ffi::duckdb_v2_table_function_claim_batch_get_bind_data, info)
                .ok_or_else(|| Error::api_error("Table function bind data not set"))?;
            let local_state = get_local_state!(ffi::duckdb_v2_table_function_claim_batch_get_local_state, info);
            let global_state = get_global_state!(ffi::duckdb_v2_table_function_claim_batch_get_global_state, info);

            let result = T::claim_batch(user_data, bind_data, global_state, local_state, &unsafe {
                Context::from_raw(context)
            })?;

            check_api_call!(ffi::duckdb_v2_table_function_claim_batch_set_claimed, info, result)
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
#[derive(Debug)]
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
    /// for every index below [`count`](Self::count).
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
    pub fn count(&self) -> Result<usize> {
        check_api_call!(
            ffi::duckdb_v2_table_function_partition_data_get_partition_column_count,
            *self.handle,
            RET
        )
        .map(|column_count| column_count as usize)
    }

    /// Map a requested partitioning column to its bind-declared column index.
    pub fn get_column_index(&self, partition_index: usize) -> Result<usize> {
        check_api_call!(
            ffi::duckdb_v2_table_function_partition_data_get_partition_column_index,
            *self.handle,
            partition_index as u64,
            RET
        )
        .map(|original_index| original_index as usize)
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
#[derive(Debug)]
pub struct PartitioningData<'a> {
    handle: &'a ffi::duckdb_v2_table_function_partitioning_info_handle,
}

impl<'a> PartitioningData<'a> {
    /// Return the number of columns in the candidate column set.
    pub fn count(&self) -> Result<usize> {
        check_api_call!(
            ffi::duckdb_v2_table_function_partitioning_get_partition_column_count,
            *self.handle,
            RET
        )
        .map(|column_count| column_count as usize)
    }

    /// Map a candidate column to its bind-declared column index.
    pub fn get_column_index(&self, partition_index: usize) -> Result<usize> {
        check_api_call!(
            ffi::duckdb_v2_table_function_partitioning_get_partition_column_index,
            *self.handle,
            partition_index as u64,
            RET
        )
        .map(|original_index| original_index as usize)
    }
}

unsafe extern "C" fn partition_data_callback<T: TablePartitioningCallbacks>(
    info: ffi::duckdb_v2_table_function_partition_data_info_handle,
    context: ffi::duckdb_v2_context_handle,
    err: *mut ffi::duckdb_v2_error_info_handle,
) {
    handle_unwind(
        || {
            let user_data = get_user_data!(ffi::duckdb_v2_table_function_partition_data_get_user_data, info);
            let bind_data = get_bind_data!(ffi::duckdb_v2_table_function_partition_data_get_bind_data, info)
                .ok_or_else(|| Error::api_error("Table function bind data not set"))?;
            let global_state = get_global_state!(ffi::duckdb_v2_table_function_partition_data_get_global_state, info);
            let local_state = get_local_state!(ffi::duckdb_v2_table_function_partition_data_get_local_state, info);

            let batch_index = T::partition_data(
                user_data,
                bind_data,
                global_state,
                local_state,
                &unsafe { Context::from_raw(context) },
                PartitionData { handle: &info },
            )?;

            // DuckDB validates the batch index on every call, whether or not a
            // downstream operator ends up using it.
            check_api_call!(
                ffi::duckdb_v2_table_function_partition_data_set_batch_index,
                info,
                batch_index as u64
            )
        },
        err,
    );
}

unsafe extern "C" fn partitioning_callback<T: TablePartitioningCallbacks>(
    info: ffi::duckdb_v2_table_function_partitioning_info_handle,
    context: ffi::duckdb_v2_context_handle,
    err: *mut ffi::duckdb_v2_error_info_handle,
) {
    handle_unwind(
        || {
            let user_data = get_user_data!(ffi::duckdb_v2_table_function_partitioning_get_user_data, info);
            let bind_data = get_bind_data!(ffi::duckdb_v2_table_function_partitioning_get_bind_data, info)
                .ok_or_else(|| Error::api_error("Table function bind data not set"))?;

            let partition_info = T::partitioning(
                user_data,
                bind_data,
                &unsafe { Context::from_raw(context) },
                PartitioningData { handle: &info },
            )?;

            check_api_call!(
                ffi::duckdb_v2_table_function_partitioning_set_partition_info,
                info,
                partition_info.into()
            )
        },
        err,
    );
}

/// Builds and registers a user-defined table function.
#[derive(Debug)]
pub struct TableFunctionBuilder<T: TableFunctionCallbacks> {
    name: String,
    signature: SignatureBuilder,
    user_data: OpaqueHandle<T>,
    projection_pushdown: bool,
    progress_callback: ffi::duckdb_v2_table_function_progress_callback_fn,
    filter_pushdown_callback: ffi::duckdb_v2_table_function_filter_pushdown_callback_fn,
    partition_data_callback: ffi::duckdb_v2_table_function_partition_data_callback_fn,
    partitioning_callback: ffi::duckdb_v2_table_function_partitioning_callback_fn,
    claim_batch_callback: ffi::duckdb_v2_table_function_claim_batch_callback_fn,
    get_bind_info_callback: ffi::duckdb_v2_table_function_get_bind_info_callback_fn,
}

impl<T: TableFilterPushdownCallbacks> TableFunctionBuilder<T> {
    /// Claim filters in the scan, enabling [`TableFilterPushdownCallbacks`].
    ///
    /// Without this DuckDB never offers the function its filters and applies
    /// all of them itself.
    pub fn with_filter_pushdown(mut self) -> Self {
        self.filter_pushdown_callback = Some(filter_pushdown_callback::<T>);
        self
    }
}

impl<T: TableGetBindInfoCallbacks> TableFunctionBuilder<T> {
    /// Enable column identifiers and reader metadata through [`TableGetBindInfoCallbacks`].
    /// Without this callback, multi-file readers match columns by name only.
    pub fn with_get_bind_info(mut self) -> Self {
        self.get_bind_info_callback = Some(get_bind_info_callback::<T>);
        self
    }
}

impl<T: TableClaimBatchCallbacks> TableFunctionBuilder<T> {
    /// Enable batch-based scanning through [`TableClaimBatchCallbacks`].
    pub fn with_claim_batch(mut self) -> Self {
        self.claim_batch_callback = Some(claim_batch_callback::<T>);
        self
    }
}

impl<T: TablePartitioningCallbacks> TableFunctionBuilder<T> {
    /// Register both callbacks; DuckDB requires partition data when partitioning is set.
    /// Batch indices preserve insertion order; single-value partitions can optimize aggregation.
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
            progress_callback: None,
            filter_pushdown_callback: None,
            partition_data_callback: None,
            partitioning_callback: None,
            claim_batch_callback: None,
            get_bind_info_callback: None,
        }
    }

    /// Enable or disable projection pushdown.
    ///
    /// When enabled, [`InitColumnData`] reports only requested columns and maps
    /// them back to the bind-declared schema. Execution output contains those
    /// projected columns in the reported order.
    pub fn with_projection_pushdown(mut self, projection_pushdown: bool) -> Self {
        self.projection_pushdown = projection_pushdown;
        self
    }

    /// Report scan progress through [`TableFunctionCallbacks::progress`].
    ///
    /// Without this DuckDB treats the function's progress as unknown.
    pub fn with_progress(mut self) -> Self {
        self.progress_callback = Some(progress_callback::<T>);
        self
    }

    fn build(&mut self, handle: &TableFunctionBuilderHandle, link: &dyn FFILink) -> Result<()> {
        check_api_call!(
            ffi::duckdb_v2_table_function_set_name,
            handle.raw(),
            &mut (&self.name).into()
        )?;

        let signature = check_api_call!(ffi::duckdb_v2_table_function_get_signature, handle.raw(), RET)?;

        let docs = check_api_call!(ffi::duckdb_v2_table_function_get_docs, handle.raw(), RET)?;

        std::mem::take(&mut self.signature).build(link, &signature, &docs)?;

        check_api_call!(
            ffi::duckdb_v2_table_function_set_projection_pushdown,
            handle.raw(),
            self.projection_pushdown
        )?;

        check_api_call!(
            ffi::duckdb_v2_table_function_set_user_data,
            handle.raw(),
            &mut self.user_data.to_handle()
        )?;

        check_api_call!(
            ffi::duckdb_v2_table_function_set_init_local_callback,
            handle.raw(),
            Some(init_local_callback::<T>)
        )?;

        check_api_call!(
            ffi::duckdb_v2_table_function_set_init_global_callback,
            handle.raw(),
            Some(init_global_callback::<T>)
        )?;

        // Only registered through `with_progress`: once a callback is set,
        // DuckDB reports 0% instead of unknown progress when it sets no value.
        if self.progress_callback.is_some() {
            check_api_call!(
                ffi::duckdb_v2_table_function_set_progress_callback,
                handle.raw(),
                self.progress_callback
            )?;
        }

        // Only registered through `with_filter_pushdown`, so that DuckDB does
        // not ask a function that cannot claim any filter.
        if self.filter_pushdown_callback.is_some() {
            check_api_call!(
                ffi::duckdb_v2_table_function_set_filter_pushdown_callback,
                handle.raw(),
                self.filter_pushdown_callback
            )?;
        }

        // Partitioning requires partition data; register both only when requested.
        // The partition data callback also controls batch ordering.
        if self.partition_data_callback.is_some() {
            check_api_call!(
                ffi::duckdb_v2_table_function_set_partition_data_callback,
                handle.raw(),
                self.partition_data_callback
            )?;

            check_api_call!(
                ffi::duckdb_v2_table_function_set_partitioning_callback,
                handle.raw(),
                self.partitioning_callback
            )?;
        }

        if self.claim_batch_callback.is_some() {
            check_api_call!(
                ffi::duckdb_v2_table_function_set_claim_batch_callback,
                handle.raw(),
                self.claim_batch_callback
            )?;
        }

        if self.get_bind_info_callback.is_some() {
            check_api_call!(
                ffi::duckdb_v2_table_function_set_get_bind_info_callback,
                handle.raw(),
                self.get_bind_info_callback
            )?;
        }

        check_api_call!(
            ffi::duckdb_v2_table_function_set_bind_callback,
            handle.raw(),
            Some(bind_callback::<T>)
        )?;

        check_api_call!(
            ffi::duckdb_v2_table_function_set_exec_callback,
            handle.raw(),
            Some(exec_callback::<T>)
        )
    }

    /// Register through a connection, consuming the builder.
    #[allow(private_bounds)]
    pub fn register<C: TableFunctionBuilderLink + FFILink>(mut self, link: &C) -> Result<()> {
        let handle = link.create_table_function_handle()?;
        self.build(&handle, link)?;

        link.register_table_function_handle(handle)
    }
}

/// Binds the schema, initializes shared and worker-local state, then produces chunks.
/// Progress is optional; filter pushdown and partitioning use separate callback traits.
pub trait TableFunctionCallbacks: Send + Sync + 'static {
    /// Data shared from binding through optimization and execution.
    type BindData: Any + Send + Sync;
    /// Mutable state local to one execution worker.
    type LocalState: Any + Send;
    /// State shared by all workers scanning one function instance.
    type GlobalState: Any + Send + Sync;

    /// Produce one output chunk; call `set_size(0)` on vector 0 or leave it empty when exhausted.
    fn exec(
        &self,
        bind_data: &Self::BindData,
        global_state: Option<&Self::GlobalState>,
        local_state: Option<&mut Self::LocalState>,
        context: &Context,
        output: DataChunkRef<'_>,
        column_info: ExecColumnInfo<'_>,
    ) -> Result<()>;

    /// **Bind:** validate arguments, declare columns, and create shared data.
    fn bind(
        &self,
        context: &Context,
        arguments: Vec<BindArgument>,
        bind_handle: BindFunctionHandle<'_>,
    ) -> Result<Self::BindData>;

    /// **Progress:** report execution progress from `0.0` to `1.0`.
    ///
    /// Only called when the function is registered with
    /// [`TableFunctionBuilder::with_progress`]. Returning `None` reports `0.0`.
    fn progress(
        &self,
        _bind_data: &Self::BindData,
        _global_state: Option<&Self::GlobalState>,
        _context: &Context,
    ) -> Result<Option<f64>> {
        Ok(None)
    }

    /// **Initialize local:** create state for one execution worker.
    fn init_local_state(
        &self,
        _bind_data: &Self::BindData,
        _global_state: Option<&Self::GlobalState>,
        _context: &Context,
        _column_data: InitColumnData<'_>,
    ) -> Result<Option<Self::LocalState>> {
        Ok(None)
    }

    /// **Initialize global:** create shared scan state and an optional thread limit.
    fn init_global_state(
        &self,
        _bind_data: &Self::BindData,
        _context: &Context,
        _column_data: InitColumnData<'_>,
    ) -> Result<(Option<Self::GlobalState>, Option<usize>)> {
        Ok((None, None))
    }
}

/// Applies query filters inside a table function.
///
/// Implement this for table functions that can evaluate some of the query's
/// filters while scanning, for instance by skipping files, row groups, or rows
/// that cannot match. Enable the callback with
/// [`TableFunctionBuilder::with_filter_pushdown`]; it is ignored, and never
/// registered, without it.
pub trait TableFilterPushdownCallbacks: TableFunctionCallbacks {
    /// Claim filters with [`PushdownData::accept_pushdown`]; every emitted row must satisfy them.
    /// Recording accepted filters in shared `BindData` requires interior mutability.
    fn pushdown_filter(
        &self,
        bind_data: &Self::BindData,
        context: &Context,
        pushdown_data: PushdownData<'_>,
    ) -> Result<()>;
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
        bind_data: &Self::BindData,
        global_state: Option<&Self::GlobalState>,
        local_state: Option<&mut Self::LocalState>,
        context: &Context,
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
        _bind_data: &Self::BindData,
        _context: &Context,
        _partitioning_data: PartitioningData<'_>,
    ) -> Result<TablePartitionInfo> {
        Ok(TablePartitionInfo::NotPartitioned)
    }
}

/// Callback-scoped access to column identifiers and reader metadata.
#[derive(Debug)]
pub struct BindInfoApi<'a> {
    handle: &'a ffi::duckdb_v2_table_function_get_bind_info_info_handle,
}

impl<'a> BindInfoApi<'a> {
    /// Assign an `INTEGER` field ID or `VARCHAR` name to a result column or nested field.
    ///
    /// `child_indexes` identifies the nested field: a `STRUCT` field index or `0`
    /// for `LIST` elements at each level. An empty path identifies the column itself.
    pub fn set_column_identifier(&self, col_index: usize, child_indexes: &[u64], identifier: &Value) -> Result<()> {
        check_api_call!(
            ffi::duckdb_v2_table_function_get_bind_info_set_column_identifier,
            *self.handle,
            col_index as u64,
            child_indexes.as_ptr(),
            child_indexes.len() as u64,
            identifier.raw()
        )
    }

    /// Set a reader metadata option, replacing any existing value for the key.
    /// Multi-file functions expose these options as per-file reader metadata.
    pub fn set_option(&self, key: impl AsRef<str>, value: &Value) -> Result<()> {
        let str = key.as_ref();
        check_api_call!(
            ffi::duckdb_v2_table_function_get_bind_info_set_option,
            *self.handle,
            &mut str.into(),
            value.raw()
        )
    }
}

/// Reports column identifiers and reader metadata for a bound table function.
///
/// Implement this for single-file readers wrapped by a
/// [`MultiFileFunctionBuilder`](crate::multi_file::MultiFileFunctionBuilder).
/// The wrapper requests this information for each file it binds, matches columns
/// by the reported identifiers (so field IDs can identify renamed columns), and
/// exposes the options as per-file reader metadata. The callback is only registered
/// when [`TableFunctionBuilder::with_get_bind_info`] is called.
pub trait TableGetBindInfoCallbacks: TableFunctionCallbacks {
    /// **Bind info:** report column identifiers and reader metadata for `bind_data` through `api`.
    /// May run multiple times per bind; report only information established by [`bind`](TableFunctionCallbacks::bind).
    fn bind_info(&self, bind_data: &Self::BindData, context: &Context, api: BindInfoApi<'_>) -> Result<()>;
}

/// Distributes scan work across threads in batches.
///
/// Each scanning thread claims a batch into its local state before its first
/// [`exec`](TableFunctionCallbacks::exec) call and whenever `exec` returns an empty
/// output chunk. Each `exec` call produces rows only from the current batch.
/// Returning `false` from `claim_batch` ends the scan for that thread.
///
/// Batch boundaries let multi-file functions preserve per-file row order across
/// threads. The callback is only registered when
/// [`TableFunctionBuilder::with_claim_batch`] is called.
pub trait TableClaimBatchCallbacks: TableFunctionCallbacks {
    /// **Claim batch:** reserve the next batch in `local_state`, returning `false` if none remain.
    /// Other threads may block on this callback; reserve the work here and read it in `exec`.
    fn claim_batch(
        &self,
        bind_data: &Self::BindData,
        global_state: Option<&Self::GlobalState>,
        local_state: Option<&mut Self::LocalState>,
        context: &Context,
    ) -> Result<bool>;
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests;

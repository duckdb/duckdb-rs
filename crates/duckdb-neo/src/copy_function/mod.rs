//! User-defined `COPY TO` and `COPY FROM` formats.
//!
//! Implement [`CopyToFunctionCallbacks`] to inspect input columns, initialize the
//! destination, prepare each [`ColumnDataCollection`] as batch data, flush
//! prepared batches, and finalize the output. Implement
//! [`CopyFromFunctionCallbacks`] to bind the file path and target columns,
//! optionally set up shared and worker-local read state, and produce output
//! chunks. A type may implement either trait, or both to support the format's
//! name on both sides of `COPY`. Register with [`CopyFunctionBuilder`].

use std::{any::Any, collections::HashMap};

use crate::{
    Result,
    builder_helpers::{
        OpaqueHandle, get_bind_data, get_global_state, get_init_data, get_local_state, get_opaque_data_ref,
        get_user_data, handle_unwind, into_opaque,
    },
    check_api_call,
    column_data_collection::ColumnDataCollection,
    connection::Context,
    data_chunk::DataChunkRef,
    ffi,
    handles::{CopyFunctionBuilderHandle, CopyFunctionBuilderLink},
    logical_type::LogicalType,
    value::Value,
};

struct CopyFunctionBindData<T> {
    data: T,
    logical_types: Vec<LogicalType>,
}

unsafe extern "C" fn bind_to_callback<T: CopyToFunctionCallbacks>(
    info: ffi::duckdb_v2_copy_to_bind_info_handle,
    context: ffi::duckdb_v2_context_handle,
    err: *mut ffi::duckdb_v2_error_info_handle,
) {
    handle_unwind(
        || {
            let bind_info = CopyToBindInfo { handle: info };
            let user_data = get_user_data!(ffi::duckdb_v2_copy_to_bind_get_user_data, info);
            let logical_types = bind_info.logical_types()?;

            let bind_data = T::bind(user_data, &Context(context), &bind_info)?;

            check_api_call!(
                ffi::duckdb_v2_copy_to_bind_set_bind_data,
                info,
                &mut into_opaque(CopyFunctionBindData {
                    data: bind_data,
                    logical_types,
                })
            )?;

            Ok(())
        },
        err,
    );
}

unsafe extern "C" fn init_to_callback<T: CopyToFunctionCallbacks>(
    info: ffi::duckdb_v2_copy_to_init_info_handle,
    context: ffi::duckdb_v2_context_handle,
    err: *mut ffi::duckdb_v2_error_info_handle,
) {
    handle_unwind(
        || {
            let user_data = get_user_data!(ffi::duckdb_v2_copy_to_init_get_user_data, info);

            let bind_data = check_api_call!(ffi::duckdb_v2_copy_to_init_get_bind_data, info, RET)?;
            let bind_data = unsafe { get_opaque_data_ref::<CopyFunctionBindData<T::BindData>>(bind_data) }.unwrap();

            let file_path = check_api_call!(ffi::duckdb_v2_copy_to_init_get_file_path, info, RET).map(|x| x.into())?;

            let init_data = T::init(user_data, &Context(context), &bind_data.data, file_path)?;

            check_api_call!(
                ffi::duckdb_v2_copy_to_init_set_init_data,
                info,
                &mut into_opaque(init_data)
            )?;

            Ok(())
        },
        err,
    );
}

unsafe extern "C" fn batch_to_callback<T: CopyToFunctionCallbacks>(
    info: ffi::duckdb_v2_copy_to_batch_info_handle,
    context: ffi::duckdb_v2_context_handle,
    err: *mut ffi::duckdb_v2_error_info_handle,
) {
    handle_unwind(
        || {
            let input = check_api_call!(ffi::duckdb_v2_copy_to_batch_take_input, info, RET)?;

            let user_data = get_user_data!(ffi::duckdb_v2_copy_to_batch_get_user_data, info);
            let bind_data = check_api_call!(ffi::duckdb_v2_copy_to_batch_get_bind_data, info, RET)?;
            let bind_data = unsafe { get_opaque_data_ref::<CopyFunctionBindData<T::BindData>>(bind_data) }.unwrap();
            let init_data = get_init_data!(ffi::duckdb_v2_copy_to_batch_get_init_data, info).unwrap();

            let collection = ColumnDataCollection {
                handle: input,
                logical_types: bind_data.logical_types.clone(),
                database: None,
            };

            let batch_data = T::batch(user_data, &Context(context), &bind_data.data, init_data, collection)?;

            check_api_call!(
                ffi::duckdb_v2_copy_to_batch_set_batch_data,
                info,
                &mut into_opaque(batch_data)
            )?;

            Ok(())
        },
        err,
    );
}

unsafe extern "C" fn batch_size_callback<T: CopyToFunctionCallbacks>(
    info: ffi::duckdb_v2_copy_to_batch_size_info_handle,
    context: ffi::duckdb_v2_context_handle,
    err: *mut ffi::duckdb_v2_error_info_handle,
) {
    handle_unwind(
        || {
            let user_data = get_user_data!(ffi::duckdb_v2_copy_to_batch_size_get_user_data, info);
            let bind_data = check_api_call!(ffi::duckdb_v2_copy_to_batch_size_get_bind_data, info, RET)?;
            let bind_data = unsafe { get_opaque_data_ref::<CopyFunctionBindData<T::BindData>>(bind_data) }.unwrap();

            let target = T::batch_size(user_data, &Context(context), &bind_data.data);

            if let Some(target) = target {
                check_api_call!(ffi::duckdb_v2_copy_to_batch_size_set_target, info, target as u64)
            } else {
                Ok(())
            }
        },
        err,
    );
}

unsafe extern "C" fn flush_to_callback<T: CopyToFunctionCallbacks>(
    info: ffi::duckdb_v2_copy_to_flush_info_handle,
    context: ffi::duckdb_v2_context_handle,
    err: *mut ffi::duckdb_v2_error_info_handle,
) {
    handle_unwind(
        || {
            let user_data = get_user_data!(ffi::duckdb_v2_copy_to_flush_get_user_data, info);

            let bind_data = check_api_call!(ffi::duckdb_v2_copy_to_flush_get_bind_data, info, RET)?;
            let bind_data = unsafe { get_opaque_data_ref::<CopyFunctionBindData<T::BindData>>(bind_data) }.unwrap();

            let init_data = get_init_data!(ffi::duckdb_v2_copy_to_flush_get_init_data, info).unwrap();

            let batch_data = check_api_call!(ffi::duckdb_v2_copy_to_flush_get_batch_data, info, RET)?;

            let batch_data = unsafe { get_opaque_data_ref(batch_data) }.unwrap();

            T::flush(user_data, &Context(context), &bind_data.data, init_data, batch_data)?;

            Ok(())
        },
        err,
    );
}

unsafe extern "C" fn finalize_to_callback<T: CopyToFunctionCallbacks>(
    info: ffi::duckdb_v2_copy_to_finalize_info_handle,
    context: ffi::duckdb_v2_context_handle,
    err: *mut ffi::duckdb_v2_error_info_handle,
) {
    handle_unwind(
        || {
            let user_data = get_user_data!(ffi::duckdb_v2_copy_to_finalize_get_user_data, info);

            let bind_data = check_api_call!(ffi::duckdb_v2_copy_to_finalize_get_bind_data, info, RET)?;
            let bind_data = unsafe { get_opaque_data_ref::<CopyFunctionBindData<T::BindData>>(bind_data) }.unwrap();

            let init_data = get_init_data!(ffi::duckdb_v2_copy_to_finalize_get_init_data, info).unwrap();

            T::finalize(user_data, &Context(context), &bind_data.data, init_data)?;

            Ok(())
        },
        err,
    );
}

/// Builds and registers a user-defined `COPY TO` format.
pub struct CopyFunctionBuilder<T> {
    user_data: OpaqueHandle<T>,
    name: String,
    progress: bool,
}

impl<T> CopyFunctionBuilder<T> {
    /// Create a copy-function builder with its SQL format name.
    pub fn new(name: impl Into<String>, user_data: T) -> Self {
        Self {
            name: name.into(),
            user_data: OpaqueHandle::new(user_data),
            progress: false,
        }
    }
}

impl<T> CopyFunctionBuilder<T> {
    fn build_common(&self, handle: &CopyFunctionBuilderHandle) -> Result<()> {
        check_api_call!(
            ffi::duckdb_v2_copy_function_set_name,
            **handle,
            &mut (&self.name).into()
        )?;

        check_api_call!(
            ffi::duckdb_v2_copy_function_set_user_data,
            **handle,
            &mut self.user_data.to_handle()
        )?;

        Ok(())
    }
}

impl<T: CopyToFunctionCallbacks> CopyFunctionBuilder<T> {
    fn build(&self, handle: &CopyFunctionBuilderHandle) -> Result<()> {
        self.build_common(handle)?;

        check_api_call!(
            ffi::duckdb_v2_copy_to_set_bind_callback,
            **handle,
            Some(bind_to_callback::<T>)
        )?;

        check_api_call!(
            ffi::duckdb_v2_copy_to_set_init_callback,
            **handle,
            Some(init_to_callback::<T>)
        )?;

        check_api_call!(
            ffi::duckdb_v2_copy_to_set_batch_callback,
            **handle,
            Some(batch_to_callback::<T>)
        )?;

        check_api_call!(
            ffi::duckdb_v2_copy_to_set_flush_callback,
            **handle,
            Some(flush_to_callback::<T>)
        )?;

        check_api_call!(
            ffi::duckdb_v2_copy_to_set_batch_size_callback,
            **handle,
            Some(batch_size_callback::<T>)
        )?;

        check_api_call!(
            ffi::duckdb_v2_copy_to_set_finalize_callback,
            **handle,
            Some(finalize_to_callback::<T>)
        )?;

        Ok(())
    }

    /// Register the `COPY TO` side through a connection or extension, consuming the builder.
    #[allow(private_bounds)]
    pub fn register<C: CopyFunctionBuilderLink>(self, link: &C) -> Result<()> {
        let handle = link.create_copy_function_handle()?;
        self.build(&handle)?;

        check_api_call!(ffi::duckdb_v2_copy_function_register, *handle)
    }
}

impl<T: CopyFromFunctionCallbacks> CopyFunctionBuilder<T> {
    /// Report `COPY FROM` progress through [`CopyFromFunctionCallbacks::progress`].
    ///
    /// Without this DuckDB treats the read's progress as unknown.
    pub fn with_progress(mut self) -> Self {
        self.progress = true;
        self
    }

    fn set_from_callbacks(&self, handle: &CopyFunctionBuilderHandle) -> Result<()> {
        check_api_call!(
            ffi::duckdb_v2_copy_from_set_bind_callback,
            **handle,
            Some(bind_from_callback::<T>)
        )?;

        check_api_call!(
            ffi::duckdb_v2_copy_from_set_init_global_callback,
            **handle,
            Some(init_global_from_callback::<T>)
        )?;

        check_api_call!(
            ffi::duckdb_v2_copy_from_set_init_local_callback,
            **handle,
            Some(init_local_from_callback::<T>)
        )?;

        check_api_call!(
            ffi::duckdb_v2_copy_from_set_exec_callback,
            **handle,
            Some(exec_from_callback::<T>)
        )?;

        // Only registered through `with_progress`: once a callback is set,
        // DuckDB reports 0% instead of unknown progress when it sets no value.
        if self.progress {
            check_api_call!(
                ffi::duckdb_v2_copy_from_set_progress_callback,
                **handle,
                Some(progress_from_callback::<T>)
            )?;
        }

        Ok(())
    }

    fn build_from(&self, handle: &CopyFunctionBuilderHandle) -> Result<()> {
        self.build_common(handle)?;
        self.set_from_callbacks(handle)
    }

    /// Register the `COPY FROM` side through a connection or extension, consuming the builder.
    #[allow(private_bounds)]
    pub fn register_from<C: CopyFunctionBuilderLink>(self, link: &C) -> Result<()> {
        let handle = link.create_copy_function_handle()?;
        self.build_from(&handle)?;

        check_api_call!(ffi::duckdb_v2_copy_function_register, *handle)
    }
}

impl<T: CopyToFunctionCallbacks + CopyFromFunctionCallbacks> CopyFunctionBuilder<T> {
    /// Register both `COPY TO` and `COPY FROM` through a connection or extension, consuming the builder.
    #[allow(private_bounds)]
    pub fn register_to_and_from<C: CopyFunctionBuilderLink>(self, link: &C) -> Result<()> {
        let handle = link.create_copy_function_handle()?;
        self.build(&handle)?;
        self.set_from_callbacks(&handle)?;

        check_api_call!(ffi::duckdb_v2_copy_function_register, *handle)
    }
}

/// Input columns, output path, and options available while binding `COPY TO`.
///
/// Column order matches the input relation being copied. Returned logical
/// types are owned copies.
pub struct CopyToBindInfo {
    handle: ffi::duckdb_v2_copy_to_bind_info_handle,
}

impl CopyToBindInfo {
    fn logical_types(&self) -> Result<Vec<LogicalType>> {
        (0..self.len()?)
            .map(|index| self.get_column(index).map(|(_, logical_type)| logical_type))
            .collect()
    }

    /// Return the number of input columns.
    pub fn len(&self) -> Result<usize> {
        let res = check_api_call!(ffi::duckdb_v2_copy_to_bind_get_column_count, self.handle, RET)?;

        Ok(res as usize)
    }

    /// Return whether there are no input columns.
    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.len()? == 0)
    }

    /// Return a column's borrowed name and owned logical type.
    ///
    /// An out-of-range index returns an error.
    pub fn get_column(&self, index: usize) -> Result<(&str, LogicalType)> {
        let mut name = ffi::duckdb_v2_str::default();

        // TODO: Update lifetime
        check_api_call!(
            ffi::duckdb_v2_copy_to_bind_get_column_name,
            self.handle,
            index as u64,
            &mut name
        )?;

        let borrowed_type = check_api_call!(
            ffi::duckdb_v2_copy_to_bind_get_column_type,
            self.handle,
            index as u64,
            RET
        )?;

        let logical_type = LogicalType { handle: borrowed_type };

        Ok((name.into(), logical_type))
    }

    /// Return the output path as written in the `COPY TO` statement.
    ///
    /// The initialization callback receives the actual file path, which may
    /// differ for temporary files or partitioned output.
    pub fn file_path(&self) -> Result<String> {
        check_api_call!(ffi::duckdb_v2_copy_to_bind_get_file_path, self.handle, RET).map(|x| x.into())
    }

    fn option_count(&self) -> Result<usize> {
        check_api_call!(ffi::duckdb_v2_copy_to_bind_get_option_count, self.handle, RET).map(|x| x as usize)
    }

    /// Return owned names and values of the format-specific `COPY TO` options.
    ///
    /// Engine-handled options such as `USE_TMP_FILE` and `BATCH_SIZE` are excluded.
    /// Bare options yield `true`; parenthesized lists yield unnamed struct values.
    pub fn options(&self) -> Result<HashMap<String, Value>> {
        let len = self.option_count()?;
        let mut items: HashMap<String, Value> = HashMap::with_capacity(len);

        for i in 0..len {
            let name = check_api_call!(ffi::duckdb_v2_copy_to_bind_get_option_name, self.handle, i as u64, RET)?;
            let handle = check_api_call!(ffi::duckdb_v2_copy_to_bind_get_option_value, self.handle, i as u64, RET)?;

            items.insert(name.into(), Value { handle });
        }
        Ok(items)
    }
}

unsafe extern "C" fn bind_from_callback<T: CopyFromFunctionCallbacks>(
    info: ffi::duckdb_v2_copy_from_bind_info_handle,
    context: ffi::duckdb_v2_context_handle,
    err: *mut ffi::duckdb_v2_error_info_handle,
) {
    handle_unwind(
        || {
            let user_data = get_user_data!(ffi::duckdb_v2_copy_from_bind_get_user_data, info);

            let mut file_path = ffi::duckdb_v2_str::default();
            check_api_call!(ffi::duckdb_v2_copy_from_bind_get_file_path, info, &mut file_path)?;

            let bind_info = CopyFromBindInfo { handle: info };

            let (bind_data, cardinality) = T::bind(user_data, &Context(context), file_path.into(), &bind_info)?;

            check_api_call!(
                ffi::duckdb_v2_copy_from_bind_set_bind_data,
                info,
                &mut into_opaque(bind_data)
            )?;

            if let Some(cardinality) = cardinality {
                check_api_call!(
                    ffi::duckdb_v2_copy_from_bind_set_cardinality,
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

unsafe extern "C" fn init_global_from_callback<T: CopyFromFunctionCallbacks>(
    info: ffi::duckdb_v2_copy_from_init_global_info_handle,
    context: ffi::duckdb_v2_context_handle,
    err: *mut ffi::duckdb_v2_error_info_handle,
) {
    handle_unwind(
        || {
            let user_data = get_user_data!(ffi::duckdb_v2_copy_from_init_global_get_user_data, info);
            let bind_data = get_bind_data!(ffi::duckdb_v2_copy_from_init_global_get_bind_data, info);

            let (global_state, max_threads) = T::init_global_state(user_data, bind_data, &Context(context))?;

            if let Some(global_state) = global_state {
                check_api_call!(
                    ffi::duckdb_v2_copy_from_init_global_set_global_state,
                    info,
                    &mut into_opaque(global_state)
                )?;
            }

            if let Some(max_threads) = max_threads {
                check_api_call!(
                    ffi::duckdb_v2_copy_from_init_global_set_max_threads,
                    info,
                    max_threads as u64
                )?;
            }

            Ok(())
        },
        err,
    );
}

unsafe extern "C" fn init_local_from_callback<T: CopyFromFunctionCallbacks>(
    info: ffi::duckdb_v2_copy_from_init_local_info_handle,
    context: ffi::duckdb_v2_context_handle,
    err: *mut ffi::duckdb_v2_error_info_handle,
) {
    handle_unwind(
        || {
            let user_data = get_user_data!(ffi::duckdb_v2_copy_from_init_local_get_user_data, info);
            let bind_data = get_bind_data!(ffi::duckdb_v2_copy_from_init_local_get_bind_data, info);
            let global_state = get_global_state!(ffi::duckdb_v2_copy_from_init_local_get_global_state, info);

            let local_state = T::init_local_state(user_data, bind_data, &Context(context), global_state)?;

            if let Some(local_state) = local_state {
                check_api_call!(
                    ffi::duckdb_v2_copy_from_init_local_set_local_state,
                    info,
                    &mut into_opaque(local_state)
                )?;
            }

            Ok(())
        },
        err,
    );
}

unsafe extern "C" fn exec_from_callback<T: CopyFromFunctionCallbacks>(
    info: ffi::duckdb_v2_copy_from_exec_info_handle,
    context: ffi::duckdb_v2_context_handle,
    err: *mut ffi::duckdb_v2_error_info_handle,
) {
    handle_unwind(
        || {
            let user_data = get_user_data!(ffi::duckdb_v2_copy_from_exec_get_user_data, info);
            let bind_data = get_bind_data!(ffi::duckdb_v2_copy_from_exec_get_bind_data, info);
            let global_state = get_global_state!(ffi::duckdb_v2_copy_from_exec_get_global_state, info);
            let local_state = get_local_state!(ffi::duckdb_v2_copy_from_exec_get_local_state, info);

            let output_chunk = DataChunkRef::new(
                check_api_call!(ffi::duckdb_v2_copy_from_exec_get_output_chunk, info, RET)?,
                true,
            );

            T::exec(
                user_data,
                bind_data,
                global_state,
                local_state,
                &Context(context),
                output_chunk,
            )?;

            Ok(())
        },
        err,
    );
}

unsafe extern "C" fn progress_from_callback<T: CopyFromFunctionCallbacks>(
    info: ffi::duckdb_v2_copy_from_progress_info_handle,
    context: ffi::duckdb_v2_context_handle,
    err: *mut ffi::duckdb_v2_error_info_handle,
) {
    handle_unwind(
        || {
            let user_data = get_user_data!(ffi::duckdb_v2_copy_from_progress_get_user_data, info);
            let bind_data = get_bind_data!(ffi::duckdb_v2_copy_from_progress_get_bind_data, info);
            let global_state = get_global_state!(ffi::duckdb_v2_copy_from_progress_get_global_state, info);

            if let Some(progress) = T::progress(user_data, bind_data, global_state, &Context(context))? {
                check_api_call!(ffi::duckdb_v2_copy_from_progress_set_progress, info, progress)?;
            }

            Ok(())
        },
        err,
    );
}

/// The target columns, file path, and options supplied to a `COPY ... FROM`
/// format during binding.
///
/// Column order and types are fixed by the target table: the exec callback's
/// output chunk must carry them, in this order.
pub struct CopyFromBindInfo {
    handle: ffi::duckdb_v2_copy_from_bind_info_handle,
}

impl CopyFromBindInfo {
    /// Return the number of columns the target table expects.
    pub fn column_count(&self) -> Result<usize> {
        let res = check_api_call!(ffi::duckdb_v2_copy_from_bind_get_column_count, self.handle, RET)?;

        Ok(res as usize)
    }

    /// Return a target column's borrowed name and owned logical type.
    ///
    /// An out-of-range index returns an error.
    pub fn get_column(&self, index: usize) -> Result<(&str, LogicalType)> {
        let mut name = ffi::duckdb_v2_str::default();

        check_api_call!(
            ffi::duckdb_v2_copy_from_bind_get_column_name,
            self.handle,
            index as u64,
            &mut name
        )?;

        let owned_type = check_api_call!(
            ffi::duckdb_v2_copy_from_bind_get_column_type,
            self.handle,
            index as u64,
            RET
        )?;

        Ok((name.into(), LogicalType { handle: owned_type }))
    }

    /// Return the number of options the `COPY` statement passed to the function.
    ///
    /// Every option other than `FORMAT` is included.
    pub fn option_count(&self) -> Result<usize> {
        let res = check_api_call!(ffi::duckdb_v2_copy_from_bind_get_option_count, self.handle, RET)?;

        Ok(res as usize)
    }

    /// Return an option's borrowed name and owned value.
    ///
    /// An out-of-range index returns an error.
    pub fn get_option(&self, index: usize) -> Result<(&str, Value)> {
        let mut name = ffi::duckdb_v2_str::default();

        check_api_call!(
            ffi::duckdb_v2_copy_from_bind_get_option_name,
            self.handle,
            index as u64,
            &mut name
        )?;

        let value = check_api_call!(
            ffi::duckdb_v2_copy_from_bind_get_option_value,
            self.handle,
            index as u64,
            RET
        )?;

        Ok((name.into(), Value { handle: value }))
    }
}

/// A `COPY ... FROM` read's estimated or exact row count.
pub struct CopyFromCardinality {
    /// The estimated number of rows the read will produce.
    pub cardinality: usize,
    /// Whether the row count is exact, which also makes it an upper bound.
    pub is_exact: bool,
}

/// Callback lifecycle for a user-defined `COPY FROM` format.
///
/// DuckDB binds the file path, target columns and options, optionally
/// initializes shared and worker-local read state, then repeatedly invokes
/// the exec callback to produce rows until it signals an empty batch.
///
/// A type may also implement [`CopyToFunctionCallbacks`] to support `COPY
/// ... TO` under the same format name; register both sides at once with
/// [`CopyFunctionBuilder::register_to_and_from`].
pub trait CopyFromFunctionCallbacks: Send + Sync + 'static {
    /// Data resolved while binding the file path, columns, and options.
    type BindData: Any + Send + Sync;
    /// Mutable state local to one execution worker.
    type LocalState: Any + Send + 'static;
    /// State shared by all workers reading one bound statement.
    type GlobalState: Any + Send + Sync;

    /// **Bind:** inspect the file path, target columns, and options; create shared bind data.
    fn bind(
        &self,
        context: &Context,
        file_path: &str,
        bind_info: &CopyFromBindInfo,
    ) -> Result<(Self::BindData, Option<CopyFromCardinality>)>;

    /// **Execute:** produce one batch of rows in the output chunk.
    ///
    /// Producing an empty batch signals the end of the read.
    fn exec(
        &self,
        bind_data: Option<&Self::BindData>,
        global_state: Option<&Self::GlobalState>,
        local_state: Option<&mut Self::LocalState>,
        context: &Context,
        output: DataChunkRef<'_>,
    ) -> Result<()>;

    /// **Initialize global:** create shared read state and an optional thread limit.
    fn init_global_state(
        &self,
        _bind_data: Option<&Self::BindData>,
        _context: &Context,
    ) -> Result<(Option<Self::GlobalState>, Option<usize>)> {
        Ok((None, None))
    }

    /// **Initialize local:** create state for one execution worker.
    fn init_local_state(
        &self,
        _bind_data: Option<&Self::BindData>,
        _context: &Context,
        _global_state: Option<&Self::GlobalState>,
    ) -> Result<Option<Self::LocalState>> {
        Ok(None)
    }

    /// **Progress:** report execution progress from `0.0` to `1.0`.
    ///
    /// Only called when the format is registered with
    /// [`CopyFunctionBuilder::with_progress`]. Returning `None` reports `0.0`.
    fn progress(
        &self,
        _bind_data: Option<&Self::BindData>,
        _global_state: Option<&Self::GlobalState>,
        _context: &Context,
    ) -> Result<Option<f64>> {
        Ok(None)
    }
}

/// Callback lifecycle for a user-defined `COPY TO` format.
///
/// DuckDB binds the input columns, initializes one output file, prepares input
/// batches, flushes each prepared batch, and finalizes the file.
///
/// A type may also implement [`CopyFromFunctionCallbacks`] to support `COPY
/// ... FROM` under the same format name; register both sides at once with
/// [`CopyFunctionBuilder::register_to_and_from`].
pub trait CopyToFunctionCallbacks: Send + Sync + 'static {
    /// State created once for the bound copy operation.
    type InitData: Any + Send + Sync;
    /// Data resolved while binding the input columns.
    type BindData: Any + Send + Sync;
    /// A prepared batch passed from [`Self::batch`] to [`Self::flush`].
    type BatchData: Any + Send + Sync;

    /// **Bind:** inspect the input columns and create shared bind data.
    fn bind(&self, context: &Context, bind_info: &CopyToBindInfo) -> Result<Self::BindData>;

    /// **Initialize:** open the output path and create file-level state.
    fn init(&self, context: &Context, bind_data: &Self::BindData, file_path: &str) -> Result<Self::InitData>;

    /// **Batch:** prepare one input collection for flushing.
    ///
    /// `input` must not outlive the connection running the copy, for example by
    /// being stored in a `static` or sent to code outside DuckDB.
    fn batch(
        &self,
        context: &Context,
        bind_data: &Self::BindData,
        init_data: &Self::InitData,
        input: ColumnDataCollection,
    ) -> Result<Self::BatchData>;

    /// **Batch size:** choose the target number of rows per batch during planning.
    ///
    /// Skipped when the statement supplies `BATCH_SIZE`. A supplied count must be positive.
    /// The final batch or a batch limited by `BATCH_SIZE_BYTES` may be smaller.
    #[allow(unused_variables)]
    fn batch_size(&self, context: &Context, bind_data: &Self::BindData) -> Option<usize> {
        None
    }

    /// **Flush:** write one prepared batch to the output.
    fn flush(
        &self,
        context: &Context,
        bind_data: &Self::BindData,
        init_data: &Self::InitData,
        batch_data: &Self::BatchData,
    ) -> Result<()>;

    /// **Finalize:** finish and close the output after all batches are flushed.
    fn finalize(&self, context: &Context, bind_data: &Self::BindData, init_data: &Self::InitData) -> Result<()>;
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests;

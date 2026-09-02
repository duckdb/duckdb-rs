//! User-defined `COPY TO` formats.
//!
//! Implement [`CopyFunctionCallbacks`] to inspect input columns, initialize the
//! destination, prepare each [`ColumnDataCollection`] as batch data, flush
//! prepared batches, and finalize the output. Register the format's SQL name
//! with [`CopyFunctionBuilder`].

use std::{any::Any, ops::Deref};

use crate::{
    Context, Result,
    builder_helpers::{
        OpaqueHandle, context_and_connection_fn, get_init_data, get_opaque_data_ref, get_user_data, handle_unwind,
        into_opaque,
    },
    check_api_call, check_api_call_no_err,
    column_data_collection::ColumnDataCollection,
    ffi,
    logical_type::LogicalType,
};

struct CopyFunctionBindData<T> {
    data: T,
    logical_types: Vec<LogicalType>,
}

unsafe extern "C" fn bind_callback<T: CopyFunctionCallbacks>(
    info: ffi::duckdb_v2_copy_function_bind_info_handle,
    context: ffi::duckdb_v2_context_handle,
    err: *mut ffi::duckdb_v2_error_info_handle,
) {
    handle_unwind(
        || {
            let column_info = ColumnInfo { handle: info };
            let logical_types = column_info.logical_types()?;

            let user_data = get_user_data!(ffi::duckdb_v2_copy_function_bind_get_user_data, info);

            let bind_data = T::bind(user_data, Context(context), column_info)?;

            check_api_call!(
                ffi::duckdb_v2_copy_function_bind_set_bind_data,
                info,
                into_opaque(CopyFunctionBindData {
                    data: bind_data,
                    logical_types,
                })
            )?;

            Ok(())
        },
        err,
    );
}

unsafe extern "C" fn init_callback<T: CopyFunctionCallbacks>(
    info: ffi::duckdb_v2_copy_function_init_info_handle,
    context: ffi::duckdb_v2_context_handle,
    err: *mut ffi::duckdb_v2_error_info_handle,
) {
    handle_unwind(
        || {
            let user_data = get_user_data!(ffi::duckdb_v2_copy_function_init_get_user_data, info);

            let bind_data = check_api_call!(ffi::duckdb_v2_copy_function_init_get_bind_data, info, RET)?;
            let bind_data = unsafe { get_opaque_data_ref::<CopyFunctionBindData<T::BindData>>(bind_data) }.unwrap();

            let mut file_path = ffi::duckdb_v2_str::default();

            check_api_call!(ffi::duckdb_v2_copy_function_init_get_file_path, info, &mut file_path)?;

            let init_data = T::init(user_data, Context(context), &bind_data.data, file_path.into())?;

            check_api_call!(
                ffi::duckdb_v2_copy_function_init_set_init_data,
                info,
                into_opaque(init_data)
            )?;

            Ok(())
        },
        err,
    );
}

unsafe extern "C" fn batch_callback<T: CopyFunctionCallbacks>(
    info: ffi::duckdb_v2_copy_function_batch_info_handle,
    context: ffi::duckdb_v2_context_handle,
    err: *mut ffi::duckdb_v2_error_info_handle,
) {
    handle_unwind(
        || {
            let input = check_api_call!(ffi::duckdb_v2_copy_function_batch_get_input, info, RET)?;

            let user_data = get_user_data!(ffi::duckdb_v2_copy_function_batch_get_user_data, info);

            let bind_data = check_api_call!(ffi::duckdb_v2_copy_function_batch_get_bind_data, info, RET)?;
            let bind_data = unsafe { get_opaque_data_ref::<CopyFunctionBindData<T::BindData>>(bind_data) }.unwrap();

            let init_data = get_init_data!(ffi::duckdb_v2_copy_function_batch_get_init_data, info).unwrap();

            let collection = ColumnDataCollection {
                handle: input,
                logical_types: bind_data.logical_types.clone(),
            };

            let batch_data = T::batch(user_data, Context(context), &bind_data.data, init_data, collection)?;

            check_api_call!(
                ffi::duckdb_v2_copy_function_batch_set_batch_data,
                info,
                into_opaque(batch_data)
            )?;

            Ok(())
        },
        err,
    );
}

unsafe extern "C" fn flush_callback<T: CopyFunctionCallbacks>(
    info: ffi::duckdb_v2_copy_function_flush_info_handle,
    context: ffi::duckdb_v2_context_handle,
    err: *mut ffi::duckdb_v2_error_info_handle,
) {
    handle_unwind(
        || {
            let user_data = get_user_data!(ffi::duckdb_v2_copy_function_flush_get_user_data, info);

            let bind_data = check_api_call!(ffi::duckdb_v2_copy_function_flush_get_bind_data, info, RET)?;
            let bind_data = unsafe { get_opaque_data_ref::<CopyFunctionBindData<T::BindData>>(bind_data) }.unwrap();

            let init_data = get_init_data!(ffi::duckdb_v2_copy_function_flush_get_init_data, info).unwrap();

            let batch_data = check_api_call!(ffi::duckdb_v2_copy_function_flush_get_batch_data, info, RET)?;

            let batch_data = unsafe { get_opaque_data_ref(batch_data) }.unwrap();

            T::flush(user_data, Context(context), &bind_data.data, init_data, batch_data)?;

            Ok(())
        },
        err,
    );
}

unsafe extern "C" fn finalize_callback<T: CopyFunctionCallbacks>(
    info: ffi::duckdb_v2_copy_function_finalize_info_handle,
    context: ffi::duckdb_v2_context_handle,
    err: *mut ffi::duckdb_v2_error_info_handle,
) {
    handle_unwind(
        || {
            let user_data = get_user_data!(ffi::duckdb_v2_copy_function_finalize_get_user_data, info);

            let bind_data = check_api_call!(ffi::duckdb_v2_copy_function_finalize_get_bind_data, info, RET)?;
            let bind_data = unsafe { get_opaque_data_ref::<CopyFunctionBindData<T::BindData>>(bind_data) }.unwrap();

            let init_data = get_init_data!(ffi::duckdb_v2_copy_function_finalize_get_init_data, info).unwrap();

            T::finalize(user_data, Context(context), &bind_data.data, init_data)?;

            Ok(())
        },
        err,
    );
}

struct CopyFunctionBuilderHandle(ffi::duckdb_v2_copy_function_builder_handle);

impl Deref for CopyFunctionBuilderHandle {
    type Target = ffi::duckdb_v2_copy_function_builder_handle;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Drop for CopyFunctionBuilderHandle {
    fn drop(&mut self) {
        check_api_call_no_err!(ffi::duckdb_v2_copy_function_builder_destroy, &mut self.0).unwrap();
    }
}

/// Builds and registers a user-defined `COPY TO` format.
pub struct CopyFunctionBuilder<T: CopyFunctionCallbacks> {
    user_data: OpaqueHandle<T>,
    name: String,
}

impl<T: CopyFunctionCallbacks> CopyFunctionBuilder<T> {
    /// Create a copy-function builder with its SQL format name.
    pub fn new(name: impl Into<String>, user_data: T) -> Self {
        Self {
            name: name.into(),
            user_data: OpaqueHandle::new(user_data),
        }
    }

    fn build(&self) -> Result<CopyFunctionBuilderHandle> {
        let handle = CopyFunctionBuilderHandle(check_api_call!(ffi::duckdb_v2_copy_function_builder_create, RET)?);

        check_api_call!(
            ffi::duckdb_v2_copy_function_builder_set_name,
            handle.0,
            (&self.name).into()
        )?;

        check_api_call!(
            ffi::duckdb_v2_copy_function_builder_set_user_data,
            *handle,
            self.user_data.to_handle()
        )?;

        check_api_call!(
            ffi::duckdb_v2_copy_function_builder_set_bind_callback,
            *handle,
            Some(bind_callback::<T>)
        )?;

        check_api_call!(
            ffi::duckdb_v2_copy_function_builder_set_init_callback,
            *handle,
            Some(init_callback::<T>)
        )?;

        check_api_call!(
            ffi::duckdb_v2_copy_function_builder_set_batch_callback,
            *handle,
            Some(batch_callback::<T>)
        )?;

        check_api_call!(
            ffi::duckdb_v2_copy_function_builder_set_flush_callback,
            *handle,
            Some(flush_callback::<T>)
        )?;

        check_api_call!(
            ffi::duckdb_v2_copy_function_builder_set_finalize_callback,
            *handle,
            Some(finalize_callback::<T>)
        )?;

        Ok(handle)
    }

    context_and_connection_fn! {
        /// Register the copy function through a connection or callback context.
        pub fn register_with_[context, connection](self) -> Result<()>
        {
            context_fn: ffi::duckdb_v2_copy_function_builder_register_with_context,
            connection_fn: ffi::duckdb_v2_copy_function_builder_register_with_connection,
        }
        let handle = self.build()?;

        check_api_call!(
            api_fn!(),
            **api_arg!(),
            *handle,
        )?;

        Ok(())
    }
}
/// Columns supplied to a copy function during binding.
///
/// Column order matches the input relation being copied. Returned logical
/// types are owned copies.
pub struct ColumnInfo {
    handle: ffi::duckdb_v2_copy_function_bind_info_handle,
}

impl ColumnInfo {
    fn logical_types(&self) -> Result<Vec<LogicalType>> {
        (0..self.len()?)
            .map(|index| self.get_column(index).map(|(_, logical_type)| logical_type))
            .collect()
    }

    /// Return the number of input columns.
    pub fn len(&self) -> Result<usize> {
        let res = check_api_call!(ffi::duckdb_v2_copy_function_bind_get_column_count, self.handle, RET)?;

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
            ffi::duckdb_v2_copy_function_bind_get_column_name,
            self.handle,
            index as u64,
            &mut name
        )?;

        let borrowed_type = check_api_call!(
            ffi::duckdb_v2_copy_function_bind_get_column_type,
            self.handle,
            index as u64,
            RET
        )?;
        let logical_type = LogicalType {
            handle: check_api_call!(ffi::duckdb_v2_logical_type_copy, borrowed_type, RET)?,
        };

        Ok((name.into(), logical_type))
    }
}

/// Callback lifecycle for a user-defined `COPY TO` format.
///
/// DuckDB binds the input columns, initializes one output file, prepares input
/// batches, flushes each prepared batch, and finalizes the file.
pub trait CopyFunctionCallbacks: Send + Sync + 'static {
    /// State created once for the bound copy operation.
    type InitData: Any + Send + Sync;
    /// Data resolved while binding the input columns.
    type BindData: Any + Send + Sync;
    /// A prepared batch passed from [`Self::batch`] to [`Self::flush`].
    type BatchData: Any + Send + Sync;

    /// **Bind:** inspect the input columns and create shared bind data.
    fn bind(&self, context: Context, column_info: ColumnInfo) -> Result<Self::BindData>;

    /// **Initialize:** open the output path and create file-level state.
    fn init(&self, _context: Context, _bind_data: &Self::BindData, file_path: &str) -> Result<Self::InitData>;

    /// **Batch:** prepare one input collection for flushing.
    fn batch(
        &self,
        _context: Context,
        _bind_data: &Self::BindData,
        _init_data: &Self::InitData,
        input: ColumnDataCollection,
    ) -> Result<Self::BatchData>;

    /// **Flush:** write one prepared batch to the output.
    fn flush(
        &self,
        _context: Context,
        _bind_data: &Self::BindData,
        _init_data: &Self::InitData,
        _batch_data: &Self::BatchData,
    ) -> Result<()>;

    /// **Finalize:** finish and close the output after all batches are flushed.
    fn finalize(&self, _context: Context, _bind_data: &Self::BindData, _init_data: &Self::InitData) -> Result<()>;
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests;

//! Internal owned handles and their factories.
//!
//! Callback-borrowed views and wrappers that retain parent objects stay in
//! their respective modules.

use crate::{
    Result, check_api_call,
    column_data_collection::ColumnDataCollection,
    connection::{Connection, Extension},
    database::Database,
    ffi,
};

macro_rules! define_handle {
    (
        name: $name:ident,
        link: $link:ident,
        create: $create:ident,
        handle: $handle:ty,
        destroy: $destroy:expr,
        factories: {
            $( $type:ty => $func:expr ),* $(,)?
        } $(,)?
    ) => {
        define_handle! {
            name: $name,
            handle: $handle,
            destroy: $destroy,
        }

        pub(crate) trait $link {
            fn $create(&self) -> $crate::Result<$name>;
        }

        $(
            impl $link for $type {
                fn $create(&self) -> $crate::Result<$name> {
                    Ok($name($crate::check_api_call!($func, **self, RET)?))
                }
            }
        )*
    };
    (
        name: $name:ident,
        handle: $handle:ty,
        destroy: $destroy:expr $(,)?
    ) => {
        pub(crate) struct $name($handle);

        impl ::std::ops::Drop for $name {
            fn drop(&mut self) {
                $crate::check_api_call_no_err!($destroy, &mut self.0)
                    .expect(concat!("Failed to destroy ", stringify!($name)));
            }
        }

        impl ::std::ops::Deref for $name {
            type Target = $handle;

            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }
    };
}

define_handle! {
    name: ScalarFunctionBuilderHandle,
    link: ScalarFunctionBuilderLink,
    create: create_scalar_function_handle,
    handle: ffi::duckdb_v2_scalar_function_handle,
    destroy: ffi::duckdb_v2_scalar_function_destroy,
    factories: {
        Connection => ffi::duckdb_v2_scalar_function_create_with_connection,
        Extension => ffi::duckdb_v2_scalar_function_create_with_extension,
    },
}

define_handle! {
    name: AggregateFunctionBuilderHandle,
    link: AggregateFunctionBuilderLink,
    create: create_aggregate_function_handle,
    handle: ffi::duckdb_v2_aggregate_function_handle,
    destroy: ffi::duckdb_v2_aggregate_function_destroy,
    factories: {
        Connection => ffi::duckdb_v2_aggregate_function_create_with_connection,
        Extension => ffi::duckdb_v2_aggregate_function_create_with_extension,
    },
}

define_handle! {
    name: TableFunctionBuilderHandle,
    link: TableFunctionBuilderLink,
    create: create_table_function_handle,
    handle: ffi::duckdb_v2_table_function_handle,
    destroy: ffi::duckdb_v2_table_function_destroy,
    factories: {
        Connection => ffi::duckdb_v2_table_function_create_with_connection,
        Extension => ffi::duckdb_v2_table_function_create_with_extension,
    },
}

define_handle! {
    name: CastFunctionHandle,
    link: CastFunctionLink,
    create: create_cast_function_handle,
    handle: ffi::duckdb_v2_cast_function_handle,
    destroy: ffi::duckdb_v2_cast_function_destroy,
    factories: {
        Connection => ffi::duckdb_v2_cast_function_create_with_connection,
        Extension => ffi::duckdb_v2_cast_function_create_with_extension,
    },
}

define_handle! {
    name: CopyFunctionBuilderHandle,
    link: CopyFunctionBuilderLink,
    create: create_copy_function_handle,
    handle: ffi::duckdb_v2_copy_function_handle,
    destroy: ffi::duckdb_v2_copy_function_destroy,
    factories: {
        Connection => ffi::duckdb_v2_copy_function_create_with_connection,
        Extension => ffi::duckdb_v2_copy_function_create_with_extension,
    },
}

define_handle! {
    name: CustomTypeBuilderHandle,
    link: CustomTypeBuilderLink,
    create: create_custom_type_handle,
    handle: ffi::duckdb_v2_custom_type_handle,
    destroy: ffi::duckdb_v2_custom_type_destroy,
    factories: {
        Connection => ffi::duckdb_v2_custom_type_create_with_connection,
        Extension => ffi::duckdb_v2_custom_type_create_with_extension,
    },
}

define_handle! {
    name: ReplacementScanBuilderHandle,
    link: ReplacementScanBuilderLink,
    create: create_replacement_scan_handle,
    handle: ffi::duckdb_v2_replacement_scan_handle,
    destroy: ffi::duckdb_v2_replacement_scan_destroy,
    factories: {
        Connection => ffi::duckdb_v2_replacement_scan_create_with_connection,
        Extension => ffi::duckdb_v2_replacement_scan_create_with_extension,
    },
}

impl ReplacementScanBuilderLink for Database {
    fn create_replacement_scan_handle(&self) -> Result<ReplacementScanBuilderHandle> {
        Ok(ReplacementScanBuilderHandle(check_api_call!(
            ffi::duckdb_v2_replacement_scan_create_with_database,
            self.handle.lock().unwrap().handle,
            RET
        )?))
    }
}

define_handle! {
    name: ColumnDataCollectionAppendStateHandle,
    link: ColumnDataCollectionAppendLink,
    create: create_append_state,
    handle: ffi::duckdb_v2_column_data_collection_append_state_handle,
    destroy: ffi::duckdb_v2_column_data_collection_append_state_destroy,
    factories: {
        ColumnDataCollection => ffi::duckdb_v2_column_data_collection_append_state_create,
    },
}

define_handle! {
    name: ColumnDataCollectionWorkerScanStateHandle,
    link: ColumnDataCollectionWorkerScanLink,
    create: create_worker_scan_state,
    handle: ffi::duckdb_v2_column_data_collection_worker_scan_state_handle,
    destroy: ffi::duckdb_v2_column_data_collection_worker_scan_state_destroy,
    factories: {
        ColumnDataCollection => ffi::duckdb_v2_column_data_collection_worker_scan_state_create,
    },
}

define_handle! {
    name: ColumnDataCollectionSharedScanStateHandle,
    link: ColumnDataCollectionSharedScanLink,
    create: create_shared_scan_state,
    handle: ffi::duckdb_v2_column_data_collection_shared_scan_state_handle,
    destroy: ffi::duckdb_v2_column_data_collection_shared_scan_state_destroy,
    factories: {
        ColumnDataCollection => ffi::duckdb_v2_column_data_collection_shared_scan_state_create,
    },
}

#[cfg(feature = "capi-v2-p4")]
define_handle! {
    name: LogStorageBuilderHandle,
    handle: ffi::duckdb_v2_log_storage_builder_handle,
    destroy: ffi::duckdb_v2_log_storage_builder_destroy,
}

#[cfg(feature = "capi-v2-p4")]
impl LogStorageBuilderHandle {
    pub(crate) fn new() -> Result<Self> {
        Ok(Self(check_api_call!(ffi::duckdb_v2_log_storage_builder_create, RET)?))
    }
}

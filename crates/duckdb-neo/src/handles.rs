//! Internal owned
//! //! Internal factoryhandles and their registerers.
//!
//! Callback-borrowed views and wrappers that retain parent objects stay in
//! their respective modules.

use crate::{column_data_collection::ColumnDataCollection, connection::Connection, ffi, instance::Instance};
use crate::{connection::HasFactory, extension::Extension};

macro_rules! define_handle {
    (
        name: $name:ident,
        link: $link:ident,
        create: $create:ident,
        register: $register:ident,
        handle: $handle:ty,
        destroy: $destroy:expr,
        factory: $factory:expr,
        registerers: {
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

            fn $register(&self, handle: $name) -> $crate::Result<()>;
        }

        $(
            impl $link for $type {
                fn $create(&self) -> $crate::Result<$name> {
                    $crate::check_api_call!($factory, $crate::raw::RawExt::raw(self.factory()?), RET).map($name)
                }

                fn $register(&self, handle: $name) -> $crate::Result<()> {
                    $crate::check_api_call!($func, $crate::raw::RawExt::raw(self), $crate::raw::RawExt::raw(&handle))
                }
            }
        )*
    };
    (
        name: $name:ident,
        handle: $handle:ty,
        destroy: $destroy:expr $(,)?
    ) => {
        #[derive(Debug, $crate::AsRaw, $crate::DropHandle)]
        #[drop_handle($destroy)]
        pub(crate) struct $name($handle);
    };
}

/// Like [`define_handle`], but for states created from a [`ColumnDataCollection`]
/// rather than a factory; the generic link type rules out the shared `impl` arm.
macro_rules! define_handle_column_data_collection {
    (
        name: $name:ident,
        link: $link:ident,
        create: $create:ident,
        handle: $handle:ty,
        destroy: $destroy:expr,
        factory: $factory:expr $(,)?
    ) => {
        define_handle! {
            name: $name,
            handle: $handle,
            destroy: $destroy,
        }

        pub(crate) trait $link {
            fn $create(&mut self) -> $crate::Result<$name>;
        }

        impl $link for ColumnDataCollection<'_> {
            fn $create(&mut self) -> $crate::Result<$name> {
                $crate::check_api_call!($factory, $crate::raw::RawExt::raw(self), RET).map($name)
            }
        }
    };
}

define_handle! {
    name: LogSinkBuilderHandle,
    link: LogSinkBuilderLink,
    create: create_log_sink_handle,
    register: register_log_sink,
    handle: ffi::duckdb_v2_log_sink_handle,
    destroy: ffi::duckdb_v2_log_sink_destroy,
    factory: ffi::duckdb_v2_log_sink_create,
    registerers: {
        Instance => ffi::duckdb_v2_instance_register_log_sink,
        Extension => ffi::duckdb_v2_extension_register_log_sink,
    },
}

define_handle! {
    name: ScalarFunctionBuilderHandle,
    link: ScalarFunctionBuilderLink,
    create: create_scalar_function_handle,
    register: register_scalar_function,
    handle: ffi::duckdb_v2_scalar_function_handle,
    destroy: ffi::duckdb_v2_scalar_function_destroy,
    factory: ffi::duckdb_v2_scalar_function_create,
    registerers: {
        Connection => ffi::duckdb_v2_connection_register_scalar_function,
        Extension => ffi::duckdb_v2_extension_register_scalar_function,
    },
}

define_handle! {
    name: AggregateFunctionBuilderHandle,
    link: AggregateFunctionBuilderLink,
    create: create_aggregate_function_handle,
    register: register_aggregate_function,
    handle: ffi::duckdb_v2_aggregate_function_handle,
    destroy: ffi::duckdb_v2_aggregate_function_destroy,
    factory: ffi::duckdb_v2_aggregate_function_create,
    registerers: {
        Connection => ffi::duckdb_v2_connection_register_aggregate_function,
        Extension => ffi::duckdb_v2_extension_register_aggregate_function
    }
}

define_handle! {
    name: TableFunctionBuilderHandle,
    link: TableFunctionBuilderLink,
    create: create_table_function_handle,
    register: register_table_function_handle,
    handle: ffi::duckdb_v2_table_function_handle,
    destroy: ffi::duckdb_v2_table_function_destroy,
    factory: ffi::duckdb_v2_table_function_create,
    registerers: {
        Connection => ffi::duckdb_v2_connection_register_table_function,
        Extension => ffi::duckdb_v2_extension_register_table_function,
    },
}

define_handle! {
    name: CustomOptionBuilderHandle,
    link: CustomOptionBuilderLink,
    create: create_custom_option_handle,
    register: register_custom_option_handle,
    handle: ffi::duckdb_v2_custom_option_handle,
    destroy: ffi::duckdb_v2_custom_option_destroy,
    factory: ffi::duckdb_v2_custom_option_create,
    registerers: {
        Connection => ffi::duckdb_v2_connection_register_custom_option,
        Extension => ffi::duckdb_v2_extension_register_custom_option,
    },
}

define_handle! {
    name: PragmaFunctionBuilderHandle,
    link: PragmaFunctionBuilderLink,
    create: create_pragma_function_handle,
    register: register_pragma_function_handle,
    handle: ffi::duckdb_v2_pragma_function_handle,
    destroy: ffi::duckdb_v2_pragma_function_destroy,
    factory: ffi::duckdb_v2_pragma_function_create,
    registerers: {
        Connection => ffi::duckdb_v2_connection_register_pragma_function,
        Extension => ffi::duckdb_v2_extension_register_pragma_function,
    },
}

define_handle! {
    name: CastFunctionHandle,
    link: CastFunctionLink,
    create: create_cast_function_handle,
    register: register_cast_function_handle,
    handle: ffi::duckdb_v2_cast_function_handle,
    destroy: ffi::duckdb_v2_cast_function_destroy,
    factory: ffi::duckdb_v2_cast_function_create,
    registerers: {
        Connection => ffi::duckdb_v2_connection_register_cast_function,
        Extension => ffi::duckdb_v2_extension_register_cast_function,
    },
}

define_handle! {
    name: CopyFunctionBuilderHandle,
    link: CopyFunctionBuilderLink,
    create: create_copy_function_handle,
    register: register_copy_function_handle,
    handle: ffi::duckdb_v2_copy_function_handle,
    destroy: ffi::duckdb_v2_copy_function_destroy,
    factory: ffi::duckdb_v2_copy_function_create,
    registerers: {
        Connection => ffi::duckdb_v2_connection_register_copy_function,
        Extension => ffi::duckdb_v2_extension_register_copy_function,
    },
}

define_handle! {
    name: CustomTypeBuilderHandle,
    link: CustomTypeBuilderLink,
    create: create_custom_type_handle,
    register: register_custom_type_handle,
    handle: ffi::duckdb_v2_custom_type_handle,
    destroy: ffi::duckdb_v2_custom_type_destroy,
    factory: ffi::duckdb_v2_custom_type_create,
    registerers: {
        Connection => ffi::duckdb_v2_connection_register_custom_type,
        Extension => ffi::duckdb_v2_extension_register_custom_type,
    },
}

define_handle! {
    name: ReplacementScanBuilderHandle,
    link: ReplacementScanBuilderLink,
    create: create_replacement_scan_handle,
    register: register_replacement_scan_handle,
    handle: ffi::duckdb_v2_replacement_scan_handle,
    destroy: ffi::duckdb_v2_replacement_scan_destroy,
    factory: ffi::duckdb_v2_replacement_scan_create,
    registerers: {
        Connection => ffi::duckdb_v2_connection_register_replacement_scan,
        Extension => ffi::duckdb_v2_extension_register_replacement_scan,
        Instance => ffi::duckdb_v2_instance_register_replacement_scan,
    },
}

define_handle_column_data_collection! {
    name: ColumnDataCollectionAppendStateHandle,
    link: ColumnDataCollectionAppendLink,
    create: create_append_state,
    handle: ffi::duckdb_v2_column_data_collection_append_state_handle,
    destroy: ffi::duckdb_v2_column_data_collection_append_state_destroy,
    factory: ffi::duckdb_v2_column_data_collection_append_state_create,
}

define_handle_column_data_collection! {
    name: ColumnDataCollectionWorkerScanStateHandle,
    link: ColumnDataCollectionWorkerScanLink,
    create: create_worker_scan_state,
    handle: ffi::duckdb_v2_column_data_collection_worker_scan_state_handle,
    destroy: ffi::duckdb_v2_column_data_collection_worker_scan_state_destroy,
    factory: ffi::duckdb_v2_column_data_collection_worker_scan_state_create,
}

define_handle_column_data_collection! {
    name: ColumnDataCollectionSharedScanStateHandle,
    link: ColumnDataCollectionSharedScanLink,
    create: create_shared_scan_state,
    handle: ffi::duckdb_v2_column_data_collection_shared_scan_state_handle,
    destroy: ffi::duckdb_v2_column_data_collection_shared_scan_state_destroy,
    factory: ffi::duckdb_v2_column_data_collection_shared_scan_state_create,
}

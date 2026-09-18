//! Internal dispatch for operations available through multiple FFI link types.

use crate::{
    connection::{Connection, Context},
    ffi,
};

// Share the Rust signature and FFI arguments; only the link type and C function vary.
macro_rules! define_link {
    (
        name: $name:ident,
        method: fn $method:ident $params:tt -> $ret:ty,
        args: $args:tt,
        implementations: {
            $( $type:ty => $func:expr ),* $(,)?
        } $(,)?
    ) => {
        define_link!(@trait $name, $method, $params, $ret);
        $(
            define_link!(@impl $name, $type, $method, $params, $ret, $func, $args);
        )*
    };
    (@trait $name:ident, $method:ident, ($($arg:ident: $arg_ty:ty),* $(,)?), $ret:ty) => {
        pub(crate) trait $name {
            fn $method(&self, $($arg: $arg_ty),*) -> $crate::Result<$ret>;
        }
    };
    (
        @impl $name:ident, $type:ty, $method:ident,
        ($($arg:ident: $arg_ty:ty),* $(,)?), $ret:ty,
        $func:expr, ($($args:tt)*)
    ) => {
        impl $name for $type {
            fn $method(&self, $($arg: $arg_ty),*) -> $crate::Result<$ret> {
                $crate::check_api_call!($func, **self, $($args)*)
            }
        }
    };
}

define_link! {
    name: ColumnDataCollectionLink,
    method: fn create_column_data_collection(types: &[ffi::duckdb_v2_logical_type_handle])
        -> ffi::duckdb_v2_column_data_collection_handle,
    args: (types.as_ptr(), types.len() as u64, RET),
    implementations: {
        Connection => ffi::duckdb_v2_column_data_collection_create_with_connection,
        Context => ffi::duckdb_v2_column_data_collection_create_with_context,
    },
}

define_link! {
    name: FileSystemLink,
    method: fn get_file_system() -> ffi::duckdb_v2_file_system_handle,
    args: (RET),
    implementations: {
        Connection => ffi::duckdb_v2_file_system_get_from_connection,
        Context => ffi::duckdb_v2_file_system_get_from_context,
    },
}

define_link! {
    name: LogicalTypeFromIdLink,
    method: fn create_logical_type_from_id(
        type_id: ffi::DUCKDB_V2_LOGICAL_TYPE_ID,
        names: Option<&[ffi::duckdb_v2_str]>,
        values: &[ffi::duckdb_v2_value_handle],
    ) -> ffi::duckdb_v2_logical_type_handle,
    args: (
        type_id,
        names.map_or(std::ptr::null(), |names| names.as_ptr()),
        values.as_ptr(),
        values.len() as u64,
        RET
    ),
    implementations: {
        Connection => ffi::duckdb_v2_connection_create_type_from_id,
        Context => ffi::duckdb_v2_context_create_type_from_id,
    },
}

define_link! {
    name: LogicalTypeFromNameLink,
    method: fn create_logical_type_from_name(
        name: ffi::duckdb_v2_qname_handle,
        names: Option<&[ffi::duckdb_v2_str]>,
        values: &[ffi::duckdb_v2_value_handle],
    ) -> ffi::duckdb_v2_logical_type_handle,
    args: (
        name,
        names.map_or(std::ptr::null(), |names| names.as_ptr()),
        values.as_ptr(),
        values.len() as u64,
        RET
    ),
    implementations: {
        Connection => ffi::duckdb_v2_connection_create_type_from_name,
        Context => ffi::duckdb_v2_context_create_type_from_name,
    },
}

define_link! {
    name: LogicalTypeFromTextLink,
    method: fn create_logical_type_from_text(text: &str) -> ffi::duckdb_v2_logical_type_handle,
    args: (text.into(), RET),
    implementations: {
        Connection => ffi::duckdb_v2_connection_create_type_from_text,
        Context => ffi::duckdb_v2_context_create_type_from_text,
    },
}

define_link! {
    name: LogicalTypeAliasLink,
    method: fn create_logical_type_with_alias(
        logical_type: ffi::duckdb_v2_logical_type_handle,
        alias: &str,
    ) -> ffi::duckdb_v2_logical_type_handle,
    args: (logical_type, alias.into(), RET),
    implementations: {
        Connection => ffi::duckdb_v2_connection_create_type_with_alias,
        Context => ffi::duckdb_v2_context_create_type_with_alias,
    },
}

define_link! {
    name: ValueCastLink,
    method: fn cast_value(
        value: ffi::duckdb_v2_value_handle,
        target_type: ffi::duckdb_v2_logical_type_handle,
    ) -> ffi::duckdb_v2_value_handle,
    args: (value, target_type, RET),
    implementations: {
        Connection => ffi::duckdb_v2_value_cast_with_connection,
        Context => ffi::duckdb_v2_value_cast_with_context,
    },
}

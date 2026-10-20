//! Internal dispatch for operations available through multiple FFI link types.

use std::{any::Any, sync::Arc};

use crate::{
    connection::{Connection, Context, Extension},
    ffi,
    instance::Instance,
};

/// Keeps a connection and its instance alive, and so their allocator and buffer managers.
pub(crate) type KeepAlive = Arc<dyn Any + Send + Sync>;

/// What an object allocated through a link must hold to keep its connection and instance alive.
///
/// A [`Context`] gives none: it only exists inside callbacks, whose connection outlives them.
pub(crate) trait InstanceKeepAlive {
    fn keep_alive(&self) -> Option<KeepAlive>;
}

impl InstanceKeepAlive for Connection {
    fn keep_alive(&self) -> Option<KeepAlive> {
        Some(Arc::new((self.inner.clone(), self.instance.clone())))
    }
}

impl InstanceKeepAlive for Instance {
    fn keep_alive(&self) -> Option<KeepAlive> {
        Some(Arc::new(self.handle.clone()))
    }
}

impl InstanceKeepAlive for Context {
    fn keep_alive(&self) -> Option<KeepAlive> {
        None
    }
}

/// The connection a link belongs to, if it is one.
///
/// Used to check that a collection is registered only on the connection whose buffer manager it uses.
pub(crate) trait ConnectionOrigin {
    fn connection_origin(&self) -> Option<ffi::duckdb_v2_connection_handle>;
}

impl ConnectionOrigin for Connection {
    fn connection_origin(&self) -> Option<ffi::duckdb_v2_connection_handle> {
        Some(self.inner.handle)
    }
}

impl ConnectionOrigin for Context {
    fn connection_origin(&self) -> Option<ffi::duckdb_v2_connection_handle> {
        None
    }
}

impl ConnectionOrigin for Instance {
    fn connection_origin(&self) -> Option<ffi::duckdb_v2_connection_handle> {
        None
    }
}

impl ConnectionOrigin for Extension {
    fn connection_origin(&self) -> Option<ffi::duckdb_v2_connection_handle> {
        None
    }
}

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
                use crate::raw::RawExt;

                $crate::check_api_call!($func, self.raw(), $($args)*)
            }
        }
    };
}

define_link! {
    name: FileSystemLink,
    method: fn get_file_system() -> ffi::duckdb_v2_file_system_handle,
    args: (RET),
    implementations: {
        Connection => ffi::duckdb_v2_connection_get_file_system,
        Context => ffi::duckdb_v2_context_get_file_system,
        Instance => ffi::duckdb_v2_instance_get_file_system,
    },
}

use std::{any::Any, cell::RefCell, ffi::c_void, panic::AssertUnwindSafe};

use libduckdb_sys::{self as ffi};

use crate::{
    Result,
    error::{DuckDBError, Error, check_api_call_no_err},
};

pub(crate) struct OpaqueHandle<T> {
    data: *mut T,
    success: RefCell<bool>,
}

impl<T> OpaqueHandle<T> {
    pub(crate) fn new(data: T) -> Self {
        let boxed = Box::new(data);
        let raw = Box::into_raw(boxed);

        OpaqueHandle {
            data: raw,
            success: RefCell::new(false),
        }
    }

    pub(crate) fn to_handle(&self) -> ffi::duckdb_v2_opaque {
        let mut success = self.success.borrow_mut();
        *success = true;

        ffi::duckdb_v2_opaque {
            ptr: self.data as *mut c_void,
            equals: None,
            destroy: Some(drop_opaque::<T>),
        }
    }
}

impl<T> Drop for OpaqueHandle<T> {
    fn drop(&mut self) {
        if !*self.success.borrow() {
            unsafe { drop_opaque::<T>(self.data as *mut c_void) }
        }
    }
}

pub(crate) fn set_error(err: *mut ffi::duckdb_v2_error_info_handle, error: &Error) {
    check_api_call_no_err!(ffi::duckdb_v2_error_info_set_code, *err, error.code)
        .expect("Failed to set error code");
    check_api_call_no_err!(
        ffi::duckdb_v2_error_info_set_text,
        *err,
        (&error.message).into()
    )
    .expect("Failed to set error text");
}

pub(crate) unsafe extern "C" fn drop_opaque<T>(ptr: *mut c_void) {
    if !ptr.is_null() {
        let _ = unsafe { Box::from_raw(ptr as *mut T) };
    }
}

pub(crate) fn into_opaque<T>(value: T) -> ffi::duckdb_v2_opaque {
    let raw = Box::into_raw(Box::new(value));

    ffi::duckdb_v2_opaque {
        ptr: raw as *mut c_void,
        equals: None,
        destroy: Some(drop_opaque::<T>),
    }
}

pub(crate) unsafe fn get_opaque_data_ref<'a, T>(ptr: *mut c_void) -> Option<&'a T> {
    if ptr.is_null() {
        None
    } else {
        Some(unsafe { &*(ptr as *const T) })
    }
}

pub(crate) unsafe fn get_opaque_data_ref_mut<'a, T>(ptr: *mut c_void) -> Option<&'a mut T> {
    if ptr.is_null() {
        None
    } else {
        Some(unsafe { &mut *(ptr as *mut T) })
    }
}

fn panic_message(panic: &(dyn Any + Send)) -> &str {
    if let Some(text) = panic.downcast_ref::<String>() {
        text.as_str()
    } else if let Some(text) = panic.downcast_ref::<&str>() {
        text
    } else {
        "Unknown panic occurred"
    }
}

pub(crate) fn handle_unwind<T, F: FnOnce() -> Result<T>>(
    f: F,
    err: *mut ffi::duckdb_v2_error_info_handle,
) -> Option<T> {
    let result = std::panic::catch_unwind(AssertUnwindSafe(f));

    match result {
        Ok(Ok(value)) => Some(value),
        Ok(Err(e)) => {
            dbg!(&e);
            set_error(err, &e);
            None
        }
        Err(panic) => {
            let text = panic_message(panic.as_ref());
            let error = Error {
                code: DuckDBError::DUCKDB_V2_ERROR_RUNTIME_INTERNAL,
                message: format!("Panic occurred: {text}"),
            };
            dbg!(&error);
            set_error(err, &error);
            None
        }
    }
}

macro_rules! context_and_connection_fn {
    (
        $(#[$meta:meta])*
        $vis:vis fn $prefix:ident [$context_suffix:ident, $connection_suffix:ident]
        $(<$($gen:ident),+ $(,)?>)? (
            &$receiver:ident $(, $arg_name:ident : $arg_ty:ty)* $(,)?
        ) -> $ret:ty
        {
            context_fn: $ctx_fn:expr,
            connection_fn: $conn_fn:expr,
        }
        $($body:tt)*
    ) => {
        paste::paste! {
            $(#[$meta])*
            $vis fn [<$prefix $context_suffix>] $(<$($gen),+>)? (
                &$receiver,
                context: &$crate::connection::Context,
                $($arg_name: $arg_ty),*
            ) -> $ret {
                macro_rules! api_fn { () => { $ctx_fn } }
                macro_rules! api_arg { () => { context } }
                $($body)*
            }

            $(#[$meta])*
            $vis fn [<$prefix $connection_suffix>] $(<$($gen),+>)? (
                &$receiver,
                connection: &$crate::connection::Connection,
                $($arg_name: $arg_ty),*
            ) -> $ret {
                macro_rules! api_fn { () => { $conn_fn } }
                macro_rules! api_arg { () => { connection } }
                $($body)*
            }
        }
    };
    (
        $(#[$meta:meta])*
        $vis:vis fn $prefix:ident [$context_suffix:ident, $connection_suffix:ident]
        $(<$($gen:ident),+ $(,)?>)? (
            $($arg_name:ident : $arg_ty:ty),* $(,)?
        ) -> $ret:ty
        {
            context_fn: $ctx_fn:expr,
            connection_fn: $conn_fn:expr,
        }
        $($body:tt)*
    ) => {
        paste::paste! {
            $(#[$meta])*
            $vis fn [<$prefix $context_suffix>] $(<$($gen),+>)? (
                context: &$crate::connection::Context,
                $($arg_name: $arg_ty),*
            ) -> $ret {
                macro_rules! api_fn { () => { $ctx_fn } }
                macro_rules! api_arg { () => { context } }
                $($body)*
            }

            $(#[$meta])*
            $vis fn [<$prefix $connection_suffix>] $(<$($gen),+>)? (
                connection: &$crate::connection::Connection,
                $($arg_name: $arg_ty),*
            ) -> $ret {
                macro_rules! api_fn { () => { $conn_fn } }
                macro_rules! api_arg { () => { connection } }
                $($body)*
            }
        }
    };
    (
        $(#[$meta:meta])*
        $vis:vis fn $prefix:ident [$context_suffix:ident, $connection_suffix:ident]
        $(<$($gen:ident),+ $(,)?>)? (
            $receiver:ident $(, $arg_name:ident : $arg_ty:ty)* $(,)?
        ) -> $ret:ty
        {
            context_fn: $ctx_fn:expr,
            connection_fn: $conn_fn:expr,
        }
        $($body:tt)*
    ) => {
        paste::paste! {
            $(#[$meta])*
            $vis fn [<$prefix $context_suffix>] $(<$($gen),+>)? (
                $receiver,
                context: &$crate::connection::Context,
                $($arg_name: $arg_ty),*
            ) -> $ret {
                macro_rules! api_fn { () => { $ctx_fn } }
                macro_rules! api_arg { () => { context } }
                $($body)*
            }

            $(#[$meta])*
            $vis fn [<$prefix $connection_suffix>] $(<$($gen),+>)? (
                $receiver,
                connection: &$crate::connection::Connection,
                $($arg_name: $arg_ty),*
            ) -> $ret {
                macro_rules! api_fn { () => { $conn_fn } }
                macro_rules! api_arg { () => { connection } }
                $($body)*
            }
        }
    };
}

macro_rules! get_user_data {
    ($ffi_call:expr, $handle:expr) => {{
        let user_data = check_api_call!($ffi_call, $handle, RET)?;

        unsafe { $crate::builder_helpers::get_opaque_data_ref::<T>(user_data) }.unwrap()
    }};
}

macro_rules! get_bind_data {
    ($ffi_call:expr, $handle:expr) => {{
        let bind_data = check_api_call!($ffi_call, $handle, RET)?;

        unsafe { $crate::builder_helpers::get_opaque_data_ref::<T::BindData>(bind_data) }
    }};
}

macro_rules! get_init_data {
    ($ffi_call:expr, $handle:expr) => {{
        let bind_data = check_api_call!($ffi_call, $handle, RET)?;

        unsafe { $crate::builder_helpers::get_opaque_data_ref::<T::InitData>(bind_data) }
    }};
}

macro_rules! get_global_state {
    ($ffi_call:expr, $handle:expr) => {{
        let global_data = check_api_call!($ffi_call, $handle, RET)?;

        unsafe { $crate::builder_helpers::get_opaque_data_ref::<T::GlobalState>(global_data) }
    }};
}

macro_rules! get_local_state {
    ($ffi_call:expr, $handle:expr) => {{
        let local_data = check_api_call!($ffi_call, $handle, RET)?;
        unsafe { $crate::builder_helpers::get_opaque_data_ref_mut::<T::LocalState>(local_data) }
    }};
}

macro_rules! ffi_enum_redeclaration {
    (
        $(#[$meta:meta])*
        $vis:vis enum $name:ident <- $ffi_ty:path {
            $($(#[$variant_meta:meta])* $variant:ident = $ffi_variant:ident),* $(,)?
        }
    ) => {
        $(#[$meta])*
        #[repr(u32)]
        #[derive(Debug, PartialEq, Eq, Clone)]
        $vis enum $name {
            $($(#[$variant_meta])* $variant = <$ffi_ty>::$ffi_variant as u32),*
        }

        impl From<$name> for $ffi_ty {
            fn from(value: $name) -> Self {
                match value {
                    $($name::$variant => <$ffi_ty>::$ffi_variant,)*
                }
            }
        }

        impl TryInto<$name> for $ffi_ty {
            type Error = $crate::Error;
            fn try_into(self) -> $crate::Result<$name> {
                match self {
                    $(<$ffi_ty>::$ffi_variant => Ok($name::$variant),)*
                    _ => Err($crate::Error::api_error(format!(
                        "Unknown {}: {:?}",
                        stringify!($name),
                        self
                    ))),
                }
            }
        }
    };
}

pub(crate) use ffi_enum_redeclaration;

pub(crate) use context_and_connection_fn;
pub(crate) use get_bind_data;
pub(crate) use get_global_state;
pub(crate) use get_init_data;
pub(crate) use get_local_state;
pub(crate) use get_user_data;

#[cfg(test)]
#[cfg(feature = "capi-v2-p2")]
macro_rules! scalar_callback {
    ($name:ident, $result_type:ty, |$input:ident, $result:ident, $ctx:ident, $user_data:ident| $body:block) => {
        struct $name;

        impl $crate::scalar::ScalarCallbacks for $name {
            type BindData = ();
            type InitData = ();
            type ResultType = $result_type;

            fn exec(
                &self,
                _bind_data: Option<&Self::BindData>,
                _init_data: Option<&Self::InitData>,
                $ctx: $crate::connection::Context,
                $input: &$crate::data_chunk::DataChunk,
                $result: $crate::vector::Vector<'_>,
            ) -> $crate::Result<()> {
                let $result = $result.cast::<Self::ResultType>()?;
                $body
            }
        }
    };
}

#[cfg(not(feature = "capi-v2-p2"))]
macro_rules! scalar_callback {
    ($name:ident, $result_type:ty, |$input:ident, $result:ident, $ctx:ident, $user_data:ident| $body:block) => {};
}

#[cfg(test)]
pub(crate) use scalar_callback;

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::panic_message;
    use std::any::Any;

    #[test]
    fn extracts_standard_panic_messages() {
        let owned: Box<dyn Any + Send> = Box::new("owned panic".to_string());
        let borrowed: Box<dyn Any + Send> = Box::new("borrowed panic");
        let unknown: Box<dyn Any + Send> = Box::new(42_u32);

        assert_eq!(panic_message(owned.as_ref()), "owned panic");
        assert_eq!(panic_message(borrowed.as_ref()), "borrowed panic");
        assert_eq!(panic_message(unknown.as_ref()), "Unknown panic occurred");
    }
}

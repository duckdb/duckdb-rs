use crate::{
    Result,
    error::{Error, check_api_call_no_err},
    ffi,
    ffi_str::DuckDBStr,
};

pub(crate) unsafe extern "C" fn copy_render_box(
    text: *const ffi::duckdb_v2_str,
    user_data: *mut std::os::raw::c_void,
    err: *mut ffi::duckdb_v2_error_info_handle,
) {
    handle_unwind(
        || {
            if text.is_null() || user_data.is_null() {
                return Err(Error::api_error("NULL value in copy_render_box"));
            }

            let string = unsafe { &mut *(user_data as *mut String) };

            string.push_str(DuckDBStr::from_raw(unsafe { *text }).as_str().unwrap_or_default());

            Ok(())
        },
        err,
    );
}

#[derive(Debug)]
pub(crate) struct OpaqueHandle<T> {
    data: *mut T,
    success: RefCell<bool>,
}

#[derive(Debug, Default)]
pub struct ArgCount {
    pub positional_fixed: usize,
    pub positional_variadic: usize,
    pub named_fixed: usize,
    pub named_variadic: usize,
}

impl ArgCount {
    pub(crate) fn from_func<F>(x: F) -> Result<Self>
    where
        F: FnOnce(&mut ffi::idx_t, &mut ffi::idx_t, &mut ffi::idx_t, &mut ffi::idx_t) -> Result<()>,
    {
        let mut positional_fixed = 0 as ffi::idx_t;
        let mut positional_variadic = 0 as ffi::idx_t;
        let mut named_fixed = 0 as ffi::idx_t;
        let mut named_variadic = 0 as ffi::idx_t;

        x(
            &mut positional_fixed,
            &mut positional_variadic,
            &mut named_fixed,
            &mut named_variadic,
        )?;

        Ok(ArgCount {
            positional_fixed: positional_fixed as usize,
            positional_variadic: positional_variadic as usize,
            named_fixed: named_fixed as usize,
            named_variadic: named_variadic as usize,
        })
    }

    pub fn total(&self) -> usize {
        self.positional_fixed + self.positional_variadic + self.named_fixed + self.named_variadic
    }
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
    if err.is_null() || unsafe { (*err).is_null() } {
        return;
    }
    let code = match error.code {
        ffi::DUCKDB_V2_ERROR::DUCKDB_V2_ERROR_NONE => ffi::DUCKDB_V2_ERROR::DUCKDB_V2_ERROR_API,
        code => code,
    };
    let _ = check_api_call_no_err!(ffi::duckdb_v2_error_info_set_code, *err, code);
    let _ = check_api_call_no_err!(ffi::duckdb_v2_error_info_set_text, *err, &(&error.message).into());
}

pub(crate) unsafe extern "C" fn drop_opaque<T>(ptr: *mut c_void) {
    if !ptr.is_null() {
        let _ = unsafe { Box::from_raw(ptr as *mut T) };
    }
}

unsafe extern "C" fn equals_opaque<T: PartialEq>(a: *mut c_void, b: *mut c_void) -> bool {
    unsafe { *(a as *const T) == *(b as *const T) }
}

pub(crate) fn into_opaque<T>(value: T) -> ffi::duckdb_v2_opaque {
    let raw = Box::into_raw(Box::new(value));

    ffi::duckdb_v2_opaque {
        ptr: raw as *mut c_void,
        equals: None,
        destroy: Some(drop_opaque::<T>),
    }
}

pub(crate) fn into_opaque_eq<T: PartialEq>(value: T) -> ffi::duckdb_v2_opaque {
    ffi::duckdb_v2_opaque {
        equals: Some(equals_opaque::<T>),
        ..into_opaque(value)
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
            set_error(err, &e);
            None
        }
        Err(panic) => {
            let text = panic_message(panic.as_ref());
            // INTERNAL intentionally invalidates the instance and is reported as an internal DuckDB failure.
            let error = Error::internal(format!("Panic occurred: {text}"));
            set_error(err, &error);
            None
        }
    }
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
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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

        impl TryFrom<$ffi_ty> for $name {
            type Error = $crate::Error;
            fn try_from(value: $ffi_ty) -> $crate::Result<Self> {
                match value {
                    $(<$ffi_ty>::$ffi_variant => Ok($name::$variant),)*
                    _ => Err($crate::Error::api_error(format!(
                        "Unknown {}: {:?}",
                        stringify!($name),
                        value
                    ))),
                }
            }
        }
    };
}

use std::{any::Any, cell::RefCell, ffi::c_void, panic::AssertUnwindSafe};

pub(crate) use ffi_enum_redeclaration;

pub(crate) use get_bind_data;
pub(crate) use get_global_state;
pub(crate) use get_init_data;
pub(crate) use get_local_state;
pub(crate) use get_user_data;

#[cfg(test)]
macro_rules! scalar_callback {
    ($name:ident, $result_type:ty, |$input:ident, $result:ident, $ctx:ident, $user_data:ident| $body:block) => {
        struct $name;

        impl $crate::scalar::ScalarCallbacks for $name {
            type BindData = ();
            type InitData = ();

            fn exec(
                &self,
                _bind_data: Option<&Self::BindData>,
                _init_data: Option<&mut Self::InitData>,
                $ctx: &$crate::connection::Context,
                $input: &mut $crate::data_chunk::VectorCollection,
                $result: $crate::vector::Vector<'_, crate::vector::Unknown>,
            ) -> $crate::Result<()> {
                let $result = $result.cast::<$result_type>()?;
                $body
            }
        }
    };
}

#[cfg(test)]
pub(crate) use scalar_callback;

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::panic_message;
    use std::any::Any;

    #[test]
    fn test_enum_conversion_traits() -> crate::Result<()> {
        fn assert_copy_hash<T: Copy + std::hash::Hash>() {}
        assert_copy_hash::<crate::cast::CastMode>();
        let mode = crate::cast::CastMode::try_from(crate::ffi::DUCKDB_V2_CAST_MODE::DUCKDB_V2_CAST_MODE_TRY)?;
        assert_eq!(mode, crate::cast::CastMode::Try);
        assert_eq!(
            crate::ffi::DUCKDB_V2_CAST_MODE::from(mode),
            crate::ffi::DUCKDB_V2_CAST_MODE::DUCKDB_V2_CAST_MODE_TRY
        );
        Ok(())
    }

    #[test]
    fn test_set_error_maps_none_to_api() -> crate::Result<()> {
        let mut handle = std::ptr::null_mut();
        // SAFETY: the byte argument and output slot are valid for this call.
        unsafe { crate::ffi::duckdb_v2_validate_utf8(&crate::ffi_str::bytes_arg(b"\xff"), &mut handle) };
        assert!(!handle.is_null());
        let error = crate::Error {
            code: crate::error::DuckDBError::DUCKDB_V2_ERROR_NONE,
            message: "callback error".to_owned(),
        };
        super::set_error(&mut handle, &error);
        let mut code = crate::error::DuckDBError::DUCKDB_V2_ERROR_NONE;
        super::check_api_call_no_err!(crate::ffi::duckdb_v2_error_info_get_code, handle, &mut code)?;
        // SAFETY: this test transfers ownership of the live error handle.
        let error = unsafe { crate::Error::from_code_and_handle(code, &mut handle) };
        assert_eq!(error.code, crate::error::DuckDBError::DUCKDB_V2_ERROR_API);
        assert_eq!(error.message, "callback error");
        Ok(())
    }

    #[test]
    fn test_set_error_null_slot() {
        let error = crate::Error::api_error("callback error");
        super::set_error(std::ptr::null_mut(), &error);
        let mut slot = std::ptr::null_mut();
        super::set_error(&mut slot, &error);
        assert!(slot.is_null());
    }

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

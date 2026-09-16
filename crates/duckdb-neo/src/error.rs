//! Errors returned by DuckDB operations.
//!
//! Most fallible crate APIs return [`crate::Result`], whose [`Error`] preserves
//! the authoritative [`DuckDBError`] code from the C API together with its
//! human-readable message. Callers can inspect the code when they need to
//! distinguish error classes without matching message text.

use std::fmt;

use crate::ffi;

/// [`DuckDBError`] contains the error codes provided by the DuckDB C API.
pub type DuckDBError = ffi::DUCKDB_V2_ERROR;

/// [`Error`] is a representation of an error returned by the DuckDB C API.
///
/// It contains the raw error code and a human-readable message, if available.
///
/// This struct is used extensively in the crate to represent errors returned by the DuckDB C API.
#[derive(Debug, Clone)]
pub struct Error {
    /// The raw `DUCKDB_V2_ERROR`.
    pub code: DuckDBError,
    /// Human-readable message, empty if the API provided none.
    pub message: String,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.message.is_empty() {
            write!(f, "DuckDB error (code {:#x})", self.code as u32)
        } else {
            write!(f, "{} (code {:#x})", self.message, self.code as u32)
        }
    }
}

impl Error {
    /// Create an [`Error`] from a raw error code and an error handle.
    ///
    /// Internally this fetches the text corresponding to the error, if available.
    /// # Safety
    /// This function crosses the FFI boundary to fetch the text in the error handle.
    /// It is the caller's responsibility to ensure that the handle is valid.
    pub(crate) unsafe fn from_code_and_handle(code: DuckDBError, handle: ffi::duckdb_v2_error_info_handle) -> Self {
        match handle.is_null() {
            true => Error {
                code,
                message: "Unknown error".to_string(),
            },
            false => {
                let mut text = ffi::duckdb_v2_str::default();

                let message = if unsafe { ffi::duckdb_v2_error_info_get_text(handle, &mut text) }
                    == DuckDBError::DUCKDB_V2_ERROR_NONE
                    && !text.ptr.is_null()
                    && text.len > 0
                {
                    // Borrowed view, valid until we destroy the handle below — copy it out.
                    let bytes = unsafe { std::slice::from_raw_parts(text.ptr as *const u8, text.len as usize) };
                    String::from_utf8_lossy(bytes).into_owned()
                } else {
                    "Unknown error".to_string()
                };

                Error { code, message }
            }
        }
    }

    /// Create an API-classified error with a human-readable message.
    pub fn api_error(message: String) -> Self {
        Error {
            code: DuckDBError::DUCKDB_V2_ERROR_API,
            message,
        }
    }
}

impl std::error::Error for Error {}

/// [`check_api_call_no_err!`] is a macro that safely converts FFI calls into `Results`.
///
/// An arbitrary number of arguments can be passed to the macro.
/// This macro does not account for error handling, and its intended use is for FFI calls that can't fail.
///
/// # Usage
/// ```ignore
/// check_api_call_no_err!(ffi::duckdb_v2_scalar_function_builder_destroy, &mut handle)
///     .unwrap();
/// ```
macro_rules! check_api_call_no_err {
    ($call:expr $(, $arg:expr)*) => {
        {
            unsafe {
                let code = $call($($arg,)*);

                match code {
                    $crate::error::DuckDBError::DUCKDB_V2_ERROR_NONE => Ok(()),
                    _ => Err($crate::error::Error { code, message: String::new() }),
                }
            }
        }
    };
}

/// [`check_api_call!`] is a macro that safely converts FFI calls into `Results`.
///
/// An arbitrary number of arguments can be passed to the macro. The macro automatically appends the
/// trailing `&mut duckdb_v2_error_info_handle` argument.
///
/// The macro will automatically create a temporary error handle, pass it to the FFI call, and then check the return code.
/// If the return code indicates an error, the macro will convert the error handle into a Rust `Error` type and return it as an `Err` variant of a `Result`.
///
/// Passing the marker `RET` in place of an argument makes the macro allocate an out-parameter at
/// that position and return its value in the `Ok` variant.
///
/// # Usage
/// ```ignore
/// // Without an out-parameter, evaluates to `Result<()>`
/// check_api_call!(ffi::duckdb_v2_statement_add_collection, handle, name.into())?;
///
/// // With an out-parameter, evaluates to `Result<T>`
/// let handle = check_api_call!(ffi::duckdb_v2_create_environment, RET)?;
///
/// println!("Handle address: {:?}", handle);
/// ```
macro_rules! check_api_call {
    // Entry point
    ($call:expr $(, $($args:tt)*)?) => {
        $crate::check_api_call!(@before $call; (); $($($args)*)?)
    };

    // `ret` found, more args follow it
    (@before $call:expr; ($($b:expr),*); RET, $($after:expr),* $(,)?) => {
        $crate::check_api_call!(@finish_ret $call; ($($b),*); ($($after),*))
    };
    // `ret` found, nothing follows it
    (@before $call:expr; ($($b:expr),*); RET) => {
        $crate::check_api_call!(@finish_ret $call; ($($b),*); ())
    };
    // ordinary arg, more tokens follow
    (@before $call:expr; ($($b:expr),*); $head:expr, $($rest:tt)*) => {
        $crate::check_api_call!(@before $call; ($($b,)* $head); $($rest)*)
    };
    // ordinary arg, last token (no trailing comma)
    (@before $call:expr; ($($b:expr),*); $head:expr) => {
        $crate::check_api_call!(@before $call; ($($b,)* $head); )
    };
    // no more tokens, `ret` never seen
    (@before $call:expr; ($($b:expr),*); ) => {
        $crate::check_api_call!(@finish_noret $call; ($($b),*))
    };

    // No `ret` -> Ok(())
    (@finish_noret $call:expr; ($($b:expr),*)) => {{
        let mut err: $crate::ffi::duckdb_v2_error_info_handle = std::ptr::null_mut();
        unsafe {
            let code = $call($($b,)* &mut err);
            if code != $crate::error::DuckDBError::DUCKDB_V2_ERROR_NONE {
                let error = $crate::error::Error::from_code_and_handle(code, err);
                ffi::duckdb_v2_error_info_destroy(&mut err);
                Err(error)
            } else {
                Ok(())
            }
        }
    }};

    // `ret` present -> Ok(value)
    (@finish_ret $call:expr; ($($b:expr),*); ($($a:expr),*)) => {{
        let mut err: $crate::ffi::duckdb_v2_error_info_handle = std::ptr::null_mut();
        let mut __out = std::mem::MaybeUninit::uninit();
        unsafe {
            let code = $call($($b,)* __out.as_mut_ptr(), $($a,)* &mut err);
            if code != $crate::error::DuckDBError::DUCKDB_V2_ERROR_NONE {
                let error = $crate::error::Error::from_code_and_handle(code, err);
                $crate::ffi::duckdb_v2_error_info_destroy(&mut err);
                Err(error)
            } else {
                Ok(__out.assume_init())
            }
        }
    }};
}

pub(crate) use check_api_call;
pub(crate) use check_api_call_no_err;

#[cfg(test)]
mod tests {
    #[test]
    pub fn test_error_to_string() -> crate::Result<()> {
        let err = crate::error::Error {
            code: crate::error::DuckDBError::DUCKDB_V2_ERROR_API,
            message: "Test error".to_string(),
        };

        assert_eq!(format!("{}", err), "Test error (code 0x1)".to_string());
        Ok(())
    }
}

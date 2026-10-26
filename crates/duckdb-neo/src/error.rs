//! Errors returned by DuckDB operations.
//!
//! Most fallible crate APIs return [`crate::Result`], whose [`Error`] preserves
//! the authoritative [`DuckDBError`] code from the C API together with its
//! human-readable message. Callers can inspect the code when they need to
//! distinguish error classes without matching message text.

use std::fmt;

use crate::{builder_helpers::ffi_enum_redeclaration, ffi};

ffi_enum_redeclaration! {
    /// Error codes returned by the DuckDB C API.
    pub enum DuckDBError <- ffi::DUCKDB_V2_ERROR {
        /// The operation succeeded.
        None = DUCKDB_V2_ERROR_NONE,
        Api = DUCKDB_V2_ERROR_API,
        IoFileNotFound = DUCKDB_V2_ERROR_IO_FILE_NOT_FOUND,
        IoReadFailure = DUCKDB_V2_ERROR_IO_READ_FAILURE,
        IoEof = DUCKDB_V2_ERROR_IO_EOF,
        IoGeneral = DUCKDB_V2_ERROR_IO_GENERAL,
        IoNetwork = DUCKDB_V2_ERROR_IO_NETWORK,
        IoHttp = DUCKDB_V2_ERROR_IO_HTTP,
        InputInvalid = DUCKDB_V2_ERROR_INPUT_INVALID,
        InputParameterInvalid = DUCKDB_V2_ERROR_INPUT_PARAMETER_INVALID,
        InputOutOfRange = DUCKDB_V2_ERROR_INPUT_OUT_OF_RANGE,
        InputObjectSize = DUCKDB_V2_ERROR_INPUT_OBJECT_SIZE,
        /// Metadata is available only after stepping the expanding statement.
        InputMetadataDeferred = DUCKDB_V2_ERROR_INPUT_METADATA_DEFERRED,
        ResourceInUse = DUCKDB_V2_ERROR_RESOURCE_IN_USE,
        ResourceOutOfMemory = DUCKDB_V2_ERROR_RESOURCE_OUT_OF_MEMORY,
        ResourceConnection = DUCKDB_V2_ERROR_RESOURCE_CONNECTION,
        ResourceDependency = DUCKDB_V2_ERROR_RESOURCE_DEPENDENCY,
        ResourceMissingExtension = DUCKDB_V2_ERROR_RESOURCE_MISSING_EXTENSION,
        ResourceAutoload = DUCKDB_V2_ERROR_RESOURCE_AUTOLOAD,
        TypeConversion = DUCKDB_V2_ERROR_TYPE_CONVERSION,
        TypeUnknown = DUCKDB_V2_ERROR_TYPE_UNKNOWN,
        TypeInvalid = DUCKDB_V2_ERROR_TYPE_INVALID,
        TypeMismatch = DUCKDB_V2_ERROR_TYPE_MISMATCH,
        TypeDecimal = DUCKDB_V2_ERROR_TYPE_DECIMAL,
        TypeDivideByZero = DUCKDB_V2_ERROR_TYPE_DIVIDE_BY_ZERO,
        QueryParser = DUCKDB_V2_ERROR_QUERY_PARSER,
        QuerySyntax = DUCKDB_V2_ERROR_QUERY_SYNTAX,
        QueryBinder = DUCKDB_V2_ERROR_QUERY_BINDER,
        QueryPlanner = DUCKDB_V2_ERROR_QUERY_PLANNER,
        QueryOptimizer = DUCKDB_V2_ERROR_QUERY_OPTIMIZER,
        QueryExpression = DUCKDB_V2_ERROR_QUERY_EXPRESSION,
        QueryExecutor = DUCKDB_V2_ERROR_QUERY_EXECUTOR,
        QueryScheduler = DUCKDB_V2_ERROR_QUERY_SCHEDULER,
        QueryNotImplemented = DUCKDB_V2_ERROR_QUERY_NOT_IMPLEMENTED,
        QueryParameterNotResolved = DUCKDB_V2_ERROR_QUERY_PARAMETER_NOT_RESOLVED,
        QueryParameterNotAllowed = DUCKDB_V2_ERROR_QUERY_PARAMETER_NOT_ALLOWED,
        DatabaseCatalog = DUCKDB_V2_ERROR_DATABASE_CATALOG,
        DatabaseTransaction = DUCKDB_V2_ERROR_DATABASE_TRANSACTION,
        DatabaseConstraint = DUCKDB_V2_ERROR_DATABASE_CONSTRAINT,
        DatabaseIndex = DUCKDB_V2_ERROR_DATABASE_INDEX,
        DatabaseSequence = DUCKDB_V2_ERROR_DATABASE_SEQUENCE,
        DatabaseStatistics = DUCKDB_V2_ERROR_DATABASE_STATISTICS,
        DatabaseSerialization = DUCKDB_V2_ERROR_DATABASE_SERIALIZATION,
        ConfigurationSettings = DUCKDB_V2_ERROR_CONFIGURATION_SETTINGS,
        ConfigurationInvalid = DUCKDB_V2_ERROR_CONFIGURATION_INVALID,
        ConfigurationPermission = DUCKDB_V2_ERROR_CONFIGURATION_PERMISSION,
        RuntimeInternal = DUCKDB_V2_ERROR_RUNTIME_INTERNAL,
        RuntimeFatal = DUCKDB_V2_ERROR_RUNTIME_FATAL,
        RuntimeInterrupt = DUCKDB_V2_ERROR_RUNTIME_INTERRUPT,
        RuntimeNullPointer = DUCKDB_V2_ERROR_RUNTIME_NULL_POINTER,
    }
}

/// [`Error`] is a representation of an error returned by the DuckDB C API.
///
/// It contains the raw error code and a human-readable message, if available.
///
/// This struct is used extensively in the crate to represent errors returned by the DuckDB C API.
#[derive(Debug, Clone)]
pub struct Error {
    /// The error code returned by DuckDB.
    pub code: DuckDBError,
    /// Human-readable message, empty if the API provided none.
    pub message: String,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.message.is_empty() {
            write!(f, "DuckDB error (code {})", self.code as u32)
        } else {
            write!(f, "{} (code {})", self.message, self.code as u32)
        }
    }
}

impl Error {
    /// Copy an error's text, then destroy and clear its handle.
    ///
    /// # Safety
    /// `handle` must be null or an owned live error handle.
    pub(crate) unsafe fn from_code_and_handle(
        code: ffi::DUCKDB_V2_ERROR,
        handle: &mut ffi::duckdb_v2_error_info_handle,
    ) -> Self {
        let mut message = String::new();
        if !handle.is_null() {
            let mut text = ffi::duckdb_v2_str::default();
            // SAFETY: the caller supplies a live error handle.
            if unsafe { ffi::duckdb_v2_error_info_get_text(*handle, &mut text) }
                == ffi::DUCKDB_V2_ERROR::DUCKDB_V2_ERROR_NONE
                && !text.ptr.is_null()
                && text.len > 0
            {
                // SAFETY: the borrowed text stays valid until the handle is destroyed below.
                let bytes = unsafe { std::slice::from_raw_parts(text.ptr as *const u8, text.len as usize) };
                message = String::from_utf8_lossy(bytes).into_owned();
            }
            // SAFETY: the caller transfers ownership of this live handle.
            unsafe { ffi::duckdb_v2_error_info_destroy(handle) };
        }
        match code.try_into() {
            Ok(code) => Error { code, message },
            Err(error) => error,
        }
    }

    /// Create an API-classified error with a human-readable message.
    pub fn api_error(message: impl Into<String>) -> Self {
        Error {
            code: DuckDBError::Api,
            message: message.into(),
        }
    }

    /// Create an error for invalid caller-supplied input.
    pub fn invalid_input(message: impl Into<String>) -> Self {
        Error {
            code: DuckDBError::InputInvalid,
            message: message.into(),
        }
    }

    /// Create an error for an invalid caller-supplied parameter, such as an out-of-range index.
    pub fn invalid_parameter(message: impl Into<String>) -> Self {
        Error {
            code: DuckDBError::InputParameterInvalid,
            message: message.into(),
        }
    }

    /// Create an INTERNAL error that intentionally invalidates the instance.
    /// DuckDB describes this as an internal failure, including for callback panics.
    pub fn internal(message: impl Into<String>) -> Self {
        Error {
            code: DuckDBError::RuntimeInternal,
            message: message.into(),
        }
    }
}

impl std::error::Error for Error {}

/// [`check_api_call_no_err!`] is a macro that safely converts FFI calls into `Results`.
///
/// An arbitrary number of arguments can be passed to the macro.
/// This macro is intended for FFI calls with no error out-parameter.
///
/// # Usage
/// ```ignore
/// check_api_call_no_err!(ffi::duckdb_v2_environment_destroy, &mut handle)
///     .unwrap();
/// ```
macro_rules! check_api_call_no_err {
    ($call:expr $(, $arg:expr)*) => {
        {
            unsafe {
                let code = $call($($arg,)*);

                match code {
                    $crate::ffi::DUCKDB_V2_ERROR::DUCKDB_V2_ERROR_NONE => Ok(()),
                    _ => Err(match code.try_into() {
                        Ok(code) => $crate::error::Error { code, message: String::new() },
                        Err(error) => error,
                    }),
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
/// that position and return its value in the `Ok` variant. Use it only when the callee writes the
/// out-parameter on every successful return; otherwise pass an initialized local.
///
/// # Usage
/// ```ignore
/// // Without an out-parameter, evaluates to `Result<()>`
/// check_api_call!(ffi::duckdb_v2_statement_add_collection, handle, &name.into())?;
///
/// // With an out-parameter, evaluates to `Result<T>`
/// let handle = check_api_call!(ffi::duckdb_v2_environment_create, RET)?;
///
/// println!("Handle address: {:?}", handle);
/// ```
macro_rules! check_api_call {
    // Entry point
    ($call:expr $(, $($args:tt)*)?) => {
        $crate::check_api_call!(@before $call; (); $($($args)*)?)
    };

    // `RET` found, more args follow it
    (@before $call:expr; ($($b:expr),*); RET, $($after:expr),* $(,)?) => {
        $crate::check_api_call!(@finish_ret $call; ($($b),*); ($($after),*))
    };
    // `RET` found, nothing follows it
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
    // no more tokens, `RET` never seen
    (@before $call:expr; ($($b:expr),*); ) => {
        $crate::check_api_call!(@finish_noret $call; ($($b),*))
    };

    // No `RET` -> Ok(())
    (@finish_noret $call:expr; ($($b:expr),*)) => {{
        let mut err: $crate::ffi::duckdb_v2_error_info_handle = std::ptr::null_mut();
        unsafe {
            let code = $call($($b,)* &mut err);
            if code != $crate::ffi::DUCKDB_V2_ERROR::DUCKDB_V2_ERROR_NONE {
                let error = $crate::error::Error::from_code_and_handle(code, &mut err);
                Err(error)
            } else {
                Ok(())
            }
        }
    }};

    // `RET` present -> Ok(value)
    (@finish_ret $call:expr; ($($b:expr),*); ($($a:expr),*)) => {{
        let mut err: $crate::ffi::duckdb_v2_error_info_handle = std::ptr::null_mut();
        let mut __out = std::mem::MaybeUninit::uninit();
        unsafe {
            let code = $call($($b,)* __out.as_mut_ptr(), $($a,)* &mut err);
            if code != $crate::ffi::DUCKDB_V2_ERROR::DUCKDB_V2_ERROR_NONE {
                let error = $crate::error::Error::from_code_and_handle(code, &mut err);
                Err(error)
            } else {
                Ok(__out.assume_init())
            }
        }
    }};
}

/// Read a caller-buffer C API string using a sizing call followed by a write.
/// Appends `(buffer, capacity, out_length, err)`; the length excludes the NUL.
/// Evaluates inputs once and returns `Result<String>`.
///
/// ```ignore
/// check_api_call_string!(ffi::duckdb_v2_logical_type_to_text, self.handle)
/// ```
macro_rules! check_api_call_string {
    ($call:expr $(, $arg:expr)* $(,)?) => {{
        let call = $call;
        $crate::error::check_api_call_string!(@bind call; (); 0; $($arg),*)
    }};
    (@bind $call:ident; ($($args:ident,)*); $i:tt; $head:expr $(, $rest:expr)*) => {{
        paste::paste! {
            let [<arg $i>] = $head;
            $crate::error::check_api_call_string!(@bump $call; ($($args,)* [<arg $i>],); $i; $($rest),*)
        }
    }};
    (@bump $call:ident; ($($args:ident,)*); 0; $($rest:tt)*) => {
        $crate::error::check_api_call_string!(@bind $call; ($($args,)*); 1; $($rest)*)
    };
    (@bump $call:ident; ($($args:ident,)*); 1; $($rest:tt)*) => {
        $crate::error::check_api_call_string!(@bind $call; ($($args,)*); 2; $($rest)*)
    };
    (@bump $call:ident; ($($args:ident,)*); 2; $($rest:tt)*) => {
        $crate::error::check_api_call_string!(@bind $call; ($($args,)*); 3; $($rest)*)
    };
    (@bump $call:ident; ($($args:ident,)*); 3; $($rest:tt)*) => {
        $crate::error::check_api_call_string!(@bind $call; ($($args,)*); 4; $($rest)*)
    };
    (@bump $call:ident; ($($args:ident,)*); 4; $($rest:tt)*) => {
        $crate::error::check_api_call_string!(@bind $call; ($($args,)*); 5; $($rest)*)
    };
    (@bump $call:ident; ($($args:ident,)*); 5; $($rest:tt)*) => {
        $crate::error::check_api_call_string!(@bind $call; ($($args,)*); 6; $($rest)*)
    };
    (@bump $call:ident; ($($args:ident,)*); 6; $($rest:tt)*) => {
        $crate::error::check_api_call_string!(@bind $call; ($($args,)*); 7; $($rest)*)
    };
    (@bump $call:ident; ($($args:ident,)*); 7; $($rest:tt)*) => {
        $crate::error::check_api_call_string!(@bind $call; ($($args,)*); 8; $($rest)*)
    };
    (@bind $call:ident; ($($args:ident,)*); $i:tt; ) => {{
        (|| -> $crate::Result<String> {
            let length = $crate::check_api_call!($call, $($args,)* std::ptr::null_mut(), 0, RET)?;
            let capacity = usize::try_from(length)
                .ok()
                .and_then(|length| length.checked_add(1))
                .filter(|&capacity| capacity <= isize::MAX as usize)
                .ok_or_else(|| $crate::error::Error::api_error("String too large"))?;
            let mut text = vec![0u8; capacity];
            let written = $crate::check_api_call!(
                $call, $($args,)* text.as_mut_ptr().cast(), capacity as $crate::ffi::idx_t, RET
            )?;
            if written >= capacity as $crate::ffi::idx_t {
                return Err($crate::error::Error::api_error("Invalid string length"));
            }
            text.truncate(written as usize);
            String::from_utf8(text)
                .map_err(|_| $crate::error::Error::api_error("Invalid UTF-8"))
        })()
    }};
}

pub(crate) use check_api_call;
pub(crate) use check_api_call_no_err;
pub(crate) use check_api_call_string;

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use crate::{
        ToValue,
        environment::{Environment, StorageLocation},
        logical_type::LogicalType,
        qualified_name::QualifiedName,
    };

    #[test]
    fn test_string_renderers() -> crate::Result<()> {
        let env = Environment::new()?;
        let instance = env.open(StorageLocation::InMemory)?;
        let conn = instance.connect()?;
        let logical_type = LogicalType::from_text(&conn, "MAP(VARCHAR, INTEGER)")?;
        assert_eq!(logical_type.to_text()?, "MAP(VARCHAR, INTEGER)");
        for text in ["", "hello", "\u{e9}", &"long text".repeat(1024)] {
            assert_eq!(text.value(&conn)?.dbg_string()?, text);
        }
        assert_eq!(crate::render_identifier_quoted("hello")?, "hello");
        assert_eq!(crate::render_identifier_quoted("a\"b")?, "\"a\"\"b\"");
        let name = QualifiedName::from_parts(&["main", "a\"b"])?;
        assert_eq!(name.render()?, "main.\"a\"\"b\"");
        Ok(())
    }

    #[test]
    fn test_from_null_error_handle() {
        let mut handle = std::ptr::null_mut();
        // SAFETY: a null error handle is allowed.
        let error = unsafe { super::Error::from_code_and_handle(super::DuckDBError::Api.into(), &mut handle) };
        assert!(handle.is_null());
        assert!(error.message.is_empty());
        assert_eq!(error.to_string(), "DuckDB error (code 1)");
    }

    #[test]
    fn test_from_live_error_handle() -> crate::Result<()> {
        for message in ["error detail", ""] {
            let mut handle = std::ptr::null_mut();
            // SAFETY: the byte argument and output slot are valid for this call.
            let code = unsafe { crate::ffi::duckdb_v2_validate_utf8(&crate::ffi_str::bytes_arg(b"\xff"), &mut handle) };
            assert_ne!(code, super::DuckDBError::None.into());
            assert!(!handle.is_null());
            super::check_api_call_no_err!(crate::ffi::duckdb_v2_error_info_set_text, handle, &message.into())?;
            // SAFETY: this test owns the live error handle.
            let error = unsafe { super::Error::from_code_and_handle(code, &mut handle) };
            assert!(handle.is_null());
            assert_eq!(error.code, code.try_into()?);
            assert_eq!(error.message, message);
        }
        Ok(())
    }

    #[test]
    pub fn test_error_to_string() -> crate::Result<()> {
        let err = crate::error::Error {
            code: crate::error::DuckDBError::Api,
            message: "Test error".to_string(),
        };

        assert_eq!(format!("{}", err), "Test error (code 1)".to_string());
        Ok(())
    }
}

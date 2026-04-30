use crate::duckdb_state;
use std::{error, fmt};

/// Error Codes
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ErrorCode {
    /// Internal logic error in SQLite
    InternalMalfunction,
    /// Access permission denied
    PermissionDenied,
    /// Callback routine requested an abort
    OperationAborted,
    /// The database file is locked
    DatabaseBusy,
    /// A table in the database is locked
    DatabaseLocked,
    /// A malloc() failed
    OutOfMemory,
    /// Attempt to write a readonly database
    ReadOnly,
    /// Operation terminated by sqlite3_interrupt()
    OperationInterrupted,
    /// Some kind of disk I/O error occurred
    SystemIoFailure,
    /// The database disk image is malformed
    DatabaseCorrupt,
    /// Unknown opcode in sqlite3_file_control()
    NotFound,
    /// Insertion failed because database is full
    DiskFull,
    /// Unable to open the database file
    CannotOpen,
    /// Database lock protocol error
    FileLockingProtocolFailed,
    /// The database schema changed
    SchemaChanged,
    /// String or BLOB exceeds size limit
    TooBig,
    /// Abort due to constraint violation
    ConstraintViolation,
    /// Data type mismatch
    TypeMismatch,
    /// Library used incorrectly
    ApiMisuse,
    /// Uses OS features not supported on host
    NoLargeFileSupport,
    /// Authorization denied
    AuthorizationForStatementDenied,
    /// 2nd parameter to sqlite3_bind out of range
    ParameterOutOfRange,
    /// File opened that is not a database file
    NotADatabase,
    /// SQL error or missing database
    Unknown,
    /// HTTP error
    Http,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Error {
    pub code: ErrorCode,
    pub extended_code: duckdb_state,
}

impl Error {
    pub fn new(result_code: duckdb_state) -> Self {
        Self {
            code: ErrorCode::Unknown,
            extended_code: result_code,
        }
    }

    pub unsafe fn from_result(result: *mut crate::duckdb_result) -> Self {
       let raw_type = unsafe { crate::duckdb_result_error_type(result) };
       Self {
            code: ErrorCode::from_duckdb_error_type(raw_type),
            extended_code: crate::duckdb_state_DuckDBError,
       }
    }
}

impl ErrorCode {
    pub fn from_duckdb_error_type(raw: crate::duckdb_error_type) -> Self {
        match raw {
            crate::duckdb_error_type_DUCKDB_ERROR_INVALID => ErrorCode::Unknown,
            crate::duckdb_error_type_DUCKDB_ERROR_OUT_OF_RANGE => ErrorCode::ParameterOutOfRange,
            crate::duckdb_error_type_DUCKDB_ERROR_CONVERSION => ErrorCode::TypeMismatch,
            crate::duckdb_error_type_DUCKDB_ERROR_UNKNOWN_TYPE => ErrorCode::Unknown,
            crate::duckdb_error_type_DUCKDB_ERROR_DECIMAL => ErrorCode::TypeMismatch,
            crate::duckdb_error_type_DUCKDB_ERROR_MISMATCH_TYPE => ErrorCode::TypeMismatch,
            crate::duckdb_error_type_DUCKDB_ERROR_DIVIDE_BY_ZERO => ErrorCode::OperationAborted,
            crate::duckdb_error_type_DUCKDB_ERROR_OBJECT_SIZE => ErrorCode::TooBig,
            crate::duckdb_error_type_DUCKDB_ERROR_INVALID_TYPE => ErrorCode::TypeMismatch,
            crate::duckdb_error_type_DUCKDB_ERROR_SERIALIZATION => ErrorCode::DatabaseCorrupt,
            crate::duckdb_error_type_DUCKDB_ERROR_TRANSACTION => ErrorCode::OperationAborted,
            crate::duckdb_error_type_DUCKDB_ERROR_NOT_IMPLEMENTED => ErrorCode::ApiMisuse,
            crate::duckdb_error_type_DUCKDB_ERROR_EXPRESSION => ErrorCode::ApiMisuse,
            crate::duckdb_error_type_DUCKDB_ERROR_CATALOG => ErrorCode::NotFound,
            crate::duckdb_error_type_DUCKDB_ERROR_PARSER => ErrorCode::ApiMisuse,
            crate::duckdb_error_type_DUCKDB_ERROR_PLANNER => ErrorCode::ApiMisuse,
            crate::duckdb_error_type_DUCKDB_ERROR_SCHEDULER => ErrorCode::OperationAborted,
            crate::duckdb_error_type_DUCKDB_ERROR_EXECUTOR => ErrorCode::OperationAborted,
            crate::duckdb_error_type_DUCKDB_ERROR_CONSTRAINT => ErrorCode::ConstraintViolation,
            crate::duckdb_error_type_DUCKDB_ERROR_INDEX => ErrorCode::ApiMisuse,
            crate::duckdb_error_type_DUCKDB_ERROR_STAT => ErrorCode::SystemIoFailure,
            crate::duckdb_error_type_DUCKDB_ERROR_CONNECTION => ErrorCode::CannotOpen,
            crate::duckdb_error_type_DUCKDB_ERROR_SYNTAX => ErrorCode::ApiMisuse,
            crate::duckdb_error_type_DUCKDB_ERROR_SETTINGS => ErrorCode::ApiMisuse,
            crate::duckdb_error_type_DUCKDB_ERROR_BINDER => ErrorCode::ApiMisuse,
            crate::duckdb_error_type_DUCKDB_ERROR_NETWORK => ErrorCode::SystemIoFailure,
            crate::duckdb_error_type_DUCKDB_ERROR_OPTIMIZER => ErrorCode::InternalMalfunction,
            crate::duckdb_error_type_DUCKDB_ERROR_NULL_POINTER => ErrorCode::InternalMalfunction,
            crate::duckdb_error_type_DUCKDB_ERROR_IO => ErrorCode::SystemIoFailure,
            crate::duckdb_error_type_DUCKDB_ERROR_INTERRUPT => ErrorCode::OperationInterrupted,
            crate::duckdb_error_type_DUCKDB_ERROR_FATAL => ErrorCode::InternalMalfunction,
            crate::duckdb_error_type_DUCKDB_ERROR_INTERNAL => ErrorCode::InternalMalfunction,
            crate::duckdb_error_type_DUCKDB_ERROR_INVALID_INPUT => ErrorCode::ApiMisuse,
            crate::duckdb_error_type_DUCKDB_ERROR_OUT_OF_MEMORY => ErrorCode::OutOfMemory,
            crate::duckdb_error_type_DUCKDB_ERROR_PERMISSION => ErrorCode::PermissionDenied,
            crate::duckdb_error_type_DUCKDB_ERROR_PARAMETER_NOT_RESOLVED => ErrorCode::ApiMisuse,
            crate::duckdb_error_type_DUCKDB_ERROR_PARAMETER_NOT_ALLOWED => ErrorCode::ApiMisuse,
            crate::duckdb_error_type_DUCKDB_ERROR_DEPENDENCY => ErrorCode::ConstraintViolation,
            crate::duckdb_error_type_DUCKDB_ERROR_HTTP => ErrorCode::Http,
            crate::duckdb_error_type_DUCKDB_ERROR_MISSING_EXTENSION => ErrorCode::NotFound,
            crate::duckdb_error_type_DUCKDB_ERROR_AUTOLOAD => ErrorCode::NotFound,
            crate::duckdb_error_type_DUCKDB_ERROR_SEQUENCE => ErrorCode::OperationAborted,
            crate:: duckdb_error_type_DUCKDB_INVALID_CONFIGURATION => ErrorCode::ApiMisuse,
            _ => ErrorCode::Unknown,
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Error code {}: {}",
            self.extended_code,
            code_to_str(self.extended_code)
        )
    }
}

impl error::Error for Error {
    fn description(&self) -> &str {
        code_to_str(self.extended_code)
    }
}

pub fn code_to_str(_: duckdb_state) -> &'static str {
    "Unknown error code"
}

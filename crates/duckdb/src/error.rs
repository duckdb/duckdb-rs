use arrow::datatypes::DataType;

use super::Result;
use crate::{
    ffi,
    types::{FromSqlError, Type},
};
use std::{error, ffi::CStr, fmt, path::PathBuf, str};

/// Enum listing possible errors from duckdb.
#[derive(Debug)]
#[allow(clippy::enum_variant_names)]
#[non_exhaustive]
pub enum Error {
    /// An error from an underlying DuckDB call.
    DuckDBFailure(ffi::Error, Option<String>),

    /// Error when the value of a particular column is requested, but it cannot
    /// be converted to the requested Rust type.
    FromSqlConversionFailure(usize, Type, Box<dyn error::Error + Send + Sync + 'static>),

    /// Error when DuckDB gives us an integral value outside the range of the
    /// requested type (e.g., trying to get the value 1000 into a `u8`).
    /// The associated `usize` is the column index,
    /// and the associated `i128` is the value returned by DuckDB.
    IntegralValueOutOfRange(usize, i128),

    /// Error when DuckDB gives us an unsigned integral value outside the range
    /// of the requested type. The associated `usize` is the column index, and
    /// the associated `u128` is the value returned by DuckDB.
    UnsignedIntegralValueOutOfRange(usize, u128),

    /// Error converting a string to UTF-8.
    Utf8Error(str::Utf8Error),

    /// Error converting a string to a C-compatible string because it contained
    /// an embedded nul.
    NulError(::std::ffi::NulError),

    /// Error when using SQL named parameters and passing a parameter name not
    /// present in the SQL.
    InvalidParameterName(String),

    /// Error converting a file path to a string.
    InvalidPath(PathBuf),

    /// Error returned when an [`execute`](crate::Connection::execute) call
    /// returns rows.
    ExecuteReturnedResults,

    /// Error when a query that was expected to return at least one row (e.g.,
    /// for [`query_row`](crate::Connection::query_row)) did not return any.
    QueryReturnedNoRows,

    /// Error when a query that was expected to return only one row (e.g.,
    /// for [`query_one`](crate::Connection::query_one)) did return more than one.
    QueryReturnedMoreThanOneRow,

    /// Error when the value of a particular column is requested, but the index
    /// is out of range for the statement.
    InvalidColumnIndex(usize),

    /// Error when the value of a named column is requested, but no column
    /// matches the name for the statement.
    InvalidColumnName(String),

    /// Error when the value of a particular column is requested, but the type
    /// of the result in that column cannot be converted to the requested
    /// Rust type.
    InvalidColumnType(usize, String, Type),

    /// Error when datatype to duckdb type
    ArrowTypeToDuckdbType(String, DataType),

    /// Error when a query that was expected to insert one row did not insert
    /// any or insert many.
    StatementChangedRows(usize),

    /// Error available for the implementors of the
    /// [`ToSql`](crate::types::ToSql) trait.
    ToSqlConversionFailure(Box<dyn error::Error + Send + Sync + 'static>),

    /// Error when the SQL is not a `SELECT`, is not read-only.
    InvalidQuery,

    /// Error when the SQL contains multiple statements.
    MultipleStatement,

    /// Error when the number of bound parameters does not match the number of
    /// parameters in the query. The first `usize` is how many parameters were
    /// given, the 2nd is how many were expected.
    InvalidParameterCount(usize, usize),

    /// Error when a parameter is requested, but the index is out of range
    /// for the statement.
    InvalidParameterIndex(usize),

    /// Append Error
    AppendError,
}

impl PartialEq for Error {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::DuckDBFailure(e1, s1), Self::DuckDBFailure(e2, s2)) => e1 == e2 && s1 == s2,
            (Self::IntegralValueOutOfRange(i1, n1), Self::IntegralValueOutOfRange(i2, n2)) => i1 == i2 && n1 == n2,
            (Self::UnsignedIntegralValueOutOfRange(i1, n1), Self::UnsignedIntegralValueOutOfRange(i2, n2)) => {
                i1 == i2 && n1 == n2
            }
            (Self::Utf8Error(e1), Self::Utf8Error(e2)) => e1 == e2,
            (Self::NulError(e1), Self::NulError(e2)) => e1 == e2,
            (Self::InvalidParameterName(n1), Self::InvalidParameterName(n2)) => n1 == n2,
            (Self::InvalidPath(p1), Self::InvalidPath(p2)) => p1 == p2,
            (Self::ExecuteReturnedResults, Self::ExecuteReturnedResults) => true,
            (Self::QueryReturnedNoRows, Self::QueryReturnedNoRows) => true,
            (Self::QueryReturnedMoreThanOneRow, Self::QueryReturnedMoreThanOneRow) => true,
            (Self::InvalidColumnIndex(i1), Self::InvalidColumnIndex(i2)) => i1 == i2,
            (Self::InvalidColumnName(n1), Self::InvalidColumnName(n2)) => n1 == n2,
            (Self::InvalidColumnType(i1, n1, t1), Self::InvalidColumnType(i2, n2, t2)) => {
                i1 == i2 && t1 == t2 && n1 == n2
            }
            (Self::StatementChangedRows(n1), Self::StatementChangedRows(n2)) => n1 == n2,
            (Self::InvalidParameterCount(i1, n1), Self::InvalidParameterCount(i2, n2)) => i1 == i2 && n1 == n2,
            (Self::InvalidParameterIndex(i1), Self::InvalidParameterIndex(i2)) => i1 == i2,
            (..) => false,
        }
    }
}

impl From<str::Utf8Error> for Error {
    #[cold]
    fn from(err: str::Utf8Error) -> Self {
        Self::Utf8Error(err)
    }
}

impl From<::std::ffi::NulError> for Error {
    #[cold]
    fn from(err: ::std::ffi::NulError) -> Self {
        Self::NulError(err)
    }
}

const UNKNOWN_COLUMN: usize = usize::MAX;

/// The conversion isn't precise, but it's convenient to have it
/// to allow use of `get_raw(…).as_…()?` in callbacks that take `Error`.
impl From<FromSqlError> for Error {
    #[cold]
    fn from(err: FromSqlError) -> Self {
        // The error type requires index and type fields, but they aren't known in this
        // context.
        match err {
            FromSqlError::OutOfRange(val) => Self::IntegralValueOutOfRange(UNKNOWN_COLUMN, val),
            FromSqlError::OutOfRangeUnsigned(val) => Self::UnsignedIntegralValueOutOfRange(UNKNOWN_COLUMN, val),
            #[cfg(feature = "uuid")]
            FromSqlError::InvalidUuidSize(_) => {
                Self::FromSqlConversionFailure(UNKNOWN_COLUMN, Type::Blob, Box::new(err))
            }
            FromSqlError::Other(source) => Self::FromSqlConversionFailure(UNKNOWN_COLUMN, Type::Null, source),
            _ => Self::FromSqlConversionFailure(UNKNOWN_COLUMN, Type::Null, Box::new(err)),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Self::DuckDBFailure(ref err, None) => err.fmt(f),
            Self::DuckDBFailure(_, Some(ref s)) => write!(f, "{s}"),
            Self::FromSqlConversionFailure(i, ref t, ref err) => {
                if i != UNKNOWN_COLUMN {
                    write!(f, "Conversion error from type {t} at index: {i}, {err}")
                } else {
                    err.fmt(f)
                }
            }
            Self::IntegralValueOutOfRange(col, val) => {
                if col != UNKNOWN_COLUMN {
                    write!(f, "Integer {val} out of range at index {col}")
                } else {
                    write!(f, "Integer {val} out of range")
                }
            }
            Self::UnsignedIntegralValueOutOfRange(col, val) => {
                if col != UNKNOWN_COLUMN {
                    write!(f, "Unsigned integer {val} out of range at index {col}")
                } else {
                    write!(f, "Unsigned integer {val} out of range")
                }
            }
            Self::Utf8Error(ref err) => err.fmt(f),
            Self::NulError(ref err) => err.fmt(f),
            Self::InvalidParameterName(ref name) => write!(f, "Invalid parameter name: {name}"),
            Self::InvalidPath(ref p) => write!(f, "Invalid path: {}", p.to_string_lossy()),
            Self::ExecuteReturnedResults => {
                write!(f, "Execute returned results - did you mean to call query?")
            }
            Self::QueryReturnedNoRows => write!(f, "Query returned no rows"),
            Self::QueryReturnedMoreThanOneRow => write!(f, "Query returned more than one row"),
            Self::InvalidColumnIndex(i) => write!(f, "Invalid column index: {i}"),
            Self::InvalidColumnName(ref name) => write!(f, "Invalid column name: {name}"),
            Self::InvalidColumnType(i, ref name, ref t) => {
                write!(f, "Invalid column type {t} at index: {i}, name: {name}")
            }
            Self::ArrowTypeToDuckdbType(ref name, ref t) => {
                write!(f, "Invalid column type {t} , name: {name}")
            }
            Self::InvalidParameterCount(i1, n1) => {
                write!(f, "Wrong number of parameters passed to query. Got {i1}, needed {n1}")
            }
            Self::InvalidParameterIndex(i) => write!(f, "Invalid parameter index: {i}"),
            Self::StatementChangedRows(i) => write!(f, "Query changed {i} rows"),
            Self::ToSqlConversionFailure(ref err) => err.fmt(f),
            Self::InvalidQuery => write!(f, "Query is not read-only"),
            Self::MultipleStatement => write!(f, "Multiple statements provided"),
            Self::AppendError => write!(f, "Append error"),
        }
    }
}

impl error::Error for Error {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match *self {
            Self::DuckDBFailure(ref err, _) => Some(err),
            Self::Utf8Error(ref err) => Some(err),
            Self::NulError(ref err) => Some(err),

            Self::IntegralValueOutOfRange(..)
            | Self::UnsignedIntegralValueOutOfRange(..)
            | Self::InvalidParameterName(_)
            | Self::ExecuteReturnedResults
            | Self::QueryReturnedNoRows
            | Self::QueryReturnedMoreThanOneRow
            | Self::InvalidColumnIndex(_)
            | Self::InvalidColumnName(_)
            | Self::InvalidColumnType(..)
            | Self::InvalidPath(_)
            | Self::InvalidParameterCount(..)
            | Self::InvalidParameterIndex(_)
            | Self::StatementChangedRows(_)
            | Self::InvalidQuery
            | Self::AppendError
            | Self::ArrowTypeToDuckdbType(..)
            | Self::MultipleStatement => None,
            Self::FromSqlConversionFailure(_, _, ref err) | Self::ToSqlConversionFailure(ref err) => Some(&**err),
        }
    }
}

// These are public but not re-exported by lib.rs, so only visible within crate.

#[inline]
fn result_from_duckdb_state(code: ffi::duckdb_state, message: Option<String>) -> Result<()> {
    Err(duckdb_failure_from_state(code, message))
}

#[inline]
pub(crate) fn duckdb_failure_from_state(code: ffi::duckdb_state, message: Option<String>) -> Error {
    Error::DuckDBFailure(ffi::Error::new(code), message)
}

#[inline]
pub(crate) fn duckdb_failure_from_message(message: impl Into<String>) -> Error {
    duckdb_failure_from_state(ffi::DuckDBError, Some(message.into()))
}

#[inline]
pub(crate) fn arrow_conversion_failure(context: &str, err: impl fmt::Display) -> Error {
    duckdb_failure_from_message(format!("{context}: {err}"))
}

#[inline]
fn appender_error_message(appender: ffi::duckdb_appender) -> Option<String> {
    if appender.is_null() {
        return Some("appender is null".to_string());
    }
    unsafe {
        let c_err = ffi::duckdb_appender_error(appender);
        if c_err.is_null() {
            None
        } else {
            Some(CStr::from_ptr(c_err).to_string_lossy().into_owned())
        }
    }
}

#[inline]
pub(crate) fn error_from_appender_code(code: ffi::duckdb_state, appender: ffi::duckdb_appender) -> Error {
    Error::DuckDBFailure(ffi::Error::new(code), appender_error_message(appender))
}

#[cold]
#[inline]
pub fn result_from_duckdb_appender(code: ffi::duckdb_state, appender: *mut ffi::duckdb_appender) -> Result<()> {
    if code == ffi::DuckDBSuccess {
        return Ok(());
    }
    unsafe {
        let message = appender_error_message(*appender);
        if !(*appender).is_null() {
            ffi::duckdb_appender_destroy(appender);
        }
        result_from_duckdb_state(code, message)
    }
}

#[cold]
#[inline]
pub fn result_from_duckdb_prepare(code: ffi::duckdb_state, mut prepare: ffi::duckdb_prepared_statement) -> Result<()> {
    if code == ffi::DuckDBSuccess {
        return Ok(());
    }
    unsafe {
        let message = if prepare.is_null() {
            Some("prepare is null".to_string())
        } else {
            let c_err = ffi::duckdb_prepare_error(prepare);
            let message = Some(CStr::from_ptr(c_err).to_string_lossy().to_string());
            ffi::duckdb_destroy_prepare(&mut prepare);
            message
        };
        result_from_duckdb_state(code, message)
    }
}

#[cold]
#[inline]
pub fn result_from_duckdb_result(code: ffi::duckdb_state, result: *mut ffi::duckdb_result) -> Result<()> {
    if code == ffi::DuckDBSuccess {
        return Ok(());
    }
    unsafe {
        let message = if result.is_null() {
            Some("result is null".to_string())
        } else {
            result_error_message(result)
        };
        if !result.is_null() {
            ffi::duckdb_destroy_result(result);
        }
        result_from_duckdb_state(code, message)
    }
}

/// Returns the current DuckDB error message for a result handle.
///
/// # Safety
///
/// `result` must be a live DuckDB result handle.
pub(crate) unsafe fn result_error_message(result: *mut ffi::duckdb_result) -> Option<String> {
    unsafe {
        debug_assert!(!result.is_null());
        let c_err = ffi::duckdb_result_error(result);
        if c_err.is_null() {
            None
        } else {
            Some(CStr::from_ptr(c_err).to_string_lossy().to_string())
        }
    }
}

/// Converts a DuckDB error-data handle into this crate's `Result`.
///
/// # Safety
///
/// `error_data` must be null or a valid handle allocated by DuckDB. This
/// function takes ownership of non-null handles and always destroys them before
/// returning.
#[inline]
pub(crate) unsafe fn result_from_duckdb_error_data(mut error_data: ffi::duckdb_error_data) -> Result<()> {
    unsafe {
        if error_data.is_null() {
            return Ok(());
        }
        if !ffi::duckdb_error_data_has_error(error_data) {
            ffi::duckdb_destroy_error_data(&mut error_data);
            return Ok(());
        }

        let message = {
            let c_err = ffi::duckdb_error_data_message(error_data);
            if c_err.is_null() {
                None
            } else {
                Some(CStr::from_ptr(c_err).to_string_lossy().to_string())
            }
        };
        ffi::duckdb_destroy_error_data(&mut error_data);
        result_from_duckdb_state(ffi::DuckDBError, message)
    }
}

#[cold]
#[inline]
pub fn result_from_duckdb_extract(
    num_statements: ffi::idx_t,
    mut extracted: ffi::duckdb_extracted_statements,
) -> Result<()> {
    if num_statements > 0 {
        return Ok(());
    }
    unsafe {
        let message = if extracted.is_null() {
            Some("extracted statements are null".to_string())
        } else {
            let c_err = ffi::duckdb_extract_statements_error(extracted);
            let message = if c_err.is_null() {
                None
            } else {
                Some(CStr::from_ptr(c_err).to_string_lossy().to_string())
            };
            ffi::duckdb_destroy_extracted(&mut extracted);
            message
        };
        result_from_duckdb_state(ffi::DuckDBError, message)
    }
}

#[cfg(test)]
mod tests {
    use std::{ffi::CString, ptr};

    use super::{Error, result_from_duckdb_error_data};
    use crate::ffi;

    #[test]
    fn result_from_duckdb_error_data_accepts_null() {
        unsafe {
            result_from_duckdb_error_data(ptr::null_mut()).unwrap();
        }
    }

    #[test]
    fn result_from_duckdb_error_data_accepts_no_error_handle() {
        unsafe {
            let mut db = ptr::null_mut();
            assert_eq!(ffi::DuckDBSuccess, ffi::duckdb_open(ptr::null(), &mut db));

            let mut con = ptr::null_mut();
            assert_eq!(ffi::DuckDBSuccess, ffi::duckdb_connect(db, &mut con));

            let sql = CString::new("CREATE TABLE t(i INTEGER)").unwrap();
            let mut result: ffi::duckdb_result = std::mem::zeroed();
            assert_eq!(ffi::DuckDBSuccess, ffi::duckdb_query(con, sql.as_ptr(), &mut result));
            ffi::duckdb_destroy_result(&mut result);

            let table = CString::new("t").unwrap();
            let mut appender = ptr::null_mut();
            assert_eq!(
                ffi::DuckDBSuccess,
                ffi::duckdb_appender_create(con, ptr::null(), table.as_ptr(), &mut appender)
            );

            let error_data = ffi::duckdb_appender_error_data(appender);
            assert!(!error_data.is_null());
            assert!(!ffi::duckdb_error_data_has_error(error_data));

            result_from_duckdb_error_data(error_data).unwrap();

            assert_eq!(ffi::DuckDBSuccess, ffi::duckdb_appender_destroy(&mut appender));
            ffi::duckdb_disconnect(&mut con);
            ffi::duckdb_close(&mut db);
        }
    }

    #[test]
    fn result_from_duckdb_error_data_returns_error_message() {
        let message = CString::new("synthetic error").unwrap();
        let error_data =
            unsafe { ffi::duckdb_create_error_data(ffi::duckdb_error_type_DUCKDB_ERROR_INTERNAL, message.as_ptr()) };

        let err = unsafe { result_from_duckdb_error_data(error_data) }.unwrap_err();

        match err {
            Error::DuckDBFailure(_, Some(message)) => assert_eq!(message, "synthetic error"),
            err => panic!("unexpected error: {err:?}"),
        }
    }
}

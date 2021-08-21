use super::Result;
use crate::ffi;
use crate::types::FromSqlError;
use crate::types::Type;
use std::error;
use std::ffi::CStr;
use std::fmt;
use std::os::raw::{c_uint, c_void};
use std::path::PathBuf;
use std::str;

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
    /// and the associated `i64` is the value returned by SQLite.
    IntegralValueOutOfRange(usize, i128),

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
}

impl PartialEq for Error {
    fn eq(&self, other: &Error) -> bool {
        match (self, other) {
            (Error::DuckDBFailure(e1, s1), Error::DuckDBFailure(e2, s2)) => e1 == e2 && s1 == s2,
            (Error::IntegralValueOutOfRange(i1, n1), Error::IntegralValueOutOfRange(i2, n2)) => i1 == i2 && n1 == n2,
            (Error::Utf8Error(e1), Error::Utf8Error(e2)) => e1 == e2,
            (Error::NulError(e1), Error::NulError(e2)) => e1 == e2,
            (Error::InvalidParameterName(n1), Error::InvalidParameterName(n2)) => n1 == n2,
            (Error::InvalidPath(p1), Error::InvalidPath(p2)) => p1 == p2,
            (Error::ExecuteReturnedResults, Error::ExecuteReturnedResults) => true,
            (Error::QueryReturnedNoRows, Error::QueryReturnedNoRows) => true,
            (Error::InvalidColumnIndex(i1), Error::InvalidColumnIndex(i2)) => i1 == i2,
            (Error::InvalidColumnName(n1), Error::InvalidColumnName(n2)) => n1 == n2,
            (Error::InvalidColumnType(i1, n1, t1), Error::InvalidColumnType(i2, n2, t2)) => {
                i1 == i2 && t1 == t2 && n1 == n2
            }
            (Error::StatementChangedRows(n1), Error::StatementChangedRows(n2)) => n1 == n2,
            (Error::InvalidParameterCount(i1, n1), Error::InvalidParameterCount(i2, n2)) => i1 == i2 && n1 == n2,
            (..) => false,
        }
    }
}

impl From<str::Utf8Error> for Error {
    #[cold]
    fn from(err: str::Utf8Error) -> Error {
        Error::Utf8Error(err)
    }
}

impl From<::std::ffi::NulError> for Error {
    #[cold]
    fn from(err: ::std::ffi::NulError) -> Error {
        Error::NulError(err)
    }
}

const UNKNOWN_COLUMN: usize = std::usize::MAX;

/// The conversion isn't precise, but it's convenient to have it
/// to allow use of `get_raw(…).as_…()?` in callbacks that take `Error`.
impl From<FromSqlError> for Error {
    #[cold]
    fn from(err: FromSqlError) -> Error {
        // The error type requires index and type fields, but they aren't known in this
        // context.
        match err {
            FromSqlError::OutOfRange(val) => Error::IntegralValueOutOfRange(UNKNOWN_COLUMN, val),
            #[cfg(feature = "uuid")]
            FromSqlError::InvalidUuidSize(_) => {
                Error::FromSqlConversionFailure(UNKNOWN_COLUMN, Type::Blob, Box::new(err))
            }
            FromSqlError::Other(source) => Error::FromSqlConversionFailure(UNKNOWN_COLUMN, Type::Null, source),
            _ => Error::FromSqlConversionFailure(UNKNOWN_COLUMN, Type::Null, Box::new(err)),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Error::DuckDBFailure(ref err, None) => err.fmt(f),
            Error::DuckDBFailure(_, Some(ref s)) => write!(f, "{}", s),
            Error::FromSqlConversionFailure(i, ref t, ref err) => {
                if i != UNKNOWN_COLUMN {
                    write!(f, "Conversion error from type {} at index: {}, {}", t, i, err)
                } else {
                    err.fmt(f)
                }
            }
            Error::IntegralValueOutOfRange(col, val) => {
                if col != UNKNOWN_COLUMN {
                    write!(f, "Integer {} out of range at index {}", val, col)
                } else {
                    write!(f, "Integer {} out of range", val)
                }
            }
            Error::Utf8Error(ref err) => err.fmt(f),
            Error::NulError(ref err) => err.fmt(f),
            Error::InvalidParameterName(ref name) => write!(f, "Invalid parameter name: {}", name),
            Error::InvalidPath(ref p) => write!(f, "Invalid path: {}", p.to_string_lossy()),
            Error::ExecuteReturnedResults => {
                write!(f, "Execute returned results - did you mean to call query?")
            }
            Error::QueryReturnedNoRows => write!(f, "Query returned no rows"),
            Error::InvalidColumnIndex(i) => write!(f, "Invalid column index: {}", i),
            Error::InvalidColumnName(ref name) => write!(f, "Invalid column name: {}", name),
            Error::InvalidColumnType(i, ref name, ref t) => {
                write!(f, "Invalid column type {} at index: {}, name: {}", t, i, name)
            }
            Error::InvalidParameterCount(i1, n1) => write!(
                f,
                "Wrong number of parameters passed to query. Got {}, needed {}",
                i1, n1
            ),
            Error::StatementChangedRows(i) => write!(f, "Query changed {} rows", i),
            Error::ToSqlConversionFailure(ref err) => err.fmt(f),
            Error::InvalidQuery => write!(f, "Query is not read-only"),
            Error::MultipleStatement => write!(f, "Multiple statements provided"),
        }
    }
}

impl error::Error for Error {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match *self {
            Error::DuckDBFailure(ref err, _) => Some(err),
            Error::Utf8Error(ref err) => Some(err),
            Error::NulError(ref err) => Some(err),

            Error::IntegralValueOutOfRange(..)
            | Error::InvalidParameterName(_)
            | Error::ExecuteReturnedResults
            | Error::QueryReturnedNoRows
            | Error::InvalidColumnIndex(_)
            | Error::InvalidColumnName(_)
            | Error::InvalidColumnType(..)
            | Error::InvalidPath(_)
            | Error::InvalidParameterCount(..)
            | Error::StatementChangedRows(_)
            | Error::InvalidQuery
            | Error::MultipleStatement => None,
            Error::FromSqlConversionFailure(_, _, ref err) | Error::ToSqlConversionFailure(ref err) => Some(&**err),
        }
    }
}

// These are public but not re-exported by lib.rs, so only visible within crate.

#[inline]
fn error_from_duckdb_code(code: c_uint, message: Option<String>) -> Result<()> {
    Err(Error::DuckDBFailure(ffi::Error::new(code as u32), message))
}

#[cold]
#[inline]
pub fn result_from_duckdb_appender(code: c_uint, mut appender: ffi::duckdb_appender) -> Result<()> {
    if code == ffi::DuckDBSuccess {
        return Ok(());
    }
    unsafe {
        let message = if appender.is_null() {
            Some("appender is null".to_string())
        } else {
            let c_err = ffi::duckdb_appender_error(appender);
            let message = Some(CStr::from_ptr(c_err).to_string_lossy().to_string());
            ffi::duckdb_free(c_err as *mut c_void);
            ffi::duckdb_appender_destroy(&mut appender);
            message
        };
        error_from_duckdb_code(code, message)
    }
}

#[cold]
#[inline]
pub fn result_from_duckdb_prepare(code: c_uint, mut prepare: ffi::duckdb_prepared_statement) -> Result<()> {
    if code == ffi::DuckDBSuccess {
        return Ok(());
    }
    unsafe {
        let message = if prepare.is_null() {
            Some("prepare is null".to_string())
        } else {
            let c_err = ffi::duckdb_prepare_error(prepare);
            let message = Some(CStr::from_ptr(c_err).to_string_lossy().to_string());
            // TODO: wait for https://github.com/duckdb/duckdb/issues/2170
            // ffi::duckdb_free(c_err as *mut c_void);
            ffi::duckdb_destroy_prepare(&mut prepare);
            message
        };
        error_from_duckdb_code(code, message)
    }
}

#[cold]
#[inline]
pub fn result_from_duckdb_arrow(code: c_uint, mut out: ffi::duckdb_arrow) -> Result<()> {
    if code == ffi::DuckDBSuccess {
        return Ok(());
    }
    unsafe {
        let message = if out.is_null() {
            Some("out is null".to_string())
        } else {
            let c_err = ffi::duckdb_query_arrow_error(out);
            let message = Some(CStr::from_ptr(c_err).to_string_lossy().to_string());
            ffi::duckdb_free(c_err as *mut c_void);
            ffi::duckdb_destroy_arrow(&mut out);
            message
        };
        error_from_duckdb_code(code, message)
    }
}

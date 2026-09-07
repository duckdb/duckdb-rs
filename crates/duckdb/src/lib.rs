#![doc(
    html_logo_url = "https://upload.wikimedia.org/wikipedia/commons/4/40/DuckDB_logo.svg?utm_source=commons.wikimedia.org&utm_campaign=index&utm_content=original"
)]
//! Safe Rust bindings for [DuckDB's](https://duckdb.org/) C API V2.
//!
//! Use this crate to embed DuckDB in a Rust application, open databases, execute
//! SQL, bind values, and consume columnar results through type-safe handles.
//!
//! # Example
//!
//! ```
//! use duckdb_rs::{
//!     Parameters,
//!     environment::{Environment, StorageLocation},
//! };
//!
//! # fn main() -> duckdb_rs::Result<()> {
//! let environment = Environment::new()?;
//! let database = environment.open(StorageLocation::InMemory)?;
//! let connection = database.connect()?;
//!
//! let mut result = connection.query("SELECT $1::INTEGER", Parameters::positional(&[&42]))?;
//! let chunk = result.next().transpose()?.expect("query returned no rows");
//! let values = chunk.get_vector_at::<i32>(0)?;
//!
//! assert_eq!(values.get(0)?, Some(&42));
//! # Ok(())
//! # }
//! ```

use libduckdb_sys as ffi;

mod builder_helpers;
pub mod connection_options;

pub(crate) mod bytes;
pub mod connection;
pub mod data_chunk;
pub mod database;
pub mod environment;
pub mod error;
pub mod logical_type;
pub(crate) mod parameter;
pub mod query_result;
pub mod schema;
pub mod statement;
pub mod types;
pub mod value;
pub mod vector;
use crate::error::{Error, check_api_call, check_api_call_no_err};
pub use bytes::DuckDBBytes;
pub use parameter::{Parameters, QueryParameter};
pub use types::{DuckDBType, FromValue, ToValue};

#[cfg(feature = "capi-v2-p2")]
pub mod aggregate;
#[cfg(feature = "capi-v2-p2")]
pub mod arrow;
#[cfg(feature = "capi-v2-p2")]
pub mod bind_arguments;
#[cfg(feature = "capi-v2-p2")]
pub mod cast;
#[cfg(feature = "capi-v2-p2")]
pub mod column_data_collection;
#[cfg(feature = "capi-v2-p2")]
pub mod copy_function;
#[cfg(feature = "capi-v2-p2")]
pub mod custom_type;
#[cfg(feature = "capi-v2-p2")]
pub mod enums;
#[cfg(feature = "capi-v2-p2")]
pub mod expression;
#[cfg(feature = "capi-v2-p2")]
pub mod file;
#[cfg(feature = "capi-v2-p2")]
pub mod log;
#[cfg(feature = "capi-v2-p2")]
pub mod qualified_name;
#[cfg(feature = "capi-v2-p2")]
pub mod query_progress;
#[cfg(feature = "capi-v2-p2")]
pub mod replacement_scan;
#[cfg(feature = "capi-v2-p2")]
pub mod scalar;
#[cfg(feature = "capi-v2-p2")]
pub mod signature;
#[cfg(feature = "capi-v2-p2")]
pub mod table_description;
#[cfg(feature = "capi-v2-p2")]
pub mod table_function;

/// This result type is used extensively throughout the crate to represent the result of (FFI) operations that can fail.
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(feature = "capi-v2-p2")]
/// Render a name as SQL, quoting and escaping it only when required.
pub fn render_identifier_quoted(text: &str) -> Result<String> {
    let data = check_api_call!(ffi::duckdb_v2_identifier_render_quoted, text.into(), RET)?;

    let string = unsafe {
        CStr::from_ptr(data).to_str().map_err(|e| Error {
            code: DuckDBError::DUCKDB_V2_ERROR_INPUT_INVALID,
            message: format!("Failed to convert library version to string: {}", e),
        })
    }
    .map(|v| v.to_string());

    unsafe {
        libc::free(data as *mut libc::c_void);
    }
    string
}

/// Return the linked DuckDB library version.
pub fn library_version() -> Result<&'static str> {
    check_api_call!(ffi::duckdb_v2_library_version, RET).map(|v| v.into())
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {

    use crate::{
        Parameters,
        environment::{Environment, StorageLocation},
        query_result::QueryResultStep,
    };

    use super::*;

    #[test]
    fn test_query_parameters() -> crate::Result<()> {
        let env = Environment::new()?;
        let db = env.open(StorageLocation::InMemory)?;
        let conn = db.connect()?;

        let result = conn
            .query("SELECT $1", Parameters::positional(&[&1]))?
            .next()
            .unwrap()?;

        let vector = result.get_vector_at::<i32>(0).unwrap();
        let value = vector.iter().unwrap().next().unwrap();
        assert_eq!(value, Some(&1));

        let result = conn
            .query("SELECT $test", Parameters::named(&[("test", &2)]))?
            .next()
            .unwrap()?;

        let vector = result.get_vector_at::<i32>(0).unwrap();
        let value = vector.iter().unwrap().next().unwrap();
        assert_eq!(value, Some(&2));

        Ok(())
    }

    #[test]
    fn open_in_memory_and_drop() -> crate::Result<()> {
        let env = Environment::new()?;
        let db = env.open(StorageLocation::InMemory)?;
        let conn = db.connect()?;

        let mut total_row_count = 0;
        // Drop happens automatically at the end of the scope

        let statements = conn.parse("SELECT 42")?;
        for stmt in statements {
            let stmt = stmt?;

            let result = conn.query(stmt, Parameters::None)?;

            for chunk in result {
                let chunk = chunk?;

                let vector = chunk.get_vector_at::<i32>(0)?;

                let type_id = vector.logical_type().type_id();

                assert_eq!(
                    type_id,
                    ffi::DUCKDB_V2_LOGICAL_TYPE_ID::DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER
                );
                assert_eq!(vector.len(), 1);

                let value = vector.iter().unwrap().next().unwrap();

                assert_eq!(value, Some(&42));

                total_row_count += chunk.row_count().unwrap();
            }
        }

        assert_eq!(total_row_count, 1, "Expected 1 row in the result chunk");

        Ok(())
    }

    #[test]
    #[cfg(feature = "capi-v2-p2")]
    fn test_prepared_statement() -> crate::Result<()> {
        let env = Environment::new()?;
        let db = env.open(StorageLocation::InMemory)?;
        let conn = db.connect()?;

        let statements = conn.parse(
            r#"
        CREATE TABLE t(x INTEGER);
        INSERT INTO t VALUES (1), (2), (3);
        "#,
        )?;

        for stmt in statements {
            let stmt = stmt?;
            conn.execute(stmt, Parameters::None)?;
        }

        let mut to_be_prepared = conn.parse("SELECT 2 * x FROM t")?;
        let prepared = to_be_prepared.next().unwrap()?.prepare(&conn, false)?;

        let result = prepared.execute(Parameters::None)?;

        for chunk in result {
            let chunk = chunk?;

            let vector = chunk.get_vector_at::<i32>(0)?;

            assert_eq!(vector.len(), 3);

            let values: Vec<Option<&i32>> = vector.iter().unwrap().collect();

            assert_eq!(values, vec![Some(&2), Some(&4), Some(&6)]);
        }

        Ok(())
    }

    #[test]
    fn test_library_version() -> crate::Result<()> {
        let version = library_version()?;
        assert!(!version.is_empty(), "Library version should not be empty");
        println!("DuckDB library version: {}", version);

        Ok(())
    }

    #[test]
    #[cfg(feature = "capi-v2-p2")]
    fn test_identifier_render_quoted() -> crate::Result<()> {
        let identifier = ffi::duckdb_v2_str {
            ptr: "10".as_ptr() as *const i8,
            len: "10".len() as u64,
            _marker: std::marker::PhantomData,
        };

        let quoted = render_identifier_quoted(identifier.into())?;
        assert_eq!(quoted, "\"10\"");

        Ok(())
    }

    #[test]
    fn test_connection_interrupt() -> crate::Result<()> {
        let env = Environment::new()?;
        let db = env.open(StorageLocation::InMemory)?;
        let conn = db.connect()?;

        let mut result = conn.query("SELECT * from range(0,10_000)", Parameters::None)?;

        let _ = result.next().unwrap()?;

        conn.interrupt_query()?;

        let step = result.step()?;

        assert!(matches!(step, QueryResultStep::Canceled));
        Ok(())
    }

    #[test]
    fn test_multiple_connections() -> crate::Result<()> {
        let env = Environment::new()?;
        let db = env.open(StorageLocation::InMemory)?;
        let db_2 = env.open(StorageLocation::OnDisk("test.db".into()))?;

        assert_eq!(env.get_database_count()?, 2);

        let conn1 = db.connect()?;
        let conn2 = db_2.connect()?;

        let result1 = conn1.query("SELECT 1", Parameters::None)?.next().unwrap()?;
        let result2 = conn2.query("SELECT 2", Parameters::None)?.next().unwrap()?;

        let vector1 = result1.get_vector_at::<i32>(0)?;
        let vector2 = result2.get_vector_at::<i32>(0)?;

        let value1 = vector1.get(0)?;
        let value2 = vector2.get(0)?;

        assert_eq!(value1, Some(&1));
        assert_eq!(value2, Some(&2));

        Ok(())
    }
}

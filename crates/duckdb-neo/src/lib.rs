#![doc(
    html_logo_url = "https://upload.wikimedia.org/wikipedia/commons/4/40/DuckDB_logo.svg?utm_source=commons.wikimedia.org&utm_campaign=index&utm_content=original"
)]
#![cfg_attr(coverage_nightly, feature(coverage_attribute))]
#![warn(missing_debug_implementations)]
//! Safe Rust bindings for [DuckDB's](https://duckdb.org/) C API V2.
//!
//! Use this crate to embed DuckDB in a Rust application, open databases, execute
//! SQL, bind values, and consume columnar results through type-safe handles.
//!
//! # Example
//!
//! ```
//! use duckdb_neo::{
//!     Parameters,
//!     environment::{Environment, StorageLocation},
//! };
//!
//! # fn main() -> duckdb_neo::Result<()> {
//! let environment = Environment::new()?;
//! let instance = environment.open(StorageLocation::InMemory)?;
//! let connection = instance.connect()?;
//!
//! let mut result = connection.query("SELECT $1::INTEGER", Parameters::positional(&[&42]))?;
//! let chunk = result.next_chunk()?.expect("query returned no rows");
//! let values = chunk.get_vector_at::<i32>(0)?.reader()?;
//!
//! assert_eq!(values.get(0)?, Some(&42));
//! # Ok(())
//! # }
//! ```

#[cfg(doctest)]
#[doc = include_str!("../README.md")]
struct ReadmeDoctests;

// Lets the raw handle derives name `::duckdb_neo` inside this crate.
extern crate self as duckdb_neo;

mod builder_helpers;
mod handles;
mod links;

pub(crate) mod bytes;
pub(crate) mod ffi_str;
pub(crate) mod parameter;

pub mod aggregate;
pub mod arrow;
pub mod bind_arguments;
pub mod cast;
pub mod column_data_collection;
pub mod connection;
pub mod connection_options;
pub mod copy_function;
pub mod custom_option;
pub mod custom_type;
pub mod data_chunk;
pub mod description;
pub mod enums;
pub mod environment;
pub mod error;
pub mod expression;
pub mod extension;
pub mod file;
pub mod instance;
pub mod log;
pub mod log_sink;
pub mod logical_type;
pub mod macro_function;
pub mod metrics;
pub mod multi_file;
pub mod pragma_function;
pub mod qualified_name;
pub mod query_progress;
pub mod query_result;
#[cfg(feature = "r2d2")]
pub mod r2d2;
pub mod raw;
pub mod replacement_scan;
pub mod scalar;
pub mod schema;
pub mod signature;
pub mod statement;
pub mod table_function;
pub mod tokens;
pub mod types;
pub mod value;
pub mod vector;

use crate::error::{check_api_call, check_api_call_string};
use connection::Factory;
pub use libduckdb_sys::v2 as ffi;

pub(crate) use duckdb_loadable_macros::DropHandle;
pub use duckdb_loadable_macros::duckdb_entrypoint_c_api_v2;
pub use duckdb_loadable_macros::{AsRaw, FromRaw};
pub use error::Error;
pub use parameter::{Parameters, QueryParameter};
pub use raw::{AsRaw, FromRaw};
pub use types::{DuckDBType, FromValue, ToValue};

/// This result type is used extensively throughout the crate to represent the result of (FFI) operations that can fail.
///
///     let result: duckdb_neo::Result<()> = Err(duckdb_neo::Error::invalid_input("invalid"));
///     assert!(result.is_err());
pub type Result<T> = std::result::Result<T, Error>;

/// Render a name as SQL, quoting and escaping it only when required.
pub fn render_identifier_quoted(text: &str) -> Result<String> {
    check_api_call_string!(ffi::duckdb_v2_identifier_render_quoted, &text.into())
}

/// Return the linked DuckDB library version as a static string owned by the library.
pub fn library_version() -> Result<&'static str> {
    // The version string is static in the library.
    check_api_call!(ffi::duckdb_v2_library_version, RET)
        .map(|v| ffi_str::DuckDBStr::from_raw(v).as_str().unwrap_or_default())
}

/// Validate that `bytes` are well-formed UTF-8, including bytes after any embedded NULs.
pub fn validate_utf8(bytes: &[u8]) -> Result<()> {
    check_api_call!(ffi::duckdb_v2_validate_utf8, &ffi_str::bytes_arg(bytes))
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {

    use crate::{
        Parameters,
        environment::{Environment, StorageLocation},
        error::DuckDBError,
        query_result::QueryResultStep,
    };

    use super::*;

    #[test]
    fn test_validate_utf8() {
        assert!(validate_utf8("héllo".as_bytes()).is_ok());
        assert!(validate_utf8(b"a\0b").is_ok());
        assert!(validate_utf8(b"a\0\xff").is_err());
    }

    #[test]
    fn test_query_parameters() -> crate::Result<()> {
        let env = Environment::new()?;
        let instance = env.open(StorageLocation::InMemory)?;
        let conn = instance.connect()?;

        let result = conn
            .query("SELECT $1", Parameters::positional(&[&1]))?
            .next_chunk()?
            .unwrap();

        let vector = result.get_vector_at::<i32>(0).unwrap();
        let value = vector.reader()?.iter().next().unwrap();
        assert_eq!(value, Some(&1));

        let result = conn
            .query("SELECT $test", Parameters::named(&[("test", &2)]))?
            .next_chunk()?
            .unwrap();

        let vector = result.get_vector_at::<i32>(0).unwrap();
        let value = vector.reader()?.iter().next().unwrap();
        assert_eq!(value, Some(&2));

        Ok(())
    }

    #[test]
    fn test_open_in_memory_and_drop() -> crate::Result<()> {
        let env = Environment::new()?;
        let instance = env.open(StorageLocation::InMemory)?;
        let conn = instance.connect()?;

        let mut total_row_count = 0;

        let statements = conn.parse("SELECT 42")?;
        for stmt in statements {
            let stmt = stmt?;

            let mut result = conn.query(stmt, Parameters::None)?;

            while let Some(chunk) = result.next_chunk()? {
                let vector = chunk.get_vector_at::<i32>(0)?;

                let type_id = vector.logical_type().id()?;

                assert_eq!(type_id, logical_type::LogicalTypeID::Integer);
                assert_eq!(vector.len(), 1);

                let value = vector.reader()?.iter().next().unwrap();

                assert_eq!(value, Some(&42));

                total_row_count += chunk.row_count().unwrap();
            }
        }

        assert_eq!(total_row_count, 1, "Expected 1 row in the result chunk");

        Ok(())
    }

    #[test]
    fn test_prepared_statement() -> crate::Result<()> {
        let env = Environment::new()?;
        let instance = env.open(StorageLocation::InMemory)?;
        let conn = instance.connect()?;

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

        let mut result = prepared.execute(Parameters::None)?;

        let chunk = result.next_chunk()?.expect("one chunk");
        let vector = chunk.get_vector_at::<i32>(0)?;

        assert_eq!(vector.len(), 3);

        let values: Vec<Option<&i32>> = vector.reader()?.iter().collect();

        assert_eq!(values, vec![Some(&2), Some(&4), Some(&6)]);
        assert!(result.next_chunk()?.is_none());

        Ok(())
    }

    #[test]
    fn test_library_version() -> crate::Result<()> {
        let expected = include_str!("../../libduckdb-sys/.duckdb-release")
            .lines()
            .find_map(|line| line.strip_prefix("DUCKDB_RELEASE_VERSION="))
            .expect("release pin must specify DUCKDB_RELEASE_VERSION");
        assert_eq!(library_version()?, expected);

        Ok(())
    }

    #[test]
    fn test_identifier_render_quoted() -> crate::Result<()> {
        let quoted = render_identifier_quoted("10")?;
        assert_eq!(quoted, "\"10\"");

        Ok(())
    }

    #[test]
    fn test_connection_interrupt() -> crate::Result<()> {
        let env = Environment::new()?;
        let instance = env.open(StorageLocation::InMemory)?;
        let conn = instance.connect()?;

        let mut result = conn.query("SELECT * from range(0,10_000)", Parameters::None)?;

        let _ = result.next_chunk()?.unwrap();

        conn.interrupt_query()?;

        let step = result.step()?;

        assert!(matches!(step, QueryResultStep::Canceled));
        Ok(())
    }

    #[test]
    fn test_connection_interrupt_other_thread() -> crate::Result<()> {
        let env = Environment::new()?;
        let instance = env.open(StorageLocation::InMemory)?;
        let conn = instance.connect()?;

        let (sender, receiver) = std::sync::mpsc::channel();
        let (sender_2, receiver_2) = std::sync::mpsc::channel();

        let interrupt_handle = conn.interrupt_handle();

        let t1 = std::thread::spawn(move || {
            let mut query = conn.query("SELECT * from range(0,10_000)", Parameters::None).unwrap();

            let _ = query.next_chunk().unwrap().unwrap();

            sender.send(()).unwrap();
            receiver_2.recv().unwrap();

            let error = query.next_chunk().err().unwrap();

            assert_eq!(error.code, DuckDBError::RuntimeInterrupt);
        });

        receiver.recv().unwrap();
        interrupt_handle.interrupt()?;
        sender_2.send(()).unwrap();

        t1.join().unwrap();

        Ok(())
    }

    #[test]
    fn test_multiple_instances() -> crate::Result<()> {
        let directory = tempfile::tempdir().unwrap();
        let env = Environment::new()?;
        let instance = env.open(StorageLocation::InMemory)?;
        let db_2 = env.open(StorageLocation::OnDisk(
            directory.path().join("test.db").to_str().unwrap().to_owned(),
        ))?;

        assert_eq!(env.instance_count()?, 2);

        let conn1 = instance.connect()?;
        let conn2 = db_2.connect()?;

        let result1 = conn1.query("SELECT 1", Parameters::None)?.next_chunk()?.unwrap();
        let result2 = conn2.query("SELECT 2", Parameters::None)?.next_chunk()?.unwrap();

        let reader1 = result1.get_vector_at::<i32>(0)?.reader()?;
        let reader2 = result2.get_vector_at::<i32>(0)?.reader()?;

        let value1 = reader1.get(0)?;
        let value2 = reader2.get(0)?;

        assert_eq!(value1, Some(&1));
        assert_eq!(value2, Some(&2));

        Ok(())
    }
}

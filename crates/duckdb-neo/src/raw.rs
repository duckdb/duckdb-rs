//! Escape hatches to and from the raw C API handles behind the wrappers.
//!
//! Use these to call `libduckdb_sys::v2` functions that this crate does not wrap yet.
//!
//! ```
//! use duckdb_neo::{AsRaw, FromRaw, ToValue, environment::{Environment, StorageLocation}, value::Value};
//!
//! # fn main() -> duckdb_neo::Result<()> {
//! let env = Environment::new()?;
//! let conn = env.open(StorageLocation::InMemory)?.connect()?;
//!
//! let value = 42_i32.value(&conn)?;
//! // SAFETY: `value` stays alive while the handle is in use.
//! let raw = unsafe { value.get_raw() };
//! // Hand ownership to a new wrapper, so only one of them destroys the handle.
//! std::mem::forget(value);
//! // SAFETY: `raw` is a valid value handle with no other owner.
//! let value = unsafe { Value::from_raw(raw) };
//! assert_eq!(value.get::<i32>()?, Some(42));
//! # Ok(())
//! # }
//! ```
//!
//! Wrappers no longer dereference to their handle, and reading it requires `unsafe`:
//!
//! ```compile_fail,E0133
//! # use duckdb_neo::{AsRaw, environment::{Environment, StorageLocation}};
//! # fn main() -> duckdb_neo::Result<()> {
//! # let conn = Environment::new()?.open(StorageLocation::InMemory)?.connect()?;
//! let raw = conn.get_raw();
//! # Ok(())
//! # }
//! ```
//!
//! ```compile_fail,E0614
//! # use duckdb_neo::environment::{Environment, StorageLocation};
//! # fn main() -> duckdb_neo::Result<()> {
//! # let conn = Environment::new()?.open(StorageLocation::InMemory)?.connect()?;
//! let raw = *conn;
//! # Ok(())
//! # }
//! ```

/// A wrapper around a raw DuckDB handle.
///
/// Implement it with `#[derive(AsRaw)]`.
pub trait AsRaw {
    /// The raw C API handle type.
    type Raw: Copy;

    /// Return the raw handle without giving up ownership.
    ///
    /// # Safety
    ///
    /// The handle is borrowed from `self`. It must not be used after `self` is
    /// dropped, must not be destroyed, and must not be used to break invariants
    /// that `self` relies on.
    unsafe fn get_raw(&self) -> Self::Raw;
}

/// A wrapper that can be rebuilt from its raw DuckDB handle alone.
///
/// Implement it with `#[derive(FromRaw)]`.
pub trait FromRaw: AsRaw {
    /// Wrap a raw handle.
    ///
    /// # Safety
    ///
    /// `raw` must be a valid handle of the wrapped kind. Owning wrappers take
    /// ownership and destroy the handle on drop, so the caller must not destroy
    /// or wrap it again. Borrowing wrappers, such as callback contexts, must not
    /// outlive the handle.
    unsafe fn from_raw(raw: Self::Raw) -> Self;
}

/// Safe raw handle access for code inside this crate, which upholds [`AsRaw::get_raw`]'s contract.
pub(crate) trait RawExt: AsRaw {
    fn raw(&self) -> Self::Raw {
        // SAFETY: crate code keeps the wrapper alive while using the handle and never destroys it.
        unsafe { self.get_raw() }
    }
}

impl<T: AsRaw + ?Sized> RawExt for T {}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;
    use crate::{
        DuckDBType,
        column_data_collection::ColumnDataCollection,
        connection::Context,
        environment::{Environment, StorageLocation},
        ffi,
        logical_type::{LogicalType, LogicalTypeID},
    };

    #[test]
    fn test_raw_round_trip() -> crate::Result<()> {
        let env = Environment::new()?;
        let conn = env.open(StorageLocation::InMemory)?.connect()?;

        let logical_type = i64::logical_type(&conn)?;
        let raw = unsafe { logical_type.get_raw() };
        std::mem::forget(logical_type);
        let logical_type = unsafe { LogicalType::from_raw(raw) };
        assert_eq!(logical_type.type_id(), LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT);

        let collection = ColumnDataCollection::new(&conn, [i64::logical_type(&conn)?])?;
        assert!(!unsafe { collection.get_raw() }.is_null());
        assert!(!unsafe { conn.get_raw() }.is_null());
        Ok(())
    }

    #[test]
    fn test_borrowed_from_raw() {
        let raw = std::ptr::dangling_mut::<ffi::_duckdb_v2_context>();
        // Borrowing wrappers never touch the handle on their own.
        let context = unsafe { Context::from_raw(raw) };
        assert_eq!(unsafe { context.get_raw() }, raw);
    }
}

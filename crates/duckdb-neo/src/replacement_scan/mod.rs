//! Replacing unresolved table references with table functions, column data collections, or subqueries.

use crate::{
    Result,
    builder_helpers::{OpaqueHandle, get_user_data, handle_unwind},
    check_api_call,
    column_data_collection::ColumnDataCollection,
    connection::Context,
    ffi,
    handles::{ReplacementScanBuilderHandle, ReplacementScanBuilderLink},
    qualified_name::QualifiedName,
    value::Value,
};

/// Callback-scoped controls for claiming an unresolved table reference.
///
/// Use [`Self::set_reference`] to replace it with a table function, column data
/// collection, or subquery. Parameters can be added only after selecting a
/// table function.
pub struct ReplacementHandle<'a> {
    info: &'a ffi::duckdb_v2_replacement_scan_info_handle,
}

/// A replacement source for an unresolved table reference, selected with
/// [`ReplacementHandle::set_reference`].
///
/// The table-function and subquery forms are borrowed and copied, so they carry
/// no lifetime obligation. The column-data-collection forms are borrowed, not
/// copied: DuckDB keeps a pointer to the collection and reads its buffers
/// directly, so the collection must stay alive as long as any result or
/// prepared statement over the claimed name does.
pub enum ReplacementType<'a> {
    /// A table function's name, optionally qualified by schema and catalog.
    Table(QualifiedName),

    /// A `SELECT` statement read instead of the table.
    Subquery(String),

    /// A borrowed column data collection to be read. Has default column names (`col1`, `col2`, ...).
    ColumnDataCollection(&'a ColumnDataCollection),

    /// A borrowed column data collection with explicitly named columns.
    NamedColumnDataCollection((&'a ColumnDataCollection, Vec<String>)),
}

impl<'a> ReplacementHandle<'a> {
    /// Append a positional table-function parameter.
    pub fn add_parameter(&self, value: Value) -> Result<()> {
        check_api_call!(ffi::duckdb_v2_replacement_scan_add_argument, *self.info, value.handle,)
    }

    /// Add a named table-function parameter.
    pub fn add_parameter_with_name(&self, name: &str, value: Value) -> Result<()> {
        check_api_call!(
            ffi::duckdb_v2_replacement_scan_add_named_argument,
            *self.info,
            name.into(),
            value.handle,
        )
    }

    /// Claim the reference with a table function, column data collection, or SELECT subquery.
    ///
    /// Returns an error if a different replacement kind has already claimed the
    /// reference. A subquery must contain exactly one SELECT statement.
    ///
    /// The column-data-collection forms are borrowed, not copied: the borrow is
    /// tied to the callback's user data (`&self`), which the registration keeps
    /// alive until the scan is destroyed. Store the collection in the callback
    /// struct (or somewhere with an equal or longer lifetime) to keep it valid
    /// for as long as any result or prepared statement reads it.
    pub fn set_reference(&self, replacement_type: ReplacementType<'a>) -> Result<()> {
        match replacement_type {
            ReplacementType::Table(name) => {
                check_api_call!(ffi::duckdb_v2_replacement_scan_set_function_name, *self.info, *name)
            }
            ReplacementType::Subquery(query) => {
                check_api_call!(
                    ffi::duckdb_v2_replacement_scan_set_subquery,
                    *self.info,
                    (&query).into()
                )
            }
            ReplacementType::ColumnDataCollection(collection) => check_api_call!(
                ffi::duckdb_v2_replacement_scan_set_collection,
                *self.info,
                **collection,
                std::ptr::null(),
                0
            ),
            ReplacementType::NamedColumnDataCollection((collection, names)) => check_api_call!(
                ffi::duckdb_v2_replacement_scan_set_collection,
                *self.info,
                **collection,
                names.iter().map(|n| n.into()).collect::<Vec<_>>().as_ptr(),
                names.len() as u64
            ),
        }
    }

    /// Set the replacement's alias unless the query supplies one.
    pub fn set_alias(&self, name: &str) -> Result<()> {
        check_api_call!(ffi::duckdb_v2_replacement_scan_set_alias, *self.info, name.into())
    }
}

unsafe extern "C" fn replacement_callback<T: ReplacementScanCallbacks>(
    info: ffi::duckdb_v2_replacement_scan_info_handle,
    context: ffi::duckdb_v2_context_handle,
    err: *mut ffi::duckdb_v2_error_info_handle,
) {
    handle_unwind(
        || {
            let user_data = get_user_data!(ffi::duckdb_v2_replacement_scan_get_user_data, info);

            let qname = QualifiedName {
                handle: check_api_call!(ffi::duckdb_v2_replacement_scan_get_name, info, RET)?,
            };

            T::scan(user_data, &Context(context), &qname, ReplacementHandle { info: &info })
        },
        err,
    );
}

/// Registers a replacement scan callback.
///
/// Callbacks are consulted when binding cannot resolve a table name, in
/// registration order within each scope. Connection-local scans run before
/// database-wide scans; the first to claim the reference wins.
///
/// Registering through a connection keeps the scan local to that connection
/// until it closes. Registering through a database or extension makes the scan
/// visible to all connections until the database closes.
pub struct ReplacementScanBuilder<T> {
    implementation: OpaqueHandle<T>,
}

impl<T> ReplacementScanBuilder<T>
where
    T: ReplacementScanCallbacks,
{
    /// Create a builder from its callback implementation.
    pub fn new(implementation: T) -> Self {
        Self {
            implementation: OpaqueHandle::new(implementation),
        }
    }

    fn build(&self, handle: &ReplacementScanBuilderHandle) -> Result<()> {
        check_api_call!(
            ffi::duckdb_v2_replacement_scan_set_callback,
            **handle,
            Some(replacement_callback::<T>)
        )?;

        check_api_call!(
            ffi::duckdb_v2_replacement_scan_set_user_data,
            **handle,
            &mut self.implementation.to_handle()
        )?;

        Ok(())
    }

    /// Register through a connection, extension, or database, consuming the builder.
    #[allow(private_bounds)]
    pub fn register<C: ReplacementScanBuilderLink>(self, link: &C) -> Result<()> {
        let handle = link.create_replacement_scan_handle()?;
        self.build(&handle)?;

        check_api_call!(ffi::duckdb_v2_replacement_scan_register, *handle)
    }
}

/// Binding callback for unresolved table references.
pub trait ReplacementScanCallbacks: Send + Sync + 'static {
    /// **Bind:** claim, decline, or reject an unresolved table reference.
    ///
    /// Call [`ReplacementHandle::set_reference`] to claim it, return without
    /// doing so to let the next replacement scan try, or return an error to
    /// reject the query.
    fn scan<'a>(&'a self, context: &Context, name: &QualifiedName, handle: ReplacementHandle<'a>) -> Result<()>;
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests;

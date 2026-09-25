//! Replacing unresolved table references with table functions, column data collections, or subqueries.

use std::collections::HashMap;

use crate::{
    Result,
    builder_helpers::{OpaqueHandle, get_opaque_data_ref, handle_unwind},
    check_api_call,
    column_data_collection::ColumnDataCollection,
    connection::Context,
    error::{DuckDBError, Error},
    ffi,
    handles::ReplacementScanBuilderLink,
    links::ConnectionOrigin,
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
    collections: &'a HashMap<String, ColumnDataCollection<'static>>,
}

/// A replacement source for an unresolved table reference, selected with
/// [`ReplacementHandle::set_reference`].
///
/// The column-data-collection forms name a collection added with
/// [`ReplacementScanBuilder::collection`]; the registration keeps it alive for
/// as long as any result or prepared statement reads it.
pub enum ReplacementType<'a> {
    /// A table function's name, optionally qualified by schema and catalog.
    Table(QualifiedName),

    /// A `SELECT` statement read instead of the table.
    Subquery(String),

    /// A registered collection, by name. Has default column names (`col1`, `col2`, ...).
    ColumnDataCollection(&'a str),

    /// A registered collection, by name, with explicitly named columns.
    NamedColumnDataCollection((&'a str, Vec<String>)),
}

impl ReplacementHandle<'_> {
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

    /// Return the collection added to the builder under `name`, if any.
    pub fn collection(&self, name: &str) -> Option<&ColumnDataCollection<'_>> {
        self.collections.get(name)
    }

    /// Claim the reference with a table function, column data collection, or SELECT subquery.
    ///
    /// Returns an error if a different replacement kind has already claimed the
    /// reference, or if no collection was registered under the given name. A
    /// subquery must contain exactly one SELECT statement.
    pub fn set_reference(&self, replacement_type: ReplacementType<'_>) -> Result<()> {
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
            ReplacementType::ColumnDataCollection(name) => check_api_call!(
                ffi::duckdb_v2_replacement_scan_set_collection,
                *self.info,
                **self.registered_collection(name)?,
                std::ptr::null(),
                0
            ),
            ReplacementType::NamedColumnDataCollection((name, names)) => check_api_call!(
                ffi::duckdb_v2_replacement_scan_set_collection,
                *self.info,
                **self.registered_collection(name)?,
                names.iter().map(|n| n.into()).collect::<Vec<_>>().as_ptr(),
                names.len() as u64
            ),
        }
    }

    /// Set the replacement's alias unless the query supplies one.
    pub fn set_alias(&self, name: &str) -> Result<()> {
        check_api_call!(ffi::duckdb_v2_replacement_scan_set_alias, *self.info, name.into())
    }

    fn registered_collection(&self, name: &str) -> Result<&ColumnDataCollection<'_>> {
        self.collection(name).ok_or_else(|| Error {
            code: DuckDBError::DUCKDB_V2_ERROR_INPUT_INVALID,
            message: format!("No collection named \"{name}\" is registered on this replacement scan"),
        })
    }
}

/// What the registration owns: the callback and the collections it can claim references with.
struct ReplacementScanData<T> {
    implementation: T,
    collections: HashMap<String, ColumnDataCollection<'static>>,
}

unsafe extern "C" fn replacement_callback<T: ReplacementScanCallbacks>(
    info: ffi::duckdb_v2_replacement_scan_info_handle,
    context: ffi::duckdb_v2_context_handle,
    err: *mut ffi::duckdb_v2_error_info_handle,
) {
    handle_unwind(
        || {
            let user_data = check_api_call!(ffi::duckdb_v2_replacement_scan_get_user_data, info, RET)?;
            let data = unsafe { get_opaque_data_ref::<ReplacementScanData<T>>(user_data) }.unwrap();

            let qname = QualifiedName {
                handle: check_api_call!(ffi::duckdb_v2_replacement_scan_get_name, info, RET)?,
            };

            let handle = ReplacementHandle {
                info: &info,
                collections: &data.collections,
            };

            data.implementation.scan(&Context(context), &qname, handle)
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
///
/// The registration owns the callback and any collections added with
/// [`Self::collection`] until then.
pub struct ReplacementScanBuilder<'conn, T> {
    implementation: T,
    collections: HashMap<String, ColumnDataCollection<'conn>>,
}

impl<'conn, T> ReplacementScanBuilder<'conn, T>
where
    T: ReplacementScanCallbacks,
{
    /// Create a builder from its callback implementation.
    pub fn new(implementation: T) -> Self {
        Self {
            implementation,
            collections: HashMap::new(),
        }
    }

    /// Hand `collection` to the scan under `name`, replacing any collection already added under it.
    ///
    /// The callback claims references with it through [`ReplacementType::ColumnDataCollection`]
    /// or [`ReplacementType::NamedColumnDataCollection`]. The scan must be registered on the
    /// connection the collection was created from.
    pub fn collection(mut self, name: impl Into<String>, collection: ColumnDataCollection<'conn>) -> Self {
        self.collections.insert(name.into(), collection);
        self
    }

    /// Register through a connection, extension, or database, consuming the builder.
    ///
    /// Returns an error if a collection was not created from `link`: only a
    /// connection can register collections, and only its own.
    #[allow(private_bounds)]
    pub fn register<C: ReplacementScanBuilderLink + ConnectionOrigin>(self, link: &C) -> Result<()> {
        let origin = link.connection_origin();

        if let Some((name, _)) = self
            .collections
            .iter()
            .find(|(_, collection)| origin.is_none() || collection.origin != origin)
        {
            return Err(Error {
                code: DuckDBError::DUCKDB_V2_ERROR_INPUT_INVALID,
                message: format!(
                    "Collection \"{name}\" can only be registered on a replacement scan of the connection it was created from"
                ),
            });
        }

        // SAFETY: every collection comes from `link`'s connection, which drops the scan, and the
        // collections with it, when it is destroyed. It keeps the database alive until then.
        let collections = unsafe {
            std::mem::transmute::<
                HashMap<String, ColumnDataCollection<'conn>>,
                HashMap<String, ColumnDataCollection<'static>>,
            >(self.collections)
        };

        let data = OpaqueHandle::new(ReplacementScanData {
            implementation: self.implementation,
            collections,
        });

        let handle = link.create_replacement_scan_handle()?;

        check_api_call!(
            ffi::duckdb_v2_replacement_scan_set_callback,
            *handle,
            Some(replacement_callback::<T>)
        )?;

        check_api_call!(
            ffi::duckdb_v2_replacement_scan_set_user_data,
            *handle,
            &mut data.to_handle()
        )?;

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
    fn scan(&self, context: &Context, name: &QualifiedName, handle: ReplacementHandle<'_>) -> Result<()>;
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests;

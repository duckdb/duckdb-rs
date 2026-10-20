//! Catalog metadata for a resolved base table.

use crate::ffi;

use crate::ffi_str::DuckDBStr;
use crate::{
    DropHandle, Result, check_api_call, connection::Connection, logical_type::LogicalType,
    qualified_name::QualifiedName, raw::RawExt,
};

/// An owned snapshot of a table column's name, type, and default or generated status.
///
/// Obtained from [`TableDescription::column`] or [`TableDescription::columns`]
/// and independent of the table description's lifetime.
#[derive(Debug, DropHandle)]
#[drop_handle(ffi::duckdb_v2_column_description_destroy)]
pub struct ColumnDescription {
    handle: ffi::duckdb_v2_column_description_handle,
}

// SAFETY: an owned C++ snapshot; `&self` methods only read it and shared types are atomically refcounted.
unsafe impl Send for ColumnDescription {}
unsafe impl Sync for ColumnDescription {}

impl ColumnDescription {
    /// Return the column's name with its declared casing.
    pub fn name(&self) -> Result<&str> {
        check_api_call!(ffi::duckdb_v2_column_description_get_name, self.handle, RET)
            .map(|name| DuckDBStr::from_raw(name).as_str().unwrap_or_default())
    }

    /// Return an owned copy of the column's logical type.
    pub fn logical_type(&self) -> Result<LogicalType> {
        let mut handle = check_api_call!(ffi::duckdb_v2_column_description_get_type, self.handle, RET)?;

        LogicalType::copy_handle(&mut handle)
    }

    /// Return whether the column declares a default expression; false for generated columns.
    pub fn has_default(&self) -> Result<bool> {
        check_api_call!(ffi::duckdb_v2_column_description_has_default, self.handle, RET)
    }

    /// Return whether the column is generated from an expression.
    pub fn has_generated(&self) -> Result<bool> {
        check_api_call!(ffi::duckdb_v2_column_description_has_generated, self.handle, RET)
    }
}

/// An owned snapshot of a base table's catalog metadata.
///
/// The snapshot records the fully resolved name, columns, and per-column
/// properties at creation time; later DDL does not update it.
///
/// # Example
/// ```
/// use duckdb_neo::{Parameters, environment::{Environment, StorageLocation}};
/// use duckdb_neo::qualified_name::QualifiedName;
/// use duckdb_neo::description::TableDescription;
///
/// # fn main() -> duckdb_neo::Result<()> {
/// let env = Environment::new()?;
/// let instance = env.open(StorageLocation::InMemory)?;
/// let conn = instance.connect()?;
/// let mut statements =
///     conn.parse("CREATE TABLE items (id INTEGER, name VARCHAR DEFAULT 'unknown')")?;
/// conn.execute(
///     statements.next().expect("expected a statement")?,
///     Parameters::None,
/// )?;
///
/// let name = QualifiedName::from_sql("items")?;
/// let description = TableDescription::new(&conn, &name)?;
///
/// assert!(description.column(1)?.has_default()?);
/// # Ok(())
/// # }
/// ```
#[derive(Debug, DropHandle)]
#[drop_handle(ffi::duckdb_v2_table_description_destroy)]
pub struct TableDescription {
    handle: ffi::duckdb_v2_table_description_handle,
}

// SAFETY: an owned C++ snapshot; `&self` methods only read it and shared types are atomically refcounted.
unsafe impl Send for TableDescription {}
unsafe impl Sync for TableDescription {}

impl TableDescription {
    /// Resolve a base table through the connection's catalog and search path.
    /// Returns an error for missing tables, views, or ambiguous schema/catalog qualifiers.
    pub fn new(connection: &Connection, name: &QualifiedName) -> Result<Self> {
        check_api_call!(
            ffi::duckdb_v2_connection_describe_table,
            connection.raw(),
            name.handle,
            RET
        )
        .map(|handle| TableDescription { handle })
    }

    /// Return the table's fully resolved qualified name.
    pub fn qname(&self) -> Result<QualifiedName> {
        check_api_call!(ffi::duckdb_v2_table_description_get_qname, self.handle, RET)
            .map(|handle| QualifiedName { handle })
    }

    /// Return the number of columns, including generated columns.
    pub fn column_count(&self) -> Result<usize> {
        check_api_call!(ffi::duckdb_v2_table_description_get_column_count, self.handle, RET).map(|count| count as usize)
    }

    /// Return an owned description at the zero-based index; out-of-range indices return an error.
    /// Columns follow declaration order, including generated columns.
    pub fn column(&self, index: usize) -> Result<ColumnDescription> {
        check_api_call!(
            ffi::duckdb_v2_table_description_get_column,
            self.handle,
            index as u64,
            RET
        )
        .map(|handle| ColumnDescription { handle })
    }

    /// Return owned descriptions of all columns in declaration order, including generated columns.
    pub fn columns(&self) -> Result<Vec<ColumnDescription>> {
        let count = self.column_count()?;

        let mut columns = Vec::with_capacity(count);

        for i in 0..count {
            columns.push(self.column(i)?);
        }
        Ok(columns)
    }

    /// Return whether the table belongs to a read-only catalog.
    /// A `false` result does not guarantee that writes will succeed.
    pub fn readonly(&self) -> Result<bool> {
        check_api_call!(ffi::duckdb_v2_table_description_is_readonly, self.handle, RET)
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use crate::logical_type::LogicalTypeID;
    use crate::{
        Parameters,
        description::TableDescription,
        environment::{Environment, StorageLocation},
        qualified_name::QualifiedName,
    };
    #[test]
    fn test_description_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<TableDescription>();
        assert_send_sync::<super::ColumnDescription>();
    }

    #[test]
    fn test_table_description() -> crate::Result<()> {
        let env = Environment::new()?;
        let instance = env.open(StorageLocation::InMemory)?;
        let conn = instance.connect()?;

        let mut statements =
            conn.parse("CREATE TABLE test (id INTEGER, name VARCHAR DEFAULT \'DEF\', td AS (2*id));")?;

        conn.execute(statements.next().unwrap()?, Parameters::None)?;

        let qname = QualifiedName::from_sql("test")?;
        let table_desc = TableDescription::new(&conn, &qname)?;

        assert!(!table_desc.readonly()?);

        let columns = table_desc.columns()?;

        assert_eq!(columns.len(), 3);

        let id_col = &columns[0];

        assert!(!id_col.has_default()?);
        assert!(!id_col.has_generated()?);
        assert_eq!(
            id_col.logical_type()?.id()?,
            LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER
        );
        assert_eq!(id_col.name()?, "id");

        let name = &columns[1];

        assert!(name.has_default()?);
        assert!(!name.has_generated()?);
        assert_eq!(
            name.logical_type()?.id()?,
            LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR
        );
        assert_eq!(name.name()?, "name");

        let td = &columns[2];

        assert!(td.has_generated()?);
        assert!(!td.has_default()?);

        Ok(())
    }
}

//! Catalog metadata for a resolved base table.

use crate::ffi;

use crate::{
    Result, check_api_call, check_api_call_no_err, connection::Connection, logical_type::LogicalType,
    qualified_name::QualifiedName,
};

/// An owned snapshot of a table column's name, type, and default or generated status.
///
/// Obtained from [`TableDescription::column`] or [`TableDescription::columns`]
/// and independent of the table description's lifetime.
pub struct ColumnDescription {
    handle: ffi::duckdb_v2_column_description_handle,
}

impl Drop for ColumnDescription {
    fn drop(&mut self) {
        check_api_call_no_err!(ffi::duckdb_v2_column_description_destroy, &mut self.handle)
            .expect("Failed to destroy column description handle");
    }
}

impl ColumnDescription {
    /// Return the column's name with its declared casing.
    pub fn name(&self) -> Result<&str> {
        let name = check_api_call!(ffi::duckdb_v2_column_description_get_name, self.handle, RET)?;

        Ok(name.into())
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
/// let db = env.open(StorageLocation::InMemory)?;
/// let conn = db.connect()?;
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
pub struct TableDescription {
    handle: ffi::duckdb_v2_table_description_handle,
}

impl TableDescription {
    /// Resolve a base table through the connection's catalog and search path.
    pub fn new(connection: &Connection, name: &QualifiedName) -> Result<Self> {
        Ok(TableDescription {
            handle: check_api_call!(ffi::duckdb_v2_connection_describe_table, **connection, name.handle, RET)?,
        })
    }

    /// Return the table's fully resolved qualified name.
    pub fn qname(&self) -> Result<QualifiedName> {
        let handle = check_api_call!(ffi::duckdb_v2_table_description_get_qname, self.handle, RET)?;

        Ok(QualifiedName { handle })
    }

    /// Return the number of columns, including generated columns.
    pub fn column_count(&self) -> Result<usize> {
        Ok(check_api_call!(ffi::duckdb_v2_table_description_get_column_count, self.handle, RET)? as usize)
    }

    /// Return an owned description of the column at the zero-based index.
    ///
    /// Columns follow declaration order, including generated columns.
    pub fn column(&self, index: usize) -> Result<ColumnDescription> {
        Ok(ColumnDescription {
            handle: check_api_call!(
                ffi::duckdb_v2_table_description_get_column,
                self.handle,
                index as u64,
                RET
            )?,
        })
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
    pub fn readonly(&self) -> Result<bool> {
        check_api_call!(ffi::duckdb_v2_table_description_is_readonly, self.handle, RET)
    }
}

impl Drop for TableDescription {
    fn drop(&mut self) {
        check_api_call_no_err!(ffi::duckdb_v2_table_description_destroy, &mut self.handle).unwrap();
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
    fn test_table_description() -> crate::Result<()> {
        let env = Environment::new()?;
        let db = env.open(StorageLocation::InMemory)?;
        let conn = db.connect()?;

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
            id_col.logical_type()?.type_id(),
            LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER
        );
        assert_eq!(id_col.name()?, "id");

        let name = &columns[1];

        assert!(name.has_default()?);
        assert!(!name.has_generated()?);
        assert_eq!(
            name.logical_type()?.type_id(),
            LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR
        );
        assert_eq!(name.name()?, "name");

        let td = &columns[2];

        assert!(td.has_generated()?);
        assert!(!td.has_default()?);

        Ok(())
    }
}

//! Ordered field schemas used across DuckDB APIs.

use libduckdb_sys::{self as ffi};

use crate::{Result, check_api_call, check_api_call_no_err, logical_type::LogicalType};

/// An owned, ordered list of field names and logical types.
///
/// Schemas describe statement parameters, result columns, tables, and Arrow
/// conversions. Field names may repeat or be empty.
///
/// # Example
/// ```
/// use duckdb_rs::{
///     Parameters,
///     environment::Environment,
///     environment::StorageLocation,
/// };
/// use duckdb_rs::logical_type::LogicalTypeID;
///
/// # fn main() -> duckdb_rs::Result<()> {
/// let env = Environment::new()?;
/// let db = env.open(StorageLocation::InMemory)?;
/// let conn = db.connect()?;
/// let mut statements = conn.parse("SELECT 42 AS answer")?;
/// let statement = statements.next().expect("expected a statement")?;
/// let schema = conn
///     .query(statement, Parameters::None)?
///     .schema()?;
/// let (name, logical_type) = schema.get(0)?;
///
/// assert_eq!(name, "answer");
/// assert_eq!(
///     logical_type.type_id(),
///     LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER
/// );
/// # Ok(())
/// # }
/// ```
pub struct Schema {
    /// The owned DuckDB schema handle.
    pub handle: ffi::duckdb_v2_schema_handle,
}

impl Schema {
    /// Return whether the schema has no fields.
    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.len()? == 0)
    }

    /// Return the number of fields.
    pub fn len(&self) -> Result<usize> {
        let count: u64 = check_api_call!(ffi::duckdb_v2_schema_get_count, self.handle, RET)?;

        Ok(count as usize)
    }

    /// Return a field's borrowed name and owned logical type.
    ///
    /// An out-of-range index returns an error.
    pub fn get(&self, index: usize) -> Result<(&str, LogicalType)> {
        let mut out_type = std::ptr::null_mut();

        let out_name: ffi::duckdb_v2_str = check_api_call!(
            ffi::duckdb_v2_schema_get_field,
            self.handle,
            index as u64,
            RET,
            &mut out_type
        )?;

        // out_type is borrowed from the schema; copy it so the returned
        // LogicalType owns a handle it may destroy.
        let owned_type = check_api_call!(ffi::duckdb_v2_logical_type_copy, out_type, RET)?;

        Ok((out_name.into(), LogicalType { handle: owned_type }))
    }

    /// Return all fields in declaration order.
    pub fn get_all(&self) -> Result<Vec<(String, LogicalType)>> {
        let mut result = Vec::new();
        let count = self.len()?;

        for i in 0..count {
            let item = self.get(i)?;
            result.push((item.0.to_string(), item.1));
        }

        Ok(result)
    }
}

impl Drop for Schema {
    fn drop(&mut self) {
        check_api_call_no_err!(ffi::duckdb_v2_schema_destroy, &mut self.handle).unwrap();
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use crate::{
        DuckDBType, Parameters,
        environment::{Environment, StorageLocation},
    };

    #[test]
    fn test_schema_statement_bind() -> crate::Result<()> {
        let env = Environment::new()?;
        let db = env.open(StorageLocation::InMemory)?;
        let conn = db.connect()?;

        let mut statements = conn.parse(
            r#"
        CREATE TABLE t(a INTEGER, b VARCHAR);

        SELECT * FROM t WHERE a = $1 AND b = $2
        "#,
        )?;

        let statement = statements.next().unwrap()?;
        conn.execute(statement, Parameters::None)?;

        let statement = statements.next().unwrap()?;

        let bind = statement.bind(&conn)?;

        assert_eq!(bind.schema.len()?, 2);
        assert_eq!(bind.parameters.len()?, 2);

        assert_eq!(
            bind.schema.get_all()?,
            vec![
                ("a".to_string(), i32::logical_type(&conn)?),
                ("b".to_string(), String::logical_type(&conn)?),
            ]
        );

        assert_eq!(
            bind.parameters.get_all()?,
            vec![
                ("1".to_string(), i32::logical_type(&conn)?),
                ("2".to_string(), String::logical_type(&conn)?),
            ]
        );

        conn.execute(statement, Parameters::positional(&[&1_i32, &"value"]))?;

        Ok(())
    }
}

//! Ordered field schemas used across DuckDB APIs.

use crate::ffi;

use crate::ffi_str::DuckDBStr;
use crate::{AsRaw, DropHandle, FromRaw, Result, check_api_call, logical_type::LogicalType};

/// An owned, ordered list of field names and logical types.
///
/// Schemas describe statement parameters, result columns, and Arrow imports.
/// Field names may repeat or be empty, and a schema may have no fields.
///
/// # Example
/// ```
/// use duckdb_neo::{
///     Parameters,
///     environment::Environment,
///     environment::StorageLocation,
/// };
/// use duckdb_neo::logical_type::LogicalTypeID;
///
/// # fn main() -> duckdb_neo::Result<()> {
/// let env = Environment::new()?;
/// let instance = env.open(StorageLocation::InMemory)?;
/// let conn = instance.connect()?;
/// let mut statements = conn.parse("SELECT 42 AS answer")?;
/// let statement = statements.next().expect("expected a statement")?;
/// let schema = conn
///     .query(statement, Parameters::None)?
///     .schema()?;
/// let (name, logical_type) = schema.get(0)?;
///
/// assert_eq!(name, "answer");
/// assert_eq!(
///     logical_type.id()?,
///     LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER
/// );
/// # Ok(())
/// # }
/// ```
#[derive(Debug, AsRaw, FromRaw, DropHandle)]
#[drop_handle(ffi::duckdb_v2_schema_destroy)]
pub struct Schema {
    pub(crate) handle: ffi::duckdb_v2_schema_handle,
}

// SAFETY: the field list is owned, self-contained, and never mutated after creation.
unsafe impl Send for Schema {}
unsafe impl Sync for Schema {}

impl Schema {
    /// Return whether the schema has no fields.
    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.len()? == 0)
    }

    /// Return the number of fields.
    pub fn len(&self) -> Result<usize> {
        check_api_call!(ffi::duckdb_v2_schema_get_count, self.handle, RET).map(|count| count as usize)
    }

    /// Return a field's borrowed name; an out-of-range index returns an error.
    pub fn name(&self, index: usize) -> Result<&str> {
        self.field(index).map(|(name, _)| name)
    }

    /// Return a field's owned logical type; an out-of-range index returns an error.
    pub fn field_type(&self, index: usize) -> Result<LogicalType> {
        let (_, mut logical_type) = self.field(index)?;
        LogicalType::copy_handle(&mut logical_type)
    }

    /// Return a field's borrowed name and owned logical type; out-of-range indices return an error.
    pub fn get(&self, index: usize) -> Result<(&str, LogicalType)> {
        let (name, mut logical_type) = self.field(index)?;
        Ok((name, LogicalType::copy_handle(&mut logical_type)?))
    }

    fn field(&self, index: usize) -> Result<(&str, ffi::duckdb_v2_logical_type_handle)> {
        let mut out_type = std::ptr::null_mut();
        let out_name = check_api_call!(
            ffi::duckdb_v2_schema_get_field,
            self.handle,
            index as u64,
            RET,
            &mut out_type
        )?;
        Ok((DuckDBStr::from_raw(out_name).as_str().unwrap_or_default(), out_type))
    }

    /// Iterate over borrowed names and owned logical types in declaration order.
    pub fn iter(&self) -> Result<impl ExactSizeIterator<Item = Result<(&str, LogicalType)>> + '_> {
        Ok((0..self.len()?).map(|index| self.get(index)))
    }

    /// Return all fields in declaration order.
    pub fn get_all(&self) -> Result<Vec<(String, LogicalType)>> {
        let count = self.len()?;
        let mut result = Vec::with_capacity(count);

        for i in 0..count {
            let item = self.get(i)?;
            result.push((item.0.to_string(), item.1));
        }

        Ok(result)
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
    fn test_schema_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<super::Schema>();
    }

    #[test]
    fn test_schema_accessors() -> crate::Result<()> {
        let env = Environment::new()?;
        let instance = env.open(StorageLocation::InMemory)?;
        let conn = instance.connect()?;
        let schema = conn.query("SELECT 42 AS answer", Parameters::None)?.schema()?;

        assert_eq!(schema.name(0)?, "answer");
        assert_eq!(schema.field_type(0)?, i32::logical_type(&conn)?);
        assert!(schema.name(1).is_err());
        assert!(schema.field_type(1).is_err());
        let mut fields = schema.iter()?;
        assert_eq!(fields.len(), 1);
        assert_eq!(fields.next().transpose()?, Some(schema.get(0)?));
        assert!(fields.next().is_none());
        Ok(())
    }

    #[test]
    fn test_schema_raw_round_trip() -> crate::Result<()> {
        use crate::{AsRaw, FromRaw};

        let env = Environment::new()?;
        let instance = env.open(StorageLocation::InMemory)?;
        let conn = instance.connect()?;
        let schema = conn.query("SELECT 42 AS answer", Parameters::None)?.schema()?;
        // SAFETY: ownership is transferred from schema to the replacement wrapper.
        let raw = unsafe { schema.get_raw() };
        std::mem::forget(schema);
        // SAFETY: raw is a live schema handle with no other owner.
        let schema = unsafe { super::Schema::from_raw(raw) };
        assert_eq!(schema.name(0)?, "answer");
        assert_eq!(schema.field_type(0)?, i32::logical_type(&conn)?);
        Ok(())
    }

    #[test]
    fn test_schema_statement_bind() -> crate::Result<()> {
        let env = Environment::new()?;
        let instance = env.open(StorageLocation::InMemory)?;
        let conn = instance.connect()?;

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

//! Registration of named custom logical types.

use libduckdb_sys::v2 as ffi;

use crate::{
    Result, check_api_call,
    handles::{CustomTypeBuilderHandle, CustomTypeBuilderLink},
    logical_type::LogicalType,
};

/// A builder for a named logical type backed by an existing type.
///
/// A custom type is logically distinct and can define its own casts, while
/// retaining the physical representation of its base type. Registration makes
/// the name available to every connection on the target database. DuckDB
/// copies the definition, so dropping this builder does not unregister it.
///
/// # Example
/// ```
/// use duckdb_neo::{DuckDBType, environment::{Environment, StorageLocation}};
/// use duckdb_neo::custom_type::CustomType;
///
/// # fn main() -> duckdb_neo::Result<()> {
/// let env = Environment::new()?;
/// let db = env.open(StorageLocation::InMemory)?;
/// let conn = db.connect()?;
///
/// let temperature = CustomType::new("TEMPERATURE", i32::logical_type(&conn)?)?;
/// temperature.register(&conn)?;
///
/// let logical_type = i32::logical_type(&conn)?.to_alias(&conn, "TEMPERATURE")?;
/// assert_eq!(logical_type.to_string()?, "TEMPERATURE");
/// # Ok(())
/// # }
/// ```
pub struct CustomType {
    base_type: LogicalType,
    name: String,
}

impl CustomType {
    /// Create a custom type definition with a name and base type.
    pub fn new(name: &str, base_type: LogicalType) -> Result<Self> {
        Ok(CustomType {
            base_type,
            name: name.to_string(),
        })
    }

    fn build(&self, handle: &CustomTypeBuilderHandle) -> Result<()> {
        check_api_call!(
            ffi::duckdb_v2_custom_type_set_base_type,
            **handle,
            self.base_type.handle
        )?;

        check_api_call!(ffi::duckdb_v2_custom_type_set_name, **handle, (&self.name).into(),)?;

        Ok(())
    }

    /// Register through a connection or extension, consuming the builder.
    #[allow(private_bounds)]
    pub fn register<C: CustomTypeBuilderLink>(self, link: &C) -> Result<()> {
        let handle = link.create_custom_type_handle()?;
        self.build(&handle)?;

        check_api_call!(ffi::duckdb_v2_custom_type_register, *handle)
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use crate::{
        DuckDBType,
        custom_type::CustomType,
        environment::{Environment, StorageLocation},
        logical_type::LogicalTypeID,
    };

    #[test]
    fn test_custom_type() -> crate::Result<()> {
        let env = Environment::new()?;
        let db = env.open(StorageLocation::InMemory)?;
        let conn = db.connect()?;

        let custom_type = CustomType::new("TEMPERATURE", i32::logical_type(&conn)?)?;

        custom_type.register(&conn)?;

        let integer = i32::logical_type(&conn)?;
        let temperature = integer.to_alias(&conn, "TEMPERATURE")?;

        assert_eq!(temperature.to_string()?, "TEMPERATURE");
        assert_eq!(temperature.type_id(), LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);

        Ok(())
    }
}

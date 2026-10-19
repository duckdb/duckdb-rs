//! Registration of named custom logical types.

use crate::ffi;

use crate::{
    Result, check_api_call,
    handles::{CustomTypeBuilderHandle, CustomTypeBuilderLink},
    logical_type::LogicalType,
    raw::RawExt,
};

/// A builder for a named logical type backed by an existing type.
///
/// A custom type keeps its base representation but has a distinct name and casts.
/// Registration copies the definition; other connections see it after the transaction commits.
///
/// # Example
/// ```
/// use duckdb_neo::{DuckDBType, environment::{Environment, StorageLocation}};
/// use duckdb_neo::custom_type::CustomType;
///
/// # fn main() -> duckdb_neo::Result<()> {
/// let env = Environment::new()?;
/// let instance = env.open(StorageLocation::InMemory)?;
/// let conn = instance.connect()?;
///
/// let temperature = CustomType::new("TEMPERATURE", i32::logical_type(&conn)?);
/// temperature.register(&conn)?;
///
/// let logical_type = duckdb_neo::logical_type::LogicalType::from_text(&conn, "TEMPERATURE")?;
/// assert_eq!(logical_type.to_text()?, "TEMPERATURE");
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct CustomType {
    base_type: LogicalType,
    name: String,
}

impl CustomType {
    /// Create a custom type definition with a name and base type.
    pub fn new(name: impl Into<String>, base_type: LogicalType) -> Self {
        CustomType {
            base_type,
            name: name.into(),
        }
    }

    fn build(&self, handle: &CustomTypeBuilderHandle) -> Result<()> {
        check_api_call!(
            ffi::duckdb_v2_custom_type_set_base_type,
            handle.raw(),
            self.base_type.handle
        )?;

        check_api_call!(ffi::duckdb_v2_custom_type_set_name, handle.raw(), &(&self.name).into(),)
    }

    /// Register through a connection, consuming the builder and replacing any same-named type.
    /// Returns an error for an empty name or a base type that is not fully defined and concrete.
    #[allow(private_bounds)]
    pub fn register<C: CustomTypeBuilderLink>(self, link: &C) -> Result<()> {
        let handle = link.create_custom_type_handle()?;
        self.build(&handle)?;

        link.register_custom_type_handle(handle)
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use crate::{
        DuckDBType,
        custom_type::CustomType,
        environment::{Environment, StorageLocation},
        logical_type::{LogicalType, LogicalTypeID},
    };

    #[test]
    fn test_custom_type() -> crate::Result<()> {
        let env = Environment::new()?;
        let instance = env.open(StorageLocation::InMemory)?;
        let conn = instance.connect()?;

        let custom_type = CustomType::new("TEMPERATURE", i32::logical_type(&conn)?);

        custom_type.register(&conn)?;

        let temperature = LogicalType::from_text(&conn, "TEMPERATURE")?;

        assert_eq!(temperature.to_text()?, "TEMPERATURE");
        assert_eq!(temperature.id()?, LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);

        Ok(())
    }
}

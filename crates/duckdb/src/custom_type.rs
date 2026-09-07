//! Registration of named custom logical types.

use libduckdb_sys as ffi;

use crate::{
    Result, builder_helpers::context_and_connection_fn, check_api_call, check_api_call_no_err,
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
/// use duckdb_rs::{DuckDBType, Environment, StorageLocation};
/// use duckdb_rs::custom_type::CustomType;
///
/// # fn main() -> duckdb_rs::Result<()> {
/// let env = Environment::new()?;
/// let db = env.open(StorageLocation::InMemory)?;
/// let conn = db.connect()?;
///
/// let temperature = CustomType::new("TEMPERATURE", i32::logical_type(&conn)?)?;
/// temperature.register_with_connection(&conn)?;
///
/// let logical_type = i32::logical_type(&conn)?.to_alias("TEMPERATURE")?;
/// assert_eq!(logical_type.to_string()?, "TEMPERATURE");
/// # Ok(())
/// # }
/// ```
pub struct CustomType {
    base_type: LogicalType,
    name: String,
}

struct CustomTypeBuilderHandle(ffi::duckdb_v2_custom_type_builder_handle);

impl Deref for CustomTypeBuilderHandle {
    type Target = ffi::duckdb_v2_custom_type_builder_handle;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Drop for CustomTypeBuilderHandle {
    fn drop(&mut self) {
        check_api_call_no_err!(ffi::duckdb_v2_custom_type_builder_destroy, &mut self.0).unwrap();
    }
}

impl CustomType {
    /// Create a custom type definition with a name and base type.
    pub fn new(name: &str, base_type: LogicalType) -> Result<Self> {
        Ok(CustomType {
            base_type,
            name: name.to_string(),
        })
    }

    fn build(&self) -> Result<CustomTypeBuilderHandle> {
        let handle = CustomTypeBuilderHandle(check_api_call!(
            ffi::duckdb_v2_custom_type_builder_create,
            *handle,
            RET
        )?);

        check_api_call!(
            ffi::duckdb_v2_custom_type_builder_set_base_type,
            *handle,
            self.base_type.handle
        )?;

        check_api_call!(
            ffi::duckdb_v2_custom_type_builder_set_name,
            *handle,
            (&self.name).into(),
        )?;

        Ok(handle)
    }

    context_and_connection_fn! {
        /// Register the type through a connection or callback context.
        pub fn register_with_[context, connection](self) -> Result<()>
        {
            context_fn: ffi::duckdb_v2_custom_type_builder_register_with_context,
            connection_fn: ffi::duckdb_v2_custom_type_builder_register_with_connection,
        }
        let handle = self.build()?;

        check_api_call!(
            api_fn!(),
            **api_arg!(),
            *handle,
        )
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use crate::{DuckDBType, Environment, StorageLocation, custom_type::CustomType, logical_type::LogicalTypeID};

    #[test]
    fn test_custom_type() -> crate::Result<()> {
        let env = Environment::new()?;
        let db = env.open(StorageLocation::InMemory)?;
        let conn = db.connect()?;

        let custom_type = CustomType::new("TEMPERATURE", i32::logical_type(&conn)?)?;

        custom_type.register_with_connection(&conn)?;

        let integer = i32::logical_type(&conn)?;
        let temperature = integer.to_alias("TEMPERATURE")?;

        assert_eq!(temperature.to_string()?, "TEMPERATURE");
        assert_eq!(temperature.type_id(), LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);

        Ok(())
    }
}

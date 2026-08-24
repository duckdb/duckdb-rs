//! Configuration options shared with databases and connections.

use std::ops::Deref;

use libduckdb_sys::{self as ffi};

use crate::{
    Result,
    builder_helpers::ffi_enum_redeclaration,
    error::{check_api_call, check_api_call_no_err},
};

ffi_enum_redeclaration! {
    /// Where DuckDB permits a configuration option's setting to be written.
    ///
    /// The target scope is part of the option declaration and is distinct from
    /// the scope selected for a particular write.
    pub enum TargetScope <- ffi::DUCKDB_V2_OPTION_TARGET_SCOPE {
        /// The target scope is not known.
        Unknown = DUCKDB_V2_OPTION_TARGET_SCOPE_UNKNOWN,
        /// The option may only be written at global database scope.
        GlobalOnly = DUCKDB_V2_OPTION_TARGET_SCOPE_GLOBAL_ONLY,
        /// The option may only be written at local session scope.
        LocalOnly = DUCKDB_V2_OPTION_TARGET_SCOPE_LOCAL_ONLY,
        /// The option accepts either scope and defaults to global.
        GlobalDefault = DUCKDB_V2_OPTION_TARGET_SCOPE_GLOBAL_DEFAULT,
        /// The option accepts either scope and defaults to local.
        LocalDefault = DUCKDB_V2_OPTION_TARGET_SCOPE_LOCAL_DEFAULT,
    }
}

/// A resolved DuckDB configuration option.
///
/// Fetched from a database or connection, this contains the effective setting
/// and canonical metadata such as aliases, description, default setting, and
/// target scope. Use [`ConfigOptionValue`] to set an option.
///
/// # Example
/// ```
/// use duckdb_rs::{environment::Environment, environment::StorageLocation};
/// use duckdb_rs::connection_options::ConfigOptionValue;
///
/// # fn main() -> duckdb_rs::Result<()> {
/// let env = Environment::new()?;
/// let db = env.open(StorageLocation::InMemory)?;
///
/// let option = ConfigOptionValue::new("worker_threads", "2")?;
/// db.set_option(&option)?;
///
/// let resolved = db.get_option("threads")?;
/// assert_eq!(resolved.setting()?, "2");
/// assert_eq!(resolved.canonical_name()?, "threads");
/// # Ok(())
/// # }
/// ```
pub struct ConfigOption {
    /// The owned DuckDB option handle.
    pub handle: ffi::duckdb_v2_option_handle,
}

/// A name and string-encoded setting to apply to DuckDB.
///
/// Create a value with [`ConfigOptionValue::new`], then pass it to a
/// database or connection's `set_option` method. DuckDB resolves the name and
/// validates the setting when it is applied.
pub struct ConfigOptionValue {
    /// The owned DuckDB option handle.
    pub handle: ffi::duckdb_v2_option_handle,
}

impl ConfigOptionValue {
    /// Create an option value from a name and string-encoded setting.
    pub fn new(name: &str, setting: &str) -> Result<ConfigOptionValue> {
        let handle = check_api_call!(
            ffi::duckdb_v2_option_create,
            name.into(),
            setting.into(),
            RET
        )?;

        Ok(ConfigOptionValue { handle })
    }
}

impl Drop for ConfigOptionValue {
    fn drop(&mut self) {
        check_api_call_no_err!(ffi::duckdb_v2_option_destroy, &mut self.handle).unwrap();
    }
}

impl Deref for ConfigOptionValue {
    type Target = ffi::duckdb_v2_option_handle;

    fn deref(&self) -> &Self::Target {
        &self.handle
    }
}

impl Deref for ConfigOption {
    type Target = ffi::duckdb_v2_option_handle;

    fn deref(&self) -> &Self::Target {
        &self.handle
    }
}

impl ConfigOption {
    /// Return the canonical option name.
    pub fn canonical_name(&self) -> Result<String> {
        let name: ffi::duckdb_v2_str =
            check_api_call!(ffi::duckdb_v2_option_get_name, self.handle, RET)?;

        let name: &str = name.into();
        Ok(name.to_string())
    }

    /// Return the option's string-encoded setting.
    pub fn setting(&self) -> Result<String> {
        let setting: ffi::duckdb_v2_str =
            check_api_call!(ffi::duckdb_v2_option_get_setting, self.handle, RET)?;

        let setting: &str = setting.into();
        Ok(setting.to_string())
    }

    /// Return the option's static default setting.
    pub fn default_setting(&self) -> Result<String> {
        let setting: ffi::duckdb_v2_str =
            check_api_call!(ffi::duckdb_v2_option_get_default_setting, self.handle, RET)?;

        let setting: &str = setting.into();
        Ok(setting.to_string())
    }

    /// Return the option's human-readable description.
    pub fn description(&self) -> Result<String> {
        let description: ffi::duckdb_v2_str =
            check_api_call!(ffi::duckdb_v2_option_get_description, self.handle, RET)?;

        let description: &str = description.into();
        Ok(description.to_string())
    }

    /// Return the scopes where DuckDB permits this option to be set.
    ///
    /// The scope is unknown for options without an explicit scope declaration.
    pub fn target_scope(&self) -> Result<TargetScope> {
        check_api_call!(ffi::duckdb_v2_option_get_target_scope, self.handle, RET)?.try_into()
    }

    /// Return the number of registered aliases.
    pub fn alias_count(&self) -> Result<usize> {
        let count: u64 = check_api_call!(ffi::duckdb_v2_option_get_alias_count, self.handle, RET)?;

        Ok(count as usize)
    }

    /// Return the alias at `index`.
    ///
    /// An out-of-range index returns an error.
    pub fn alias(&self, index: usize) -> Result<String> {
        let alias: ffi::duckdb_v2_str = check_api_call!(
            ffi::duckdb_v2_option_get_alias,
            self.handle,
            index as u64,
            RET
        )?;

        let alias: &str = alias.into();
        Ok(alias.to_string())
    }

    /// Return all registered aliases.
    pub fn aliases(&self) -> Result<Vec<String>> {
        let count = self.alias_count()?;
        let mut aliases = Vec::with_capacity(count);

        for i in 0..count {
            let alias = self.alias(i)?;
            aliases.push(alias);
        }

        Ok(aliases)
    }
}

impl Drop for ConfigOption {
    fn drop(&mut self) {
        check_api_call_no_err!(ffi::duckdb_v2_option_destroy, &mut self.handle).unwrap();
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod test {
    use crate::environment::{Environment, StorageLocation};

    use super::*;

    #[test]
    fn test_connection_options_get() -> crate::Result<()> {
        let env = Environment::new()?;
        let db = env.open(StorageLocation::InMemory)?;
        let conn = db.connect()?;

        let options = conn.get_options()?;

        assert_eq!(
            options[0].canonical_name()?,
            conn.get_option_by_index(0)?.canonical_name()?
        );

        Ok(())
    }

    #[test]
    fn test_connection_option() -> crate::Result<()> {
        let env = Environment::new().expect("Failed to create environment");
        let db = env
            .open(StorageLocation::InMemory)
            .expect("Failed to open in-memory database");
        let conn = db.connect().expect("Failed to connect to database");

        let option = ConfigOptionValue::new("worker_threads", &12.to_string()).unwrap();

        conn.set_option(&option, None)
            .expect("Failed to set option");

        let option_real = conn
            .get_option("threads")
            .expect("Failed to get option by name");

        assert_eq!(option_real.setting().unwrap(), "12");
        assert_eq!(option_real.alias_count().unwrap(), 1);

        assert_eq!(option_real.target_scope().unwrap(), TargetScope::Unknown);

        assert_eq!(
            conn.get_option("temp_file_encryption")
                .expect("Failed to get option by name")
                .default_setting()
                .unwrap(),
            "false"
        );
        Ok(())
    }

    #[test]
    fn test_connection_option_from_name() -> crate::Result<()> {
        let env = Environment::new().expect("Failed to create environment");
        let db = env
            .open(StorageLocation::InMemory)
            .expect("Failed to open in-memory database");
        let conn = db.connect()?;

        let db_option = db.get_option("profile_output").unwrap();
        let conn_option = conn.get_option("profile_output")?;

        assert_eq!(db_option.setting().unwrap(), conn_option.setting().unwrap());

        let aliases = db_option.aliases().expect("Failed to get aliases");
        assert_eq!(aliases.len(), 1);
        assert_eq!(aliases[0], "profile_output");

        assert_eq!(db_option.target_scope().unwrap(), TargetScope::Unknown);

        assert_eq!(db_option.canonical_name().unwrap(), "profiling_output");
        assert_eq!(
            db_option.description().unwrap(),
            "The file to which profile output should be saved, or empty to print to the terminal"
        );

        Ok(())
    }
}

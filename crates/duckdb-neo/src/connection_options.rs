//! Configuration options for instances, connections, and callback contexts.

use std::marker::PhantomData;

use crate::connection::SettingScope;
use crate::ffi;

use crate::ffi_str::DuckDBStr;
use crate::raw::RawExt;
use crate::value::Value;
use crate::{AsRaw, DropHandle, FromRaw, Result, error::check_api_call};

/// Describes a registered option's name, aliases, description, default value, and supported/default scopes.
/// Read its current value with [`Config::get_value`].
///
/// # Example
/// ```
/// use duckdb_neo::{
///     FromValue,
///     connection::SettingScope,
///     environment::{Environment, StorageLocation},
/// };
///
/// # fn main() -> duckdb_neo::Result<()> {
/// let env = Environment::new()?;
/// let instance = env.open(StorageLocation::InMemory)?;
///
/// let config = instance.config()?;
/// config.set_text("worker_threads", "2", SettingScope::Global)?;
///
/// let (value, _scope) = config.get_value("threads")?;
/// assert_eq!(i64::from_value(&value)?, Some(2));
///
/// let option = config.get_by_name("worker_threads")?;
/// assert_eq!(option.canonical_name()?, "threads");
/// # Ok(())
/// # }
/// ```
#[derive(Debug, AsRaw, FromRaw, DropHandle)]
#[drop_handle(ffi::duckdb_v2_option_destroy)]
pub struct ConfigOption {
    /// The owned DuckDB option handle.
    pub(crate) handle: ffi::duckdb_v2_option_handle,
}

// SAFETY: the descriptor owns self-contained metadata, read only through `&self`.
unsafe impl Send for ConfigOption {}
// SAFETY: the descriptor owns self-contained metadata, read only through `&self`.
unsafe impl Sync for ConfigOption {}

impl ConfigOption {
    /// Return the canonical option name.
    pub fn canonical_name(&self) -> Result<String> {
        check_api_call!(ffi::duckdb_v2_option_get_name, self.raw(), RET)
            .map(|name| DuckDBStr::from_raw(name).as_str().unwrap_or_default().to_owned())
    }

    /// Return the alias at `index`.
    ///
    /// An out-of-range index returns an error.
    pub fn alias(&self, index: usize) -> Result<String> {
        check_api_call!(ffi::duckdb_v2_option_get_alias, self.raw(), index as u64, RET)
            .map(|alias| DuckDBStr::from_raw(alias).as_str().unwrap_or_default().to_owned())
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

    /// Return the number of aliases registered for this option.
    pub fn alias_count(&self) -> Result<usize> {
        check_api_call!(ffi::duckdb_v2_option_get_alias_count, self.raw(), RET).map(|x| x as usize)
    }

    /// Return the option's human-readable description.
    pub fn description(&self) -> Result<String> {
        check_api_call!(ffi::duckdb_v2_option_get_description, self.raw(), RET)
            .map(|description| DuckDBStr::from_raw(description).as_str().unwrap_or_default().to_owned())
    }

    /// Return the GLOBAL or SESSION scope used for a DEFAULT write.
    pub fn default_scope(&self) -> Result<SettingScope> {
        check_api_call!(ffi::duckdb_v2_option_get_default_scope, self.raw(), RET)?.try_into()
    }

    /// Return the declared default, or a SQL NULL value if none was declared.
    pub fn default_value(&self) -> Result<Value> {
        check_api_call!(ffi::duckdb_v2_option_get_default_value, self.raw(), RET).map(|handle| Value { handle })
    }

    /// Report whether the option is writable at `scope`; DEFAULT means either GLOBAL or SESSION.
    pub fn supports_scope(&self, scope: SettingScope) -> Result<bool> {
        check_api_call!(ffi::duckdb_v2_option_supports_scope, self.raw(), scope.into(), RET)
    }
}

/// The settings of an instance, a connection or a callback context, borrowed from it.
///
/// ```compile_fail,E0597
/// # use duckdb_neo::environment::{Environment, StorageLocation};
/// # fn main() -> duckdb_neo::Result<()> {
/// let instance = Environment::new()?.open(StorageLocation::InMemory)?;
/// let config = {
///     let conn = instance.connect()?;
///     conn.config()?
/// };
/// config.count()?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, AsRaw, FromRaw)]
pub struct Config<'a> {
    pub(crate) handle: ffi::duckdb_v2_config_handle,
    pub(crate) _source: PhantomData<&'a ()>,
}

impl Config<'_> {
    pub(crate) fn new(handle: ffi::duckdb_v2_config_handle) -> Self {
        Config {
            handle,
            _source: PhantomData,
        }
    }

    /// Count registered core and extension options, excluding aliases.
    pub fn count(&self) -> Result<usize> {
        check_api_call!(ffi::duckdb_v2_config_get_option_count, self.raw(), RET).map(|x| x as usize)
    }

    /// Read the effective value and its GLOBAL or SESSION scope; unknown names return INPUT_INVALID.
    pub fn get_value(&self, name: &str) -> Result<(Value, SettingScope)> {
        let mut scope = ffi::DUCKDB_V2_SETTING_SCOPE::DUCKDB_V2_SETTING_SCOPE_MAX_ENUM;

        let handle = check_api_call!(
            ffi::duckdb_v2_config_get_option_value,
            self.raw(),
            &mut name.into(),
            RET,
            &mut scope
        )?;

        Ok((Value { handle }, scope.try_into()?))
    }

    /// Read the effective value and scope, returning `None` for an unknown option name.
    pub fn try_get_value(&self, name: &str) -> Result<Option<(Value, SettingScope)>> {
        match self.get_value(name) {
            Ok(value) => Ok(Some(value)),
            Err(error) if error.code == ffi::DUCKDB_V2_ERROR::DUCKDB_V2_ERROR_INPUT_INVALID => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// Iterate registered descriptors, yielding any count or lookup error as an item.
    /// Avoid registering extension options while iterating: their indices are not stable.
    pub fn options(&self) -> impl Iterator<Item = Result<ConfigOption>> + '_ {
        let (count, error) = match self.count() {
            Ok(count) => (count, None),
            Err(error) => (0, Some(Err(error))),
        };
        error
            .into_iter()
            .chain((0..count).map(|index| self.get_by_index(index)))
    }

    /// Write a cast value; DEFAULT uses the option's default scope for connections and contexts.
    /// Instance configs reject SESSION and write GLOBAL; unsupported scopes return INPUT_INVALID.
    pub fn set_value(&self, name: &str, value: &Value, scope: SettingScope) -> Result<()> {
        check_api_call!(
            ffi::duckdb_v2_config_set_option_value,
            self.raw(),
            &name.into(),
            value.raw(),
            scope.into()
        )
    }

    /// Write a text value with the same casts, scope rules, and errors as [`Self::set_value`].
    pub fn set_text(&self, name: &str, value: &str, scope: SettingScope) -> Result<()> {
        check_api_call!(
            ffi::duckdb_v2_config_set_option_text,
            self.raw(),
            &name.into(),
            &value.into(),
            scope.into()
        )
    }

    /// Describe an option by canonical name or alias; unknown names return INPUT_INVALID.
    pub fn get_by_name(&self, name: &str) -> Result<ConfigOption> {
        check_api_call!(ffi::duckdb_v2_config_get_option_by_name, self.raw(), &name.into(), RET)
            .map(|handle| ConfigOption { handle })
    }

    /// Describe an option by index; out-of-range indices return INPUT_INVALID.
    /// Indices stay stable only while no extension registers new options.
    pub fn get_by_index(&self, index: usize) -> Result<ConfigOption> {
        check_api_call!(ffi::duckdb_v2_config_get_option_by_index, self.raw(), index as u64, RET)
            .map(|handle| ConfigOption { handle })
    }
}

#[cfg(test)]
mod test {
    use crate::{
        FromValue,
        environment::{Environment, StorageLocation},
    };

    use super::*;

    #[test]
    fn test_config_lookup_and_options() -> crate::Result<()> {
        let env = Environment::new()?;
        let instance = env.open(StorageLocation::InMemory)?;
        let config = instance.config()?;
        config.set_text("threads", "2", SettingScope::Global)?;
        let (value, scope) = config.try_get_value("worker_threads")?.expect("registered alias");
        assert_eq!(i64::from_value(&value)?, Some(2));
        assert_eq!(scope, SettingScope::Global);
        assert!(config.try_get_value("unknown_config_option")?.is_none());
        let options = config.options().collect::<crate::Result<Vec<_>>>()?;
        assert_eq!(options.len(), config.count()?);
        assert!(
            options
                .iter()
                .any(|option| option.canonical_name().unwrap() == "threads")
        );
        Ok(())
    }

    #[test]
    fn test_config_option_send_sync() -> crate::Result<()> {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<ConfigOption>();

        let option = {
            let env = Environment::new()?;
            let instance = env.open(StorageLocation::InMemory)?;
            instance.config()?.get_by_name("threads")?
        };
        assert_eq!(
            std::thread::spawn(move || option.canonical_name()).join().unwrap()?,
            "threads"
        );
        Ok(())
    }

    #[test]
    fn test_connection_options_get() -> crate::Result<()> {
        let env = Environment::new()?;
        let instance = env.open(StorageLocation::InMemory)?;
        let conn = instance.connect()?;

        let mut options = vec![];

        let config = instance.config()?;

        for i in 0..config.count()? {
            options.push(config.get_by_index(i)?);
        }

        assert_eq!(
            options[0].canonical_name()?,
            conn.config()?.get_by_index(0)?.canonical_name()?
        );

        Ok(())
    }

    #[test]
    fn test_connection_option() -> crate::Result<()> {
        let env = Environment::new()?;
        let instance = env.open(StorageLocation::InMemory)?;
        let conn = instance.connect()?;

        conn.config()?
            .set_text("worker_threads", &12.to_string(), SettingScope::Global)?;

        let option_real = conn.config()?.get_value("threads")?;

        assert_eq!(i64::from_value(&option_real.0)?, Some(12i64));

        assert_eq!(
            conn.config()?
                .get_by_name("temp_file_encryption")?
                .default_scope()
                .unwrap(),
            SettingScope::Global
        );
        Ok(())
    }

    #[test]
    fn test_connection_option_from_name() -> crate::Result<()> {
        let env = Environment::new()?;
        let instance = env
            .open(StorageLocation::InMemory)
            .expect("Failed to open in-memory database");
        let conn = instance.connect()?;

        let instance_option = instance.config()?.get_value("profile_output")?;
        let conn_option = conn.config()?.get_value("profile_output")?;

        assert_eq!(
            String::from_value(&instance_option.0)?,
            String::from_value(&conn_option.0)?
        );

        let instance_config_option = instance.config()?.get_by_name("profile_output")?;

        let aliases = instance_config_option.aliases()?;
        assert_eq!(aliases.len(), 1);
        assert_eq!(aliases[0], "profile_output");

        assert_eq!(instance_config_option.canonical_name().unwrap(), "profiling_output");
        assert_eq!(
            instance_config_option.description().unwrap(),
            "The file to which profiler output is written. When empty, the output is printed to the terminal."
        );

        Ok(())
    }
}

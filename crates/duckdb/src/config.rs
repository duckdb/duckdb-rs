use super::{ffi, Result};
use crate::error::Error;
use std::{default::Default, ffi::CString, os::raw::c_char, ptr};

use strum::{Display, EnumString};

/// duckdb access mode, default is Automatic
#[derive(Debug, Eq, PartialEq, EnumString, Display)]
pub enum AccessMode {
    /// Access mode of the database AUTOMATIC
    #[strum(to_string = "AUTOMATIC")]
    Automatic,
    /// Access mode of the database READ_ONLY
    #[strum(to_string = "READ_ONLY")]
    ReadOnly,
    /// Access mode of the database READ_WRITE
    #[strum(to_string = "READ_WRITE")]
    ReadWrite,
}

/// duckdb default order, default is Asc
#[derive(Debug, Eq, PartialEq, EnumString, Display)]
pub enum DefaultOrder {
    /// The order type, ASC
    #[strum(to_string = "ASC")]
    Asc,
    /// The order type, DESC
    #[strum(to_string = "DESC")]
    Desc,
}

/// duckdb default null order, default is nulls first
#[derive(Debug, Eq, PartialEq, EnumString, Display)]
pub enum DefaultNullOrder {
    /// Null ordering, NullsFirst
    #[strum(to_string = "NULLS_FIRST")]
    NullsFirst,
    /// Null ordering, NullsLast
    #[strum(to_string = "NULLS_LAST")]
    NullsLast,
}

/// duckdb configuration
/// Refer to <https://github.com/duckdb/duckdb/blob/master/src/main/config.cpp>
#[derive(Default)]
pub struct Config {
    config: Option<ffi::duckdb_config>,
}

impl Config {
    pub(crate) fn duckdb_config(&self) -> ffi::duckdb_config {
        self.config.unwrap_or(std::ptr::null_mut() as ffi::duckdb_config)
    }

    /// enable autoload extensions
    pub fn enable_autoload_extension(mut self, enabled: bool) -> Result<Config> {
        self.set("autoinstall_known_extensions", &(enabled as i32).to_string())?;
        self.set("autoload_known_extensions", &(enabled as i32).to_string())?;
        Ok(self)
    }

    /// Access mode of the database ([AUTOMATIC], READ_ONLY or READ_WRITE)
    pub fn access_mode(mut self, mode: AccessMode) -> Result<Config> {
        self.set("access_mode", &mode.to_string())?;
        Ok(self)
    }

    /// Metadata from DuckDB callers
    pub fn custom_user_agent(mut self, custom_user_agent: &str) -> Result<Config> {
        self.set("custom_user_agent", custom_user_agent)?;
        Ok(self)
    }

    /// The order type used when none is specified ([ASC] or DESC)
    pub fn default_order(mut self, order: DefaultOrder) -> Result<Config> {
        self.set("default_order", &order.to_string())?;
        Ok(self)
    }

    /// Null ordering used when none is specified ([NULLS_FIRST] or NULLS_LAST)
    pub fn default_null_order(mut self, null_order: DefaultNullOrder) -> Result<Config> {
        self.set("default_null_order", &null_order.to_string())?;
        Ok(self)
    }

    /// Allow the database to access external state (through e.g. COPY TO/FROM, CSV readers, pandas replacement scans, etc)
    pub fn enable_external_access(mut self, enabled: bool) -> Result<Config> {
        self.set("enable_external_access", &enabled.to_string())?;
        Ok(self)
    }

    /// Whether or not object cache is used to cache e.g. Parquet metadata
    pub fn enable_object_cache(mut self, enabled: bool) -> Result<Config> {
        self.set("enable_object_cache", &enabled.to_string())?;
        Ok(self)
    }

    /// Allow to load third-party duckdb extensions.
    pub fn allow_unsigned_extensions(mut self) -> Result<Config> {
        self.set("allow_unsigned_extensions", "true")?;
        Ok(self)
    }

    /// The maximum memory of the system (e.g. 1GB)
    pub fn max_memory(mut self, memory: &str) -> Result<Config> {
        self.set("max_memory", memory)?;
        Ok(self)
    }

    /// The number of total threads used by the system
    pub fn threads(mut self, thread_num: i64) -> Result<Config> {
        self.set("threads", &thread_num.to_string())?;
        Ok(self)
    }

    /// Add any setting to the config. DuckDB will return an error if the setting is unknown or
    /// otherwise invalid.
    pub fn with(mut self, key: impl AsRef<str>, value: impl AsRef<str>) -> Result<Config> {
        self.set(key.as_ref(), value.as_ref())?;
        Ok(self)
    }

    fn set(&mut self, key: &str, value: &str) -> Result<()> {
        if self.config.is_none() {
            let mut config: ffi::duckdb_config = ptr::null_mut();
            let state = unsafe { ffi::duckdb_create_config(&mut config) };
            assert_eq!(state, ffi::DuckDBSuccess);
            self.config = Some(config);
        }
        let c_key = CString::new(key).unwrap();
        let c_value = CString::new(value).unwrap();
        let state = unsafe {
            ffi::duckdb_set_config(
                self.config.unwrap(),
                c_key.as_ptr() as *const c_char,
                c_value.as_ptr() as *const c_char,
            )
        };
        if state != ffi::DuckDBSuccess {
            return Err(Error::DuckDBFailure(
                ffi::Error::new(state),
                Some(format!("set {key}:{value} error")),
            ));
        }
        Ok(())
    }
}

impl Drop for Config {
    fn drop(&mut self) {
        if self.config.is_some() {
            unsafe { ffi::duckdb_destroy_config(&mut self.config.unwrap()) };
        }
    }
}

#[cfg(test)]
mod test {
    use crate::{types::Value, Config, Connection, Result};

    #[test]
    fn test_default_config() -> Result<()> {
        let config = Config::default();
        let db = Connection::open_in_memory_with_flags(config)?;
        db.execute_batch("CREATE TABLE foo(x Text)")?;

        let mut stmt = db.prepare("INSERT INTO foo(x) VALUES (?)")?;
        stmt.execute([&"a"])?;
        stmt.execute([&"b"])?;
        stmt.execute([&"c"])?;
        stmt.execute([Value::Null])?;

        let val: Result<Vec<Option<String>>> = db
            .prepare("SELECT x FROM foo ORDER BY x")?
            .query_and_then([], |row| row.get(0))?
            .collect();
        let val = val?;
        let mut iter = val.iter();
        assert_eq!(val.len(), 4);
        assert_eq!(iter.next().unwrap().as_ref().unwrap(), "a");
        assert_eq!(iter.next().unwrap().as_ref().unwrap(), "b");
        assert_eq!(iter.next().unwrap().as_ref().unwrap(), "c");
        assert!(iter.next().unwrap().is_none());
        assert_eq!(iter.next(), None);

        Ok(())
    }

    #[test]
    fn test_all_config() -> Result<()> {
        let config = Config::default()
            .access_mode(crate::AccessMode::ReadWrite)?
            .default_null_order(crate::DefaultNullOrder::NullsLast)?
            .default_order(crate::DefaultOrder::Desc)?
            .enable_external_access(true)?
            .enable_object_cache(false)?
            .enable_autoload_extension(true)?
            .allow_unsigned_extensions()?
            .custom_user_agent("test_user_agent")?
            .max_memory("2GB")?
            .threads(4)?
            .with("preserve_insertion_order", "true")?;

        let db = Connection::open_in_memory_with_flags(config)?;
        db.execute_batch("CREATE TABLE foo(x Text)")?;

        let mut stmt = db.prepare("INSERT INTO foo(x) VALUES (?)")?;
        stmt.execute([&"a"])?;
        stmt.execute([&"b"])?;
        stmt.execute([&"c"])?;
        stmt.execute([Value::Null])?;

        let val: Result<Vec<Option<String>>> = db
            .prepare("SELECT x FROM foo ORDER BY x")?
            .query_and_then([], |row| row.get(0))?
            .collect();
        let val = val?;
        let mut iter = val.iter();
        assert_eq!(iter.next().unwrap().as_ref().unwrap(), "c");
        assert_eq!(iter.next().unwrap().as_ref().unwrap(), "b");
        assert_eq!(iter.next().unwrap().as_ref().unwrap(), "a");
        assert!(iter.next().unwrap().is_none());
        assert_eq!(iter.next(), None);

        let user_agent: Result<String> = db.query_row("PRAGMA USER_AGENT", [], |row| row.get(0));
        let user_agent = user_agent.unwrap();
        assert!(&user_agent.ends_with("rust test_user_agent"));

        Ok(())
    }

    #[test]
    fn test_invalid_setting() -> Result<()> {
        let config = Config::default().with("some-invalid-setting", "true")?;
        let res = Connection::open_in_memory_with_flags(config);
        assert_eq!(
            res.unwrap_err().to_string(),
            "Invalid Input Error: Unrecognized configuration property \"some-invalid-setting\""
        );
        Ok(())
    }
}

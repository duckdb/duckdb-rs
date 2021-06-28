//! Configure database connections

use crate::{Connection, Result};

/// Database Connection Configuration Options
/// See [Database Connection Configuration Options](https://sqlite.org/c3ref/c_dbconfig_enable_fkey.html) for details.
#[repr(i32)]
#[allow(non_snake_case, non_camel_case_types)]
#[non_exhaustive]
#[allow(clippy::upper_case_acronyms)]
pub enum DbConfig {
    //SQLITE_DBCONFIG_MAINDBNAME = 1000, /* const char* */
    //SQLITE_DBCONFIG_LOOKASIDE = 1001,  /* void* int int */
    /// Enable or disable the enforcement of foreign key constraints.
    SQLITE_DBCONFIG_ENABLE_FKEY = 1002,
    /// Enable or disable triggers.
    SQLITE_DBCONFIG_ENABLE_TRIGGER = 1003,
    /// Enable or disable the fts3_tokenizer() function which is part of the
    /// FTS3 full-text search engine extension.
    SQLITE_DBCONFIG_ENABLE_FTS3_TOKENIZER = 1004, // 3.12.0
    //SQLITE_DBCONFIG_ENABLE_LOAD_EXTENSION = 1005,
    /// In WAL mode, enable or disable the checkpoint operation before closing
    /// the connection.
    SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE = 1006, // 3.16.2
    /// Activates or deactivates the query planner stability guarantee (QPSG).
    SQLITE_DBCONFIG_ENABLE_QPSG = 1007, // 3.20.0
    /// Includes or excludes output for any operations performed by trigger
    /// programs from the output of EXPLAIN QUERY PLAN commands.
    SQLITE_DBCONFIG_TRIGGER_EQP = 1008, // 3.22.0
    //SQLITE_DBCONFIG_RESET_DATABASE = 1009,
    /// Activates or deactivates the "defensive" flag for a database connection.
    SQLITE_DBCONFIG_DEFENSIVE = 1010, // 3.26.0
}

impl Connection {
    /// Returns the current value of a `config`.
    ///
    /// - SQLITE_DBCONFIG_ENABLE_FKEY: return `false` or `true` to indicate
    ///   whether FK enforcement is off or on
    /// - SQLITE_DBCONFIG_ENABLE_TRIGGER: return `false` or `true` to indicate
    ///   whether triggers are disabled or enabled
    /// - SQLITE_DBCONFIG_ENABLE_FTS3_TOKENIZER: return `false` or `true` to
    ///   indicate whether fts3_tokenizer are disabled or enabled
    /// - SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE: return `false` to indicate
    ///   checkpoints-on-close are not disabled or `true` if they are
    /// - SQLITE_DBCONFIG_ENABLE_QPSG: return `false` or `true` to indicate
    ///   whether the QPSG is disabled or enabled
    /// - SQLITE_DBCONFIG_TRIGGER_EQP: return `false` to indicate
    ///   output-for-trigger are not disabled or `true` if it is
    #[inline]
    pub fn db_config(&self, _: DbConfig) -> Result<bool> {
        panic!("not supported")
    }

    /// Make configuration changes to a database connection
    ///
    /// - SQLITE_DBCONFIG_ENABLE_FKEY: `false` to disable FK enforcement, `true`
    ///   to enable FK enforcement
    /// - SQLITE_DBCONFIG_ENABLE_TRIGGER: `false` to disable triggers, `true` to
    ///   enable triggers
    /// - SQLITE_DBCONFIG_ENABLE_FTS3_TOKENIZER: `false` to disable
    ///   fts3_tokenizer(), `true` to enable fts3_tokenizer()
    /// - SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE: `false` (the default) to enable
    ///   checkpoints-on-close, `true` to disable them
    /// - SQLITE_DBCONFIG_ENABLE_QPSG: `false` to disable the QPSG, `true` to
    ///   enable QPSG
    /// - SQLITE_DBCONFIG_TRIGGER_EQP: `false` to disable output for trigger
    ///   programs, `true` to enable it
    #[inline]
    pub fn set_db_config(&self, _: DbConfig, _: bool) -> Result<bool> {
        panic!("not supported")
    }
}

#[cfg(test)]
mod test {
    use super::DbConfig;
    use crate::{Connection, Result};

    #[test]
    #[ignore]
    fn test_db_config() -> Result<()> {
        let db = Connection::open_in_memory()?;

        let opposite = !db.db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_FKEY)?;
        assert_eq!(
            db.set_db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_FKEY, opposite),
            Ok(opposite)
        );
        assert_eq!(db.db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_FKEY), Ok(opposite));

        let opposite = !db.db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_TRIGGER)?;
        assert_eq!(
            db.set_db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_TRIGGER, opposite),
            Ok(opposite)
        );
        assert_eq!(db.db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_TRIGGER), Ok(opposite));
        Ok(())
    }
}

//! Pragma helpers

use std::ops::Deref;

use crate::error::Error;
use crate::ffi;
use crate::types::{ToSql, ToSqlOutput, ValueRef};
use crate::{Connection, DatabaseName, Result, Row};

pub struct Sql {
    buf: String,
}

impl Sql {
    pub fn new() -> Sql {
        Sql { buf: String::new() }
    }

    pub fn push_pragma(&mut self, schema_name: Option<DatabaseName<'_>>, pragma_name: &str) -> Result<()> {
        self.push_keyword("PRAGMA")?;
        self.push_space();
        if let Some(schema_name) = schema_name {
            self.push_schema_name(schema_name);
            self.push_dot();
        }
        self.push_keyword(pragma_name)
    }

    pub fn push_keyword(&mut self, keyword: &str) -> Result<()> {
        if !keyword.is_empty() && is_identifier(keyword) {
            self.buf.push_str(keyword);
            Ok(())
        } else {
            Err(Error::DuckDBFailure(
                ffi::Error::new(ffi::DuckDBError),
                Some(format!("Invalid keyword \"{}\"", keyword)),
            ))
        }
    }

    pub fn push_schema_name(&mut self, schema_name: DatabaseName<'_>) {
        match schema_name {
            DatabaseName::Main => self.buf.push_str("main"),
            DatabaseName::Temp => self.buf.push_str("temp"),
            DatabaseName::Attached(s) => self.push_identifier(s),
        };
    }

    pub fn push_identifier(&mut self, s: &str) {
        if is_identifier(s) {
            self.buf.push_str(s);
        } else {
            self.wrap_and_escape(s, '"');
        }
    }

    pub fn push_value(&mut self, value: &dyn ToSql) -> Result<()> {
        let value = value.to_sql()?;
        let value = match value {
            ToSqlOutput::Borrowed(v) => v,
            ToSqlOutput::Owned(ref v) => ValueRef::from(v),
        };
        match value {
            ValueRef::BigInt(i) => {
                self.push_int(i);
            }
            ValueRef::Double(r) => {
                self.push_real(r);
            }
            ValueRef::Text(s) => {
                let s = std::str::from_utf8(s)?;
                self.push_string_literal(s);
            }
            _ => {
                return Err(Error::DuckDBFailure(
                    ffi::Error::new(ffi::DuckDBError),
                    Some(format!("Unsupported value \"{:?}\"", value)),
                ));
            }
        };
        Ok(())
    }

    pub fn push_string_literal(&mut self, s: &str) {
        self.wrap_and_escape(s, '\'');
    }

    pub fn push_int(&mut self, i: i64) {
        self.buf.push_str(&i.to_string());
    }

    pub fn push_real(&mut self, f: f64) {
        self.buf.push_str(&f.to_string());
    }

    pub fn push_space(&mut self) {
        self.buf.push(' ');
    }

    pub fn push_dot(&mut self) {
        self.buf.push('.');
    }

    pub fn push_equal_sign(&mut self) {
        self.buf.push('=');
    }

    pub fn open_brace(&mut self) {
        self.buf.push('(');
    }

    pub fn close_brace(&mut self) {
        self.buf.push(')');
    }

    pub fn as_str(&self) -> &str {
        &self.buf
    }

    fn wrap_and_escape(&mut self, s: &str, quote: char) {
        self.buf.push(quote);
        let chars = s.chars();
        for ch in chars {
            // escape `quote` by doubling it
            if ch == quote {
                self.buf.push(ch);
            }
            self.buf.push(ch)
        }
        self.buf.push(quote);
    }
}

impl Deref for Sql {
    type Target = str;

    fn deref(&self) -> &str {
        self.as_str()
    }
}

impl Connection {
    /// Query the current value of `pragma_name`.
    ///
    /// Some pragmas will return multiple rows/values which cannot be retrieved
    /// with this method.
    ///
    /// Prefer [PRAGMA function](https://sqlite.org/pragma.html#pragfunc) introduced in DuckDB 3.20:
    /// `SELECT user_version FROM pragma_user_version;`
    pub fn pragma_query_value<T, F>(&self, schema_name: Option<DatabaseName<'_>>, pragma_name: &str, f: F) -> Result<T>
    where
        F: FnOnce(&Row<'_>) -> Result<T>,
    {
        let mut query = Sql::new();
        query.push_pragma(schema_name, pragma_name)?;
        self.query_row(&query, [], f)
    }

    /// Query the current rows/values of `pragma_name`.
    ///
    /// Prefer [PRAGMA function](https://sqlite.org/pragma.html#pragfunc) introduced in DuckDB 3.20:
    /// `SELECT * FROM pragma_collation_list;`
    pub fn pragma_query<F>(&self, schema_name: Option<DatabaseName<'_>>, pragma_name: &str, mut f: F) -> Result<()>
    where
        F: FnMut(&Row<'_>) -> Result<()>,
    {
        let mut query = Sql::new();
        query.push_pragma(schema_name, pragma_name)?;
        let mut stmt = self.prepare(&query)?;
        let mut rows = stmt.query([])?;
        while let Some(result_row) = rows.next()? {
            let row = result_row;
            f(&row)?;
        }
        Ok(())
    }

    /// Query the current value(s) of `pragma_name` associated to
    /// `pragma_value`.
    ///
    /// This method can be used with query-only pragmas which need an argument
    /// (e.g. `table_info('one_tbl')`) or pragmas which returns value(s)
    /// (e.g. `integrity_check`).
    ///
    /// Prefer [PRAGMA function](https://sqlite.org/pragma.html#pragfunc) introduced in DuckDB 3.20:
    /// `SELECT * FROM pragma_table_info(?);`
    pub fn pragma<F>(
        &self,
        schema_name: Option<DatabaseName<'_>>,
        pragma_name: &str,
        pragma_value: &dyn ToSql,
        mut f: F,
    ) -> Result<()>
    where
        F: FnMut(&Row<'_>) -> Result<()>,
    {
        let mut sql = Sql::new();
        sql.push_pragma(schema_name, pragma_name)?;
        // The argument may be either in parentheses
        // or it may be separated from the pragma name by an equal sign.
        // The two syntaxes yield identical results.
        sql.open_brace();
        sql.push_value(pragma_value)?;
        sql.close_brace();
        let mut stmt = self.prepare(&sql)?;
        let mut rows = stmt.query([])?;
        while let Some(result_row) = rows.next()? {
            let row = result_row;
            f(&row)?;
        }
        Ok(())
    }

    /// Set a new value to `pragma_name`.
    ///
    /// Some pragmas will return the updated value which cannot be retrieved
    /// with this method.
    pub fn pragma_update(
        &self,
        schema_name: Option<DatabaseName<'_>>,
        pragma_name: &str,
        pragma_value: &dyn ToSql,
    ) -> Result<()> {
        let mut sql = Sql::new();
        sql.push_pragma(schema_name, pragma_name)?;
        // The argument may be either in parentheses
        // or it may be separated from the pragma name by an equal sign.
        // The two syntaxes yield identical results.
        sql.push_equal_sign();
        sql.push_value(pragma_value)?;
        self.execute_batch(&sql)
    }

    /// Set a new value to `pragma_name` and return the updated value.
    ///
    /// Only few pragmas automatically return the updated value.
    pub fn pragma_update_and_check<F, T>(
        &self,
        schema_name: Option<DatabaseName<'_>>,
        pragma_name: &str,
        pragma_value: &dyn ToSql,
        f: F,
    ) -> Result<T>
    where
        F: FnOnce(&Row<'_>) -> Result<T>,
    {
        let mut sql = Sql::new();
        sql.push_pragma(schema_name, pragma_name)?;
        // The argument may be either in parentheses
        // or it may be separated from the pragma name by an equal sign.
        // The two syntaxes yield identical results.
        sql.push_equal_sign();
        sql.push_value(pragma_value)?;
        self.query_row(&sql, [], f)
    }
}

fn is_identifier(s: &str) -> bool {
    let chars = s.char_indices();
    for (i, ch) in chars {
        if i == 0 {
            if !is_identifier_start(ch) {
                return false;
            }
        } else if !is_identifier_continue(ch) {
            return false;
        }
    }
    true
}

fn is_identifier_start(c: char) -> bool {
    ('A'..='Z').contains(&c) || c == '_' || ('a'..='z').contains(&c) || c > '\x7F'
}

fn is_identifier_continue(c: char) -> bool {
    c == '$'
        || ('0'..='9').contains(&c)
        || ('A'..='Z').contains(&c)
        || c == '_'
        || ('a'..='z').contains(&c)
        || c > '\x7F'
}

#[cfg(test)]
mod test {
    use super::Sql;
    use crate::pragma;
    use crate::{Connection, DatabaseName, Result};

    #[test]
    fn pragma_query_value() -> Result<()> {
        let db = Connection::open_in_memory()?;
        let version: String = db.pragma_query_value(None, "version", |row| row.get(0))?;
        assert!(!version.is_empty());
        Ok(())
    }

    #[test]
    #[ignore = "not supported"]
    fn pragma_query_with_schema() -> Result<()> {
        let db = Connection::open_in_memory()?;
        let mut version = "".to_string();
        db.pragma_query(Some(DatabaseName::Main), "version", |row| {
            version = row.get(0)?;
            Ok(())
        })?;
        assert!(!version.is_empty());
        Ok(())
    }

    #[test]
    fn pragma() -> Result<()> {
        let db = Connection::open_in_memory()?;
        let mut columns = Vec::new();
        db.pragma(None, "table_info", &"sqlite_master", |row| {
            let column: String = row.get(1)?;
            columns.push(column);
            Ok(())
        })?;
        assert_eq!(5, columns.len());
        Ok(())
    }

    #[test]
    fn pragma_update() -> Result<()> {
        let db = Connection::open_in_memory()?;
        db.pragma_update(None, "explain_output", &"PHYSICAL_ONLY")
    }

    #[test]
    #[ignore = "don't support query pragma"]
    fn test_pragma_update_and_check() -> Result<()> {
        let db = Connection::open_in_memory()?;
        let journal_mode: String =
            db.pragma_update_and_check(None, "explain_output", &"OPTIMIZED_ONLY", |row| row.get(0))?;
        assert_eq!("OPTIMIZED_ONLY", &journal_mode);
        Ok(())
    }

    #[test]
    fn is_identifier() {
        assert!(pragma::is_identifier("full"));
        assert!(pragma::is_identifier("r2d2"));
        assert!(!pragma::is_identifier("sp ce"));
        assert!(!pragma::is_identifier("semi;colon"));
    }

    #[test]
    fn double_quote() {
        let mut sql = Sql::new();
        sql.push_schema_name(DatabaseName::Attached(r#"schema";--"#));
        assert_eq!(r#""schema"";--""#, sql.as_str());
    }

    #[test]
    fn wrap_and_escape() {
        let mut sql = Sql::new();
        sql.push_string_literal("value'; --");
        assert_eq!("'value''; --'", sql.as_str());
    }

    #[test]
    #[ignore]
    fn test_locking_mode() -> Result<()> {
        let db = Connection::open_in_memory()?;
        let r = db.pragma_update(None, "locking_mode", &"exclusive");
        if cfg!(feature = "extra_check") {
            r.unwrap_err();
        } else {
            r?;
        }
        Ok(())
    }
}

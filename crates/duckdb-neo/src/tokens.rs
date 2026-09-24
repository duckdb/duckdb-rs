//! SQL tokenization into typed lexemes with source offsets.

use libduckdb_sys::v2::DuckDBStr;

use crate::{
    Result,
    connection::Connection,
    enums::TokenType,
    error::{check_api_call, check_api_call_no_err},
    ffi,
};

/// One lexical token from a tokenized SQL statement.
#[derive(Debug)]
pub struct TokenData {
    /// The token's lexical class.
    pub token_type: TokenType,
    /// Byte offset of the token's first byte in the source SQL.
    pub start: usize,
    /// Length of the token in bytes.
    pub length: usize,
}

/// A streaming iterator over the tokens of an SQL string.
///
/// Tokens are produced lazily by [`Iterator::next`], each carrying its
/// [`TokenType`], byte offset, and length back into the source SQL. The
/// iterator is created with [`SqlTokenIterator::new`]; tokenizing copies every
/// token up front, so the iterator borrows neither the SQL text nor the
/// connection.
pub struct SqlTokenIterator {
    handle: ffi::duckdb_v2_token_iterator_handle,
}

impl Drop for SqlTokenIterator {
    fn drop(&mut self) {
        check_api_call_no_err!(ffi::duckdb_v2_token_iterator_destroy, &mut self.handle).unwrap()
    }
}

impl SqlTokenIterator {
    /// Tokenize `sql`, returning a lazy iterator over its lexical tokens.
    ///
    /// The tokenizer consumes the SQL text immediately; tokens are then yielded
    /// on demand as the iterator is advanced.
    pub fn new(conn: &Connection, sql: &str) -> Result<SqlTokenIterator> {
        Ok(Self {
            handle: check_api_call!(ffi::duckdb_v2_tokenize_sql, **conn, DuckDBStr::from(sql), RET)?,
        })
    }

    /// Whether the statement ends inside an unterminated string or quoted identifier.
    pub fn ends_undetermined(&self) -> Result<bool> {
        check_api_call!(ffi::duckdb_v2_token_iterator_ends_unterminated, self.handle, RET)
    }
}

impl Iterator for SqlTokenIterator {
    type Item = Result<TokenData>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut token_type = ffi::DUCKDB_V2_TOKEN_TYPE::DUCKDB_V2_TOKEN_TYPE_INVALID;
        let mut start: u64 = 0;
        let mut length: u64 = 0;

        if let Err(e) = check_api_call!(
            ffi::duckdb_v2_token_iterator_next,
            self.handle,
            &mut token_type,
            &mut start,
            &mut length,
        ) {
            return Some(Err(e));
        }

        if token_type == ffi::DUCKDB_V2_TOKEN_TYPE::DUCKDB_V2_TOKEN_TYPE_END_OF_INPUT {
            return None;
        }

        let token_type = match token_type.try_into() {
            Ok(token_type) => token_type,
            Err(e) => return Some(Err(e)),
        };

        Some(Ok(TokenData {
            token_type,
            start: start as usize,
            length: length as usize,
        }))
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        enums::TokenType,
        environment::{Environment, StorageLocation},
        tokens::SqlTokenIterator,
    };

    #[test]
    fn test_tokenizer() -> crate::Result<()> {
        let env: Environment = Environment::new()?;
        let db = env.open(StorageLocation::InMemory)?;
        let conn = db.connect()?;

        let sql = "SELECT * FROM test_data(10)";
        let token_iterator = SqlTokenIterator::new(&conn, sql)?;

        let tokens = token_iterator.map(|x| x.unwrap()).collect::<Vec<_>>();

        let types = tokens.iter().map(|t| t.token_type.clone()).collect::<Vec<_>>();
        assert_eq!(
            types,
            vec![
                TokenType::Keyword,       // SELECT
                TokenType::Operator,      // *
                TokenType::Keyword,       // FROM
                TokenType::Identifier,    // test_data
                TokenType::Operator,      // (
                TokenType::NumberLiteral, // 10
                TokenType::Operator,      // )
            ]
        );

        // Each token's (start, length) must point back at the matching lexeme in the source.
        let lexemes = tokens
            .iter()
            .map(|t| &sql[t.start..(t.start + t.length)])
            .collect::<Vec<_>>();
        assert_eq!(lexemes, vec!["SELECT", "*", "FROM", "test_data", "(", "10", ")"]);

        Ok(())
    }
}

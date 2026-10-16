//! SQL tokenization into typed lexemes with source offsets.

use crate::{DropHandle, Result, connection::Connection, enums::TokenType, error::check_api_call, ffi, raw::RawExt};

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
#[derive(Debug, DropHandle)]
#[drop_handle(ffi::duckdb_v2_token_iterator_destroy)]
pub struct SqlTokenIterator {
    handle: ffi::duckdb_v2_token_iterator_handle,
}

impl SqlTokenIterator {
    /// Tokenize `sql`, returning a lazy iterator over its lexical tokens.
    ///
    /// The tokenizer consumes the SQL text immediately; tokens are then yielded
    /// on demand as the iterator is advanced.
    pub fn new(conn: &Connection, sql: impl AsRef<str>) -> Result<SqlTokenIterator> {
        check_api_call!(ffi::duckdb_v2_tokenize_sql, conn.raw(), &sql.as_ref().into(), RET)
            .map(|handle| Self { handle })
    }

    /// Whether the input ends inside an open string, quoted identifier, block comment or
    /// dollar-quoted string, or in a line comment with no trailing newline.
    pub fn ends_unterminated(&self) -> Result<bool> {
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

        let types = tokens.iter().map(|t| t.token_type).collect::<Vec<_>>();
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

    #[test]
    fn tokenizer_accepts_owned_string() -> crate::Result<()> {
        let env = Environment::new()?;
        let conn = env.open(StorageLocation::InMemory)?.connect()?;
        let tokens = SqlTokenIterator::new(&conn, String::from("SELECT 1"))?.collect::<crate::Result<Vec<_>>>()?;
        assert_eq!(tokens.len(), 2);
        assert_eq!(tokens[0].token_type, TokenType::Keyword);
        assert_eq!(tokens[1].token_type, TokenType::NumberLiteral);
        Ok(())
    }

    #[test]
    fn test_ends_unterminated() -> crate::Result<()> {
        let env: Environment = Environment::new()?;
        let db = env.open(StorageLocation::InMemory)?;
        let conn = db.connect()?;

        let cases = [
            ("SELECT 'abc", true),
            ("SELECT \"abc", true),
            ("SELECT /* abc", true),
            ("SELECT $$abc", true),
            ("SELECT 1 -- abc", true),
            ("SELECT 'abc'", false),
            ("SELECT \"abc\"", false),
            ("SELECT /* abc */", false),
            ("SELECT $$abc$$", false),
            ("SELECT 1 -- abc\n", false),
            ("", false),
            ("  ", false),
        ];
        for (sql, expected) in cases {
            let mut tokens = SqlTokenIterator::new(&conn, sql)?;
            assert_eq!(tokens.ends_unterminated()?, expected, "before iterating {sql:?}");
            if let Some(token) = tokens.next() {
                token?;
            }
            assert_eq!(tokens.ends_unterminated()?, expected, "after one token of {sql:?}");
            for token in tokens.by_ref() {
                token?;
            }
            assert_eq!(tokens.ends_unterminated()?, expected, "after draining {sql:?}");
        }

        Ok(())
    }
}

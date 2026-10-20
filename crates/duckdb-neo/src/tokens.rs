//! SQL tokenization into typed lexemes with source offsets.

use crate::{DropHandle, Result, connection::Connection, enums::TokenType, error::check_api_call, ffi, raw::RawExt};

/// One lexical token from a tokenized SQL statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokenData {
    /// The token's lexical class.
    pub token_type: TokenType,
    /// Byte offset of the token's first byte in the source SQL.
    pub start: usize,
    /// Length of the token in bytes.
    pub length: usize,
}

impl TokenData {
    /// Return this token's text, or `None` if its byte range is invalid for `sql`.
    pub fn lexeme<'a>(&self, sql: &'a str) -> Option<&'a str> {
        sql.get(self.start..self.start.checked_add(self.length)?)
    }
}

/// An iterator over stored SQL tokens in input order.
///
/// [`SqlTokenIterator::new`] tokenizes immediately; the iterator then yields stored
/// tokens without borrowing the SQL text or connection.
#[derive(Debug, DropHandle)]
#[drop_handle(ffi::duckdb_v2_token_iterator_destroy)]
pub struct SqlTokenIterator {
    handle: ffi::duckdb_v2_token_iterator_handle,
}

// SAFETY: the handle owns only token data and has no connection or thread affinity.
unsafe impl Send for SqlTokenIterator {}
// SAFETY: shared access reads only the immutable unterminated flag; advancing requires &mut self.
unsafe impl Sync for SqlTokenIterator {}

impl SqlTokenIterator {
    /// Tokenize `sql` immediately, returning an iterator over the stored tokens.
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

impl std::iter::FusedIterator for SqlTokenIterator {}

#[cfg(test)]
mod tests {
    use crate::{
        enums::TokenType,
        environment::{Environment, StorageLocation},
        tokens::{SqlTokenIterator, TokenData},
    };

    #[test]
    fn token_data_lexeme() {
        let token = TokenData {
            token_type: TokenType::Identifier,
            start: 7,
            length: 4,
        };
        assert_eq!(token.lexeme("SELECT 🦆"), Some("🦆"));
        assert_eq!(token.lexeme("SELECT"), None);
        assert_eq!(token.clone(), token);
    }

    #[test]
    fn token_iterator_is_thread_safe_and_fused() -> crate::Result<()> {
        fn assert_traits<T: Send + Sync + std::iter::FusedIterator>() {}
        assert_traits::<SqlTokenIterator>();

        let env = Environment::new()?;
        let conn = env.open(StorageLocation::InMemory)?.connect()?;
        let mut tokens = SqlTokenIterator::new(&conn, "SELECT")?;
        assert_eq!(tokens.next().transpose()?.unwrap().token_type, TokenType::Keyword);
        assert!(tokens.next().is_none());
        assert!(tokens.next().is_none());
        Ok(())
    }

    #[test]
    fn test_tokenizer() -> crate::Result<()> {
        let env: Environment = Environment::new()?;
        let instance = env.open(StorageLocation::InMemory)?;
        let conn = instance.connect()?;

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
        let instance = env.open(StorageLocation::InMemory)?;
        let conn = instance.connect()?;

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

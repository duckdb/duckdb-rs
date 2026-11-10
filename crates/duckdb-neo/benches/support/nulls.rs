//! Deterministic, independently salted NULL patterns shared by the function benchmark ports.

use duckdb_neo::{Error, Result};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Nulls(u8);

impl Nulls {
    pub fn new(percent: u8) -> Result<Self> {
        if percent > 100 {
            return Err(Error::invalid_input(
                "NEO_BENCH_NULL_PERCENT must be an integer from 0 to 100",
            ));
        }
        Ok(Self(percent))
    }

    pub fn from_env() -> Result<Self> {
        match std::env::var("NEO_BENCH_NULL_PERCENT") {
            Ok(value) => Self::parse(&value),
            Err(std::env::VarError::NotPresent) => Self::new(10),
            Err(error) => Err(Error::invalid_input(error.to_string())),
        }
    }

    fn parse(value: &str) -> Result<Self> {
        if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(Error::invalid_input(
                "NEO_BENCH_NULL_PERCENT must be an integer from 0 to 100",
            ));
        }
        Self::new(
            value
                .parse()
                .map_err(|_| Error::invalid_input("NEO_BENCH_NULL_PERCENT must be an integer from 0 to 100"))?,
        )
    }

    /// Salt each field separately so parent NULLs do not hide all nested NULL values.
    pub fn expression(self, value: &str, seed: u8) -> String {
        format!(
            "CASE WHEN hash(i::UBIGINT, {seed}::UBIGINT) % 100 < {} THEN NULL ELSE {value} END",
            self.0
        )
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn accepts_integer_percentages() {
        use super::Nulls;

        for value in ["0", "10", "100", "0010"] {
            assert!(Nulls::parse(value).is_ok(), "{value}");
        }
        assert_eq!(Nulls::parse("0010").unwrap(), Nulls::new(10).unwrap());
    }

    #[test]
    fn rejects_invalid_percentages() {
        use super::Nulls;

        for value in ["", "-1", "+10", "101", "256", "10.5", "NaN", " 10", "10 ", "１０"] {
            assert!(Nulls::parse(value).is_err(), "{value}");
        }
        assert!(Nulls::new(101).is_err());
    }
}

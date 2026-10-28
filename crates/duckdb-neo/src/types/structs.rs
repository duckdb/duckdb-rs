//! The `STRUCT` logical type.

use crate::{enums::LogicalTypeID, vector::VectorElement};

/// Reads `STRUCT` and `TUPLE` vectors as child fields.
#[derive(Debug)]
pub struct Struct;

unsafe impl VectorElement for Struct {
    const TYPE_ID: crate::enums::LogicalTypeID = LogicalTypeID::Struct;
    type Ref<'a>
        = ()
    where
        Self: 'a;
    type Internal<'a> = ();

    unsafe fn get<'a, U: VectorElement>(
        _reader: &crate::vector::VectorReader<'a, U>,
        _physical: usize,
        _logical: usize,
    ) -> Self::Ref<'a>
    where
        Self: 'a,
    {
        return ();
    }
}

#[cfg(test)]
mod tests {
    use duckdb_loadable_macros::{AsDuckDBType, AsFromValue, AsToValue};

    use super::*;
    use crate::{
        DuckDBType, Parameters, Result, ToValue,
        enums::LogicalTypeID,
        environment::{Environment, StorageLocation},
        vector::Unknown,
    };

    #[derive(Debug, PartialEq, AsDuckDBType, AsFromValue, AsToValue)]
    pub struct Tester {
        name: String,
        value: i32,
    }

    #[test]
    fn test_duckdb_type_macro() -> crate::Result<()> {
        let env: Environment = Environment::new()?;
        let conn = env.open(StorageLocation::InMemory)?.connect()?;

        let lt = <Tester as DuckDBType>::logical_type(&conn)?;
        assert_eq!(lt.id()?, LogicalTypeID::Struct);
        assert_eq!(lt.to_text()?, "STRUCT(\"name\" VARCHAR, \"value\" INTEGER)");
        assert_eq!(lt.param_count()?, 2);
        assert_eq!(lt.get_param(0)?.0, "name");
        assert_eq!(
            lt.get_param(0)?.1.type_value_get_inner()?,
            <String as DuckDBType>::logical_type(&conn)?
        );
        assert_eq!(lt.get_param(1)?.0, "value");
        assert_eq!(
            lt.get_param(1)?.1.type_value_get_inner()?,
            <i32 as DuckDBType>::logical_type(&conn)?
        );

        Ok(())
    }

    mod derived_types {
        use duckdb_loadable_macros::{AsDuckDBType, AsFromValue, AsToValue};

        #[derive(Debug, AsDuckDBType, AsToValue)]
        pub struct Nested {
            pub record: super::Tester,
            pub values: std::vec::Vec<Option<i32>>,
            pub pair: (i32, std::string::String),
        }

        #[derive(Debug, AsToValue)]
        pub struct ValueOnly {
            pub text: &'static str,
            pub missing: Option<i32>,
        }

        #[derive(Debug, PartialEq, AsFromValue, AsToValue)]
        pub struct Empty {}

        #[derive(Debug, PartialEq, AsDuckDBType, AsFromValue, AsToValue)]
        pub struct Decoded {
            pub record: Option<super::Tester>,
            pub values: std::option::Option<std::vec::Vec<Option<i32>>>,
            pub pair: Option<(Option<i32>, Option<std::string::String>)>,
        }

        #[derive(Debug, PartialEq, AsFromValue)]
        pub struct Generic<T> {
            pub value: core::option::Option<T>,
        }
    }

    #[test]
    fn test_duckdb_type_macro_nested_fields() -> Result<()> {
        let env = Environment::new()?;
        let conn = env.open(StorageLocation::InMemory)?.connect()?;
        let lt = <derived_types::Nested as DuckDBType>::logical_type(&conn)?;

        dbg!(&lt.to_string());

        assert_eq!(lt.id()?, LogicalTypeID::Struct);
        assert_eq!(lt.param_count()?, 3);
        assert_eq!(lt.get_param(0)?.0, "record");
        assert_eq!(
            lt.get_param(0)?.1.type_value_get_inner()?,
            <Tester as DuckDBType>::logical_type(&conn)?
        );
        assert_eq!(lt.get_param(1)?.0, "values");
        assert_eq!(
            lt.get_param(1)?.1.type_value_get_inner()?,
            <Vec<Option<i32>> as DuckDBType>::logical_type(&conn)?
        );
        assert_eq!(lt.get_param(2)?.0, "pair");
        assert_eq!(
            lt.get_param(2)?.1.type_value_get_inner()?,
            <(i32, String) as DuckDBType>::logical_type(&conn)?
        );
        Ok(())
    }

    #[test]
    fn test_to_value_macro_nested_fields() -> Result<()> {
        let env = Environment::new()?;
        let conn = env.open(StorageLocation::InMemory)?.connect()?;
        let input = derived_types::Nested {
            record: Tester {
                name: "duck".to_owned(),
                value: 42,
            },
            values: vec![Some(7), None, Some(9)],
            pair: (3, "tuple".to_owned()),
        };
        let value = input.value(&conn)?;
        assert_eq!(
            value.logical_type()?,
            <derived_types::Nested as DuckDBType>::logical_type(&conn)?
        );
        assert_eq!(value.child_count()?, 3);
        assert_eq!(
            value.get_child(0)?.get_child(0)?.get::<String>()?,
            Some("duck".to_owned())
        );
        assert_eq!(value.get_child(0)?.get_child(1)?.get::<i32>()?, Some(42));
        assert_eq!(
            value.get_child(1)?.get::<Vec<Option<i32>>>()?,
            Some(input.values.clone())
        );
        assert_eq!(
            value.get_child(2)?.get::<(Option<i32>, Option<String>)>()?,
            Some((Some(3), Some("tuple".to_owned())))
        );

        let chunk = conn
            .query(
                "SELECT s.record.name, s.record.value FROM (SELECT $1 AS s)",
                Parameters::positional(&[&input]),
            )?
            .next_chunk()?
            .unwrap();
        assert_eq!(chunk.get_vector_at::<String>(0)?.reader()?.get(0)?, Some("duck"));
        assert_eq!(chunk.get_vector_at::<i32>(1)?.reader()?.get(0)?, Some(&42));
        Ok(())
    }

    #[test]
    fn test_to_value_macro_without_duckdb_type() -> Result<()> {
        let env = Environment::new()?;
        let conn = env.open(StorageLocation::InMemory)?.connect()?;
        let input = derived_types::ValueOnly {
            text: "borrowed",
            missing: None,
        };
        let value = input.value(&conn)?;
        assert_eq!(value.child_count()?, 2);
        let logical_type = value.logical_type()?;
        assert_eq!(logical_type.id()?, LogicalTypeID::Struct);
        assert_eq!(logical_type.get_param(0)?.0, "text");
        assert_eq!(logical_type.get_param(1)?.0, "missing");
        assert_eq!(value.get_child(0)?.get::<String>()?, Some("borrowed".to_owned()));
        assert_eq!(value.get_child(1)?.get::<i32>()?, None);

        let empty = derived_types::Empty {}.value(&conn)?;
        assert_eq!(empty.logical_type()?.id()?, LogicalTypeID::Struct);
        assert_eq!(empty.child_count()?, 0);
        assert!(!empty.is_null()?);
        Ok(())
    }

    #[test]
    fn test_from_value_macro_roundtrip() -> Result<()> {
        let env = Environment::new()?;
        let conn = env.open(StorageLocation::InMemory)?.connect()?;
        let mut input = derived_types::Decoded {
            record: Some(Tester {
                name: "duck".to_owned(),
                value: 42,
            }),
            values: Some(vec![Some(7), None]),
            pair: Some((None, Some("tuple".to_owned()))),
        };
        assert_eq!(input.value(&conn)?.get::<derived_types::Decoded>()?, Some(input));
        input = derived_types::Decoded {
            record: None,
            values: None,
            pair: None,
        };
        assert_eq!(input.value(&conn)?.get::<derived_types::Decoded>()?, Some(input));
        let null = <derived_types::Decoded as ToValue>::null_value(&conn)?;
        assert_eq!(null.get::<derived_types::Decoded>()?, None);
        let empty = derived_types::Empty {};
        assert_eq!(empty.value(&conn)?.get::<derived_types::Empty>()?, Some(empty));
        Ok(())
    }

    #[test]
    fn test_from_value_macro_matches_names() -> Result<()> {
        let env = Environment::new()?;
        let conn = env.open(StorageLocation::InMemory)?.connect()?;
        let chunk = conn
            .query(
                "SELECT {'VALUE': 42, 'Name': 'duck'}, {'value': NULL::INTEGER}",
                Parameters::None,
            )?
            .next_chunk()?
            .unwrap();
        assert_eq!(
            chunk.get_vector_at::<Struct>(0)?.get_value_slow(0)?.get::<Tester>()?,
            Some(Tester {
                name: "duck".to_owned(),
                value: 42,
            })
        );
        assert_eq!(
            chunk
                .get_vector_at::<Struct>(1)?
                .get_value_slow(0)?
                .get::<derived_types::Generic<i32>>()?,
            Some(derived_types::Generic { value: None })
        );
        let chunk = conn
            .query("SELECT {'value': 7}", Parameters::None)?
            .next_chunk()?
            .unwrap();
        assert_eq!(
            chunk
                .get_vector_at::<Struct>(0)?
                .get_value_slow(0)?
                .get::<derived_types::Generic<i32>>()?,
            Some(derived_types::Generic { value: Some(7) })
        );
        Ok(())
    }

    #[test]
    fn test_from_value_macro_rejects_invalid_values() -> Result<()> {
        let env = Environment::new()?;
        let conn = env.open(StorageLocation::InMemory)?.connect()?;
        for (expression, message) in [
            ("42", "Expected STRUCT value"),
            ("row('duck', 42)", "Expected STRUCT value"),
            ("{'name': 'duck'}", "STRUCT has 1 fields, expected 2"),
            (
                "{'name': 'duck', 'value': 42, 'extra': 1}",
                "STRUCT has 3 fields, expected 2",
            ),
            ("{'wrong': 'duck', 'value': 42}", "STRUCT field 'name' is missing"),
            ("{'name': NULL::VARCHAR, 'value': 42}", "STRUCT field 'name' is NULL"),
        ] {
            let chunk = conn
                .query(format!("SELECT {expression}"), Parameters::None)?
                .next_chunk()?
                .unwrap();
            let value = chunk.get_vector_at::<Unknown>(0)?.get_value_slow(0)?;
            let error = value.get::<Tester>().unwrap_err();
            assert!(error.message.contains(message), "{expression}: {error}");
        }
        let chunk = conn
            .query("SELECT {'name': 'duck', 'value': 'invalid'}", Parameters::None)?
            .next_chunk()?
            .unwrap();
        assert!(
            chunk
                .get_vector_at::<Struct>(0)?
                .get_value_slow(0)?
                .get::<Tester>()
                .is_err()
        );
        Ok(())
    }
}

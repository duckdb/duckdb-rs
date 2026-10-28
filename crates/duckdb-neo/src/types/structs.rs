//! The `STRUCT` logical type.
//!
//! Named Rust structs map to `STRUCT` through `#[derive(AsDuckDB)]`, which generates typed
//! values, borrowed vector rows, and vector writes. [`Struct`] is the untyped fallback.

use crate::{
    Result, data_chunk::VectorCollection, enums::LogicalTypeID, logical_type::LogicalType, vector::VectorElement,
};

/// Accepts any `STRUCT` or `TUPLE` vector without checking its fields.
///
/// Rows only report NULLs. Derive `AsDuckDB` for typed fields, or use
/// [`Vector::get_value_slow`](crate::vector::Vector::get_value_slow) and
/// [`Vector::write_value_slow`](crate::vector::Vector::write_value_slow) when the schema is only known at runtime.
#[derive(Debug)]
pub struct Struct;

// SAFETY: there is no physical row data, and `get` reads nothing.
unsafe impl VectorElement for Struct {
    const TYPE_ID: LogicalTypeID = LogicalTypeID::Struct;
    type Ref<'a>
        = ()
    where
        Self: 'a;
    type Internal<'a> = ();

    fn validate(other: &LogicalType, _children: Option<&VectorCollection>) -> Result<bool> {
        Ok(matches!(other.id()?, LogicalTypeID::Struct | LogicalTypeID::Tuple))
    }

    unsafe fn get<'a, U: VectorElement>(
        _reader: &crate::vector::VectorReader<'a, U>,
        _physical: usize,
        _logical: usize,
    ) -> Self::Ref<'a>
    where
        Self: 'a,
    {
    }
}

/// Runtime support for the struct derives in `duckdb-loadable-macros`; not a stable API.
pub mod derive_support {
    use crate::{
        Error, Result, data_chunk::VectorCollection, logical_type::LogicalType, logical_type::LogicalTypeID,
        value::Value, vector::VectorElement,
    };

    /// Check that `value` is a `STRUCT` with `expected` fields and return their names.
    pub fn value_field_names(value: &Value, expected: usize) -> Result<Vec<String>> {
        let logical_type = value.logical_type()?;
        if logical_type.id()? != LogicalTypeID::Struct {
            return Err(Error::invalid_input(format!(
                "Expected STRUCT value, found {}",
                logical_type.to_text()?
            )));
        }
        let fields = logical_type.get_params()?;
        if fields.len() != expected {
            return Err(Error::invalid_input(format!(
                "STRUCT has {} fields, expected {expected}",
                fields.len()
            )));
        }
        Ok(fields.into_iter().map(|(name, _)| name).collect())
    }

    /// Return the child value of field `name`, preferring an exact match over a case-insensitive one.
    pub fn value_field(value: &Value, names: &[String], name: &str) -> Result<Value> {
        let index = names
            .iter()
            .position(|field| field == name)
            .or_else(|| names.iter().position(|field| field.eq_ignore_ascii_case(name)))
            .ok_or_else(|| Error::invalid_input(format!("STRUCT field '{name}' is missing")))?;
        value.get_child(index)
    }

    /// Reject a SQL NULL for a field whose Rust type is not optional.
    pub fn required<T>(value: Option<T>, name: &str) -> Result<T> {
        value.ok_or_else(|| Error::invalid_input(format!("STRUCT field '{name}' is NULL")))
    }

    /// Check that a vector type is a `STRUCT` with `count` fields and child vectors.
    pub fn validate_layout(other: &LogicalType, children: Option<&VectorCollection>, count: usize) -> Result<bool> {
        Ok(other.id()? == LogicalTypeID::Struct
            && other.param_count()? == count
            && children.map_or(0, VectorCollection::col_count) == count)
    }

    /// Check the name (case-insensitively) and child vector of the field at `index`.
    pub fn validate_field<T: VectorElement>(
        other: &LogicalType,
        children: Option<&VectorCollection>,
        index: usize,
        name: &str,
    ) -> Result<bool> {
        let Some(children) = children.filter(|children| index < children.col_count()) else {
            return Ok(false);
        };
        if !other.get_param(index)?.0.eq_ignore_ascii_case(name) {
            return Ok(false);
        }
        let child = children.get_untyped_vector_at(index)?;
        T::validate(child.logical_type(), child.children())
    }
}

#[cfg(test)]
mod tests {
    use duckdb_loadable_macros::{AsDuckDBType, AsFromValue, AsToValue};

    use super::*;
    use crate::{
        DuckDBType, Parameters, Result, ToValue,
        data_chunk::DataChunk,
        enums::LogicalTypeID,
        environment::{Environment, StorageLocation},
        types::List,
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

        use crate::types::List;

        #[derive(Debug, AsDuckDBType, AsToValue)]
        pub struct Nested {
            pub record: super::Tester,
            pub values: List<i32>,
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
            pub values: std::option::Option<List<i32>>,
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
            <List<i32> as DuckDBType>::logical_type(&conn)?
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
            values: List(vec![Some(7), None, Some(9)]),
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
        assert_eq!(value.get_child(1)?.get::<List<i32>>()?, Some(input.values.clone()));
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
            values: Some(List(vec![Some(7), None])),
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

    #[derive(Debug, PartialEq, crate::AsDuckDB)]
    struct Entry {
        key: i32,
        value: String,
    }

    #[derive(Debug, crate::AsDuckDB)]
    struct IntegerEntry {
        key: i32,
        value: i32,
    }

    #[derive(Debug, PartialEq, crate::AsDuckDB)]
    struct KeyOnly {
        key: i32,
    }

    #[derive(Debug, PartialEq, crate::AsDuckDB)]
    struct EmptyRow {}

    #[test]
    fn struct_marker_supports_tuple_vectors() -> Result<()> {
        let env = Environment::new()?;
        let conn = env.open(StorageLocation::InMemory)?.connect()?;
        let chunk = conn
            .query("SELECT row(42, 'duck')", Parameters::None)?
            .next_chunk()?
            .unwrap();
        let vector = chunk.get_vector_at::<Struct>(0)?;
        assert!(vector.reader()?.get(0)?.is_some());
        assert_eq!(
            vector.get_value_slow(0)?.get::<(Option<i32>, Option<String>)>()?,
            Some((Some(42), Some("duck".to_owned())))
        );
        assert!(chunk.get_vector_at::<Entry>(0).is_err());

        let mut chunk = DataChunk::create(&conn, &[<(i32, String) as DuckDBType>::logical_type(&conn)?], true)?;
        {
            let mut vector = chunk.get_vector_at_mut::<Struct>(0)?;
            vector.set_size(2)?;
            vector.write_value_slow(0, (7_i32, Option::<String>::None).value(&conn)?)?;
            vector.set_null_slow(1)?;
        }
        let vector = chunk.get_vector_at::<Struct>(0)?;
        assert_eq!(
            vector.get_value_slow(0)?.get::<(Option<i32>, Option<String>)>()?,
            Some((Some(7), None))
        );
        assert!(vector.reader()?.get(1)?.is_none());
        Ok(())
    }

    #[test]
    fn struct_type_mismatch_preserves_row() -> Result<()> {
        let env = Environment::new()?;
        let conn = env.open(StorageLocation::InMemory)?.connect()?;
        let value = conn
            .query("SELECT {'key': 42, 'value': 'duck'}", Parameters::None)?
            .next_chunk()?
            .unwrap()
            .get_vector_at::<Struct>(0)?
            .get_value_slow(0)?;
        let mut chunk = DataChunk::create(&conn, &[value.logical_type()?], true)?;
        {
            let mut vector = chunk.get_vector_at_mut::<Struct>(0)?;
            vector.set_size(2)?;
            vector.write_value_slow(0, value)?;
            vector.set_null_slow(1)?;
        }
        assert!(chunk.get_vector_at_mut::<IntegerEntry>(0).is_err());
        assert!(chunk.get_vector_at_mut::<Unknown>(0)?.cast::<IntegerEntry>().is_err());
        let vector = chunk.get_vector_at::<Entry>(0)?;
        let reader = vector.reader()?;
        let row = reader.get(0)?.unwrap();
        assert_eq!(row.key, Some(&42));
        assert_eq!(row.value, Some("duck"));
        assert!(reader.get(1)?.is_none());
        Ok(())
    }

    #[test]
    fn struct_field_names_are_case_insensitive() -> Result<()> {
        let env = Environment::new()?;
        let conn = env.open(StorageLocation::InMemory)?.connect()?;
        for sql in ["SELECT {'Key': 42}", "SELECT {'KEY': 42}", "SELECT {'key': 42}"] {
            let chunk = conn.query(sql, Parameters::None)?.next_chunk()?.unwrap();
            let vector = chunk.get_vector_at::<KeyOnly>(0)?;
            assert_eq!(vector.reader()?.get(0)?.unwrap().key, Some(&42), "{sql}");
            assert_eq!(
                vector.get_value_slow(0)?.get::<KeyOnly>()?,
                Some(KeyOnly { key: 42 }),
                "{sql}"
            );
        }
        Ok(())
    }

    #[test]
    fn test_empty_struct_read_and_write() -> Result<()> {
        let env = Environment::new()?;
        let conn = env.open(StorageLocation::InMemory)?.connect()?;

        let chunk = conn.query("SELECT {}", Parameters::None)?.next_chunk()?.unwrap();
        let vector = chunk.get_vector_at::<Struct>(0)?;
        assert!(vector.children().is_none());
        assert!(vector.reader()?.get(0)?.is_some());
        assert_eq!(vector.get_value_slow(0)?.get::<EmptyRow>()?, Some(EmptyRow {}));
        assert!(chunk.get_vector_at::<EmptyRow>(0)?.reader()?.get(0)?.is_some());
        assert!(chunk.get_vector_at::<KeyOnly>(0).is_err());

        let mut chunk = DataChunk::create(&conn, &[EmptyRow::logical_type(&conn)?], true)?;
        {
            let mut vector = chunk.get_vector_at_mut::<EmptyRow>(0)?;
            vector.set_size(2)?;
            let mut writer = vector.writer()?;
            writer.set(0, Some(EmptyRow {}))?;
            writer.set(1, None)?;
        }
        let vector = chunk.get_vector_at::<Struct>(0)?;
        let reader = vector.reader()?;
        assert!(reader.get(0)?.is_some());
        assert!(reader.get(1)?.is_none());
        Ok(())
    }
}

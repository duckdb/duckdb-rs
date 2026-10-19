//! The `STRUCT` logical type.

use std::marker::PhantomData;

use super::{DuckDBType, ToValue};
use crate::{
    Result,
    connection::FFILink,
    data_chunk::VectorCollection,
    error::Error,
    logical_type::{LogicalType, LogicalTypeID},
    parameter::{Parameters, QueryParameter},
    value::{Value, ValueInput},
    vector::{Unknown, Vector, VectorElement, WritableVectorElement},
};
/// Reads `STRUCT` and `TUPLE` vectors as child fields.
#[derive(Debug)]
pub struct Struct;

/// Defines the named fields of a [`StructValue`].
pub trait StructSchema {
    /// Return field names and types in storage order.
    fn fields<C: FFILink + ?Sized>(link: &C) -> Result<Vec<(&'static str, LogicalType)>>;
}

trait StructFieldValue {
    fn create_value(&self, link: &dyn FFILink) -> Result<Value>;

    fn logical_type(&self, link: &dyn FFILink) -> Result<LogicalType>;
}

impl<T: ToValue + DuckDBType> StructFieldValue for T {
    fn create_value(&self, link: &dyn FFILink) -> Result<Value> {
        ToValue::value(self, link)
    }

    fn logical_type(&self, link: &dyn FFILink) -> Result<LogicalType> {
        T::logical_type(link)
    }
}

/// A heterogeneous DuckDB `STRUCT` value with schema `S`.
pub struct StructValue<'a, S> {
    fields: Vec<Box<dyn StructFieldValue + 'a>>,
    _schema: PhantomData<S>,
}

impl<S> std::fmt::Debug for StructValue<'_, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StructValue")
            .field("fields", &self.fields.len())
            .finish_non_exhaustive()
    }
}

impl<'a, S> StructValue<'a, S> {
    /// Create an empty struct builder.
    pub fn new() -> Self {
        Self {
            fields: Vec::new(),
            _schema: PhantomData,
        }
    }

    /// Append a field value in schema order.
    pub fn field<T: ToValue + DuckDBType + 'a>(mut self, value: T) -> Self {
        self.fields.push(Box::new(value));
        self
    }
}

impl<S> Default for StructValue<'_, S> {
    fn default() -> Self {
        Self::new()
    }
}

impl<S: StructSchema> DuckDBType for StructValue<'_, S> {
    fn logical_type<C: FFILink + ?Sized>(link: &C) -> Result<LogicalType> {
        let fields = S::fields(link)?;
        let values = fields
            .into_iter()
            .map(|(name, logical_type)| Ok((name, Value::from_logical_type(link, &logical_type)?)))
            .collect::<Result<Vec<_>>>()?;
        let parameters = values
            .iter()
            .map(|(name, value)| (*name, value as &dyn QueryParameter))
            .collect::<Vec<_>>();
        link.logical_type_create("STRUCT", Parameters::named(&parameters))
    }
}

impl<S: StructSchema> ToValue for StructValue<'_, S> {
    fn value<C: FFILink + ?Sized>(&self, link: &C) -> Result<Value> {
        let fields = S::fields(link)?;
        if self.fields.len() != fields.len() {
            return Err(Error::invalid_input(format!(
                "StructValue has {} fields, its schema has {}",
                self.fields.len(),
                fields.len()
            )));
        }

        for (field, (name, expected)) in self.fields.iter().zip(&fields) {
            let actual = field.logical_type(&link)?;
            if actual != *expected {
                return Err(Error::invalid_input(format!(
                    "StructValue field '{}' has type {}, its schema has {}",
                    name,
                    actual.to_text()?,
                    expected.to_text()?,
                )));
            }
        }

        let children = self
            .fields
            .iter()
            .map(|field| field.create_value(&link))
            .collect::<Result<Vec<_>>>()?;
        let names = fields.iter().map(|(name, _)| *name).collect::<Vec<_>>();
        link.create_value(ValueInput::Struct {
            names: &names,
            children: &children,
        })
    }
}

// SAFETY: fields are read only through the child vectors, with their own checks.
unsafe impl VectorElement for Struct {
    const TYPE_ID: LogicalTypeID = LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_STRUCT;
    type Ref<'a> = StructRow<'a>;

    type Internal = Struct;

    fn validate(other: &LogicalType, _children: Option<&VectorCollection>) -> Result<bool> {
        Ok(matches!(
            other.id()?,
            LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_STRUCT | LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_TUPLE
        ))
    }

    unsafe fn get<'a, U: VectorElement>(vector: &'a Vector<'_, U>, _physical: usize, logical: usize) -> Self::Ref<'a>
    where
        Self: Sized + 'a,
    {
        StructRow {
            children: vector.children(),
            logical_type: vector.logical_type(),
            logical,
        }
    }
}

trait StructFieldWrite {
    fn validate(&self, child: &Vector<'_, Unknown>) -> Result<()>;

    fn write(self: Box<Self>, child: &mut Vector<'_, Unknown>, index: usize) -> Result<()>;
}

#[derive(Debug)]
struct TypedStructField<'a, T: WritableVectorElement + 'a> {
    value: Option<T::Write<'a>>,
}

impl<'a, T> StructFieldWrite for TypedStructField<'a, T>
where
    T: WritableVectorElement + 'a,
    T::Write<'a>: 'a,
{
    fn validate(&self, child: &Vector<'_, Unknown>) -> Result<()> {
        child.validate_as::<T>()
    }

    fn write(self: Box<Self>, child: &mut Vector<'_, Unknown>, index: usize) -> Result<()> {
        child.write_as::<T>(index, self.value)
    }
}

/// Heterogeneous field values written into one struct row.
#[derive(Default)]
pub struct StructWrite<'a> {
    fields: Vec<Box<dyn StructFieldWrite + 'a>>,
}

impl std::fmt::Debug for StructWrite<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StructWrite")
            .field("fields", &self.fields.len())
            .finish_non_exhaustive()
    }
}

impl<'a> StructWrite<'a> {
    /// Append a typed field value in schema order.
    pub fn field<T>(mut self, value: Option<T::Write<'a>>) -> Self
    where
        T: WritableVectorElement + 'a,
        T::Write<'a>: 'a,
    {
        self.fields.push(Box::new(TypedStructField::<T> { value }));
        self
    }
}

// SAFETY: fields are written only through the child vectors' own `write`.
unsafe impl WritableVectorElement for Struct {
    type Write<'a> = StructWrite<'a>;

    unsafe fn write(vector: &mut Vector<'_, Self>, index: usize, value: Option<Self::Write<'_>>) -> Result<()> {
        let Some(value) = value else {
            // DuckDB requires every descendant of a NULL struct row to be NULL; this recurses.
            return vector.set_null_slow(index);
        };
        if value.fields.len() != vector.children().map_or(0, VectorCollection::col_count) {
            return Err(Error::invalid_input(format!(
                "Struct row has {} fields, expected {}",
                value.fields.len(),
                vector.children().map_or(0, VectorCollection::col_count)
            )));
        }

        vector.check_row_writable(index)?;
        if let Some(children) = vector.children() {
            for (i, field) in value.fields.iter().enumerate() {
                field.validate(children.get_untyped_vector_at(i)?)?;
            }
        }

        let vector_len = vector.len();
        if let Some(children) = vector.children_collection_mut() {
            for (i, field) in value.fields.into_iter().enumerate() {
                let child = children.cached_mut::<Unknown>(i)?;
                if child.len() != vector_len {
                    child.set_size(vector_len)?;
                }
                field.write(child, index)?;
            }
        }
        vector.set_row_validity(index, true)
    }
}

/// A borrowed struct row that resolves named fields to child vectors.
#[derive(Debug)]
pub struct StructRow<'a> {
    children: Option<&'a VectorCollection>,
    logical_type: &'a LogicalType,
    logical: usize,
}

impl<'a> StructRow<'a> {
    /// Return a field by its zero-based position after validating its logical type.
    pub fn get_index<T: VectorElement>(&self, index: usize) -> Result<Option<T::Ref<'a>>> {
        let child = self
            .children
            .filter(|children| index < children.col_count())
            .map(|children| children.cached_unchecked(index))
            .ok_or_else(|| Error::invalid_parameter(format!("Struct field index {} is out of bounds", index)))?;
        child.get_as_checked::<T>(self.logical)
    }

    /// Return a struct field by name after validating its logical type.
    pub fn get<T: VectorElement>(&self, name: &str) -> Result<Option<T::Ref<'a>>> {
        let fields = self.logical_type.get_params()?;
        let index = fields
            .iter()
            .position(|(field_name, _)| field_name == name)
            .or_else(|| {
                fields
                    .iter()
                    .position(|(field_name, _)| field_name.eq_ignore_ascii_case(name))
            })
            .ok_or_else(|| Error::invalid_parameter(format!("Field '{}' not found in struct", name)))?;
        self.get_index::<T>(index)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        data_chunk::DataChunk,
        environment::{Environment, StorageLocation},
    };

    #[test]
    fn struct_positional_access_supports_tuple_vectors() -> Result<()> {
        let env = Environment::new()?;
        let conn = env.open(StorageLocation::InMemory)?.connect()?;
        let chunk = conn
            .query("SELECT row(42, 'duck')", Parameters::None)?
            .next_chunk()?
            .unwrap();
        let row = chunk.get_vector_at::<Struct>(0)?.get(0)?.unwrap();
        assert_eq!(row.get_index::<i32>(0)?, Some(&42));
        assert_eq!(row.get_index::<String>(1)?, Some("duck"));
        assert!(row.get_index::<i32>(2).is_err());

        let mut chunk = DataChunk::create(&conn, &[<(i32, String) as DuckDBType>::logical_type(&conn)?], true)?;
        {
            let mut vector = chunk.get_vector_at_mut::<Struct>(0)?;
            vector.set_size(1)?;
            vector.write(
                0,
                Some(StructWrite::default().field::<i32>(Some(7)).field::<String>(None)),
            )?;
        }
        let row = chunk.get_vector_at::<Struct>(0)?.get(0)?.unwrap();
        assert_eq!(row.get_index::<i32>(0)?, Some(&7));
        assert_eq!(row.get_index::<String>(1)?, None);
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
            vector.write(1, None)?;
            for index in 0..2 {
                let invalid = StructWrite::default().field::<i32>(Some(7)).field::<i32>(Some(8));
                assert!(vector.write(index, Some(invalid)).is_err());
            }
        }
        let vector = chunk.get_vector_at::<Struct>(0)?;
        let row = vector.get(0)?.unwrap();
        assert_eq!(row.get::<i32>("key")?, Some(&42));
        assert_eq!(row.get::<String>("value")?, Some("duck"));
        assert!(vector.get(1)?.is_none());
        Ok(())
    }

    #[test]
    fn struct_field_names_are_case_insensitive() -> Result<()> {
        let env = Environment::new()?;
        let conn = env.open(StorageLocation::InMemory)?.connect()?;
        let chunk = conn
            .query("SELECT {'Key': 42}", Parameters::None)?
            .next_chunk()?
            .unwrap();
        let row = chunk.get_vector_at::<Struct>(0)?.get(0)?.unwrap();
        assert_eq!(row.get::<i32>("Key")?, Some(&42));
        assert_eq!(row.get::<i32>("KEY")?, Some(&42));
        assert_eq!(row.get::<i32>("key")?, Some(&42));
        Ok(())
    }
}

//! The `STRUCT` logical type.

use std::marker::PhantomData;

use super::{DuckDBType, ToValue};
use crate::{
    Result,
    connection::FFILink,
    error::{DuckDBError, Error},
    logical_type::{LogicalType, LogicalTypeID},
    parameter::{Parameters, QueryParameter},
    value::{Value, ValueInput},
    vector::{Unknown, Vector, VectorElement, WritableVectorElement},
};

/// Reads a `STRUCT` vector as named fields.
pub struct Struct;

/// Defines the named fields of a [`StructValue`].
pub trait StructSchema {
    /// Return field names and types in storage order.
    fn fields<C: FFILink + ?Sized>(link: &C) -> Result<Vec<(&'static str, LogicalType)>>;
}

trait StructFieldValue {
    fn create_value(&self, link: &dyn FFILink) -> Result<Value>;
}

impl<T: ToValue> StructFieldValue for T {
    fn create_value(&self, link: &dyn FFILink) -> Result<Value> {
        ToValue::value(self, link)
    }
}

/// A heterogeneous DuckDB `STRUCT` value with schema `S`.
pub struct StructValue<'a, S> {
    fields: Vec<Box<dyn StructFieldValue + 'a>>,
    _schema: PhantomData<S>,
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
    pub fn field<T: ToValue + 'a>(mut self, value: T) -> Self {
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
        let children = self
            .fields
            .iter()
            .map(|field| field.create_value(&link))
            .collect::<Result<Vec<_>>>()?;
        let fields = S::fields(link)?;
        let names = fields.iter().map(|(name, _)| *name).collect::<Vec<_>>();
        link.create_value(ValueInput::Struct {
            names: &names,
            children: &children,
        })
    }
}

impl VectorElement for Struct {
    const TYPE_ID: LogicalTypeID = LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_STRUCT;
    type Ref<'a> = StructRow<'a>;

    type Internal = Struct;

    fn get<'a, U: VectorElement>(vector: &'a Vector<'_, U>, _physical: usize, logical: usize) -> Self::Ref<'a>
    where
        Self: Sized + 'a,
    {
        StructRow {
            children: &vector.children,
            logical_type: vector.logical_type(),
            logical,
        }
    }
}

trait StructFieldWrite {
    fn write(self: Box<Self>, child: &mut Vector<'_, Unknown>, index: usize) -> Result<()>;
}

struct TypedStructField<'a, T: WritableVectorElement + 'a> {
    value: Option<T::Write<'a>>,
}

impl<'a, T> StructFieldWrite for TypedStructField<'a, T>
where
    T: WritableVectorElement + 'a,
    T::Write<'a>: 'a,
{
    fn write(self: Box<Self>, child: &mut Vector<'_, Unknown>, index: usize) -> Result<()> {
        child.write_as::<T>(index, self.value)
    }
}

/// Heterogeneous field values written into one struct row.
#[derive(Default)]
pub struct StructWrite<'a> {
    fields: Vec<Box<dyn StructFieldWrite + 'a>>,
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

impl WritableVectorElement for Struct {
    type Write<'a> = StructWrite<'a>;

    fn write(vector: &mut Vector<'_, Self>, index: usize, value: Option<Self::Write<'_>>) -> Result<()> {
        let Some(value) = value else {
            return vector.set_row_validity(index, false);
        };
        if value.fields.len() != vector.children.len() {
            return Err(Error {
                code: DuckDBError::DUCKDB_V2_ERROR_INPUT_INVALID,
                message: format!(
                    "Struct row has {} fields, expected {}",
                    value.fields.len(),
                    vector.children.len()
                ),
            });
        }

        vector.set_row_validity(index, true)?;
        for (field, child) in value.fields.into_iter().zip(&mut vector.children) {
            if child.len() != vector.len {
                child.set_size(vector.len)?;
            }
            field.write(child, index)?;
        }
        Ok(())
    }
}

/// A borrowed struct row that resolves named fields to child vectors.
pub struct StructRow<'a> {
    children: &'a [Vector<'a, Unknown>],
    logical_type: &'a LogicalType,
    logical: usize,
}

impl<'a> StructRow<'a> {
    // TODO: fn get_index(idx: usize)

    /// Return a struct field by name after validating its logical type.
    pub fn get<T: VectorElement>(&self, name: &str) -> Result<Option<T::Ref<'a>>> {
        let fields = self.logical_type.get_params()?;
        let index = fields
            .iter()
            .position(|(field_name, _)| field_name == name)
            .ok_or_else(|| Error {
                code: DuckDBError::DUCKDB_V2_ERROR_INPUT_PARAMETER_INVALID,
                message: format!("Field '{}' not found in struct", name),
            })?;
        let child = self.children.get(index).ok_or_else(|| Error {
            code: DuckDBError::DUCKDB_V2_ERROR_INPUT_INVALID,
            message: format!("Struct field '{}' is missing its child vector", name),
        })?;

        if child.logical_type().type_id() != T::TYPE_ID {
            return Err(Error {
                code: DuckDBError::DUCKDB_V2_ERROR_INPUT_INVALID,
                message: format!(
                    "Field '{}' has type {:?}, expected {:?}",
                    name,
                    child.logical_type().type_id(),
                    T::TYPE_ID
                ),
            });
        }

        child.get_as_checked::<T>(self.logical)
    }
}

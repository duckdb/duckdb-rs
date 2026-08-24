use crate::{Result, ToValue, connection::FFILink, value::Value};

/// A value that can be bound as a positional or named query parameter.
///
/// Types implementing [`ToValue`] are converted to owned DuckDB values.
/// Existing [`Value`] handles are borrowed directly.
pub trait QueryParameter {
    #[doc(hidden)]
    fn to_value<'a>(&'a self, link: &dyn FFILink) -> Result<ParameterValue<'a>>;
}

impl<T: ToValue> QueryParameter for T {
    fn to_value<'a>(&'a self, link: &dyn FFILink) -> Result<ParameterValue<'a>> {
        self.value(link).map(ParameterValue::Owned)
    }
}

impl QueryParameter for Value {
    fn to_value<'a>(&'a self, _link: &dyn FFILink) -> Result<ParameterValue<'a>> {
        Ok(ParameterValue::Borrowed(self))
    }
}

#[doc(hidden)]
pub enum ParameterValue<'a> {
    Borrowed(&'a Value),
    Owned(Value),
}

impl ParameterValue<'_> {
    pub(crate) fn as_value(&self) -> &Value {
        match self {
            ParameterValue::Borrowed(value) => value,
            ParameterValue::Owned(value) => value,
        }
    }
}

/// Positional, named, or empty parameters for a DuckDB operation.
pub enum Parameters<'a> {
    /// Execute without parameters.
    None,
    /// Bind parameters by position.
    Positional(&'a [&'a dyn QueryParameter]),
    /// Bind parameters by name.
    Named(&'a [(&'a str, &'a dyn QueryParameter)]),
}

impl<'a> Parameters<'a> {
    /// Bind parameters by position.
    pub fn positional(params: &'a [&'a dyn QueryParameter]) -> Self {
        Self::Positional(params)
    }

    /// Bind parameters by name.
    pub fn named(params: &'a [(&'a str, &'a dyn QueryParameter)]) -> Self {
        Self::Named(params)
    }

    pub(crate) fn into_values(
        self,
        link: &dyn FFILink,
    ) -> Result<(Option<Vec<&'a str>>, Vec<ParameterValue<'a>>)> {
        match self {
            Parameters::None => Ok((None, Vec::new())),
            Parameters::Positional(params) => {
                let values = params
                    .iter()
                    .map(|param| param.to_value(link))
                    .collect::<Result<Vec<_>>>()?;
                Ok((None, values))
            }
            Parameters::Named(params) => {
                let names = params.iter().map(|(name, _)| *name).collect();
                let values = params
                    .iter()
                    .map(|(_, param)| param.to_value(link))
                    .collect::<Result<Vec<_>>>()?;
                Ok((Some(names), values))
            }
        }
    }
}

//! Function parameter and return-type declarations.
//!
//! Use [`crate::signature::SignatureBuilder`] when registering scalar,
//! aggregate, or table functions. Built [`crate::signature::Signature`] values
//! expose the resolved declaration.

use std::ops::Deref;

use crate::ffi;

use crate::{Result, check_api_call, logical_type::LogicalType, value::Value};

/// An owned function signature.
///
/// Fixed parameters retain their declaration order. A signature may also
/// define a variadic tail and, depending on the function family, a return
/// type.
pub struct Signature(ffi::duckdb_v2_function_signature_handle);

impl Deref for Signature {
    type Target = ffi::duckdb_v2_function_signature_handle;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// A parameter declaration accepted by [`SignatureBuilder`].
pub enum ParameterType {
    /// A required fixed parameter.
    Normal(NormalParameter),
    /// A fixed parameter that callers may omit.
    WithDefault(WithDefaultParameter),
    /// A variadic tail accepting zero or more arguments.
    TailVararg(TailVarargParameter),
}

/// A required fixed parameter.
pub struct NormalParameter {
    /// The parameter name.
    pub name: String,
    /// The required argument type.
    pub logical_type: LogicalType,
}

/// A fixed parameter with a default value.
pub struct WithDefaultParameter {
    /// The parameter name.
    pub name: String,
    /// The required argument type.
    pub logical_type: LogicalType,
    /// The value used when the argument is omitted.
    pub default_value: Value,
}

/// A variadic tail parameter.
pub struct TailVarargParameter {
    /// The tail label; built signatures retain only its type.
    pub name: String,
    /// The type accepted by each trailing argument.
    pub logical_type: LogicalType,
}

/// A fixed parameter read from a built [`Signature`].
#[derive(Debug)]
pub struct Parameter {
    /// The parameter name.
    pub name: String,

    /// The required argument type.
    pub logical_type: LogicalType,

    /// The value used when the argument is omitted.
    pub default_value: Option<Value>,
}

impl Parameter {
    /// Declare a required fixed parameter.
    pub fn normal(name: impl Into<String>, logical_type: LogicalType) -> ParameterType {
        ParameterType::Normal(NormalParameter {
            name: name.into(),
            logical_type,
        })
    }

    /// Declare a fixed parameter with a default value.
    pub fn normal_with_default(
        name: impl Into<String>,
        logical_type: LogicalType,
        default_value: Value,
    ) -> ParameterType {
        ParameterType::WithDefault(WithDefaultParameter {
            name: name.into(),
            logical_type,
            default_value,
        })
    }

    /// Declare a variadic tail parameter.
    pub fn tail_vararg(name: impl Into<String>, logical_type: LogicalType) -> ParameterType {
        ParameterType::TailVararg(TailVarargParameter {
            name: name.into(),
            logical_type,
        })
    }
}

/// Builds signatures for user-defined functions.
///
/// Parameters with defaults must follow required parameters, and parameter
/// names must be unique. DuckDB checks these structural rules when the
/// function is registered.
pub struct SignatureBuilder {
    parameters: Vec<ParameterType>,
    return_type: Option<LogicalType>,
}

impl SignatureBuilder {
    /// Create a signature builder with a return type.
    pub fn new(parameters: impl Into<Vec<ParameterType>>, return_type: LogicalType) -> Self {
        SignatureBuilder {
            parameters: parameters.into(),
            return_type: Some(return_type),
        }
    }

    /// Create a signature builder without a return type.
    pub fn without_return_type(parameters: impl Into<Vec<ParameterType>>) -> Self {
        SignatureBuilder {
            parameters: parameters.into(),
            return_type: None,
        }
    }

    /// Append parameters and apply configured types to a borrowed signature.
    pub fn build(&self, handle: &ffi::duckdb_v2_function_signature_handle) -> Result<()> {
        if let Some(return_handle) = self.return_type.as_ref() {
            check_api_call!(
                ffi::duckdb_v2_function_signature_set_return_type,
                *handle,
                return_handle.handle
            )?;
        }

        for param in &self.parameters {
            match param {
                ParameterType::Normal(param) => {
                    check_api_call!(
                        ffi::duckdb_v2_function_signature_add_parameter,
                        *handle,
                        (&param.name).into(),
                        param.logical_type.handle,
                        std::ptr::null_mut()
                    )?;
                }
                ParameterType::WithDefault(param) => {
                    check_api_call!(
                        ffi::duckdb_v2_function_signature_add_parameter,
                        *handle,
                        (&param.name).into(),
                        param.logical_type.handle,
                        param.default_value.handle
                    )?;
                }
                ParameterType::TailVararg(param) => {
                    check_api_call!(
                        ffi::duckdb_v2_function_signature_set_varargs,
                        *handle,
                        param.logical_type.handle
                    )?;
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod test {
    use crate::Parameters;
    use crate::builder_helpers::scalar_callback;
    use crate::scalar::ScalarFunctionBuilder;
    use crate::{
        DuckDBType, ToValue,
        environment::{Environment, StorageLocation},
    };

    use super::*;

    scalar_callback!(DefaultParameterScalar, u64, |input, result, _ctx, _user_data| {
        let vectors_len = input.vectors_count();
        let vector = input.get_vector_at::<i32>(2)?;
        let mut result = result;
        result.set_size(1)?;
        result.write(0, Some(*vector.get(0).unwrap().unwrap() as u64 + vectors_len as u64))?;
        Ok(())
    });

    scalar_callback!(VarargScalar, u64, |input, result, _ctx, _user_data| {
        let vectors_len = input.vectors_count();
        let mut result = result;
        result.set_size(1)?;
        result.write(0, Some(vectors_len as u64))?;
        Ok(())
    });

    #[test]
    fn test_signature_build() -> crate::Result<()> {
        let env = Environment::new()?;
        let db = env.open(StorageLocation::InMemory)?;
        let conn = db.connect()?;

        let default_value = 42_i32.value(&conn)?;

        let sig = SignatureBuilder::new(
            [
                Parameter::normal("param1", i32::logical_type(&conn)?),
                Parameter::normal("param2", String::logical_type(&conn)?),
                Parameter::normal_with_default("param3", i32::logical_type(&conn)?, default_value),
            ],
            u64::logical_type(&conn)?,
        );

        assert_eq!(sig.parameters.len(), 3);

        ScalarFunctionBuilder::new("test", sig, DefaultParameterScalar).register(&conn)?;

        let statements = conn.query("SELECT test(10, 'AA')", Parameters::None)?;

        for chunk in statements {
            let chunk = chunk?;

            let vector = chunk.get_vector_at::<u64>(0)?;

            assert_eq!(vector.get(0)?, Some(&45u64));
        }

        Ok(())
    }

    #[test]
    fn test_signature_vararg() -> crate::Result<()> {
        let env = Environment::new()?;
        let db = env.open(StorageLocation::InMemory)?;
        let conn = db.connect()?;

        let sig = SignatureBuilder::new(
            [
                Parameter::normal("param1", i32::logical_type(&conn)?),
                Parameter::normal("param2", String::logical_type(&conn)?),
                Parameter::tail_vararg("param3", i32::logical_type(&conn)?),
            ],
            u64::logical_type(&conn)?,
        );

        assert_eq!(sig.parameters.len(), 3);

        ScalarFunctionBuilder::new("test", sig, VarargScalar).register(&conn)?;

        let statements = conn.query("SELECT test(10, 'AA', 1, 2, 3, 4, 5, 6, 7, 8, 9)", Parameters::None)?;

        for chunk in statements {
            let chunk = chunk?;

            let vector = chunk.get_vector_at::<u64>(0)?;

            assert_eq!(vector.get(0)?, Some(&11u64));
        }

        Ok(())
    }
}

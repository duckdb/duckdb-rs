//! Function parameter and return-type declarations.
//!
//! Use [`crate::signature::SignatureBuilder`] when registering scalar,
//! aggregate, or table functions. Built [`crate::signature::Signature`] values
//! expose the resolved declaration.

use std::ops::Deref;

use libduckdb_sys::{self as ffi};

use crate::{
    Parameters, Result, check_api_call, check_api_call_no_err, logical_type::LogicalType,
    value::Value,
};

/// An owned function signature.
///
/// Fixed parameters retain their declaration order. A signature may also
/// define a variadic tail and, depending on the function family, a return
/// type.
pub struct Signature(ffi::duckdb_v2_function_signature_handle);

impl Signature {
    /// Return the number of fixed parameters.
    pub fn parameter_count(&self) -> Result<usize> {
        let count = check_api_call!(
            ffi::duckdb_v2_function_signature_get_parameter_count,
            self.0,
            RET
        )?;

        Ok(count as usize)
    }

    /// Return whether a return type is defined.
    fn has_return_type(&self) -> Result<bool> {
        let has_return_type = check_api_call!(
            ffi::duckdb_v2_function_signature_has_return_type,
            self.0,
            RET
        )?;

        Ok(has_return_type)
    }

    /// Return an owned copy of the return type, or `None` when it is undefined.
    pub fn return_type(&self) -> Result<Option<LogicalType>> {
        if !self.has_return_type()? {
            return Ok(None);
        }
        let handle = check_api_call!(
            ffi::duckdb_v2_function_signature_get_return_type,
            self.0,
            RET
        )?;

        Ok(Some(LogicalType { handle }))
    }

    /// Return an owned copy of a fixed parameter's type.
    pub fn parameter_type(&self, index: usize) -> Result<LogicalType> {
        let handle = check_api_call!(
            ffi::duckdb_v2_function_signature_get_parameter_type,
            self.0,
            index as u64,
            RET
        )?;

        Ok(LogicalType { handle })
    }

    /// Return a fixed parameter's name.
    pub fn parameter_name(&self, index: usize) -> Result<String> {
        let name: ffi::duckdb_v2_str = check_api_call!(
            ffi::duckdb_v2_function_signature_get_parameter_name,
            self.0,
            index as u64,
            RET
        )?;

        let name: &str = name.into();
        Ok(name.to_string())
    }

    /// Return whether a fixed parameter has a default value.
    pub fn parameter_has_default(&self, index: usize) -> Result<bool> {
        let has_default = check_api_call!(
            ffi::duckdb_v2_function_signature_parameter_has_default,
            self.0,
            index as u64,
            RET
        )?;

        Ok(has_default)
    }

    /// Return an owned copy of a fixed parameter's default value.
    pub fn parameter_default_value(&self, index: usize) -> Result<Value> {
        let handle = check_api_call!(
            ffi::duckdb_v2_function_signature_get_parameter_default,
            self.0,
            index as u64,
            RET
        )?;

        Ok(Value { handle })
    }

    /// Return a fixed parameter and its optional default value.
    pub fn parameter(&self, index: usize) -> Result<Parameter> {
        Ok(Parameter {
            name: self.parameter_name(index)?,
            logical_type: self.parameter_type(index)?,
            default_value: if self.parameter_has_default(index)? {
                Some(self.parameter_default_value(index)?)
            } else {
                None
            },
        })
    }

    /// Return all fixed parameters in declaration order.
    pub fn parameters(&self) -> Result<Vec<Parameter>> {
        let count = self.parameter_count()?;
        let mut params = Vec::with_capacity(count);

        for i in 0..count {
            params.push(self.parameter(i)?);
        }

        Ok(params)
    }

    /// Return whether the signature accepts trailing arguments.
    fn has_varargs(&self) -> Result<bool> {
        check_api_call!(ffi::duckdb_v2_function_signature_has_varargs, self.0, RET)
    }

    /// Return an owned copy of the variadic tail type, or `None` when it is undefined.
    pub fn vararg(&self) -> Result<Option<LogicalType>> {
        if !self.has_varargs()? {
            return Ok(None);
        }
        let handle = check_api_call!(ffi::duckdb_v2_function_signature_get_varargs, self.0, RET)?;

        Ok(Some(LogicalType { handle }))
    }
}

impl Drop for Signature {
    fn drop(&mut self) {
        check_api_call_no_err!(ffi::duckdb_v2_function_signature_destroy, &mut self.0).unwrap();
    }
}

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
///
/// # Example
/// ```
/// use duckdb_rs::{DuckDBType, Environment, StorageLocation};
/// use duckdb_rs::logical_type::LogicalTypeID;
/// use duckdb_rs::signature::{Parameter, SignatureBuilder};
///
/// # fn main() -> duckdb_rs::Result<()> {
/// let env = Environment::new()?;
/// let db = env.open(StorageLocation::InMemory)?;
/// let conn = db.connect()?;
///
/// let signature = SignatureBuilder::new(
///     [
///         Parameter::normal("value", i32::logical_type(&conn)?),
///         Parameter::tail_vararg("extra", i32::logical_type(&conn)?),
///     ],
///     i32::logical_type(&conn)?,
/// )
/// .build()?;
///
/// assert_eq!(signature.parameter_count()?, 1);
/// assert_eq!(
///     signature.vararg()?.unwrap().type_id(),
///     LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER
/// );
/// assert_eq!(
///     signature.return_type()?.unwrap().type_id(),
///     LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER
/// );
/// # Ok(())
/// # }
/// ```
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

    /// Build an owned signature.
    pub fn build(&self) -> Result<Signature> {
        let handle = check_api_call!(ffi::duckdb_v2_function_signature_create, RET)?;

        let handle = Signature(handle);

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
                        param.logical_type.handle
                    )?;
                }
                ParameterType::WithDefault(param) => {
                    check_api_call!(
                        ffi::duckdb_v2_function_signature_add_parameter_default,
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

        Ok(handle)
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod test {
    use crate::builder_helpers::scalar_callback;
    use crate::logical_type::LogicalTypeID;
    use crate::scalar::ScalarFunctionBuilder;
    use crate::{DuckDBType, Environment, StorageLocation, ToValue};

    use super::*;

    scalar_callback!(
        DefaultParameterScalar,
        u64,
        |input, result, _ctx, _user_data| {
            let vectors_len = input.vectors_count()?;
            let vector = input.get_vector_at::<i32>(2)?;
            let mut result = result;
            result.set_size(1)?;
            result.write(
                0,
                Some(*vector.get(0).unwrap().unwrap() as u64 + vectors_len as u64),
            )?;
            Ok(())
        }
    );

    scalar_callback!(VarargScalar, u64, |input, result, _ctx, _user_data| {
        let vectors_len = input.vectors_count()?;
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

        ScalarFunctionBuilder::new("test", sig, DefaultParameterScalar)
            .register_with_connection(&conn)?;

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

        ScalarFunctionBuilder::new("test", sig, VarargScalar).register_with_connection(&conn)?;

        let statements = conn.query(
            "SELECT test(10, 'AA', 1, 2, 3, 4, 5, 6, 7, 8, 9)",
            Parameters::None,
        )?;

        for chunk in statements {
            let chunk = chunk?;

            let vector = chunk.get_vector_at::<u64>(0)?;

            assert_eq!(vector.get(0)?, Some(&11u64));
        }

        Ok(())
    }

    #[test]
    fn test_signature_reading() -> crate::Result<()> {
        let env = Environment::new()?;
        let db = env.open(StorageLocation::InMemory)?;
        let conn = db.connect()?;

        let sig = SignatureBuilder::new(
            [
                Parameter::normal("param1", i32::logical_type(&conn)?),
                Parameter::normal("param2", String::logical_type(&conn)?),
                Parameter::normal_with_default(
                    "param3",
                    i32::logical_type(&conn)?,
                    42_i32.value(&conn)?,
                ),
                Parameter::tail_vararg("param4", i8::logical_type(&conn)?),
            ],
            u64::logical_type(&conn)?,
        );

        assert_eq!(sig.parameters.len(), 4);

        let signature = sig.build()?;

        assert_eq!(signature.parameter_count()?, 3);
        assert!(signature.has_return_type()?);
        assert_eq!(
            signature.return_type()?.unwrap().type_id(),
            LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_UBIGINT
        );

        let params = signature.parameters()?;
        assert_eq!(params.len(), 3);
        assert_eq!(params[0].name, "param1");
        assert_eq!(
            params[0].logical_type.type_id(),
            LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER
        );
        assert!(params[0].default_value.is_none());
        assert_eq!(params[1].name, "param2");
        assert_eq!(
            params[1].logical_type.type_id(),
            LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR
        );
        assert!(params[1].default_value.is_none());
        assert_eq!(params[2].name, "param3");
        assert_eq!(
            params[2].logical_type.type_id(),
            LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER
        );
        assert!(params[2].default_value.is_some());
        assert_eq!(
            params[2].default_value.as_ref().unwrap().dbg_string()?,
            "42"
        );

        assert!(signature.has_varargs()?);
        assert_eq!(
            signature.vararg()?.unwrap().type_id(),
            LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_TINYINT
        );

        Ok(())
    }
}

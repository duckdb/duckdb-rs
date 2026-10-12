//! Function parameter and return-type declarations.
//!
//! Declare parameters with [`FunctionParameter`], collect them in a
//! [`SignatureBuilder`], and resolve it with [`SignatureBuilder::atomize`]
//! into a [`Signature`] when registering scalar, aggregate, or table functions.

use std::fmt;

use crate::builder_helpers::ffi_enum_redeclaration;
use crate::connection::FFILink;
use crate::{DuckDBType, ToValue, ffi};

use crate::{Result, check_api_call, logical_type::LogicalType, value::Value};

ffi_enum_redeclaration! {
    /// How callers may supply arguments for a parameter.
    pub enum FunctionParameterKind <- ffi::DUCKDB_V2_FUNCTION_PARAMETER_KIND {
        /// Supplied by position only.
        PositionalOnly = DUCKDB_V2_FUNCTION_PARAMETER_KIND_POSITIONAL_ONLY,
        /// Supplied by position or by name.
        Standard = DUCKDB_V2_FUNCTION_PARAMETER_KIND_STANDARD,
        /// A tail accepting zero or more positional arguments.
        PositionalVariadic = DUCKDB_V2_FUNCTION_PARAMETER_KIND_POSITIONAL_VARIADIC,
        /// Supplied by name only.
        Named = DUCKDB_V2_FUNCTION_PARAMETER_KIND_NAMED_ONLY,
        /// Accepts any number of additional named arguments.
        NamedVariadic = DUCKDB_V2_FUNCTION_PARAMETER_KIND_NAMED_VARIADIC,
    }
}

type LogicalTypeFn = fn(&dyn FFILink) -> Result<LogicalType>;

enum ParameterType {
    Rust(LogicalTypeFn),
    Resolved(LogicalType),
}

impl ParameterType {
    fn resolve(self, link: &dyn FFILink) -> Result<LogicalType> {
        match self {
            ParameterType::Rust(create) => create(link),
            ParameterType::Resolved(logical_type) => Ok(logical_type),
        }
    }
}

impl fmt::Debug for ParameterType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParameterType::Rust(_) => f.write_str("Rust"),
            ParameterType::Resolved(logical_type) => f.debug_tuple("Resolved").field(logical_type).finish(),
        }
    }
}

/// Object-safe form of [`ToValue`], whose method is generic over the link.
trait ErasedToValue {
    fn erased_value(&self, link: &dyn FFILink) -> Result<Value>;
}

impl<T: ToValue> ErasedToValue for T {
    fn erased_value(&self, link: &dyn FFILink) -> Result<Value> {
        self.value(link)
    }
}

/// A parameter declaration accepted by [`SignatureBuilder`].
///
/// Its logical type and default value are created by
/// [`SignatureBuilder::atomize`].
pub struct FunctionParameter {
    name: String,
    kind: FunctionParameterKind,
    logical_type: ParameterType,
    default_value: Option<Box<dyn ErasedToValue>>,
}

impl FunctionParameter {
    /// Declare a required parameter whose type is the Rust type `T`.
    pub fn new<T: DuckDBType>(name: impl Into<String>) -> Self {
        Self::declare(name.into(), Self::rust_type::<T>(), None)
    }

    /// Declare a parameter that callers may omit, using `value` in its place.
    pub fn with_default<T: DuckDBType + ToValue + 'static>(name: impl Into<String>, value: T) -> Self {
        Self::declare(name.into(), Self::rust_type::<T>(), Some(Box::new(value)))
    }

    /// Declare a variadic tail accepting zero or more arguments of type `T`.
    pub fn vararg<T: DuckDBType>(name: impl Into<String>) -> Self {
        Self::new::<T>(name).kind(FunctionParameterKind::PositionalVariadic)
    }

    /// Declare a required parameter with an explicit logical type.
    pub fn with_type(name: impl Into<String>, logical_type: LogicalType) -> Self {
        Self::declare(name.into(), ParameterType::Resolved(logical_type), None)
    }

    /// Set how callers may supply arguments for this parameter.
    pub fn kind(mut self, kind: FunctionParameterKind) -> Self {
        self.kind = kind;
        self
    }

    fn rust_type<T: DuckDBType>() -> ParameterType {
        // A non-capturing closure, since `T::logical_type::<dyn FFILink>` would require `'static`.
        ParameterType::Rust(|link| T::logical_type(link))
    }

    fn declare(name: String, logical_type: ParameterType, default_value: Option<Box<dyn ErasedToValue>>) -> Self {
        FunctionParameter {
            name,
            kind: FunctionParameterKind::Standard,
            logical_type,
            default_value,
        }
    }
}

impl fmt::Debug for FunctionParameter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FunctionParameter")
            .field("name", &self.name)
            .field("kind", &self.kind)
            .field("logical_type", &self.logical_type)
            .field("has_default_value", &self.default_value.is_some())
            .finish()
    }
}

/// Declares the signature of a user-defined function.
///
/// Parameters with defaults must follow required parameters, and parameter
/// names must be unique. DuckDB checks these structural rules when the
/// function is registered.
#[derive(Debug)]
pub struct SignatureBuilder {
    parameters: Vec<FunctionParameter>,
    return_type: Option<LogicalType>,
}

impl SignatureBuilder {
    /// Create a signature builder with a return type.
    pub fn new(parameters: impl Into<Vec<FunctionParameter>>, return_type: LogicalType) -> Self {
        SignatureBuilder {
            parameters: parameters.into(),
            return_type: Some(return_type),
        }
    }

    /// Create a signature builder without a return type.
    pub fn without_return_type(parameters: impl Into<Vec<FunctionParameter>>) -> Self {
        SignatureBuilder {
            parameters: parameters.into(),
            return_type: None,
        }
    }

    /// Create the declared logical types and default values.
    ///
    /// Extensions pass the context supplied to their entry point.
    pub fn atomize(self, link: &dyn FFILink) -> Result<Signature> {
        let parameters = self
            .parameters
            .into_iter()
            .map(|param| {
                Ok(ResolvedParameter {
                    default_value: param.default_value.map(|value| value.erased_value(link)).transpose()?,
                    logical_type: param.logical_type.resolve(link)?,
                    name: param.name,
                    kind: param.kind,
                })
            })
            .collect::<Result<_>>()?;

        Ok(Signature {
            parameters,
            return_type: self.return_type,
        })
    }
}

#[derive(Debug)]
struct ResolvedParameter {
    name: String,
    kind: FunctionParameterKind,
    logical_type: LogicalType,
    default_value: Option<Value>,
}

/// A function signature with resolved types, created by
/// [`SignatureBuilder::atomize`].
#[derive(Debug)]
pub struct Signature {
    parameters: Vec<ResolvedParameter>,
    return_type: Option<LogicalType>,
}

impl Signature {
    /// Append parameters and apply configured types to a borrowed signature.
    pub(crate) fn build(&self, handle: &ffi::duckdb_v2_function_signature_handle) -> Result<()> {
        if let Some(return_type) = self.return_type.as_ref() {
            check_api_call!(
                ffi::duckdb_v2_function_signature_set_return_type,
                *handle,
                return_type.handle
            )?;
        }

        for param in &self.parameters {
            check_api_call!(
                ffi::duckdb_v2_function_signature_add_parameter,
                *handle,
                &(&param.name).into(),
                param.logical_type.handle,
                param.default_value.as_ref().map_or(std::ptr::null_mut(), |d| d.handle),
                param.kind.clone().into(),
            )?;
        }

        Ok(())
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod test {
    use crate::Parameters;
    use crate::builder_helpers::scalar_callback;
    use crate::environment::{Environment, StorageLocation};
    use crate::scalar::ScalarFunctionBuilder;
    use crate::types::FromValue;

    use super::*;

    scalar_callback!(DefaultParameterScalar, u64, |input, result, _ctx, _user_data| {
        let vectors_len = input.col_count();
        let vector = input.get_vector_at::<i32>(2)?;
        let mut result = result;
        result.set_size(1)?;
        result.write(0, Some(*vector.get(0).unwrap().unwrap() as u64 + vectors_len as u64))?;
        Ok(())
    });

    scalar_callback!(VarargScalar, u64, |input, result, _ctx, _user_data| {
        let vectors_len = input.col_count();
        let mut result = result;
        result.set_size(1)?;
        result.write(0, Some(vectors_len as u64))?;
        Ok(())
    });

    fn query_u64(conn: &crate::connection::Connection, sql: &str) -> crate::Result<Option<u64>> {
        let mut result = conn.query(sql, Parameters::None)?;
        let chunk = result.next_chunk()?.expect("expected a result chunk");
        Ok(chunk.get_vector_at::<u64>(0)?.get(0)?.copied())
    }

    #[test]
    fn test_signature_atomize() -> crate::Result<()> {
        let env = Environment::new()?;
        let db = env.open(StorageLocation::InMemory)?;
        let conn = db.connect()?;

        let sig = SignatureBuilder::new(
            [
                FunctionParameter::new::<i32>("param1"),
                FunctionParameter::with_type("param2", LogicalType::from_text(&conn, "VARCHAR")?),
                FunctionParameter::with_default("param3", 42_i32),
            ],
            u64::logical_type(&conn)?,
        )
        .atomize(&conn)?;

        assert_eq!(sig.parameters.len(), 3);
        assert_eq!(sig.parameters[0].logical_type, i32::logical_type(&conn)?);
        assert_eq!(sig.parameters[1].logical_type, String::logical_type(&conn)?);
        assert!(sig.parameters[0].default_value.is_none());
        assert_eq!(
            sig.parameters[2]
                .default_value
                .as_ref()
                .map(i32::from_value)
                .transpose()?
                .flatten(),
            Some(42)
        );
        assert_eq!(sig.return_type, Some(u64::logical_type(&conn)?));

        Ok(())
    }

    #[test]
    fn test_signature_default_value() -> crate::Result<()> {
        let env = Environment::new()?;
        let db = env.open(StorageLocation::InMemory)?;
        let conn = db.connect()?;

        let sig = SignatureBuilder::new(
            [
                FunctionParameter::new::<i32>("param1"),
                FunctionParameter::new::<String>("param2"),
                FunctionParameter::with_default("param3", 42_i32),
            ],
            u64::logical_type(&conn)?,
        )
        .atomize(&conn)?;

        ScalarFunctionBuilder::new("test", sig, DefaultParameterScalar).register(&conn)?;

        assert_eq!(query_u64(&conn, "SELECT test(10, 'AA')")?, Some(45));
        assert_eq!(query_u64(&conn, "SELECT test(10, 'AA', 7)")?, Some(10));

        Ok(())
    }

    #[test]
    fn test_signature_named_parameter() -> crate::Result<()> {
        let env = Environment::new()?;
        let db = env.open(StorageLocation::InMemory)?;
        let conn = db.connect()?;

        let sig = SignatureBuilder::new(
            [
                FunctionParameter::new::<i32>("param1"),
                FunctionParameter::new::<String>("param2"),
                FunctionParameter::with_default("param3", 42_i32).kind(FunctionParameterKind::Named),
            ],
            u64::logical_type(&conn)?,
        )
        .atomize(&conn)?;

        ScalarFunctionBuilder::new("test", sig, DefaultParameterScalar).register(&conn)?;

        assert_eq!(query_u64(&conn, "SELECT test(10, 'AA')")?, Some(45));
        assert_eq!(query_u64(&conn, "SELECT test(10, 'AA', param3 := 7)")?, Some(10));
        assert!(conn.query("SELECT test(10, 'AA', 7)", Parameters::None).is_err());

        Ok(())
    }

    #[test]
    fn test_signature_vararg() -> crate::Result<()> {
        let env = Environment::new()?;
        let db = env.open(StorageLocation::InMemory)?;
        let conn = db.connect()?;

        let sig = SignatureBuilder::new(
            [
                FunctionParameter::new::<i32>("param1"),
                FunctionParameter::new::<String>("param2"),
                FunctionParameter::vararg::<i32>("param3"),
            ],
            u64::logical_type(&conn)?,
        )
        .atomize(&conn)?;

        ScalarFunctionBuilder::new("test", sig, VarargScalar).register(&conn)?;

        assert_eq!(
            query_u64(&conn, "SELECT test(10, 'AA', 1, 2, 3, 4, 5, 6, 7, 8, 9)")?,
            Some(11)
        );

        Ok(())
    }
}

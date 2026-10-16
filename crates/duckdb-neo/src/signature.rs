//! Function parameter and return-type declarations.
//!
//! Declare parameters with [`FunctionParameter`] and collect them in a
//! [`SignatureBuilder`], which is passed to a function builder such as
//! [`ScalarFunctionBuilder`](crate::scalar::ScalarFunctionBuilder). Its types
//! and default values are created when the function is registered.
//! A [`FunctionDocumentationBuilder`] optionally describes the overload.
//!
//! ```
//! use duckdb_neo::{
//!     DuckDBType,
//!     environment::{Environment, StorageLocation},
//!     signature::{FunctionDocumentationBuilder, FunctionParameter, SignatureBuilder},
//! };
//!
//! # fn main() -> duckdb_neo::Result<()> {
//! let env = Environment::new()?;
//! let conn = env.open(StorageLocation::InMemory)?.connect()?;
//!
//! let mut docs = FunctionDocumentationBuilder::new();
//! docs.description("Scales a value by an optional factor.");
//! docs.add_example("", "scale(21)");
//!
//! let signature = SignatureBuilder::new()
//!     .param(FunctionParameter::new::<i32>("value"))
//!     .param(FunctionParameter::with_default("factor", 2_i32))
//!     .return_type(i32::logical_type(&conn)?)
//!     .documentation(docs);
//! # Ok(())
//! # }
//! ```

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
/// Its logical type and default value are created when the function is
/// registered.
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

/// Documentation for one overload of a user-defined function.
///
/// Attach it with [`SignatureBuilder::documentation`]. DuckDB reports it in
/// the `description`, `examples` and `categories` columns of
/// `duckdb_functions()`.
#[derive(Debug, Default)]
pub struct FunctionDocumentationBuilder {
    description: Option<String>,
    // (title, example) pairs.
    examples: Vec<(String, String)>,
    categories: Vec<String>,
}

impl FunctionDocumentationBuilder {
    /// Create empty documentation.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the description, replacing any previous one.
    pub fn description(&mut self, description: &str) {
        self.description = Some(description.into());
    }

    /// Add an example call such as `my_func(42)`, with an optional title (empty for none).
    ///
    /// Examples are reported in the order they were added.
    pub fn add_example(&mut self, title: &str, example: &str) {
        self.examples.push((title.into(), example.into()));
    }

    /// Add a category such as `"string"` or `"aggregate"`.
    ///
    /// Categories are reported in the order they were added.
    pub fn add_category(&mut self, category: &str) {
        self.categories.push(category.into());
    }

    fn build(&self, handle: ffi::duckdb_v2_function_docs_handle) -> Result<()> {
        if let Some(description) = &self.description {
            check_api_call!(
                ffi::duckdb_v2_function_docs_set_description,
                handle,
                &description.into()
            )?;
        }

        for (title, example) in &self.examples {
            check_api_call!(
                ffi::duckdb_v2_function_docs_add_example,
                handle,
                &(title).into(),
                &(example).into()
            )?;
        }

        for category in &self.categories {
            check_api_call!(ffi::duckdb_v2_function_docs_add_category, handle, &(category).into(),)?;
        }

        Ok(())
    }
}

/// Declares the signature of a user-defined function.
///
/// Parameters with defaults must follow required parameters, and parameter
/// names must be unique. DuckDB checks these structural rules when the
/// function is registered.
#[derive(Debug, Default)]
pub struct SignatureBuilder {
    parameters: Vec<FunctionParameter>,
    return_type: Option<LogicalType>,
    documentation: Option<FunctionDocumentationBuilder>,
}

impl SignatureBuilder {
    /// Create an empty signature without parameters or a return type.
    pub fn new() -> Self {
        Self::default()
    }

    /// Append a parameter after the ones added so far.
    pub fn param(mut self, parameter: FunctionParameter) -> Self {
        self.parameters.push(parameter);
        self
    }

    /// Set the return type, replacing any previous one.
    ///
    /// Scalar and aggregate functions require one. Table functions leave it
    /// unset and declare their result columns when binding.
    pub fn return_type(mut self, logical_type: LogicalType) -> Self {
        self.return_type = Some(logical_type);
        self
    }

    /// Attach documentation for this overload, replacing any previous one.
    pub fn documentation(mut self, documentation: FunctionDocumentationBuilder) -> Self {
        self.documentation = Some(documentation);
        self
    }

    /// Create the declared logical types and default values through `link`,
    /// and apply them and the documentation to a borrowed signature.
    pub(crate) fn build(
        self,
        link: &dyn FFILink,
        handle: &ffi::duckdb_v2_function_signature_handle,
        docs_handle: &ffi::duckdb_v2_function_docs_handle,
    ) -> Result<()> {
        if let Some(return_type) = self.return_type.as_ref() {
            check_api_call!(
                ffi::duckdb_v2_function_signature_set_return_type,
                *handle,
                return_type.handle
            )?;
        }

        for param in self.parameters {
            let logical_type = param.logical_type.resolve(link)?;
            let default_value = param.default_value.map(|value| value.erased_value(link)).transpose()?;
            check_api_call!(
                ffi::duckdb_v2_function_signature_add_parameter,
                *handle,
                &(&param.name).into(),
                logical_type.handle,
                default_value.as_ref().map_or(std::ptr::null_mut(), |d| d.handle),
                param.kind.into(),
            )?;
        }

        match &self.documentation {
            Some(docs) => docs.build(*docs_handle),
            None => Ok(()),
        }
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod test {
    use crate::Parameters;
    use crate::builder_helpers::scalar_callback;
    use crate::connection::Connection;
    use crate::environment::{Environment, StorageLocation};
    use crate::scalar::ScalarFunctionBuilder;

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
    fn test_signature_types() -> crate::Result<()> {
        let env = Environment::new()?;
        let db = env.open(StorageLocation::InMemory)?;
        let conn = db.connect()?;

        let sig = SignatureBuilder::new()
            .param(FunctionParameter::new::<i32>("param1"))
            .param(FunctionParameter::with_type(
                "param2",
                LogicalType::from_text(&conn, "VARCHAR")?,
            ))
            .param(FunctionParameter::with_default("param3", 42_i32))
            .return_type(u64::logical_type(&conn)?);
        ScalarFunctionBuilder::new("typed", sig, DefaultParameterScalar).register(&conn)?;

        let mut result = conn.query(
            "SELECT parameter_types::VARCHAR, return_type FROM duckdb_functions() WHERE function_name = 'typed'",
            Parameters::None,
        )?;
        let chunk = result.next_chunk()?.expect("expected a result chunk");
        let column =
            |i| -> crate::Result<Option<String>> { Ok(chunk.get_vector_at::<String>(i)?.get(0)?.map(str::to_owned)) };
        assert_eq!(column(0)?.as_deref(), Some("[INTEGER, VARCHAR, INTEGER]"));
        assert_eq!(column(1)?.as_deref(), Some("UBIGINT"));

        Ok(())
    }

    #[test]
    fn test_signature_default_value() -> crate::Result<()> {
        let env = Environment::new()?;
        let db = env.open(StorageLocation::InMemory)?;
        let conn = db.connect()?;

        let sig = SignatureBuilder::new()
            .param(FunctionParameter::new::<i32>("param1"))
            .param(FunctionParameter::new::<String>("param2"))
            .param(FunctionParameter::with_default("param3", 42_i32))
            .return_type(u64::logical_type(&conn)?);

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

        let sig = SignatureBuilder::new()
            .param(FunctionParameter::new::<i32>("param1"))
            .param(FunctionParameter::new::<String>("param2"))
            .param(FunctionParameter::with_default("param3", 42_i32).kind(FunctionParameterKind::Named))
            .return_type(u64::logical_type(&conn)?);

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

        let sig = SignatureBuilder::new()
            .param(FunctionParameter::new::<i32>("param1"))
            .param(FunctionParameter::new::<String>("param2"))
            .param(FunctionParameter::vararg::<i32>("param3"))
            .return_type(u64::logical_type(&conn)?);

        ScalarFunctionBuilder::new("test", sig, VarargScalar).register(&conn)?;

        assert_eq!(
            query_u64(&conn, "SELECT test(10, 'AA', 1, 2, 3, 4, 5, 6, 7, 8, 9)")?,
            Some(11)
        );

        Ok(())
    }

    scalar_callback!(IdentityScalar, i32, |input, result, _ctx, _user_data| {
        let input = input.get_vector_at::<i32>(0)?;
        let mut result = result;
        result.set_size(input.len())?;
        for (i, value) in input.iter()?.enumerate() {
            result.write(i, value.copied())?;
        }
        Ok(())
    });

    fn register(conn: &Connection, name: &str, docs: Option<FunctionDocumentationBuilder>) -> Result<()> {
        let mut signature = SignatureBuilder::new()
            .param(FunctionParameter::new::<i32>("x"))
            .return_type(i32::logical_type(conn)?);
        if let Some(docs) = docs {
            signature = signature.documentation(docs);
        }
        ScalarFunctionBuilder::new(name, signature, IdentityScalar).register(conn)
    }

    /// Returns `description`, `examples` and `categories` from `duckdb_functions()` as text.
    fn query_docs(conn: &Connection, name: &str) -> Result<[Option<String>; 3]> {
        let sql = format!(
            "SELECT description, examples::VARCHAR, categories::VARCHAR \
             FROM duckdb_functions() WHERE function_name = '{name}'"
        );
        let mut result = conn.query(sql.as_str(), Parameters::None)?;
        let chunk = result.next_chunk()?.expect("expected a result chunk");
        assert_eq!(chunk.row_count()?, 1);
        let column = |i| -> Result<Option<String>> { Ok(chunk.get_vector_at::<String>(i)?.get(0)?.map(str::to_owned)) };
        Ok([column(0)?, column(1)?, column(2)?])
    }

    #[test]
    fn test_function_documentation() -> Result<()> {
        let env = Environment::new()?;
        let conn = env.open(StorageLocation::InMemory)?.connect()?;

        let mut docs = FunctionDocumentationBuilder::new();
        docs.description("Returns its input.");
        docs.add_example("Basic", "documented(42)");
        docs.add_example("", "documented(NULL)");
        docs.add_category("numeric");
        docs.add_category("testing");
        register(&conn, "documented", Some(docs))?;

        assert_eq!(
            query_docs(&conn, "documented")?,
            [
                Some("Returns its input.".to_owned()),
                Some("['documented(42)', 'documented(NULL)']".to_owned()),
                Some("[numeric, testing]".to_owned()),
            ]
        );
        Ok(())
    }

    #[test]
    fn test_function_documentation_partial() -> Result<()> {
        let env = Environment::new()?;
        let conn = env.open(StorageLocation::InMemory)?.connect()?;

        let mut docs = FunctionDocumentationBuilder::new();
        docs.add_category("testing");
        register(&conn, "categorized", Some(docs))?;
        register(&conn, "undocumented", None)?;

        assert_eq!(
            query_docs(&conn, "categorized")?,
            [None, Some("[]".to_owned()), Some("[testing]".to_owned())]
        );
        assert_eq!(
            query_docs(&conn, "undocumented")?,
            [None, Some("[]".to_owned()), Some("[]".to_owned())]
        );
        Ok(())
    }
}

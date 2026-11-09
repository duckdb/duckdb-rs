//! Function parameter and return-type declarations.
//!
//! Declare parameters with [`FunctionParameter`] and collect them in a
//! [`SignatureBuilder`], which is passed to a function builder such as
//! [`ScalarFunctionBuilder`](crate::scalar::ScalarFunctionBuilder). Its types
//! and default values are created when the function is registered. Callbacks
//! receive arguments in the parameter-kind order described by [`SignatureBuilder::param`].
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
//! let docs = FunctionDocumentationBuilder::new()
//!     .description("Scales a value by an optional factor.")
//!     .add_example("", "scale(21)");
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

enum TypeDeclaration {
    Rust(LogicalTypeFn),
    Resolved(LogicalType),
}

impl TypeDeclaration {
    fn rust_type<T: DuckDBType>() -> Self {
        // A non-capturing closure, since `T::logical_type::<dyn FFILink>` would require `'static`.
        Self::Rust(|link| T::logical_type(link))
    }

    fn resolve(self, link: &dyn FFILink) -> Result<LogicalType> {
        match self {
            Self::Rust(create) => create(link),
            Self::Resolved(logical_type) => Ok(logical_type),
        }
    }
}

impl fmt::Debug for TypeDeclaration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Rust(_) => f.write_str("Rust"),
            Self::Resolved(logical_type) => f.debug_tuple("Resolved").field(logical_type).finish(),
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
    logical_type: TypeDeclaration,
    default_value: Option<Box<dyn ErasedToValue>>,
}

impl FunctionParameter {
    /// Declare a required parameter whose type is the Rust type `T`.
    pub fn new<T: DuckDBType>(name: impl Into<String>) -> Self {
        Self::declare(name.into(), TypeDeclaration::rust_type::<T>(), None)
    }

    /// Declare a parameter that callers may omit, using `value` in its place.
    pub fn with_default<T: DuckDBType + ToValue + 'static>(name: impl Into<String>, value: T) -> Self {
        Self::declare(name.into(), TypeDeclaration::rust_type::<T>(), Some(Box::new(value)))
    }

    /// Declare a variadic tail accepting zero or more arguments of type `T`.
    pub fn vararg<T: DuckDBType>(name: impl Into<String>) -> Self {
        Self::new::<T>(name).kind(FunctionParameterKind::PositionalVariadic)
    }

    /// Declare a required parameter with an explicit logical type.
    pub fn with_type(name: impl Into<String>, logical_type: LogicalType) -> Self {
        Self::declare(name.into(), TypeDeclaration::Resolved(logical_type), None)
    }

    /// Set the default value while preserving the declared logical type.
    pub fn default_value(mut self, value: impl ToValue + 'static) -> Self {
        self.default_value = Some(Box::new(value));
        self
    }

    /// Set how callers may supply arguments for this parameter.
    pub fn kind(mut self, kind: FunctionParameterKind) -> Self {
        self.kind = kind;
        self
    }

    fn declare(name: String, logical_type: TypeDeclaration, default_value: Option<Box<dyn ErasedToValue>>) -> Self {
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
    pub fn description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Add an example call such as `my_func(42)`, with an optional title (empty for none).
    /// Examples are reported in the order they were added.
    pub fn add_example(mut self, title: impl Into<String>, example: impl Into<String>) -> Self {
        self.examples.push((title.into(), example.into()));
        self
    }

    /// Add a category such as `"string"` or `"aggregate"`.
    /// Categories are reported in the order they were added.
    pub fn add_category(mut self, category: impl Into<String>) -> Self {
        self.categories.push(category.into());
        self
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

/// Declares a signature; DuckDB rejects duplicate names, multiple `*args` or `**kwargs`, and variadic defaults.
/// Registration also rejects a required standard parameter following a defaulted standard parameter.
#[derive(Debug, Default)]
pub struct SignatureBuilder {
    parameters: Vec<FunctionParameter>,
    return_type: Option<TypeDeclaration>,
    documentation: Option<FunctionDocumentationBuilder>,
}

impl SignatureBuilder {
    /// Create an empty signature without parameters or a return type.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a parameter; DuckDB orders kinds as positional-only, standard, `*args`, named-only, then `**kwargs`.
    /// Insertion order is preserved within each kind; callbacks receive arguments in this order.
    pub fn param(mut self, parameter: FunctionParameter) -> Self {
        self.parameters.push(parameter);
        self
    }

    /// Set the return type, replacing any previous one.
    ///
    /// Scalar and aggregate functions require one. Table functions leave it
    /// unset and declare their result columns when binding.
    pub fn return_type(mut self, logical_type: LogicalType) -> Self {
        self.return_type = Some(TypeDeclaration::Resolved(logical_type));
        self
    }

    /// Set the return type from `T`, resolving its logical type at registration.
    pub fn return_type_of<T: DuckDBType>(mut self) -> Self {
        self.return_type = Some(TypeDeclaration::rust_type::<T>());
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
        if let Some(return_type) = self.return_type {
            let return_type = return_type.resolve(link)?;
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
    use crate::connection::{Connection, Context};
    use crate::data_chunk::VectorCollection;
    use crate::environment::{Environment, StorageLocation};
    use crate::scalar::MicroScalar;
    use crate::vector::Vector;

    use super::*;

    fn default_parameter(_ctx: &Context, input: &mut VectorCollection, mut result: Vector<'_, u64>) -> Result<()> {
        let vectors_len = input.col_count();
        let vector = input.get_vector_at::<i32>(2)?;
        result.writer()?.set(
            0,
            Some(*vector.reader()?.get(0).unwrap().unwrap() as u64 + vectors_len as u64),
        )?;
        Ok(())
    }

    fn vararg(_ctx: &Context, input: &mut VectorCollection, mut result: Vector<'_, u64>) -> Result<()> {
        let vectors_len = input.col_count();
        result.writer()?.set(0, Some(vectors_len as u64))?;
        Ok(())
    }

    fn query_u64(conn: &crate::connection::Connection, sql: &str) -> crate::Result<Option<u64>> {
        let mut result = conn.query(sql, Parameters::None)?;
        let chunk = result.next_chunk()?.expect("expected a result chunk");
        Ok(chunk.get_vector_at::<u64>(0)?.reader()?.get(0)?.copied())
    }

    #[test]
    fn test_signature_types() -> crate::Result<()> {
        let env = Environment::new()?;
        let instance = env.open(StorageLocation::InMemory)?;
        let conn = instance.connect()?;

        let sig = SignatureBuilder::new()
            .param(FunctionParameter::new::<i32>("param1"))
            .param(FunctionParameter::with_type(
                "param2",
                LogicalType::from_text(&conn, "VARCHAR")?,
            ))
            .param(FunctionParameter::with_default("param3", 42_i32))
            .return_type(u64::logical_type(&conn)?);
        MicroScalar::with_signature("typed", sig, default_parameter).register(&conn)?;

        let mut result = conn.query(
            "SELECT parameter_types::VARCHAR, return_type FROM duckdb_functions() WHERE function_name = 'typed'",
            Parameters::None,
        )?;
        let chunk = result.next_chunk()?.expect("expected a result chunk");
        let column = |i| -> crate::Result<Option<String>> {
            Ok(chunk.get_vector_at::<String>(i)?.reader()?.get(0)?.map(str::to_owned))
        };
        assert_eq!(column(0)?.as_deref(), Some("[INTEGER, VARCHAR, INTEGER]"));
        assert_eq!(column(1)?.as_deref(), Some("UBIGINT"));

        Ok(())
    }

    #[test]
    fn test_return_type_of_replaces_previous_type() -> crate::Result<()> {
        let env = Environment::new()?;
        let conn = env.open(StorageLocation::InMemory)?.connect()?;
        for (name, signature) in [
            (
                "rust_return",
                SignatureBuilder::new()
                    .return_type(i32::logical_type(&conn)?)
                    .return_type_of::<u64>(),
            ),
            (
                "resolved_return",
                SignatureBuilder::new()
                    .return_type_of::<i32>()
                    .return_type(u64::logical_type(&conn)?),
            ),
        ] {
            MicroScalar::with_signature(name, signature, vararg).register(&conn)?;
            assert_eq!(query_u64(&conn, &format!("SELECT {name}()"))?, Some(0));
        }
        Ok(())
    }

    #[test]
    fn test_return_type_of_defers_errors_to_registration() -> crate::Result<()> {
        struct InvalidType;

        impl DuckDBType for InvalidType {
            fn logical_type<C: FFILink + ?Sized>(_link: &C) -> Result<LogicalType> {
                Err(crate::Error::invalid_input("invalid return type"))
            }
        }

        let signature = SignatureBuilder::new().return_type_of::<InvalidType>();
        let env = Environment::new()?;
        let conn = env.open(StorageLocation::InMemory)?.connect()?;
        let error = MicroScalar::with_signature("invalid_return", signature, vararg)
            .register(&conn)
            .expect_err("logical type errors must propagate");
        assert_eq!(error.message, "invalid return type");
        Ok(())
    }

    #[test]
    fn test_signature_default_value() -> crate::Result<()> {
        let env = Environment::new()?;
        let instance = env.open(StorageLocation::InMemory)?;
        let conn = instance.connect()?;

        let sig = SignatureBuilder::new()
            .param(FunctionParameter::new::<i32>("param1"))
            .param(FunctionParameter::new::<String>("param2"))
            .param(FunctionParameter::with_default("param3", 42_i32))
            .return_type(u64::logical_type(&conn)?);

        MicroScalar::with_signature("test", sig, default_parameter).register(&conn)?;

        assert_eq!(query_u64(&conn, "SELECT test(10, 'AA')")?, Some(45));
        assert_eq!(query_u64(&conn, "SELECT test(10, 'AA', 7)")?, Some(10));

        Ok(())
    }

    #[test]
    fn test_signature_named_parameter() -> crate::Result<()> {
        let env = Environment::new()?;
        let instance = env.open(StorageLocation::InMemory)?;
        let conn = instance.connect()?;

        let sig = SignatureBuilder::new()
            .param(FunctionParameter::new::<i32>("param1"))
            .param(FunctionParameter::new::<String>("param2"))
            .param(FunctionParameter::with_default("param3", 42_i32).kind(FunctionParameterKind::Named))
            .return_type(u64::logical_type(&conn)?);

        MicroScalar::with_signature("test", sig, default_parameter).register(&conn)?;

        assert_eq!(query_u64(&conn, "SELECT test(10, 'AA')")?, Some(45));
        assert_eq!(query_u64(&conn, "SELECT test(10, 'AA', param3 := 7)")?, Some(10));
        assert!(conn.query("SELECT test(10, 'AA', 7)", Parameters::None).is_err());

        Ok(())
    }

    #[test]
    fn test_signature_vararg() -> crate::Result<()> {
        let env = Environment::new()?;
        let instance = env.open(StorageLocation::InMemory)?;
        let conn = instance.connect()?;

        let sig = SignatureBuilder::new()
            .param(FunctionParameter::new::<i32>("param1"))
            .param(FunctionParameter::new::<String>("param2"))
            .param(FunctionParameter::vararg::<i32>("param3"))
            .return_type(u64::logical_type(&conn)?);

        MicroScalar::with_signature("test", sig, vararg).register(&conn)?;

        assert_eq!(
            query_u64(&conn, "SELECT test(10, 'AA', 1, 2, 3, 4, 5, 6, 7, 8, 9)")?,
            Some(11)
        );

        Ok(())
    }

    fn identity(_ctx: &Context, input: &mut VectorCollection, mut result: Vector<'_, i32>) -> Result<()> {
        let input = input.get_vector_at::<i32>(0)?;
        for (i, value) in input.reader()?.iter().enumerate() {
            result.writer()?.set(i, value.copied())?;
        }
        Ok(())
    }

    #[test]
    fn test_signature_explicit_type_default_value() -> Result<()> {
        let env = Environment::new()?;
        let conn = env.open(StorageLocation::InMemory)?.connect()?;
        let signature = SignatureBuilder::new()
            .param(FunctionParameter::with_type("x", LogicalType::from_text(&conn, "INTEGER")?).default_value(42_i64))
            .return_type(i32::logical_type(&conn)?);
        MicroScalar::with_signature("typed_default", signature, identity).register(&conn)?;

        assert_eq!(query_u64(&conn, "SELECT typed_default()::UBIGINT")?, Some(42));
        assert_eq!(query_u64(&conn, "SELECT typed_default(7)::UBIGINT")?, Some(7));
        Ok(())
    }

    fn register(conn: &Connection, name: &str, docs: Option<FunctionDocumentationBuilder>) -> Result<()> {
        let mut signature = SignatureBuilder::new()
            .param(FunctionParameter::new::<i32>("x"))
            .return_type(i32::logical_type(conn)?);
        if let Some(docs) = docs {
            signature = signature.documentation(docs);
        }
        MicroScalar::with_signature(name, signature, identity).register(conn)
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
        let column = |i| -> Result<Option<String>> {
            Ok(chunk.get_vector_at::<String>(i)?.reader()?.get(0)?.map(str::to_owned))
        };
        Ok([column(0)?, column(1)?, column(2)?])
    }

    #[test]
    fn test_function_documentation() -> Result<()> {
        let env = Environment::new()?;
        let conn = env.open(StorageLocation::InMemory)?.connect()?;

        let docs = FunctionDocumentationBuilder::new()
            .description("Returns its input.".to_owned())
            .add_example("Basic", "documented(42)".to_owned())
            .add_example("", "documented(NULL)")
            .add_category("numeric".to_owned())
            .add_category("testing");
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

        let docs = FunctionDocumentationBuilder::new().add_category("testing");
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

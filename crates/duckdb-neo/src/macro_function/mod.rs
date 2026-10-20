//! SQL macros registered by extensions, with scalar-expression or table-query bodies.
//! Outside an extension, define macros with SQL `CREATE MACRO` instead.

use crate::builder_helpers::ffi_enum_redeclaration;
use crate::ffi;

use crate::{
    Result, check_api_call,
    connection::FFILink,
    handles::{MacroFunctionBuilderHandle, MacroFunctionBuilderLink},
    raw::RawExt,
    signature::SignatureBuilder,
};

ffi_enum_redeclaration! {
    pub(crate) enum MacroFunctionType <- ffi::DUCKDB_V2_MACRO_FUNCTION_TYPE {
        Scalar = DUCKDB_V2_MACRO_FUNCTION_TYPE_SCALAR,
        Table = DUCKDB_V2_MACRO_FUNCTION_TYPE_TABLE
    }
}

/// The SQL body and kind of a macro.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MacroBody {
    /// A single SQL expression, called like a scalar function.
    Scalar(String),
    /// A single `SELECT` query, called like a table function.
    Table(String),
}

impl MacroBody {
    fn macro_type(&self) -> MacroFunctionType {
        match self {
            MacroBody::Scalar(_) => MacroFunctionType::Scalar,
            MacroBody::Table(_) => MacroFunctionType::Table,
        }
    }

    fn sql(&self) -> &str {
        match self {
            MacroBody::Scalar(sql) | MacroBody::Table(sql) => sql,
        }
    }
}

/// Builds and registers a macro function through an [`Extension`](crate::extension::Extension).
///
/// The body refers to parameters by name. It is parsed during registration, but
/// names are resolved when the macro is called. The signature accepts only
/// standard parameters, with optional defaults; `ANY` leaves a parameter untyped.
/// An explicit return type is not allowed: the body determines the result type.
#[derive(Debug)]
pub struct MacroFunctionBuilder {
    name: String,
    signature: SignatureBuilder,
    body: MacroBody,
}

impl MacroFunctionBuilder {
    /// Create a builder with the given macro name, signature, and SQL body.
    pub fn new(name: impl Into<String>, signature: SignatureBuilder, body: MacroBody) -> Self {
        Self {
            name: name.into(),
            signature,
            body,
        }
    }

    fn build(&mut self, handle: &MacroFunctionBuilderHandle, link: &dyn FFILink) -> Result<()> {
        check_api_call!(
            ffi::duckdb_v2_macro_function_set_name,
            handle.raw(),
            &mut (&self.name).into()
        )?;

        let signature = check_api_call!(ffi::duckdb_v2_macro_function_get_signature, handle.raw(), RET)?;

        let docs = check_api_call!(ffi::duckdb_v2_macro_function_get_docs, handle.raw(), RET)?;

        std::mem::take(&mut self.signature).build(link, &signature, &docs)?;

        check_api_call!(
            ffi::duckdb_v2_macro_function_set_body,
            handle.raw(),
            &mut self.body.sql().into()
        )
    }

    /// Register the macro through an extension, consuming the builder.
    /// Returns an error for an invalid body or signature, or if the name is already registered.
    #[allow(private_bounds)] // Keep the handle factory private.
    pub fn register<C: MacroFunctionBuilderLink + FFILink>(mut self, link: &C) -> Result<()> {
        let handle = link.create_macro_function_handle(self.body.macro_type())?;
        self.build(&handle, link)?;

        link.register_macro_function_handle(handle)
    }
}

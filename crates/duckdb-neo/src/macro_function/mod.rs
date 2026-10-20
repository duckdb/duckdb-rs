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
    pub enum MacroFunctionType <- ffi::DUCKDB_V2_MACRO_FUNCTION_TYPE {
        Scalar = DUCKDB_V2_MACRO_FUNCTION_TYPE_SCALAR,
        Table = DUCKDB_V2_MACRO_FUNCTION_TYPE_TABLE
    }
}

#[derive(Debug)]
pub struct MacroFunctionBuilder {
    name: String,
    signature: SignatureBuilder,
    query: String,
    macro_type: MacroFunctionType,
}

impl MacroFunctionBuilder {
    pub fn new(
        name: impl Into<String>,
        signature: SignatureBuilder,
        macro_type: MacroFunctionType,
        body: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            signature,
            query: body.into(),
            macro_type,
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
            &mut (&self.query).into()
        )
    }

    #[allow(private_bounds)] // Keep the handle factory private.
    pub fn register<C: MacroFunctionBuilderLink + FFILink>(mut self, link: &C) -> Result<()> {
        let handle = link.create_macro_function_handle(self.macro_type)?;
        self.build(&handle, link)?;

        link.register_macro_function_handle(handle)
    }
}

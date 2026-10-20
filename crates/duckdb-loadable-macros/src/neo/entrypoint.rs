use darling::{FromMeta, ast::NestedMeta, util::SpannedValue};
use proc_macro2::{Span, TokenStream};
use quote::quote_spanned;
use syn::{Ident, Item, spanned::Spanned};

type Result<T> = std::result::Result<T, darling::Error>;

#[derive(Debug, FromMeta)]
struct EntrypointArgs {
    extension_name: SpannedValue<String>,
    api_version: String,
}

fn is_valid_extension_name(name: &str) -> bool {
    let mut chars = name.chars();
    chars
        .next()
        .is_some_and(|first| first.is_ascii_alphabetic() || first == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

pub fn parse(meta: TokenStream, ast: Item) -> Result<TokenStream> {
    let attr_args = NestedMeta::parse_meta_list(meta)?;

    let args = EntrypointArgs::from_list(&attr_args)?;

    let Item::Fn(func) = ast else {
        return Err(
            darling::Error::custom("#[duckdb_entrypoint_c_api_v2] can only be applied to a function").with_span(&ast),
        );
    };

    let extension_name = args.extension_name.as_str();
    if !is_valid_extension_name(extension_name) {
        return Err(darling::Error::custom(format!(
            "extension_name must be a valid identifier of ASCII letters, digits and underscores, got {extension_name:?}"
        ))
        .with_span(&args.extension_name.span()));
    }

    let c_entrypoint = Ident::new(&format!("{extension_name}_init_c_api_v2"), Span::call_site());
    let func_ident = func.sig.ident.clone();
    let describe = Ident::new(
        &format!("duckdb_extension_{extension_name}_describe"),
        Span::call_site(),
    );
    let version = args.api_version;

    Ok(quote_spanned! {func.span() =>
        /// # Safety
        /// Only DuckDB may call this, with its own entrypoint input.
        #[unsafe(no_mangle)]
        #[allow(clippy::useless_conversion)]
        pub unsafe extern "C" fn #c_entrypoint(input: *mut ::duckdb_neo::ffi::duckdb_v2_extension_input) {
            unsafe {
                ::duckdb_neo::extension::run_extension_entrypoint(input, #version, |extension| {
                    #func_ident(extension).map_err(::std::convert::Into::into)
                })
            }
        }

        ::duckdb_neo::__describe_static_extension!(#describe, #c_entrypoint, #extension_name, #version);

        #func
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use quote::quote;

    fn expand(meta: TokenStream, item: TokenStream) -> Result<TokenStream> {
        parse(meta, syn::parse2(item).unwrap())
    }

    fn error_message(meta: TokenStream, item: TokenStream) -> String {
        expand(meta, item).unwrap_err().to_string()
    }

    #[test]
    fn test_emits_entrypoint_and_describe() {
        let output = expand(
            quote!(extension_name = "my_ext", api_version = "v2.0.0"),
            quote!(
                fn entrypoint(extension: &Extension) -> Result<()> {
                    Ok(())
                }
            ),
        )
        .unwrap()
        .to_string();
        assert!(output.contains("my_ext_init_c_api_v2"));
        assert!(output.contains("duckdb_extension_my_ext_describe"));
    }

    #[test]
    fn test_rejects_non_function_item() {
        let message = error_message(
            quote!(extension_name = "my_ext", api_version = "v2.0.0"),
            quote!(
                struct NotAFunction;
            ),
        );
        assert!(message.contains("can only be applied to a function"), "{message}");
    }

    #[test]
    fn test_rejects_invalid_extension_name() {
        for name in ["my-ext", "1ext", "", "ext name"] {
            let message = error_message(
                quote!(extension_name = #name, api_version = "v2.0.0"),
                quote!(
                    fn entrypoint(extension: &Extension) -> Result<()> {
                        Ok(())
                    }
                ),
            );
            assert!(
                message.contains("extension_name must be a valid identifier"),
                "{name}: {message}"
            );
        }
    }

    #[test]
    fn test_rejects_missing_argument() {
        let message = error_message(
            quote!(extension_name = "my_ext"),
            quote!(
                fn entrypoint(extension: &Extension) -> Result<()> {
                    Ok(())
                }
            ),
        );
        assert!(message.contains("api_version"), "{message}");
    }
}

use darling::{FromMeta, ast::NestedMeta};
use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::quote_spanned;
use syn::{Ident, Item, spanned::Spanned};

type Result<T> = std::result::Result<T, darling::Error>;

#[derive(Debug, FromMeta)]
struct EntrypointArgs {
    extension_name: String,
    api_version: String,
}

pub fn parse(meta: TokenStream, ast: syn::Item) -> Result<TokenStream> {
    let attr_args = NestedMeta::parse_meta_list(meta.into())?;

    let args = EntrypointArgs::from_list(&attr_args)?;

    let func = match ast {
        Item::Fn(func) => func,
        _ => panic!("Only function items are allowed for duckdb_entrypoint_c_api_v2"),
    };

    let c_entrypoint = Ident::new(
        format!("{}_init_c_api_v2", args.extension_name).as_str(),
        Span::call_site(),
    );
    let func_ident = func.sig.ident.clone();
    let version = args.api_version;

    Ok(quote_spanned! {func.span() =>
        /// # Safety
        /// Only DuckDB may call this, with its own entrypoint input.
        #[unsafe(no_mangle)]
        #[allow(clippy::useless_conversion)]
        pub unsafe extern "C" fn #c_entrypoint(input: *mut ::duckdb_neo::ffi::duckdb_v2_extension_input) {
            unsafe {
                ::duckdb_neo::connection::run_extension_entrypoint(input, #version, |extension| {
                    #func_ident(extension).map_err(::std::convert::Into::into)
                })
            }
        }

        #func
    }
    .into())
}

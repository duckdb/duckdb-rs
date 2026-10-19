use darling::FromMeta;
use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::quote_spanned;
use syn::{Ident, Item, parse_macro_input, spanned::Spanned};

type Result<T> = std::result::Result<T, darling::Error>;

#[derive(Debug, FromMeta)]
struct EntrypointArgs {
    extension_name: String,
}

pub fn parse(meta: TokenStream, ast: syn::Item) -> Result<TokenStream> {
    // let args = EntrypointArgs::from_meta(meta)?;
    let args = EntrypointArgs {
        extension_name: "okkie".into(),
    };

    let func = match ast {
        Item::Fn(func) => func,
        _ => panic!("Only function items are allowed for duckdb_entrypoint_c_api_v2"),
    };

    let c_entrypoint = Ident::new(
        format!("{}_init_c_api_v2", args.extension_name).as_str(),
        Span::call_site(),
    );
    let func_ident = func.sig.ident.clone();
    let prefixed_original_function = func.sig.ident.clone();
    let c_entrypoint_internal = Ident::new(
        format!("{}_init_c_api_v2_internal", args.extension_name).as_str(),
        Span::call_site(),
    );

    Ok(quote_spanned! {func.span() =>
        #[unsafe(no_mangle)]
        pub unsafe fn #c_entrypoint_internal(
            extension: ::duckdb_neo::ffi::duckdb_v2_extension_handle,
            context: ::duckdb_neo::ffi::duckdb_v2_context_handle,
            err: *const extension: ::duckdb_neo::ffi::duckdb_v2_error_info_handle
        ) {
            let extension = ::duckdb_neo::connection::Extension {
                handle: extension,
                context: ::duckdb_neo::connection::Context::from_raw(context)
            }?;

            #func_ident(extension, err)
        }

    }
    .into())
}

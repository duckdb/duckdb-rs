#![allow(clippy::redundant_clone)]
use proc_macro2::{Ident, Literal, Punct, Span};

use syn::{parse_macro_input, spanned::Spanned, Item};

use proc_macro::TokenStream;
use quote::quote_spanned;

use darling::{ast::NestedMeta, Error, FromMeta};
use syn::ItemFn;

/// For parsing the arguments to the duckdb_entrypoint_c_api macro
#[derive(Debug, FromMeta)]
struct CEntryPointMacroArgs {
    #[darling(default)]
    ext_name: String,
    min_duckdb_version: Option<String>,
}

/// Wraps an entrypoint function to expose an unsafe extern "C" function of the same name.
#[proc_macro_attribute]
pub fn duckdb_entrypoint_c_api(attr: TokenStream, item: TokenStream) -> TokenStream {
    let attr_args = match NestedMeta::parse_meta_list(attr.into()) {
        Ok(v) => v,
        Err(e) => {
            return TokenStream::from(Error::from(e).write_errors());
        }
    };

    let args = match CEntryPointMacroArgs::from_list(&attr_args) {
        Ok(v) => v,
        Err(e) => {
            return TokenStream::from(e.write_errors());
        }
    };

    /// TODO FETCH THE DEFAULT AUTOMATICALLY SOMEHOW
    let minimum_duckdb_version = match args.min_duckdb_version {
        Some(i) => i,
        None => "dev".to_string(),
    };

    let ast = parse_macro_input!(item as syn::Item);

    match ast {
        Item::Fn(mut func) => {
            let c_entrypoint = Ident::new(format!("{}_init_c_api", args.ext_name).as_str(), Span::call_site());
            let original_funcname = func.sig.ident.to_string();
            let prefixed_original_function = func.sig.ident.clone();

            quote_spanned! {func.span()=>

                /// # Safety
                ///
                /// Will be called by duckdb
                #[no_mangle]
                pub unsafe extern "C" fn #c_entrypoint(info: ffi::duckdb_extension_info, access: ffi::duckdb_extension_access) {
                    ffi::duckdb_rs_extension_api_init(info, access).expect("Failed to initialize DuckDB C Extension API");

                    let db : ffi::duckdb_database = *access.get_database.unwrap()(info);
                    let connection = Connection::open_from_raw(db.cast()).expect("can't open db connection");
                    #prefixed_original_function(connection).expect("init failed");
                }

                #func
            }
            .into()
        }
        _ => panic!("Only function items are allowed on duckdb_entrypoint"),
    }
}

/// Wraps an entrypoint function to expose an unsafe extern "C" function of the same name.
#[proc_macro_attribute]
pub fn duckdb_entrypoint(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(item as syn::Item);
    match ast {
        Item::Fn(mut func) => {
            let c_entrypoint = func.sig.ident.clone();
            let c_entrypoint_version = Ident::new(
                c_entrypoint.to_string().replace("_init", "_version").as_str(),
                Span::call_site(),
            );

            let original_funcname = func.sig.ident.to_string();
            func.sig.ident = Ident::new(format!("_{}", original_funcname).as_str(), func.sig.ident.span());

            let prefixed_original_function = func.sig.ident.clone();

            quote_spanned! {func.span()=>
                #func

                /// # Safety
                ///
                /// Will be called by duckdb
                #[no_mangle]
                pub unsafe extern "C" fn #c_entrypoint(db: *mut c_void) {
                    let connection = Connection::open_from_raw(db.cast()).expect("can't open db connection");
                    #prefixed_original_function(connection).expect("init failed");
                }

                /// # Safety
                ///
                /// Predefined function, don't need to change unless you are sure
                #[no_mangle]
                pub unsafe extern "C" fn #c_entrypoint_version() -> *const c_char {
                    ffi::duckdb_library_version()
                }


            }
            .into()
        }
        _ => panic!("Only function items are allowed on duckdb_entrypoint"),
    }
}

#![allow(clippy::redundant_clone)]
use proc_macro2::{Ident, Span};

use syn::{parse_macro_input, spanned::Spanned, Item};

use proc_macro::TokenStream;
use quote::quote_spanned;

use darling::{ast::NestedMeta, Error, FromMeta};

/// For parsing the arguments to the duckdb_entrypoint_c_api macro
#[derive(Debug, FromMeta)]
struct CEntryPointMacroArgs {
    #[darling(default)]
    /// The name to be given to this extension. This name is used in the entrypoint function called by duckdb
    ext_name: String,
    /// The minimum C API version this extension requires. It is recommended to set this to the lowest possible version
    /// at which your extension still compiles
    min_duckdb_version: Option<String>,
}

/// Wraps an entrypoint function to expose an unsafe extern "C" function of the same name.
/// Warning: experimental!
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

    // Set the minimum duckdb version (dev by default)
    let minimum_duckdb_version = match args.min_duckdb_version {
        Some(i) => i,
        None => "dev".to_string(),
    };

    let ast = parse_macro_input!(item as syn::Item);

    match ast {
        Item::Fn(func) => {
            let c_entrypoint = Ident::new(format!("{}_init_c_api", args.ext_name).as_str(), Span::call_site());
            let prefixed_original_function = func.sig.ident.clone();
            let c_entrypoint_internal = Ident::new(
                format!("{}_init_c_api_internal", args.ext_name).as_str(),
                Span::call_site(),
            );

            quote_spanned! {func.span()=>
                /// # Safety
                ///
                /// Internal Entrypoint for error handling
                pub unsafe fn #c_entrypoint_internal(info: ffi::duckdb_extension_info, access: *const ffi::duckdb_extension_access) -> Result<bool, Box<dyn std::error::Error>> {
                    let have_api_struct = ffi::duckdb_rs_extension_api_init(info, access, #minimum_duckdb_version).unwrap();

                    if !have_api_struct {
                        // initialization failed to return an api struct, likely due to an API version mismatch, we can simply return here
                        return Ok(false);
                    }

                    // TODO: handle error here?
                    let db : ffi::duckdb_database = *(*access).get_database.unwrap()(info);
                    let connection = Connection::open_from_raw(db.cast())?;

                    #prefixed_original_function(connection)?;

                    Ok(true)
                }

                /// # Safety
                ///
                /// Entrypoint that will be called by DuckDB
                #[no_mangle]
                pub unsafe extern "C" fn #c_entrypoint(info: ffi::duckdb_extension_info, access: *const ffi::duckdb_extension_access) -> bool {
                    let init_result = #c_entrypoint_internal(info, access);

                    if let Err(x) = init_result {
                        let error_c_string = std::ffi::CString::new(x.to_string());

                        match error_c_string {
                            Ok(e) => {
                                (*access).set_error.unwrap()(info, e.as_ptr());
                            },
                            Err(_e) => {
                                let error_alloc_failure = c"An error occured but the extension failed to allocate memory for an error string";
                                (*access).set_error.unwrap()(info, error_alloc_failure.as_ptr());
                            }
                        }
                        return false;
                    }

                    init_result.unwrap()
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

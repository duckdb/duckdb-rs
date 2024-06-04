#![allow(clippy::redundant_clone)]
use proc_macro2::{Ident, Span};

use syn::{parse_macro_input, spanned::Spanned, Item};

use proc_macro::TokenStream;
use quote::quote_spanned;

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

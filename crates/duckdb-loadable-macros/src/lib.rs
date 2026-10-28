#![allow(clippy::redundant_clone)]
use proc_macro2::{Ident, Span};

use syn::{Item, parse_macro_input, spanned::Spanned};

use proc_macro::TokenStream;
use quote::quote_spanned;

use darling::{Error, FromMeta, ast::NestedMeta};

use std::env;

const DEFAULT_DUCKDB_VERSION: &str = "v1.2.0";

/// For parsing the arguments to the duckdb_entrypoint_c_api macro
#[derive(Debug, FromMeta)]
struct CEntryPointMacroArgs {
    #[darling(default)]
    /// The name to be given to this extension. This name is used in the entrypoint function called by duckdb
    ext_name: Option<String>,
    /// The minimum C API version this extension requires. It is recommended to set this to the lowest possible version
    /// at which your extension still compiles
    min_duckdb_version: Option<String>,
}

/// Turns a function into the entrypoint of a `duckdb-neo` extension, written against DuckDB's v2 C API.
///
/// The function takes a `&duckdb_neo::extension::Extension` to register functionality through and returns a
/// `Result` whose error converts into `Box<dyn Error>`. An error fails the load with its message. A panic is reported
/// as an internal error instead, which invalidates the database. Arguments:
///
/// - `extension_name`: the name DuckDB knows the extension by. A loadable build must be in
///   `<extension_name>.duckdb_extension`.
/// - `api_version`: the C API version the extension needs, such as `"v2.0.0"`.
///
/// It emits `<extension_name>_init_c_api_v2`, the entrypoint DuckDB calls, and needs one of `duckdb-neo`'s features:
///
/// - `build-extension`: build a `cdylib` and load it with `LOAD`.
/// - `build-extension-static`: the macro also emits `duckdb_extension_<extension_name>_describe`; pass that to
///   `duckdb_neo::extension::register_static_extension` to link the extension into the binary.
#[cfg(feature = "neo")]
#[proc_macro_attribute]
pub fn duckdb_entrypoint_c_api_v2(meta: TokenStream, item: TokenStream) -> TokenStream {
    match neo::parse(meta.into(), parse_macro_input!(item)) {
        Ok(stream) => stream.into(),
        Err(e) => TokenStream::from(e.write_errors()),
    }
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
    let minimum_duckdb_version = match (args.min_duckdb_version, env::var("DUCKDB_EXTENSION_MIN_DUCKDB_VERSION")) {
        (Some(i), _) => i,
        (None, Ok(i)) => i.to_string(),
        _ => DEFAULT_DUCKDB_VERSION.to_string(),
    };

    let extension_name = match (args.ext_name, env::var("DUCKDB_EXTENSION_NAME")) {
        (Some(i), _) => i,
        (None, Ok(i)) => i.to_string(),
        _ => env::var("CARGO_PKG_NAME").unwrap().to_string(),
    };

    let ast = parse_macro_input!(item as syn::Item);

    match ast {
        Item::Fn(func) => {
            let c_entrypoint = Ident::new(format!("{extension_name}_init_c_api").as_str(), Span::call_site());
            let prefixed_original_function = func.sig.ident.clone();
            let c_entrypoint_internal = Ident::new(
                format!("{extension_name}_init_c_api_internal").as_str(),
                Span::call_site(),
            );

            quote_spanned! {func.span()=>
                /// # Safety
                ///
                /// Internal Entrypoint for error handling
                pub unsafe fn #c_entrypoint_internal(info: ::duckdb::ffi::duckdb_extension_info, access: *const ::duckdb::ffi::duckdb_extension_access) -> ::std::result::Result<bool, Box<dyn ::std::error::Error>> {
                    unsafe {
                        let have_api_struct = ::duckdb::ffi::duckdb_rs_extension_api_init(info, access, #minimum_duckdb_version)?;
                        if !have_api_struct {
                            // initialization failed to return an api struct, likely due to an API version mismatch, we can simply return here
                            return Ok(false);
                        }

                        let get_database = (*access)
                            .get_database
                            .ok_or("get_database function pointer is null in duckdb_extension_access")?;
                        let db_ptr = get_database(info);
                        if db_ptr.is_null() {
                            // DuckDB already has the real reason for returning a null database handle.
                            return Ok(false);
                        }
                        let db: ::duckdb::ffi::duckdb_database = *db_ptr;
                        let connection = ::duckdb::Connection::open_from_raw(db.cast())?;

                        #prefixed_original_function(connection)?;

                        Ok(true)
                    }
                }

                /// # Safety
                ///
                /// Entrypoint that will be called by DuckDB
                #[unsafe(no_mangle)]
                pub unsafe extern "C" fn #c_entrypoint(info: ::duckdb::ffi::duckdb_extension_info, access: *const ::duckdb::ffi::duckdb_extension_access) -> bool {
                    unsafe {
                        let init_result = #c_entrypoint_internal(info, access);

                        match init_result {
                            ::std::result::Result::Ok(v) => v,
                            ::std::result::Result::Err(x) => {
                                if let ::std::option::Option::Some(set_error_fn) = (*access).set_error {
                                    let error_c_string = ::std::ffi::CString::new(x.to_string());
                                    match error_c_string {
                                        Ok(e) => {
                                            set_error_fn(info, e.as_ptr());
                                        },
                                        Err(_e) => {
                                            let error_alloc_failure = c"Extension initialization failed, but the error message could not be converted to a C string";
                                            set_error_fn(info, error_alloc_failure.as_ptr());
                                        }
                                    }
                                }
                                false
                            }
                        }
                    }
                }

                #[allow(clippy::missing_safety_doc)]
                #func
            }
            .into()
        }
        _ => panic!("Only function items are allowed on duckdb_entrypoint_c_api"),
    }
}

#[cfg(feature = "neo")]
mod neo;

/// Derive `duckdb_neo::AsRaw`, exposing the struct's raw DuckDB handle.
///
/// The handle field is the one marked `#[raw]`, the only field, or the field named `handle`.
#[cfg(feature = "neo")]
#[proc_macro_derive(AsRaw, attributes(raw))]
pub fn derive_as_raw(input: TokenStream) -> TokenStream {
    neo::derive_as_raw(parse_macro_input!(input as syn::DeriveInput))
}

/// Derive `duckdb_neo::FromRaw`, wrapping a raw DuckDB handle.
///
/// Requires `AsRaw`. Every field other than the handle is set to its `Default`.
#[cfg(feature = "neo")]
#[proc_macro_derive(FromRaw, attributes(raw))]
pub fn derive_from_raw(input: TokenStream) -> TokenStream {
    neo::derive_from_raw(parse_macro_input!(input as syn::DeriveInput))
}

/// Derive `Drop` for a struct that owns a raw DuckDB handle.
///
/// The handle field is picked like `AsRaw`'s. Name the destroy call with
/// `#[drop_handle(ffi::duckdb_v2_..._destroy)]`; it receives `&mut` to the handle.
#[cfg(feature = "neo")]
#[proc_macro_derive(DropHandle, attributes(raw, drop_handle))]
pub fn derive_drop_handle(input: TokenStream) -> TokenStream {
    neo::derive_drop_handle(parse_macro_input!(input as syn::DeriveInput))
}

/// Derive `DuckDBType` for a struct with named fields.
///
/// Each field must implement `DuckDBType`; field names and order define the SQL `STRUCT` type.
/// Raw identifiers drop their `r#` prefix, and names that differ only in case are rejected.
#[cfg(feature = "neo")]
#[proc_macro_derive(AsDuckDBType)]
pub fn as_duckdb_type(input: TokenStream) -> TokenStream {
    expand_neo_derive(input, neo::as_duckdb_type)
}

/// Derive `ToValue` for a struct with named fields.
///
/// Each field must implement `ToValue`; field names and order define the SQL `STRUCT` value.
#[cfg(feature = "neo")]
#[proc_macro_derive(AsToValue)]
pub fn as_to_value(input: TokenStream) -> TokenStream {
    expand_neo_derive(input, neo::as_to_value)
}

/// Derive `FromValue` for a struct with named fields.
///
/// Fields are matched by name in any order, preferring exact over case-insensitive matches;
/// missing or extra fields are errors. `Option<T>` fields preserve SQL NULL; other fields reject it.
#[cfg(feature = "neo")]
#[proc_macro_derive(AsFromValue)]
pub fn as_from_value(input: TokenStream) -> TokenStream {
    expand_neo_derive(input, neo::as_from_value)
}

/// Derive `VectorElement` and a borrowed `NameRef<'a>` for a named struct `Name`.
///
/// Fields may be `T` or `Option<T>`, with `T: VectorElement`. The generated reference
/// has the same field visibility and nullable `Option<T::Ref<'a>>` fields. Unlike `AsFromValue`,
/// STRUCT fields are matched by position: names (case-insensitive), order, and child types must
/// match. Whole-row NULLs remain separate. `NameRef` implements `Debug`, `Clone`, `Copy`,
/// `PartialEq`, `Eq`, and `Hash` whenever every field's borrowed form does.
#[cfg(feature = "neo")]
#[proc_macro_derive(AsVectorElement)]
pub fn as_vector_element(input: TokenStream) -> TokenStream {
    expand_neo_derive(input, neo::as_vector_element)
}

/// Derive `WritableVectorElement` and `OwnedVectorElement` for a named struct.
///
/// `set` takes the struct by value and `set_ref` borrows it; both write each field by reference.
/// Requires `VectorElement` and `T` or `Option<T>` fields with `T: OwnedVectorElement`.
/// Plain fields write non-NULL values; optional fields preserve NULL. Passing `None` as the
/// entire input nulls every child without requiring `Default` or a separate write struct.
/// A custom unsafe `VectorElement` implementation must validate the complete named field layout;
/// derive `AsVectorElement` to generate that validation. Writes do not revalidate the schema.
#[cfg(feature = "neo")]
#[proc_macro_derive(AsWritableVectorElement)]
pub fn as_writable_vector_element(input: TokenStream) -> TokenStream {
    expand_neo_derive(input, neo::as_writable_vector_element)
}

/// Derive `DuckDBType`, `ToValue`, `FromValue`, and the vector element traits together.
///
/// Requires a named struct whose fields support the corresponding value and vector traits.
/// Generates `NameRef<'a>` for borrowed reads; writes take `Name` or `&Name`. Plain and
/// optional fields, owned strings, `List<T>` fields, and nested structs are supported.
#[cfg(feature = "neo")]
#[proc_macro_derive(AsDuckDB)]
pub fn as_duckdb(input: TokenStream) -> TokenStream {
    expand_neo_derive(input, neo::as_duckdb)
}

#[cfg(feature = "neo")]
fn expand_neo_derive(
    input: TokenStream,
    expand: fn(&syn::DeriveInput) -> syn::Result<proc_macro2::TokenStream>,
) -> TokenStream {
    let input = parse_macro_input!(input as syn::DeriveInput);
    expand(&input).unwrap_or_else(syn::Error::into_compile_error).into()
}

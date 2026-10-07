use crate::{CApi, HeaderLocation, is_loadable_extension};

use std::{fs::OpenOptions, io::Write, path::Path};

#[cfg(feature = "loadable-extension")]
fn extract_method(ty: &syn::Type) -> Option<&syn::TypeBareFn> {
    let syn::Type::Path(type_path) = ty else {
        return None;
    };
    let segment = type_path.path.segments.last()?;
    let syn::PathArguments::AngleBracketed(args) = &segment.arguments else {
        return None;
    };
    let Some(syn::GenericArgument::Type(ty)) = args.args.first() else {
        return None;
    };
    let syn::Type::BareFn(method) = ty else {
        return None;
    };
    Some(method)
}

#[cfg(feature = "loadable-extension")]
fn generate_functions(mut output: String) -> String {
    // (1) parse sqlite3_api_routines fields from bindgen output
    let ast: syn::File = syn::parse_str(&output).expect("could not parse bindgen output");
    let duckdb_ext_api_v1: syn::ItemStruct = ast
        .items
        .into_iter()
        .find_map(|i| {
            if let syn::Item::Struct(s) = i {
                if s.ident == "duckdb_ext_api_v1" { Some(s) } else { None }
            } else {
                None
            }
        })
        .expect("could not find duckdb_ext_api_v1");

    let p_api = quote::format_ident!("p_api");
    let mut stores = Vec::new();

    for field in duckdb_ext_api_v1.fields {
        let ident = field.ident.expect("unnamed field");
        let span = ident.span();
        let function_name = ident.to_string();
        let ptr_name = syn::Ident::new(format!("__{}", function_name.to_uppercase()).as_ref(), span);

        // Create syntax name
        let duckdb_fn_name = syn::Ident::new(&function_name, span);

        let method = extract_method(&field.ty).unwrap_or_else(|| panic!("unexpected type for {function_name}"));

        let arg_names: syn::punctuated::Punctuated<&syn::Ident, syn::token::Comma> =
            method.inputs.iter().map(|i| &i.name.as_ref().unwrap().0).collect();

        let args = &method.inputs;

        let ty = &method.output;

        let tokens = quote::quote! {
            static #ptr_name: ::std::sync::atomic::AtomicPtr<()> = ::std::sync::atomic::AtomicPtr::new(::std::ptr::null_mut());
            pub unsafe fn #duckdb_fn_name(#args) #ty {
                let function_ptr = #ptr_name.load(::std::sync::atomic::Ordering::Acquire);
                assert!(!function_ptr.is_null(), "DuckDB API not initialized or DuckDB feature omitted");
                let fun: unsafe extern "C" fn(#args) #ty = ::std::mem::transmute(function_ptr);
                (fun)(#arg_names)
            }
        };

        output.push_str(&prettyplease::unparse(
            &syn::parse2(tokens).expect("could not parse quote output"),
        ));

        output.push('\n');

        stores.push(quote::quote! {
            if let Some(fun) = (*#p_api).#ident {
                #ptr_name.store(
                    fun as usize as *mut (),
                    ::std::sync::atomic::Ordering::Release,
                );
            }
        });
    }

    // (3) generate rust code similar to DUCKDB_EXTENSION_API_INIT macro
    let tokens = quote::quote! {
        /// Like DUCKDB_EXTENSION_API_INIT macro
        pub unsafe fn duckdb_rs_extension_api_init(info: duckdb_extension_info, access: *const duckdb_extension_access, version: &str) -> ::std::result::Result<bool, &'static str> {
            let version_c_string = std::ffi::CString::new(version).unwrap();
            let #p_api = (*access).get_api.unwrap()(info, version_c_string.as_ptr()) as *const duckdb_ext_api_v1;
            if #p_api.is_null() {
                // get_api can return a nullptr when the version is not matched. In this case, we don't need to set
                // an error, but can instead just stop the initialization process and let duckdb handle things
                return Ok(false);
            }
            #(#stores)*
            Ok(true)
        }
    };
    output.push_str(&prettyplease::unparse(
        &syn::parse2(tokens).expect("could not parse quote output"),
    ));
    output.push('\n');
    output
}

pub fn write_to_out_dir(header: &HeaderLocation, api: CApi, out_path: &Path) {
    let header = header.header_path(api).to_string_lossy().into_owned();
    let mut output = Vec::new();

    // ONLY generate bindings for symbols containing "duckdb" in their name
    // and for the type `idx_t` (each pass emits its own `u64` alias; type
    // aliases are interchangeable, and blocklisting it would cost bindgen
    // the `Copy` derives on structs that embed it). Use the concrete Arrow
    // ABI layouts from src/arrow_c_data.rs for both headers.
    let mut builder = bindgen::builder()
        .trust_clang_mangling(false)
        .header(header.clone())
        .allowlist_item(r#"(\w*duckdb\w*)"#)
        .allowlist_type("idx_t")
        .blocklist_type("ArrowArray")
        .blocklist_type("ArrowSchema")
        .blocklist_type("ArrowArrayStream")
        .layout_tests(false) // causes problems on WASM builds
        .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()));

    builder = match api {
        CApi::V1 => {
            if is_loadable_extension() {
                builder = builder.ignore_functions();
            }
            // We have to pass DDUCKDB_EXTENSION_API_VERSION_UNSTABLE for now,
            // until we figure out how to feature gate the generated API
            builder.clang_arg("-DDUCKDB_EXTENSION_API_VERSION_UNSTABLE")
        }
        CApi::V2 => builder
            // The v2 header spells its enums and macro constants in upper
            // case, which the case-sensitive allowlist above would drop.
            .allowlist_item(r#"DUCKDB_V2_\w*"#)
            // The v2 wrapper matches on real Rust enums rather than
            // constified integer values.
            .rustified_non_exhaustive_enum(r#"DUCKDB_V2_\w*"#),
    };

    builder
        .generate()
        .unwrap_or_else(|_| panic!("could not run bindgen on header {header}"))
        .write(Box::new(&mut output))
        .expect("could not write output of bindgen");

    let output = String::from_utf8(output).expect("bindgen output was not UTF-8?!");

    #[cfg(feature = "loadable-extension")]
    let output = if api == CApi::V1 {
        generate_functions(output)
    } else {
        output
    };

    let mut file = OpenOptions::new()
        .write(true)
        .truncate(true)
        .create(true)
        .open(out_path)
        .unwrap_or_else(|_| panic!("Could not write to {out_path:?}"));

    file.write_all(output.as_bytes())
        .unwrap_or_else(|_| panic!("Could not write to {out_path:?}"));
}

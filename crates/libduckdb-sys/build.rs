use std::{env, path::Path};

/// Tells whether we're building for Windows. This is more suitable than a plain
/// `cfg!(windows)`, since the latter does not properly handle cross-compilation
///
/// Note that there is no way to know at compile-time which system we'll be
/// targeting, and this test must be made at run-time (of the build script) See
/// https://doc.rust-lang.org/cargo/reference/environment-variables.html#environment-variables-cargo-sets-for-build-scripts
#[allow(dead_code)]
fn win_target() -> bool {
    std::env::var("CARGO_CFG_WINDOWS").is_ok()
}

/// Tells whether a given compiler will be used `compiler_name` is compared to
/// the content of `CARGO_CFG_TARGET_ENV` (and is always lowercase)
///
/// See [`win_target`]
#[allow(dead_code)]
fn is_compiler(compiler_name: &str) -> bool {
    std::env::var("CARGO_CFG_TARGET_ENV").map_or(false, |v| v == compiler_name)
}

fn main() {
    let out_dir = env::var("OUT_DIR").unwrap();
    let out_path = Path::new(&out_dir).join("bindgen.rs");
    #[cfg(feature = "bundled")]
    {
        build_bundled::main(&out_dir, &out_path);
    }
    #[cfg(not(feature = "bundled"))]
    {
        build_linked::main(&out_dir, &out_path)
    }
}

#[cfg(feature = "bundled")]
mod build_bundled {
    use std::{
        collections::{HashMap, HashSet},
        path::Path,
    };

    use crate::win_target;

    #[derive(serde::Deserialize)]
    struct Sources {
        cpp_files: HashSet<String>,
        include_dirs: HashSet<String>,
    }

    #[derive(serde::Deserialize)]
    struct Manifest {
        base: Sources,

        #[allow(unused)]
        extensions: HashMap<String, Sources>,
    }

    #[allow(unused)]
    fn add_extension(
        cfg: &mut cc::Build,
        manifest: &Manifest,
        extension: &str,
        cpp_files: &mut HashSet<String>,
        include_dirs: &mut HashSet<String>,
    ) {
        cpp_files.extend(manifest.extensions.get(extension).unwrap().cpp_files.clone());
        include_dirs.extend(manifest.extensions.get(extension).unwrap().include_dirs.clone());
        cfg.define(
            &format!("DUCKDB_EXTENSION_{}_LINKED", extension.to_uppercase()),
            Some("1"),
        );
    }

    fn untar_archive(out_dir: &str) {
        let path = "duckdb.tar.gz";

        let tar_gz = std::fs::File::open(path).expect("archive file");
        let tar = flate2::read::GzDecoder::new(tar_gz);
        let mut archive = tar::Archive::new(tar);
        archive.unpack(out_dir).expect("archive");
    }

    pub fn main(out_dir: &str, out_path: &Path) {
        let lib_name = super::lib_name();

        untar_archive(out_dir);

        if !cfg!(feature = "bundled") {
            // This is just a sanity check, the top level `main` should ensure this.
            panic!("This module should not be used: bundled feature has not been enabled");
        }

        #[cfg(feature = "buildtime_bindgen")]
        {
            use super::{bindings, HeaderLocation};
            let header = HeaderLocation::FromPath(format!("{out_dir}/{lib_name}/src/include/"));
            bindings::write_to_out_dir(header, out_path);
        }

        #[cfg(not(feature = "buildtime_bindgen"))]
        {
            use std::fs;
            fs::copy(
                #[cfg(not(feature = "loadable-extension"))]
                "src/bindgen_bundled_version.rs",
                #[cfg(feature = "loadable-extension")]
                "src/bindgen_bundled_version_loadable.rs",
                out_path,
            )
            .expect("Could not copy bindings to output directory");
        }

        let manifest_file = std::fs::File::open(format!("{out_dir}/{lib_name}/manifest.json")).expect("manifest file");
        let manifest: Manifest = serde_json::from_reader(manifest_file).expect("reading manifest file");

        let mut cpp_files = HashSet::new();
        let mut include_dirs = HashSet::new();

        cpp_files.extend(manifest.base.cpp_files.clone());
        // otherwise clippy will remove the clone here...
        // https://github.com/rust-lang/rust-clippy/issues/9011
        #[allow(clippy::all)]
        include_dirs.extend(manifest.base.include_dirs.clone());

        let mut cfg = cc::Build::new();

        #[cfg(feature = "parquet")]
        add_extension(&mut cfg, &manifest, "parquet", &mut cpp_files, &mut include_dirs);

        #[cfg(feature = "json")]
        add_extension(&mut cfg, &manifest, "json", &mut cpp_files, &mut include_dirs);

        // duckdb/tools/pythonpkg/setup.py
        cfg.define("DUCKDB_EXTENSION_AUTOINSTALL_DEFAULT", "1");
        cfg.define("DUCKDB_EXTENSION_AUTOLOAD_DEFAULT", "1");

        // Since the manifest controls the set of files, we require it to be changed to know whether
        // to rebuild the project
        println!("cargo:rerun-if-changed={out_dir}/{lib_name}/manifest.json");
        // Make sure to rebuild the project if tar file changed
        println!("cargo:rerun-if-changed=duckdb.tar.gz");

        cfg.include(lib_name);
        cfg.includes(include_dirs.iter().map(|dir| format!("{out_dir}/{lib_name}/{dir}")));

        for f in cpp_files.into_iter().map(|file| format!("{out_dir}/{file}")) {
            cfg.file(f);
        }

        cfg.cpp(true)
            .flag_if_supported("-std=c++11")
            .flag_if_supported("-stdlib=libc++")
            .flag_if_supported("-stdlib=libstdc++")
            .flag_if_supported("/bigobj")
            .warnings(false)
            .flag_if_supported("-w");

        if win_target() {
            cfg.define("DUCKDB_BUILD_LIBRARY", None);
        }
        cfg.compile(lib_name);

        println!("cargo:lib_dir={out_dir}");
    }
}

fn env_prefix() -> &'static str {
    "DUCKDB"
}

fn lib_name() -> &'static str {
    "duckdb"
}

pub enum HeaderLocation {
    FromEnvironment,
    Wrapper,
    FromPath(String),
}

impl From<HeaderLocation> for String {
    fn from(header: HeaderLocation) -> String {
        match header {
            HeaderLocation::FromEnvironment => {
                let prefix = env_prefix();
                let mut header = env::var(format!("{prefix}_INCLUDE_DIR"))
                    .unwrap_or_else(|_| env::var(format!("{}_LIB_DIR", env_prefix())).unwrap());
                header.push_str(if cfg!(feature = "loadable-extension") {
                    "/duckdb_extension.h"
                } else {
                    "/duckdb.h"
                });
                header
            }
            HeaderLocation::Wrapper => {
                if cfg!(feature = "loadable-extension") {
                    "wrapper_ext.h".into()
                } else {
                    "wrapper.h".into()
                }
            }
            HeaderLocation::FromPath(path) => format!(
                "{}/{}",
                path,
                if cfg!(feature = "loadable-extension") {
                    "duckdb_extension.h"
                } else {
                    "duckdb.h"
                }
            ),
        }
    }
}

#[cfg(not(feature = "bundled"))]
mod build_linked {
    #[cfg(feature = "vcpkg")]
    extern crate vcpkg;

    #[cfg(feature = "buildtime_bindgen")]
    use super::bindings;

    use super::{env_prefix, is_compiler, lib_name, win_target, HeaderLocation};
    use std::{env, path::Path};

    pub fn main(_out_dir: &str, out_path: &Path) {
        // We need this to config the LD_LIBRARY_PATH
        #[allow(unused_variables)]
        let header = find_duckdb();

        #[cfg(not(feature = "buildtime_bindgen"))]
        {
            std::fs::copy(
                #[cfg(not(feature = "loadable-extension"))]
                "src/bindgen_bundled_version.rs",
                #[cfg(feature = "loadable-extension")]
                "src/bindgen_bundled_version_loadable.rs",
                out_path,
            )
            .expect("Could not copy bindings to output directory");
        }

        #[cfg(feature = "buildtime_bindgen")]
        {
            bindings::write_to_out_dir(header, out_path);
        }
    }

    fn find_link_mode() -> &'static str {
        // If the user specifies DUCKDB_STATIC, do static
        // linking, unless it's explicitly set to 0.
        match &env::var(format!("{}_STATIC", env_prefix())) {
            Ok(v) if v != "0" => "static",
            _ => "dylib",
        }
    }
    // Prints the necessary cargo link commands and returns the path to the header.
    fn find_duckdb() -> HeaderLocation {
        let link_lib = lib_name();

        if !cfg!(feature = "loadable-extension") {
            println!("cargo:rerun-if-env-changed={}_INCLUDE_DIR", env_prefix());
            println!("cargo:rerun-if-env-changed={}_LIB_DIR", env_prefix());
            println!("cargo:rerun-if-env-changed={}_STATIC", env_prefix());
            if cfg!(feature = "vcpkg") && is_compiler("msvc") {
                println!("cargo:rerun-if-env-changed=VCPKGRS_DYNAMIC");
            }

            // dependents can access `DEP_DUCKDB_LINK_TARGET` (`duckdb` being the
            // `links=` value in our Cargo.toml) to get this value. This might be
            // useful if you need to ensure whatever crypto library sqlcipher relies
            // on is available, for example.
            println!("cargo:link-target={link_lib}");
        }

        if win_target() && cfg!(feature = "winduckdb") {
            if !cfg!(feature = "loadable-extension") {
                println!("cargo:rustc-link-lib=dylib={link_lib}");
            }
            return HeaderLocation::Wrapper;
        }
        // Allow users to specify where to find DuckDB.
        if let Ok(dir) = env::var(format!("{}_LIB_DIR", env_prefix())) {
            println!("cargo:rustc-env=LD_LIBRARY_PATH={dir}");
            // Try to use pkg-config to determine link commands
            let pkgconfig_path = Path::new(&dir).join("pkgconfig");
            env::set_var("PKG_CONFIG_PATH", pkgconfig_path);
            if pkg_config::Config::new().probe(link_lib).is_err() {
                // Otherwise just emit the bare minimum link commands.
                println!("cargo:rustc-link-lib={}={}", find_link_mode(), link_lib);
                println!("cargo:rustc-link-search={dir}");
            }
            return HeaderLocation::FromEnvironment;
        }

        if let Some(header) = try_vcpkg() {
            return header;
        }

        // See if pkg-config can do everything for us.
        match pkg_config::Config::new().print_system_libs(false).probe(link_lib) {
            Ok(mut lib) => {
                if let Some(header) = lib.include_paths.pop() {
                    HeaderLocation::FromPath(header.to_string_lossy().into())
                } else {
                    HeaderLocation::Wrapper
                }
            }
            Err(_) => {
                // No env var set and pkg-config couldn't help; just output the link-lib
                // request and hope that the library exists on the system paths. We used to
                // output /usr/lib explicitly, but that can introduce other linking problems;
                // see https://github.com/rusqlite/rusqlite/issues/207.
                if !cfg!(feature = "loadable-extension") {
                    println!("cargo:rustc-link-lib={}={}", find_link_mode(), link_lib);
                }
                HeaderLocation::Wrapper
            }
        }
    }

    fn try_vcpkg() -> Option<HeaderLocation> {
        if cfg!(feature = "vcpkg") && is_compiler("msvc") {
            // See if vcpkg can find it.
            if let Ok(mut lib) = vcpkg::Config::new().probe(lib_name()) {
                if let Some(header) = lib.include_paths.pop() {
                    return Some(HeaderLocation::FromPath(header.to_string_lossy().into()));
                }
            }
            None
        } else {
            None
        }
    }
}

#[cfg(feature = "buildtime_bindgen")]
mod bindings {
    use super::HeaderLocation;

    use std::{fs::OpenOptions, io::Write, path::Path};

    #[cfg(feature = "loadable-extension")]
    fn extract_method(ty: &syn::Type) -> Option<&syn::TypeBareFn> {
        match ty {
            syn::Type::Path(tp) => tp.path.segments.last(),
            _ => None,
        }
        .map(|seg| match &seg.arguments {
            syn::PathArguments::AngleBracketed(args) => args.args.first(),
            _ => None,
        })?
        .map(|arg| match arg {
            syn::GenericArgument::Type(t) => Some(t),
            _ => None,
        })?
        .map(|ty| match ty {
            syn::Type::BareFn(r) => Some(r),
            _ => None,
        })?
    }

    #[cfg(feature = "loadable-extension")]
    fn generate_functions(output: &mut String) {
        // (1) parse sqlite3_api_routines fields from bindgen output
        let ast: syn::File = syn::parse_str(output).expect("could not parse bindgen output");
        let duckdb_ext_api_v0: syn::ItemStruct = ast
            .items
            .into_iter()
            .find_map(|i| {
                if let syn::Item::Struct(s) = i {
                    if s.ident == "duckdb_ext_api_v0" {
                        Some(s)
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .expect("could not find duckdb_ext_api_v0");

        let p_api = quote::format_ident!("p_api");
        let mut stores = Vec::new();

        for field in duckdb_ext_api_v0.fields {
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
                let #p_api = (*access).get_api.unwrap()(info, version_c_string.as_ptr()) as *const duckdb_ext_api_v0;
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
    }

    pub fn write_to_out_dir(header: HeaderLocation, out_path: &Path) {
        let header: String = header.into();
        let mut output = Vec::new();
        let mut builder = bindgen::builder();

        if cfg!(feature = "loadable-extension") {
            builder = builder.ignore_functions();
        }

        builder
            .trust_clang_mangling(false)
            .header(header.clone())
            .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()))
            .generate()
            .unwrap_or_else(|_| panic!("could not run bindgen on header {header}"))
            .write(Box::new(&mut output))
            .expect("could not write output of bindgen");

        let mut output = String::from_utf8(output).expect("bindgen output was not UTF-8?!");

        #[cfg(feature = "loadable-extension")]
        {
            generate_functions(&mut output);
        }

        let mut file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(out_path)
            .unwrap_or_else(|_| panic!("Could not write to {out_path:?}"));

        file.write_all(output.as_bytes())
            .unwrap_or_else(|_| panic!("Could not write to {out_path:?}"));
    }
}

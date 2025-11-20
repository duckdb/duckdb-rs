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
    std::env::var("CARGO_CFG_TARGET_ENV").is_ok_and(|v| v == compiler_name)
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

        add_extension(&mut cfg, &manifest, "core_functions", &mut cpp_files, &mut include_dirs);

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

        // Ensure deterministic builds
        let mut cpp_files_vec: Vec<String> = cpp_files.into_iter().collect();
        cpp_files_vec.sort();
        for f in cpp_files_vec.into_iter().map(|file| format!("{out_dir}/{file}")) {
            cfg.file(f);
        }

        cfg.cpp(true)
            .flag_if_supported("-std=c++11")
            .flag_if_supported("/utf-8")
            .flag_if_supported("/bigobj")
            .warnings(false)
            .flag_if_supported("-w");

        // Define NDEBUG if not in debug mode
        let is_debug = match std::env::var("DEBUG") {
            Ok(v) => v != "false" && v != "0",
            Err(_) => false,
        };
        if !is_debug {
            cfg.define("NDEBUG", None);
        }

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
    fn from(header: HeaderLocation) -> Self {
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
    use std::{
        env, fs, io,
        path::{Path, PathBuf},
    };

    pub fn main(out_dir: &str, out_path: &Path) {
        // We need this to config the LD_LIBRARY_PATH
        #[allow(unused_variables)]
        let header = find_duckdb(out_dir);

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
    fn find_duckdb(out_dir: &str) -> HeaderLocation {
        let link_lib = lib_name();

        println!("cargo:rerun-if-env-changed={}_DOWNLOAD_LIB", env_prefix());
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

            #[cfg(feature = "pkg-config")]
            let lib_found = pkg_config::Config::new().probe(link_lib).is_ok();
            #[cfg(not(feature = "pkg-config"))]
            let lib_found = false;

            if !lib_found {
                // Otherwise just emit the bare minimum link commands.
                println!("cargo:rustc-link-lib={}={}", find_link_mode(), link_lib);
                println!("cargo:rustc-link-search={dir}");
            }

            return HeaderLocation::FromEnvironment;
        }

        if should_download_libduckdb() {
            return download_libduckdb(out_dir).unwrap_or_else(|err| panic!("Failed to download libduckdb: {err}"));
        }

        if let Some(header) = try_vcpkg() {
            return header;
        }

        // See if pkg-config can do everything for us.
        #[cfg(feature = "pkg-config")]
        {
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
        #[cfg(not(feature = "pkg-config"))]
        {
            // No pkg-config available; just output the link-lib request and hope
            // that the library exists on the system paths.
            if !cfg!(feature = "loadable-extension") {
                println!("cargo:rustc-link-lib={}={}", find_link_mode(), link_lib);
            }
            HeaderLocation::Wrapper
        }
    }

    fn try_vcpkg() -> Option<HeaderLocation> {
        #[cfg(feature = "vcpkg")]
        if is_compiler("msvc") {
            // See if vcpkg can find it.
            if let Ok(mut lib) = vcpkg::Config::new().probe(lib_name()) {
                if let Some(header) = lib.include_paths.pop() {
                    return Some(HeaderLocation::FromPath(header.to_string_lossy().into()));
                }
            }
        }
        None
    }

    fn should_download_libduckdb() -> bool {
        env::var(format!("{}_DOWNLOAD_LIB", env_prefix()))
            .map(|value| matches!(value.to_ascii_lowercase().as_str(), "1" | "true"))
            .unwrap_or(false)
    }

    fn download_libduckdb(out_dir: &str) -> Result<HeaderLocation, Box<dyn std::error::Error>> {
        let target = env::var("TARGET")?;
        let archive = LibduckdbArchive::for_target(&target)
            .ok_or_else(|| format!("No pre-built libduckdb available for target '{target}'"))?;

        let version = env!("CARGO_PKG_VERSION").to_string();

        // Cache downloads in target/duckdb-download/<target>/<version> so successive builds reuse them
        let download_dir = workspace_download_dir(out_dir)?.join(&target).join(&version);
        fs::create_dir_all(&download_dir)?;

        let archive_path = download_dir.join(archive.archive_name);
        let lib_marker = download_dir.join(archive.dynamic_lib);

        if lib_marker.exists() {
            println!("cargo:warning=Reusing libduckdb from {}", download_dir.display());
        } else {
            let client = http_client()?;
            let url = archive.download_url(&version);
            ensure_libduckdb(&client, &url, &archive_path)?;
            extract_libduckdb(&archive_path, &download_dir)?;
            if !lib_marker.exists() {
                return Err(format!(
                    "Downloaded archive did not contain expected library '{}'",
                    archive.dynamic_lib
                )
                .into());
            }
        }

        configure_link_search(&download_dir);

        copy_libduckdb(&download_dir, archive.dynamic_lib, out_dir)?;

        Ok(HeaderLocation::FromPath(download_dir.to_string_lossy().into_owned()))
    }

    fn configure_link_search(lib_dir: &Path) {
        println!("cargo:rustc-link-search=native={}", lib_dir.display());
        if !cfg!(feature = "loadable-extension") {
            println!("cargo:rustc-link-lib={}={}", find_link_mode(), lib_name());
        }
        if !win_target() {
            println!("cargo:rustc-link-arg=-Wl,-rpath,{}", lib_dir.display());
        }
    }

    // Ensures the libduckdb archive exists: reuses an existing zip or
    // downloads it into a temp file and atomically renames it into place.
    fn ensure_libduckdb(
        client: &reqwest::blocking::Client,
        url: &str,
        archive_path: &Path,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if archive_path.exists() {
            println!("cargo:warning=libduckdb already present at {}", archive_path.display());
            return Ok(());
        }
        let tmp_path = archive_path.with_extension("download");
        if let Some(parent) = archive_path.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut response = client.get(url).send()?.error_for_status()?;
        let mut tmp_file = fs::File::create(&tmp_path)?;
        io::copy(&mut response, &mut tmp_file)?;
        fs::rename(&tmp_path, archive_path)?;
        println!("cargo:warning=Downloaded libduckdb from {url}");
        Ok(())
    }

    fn extract_libduckdb(archive_path: &Path, destination: &Path) -> Result<(), Box<dyn std::error::Error>> {
        let file = fs::File::open(archive_path)?;
        let mut archive = zip::ZipArchive::new(file)?;
        archive.extract(destination)?;
        println!("cargo:warning=Extracted libduckdb to {}", destination.display());
        Ok(())
    }

    // Copy libduckdb into target/<profile>/deps so executables/tests can load it via
    // the default Cargo rpath, just like when DUCKDB_LIB_DIR is set.
    fn copy_libduckdb(
        download_dir: &Path,
        lib_filename: &str,
        out_dir: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let Some(deps_dir) = profile_deps_dir(out_dir) else {
            println!("cargo:warning=Could not determine target/deps directory, skipping runtime copy");
            return Ok(());
        };
        fs::create_dir_all(&deps_dir)?;
        let source = download_dir.join(lib_filename);
        let dest = deps_dir.join(lib_filename);
        if dest.exists() {
            fs::remove_file(&dest)?;
        }
        fs::copy(&source, &dest)?;
        println!("cargo:warning=Copied libduckdb to {}", dest.display());
        Ok(())
    }

    // Finds target/ so downloads survive rebuilds. Respects CARGO_TARGET_DIR if set.
    fn workspace_download_dir(out_dir: &str) -> Result<PathBuf, Box<dyn std::error::Error>> {
        if let Ok(dir) = env::var("CARGO_TARGET_DIR") {
            return Ok(PathBuf::from(dir).join("duckdb-download"));
        }
        let target_root = Path::new(out_dir)
            .ancestors()
            .find(|ancestor| {
                ancestor
                    .file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name == "target")
            })
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("target"));
        Ok(target_root.join("duckdb-download"))
    }

    // Mirrors Cargo's target/<profile>/deps layout (optionally with CARGO_TARGET_DIR / target triple).
    fn profile_deps_dir(out_dir: &str) -> Option<PathBuf> {
        let profile = env::var("PROFILE").unwrap_or_else(|_| "debug".to_string());
        let mut target_root = env::var("CARGO_TARGET_DIR").map(PathBuf::from).unwrap_or_else(|_| {
            Path::new(out_dir)
                .ancestors()
                .find(|ancestor| {
                    ancestor
                        .file_name()
                        .and_then(|name| name.to_str())
                        .is_some_and(|name| name == "target")
                })
                .map(Path::to_path_buf)
                .unwrap_or_else(|| PathBuf::from("target"))
        });
        if env::var("HOST").ok() != env::var("TARGET").ok() {
            if let Ok(target) = env::var("TARGET") {
                target_root.push(target);
            }
        }
        target_root.push(profile);
        target_root.push("deps");
        Some(target_root)
    }

    struct LibduckdbArchive {
        archive_name: &'static str,
        dynamic_lib: &'static str,
    }

    impl LibduckdbArchive {
        fn for_target(target: &str) -> Option<Self> {
            match target {
                t if t.ends_with("apple-darwin") => Some(Self {
                    archive_name: "libduckdb-osx-universal.zip",
                    dynamic_lib: "libduckdb.dylib",
                }),
                "x86_64-unknown-linux-gnu" => Some(Self {
                    archive_name: "libduckdb-linux-amd64.zip",
                    dynamic_lib: "libduckdb.so",
                }),
                "aarch64-unknown-linux-gnu" => Some(Self {
                    archive_name: "libduckdb-linux-arm64.zip",
                    dynamic_lib: "libduckdb.so",
                }),
                "x86_64-pc-windows-msvc" => Some(Self {
                    archive_name: "libduckdb-windows-amd64.zip",
                    dynamic_lib: "duckdb.dll",
                }),
                "aarch64-pc-windows-msvc" => Some(Self {
                    archive_name: "libduckdb-windows-arm64.zip",
                    dynamic_lib: "duckdb.dll",
                }),
                _ => None,
            }
        }

        fn download_url(&self, version: &str) -> String {
            format!(
                "https://github.com/duckdb/duckdb/releases/download/v{version}/{}",
                self.archive_name
            )
        }
    }

    fn http_client() -> Result<reqwest::blocking::Client, reqwest::Error> {
        let timeout = env::var("CARGO_HTTP_TIMEOUT")
            .or_else(|_| env::var("HTTP_TIMEOUT"))
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(90);
        reqwest::blocking::Client::builder()
            .timeout(std::time::Duration::from_secs(timeout))
            .build()
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
        let duckdb_ext_api_v1: syn::ItemStruct = ast
            .items
            .into_iter()
            .find_map(|i| {
                if let syn::Item::Struct(s) = i {
                    if s.ident == "duckdb_ext_api_v1" {
                        Some(s)
                    } else {
                        None
                    }
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
    }

    pub fn write_to_out_dir(header: HeaderLocation, out_path: &Path) {
        let header: String = header.into();
        let mut output = Vec::new();
        let mut builder = bindgen::builder();

        if cfg!(feature = "loadable-extension") {
            builder = builder.ignore_functions();
        }

        // ONLY generate bindings for symbols containing "duckdb" in their name
        // and for the type `idx_t`
        // We have to pass DDUCKDB_EXTENSION_API_VERSION_UNSTABLE for now,
        // until we figure out how to feature gate the generated API
        builder
            .trust_clang_mangling(false)
            .header(header.clone())
            .allowlist_item(r#"(\w*duckdb\w*)"#)
            .allowlist_type("idx_t")
            .layout_tests(false) // causes problems on WASM builds
            .clang_arg("-DDUCKDB_EXTENSION_API_VERSION_UNSTABLE")
            .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()))
            .generate()
            .unwrap_or_else(|_| panic!("could not run bindgen on header {header}"))
            .write(Box::new(&mut output))
            .expect("could not write output of bindgen");

        #[allow(unused_mut)]
        let mut output = String::from_utf8(output).expect("bindgen output was not UTF-8?!");

        #[cfg(feature = "loadable-extension")]
        generate_functions(&mut output);

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

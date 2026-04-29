use crate::{win_target, write_bindings};
use std::{
    collections::{HashMap, HashSet},
    path::Path,
};

#[derive(serde::Deserialize)]
struct Sources {
    cpp_files: HashSet<String>,
    include_dirs: HashSet<String>,
}

#[derive(serde::Deserialize)]
struct Manifest {
    base: Sources,
    extensions: HashMap<String, Sources>,
}

fn add_extension(
    manifest: &Manifest,
    extension: &str,
    cpp_files: &mut HashSet<String>,
    include_dirs: &mut HashSet<String>,
) {
    let sources = manifest.extensions.get(extension).unwrap_or_else(|| {
        let mut available = manifest.extensions.keys().cloned().collect::<Vec<_>>();
        available.sort();
        panic!(
            "extension `{extension}` is missing from duckdb manifest; available extensions: {}",
            available.join(", ")
        );
    });
    cpp_files.extend(sources.cpp_files.clone());
    include_dirs.extend(sources.include_dirs.clone());
}

fn extension_enabled(extension: &str) -> bool {
    extension == "core_functions"
        || (extension == "parquet" && cfg!(feature = "parquet"))
        || (extension == "json" && cfg!(feature = "json"))
}

fn untar_archive(out_dir: &str) {
    let path = "duckdb.tar.gz";

    let tar_gz = std::fs::File::open(path).expect("archive file");
    let tar = flate2::read::GzDecoder::new(tar_gz);
    let mut archive = tar::Archive::new(tar);
    archive.unpack(out_dir).expect("archive");
}

pub fn main(out_dir: &str, out_path: &Path) {
    untar_archive(out_dir);

    write_bindings(&Path::new(out_dir).join("duckdb/src/include"), out_path);

    let manifest_file = std::fs::File::open(format!("{out_dir}/duckdb/manifest.json")).expect("manifest file");
    let manifest: Manifest = serde_json::from_reader(manifest_file).expect("reading manifest file");

    let mut cpp_files = HashSet::new();
    let mut include_dirs = HashSet::new();
    let mut extensions = manifest
        .extensions
        .keys()
        .filter(|name| extension_enabled(name))
        .cloned()
        .collect::<Vec<String>>();
    extensions.sort();

    cpp_files.extend(manifest.base.cpp_files.clone());
    include_dirs.extend(manifest.base.include_dirs.iter().cloned());

    let mut cfg = cc::Build::new();

    for extension in &extensions {
        add_extension(&manifest, extension, &mut cpp_files, &mut include_dirs);
        cfg.define(
            &format!("DUCKDB_EXTENSION_{}_LINKED", extension.to_uppercase()),
            Some("1"),
        );
    }
    cfg.define("DUCKDB_EXTENSION_AUTOINSTALL_DEFAULT", "1");
    cfg.define("DUCKDB_EXTENSION_AUTOLOAD_DEFAULT", "1");

    println!("cargo:rerun-if-changed=duckdb.tar.gz");

    cfg.include("duckdb");
    cfg.includes(include_dirs.iter().map(|dir| format!("{out_dir}/duckdb/{dir}")));

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
    cfg.compile("duckdb");

    println!("cargo:lib_dir={out_dir}");
}

use std::path::PathBuf;

pub fn get_openssl(_target: &str) -> (Vec<PathBuf>, PathBuf) {
    let artifacts = openssl_src::Build::new().build();
    println!("cargo:openssl_vendored=1");
    println!("cargo:root={}", artifacts.lib_dir().parent().unwrap().display());

    (
        vec![artifacts.lib_dir().to_path_buf()],
        artifacts.include_dir().to_path_buf(),
    )
}

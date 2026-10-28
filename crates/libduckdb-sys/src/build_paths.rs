//! Derives reusable paths from Cargo's `OUT_DIR` layout.
//!
//! Cargo uses `<build-dir>/[<triple>/]<profile>/build/<pkg>-<hash>/out`, or
//! `.../build/<pkg>/<hash>/out` with the newer build-dir layout. This is an
//! implementation detail, so these helpers return `None` for unrecognized
//! layouts. Cache roots are one level above the profile directory so
//! debug and release builds share a cache.

use std::path::{Path, PathBuf};

fn profile_dir(out_dir: &Path) -> Option<&Path> {
    if out_dir.file_name()? != "out" {
        return None;
    }

    let unit_dir = out_dir.parent()?;
    let mut build_dir = unit_dir.parent()?;
    // The newer layout nests the hash below the package name: `build/<pkg>/<hash>/out`.
    if build_dir.file_name()? != "build" && is_unit_hash(unit_dir.file_name()?.to_str()?) {
        build_dir = build_dir.parent()?;
    }
    if build_dir.file_name()? != "build" {
        return None;
    }

    build_dir.parent()
}

fn is_unit_hash(name: &str) -> bool {
    name.len() == 16 && name.bytes().all(|b| b.is_ascii_hexdigit())
}

#[cfg_attr(any(feature = "bundled", not(feature = "download-lib")), allow(dead_code))]
pub(crate) fn download_root(out_dir: &Path) -> Option<PathBuf> {
    Some(profile_dir(out_dir)?.parent()?.join("duckdb-download"))
}

#[cfg_attr(not(all(feature = "bundled", not(feature = "bundled-cmake"))), allow(dead_code))]
pub(crate) fn sources_root(out_dir: &Path) -> Option<PathBuf> {
    Some(profile_dir(out_dir)?.parent()?.join("duckdb-sources"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sources_follow_cross_target_and_separate_build_dirs() {
        for root in ["/workspace/build-dir", "/workspace/target/x86_64-unknown-linux-gnu"] {
            let out_dir = Path::new(root).join("debug/build/libduckdb-sys-1234567890abcdef/out");
            assert_eq!(sources_root(&out_dir), Some(Path::new(root).join("duckdb-sources")));
        }
    }

    #[test]
    fn resolves_paths_from_default_out_dir() {
        let out_dir = Path::new("/workspace/target/debug/build/libduckdb-sys-1234567890abcdef/out");

        assert_eq!(
            download_root(out_dir),
            Some(PathBuf::from("/workspace/target/duckdb-download")),
        );
    }

    #[test]
    fn resolves_paths_from_triple_named_target_dir() {
        let out_dir = Path::new("/workspace/aarch64-apple-darwin/debug/build/libduckdb-sys-1234567890abcdef/out");

        assert_eq!(
            download_root(out_dir),
            Some(PathBuf::from("/workspace/aarch64-apple-darwin/duckdb-download")),
        );
    }

    #[test]
    fn resolves_paths_from_cross_target_out_dir() {
        let out_dir = Path::new(
            "/workspace/custom-target/x86_64-unknown-linux-gnu/release/build/libduckdb-sys-1234567890abcdef/out",
        );

        assert_eq!(
            download_root(out_dir),
            Some(PathBuf::from(
                "/workspace/custom-target/x86_64-unknown-linux-gnu/duckdb-download"
            )),
        );
    }

    #[test]
    fn rejects_new_layout_shape_without_unit_hash() {
        let out_dir = Path::new("/workspace/build/project/generated/out");

        assert_eq!(download_root(out_dir), None);
    }

    #[test]
    fn rejects_non_cargo_out_dir_with_build_ancestor() {
        let out_dir = Path::new("/var/jenkins/build/workspace/project/generated/out");

        assert_eq!(download_root(out_dir), None);
    }

    #[test]
    fn rejects_out_dir_without_build_component() {
        let out_dir = Path::new("/workspace/generated/libduckdb-sys/out");

        assert_eq!(download_root(out_dir), None);
    }

    #[test]
    fn ignores_upstream_build_directory() {
        let out_dir = Path::new("/home/ci/build/project/target/debug/build/libduckdb-sys-1234567890abcdef/out");

        assert_eq!(
            download_root(out_dir),
            Some(PathBuf::from("/home/ci/build/project/target/duckdb-download")),
        );
    }
}

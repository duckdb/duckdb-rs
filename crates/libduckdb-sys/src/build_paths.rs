//! Derives reusable paths from Cargo's `OUT_DIR` layout.
//!
//! Cargo currently uses
//! `<target-dir>/[<triple>/]<profile>/build/<pkg>-<hash>/out`. This is an
//! implementation detail, so these helpers return `None` for unrecognized
//! layouts. The download root is one level above the profile directory so
//! debug and release builds share a cache.

use std::path::{Path, PathBuf};

fn profile_dir(out_dir: &Path) -> Option<&Path> {
    if out_dir.file_name()? != "out" {
        return None;
    }

    let build_dir = out_dir.parent()?.parent()?;
    if build_dir.file_name()? != "build" {
        return None;
    }

    build_dir.parent()
}

pub(crate) fn download_root(out_dir: &Path) -> Option<PathBuf> {
    Some(profile_dir(out_dir)?.parent()?.join("duckdb-download"))
}

pub(crate) fn profile_deps_dir(out_dir: &Path) -> Option<PathBuf> {
    Some(profile_dir(out_dir)?.join("deps"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_paths_from_default_out_dir() {
        let out_dir = Path::new("/workspace/target/debug/build/libduckdb-sys-1234567890abcdef/out");

        assert_eq!(
            download_root(out_dir),
            Some(PathBuf::from("/workspace/target/duckdb-download")),
        );
        assert_eq!(
            profile_deps_dir(out_dir),
            Some(PathBuf::from("/workspace/target/debug/deps")),
        );
    }

    #[test]
    fn resolves_paths_from_triple_named_target_dir() {
        let out_dir = Path::new("/workspace/aarch64-apple-darwin/debug/build/libduckdb-sys-1234567890abcdef/out");

        assert_eq!(
            download_root(out_dir),
            Some(PathBuf::from("/workspace/aarch64-apple-darwin/duckdb-download")),
        );
        assert_eq!(
            profile_deps_dir(out_dir),
            Some(PathBuf::from("/workspace/aarch64-apple-darwin/debug/deps")),
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
        assert_eq!(
            profile_deps_dir(out_dir),
            Some(PathBuf::from(
                "/workspace/custom-target/x86_64-unknown-linux-gnu/release/deps"
            )),
        );
    }

    #[test]
    fn rejects_non_cargo_out_dir_with_build_ancestor() {
        let out_dir = Path::new("/var/jenkins/build/workspace/project/generated/out");

        assert_eq!(download_root(out_dir), None);
        assert_eq!(profile_deps_dir(out_dir), None);
    }

    #[test]
    fn rejects_out_dir_without_build_component() {
        let out_dir = Path::new("/workspace/generated/libduckdb-sys/out");

        assert_eq!(download_root(out_dir), None);
        assert_eq!(profile_deps_dir(out_dir), None);
    }

    #[test]
    fn ignores_upstream_build_directory() {
        let out_dir = Path::new("/home/ci/build/project/target/debug/build/libduckdb-sys-1234567890abcdef/out");

        assert_eq!(
            download_root(out_dir),
            Some(PathBuf::from("/home/ci/build/project/target/duckdb-download")),
        );
        assert_eq!(
            profile_deps_dir(out_dir),
            Some(PathBuf::from("/home/ci/build/project/target/debug/deps")),
        );
    }
}

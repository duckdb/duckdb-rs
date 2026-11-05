use sha2::{Digest, Sha256};
use std::{
    fs, io,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

pub(crate) const AUTOREGISTER_WRAPPER: &str = "static_extension_autoregister_rs.cpp";
const AUTOREGISTER_SOURCE: &str =
    "#include \"static_extension_autoregister.cpp\"\nextern \"C\" int duckdb_rs_static_extension_autoregister = 1;\n";
const COMPLETE: &str = ".complete";

pub(crate) fn unpack(archive_path: &Path, out_dir: &Path) -> io::Result<PathBuf> {
    let root = crate::build_paths::sources_root(out_dir).unwrap_or_else(|| out_dir.join("duckdb-sources"));
    let bytes = fs::read(archive_path)?;
    let source_dir = root.join(cache_key(&bytes));
    if source_dir.join(COMPLETE).is_file() {
        return Ok(source_dir);
    }

    fs::create_dir_all(&root)?;
    let staging = StagingDir::new(&root)?;
    let mut archive = tar::Archive::new(flate2::read::GzDecoder::new(bytes.as_slice()));
    // Reproducible archive mtimes can fall outside Windows filesystem timestamp ranges.
    archive.set_preserve_mtime(false);
    archive.unpack(staging.path())?;
    fs::write(staging.path().join(AUTOREGISTER_WRAPPER), AUTOREGISTER_SOURCE)?;
    fs::write(staging.path().join(COMPLETE), b"")?;
    publish(staging.path(), &source_dir)?;
    Ok(source_dir)
}

fn cache_key(archive: &[u8]) -> String {
    // The wrapper is written into the tree, so it is part of the tree's identity.
    let digest = Sha256::new()
        .chain_update(archive)
        .chain_update(AUTOREGISTER_SOURCE)
        .finalize();
    // Short keeps deep source paths within Windows' MAX_PATH.
    digest[..8].iter().map(|byte| format!("{byte:02x}")).collect()
}

fn publish(staging: &Path, source_dir: &Path) -> io::Result<()> {
    // No marker: an incomplete tree, e.g. one whose files a CI cache deleted. Replace it.
    if source_dir.exists()
        && !source_dir.join(COMPLETE).is_file()
        && let Err(err) = fs::remove_dir_all(source_dir)
        && err.kind() != io::ErrorKind::NotFound
    {
        return Err(err);
    }
    // Rename on the same filesystem so readers only see complete, immutable trees.
    match fs::rename(staging, source_dir) {
        Ok(()) => Ok(()),
        Err(_) if source_dir.join(COMPLETE).is_file() => Ok(()),
        Err(err) => Err(err),
    }
}

struct StagingDir(PathBuf);

impl StagingDir {
    fn new(root: &Path) -> io::Result<Self> {
        static NEXT_ID: AtomicU64 = AtomicU64::new(0);
        loop {
            let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
            let path = root.join(format!(".duckdb-unpack-{}-{id}", std::process::id()));
            match fs::create_dir(&path) {
                Ok(()) => return Ok(Self(path)),
                Err(err) if err.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(err) => return Err(err),
            }
        }
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for StagingDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn archive() -> Vec<u8> {
        let mut builder = tar::Builder::new(flate2::write::GzEncoder::new(Vec::new(), Default::default()));
        let content = b"// header\n";
        let mut header = tar::Header::new_gnu();
        header.set_size(content.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        builder
            .append_data(&mut header, "duckdb/src/include/duckdb.h", &content[..])
            .unwrap();
        builder.into_inner().unwrap().finish().unwrap()
    }

    #[test]
    fn unpack_replaces_tree_without_marker() {
        let out_dir = tempfile::tempdir().unwrap();
        let archive_path = out_dir.path().join("duckdb.tar.gz");
        let bytes = archive();
        fs::write(&archive_path, &bytes).unwrap();
        // What a CI cache that keeps directories but deletes files leaves behind.
        let source_dir = out_dir.path().join("duckdb-sources").join(cache_key(&bytes));
        fs::create_dir_all(source_dir.join("duckdb/src/include")).unwrap();

        assert_eq!(unpack(&archive_path, out_dir.path()).unwrap(), source_dir);
        assert!(source_dir.join("duckdb/src/include/duckdb.h").is_file());
        assert!(source_dir.join(AUTOREGISTER_WRAPPER).is_file());
        assert!(source_dir.join(COMPLETE).is_file());
    }

    #[test]
    fn publish_keeps_complete_tree() {
        let root = tempfile::tempdir().unwrap();
        let source_dir = root.path().join("sources");
        fs::create_dir_all(&source_dir).unwrap();
        fs::write(source_dir.join(COMPLETE), b"").unwrap();
        let staging = root.path().join("staging");
        fs::create_dir_all(staging.join("duckdb")).unwrap();

        publish(&staging, &source_dir).unwrap();
        assert!(source_dir.join(COMPLETE).is_file());
        assert!(!source_dir.join("duckdb").exists());
    }
}

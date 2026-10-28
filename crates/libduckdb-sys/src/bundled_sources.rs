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

use std::path::{Path, PathBuf};

/// Join two paths, removing the eventual leading `/` from the second path
pub fn join(a: &Path, b: &Path) -> PathBuf {
    a.join(b.strip_prefix("/").unwrap_or(b))
}

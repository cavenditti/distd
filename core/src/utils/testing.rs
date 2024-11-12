use std::{
    fs::{create_dir_all, File},
    io::Write,
    ops::Deref,
    path::{Path, PathBuf},
    str::FromStr,
};

use uuid::Uuid;

// Random path to avoid confliting file creation from mutliple concurrent tests
#[must_use]
pub fn random_path() -> PathBuf {
    PathBuf::from_str(&Uuid::new_v4().to_string()).unwrap()
}

// Random path in a subdir to avoid confliting file creation from mutliple concurrent tests
#[must_use]
pub fn random_path_subdir() -> PathBuf {
    PathBuf::from_str(&format!("random/unique/path/{}", Uuid::new_v4())).unwrap()
}

/// Creates a random path in /tmp, ensuring its parent directory exists
///
/// Use this preferably for small files
#[must_use]
pub fn temp_path() -> PathBuf {
    let path = std::env::temp_dir().join(random_path());
    create_dir_all(path.parent().unwrap_or(&path)).unwrap();
    path
}

/// Path self deleting when going out of scope
///
/// Useful for bigger files that maybe one doesn't want in /tmp
pub struct SelfDeletingPath {
    path: PathBuf,
}

impl Drop for SelfDeletingPath {
    fn drop(&mut self) {
        if self.path.exists() {
            std::fs::remove_file(&self.path).unwrap();
        }
    }
}

impl Deref for SelfDeletingPath {
    type Target = Path;

    fn deref(&self) -> &Self::Target {
        &self.path
    }
}

impl SelfDeletingPath {
    #[must_use] pub fn new(path: PathBuf) -> Self {
        SelfDeletingPath { path }
    }
}

#[must_use] pub fn selfdel_path(path: &str) -> SelfDeletingPath {
    SelfDeletingPath::new(PathBuf::from(path))
}

#[test]
fn self_deleting_path() {
    let path = PathBuf::from("self_delete_path.test");
    File::create(&path).unwrap().write(b"ok").unwrap();
    {
        let p = SelfDeletingPath::new(path.clone());
        assert!(path.exists());
        println!(
            "We need to do something with the path to prevent it from going out of scope before: {}",
            p.path.to_string_lossy()
        );
    }
    assert!(!path.exists());
}

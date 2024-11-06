use std::path::PathBuf;

use dirs::{cache_dir as user_cache_dir, config_dir as user_config_dir};

/// Returns the path to the configuration directory.
///
/// This is the directory used for reading and storing configuration files.
/// The directory is created if it does not exist.
///
/// # Panics
///
/// This function will panic if the directory cannot be created.
#[inline]
pub fn config_dir() -> PathBuf {
    let d = user_config_dir()
        .expect("Cannot find config directory")
        .join(env!("CARGO_PKG_NAME"));
    tracing::debug!("Config dir: {}", d.to_string_lossy());
    if !d.is_dir() {
        std::fs::create_dir_all(&d).unwrap();
    }
    d
}

/// Returns the path to the cache directory.
///
/// This is used for storing both temporary and persistent files.
///
/// # Panics
///
/// This function will panic if the directory cannot be created.
#[inline]
pub fn cache_dir() -> PathBuf {
    let d = user_cache_dir()
        .expect("Cannot find config directory")
        .join(env!("CARGO_PKG_NAME"));
    tracing::debug!("Cache dir: {}", d.to_string_lossy());
    if !d.is_dir() {
        std::fs::create_dir_all(&d).unwrap();
    }
    d
}

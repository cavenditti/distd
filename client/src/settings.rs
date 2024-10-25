use config::{Config, Environment, File};
use dirs::{cache_dir as user_cache_dir, config_dir as user_config_dir};
use serde::Deserialize;
use std::{env, path::PathBuf};

use crate::error::Client as ClientError;

/// Returns the path to the configuration directory.
pub fn config_dir() -> PathBuf {
    let d = user_config_dir()
        .expect("Cannot find config directory")
        .join(env!("CARGO_PKG_NAME"));
    tracing::debug!("Config dir: {}", d.to_string_lossy());
    if !d.is_dir() {
        std::fs::create_dir_all(&d).unwrap()
    }
    d
}

/// Returns the path to the cache directory.
///
/// This is used for storing both temporary and persistent files.
pub fn cache_dir() -> PathBuf {
    let d = user_cache_dir()
        .expect("Cannot find config directory")
        .join(env!("CARGO_PKG_NAME"));
    tracing::debug!("Cache dir: {}", d.to_string_lossy());
    if !d.is_dir() {
        std::fs::create_dir_all(&d).unwrap()
    }
    d
}

#[derive(Debug, Deserialize)]
#[allow(unused)]
pub struct Server {
    pub url: String,
}

#[derive(Debug, Deserialize)]
#[allow(unused)]
pub struct Log {
    pub level: String,

    /// The path to the log file.
    log_path: PathBuf,
}

impl Log {
    fn defaults() -> config::Map<String, &'static str> {
        config::Map::from([("level".into(), "INFO"), ("log_path".into(), "INFO")])
    }
}

#[derive(Debug, Deserialize)]
#[allow(unused)]
pub struct FsStorage {
    pub enabled: bool,

    /// The path to the root of the storage directory.
    pub root: String,
}

#[derive(Debug, Deserialize)]
#[allow(unused)]
pub struct Client {
    pub name: String,
    pub sync: Vec<PathBuf>,
}

#[derive(Debug, Deserialize)]
#[allow(unused)]
pub struct Settings {
    pub debug: bool,
    pub fsstorage: FsStorage,
    pub server: Server,
    pub log: Log,
    pub client: Client,
}

impl Settings {
    pub fn new(config_file: &str) -> Result<Self, ClientError> {
        //let run_mode = env::var("RUN_MODE").unwrap_or_else(|_| "development".into());

        let s = Config::builder()
            // Merge in the "default" configuration
            .set_default("log", Log::defaults())?

            // Merge in the main configuration file
            .add_source(File::with_name(config_file))

            // Add in settings from the environment (with a prefix of DISTD)
            .add_source(Environment::with_prefix("distd"))

            // TODO change this
            .set_override("debug", true)?
            .build()?;

        // You can deserialize (and thus freeze) the entire configuration as
        s.try_deserialize().map_err(ClientError::InvaldConfig)
    }
}

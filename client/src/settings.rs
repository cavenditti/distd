use config::{Config, Environment, File};
use serde::Deserialize;
use std::path::PathBuf;

use crate::error::Client as ClientError;

pub use distd_core::utils::settings::{cache_dir, config_dir};

#[derive(Debug, Deserialize)]
#[allow(unused)]
pub struct Server {
    pub url: String,
    #[serde(default)]
    pub enrollment_token: Option<String>,
    #[serde(default)]
    pub certificate_fingerprint: Option<String>,
    #[serde(default)]
    pub allow_tofu: bool,
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

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use config::{Config, File, FileFormat};

    use super::Settings;

    #[test]
    fn parses_the_documented_toml_format() {
        let settings: Settings = Config::builder()
            .add_source(File::from_str(
                r#"
                    debug = true

                    [fsstorage]
                    enabled = true
                    root = "client-root"

                    [server]
                    url = "quic://localhost:50051"
                    enrollment_token = "enroll"
                    certificate_fingerprint = "7f195bcd309d34acd3b62561311b3b59436127a8323b43f0eb798f943b7bdf96"
                    allow_tofu = false

                    [log]
                    level = "INFO"
                    log_path = "client.log"

                    [client]
                    name = "test-client"
                    sync = ["artifact"]
                "#,
                FileFormat::Toml,
            ))
            .build()
            .expect("build TOML configuration")
            .try_deserialize()
            .expect("deserialize TOML settings");

        assert_eq!(settings.server.url, "quic://localhost:50051");
        assert_eq!(settings.client.sync, [PathBuf::from("artifact")]);
    }
}

use config::{Config, Environment, File};
use serde::{de::Error as _, Deserialize, Deserializer};
use std::{fmt, path::PathBuf};

use crate::{error::Client as ClientError, publisher::PublisherPolicy};

pub use distd_core::utils::settings::{cache_dir, config_dir};

#[allow(unused)]
pub struct Server {
    pub url: String,
    pub enrollment_token: Option<String>,
    pub certificate_fingerprint: Option<String>,
    pub allow_tofu: bool,
    pub publisher_policy: PublisherPolicy,
}

#[derive(Deserialize)]
struct RawServer {
    url: String,
    #[serde(default)]
    enrollment_token: Option<String>,
    #[serde(default)]
    certificate_fingerprint: Option<String>,
    #[serde(default)]
    allow_tofu: bool,
    #[serde(default)]
    publisher_public_keys: Vec<String>,
    #[serde(default)]
    allow_unsigned_publication: bool,
}

impl<'de> Deserialize<'de> for Server {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = RawServer::deserialize(deserializer)?;
        let publisher_policy = PublisherPolicy::from_config(
            &raw.publisher_public_keys,
            raw.allow_unsigned_publication,
        )
        .map_err(D::Error::custom)?;
        Ok(Self {
            url: raw.url,
            enrollment_token: raw.enrollment_token,
            certificate_fingerprint: raw.certificate_fingerprint,
            allow_tofu: raw.allow_tofu,
            publisher_policy,
        })
    }
}

impl fmt::Debug for Server {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Server")
            .field("url", &self.url)
            .field(
                "enrollment_token",
                &self.enrollment_token.as_ref().map(|_| "<redacted>"),
            )
            .field("certificate_fingerprint", &self.certificate_fingerprint)
            .field("allow_tofu", &self.allow_tofu)
            .field("publisher_policy", &self.publisher_policy)
            .finish()
    }
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
                    publisher_public_keys = ["0101010101010101010101010101010101010101010101010101010101010101"]

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
        assert_eq!(settings.server.publisher_policy.key_count(), 1);
        assert_eq!(settings.client.sync, [PathBuf::from("artifact")]);
    }

    #[test]
    fn server_debug_redacts_the_enrollment_token() {
        let server = super::Server {
            url: String::from("quic://localhost:50051"),
            enrollment_token: Some(String::from("do-not-log-this")),
            certificate_fingerprint: None,
            allow_tofu: true,
            publisher_policy: crate::publisher::PublisherPolicy::from_config(
                &["02".repeat(32)],
                false,
            )
            .unwrap(),
        };

        let debug = format!("{server:?}");
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("do-not-log-this"));
        assert!(!debug.contains(&"02".repeat(32)));
    }

    #[test]
    fn settings_require_exactly_one_secure_or_unsigned_publisher_mode() {
        let parse = |publisher: &str| {
            Config::builder()
                .add_source(File::from_str(
                    &format!(
                        r#"
                            debug = true
                            [fsstorage]
                            enabled = true
                            root = "client-root"
                            [server]
                            url = "quic://localhost:50051"
                            {publisher}
                            [log]
                            level = "INFO"
                            log_path = "client.log"
                            [client]
                            name = "test-client"
                            sync = []
                        "#
                    ),
                    FileFormat::Toml,
                ))
                .build()
                .unwrap()
                .try_deserialize::<Settings>()
        };

        assert!(parse(&format!("publisher_public_keys = [\"{}\"]", "A".repeat(64))).is_err());
        assert!(parse("").is_err());
        let unsigned = parse("allow_unsigned_publication = true").unwrap();
        assert!(unsigned.server.publisher_policy.permits_unsigned());
        assert!(parse(&format!(
            "publisher_public_keys = [\"{}\"]\nallow_unsigned_publication = true",
            "03".repeat(32)
        ))
        .is_err());
        let secure = parse(&format!(
            "publisher_public_keys = [\"{}\"]",
            "04".repeat(32)
        ))
        .unwrap();
        assert_eq!(secure.server.publisher_policy.key_count(), 1);
    }
}

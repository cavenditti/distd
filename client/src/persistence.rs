use std::{
    collections::HashMap,
    fs,
    io::{Read, Write},
    path::{Path, PathBuf},
    process::exit,
    str::FromStr,
    sync::{Arc, Mutex, OnceLock},
};

use distd_core::durable_state::{
    read_bounded_file, read_enveloped_state, replace_enveloped_state, DurableStateError,
    DurableStateKind,
};
use serde::{de, Deserialize, Deserializer};
use thiserror::Error;
use uuid::Uuid;

use crate::{server_url::CanonicalServerUrl, settings::cache_dir};

const CLIENT_TRUST_SCHEMA: u16 = 1;
const CLIENT_TRUST_MAX_PAYLOAD: u64 = 1024 * 1024;
const CLIENT_TRUST_MAX_RECORDS: usize = 4096;
const CLIENT_TRUST_MAX_URL_BYTES: usize = 512;

#[inline]
#[must_use]
pub fn state_path() -> PathBuf {
    cache_dir().join(format!("{}_trust.state", env!("CARGO_PKG_NAME")))
}

#[inline]
#[must_use]
pub fn legacy_state_path() -> PathBuf {
    cache_dir().join(format!("{}_state.json", env!("CARGO_PKG_NAME")))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ServerTrustRecord {
    pub certificate_pin: [u8; 32],
    pub client_uuid: Uuid,
}

#[derive(Clone, Debug)]
pub struct ClientPersistentState {
    path: PathBuf,
    records: HashMap<CanonicalServerUrl, ServerTrustRecord>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InstallOutcome {
    Installed,
    Unchanged,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DurableTrustReceipt {
    url: CanonicalServerUrl,
    record: ServerTrustRecord,
    outcome: InstallOutcome,
}

impl DurableTrustReceipt {
    pub(crate) fn matches(&self, url: &CanonicalServerUrl, record: &ServerTrustRecord) -> bool {
        &self.url == url && &self.record == record
    }

    pub fn outcome(&self) -> InstallOutcome {
        self.outcome
    }
}

type PathLock = Arc<Mutex<()>>;
static PROCESS_TRUST_LOCK: OnceLock<PathLock> = OnceLock::new();

fn path_lock(_path: &Path) -> Result<PathLock, PersistenceError> {
    Ok(PROCESS_TRUST_LOCK
        .get_or_init(|| Arc::new(Mutex::new(())))
        .clone())
}

#[derive(Debug, Error)]
pub enum PersistenceError {
    #[error("client trust state error: {0}")]
    Durable(#[from] DurableStateError),
    #[error("invalid client trust state: {0}")]
    Invalid(&'static str),
    #[error("invalid client trust server URL")]
    Url,
    #[error("client trust record conflicts with durable state")]
    Conflict,
    #[error("client trust JSON migration failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("client rollback state error: {0}")]
    Rollback(#[from] crate::rollback::RollbackError),
}

struct LegacyClientPersistentState {
    client_uuid: Option<String>,
    server_certificates: HashMap<String, String>,
}

impl<'de> Deserialize<'de> for LegacyClientPersistentState {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        enum Field {
            ClientUuid,
            ServerCertificates,
        }
        impl<'de> Deserialize<'de> for Field {
            fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                let field = String::deserialize(deserializer)?;
                match field.as_str() {
                    "client_uuid" => Ok(Self::ClientUuid),
                    "server_certificates" => Ok(Self::ServerCertificates),
                    _ => Err(de::Error::unknown_field(
                        &field,
                        &["client_uuid", "server_certificates"],
                    )),
                }
            }
        }
        struct Visitor;
        impl<'de> de::Visitor<'de> for Visitor {
            type Value = LegacyClientPersistentState;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("legacy client trust object")
            }

            fn visit_map<A: de::MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                let mut client_uuid = None;
                let mut client_uuid_seen = false;
                let mut server_certificates = None;
                while let Some(field) = map.next_key()? {
                    match field {
                        Field::ClientUuid => {
                            if client_uuid_seen {
                                return Err(de::Error::duplicate_field("client_uuid"));
                            }
                            client_uuid_seen = true;
                            client_uuid = map.next_value()?;
                        }
                        Field::ServerCertificates => {
                            if server_certificates.is_some() {
                                return Err(de::Error::duplicate_field("server_certificates"));
                            }
                            server_certificates = Some(map.next_value::<UniqueStringMap>()?.0);
                        }
                    }
                }
                Ok(LegacyClientPersistentState {
                    client_uuid,
                    server_certificates: server_certificates.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_map(Visitor)
    }
}

struct UniqueStringMap(HashMap<String, String>);

impl<'de> Deserialize<'de> for UniqueStringMap {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;
        impl<'de> de::Visitor<'de> for Visitor {
            type Value = UniqueStringMap;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("server certificate mapping with unique keys")
            }

            fn visit_map<A: de::MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                let mut values = HashMap::new();
                while let Some((key, value)) = map.next_entry::<String, String>()? {
                    if values.insert(key, value).is_some() {
                        return Err(de::Error::custom("duplicate server certificate URL"));
                    }
                }
                Ok(UniqueStringMap(values))
            }
        }
        deserializer.deserialize_map(Visitor)
    }
}

impl ClientPersistentState {
    pub fn load() -> Result<Self, PersistenceError> {
        Self::load_from_paths(state_path(), legacy_state_path())
    }

    pub fn load_from_paths(path: PathBuf, legacy_path: PathBuf) -> Result<Self, PersistenceError> {
        let lock = path_lock(&path)?;
        let _guard = lock
            .lock()
            .map_err(|_| PersistenceError::Invalid("client trust path lock is poisoned"))?;
        Self::load_from_paths_locked(path, legacy_path)
    }

    fn load_from_paths_locked(
        path: PathBuf,
        legacy_path: PathBuf,
    ) -> Result<Self, PersistenceError> {
        match fs::symlink_metadata(&path) {
            Ok(_) => {
                let payload = read_enveloped_state(
                    &path,
                    DurableStateKind::ClientTrust,
                    CLIENT_TRUST_SCHEMA,
                    CLIENT_TRUST_MAX_PAYLOAD,
                )?;
                let records = decode_payload(&payload)?;
                Ok(Self { path, records })
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                match fs::symlink_metadata(&legacy_path) {
                    Ok(_) => Self::migrate_legacy(path, legacy_path),
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Self {
                        path,
                        records: HashMap::new(),
                    }),
                    Err(error) => Err(DurableStateError::Io(error).into()),
                }
            }
            Err(error) => Err(DurableStateError::Io(error).into()),
        }
    }

    pub fn record(&self, url: &CanonicalServerUrl) -> Option<&ServerTrustRecord> {
        self.records.get(url)
    }

    pub fn records(&self) -> &HashMap<CanonicalServerUrl, ServerTrustRecord> {
        &self.records
    }

    pub fn install(
        &mut self,
        url: CanonicalServerUrl,
        record: ServerTrustRecord,
    ) -> Result<DurableTrustReceipt, PersistenceError> {
        self.install_with_replace(url, record, |path, payload| {
            replace_enveloped_state(
                path,
                DurableStateKind::ClientTrust,
                CLIENT_TRUST_SCHEMA,
                payload,
                CLIENT_TRUST_MAX_PAYLOAD,
            )
        })
    }

    fn install_with_replace<F>(
        &mut self,
        url: CanonicalServerUrl,
        record: ServerTrustRecord,
        replace: F,
    ) -> Result<DurableTrustReceipt, PersistenceError>
    where
        F: FnOnce(&Path, &[u8]) -> Result<(), DurableStateError>,
    {
        if record.client_uuid.is_nil() {
            return Err(PersistenceError::Invalid("client UUID must be non-nil"));
        }
        let lock = path_lock(&self.path)?;
        let _guard = lock
            .lock()
            .map_err(|_| PersistenceError::Invalid("client trust path lock is poisoned"))?;
        let disk_records = match fs::symlink_metadata(&self.path) {
            Ok(_) => {
                let payload = read_enveloped_state(
                    &self.path,
                    DurableStateKind::ClientTrust,
                    CLIENT_TRUST_SCHEMA,
                    CLIENT_TRUST_MAX_PAYLOAD,
                )?;
                decode_payload(&payload)?
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                if self.records.is_empty() {
                    HashMap::new()
                } else {
                    return Err(PersistenceError::Invalid(
                        "client trust state disappeared after loading",
                    ));
                }
            }
            Err(error) => return Err(DurableStateError::Io(error).into()),
        };

        if let Some(current) = disk_records.get(&url) {
            return if current == &record {
                self.records = disk_records;
                Ok(DurableTrustReceipt {
                    url,
                    record,
                    outcome: InstallOutcome::Unchanged,
                })
            } else {
                Err(PersistenceError::Conflict)
            };
        }
        let mut candidate = disk_records;
        candidate.insert(url.clone(), record);
        let payload = encode_payload(&candidate)?;
        replace(&self.path, &payload)?;
        let reread = read_enveloped_state(
            &self.path,
            DurableStateKind::ClientTrust,
            CLIENT_TRUST_SCHEMA,
            CLIENT_TRUST_MAX_PAYLOAD,
        )?;
        if decode_payload(&reread)? != candidate {
            return Err(PersistenceError::Invalid(
                "durable reread differs from candidate",
            ));
        }
        self.records = candidate;
        Ok(DurableTrustReceipt {
            url,
            record,
            outcome: InstallOutcome::Installed,
        })
    }

    fn migrate_legacy(path: PathBuf, legacy_path: PathBuf) -> Result<Self, PersistenceError> {
        let bytes = read_bounded_file(&legacy_path, CLIENT_TRUST_MAX_PAYLOAD)?;
        let legacy: LegacyClientPersistentState = serde_json::from_slice(&bytes)?;
        let records = legacy_records(legacy)?;
        let payload = encode_payload(&records)?;
        replace_enveloped_state(
            &path,
            DurableStateKind::ClientTrust,
            CLIENT_TRUST_SCHEMA,
            &payload,
            CLIENT_TRUST_MAX_PAYLOAD,
        )?;
        let reread = read_enveloped_state(
            &path,
            DurableStateKind::ClientTrust,
            CLIENT_TRUST_SCHEMA,
            CLIENT_TRUST_MAX_PAYLOAD,
        )?;
        if decode_payload(&reread)? != records {
            return Err(PersistenceError::Invalid(
                "legacy migration reread differs from candidate",
            ));
        }
        Ok(Self { path, records })
    }
}

fn legacy_records(
    legacy: LegacyClientPersistentState,
) -> Result<HashMap<CanonicalServerUrl, ServerTrustRecord>, PersistenceError> {
    let uuid = legacy
        .client_uuid
        .map(|value| Uuid::parse_str(&value).map_err(|_| PersistenceError::Invalid("legacy UUID")))
        .transpose()?;
    if uuid.is_some_and(|uuid| uuid.is_nil()) {
        return Err(PersistenceError::Invalid("legacy UUID must be non-nil"));
    }
    if legacy.server_certificates.is_empty() {
        return if uuid.is_none() {
            Ok(HashMap::new())
        } else {
            Err(PersistenceError::Invalid("legacy UUID has no server pin"))
        };
    }
    let uuid = uuid.ok_or(PersistenceError::Invalid("legacy pins have no UUID"))?;
    let mut pins = HashMap::new();
    for (raw_url, raw_pin) in legacy.server_certificates {
        let url = CanonicalServerUrl::parse(&raw_url).map_err(|_| PersistenceError::Url)?;
        let pin = decode_pin(&raw_pin)?;
        if pins
            .insert(url, pin)
            .is_some_and(|previous| previous != pin)
        {
            return Err(PersistenceError::Invalid(
                "legacy canonical URL collision has conflicting pins",
            ));
        }
    }
    if pins.len() != 1 {
        return Err(PersistenceError::Invalid(
            "legacy global UUID belongs to multiple authorities",
        ));
    }
    Ok(pins
        .into_iter()
        .map(|(url, certificate_pin)| {
            (
                url,
                ServerTrustRecord {
                    certificate_pin,
                    client_uuid: uuid,
                },
            )
        })
        .collect())
}

fn encode_payload(
    records: &HashMap<CanonicalServerUrl, ServerTrustRecord>,
) -> Result<Vec<u8>, PersistenceError> {
    if records.len() > CLIENT_TRUST_MAX_RECORDS {
        return Err(PersistenceError::Invalid("too many trust records"));
    }
    let mut entries = records.iter().collect::<Vec<_>>();
    entries.sort_by(|left, right| left.0.as_str().cmp(right.0.as_str()));
    let count = u32::try_from(entries.len())
        .map_err(|_| PersistenceError::Invalid("too many trust records"))?;
    let mut payload = Vec::new();
    payload.extend_from_slice(&count.to_le_bytes());
    for (url, record) in entries {
        let url = url.as_str().as_bytes();
        if url.len() > CLIENT_TRUST_MAX_URL_BYTES {
            return Err(PersistenceError::Invalid("trust URL is too long"));
        }
        let length = u16::try_from(url.len())
            .map_err(|_| PersistenceError::Invalid("trust URL is too long"))?;
        payload.extend_from_slice(&length.to_le_bytes());
        payload.extend_from_slice(url);
        payload.extend_from_slice(&record.certificate_pin);
        payload.extend_from_slice(record.client_uuid.as_bytes());
    }
    if payload.len() as u64 > CLIENT_TRUST_MAX_PAYLOAD {
        return Err(PersistenceError::Invalid("trust payload is too large"));
    }
    Ok(payload)
}

fn decode_payload(
    payload: &[u8],
) -> Result<HashMap<CanonicalServerUrl, ServerTrustRecord>, PersistenceError> {
    if payload.len() as u64 > CLIENT_TRUST_MAX_PAYLOAD {
        return Err(PersistenceError::Invalid("trust payload is too large"));
    }
    let mut cursor = PayloadCursor::new(payload);
    let count = cursor.u32()? as usize;
    if count > CLIENT_TRUST_MAX_RECORDS {
        return Err(PersistenceError::Invalid("too many trust records"));
    }
    let mut previous_url: Option<String> = None;
    let mut records = HashMap::with_capacity(count);
    for _ in 0..count {
        let length = cursor.u16()? as usize;
        if length == 0 || length > CLIENT_TRUST_MAX_URL_BYTES {
            return Err(PersistenceError::Invalid("trust URL length is invalid"));
        }
        let raw_url = std::str::from_utf8(cursor.take(length)?)
            .map_err(|_| PersistenceError::Invalid("trust URL is not UTF-8"))?;
        let url = CanonicalServerUrl::parse(raw_url).map_err(|_| PersistenceError::Url)?;
        if url.as_str() != raw_url {
            return Err(PersistenceError::Invalid("trust URL is not canonical"));
        }
        if previous_url
            .as_deref()
            .is_some_and(|previous| previous >= raw_url)
        {
            return Err(PersistenceError::Invalid(
                "trust records are duplicated or not sorted",
            ));
        }
        previous_url = Some(raw_url.to_owned());
        let certificate_pin = cursor.array::<32>()?;
        let client_uuid = Uuid::from_bytes(cursor.array::<16>()?);
        if client_uuid.is_nil() {
            return Err(PersistenceError::Invalid("client UUID must be non-nil"));
        }
        if records
            .insert(
                url,
                ServerTrustRecord {
                    certificate_pin,
                    client_uuid,
                },
            )
            .is_some()
        {
            return Err(PersistenceError::Invalid("duplicate trust URL"));
        }
    }
    if !cursor.finished() {
        return Err(PersistenceError::Invalid("trailing trust payload bytes"));
    }
    Ok(records)
}

fn decode_pin(value: &str) -> Result<[u8; 32], PersistenceError> {
    if value.len() != 64 {
        return Err(PersistenceError::Invalid("legacy certificate pin length"));
    }
    let mut pin = [0; 32];
    for (index, byte) in pin.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)
            .map_err(|_| PersistenceError::Invalid("legacy certificate pin encoding"))?;
    }
    Ok(pin)
}

struct PayloadCursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> PayloadCursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], PersistenceError> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or(PersistenceError::Invalid("trust payload offset overflow"))?;
        let bytes = self
            .bytes
            .get(self.offset..end)
            .ok_or(PersistenceError::Invalid("truncated trust payload"))?;
        self.offset = end;
        Ok(bytes)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], PersistenceError> {
        self.take(N)?
            .try_into()
            .map_err(|_| PersistenceError::Invalid("truncated trust payload"))
    }

    fn u16(&mut self) -> Result<u16, PersistenceError> {
        Ok(u16::from_le_bytes(self.array()?))
    }

    fn u32(&mut self) -> Result<u32, PersistenceError> {
        Ok(u32::from_le_bytes(self.array()?))
    }

    fn finished(&self) -> bool {
        self.offset == self.bytes.len()
    }
}

#[derive(Debug, Clone)]
pub struct ClientPid {
    pub pid_path: PathBuf,
}

impl ClientPid {
    fn pidfile_cleanup(pid_path: &Path) {
        std::fs::remove_file(pid_path).unwrap();
    }
}

impl Default for ClientPid {
    fn default() -> Self {
        let pid_path = cache_dir().join(format!("{}_pid", env!("CARGO_PKG_NAME")));
        tracing::debug!("Pidfile: {}", pid_path.to_string_lossy());
        if pid_path.exists() {
            let mut buf = String::new();
            std::fs::File::open(&pid_path)
                .unwrap()
                .read_to_string(&mut buf)
                .unwrap();
            if PathBuf::from_str("/proc").unwrap().join(&buf).exists() {
                println!("Client already running, exiting.");
                exit(1);
            }
        }
        std::fs::File::options()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&pid_path)
            .unwrap()
            .write_all(format!("{}", std::process::id()).as_ref())
            .unwrap();
        Self { pid_path }
    }
}

impl Drop for ClientPid {
    fn drop(&mut self) {
        Self::pidfile_cleanup(&self.pid_path);
    }
}

#[derive(Debug, Clone)]
pub struct ClientState {
    /// CLI-only process marker. Embedded sessions do not own a global PID file.
    pub pid: Option<Arc<ClientPid>>,
    pub persistent: ClientPersistentState,
    pub rollback: crate::rollback::ClientRollbackState,
}

impl ClientState {
    pub fn load() -> Result<Self, PersistenceError> {
        Ok(Self {
            pid: Some(Arc::new(ClientPid::default())),
            persistent: ClientPersistentState::load()?,
            rollback: crate::rollback::ClientRollbackState::load()?,
        })
    }

    /// Load isolated durable trust and rollback state without touching the CLI
    /// PID file or process-wide cache environment. Call on a blocking worker.
    pub fn load_in(directory: &Path) -> Result<Self, PersistenceError> {
        Ok(Self {
            pid: None,
            persistent: ClientPersistentState::load_from_paths(
                directory.join("trust.state"),
                directory.join("legacy-trust.json"),
            )?,
            rollback: crate::rollback::ClientRollbackState::load_from_path(
                directory.join("rollback.state"),
            )?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use distd_core::durable_state::{encode_envelope, read_enveloped_state};
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT: AtomicU64 = AtomicU64::new(0);

    fn paths() -> (PathBuf, PathBuf, PathBuf) {
        let directory = std::env::temp_dir().join(format!(
            "distd-client-trust-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&directory).unwrap();
        (
            directory.join("trust.state"),
            directory.join("legacy.json"),
            directory,
        )
    }

    fn record(pin: u8, uuid: u128) -> ServerTrustRecord {
        ServerTrustRecord {
            certificate_pin: [pin; 32],
            client_uuid: Uuid::from_u128(uuid),
        }
    }

    fn raw_unique_url(index: usize, target_length: usize) -> String {
        let prefix = format!("n{index:x}.");
        let fixed = "quic://".len() + prefix.len() + ":1".len();
        assert!(target_length >= fixed + 1);
        let mut remaining = target_length - fixed;
        let mut host = prefix;
        while remaining > 63 {
            // Keep at least one byte for the final label; a trailing dot is
            // intentionally removed by canonical URL normalization.
            let label = (remaining - 2).min(63);
            host.extend(std::iter::repeat_n('a', label));
            host.push('.');
            remaining -= label + 1;
        }
        host.extend(std::iter::repeat_n('a', remaining));
        let url = format!("quic://{host}:1");
        assert_eq!(url.len(), target_length);
        url
    }

    fn unique_url(index: usize, target_length: usize) -> CanonicalServerUrl {
        let mut raw_length = target_length;
        loop {
            let raw = raw_unique_url(index, raw_length);
            let url = CanonicalServerUrl::parse(&raw).unwrap();
            match url.as_str().len().cmp(&target_length) {
                std::cmp::Ordering::Equal => return url,
                std::cmp::Ordering::Less => raw_length += target_length - url.as_str().len(),
                std::cmp::Ordering::Greater => panic!("canonical URL exceeded target length"),
            }
        }
    }

    fn records_with_count(
        count: usize,
        url_length: usize,
    ) -> HashMap<CanonicalServerUrl, ServerTrustRecord> {
        (0..count)
            .map(|index| (unique_url(index, url_length), record(1, index as u128 + 1)))
            .collect()
    }

    fn records_for_payload_size(target: usize) -> HashMap<CanonicalServerUrl, ServerTrustRecord> {
        let base = 4 + CLIENT_TRUST_MAX_RECORDS * (2 + 32 + 16);
        let url_total = target.checked_sub(base).unwrap();
        let short = url_total / CLIENT_TRUST_MAX_RECORDS;
        let long_count = url_total % CLIENT_TRUST_MAX_RECORDS;
        (0..CLIENT_TRUST_MAX_RECORDS)
            .map(|index| {
                let length = short + usize::from(index < long_count);
                (unique_url(index, length), record(1, index as u128 + 1))
            })
            .collect()
    }

    #[test]
    fn flat_payload_golden_is_stable() {
        let records = HashMap::from([
            (
                CanonicalServerUrl::parse("quic://b.example:2").unwrap(),
                record(2, 2),
            ),
            (
                CanonicalServerUrl::parse("quic://a.example:1").unwrap(),
                record(1, 1),
            ),
        ]);
        let payload = encode_payload(&records).unwrap();
        let hex = payload
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        assert_eq!(
            hex,
            "020000001200717569633a2f2f612e6578616d706c653a310101010101010101010101010101010101010101010101010101010101010101000000000000000000000000000000011200717569633a2f2f622e6578616d706c653a32020202020202020202020202020202020202020202020202020202020202020200000000000000000000000000000002"
        );
        let envelope = encode_envelope(
            DurableStateKind::ClientTrust,
            CLIENT_TRUST_SCHEMA,
            &payload,
            CLIENT_TRUST_MAX_PAYLOAD,
        )
        .unwrap();
        assert_eq!(
            blake3::hash(&envelope).to_hex().as_str(),
            "574d6279ace5eef03d8d395320617c11827d83085d1f0f31ba357629417143e6"
        );
        assert_eq!(decode_payload(&payload).unwrap(), records);
    }

    #[test]
    fn encoding_is_order_independent_and_decoder_requires_order() {
        let a = CanonicalServerUrl::parse("quic://a.example:1").unwrap();
        let b = CanonicalServerUrl::parse("quic://b.example:2").unwrap();
        let forward = HashMap::from([(a.clone(), record(1, 1)), (b.clone(), record(2, 2))]);
        let reverse = HashMap::from([(b, record(2, 2)), (a, record(1, 1))]);
        assert_eq!(
            encode_payload(&forward).unwrap(),
            encode_payload(&reverse).unwrap()
        );

        let mut invalid = Vec::new();
        invalid.extend_from_slice(&2_u32.to_le_bytes());
        for (url, value) in reverse.iter().collect::<Vec<_>>().into_iter().rev() {
            invalid.extend_from_slice(&(url.as_str().len() as u16).to_le_bytes());
            invalid.extend_from_slice(url.as_str().as_bytes());
            invalid.extend_from_slice(&value.certificate_pin);
            invalid.extend_from_slice(value.client_uuid.as_bytes());
        }
        if invalid != encode_payload(&reverse).unwrap() {
            assert!(decode_payload(&invalid).is_err());
        }
    }

    #[test]
    fn install_preserves_other_records_and_rejects_conflicts() {
        let (path, legacy, directory) = paths();
        let mut state = ClientPersistentState::load_from_paths(path.clone(), legacy).unwrap();
        let a = CanonicalServerUrl::parse("quic://a.example:1").unwrap();
        let b = CanonicalServerUrl::parse("quic://b.example:2").unwrap();
        let receipt = state.install(a.clone(), record(1, 1)).unwrap();
        assert_eq!(receipt.outcome(), InstallOutcome::Installed);
        assert!(receipt.matches(&a, &record(1, 1)));
        assert_eq!(
            state.install(a.clone(), record(1, 1)).unwrap().outcome(),
            InstallOutcome::Unchanged
        );
        assert!(matches!(
            state.install(a.clone(), record(9, 9)),
            Err(PersistenceError::Conflict)
        ));
        state.install(b.clone(), record(2, 2)).unwrap();

        let reloaded =
            ClientPersistentState::load_from_paths(path, directory.join("missing")).unwrap();
        assert_eq!(reloaded.record(&a), Some(&record(1, 1)));
        assert_eq!(reloaded.record(&b), Some(&record(2, 2)));
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn independently_loaded_writers_merge_distinct_authorities() {
        let (path, legacy, directory) = paths();
        let mut first =
            ClientPersistentState::load_from_paths(path.clone(), legacy.clone()).unwrap();
        let mut second =
            ClientPersistentState::load_from_paths(path.clone(), legacy.clone()).unwrap();
        let a = CanonicalServerUrl::parse("quic://a.example:1").unwrap();
        let b = CanonicalServerUrl::parse("quic://b.example:2").unwrap();
        first.install(a.clone(), record(1, 1)).unwrap();
        second.install(b.clone(), record(2, 2)).unwrap();
        let reloaded = ClientPersistentState::load_from_paths(path, legacy).unwrap();
        assert_eq!(reloaded.record(&a), Some(&record(1, 1)));
        assert_eq!(reloaded.record(&b), Some(&record(2, 2)));
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn independently_loaded_writers_conflict_on_same_authority() {
        let (path, legacy, directory) = paths();
        let mut first =
            ClientPersistentState::load_from_paths(path.clone(), legacy.clone()).unwrap();
        let mut second =
            ClientPersistentState::load_from_paths(path.clone(), legacy.clone()).unwrap();
        let url = CanonicalServerUrl::parse("quic://same.example:1").unwrap();
        first.install(url.clone(), record(1, 1)).unwrap();
        assert!(matches!(
            second.install(url.clone(), record(2, 2)),
            Err(PersistenceError::Conflict)
        ));
        let reloaded = ClientPersistentState::load_from_paths(path, legacy).unwrap();
        assert_eq!(reloaded.record(&url), Some(&record(1, 1)));
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn concurrent_loaded_writers_serialize_without_lost_updates() {
        let (path, legacy, directory) = paths();
        let first = ClientPersistentState::load_from_paths(path.clone(), legacy.clone()).unwrap();
        let second = ClientPersistentState::load_from_paths(path.clone(), legacy.clone()).unwrap();
        let barrier = Arc::new(std::sync::Barrier::new(3));
        let spawn = |mut state: ClientPersistentState,
                     barrier: Arc<std::sync::Barrier>,
                     raw_url: &'static str,
                     value: ServerTrustRecord| {
            std::thread::spawn(move || {
                barrier.wait();
                state
                    .install(CanonicalServerUrl::parse(raw_url).unwrap(), value)
                    .map(|_| ())
            })
        };
        let first_run = spawn(
            first,
            Arc::clone(&barrier),
            "quic://concurrent-a.example:1",
            record(1, 1),
        );
        let second_run = spawn(
            second,
            Arc::clone(&barrier),
            "quic://concurrent-b.example:2",
            record(2, 2),
        );
        barrier.wait();
        first_run.join().unwrap().unwrap();
        second_run.join().unwrap().unwrap();
        let reloaded = ClientPersistentState::load_from_paths(path, legacy).unwrap();
        assert_eq!(reloaded.records().len(), 2);
        fs::remove_dir_all(directory).unwrap();
    }

    #[derive(Clone, Copy)]
    enum Fault {
        BeforeCommit,
        AfterRename,
        AfterDirectorySync,
    }

    fn assert_install_fault(fault: Fault) {
        let (path, legacy, directory) = paths();
        let mut state =
            ClientPersistentState::load_from_paths(path.clone(), legacy.clone()).unwrap();
        let old_url = CanonicalServerUrl::parse("quic://old.example:1").unwrap();
        let new_url = CanonicalServerUrl::parse("quic://new.example:2").unwrap();
        state.install(old_url.clone(), record(1, 1)).unwrap();
        let before = state.records.clone();
        let result =
            state.install_with_replace(new_url.clone(), record(2, 2), |target, payload| {
                if matches!(fault, Fault::BeforeCommit) {
                    return Err(DurableStateError::Io(std::io::Error::other(
                        "ordinary injected failure",
                    )));
                }
                replace_enveloped_state(
                    target,
                    DurableStateKind::ClientTrust,
                    CLIENT_TRUST_SCHEMA,
                    payload,
                    CLIENT_TRUST_MAX_PAYLOAD,
                )?;
                let stage = match fault {
                    Fault::AfterRename => "injected post-rename indeterminate",
                    Fault::AfterDirectorySync => "injected post-directory-sync indeterminate",
                    Fault::BeforeCommit => unreachable!(),
                };
                Err(DurableStateError::CommitIndeterminate {
                    path: target.to_path_buf(),
                    source: std::io::Error::other(stage),
                })
            });
        assert!(result.is_err());
        assert_eq!(state.records, before);
        let restarted = ClientPersistentState::load_from_paths(path, legacy).unwrap();
        assert_eq!(restarted.record(&old_url), Some(&record(1, 1)));
        if matches!(fault, Fault::BeforeCommit) {
            assert!(restarted.record(&new_url).is_none());
        } else {
            assert_eq!(restarted.record(&new_url), Some(&record(2, 2)));
        }
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn ordinary_replace_failure_keeps_old_state() {
        assert_install_fault(Fault::BeforeCommit);
    }

    #[test]
    fn post_rename_indeterminate_returns_error_with_complete_new_restart() {
        assert_install_fault(Fault::AfterRename);
    }

    #[test]
    fn post_directory_sync_indeterminate_returns_error_with_complete_new_restart() {
        assert_install_fault(Fault::AfterDirectorySync);
    }

    #[test]
    fn record_count_boundary_is_exact() {
        assert!(encode_payload(&records_with_count(CLIENT_TRUST_MAX_RECORDS, 32)).is_ok());
        assert!(encode_payload(&records_with_count(CLIENT_TRUST_MAX_RECORDS + 1, 32)).is_err());
    }

    #[test]
    fn payload_size_boundary_is_exact() {
        let records = records_for_payload_size(CLIENT_TRUST_MAX_PAYLOAD as usize);
        let payload = encode_payload(&records).unwrap();
        assert_eq!(payload.len() as u64, CLIENT_TRUST_MAX_PAYLOAD);
        let oversized = records_for_payload_size(CLIENT_TRUST_MAX_PAYLOAD as usize + 1);
        assert!(encode_payload(&oversized).is_err());
    }

    #[test]
    fn canonical_url_length_boundary_is_exact() {
        assert_eq!(
            unique_url(1, CLIENT_TRUST_MAX_URL_BYTES).as_str().len(),
            CLIENT_TRUST_MAX_URL_BYTES
        );
        let too_long = raw_unique_url(1, CLIENT_TRUST_MAX_URL_BYTES + 1);
        assert_eq!(too_long.len(), CLIENT_TRUST_MAX_URL_BYTES + 1);
        assert!(CanonicalServerUrl::parse(&too_long).is_err());
    }

    #[test]
    fn migrates_exact_legacy_json_and_retains_source() {
        let (path, legacy, directory) = paths();
        let bytes = br#"{"client_uuid":"00000000-0000-0000-0000-000000000007","server_certificates":{"udp://EXAMPLE.COM.:00443/":"0101010101010101010101010101010101010101010101010101010101010101"}}"#;
        fs::write(&legacy, bytes).unwrap();
        let state = ClientPersistentState::load_from_paths(path.clone(), legacy.clone()).unwrap();
        let url = CanonicalServerUrl::parse("quic://example.com:443").unwrap();
        assert_eq!(state.record(&url), Some(&record(1, 7)));
        assert_eq!(fs::read(&legacy).unwrap(), bytes);
        let restarted = ClientPersistentState::load_from_paths(path, legacy.clone()).unwrap();
        assert_eq!(restarted.record(&url), Some(&record(1, 7)));
        assert_eq!(fs::read(&legacy).unwrap(), bytes);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn ambiguous_legacy_global_uuid_is_rejected() {
        let (path, legacy, directory) = paths();
        fs::write(
            &legacy,
            br#"{"client_uuid":"00000000-0000-0000-0000-000000000001","server_certificates":{"quic://a.example:1":"0101010101010101010101010101010101010101010101010101010101010101","quic://b.example:2":"0202020202020202020202020202020202020202020202020202020202020202"}}"#,
        )
        .unwrap();
        assert!(ClientPersistentState::load_from_paths(path, legacy,).is_err());
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn legacy_duplicate_json_keys_are_rejected() {
        for bytes in [
            br#"{"client_uuid":null,"client_uuid":null,"server_certificates":{}}"#.as_slice(),
            br#"{"client_uuid":null,"server_certificates":{},"server_certificates":{}}"#,
            br#"{"client_uuid":"00000000-0000-0000-0000-000000000001","server_certificates":{"quic://a.example:1":"0101010101010101010101010101010101010101010101010101010101010101","quic://a.example:1":"0101010101010101010101010101010101010101010101010101010101010101"}}"#,
        ] {
            let (path, legacy, directory) = paths();
            fs::write(&legacy, bytes).unwrap();
            assert!(ClientPersistentState::load_from_paths(path, legacy).is_err());
            fs::remove_dir_all(directory).unwrap();
        }
    }

    #[test]
    fn legacy_canonical_aliases_merge_only_with_identical_pins() {
        let uuid = "00000000-0000-0000-0000-000000000001";
        let pin_a = "0101010101010101010101010101010101010101010101010101010101010101";
        let pin_b = "0202020202020202020202020202020202020202020202020202020202020202";
        for (second_pin, succeeds) in [(pin_a, true), (pin_b, false)] {
            let (path, legacy, directory) = paths();
            let json = format!(
                "{{\"client_uuid\":\"{uuid}\",\"server_certificates\":{{\"quic://EXAMPLE.com:1\":\"{pin_a}\",\"udp://example.com.:1/\":\"{second_pin}\"}}}}"
            );
            fs::write(&legacy, json).unwrap();
            let result = ClientPersistentState::load_from_paths(path, legacy);
            assert_eq!(result.is_ok(), succeeds);
            if let Ok(state) = result {
                assert_eq!(state.records.len(), 1);
            }
            fs::remove_dir_all(directory).unwrap();
        }
    }

    #[test]
    fn nil_uuids_are_rejected_on_install_decode_and_migration() {
        let (path, legacy, directory) = paths();
        let mut state = ClientPersistentState::load_from_paths(path, legacy).unwrap();
        let url = CanonicalServerUrl::parse("quic://example.com:1").unwrap();
        assert!(state.install(url.clone(), record(1, 0)).is_err());

        let payload = encode_payload(&HashMap::from([(url, record(1, 0))])).unwrap();
        assert!(decode_payload(&payload).is_err());
        fs::remove_dir_all(directory).unwrap();

        let (path, legacy, directory) = paths();
        fs::write(
            &legacy,
            br#"{"client_uuid":"00000000-0000-0000-0000-000000000000","server_certificates":{"quic://example.com:1":"0101010101010101010101010101010101010101010101010101010101010101"}}"#,
        )
        .unwrap();
        assert!(ClientPersistentState::load_from_paths(path, legacy).is_err());
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn current_presence_wins_and_corruption_fails_closed() {
        let (path, legacy, directory) = paths();
        fs::write(&legacy, br#"{"client_uuid":null,"server_certificates":{}}"#).unwrap();
        fs::write(&path, b"corrupt").unwrap();
        assert!(ClientPersistentState::load_from_paths(path.clone(), legacy).is_err());
        assert_eq!(fs::read(path).unwrap(), b"corrupt");
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn wrong_kind_and_future_schema_fail_closed() {
        for (kind, schema) in [
            (DurableStateKind::FsStorage, CLIENT_TRUST_SCHEMA),
            (DurableStateKind::ClientTrust, CLIENT_TRUST_SCHEMA + 1),
        ] {
            let (path, legacy, directory) = paths();
            let envelope = encode_envelope(kind, schema, &0_u32.to_le_bytes(), 64).unwrap();
            fs::write(&path, envelope).unwrap();
            assert!(ClientPersistentState::load_from_paths(path, legacy).is_err());
            fs::remove_dir_all(directory).unwrap();
        }
    }

    #[test]
    fn payload_rejects_truncation_trailing_and_noncanonical_url() {
        let url = CanonicalServerUrl::parse("quic://a.example:1").unwrap();
        let valid = encode_payload(&HashMap::from([(url, record(1, 1))])).unwrap();
        assert!(decode_payload(&valid[..valid.len() - 1]).is_err());
        let mut trailing = valid.clone();
        trailing.push(0);
        assert!(decode_payload(&trailing).is_err());

        let mut raw = Vec::new();
        raw.extend_from_slice(&1_u32.to_le_bytes());
        let noncanonical = b"udp://A.EXAMPLE:1";
        raw.extend_from_slice(&(noncanonical.len() as u16).to_le_bytes());
        raw.extend_from_slice(noncanonical);
        raw.extend_from_slice(&[1; 32]);
        raw.extend_from_slice(Uuid::nil().as_bytes());
        assert!(decode_payload(&raw).is_err());
    }

    #[test]
    fn current_envelope_rereads_exact_payload() {
        let (path, legacy, directory) = paths();
        let mut state = ClientPersistentState::load_from_paths(path.clone(), legacy).unwrap();
        let url = CanonicalServerUrl::parse("quic://example.com:1").unwrap();
        state.install(url, record(3, 4)).unwrap();
        let payload = read_enveloped_state(
            &path,
            DurableStateKind::ClientTrust,
            CLIENT_TRUST_SCHEMA,
            CLIENT_TRUST_MAX_PAYLOAD,
        )
        .unwrap();
        assert_eq!(decode_payload(&payload).unwrap(), state.records);
        fs::remove_dir_all(directory).unwrap();
    }
}

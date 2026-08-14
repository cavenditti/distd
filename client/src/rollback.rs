//! Durable per-authority publisher release high-water marks.

use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, OnceLock},
};

use distd_core::{
    durable_state::{
        read_enveloped_state, replace_enveloped_state_with_hook, DurableReplaceStage,
        DurableStateError, DurableStateKind,
    },
    signature::{PublisherReleaseIdentity, PublisherReleaseSequence, VerifiedPublisherRelease},
};
use thiserror::Error;
use unicode_normalization::is_nfc;

use crate::{
    server_url::{CanonicalServerUrl, MAX_CANONICAL_SERVER_URL_BYTES},
    settings::cache_dir,
};

const ROLLBACK_SCHEMA: u16 = 1;
const ROLLBACK_MAGIC: &[u8; 8] = b"DSROLL1\0";
const ROLLBACK_MAX_PAYLOAD: u64 = 64 * 1024 * 1024;
const ROLLBACK_MAX_RECORDS: usize = 65_536;
const MAX_ARTIFACT_ID_BYTES: usize = 256;

#[inline]
#[must_use]
pub fn state_path() -> PathBuf {
    cache_dir().join(format!("{}_rollback.state", env!("CARGO_PKG_NAME")))
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct RollbackKey {
    authority: CanonicalServerUrl,
    artifact_id: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RollbackRecord {
    sequence: PublisherReleaseSequence,
    identity: PublisherReleaseIdentity,
}

#[derive(Debug)]
struct RollbackInner {
    path: PathBuf,
    records: Mutex<BTreeMap<RollbackKey, RollbackRecord>>,
}

/// Shared durable rollback state used by every sync for one client process.
#[derive(Clone, Debug)]
pub struct ClientRollbackState {
    inner: Arc<RollbackInner>,
}

/// Unforgeable receipt proving one exact release is durably admitted.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DurableRollbackReceipt {
    authority: CanonicalServerUrl,
    artifact_id: String,
    sequence: PublisherReleaseSequence,
    identity: PublisherReleaseIdentity,
    outcome: RollbackOutcome,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RollbackOutcome {
    Advanced,
    Unchanged,
}

impl DurableRollbackReceipt {
    #[must_use]
    pub(crate) fn matches(
        &self,
        authority: &CanonicalServerUrl,
        artifact_id: &str,
        sequence: PublisherReleaseSequence,
        identity: PublisherReleaseIdentity,
    ) -> bool {
        &self.authority == authority
            && self.artifact_id == artifact_id
            && self.sequence == sequence
            && self.identity == identity
    }

    #[must_use]
    #[cfg(test)]
    pub(crate) const fn outcome(&self) -> RollbackOutcome {
        self.outcome
    }
}

#[derive(Debug, Error)]
pub enum RollbackError {
    #[error("publisher release is older than the durable rollback watermark")]
    RollbackDetected,
    #[error("publisher release sequence conflicts with the durable statement identity")]
    SequenceConflict,
    #[error("rollback state is invalid: {0}")]
    Invalid(&'static str),
    #[error("rollback durable state failed: {0}")]
    Durable(#[from] DurableStateError),
}

type ProcessLock = Arc<Mutex<()>>;
static PROCESS_ROLLBACK_LOCK: OnceLock<ProcessLock> = OnceLock::new();

fn process_lock() -> ProcessLock {
    PROCESS_ROLLBACK_LOCK
        .get_or_init(|| Arc::new(Mutex::new(())))
        .clone()
}

impl ClientRollbackState {
    pub fn load() -> Result<Self, RollbackError> {
        Self::load_from_path(state_path())
    }

    pub fn load_from_path(path: PathBuf) -> Result<Self, RollbackError> {
        let lock = process_lock();
        let _guard = lock
            .lock()
            .map_err(|_| RollbackError::Invalid("rollback path lock is poisoned"))?;
        let records = read_records_if_present(&path)?.unwrap_or_default();
        Ok(Self {
            inner: Arc::new(RollbackInner {
                path,
                records: Mutex::new(records),
            }),
        })
    }

    /// Compare and durably advance the high-water mark for one verified V2
    /// release. This method is synchronous and must run on a blocking worker.
    pub(crate) fn authorize(
        &self,
        authority: &CanonicalServerUrl,
        artifact_id: &str,
        verified: VerifiedPublisherRelease,
    ) -> Result<DurableRollbackReceipt, RollbackError> {
        self.authorize_with_hook(authority, artifact_id, verified, |_| Ok(()))
    }

    fn authorize_with_hook<F>(
        &self,
        authority: &CanonicalServerUrl,
        artifact_id: &str,
        verified: VerifiedPublisherRelease,
        mut hook: F,
    ) -> Result<DurableRollbackReceipt, RollbackError>
    where
        F: FnMut(DurableReplaceStage) -> std::io::Result<()>,
    {
        validate_artifact_id(artifact_id)?;
        let key = RollbackKey {
            authority: authority.clone(),
            artifact_id: artifact_id.to_owned(),
        };
        let record = RollbackRecord {
            sequence: verified.sequence(),
            identity: verified.statement_identity(),
        };
        let lock = process_lock();
        let _path_guard = lock
            .lock()
            .map_err(|_| RollbackError::Invalid("rollback path lock is poisoned"))?;
        let mut runtime = self
            .inner
            .records
            .lock()
            .map_err(|_| RollbackError::Invalid("rollback state lock is poisoned"))?;
        let disk = match read_records_if_present(&self.inner.path)? {
            Some(records) => records,
            None if runtime.is_empty() => BTreeMap::new(),
            None => {
                return Err(RollbackError::Invalid(
                    "rollback state disappeared after loading",
                ));
            }
        };

        if let Some(current) = disk.get(&key) {
            if record.sequence < current.sequence {
                return Err(RollbackError::RollbackDetected);
            }
            if record.sequence == current.sequence {
                if record.identity != current.identity {
                    return Err(RollbackError::SequenceConflict);
                }
                *runtime = disk;
                return Ok(receipt(key, record, RollbackOutcome::Unchanged));
            }
        }

        let mut candidate = disk;
        candidate.insert(key.clone(), record);
        let payload = encode_payload(&candidate)?;
        replace_enveloped_state_with_hook(
            &self.inner.path,
            DurableStateKind::ClientRollback,
            ROLLBACK_SCHEMA,
            &payload,
            ROLLBACK_MAX_PAYLOAD,
            &mut hook,
        )?;
        let reread = read_records(&self.inner.path)?;
        if reread != candidate {
            return Err(RollbackError::Invalid(
                "rollback durable reread differs from candidate",
            ));
        }
        *runtime = candidate;
        Ok(receipt(key, record, RollbackOutcome::Advanced))
    }

    #[cfg(test)]
    fn snapshot(&self) -> BTreeMap<(String, String), (u64, [u8; 32])> {
        self.inner
            .records
            .lock()
            .expect("rollback state lock")
            .iter()
            .map(|(key, record)| {
                (
                    (key.authority.as_str().to_owned(), key.artifact_id.clone()),
                    (record.sequence.get(), *record.identity.as_bytes()),
                )
            })
            .collect()
    }
}

fn receipt(
    key: RollbackKey,
    record: RollbackRecord,
    outcome: RollbackOutcome,
) -> DurableRollbackReceipt {
    DurableRollbackReceipt {
        authority: key.authority,
        artifact_id: key.artifact_id,
        sequence: record.sequence,
        identity: record.identity,
        outcome,
    }
}

fn read_records_if_present(
    path: &Path,
) -> Result<Option<BTreeMap<RollbackKey, RollbackRecord>>, RollbackError> {
    match fs::symlink_metadata(path) {
        Ok(_) => read_records(path).map(Some),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(DurableStateError::Io(error).into()),
    }
}

fn read_records(path: &Path) -> Result<BTreeMap<RollbackKey, RollbackRecord>, RollbackError> {
    let payload = read_enveloped_state(
        path,
        DurableStateKind::ClientRollback,
        ROLLBACK_SCHEMA,
        ROLLBACK_MAX_PAYLOAD,
    )?;
    decode_payload(&payload)
}

fn encode_payload(
    records: &BTreeMap<RollbackKey, RollbackRecord>,
) -> Result<Vec<u8>, RollbackError> {
    if records.len() > ROLLBACK_MAX_RECORDS {
        return Err(RollbackError::Invalid("too many rollback records"));
    }
    let mut bytes = Vec::new();
    bytes.extend_from_slice(ROLLBACK_MAGIC);
    push_u32(&mut bytes, records.len())?;
    for (key, record) in records {
        validate_artifact_id(&key.artifact_id)?;
        push_string(
            &mut bytes,
            key.authority.as_str(),
            MAX_CANONICAL_SERVER_URL_BYTES,
        )?;
        push_string(&mut bytes, &key.artifact_id, MAX_ARTIFACT_ID_BYTES)?;
        bytes.extend_from_slice(&record.sequence.get().to_le_bytes());
        bytes.extend_from_slice(record.identity.as_bytes());
        if bytes.len() as u64 > ROLLBACK_MAX_PAYLOAD {
            return Err(RollbackError::Invalid("rollback payload exceeds limit"));
        }
    }
    Ok(bytes)
}

fn decode_payload(bytes: &[u8]) -> Result<BTreeMap<RollbackKey, RollbackRecord>, RollbackError> {
    let mut reader = Reader::new(bytes);
    if reader.take(ROLLBACK_MAGIC.len())? != ROLLBACK_MAGIC {
        return Err(RollbackError::Invalid("rollback payload magic is invalid"));
    }
    let count = usize::try_from(reader.u32()?)
        .map_err(|_| RollbackError::Invalid("rollback record count overflows"))?;
    if count > ROLLBACK_MAX_RECORDS || count > reader.remaining() / 44 {
        return Err(RollbackError::Invalid("rollback record count is invalid"));
    }
    let mut records = BTreeMap::new();
    let mut previous: Option<RollbackKey> = None;
    for _ in 0..count {
        let authority_raw = reader.string(MAX_CANONICAL_SERVER_URL_BYTES)?;
        let authority = CanonicalServerUrl::parse(&authority_raw)
            .map_err(|_| RollbackError::Invalid("rollback server URL is invalid"))?;
        if authority.as_str() != authority_raw {
            return Err(RollbackError::Invalid(
                "rollback server URL is noncanonical",
            ));
        }
        let artifact_id = reader.string(MAX_ARTIFACT_ID_BYTES)?;
        validate_artifact_id(&artifact_id)?;
        let key = RollbackKey {
            authority,
            artifact_id,
        };
        if previous.as_ref().is_some_and(|prior| prior >= &key) {
            return Err(RollbackError::Invalid(
                "rollback records are not strictly ordered",
            ));
        }
        let record = RollbackRecord {
            sequence: PublisherReleaseSequence::new(reader.u64()?),
            identity: PublisherReleaseIdentity::from_bytes(reader.array()?),
        };
        previous = Some(key.clone());
        records.insert(key, record);
    }
    if reader.remaining() != 0 {
        return Err(RollbackError::Invalid(
            "rollback payload has trailing bytes",
        ));
    }
    Ok(records)
}

fn validate_artifact_id(artifact_id: &str) -> Result<(), RollbackError> {
    if artifact_id.is_empty() || artifact_id.len() > MAX_ARTIFACT_ID_BYTES || !is_nfc(artifact_id) {
        return Err(RollbackError::Invalid(
            "rollback artifact ID is noncanonical",
        ));
    }
    Ok(())
}

fn push_string(bytes: &mut Vec<u8>, value: &str, limit: usize) -> Result<(), RollbackError> {
    if value.is_empty() || value.len() > limit {
        return Err(RollbackError::Invalid("rollback string length is invalid"));
    }
    let length = u16::try_from(value.len())
        .map_err(|_| RollbackError::Invalid("rollback string length overflows"))?;
    bytes.extend_from_slice(&length.to_le_bytes());
    bytes.extend_from_slice(value.as_bytes());
    Ok(())
}

fn push_u32(bytes: &mut Vec<u8>, value: usize) -> Result<(), RollbackError> {
    bytes.extend_from_slice(
        &u32::try_from(value)
            .map_err(|_| RollbackError::Invalid("rollback record count overflows"))?
            .to_le_bytes(),
    );
    Ok(())
}

struct Reader<'a> {
    remaining: &'a [u8],
}

impl<'a> Reader<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { remaining: bytes }
    }

    fn remaining(&self) -> usize {
        self.remaining.len()
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], RollbackError> {
        if length > self.remaining.len() {
            return Err(RollbackError::Invalid("rollback payload is truncated"));
        }
        let (value, remaining) = self.remaining.split_at(length);
        self.remaining = remaining;
        Ok(value)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], RollbackError> {
        self.take(N)?
            .try_into()
            .map_err(|_| RollbackError::Invalid("rollback payload is truncated"))
    }

    fn u16(&mut self) -> Result<u16, RollbackError> {
        Ok(u16::from_le_bytes(self.array()?))
    }

    fn u32(&mut self) -> Result<u32, RollbackError> {
        Ok(u32::from_le_bytes(self.array()?))
    }

    fn u64(&mut self) -> Result<u64, RollbackError> {
        Ok(u64::from_le_bytes(self.array()?))
    }

    fn string(&mut self, limit: usize) -> Result<String, RollbackError> {
        let length = usize::from(self.u16()?);
        if length == 0 || length > limit {
            return Err(RollbackError::Invalid("rollback string length is invalid"));
        }
        std::str::from_utf8(self.take(length)?)
            .map(str::to_owned)
            .map_err(|_| RollbackError::Invalid("rollback string is not UTF-8"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use distd_core::{
        chunk_storage::{hashmap_storage::HashMapStorage, ChunkStorage},
        chunks::ChunkAlgorithm,
        signature::{
            PublisherPublicKey, PublisherReleaseEnvelope, PublisherReleaseStatement,
            PublisherSignature,
        },
    };
    use ring::signature::{Ed25519KeyPair, KeyPair};
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

    fn path() -> PathBuf {
        let root = std::env::temp_dir().join(format!(
            "distd-rollback-state-{}-{}",
            std::process::id(),
            NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&root).unwrap();
        root.join("rollback.state")
    }

    fn verified(
        artifact_id: &str,
        payload: &'static [u8],
        sequence: u64,
    ) -> VerifiedPublisherRelease {
        verified_with_seed(artifact_id, payload, sequence, [19; 32])
    }

    fn verified_with_seed(
        artifact_id: &str,
        payload: &'static [u8],
        sequence: u64,
        seed: [u8; 32],
    ) -> VerifiedPublisherRelease {
        let item = HashMapStorage::default()
            .prepare_revision(
                artifact_id.to_owned(),
                artifact_id.to_owned(),
                PathBuf::from(format!("{artifact_id}.bin")),
                0,
                None,
                Bytes::from_static(payload),
                ChunkAlgorithm::Fixed256K,
            )
            .unwrap();
        let sequence = PublisherReleaseSequence::new(sequence);
        let pair = Ed25519KeyPair::from_seed_unchecked(&seed).unwrap();
        let statement = PublisherReleaseStatement::from_item(&item, sequence).unwrap();
        PublisherReleaseEnvelope::from_parts(
            PublisherPublicKey::try_from(pair.public_key().as_ref()).unwrap(),
            PublisherSignature::try_from(pair.sign(statement.as_bytes()).as_ref()).unwrap(),
        )
        .verify(&item, sequence)
        .unwrap()
    }

    fn authority() -> CanonicalServerUrl {
        CanonicalServerUrl::parse("quic://example.test:50051").unwrap()
    }

    #[test]
    fn higher_advances_exact_replay_is_unchanged_and_lower_or_conflicting_is_rejected() {
        let state_path = path();
        let state = ClientRollbackState::load_from_path(state_path.clone()).unwrap();
        let authority = authority();
        let release_seven = verified("artifact", b"release seven", 7);
        let installed = state
            .authorize(&authority, "artifact", release_seven)
            .unwrap();
        assert_eq!(installed.outcome(), RollbackOutcome::Advanced);
        assert_eq!(state.snapshot().len(), 1);

        let replayed = state
            .authorize(&authority, "artifact", release_seven)
            .unwrap();
        assert_eq!(replayed.outcome(), RollbackOutcome::Unchanged);
        assert!(matches!(
            state.authorize(
                &authority,
                "artifact",
                verified("artifact", b"conflicting seven", 7),
            ),
            Err(RollbackError::SequenceConflict)
        ));
        assert!(matches!(
            state.authorize(
                &authority,
                "artifact",
                verified("artifact", b"older six", 6),
            ),
            Err(RollbackError::RollbackDetected)
        ));

        let release_eight = verified("artifact", b"release eight", 8);
        assert_eq!(
            state
                .authorize(&authority, "artifact", release_eight)
                .unwrap()
                .outcome(),
            RollbackOutcome::Advanced
        );
        let reopened = ClientRollbackState::load_from_path(state_path).unwrap();
        assert_eq!(reopened.snapshot(), state.snapshot());
        assert_eq!(
            reopened
                .authorize(&authority, "artifact", release_eight)
                .unwrap()
                .outcome(),
            RollbackOutcome::Unchanged
        );
    }

    #[test]
    fn records_are_scoped_by_exact_authority_and_artifact_and_encode_canonically() {
        let state_path = path();
        let state = ClientRollbackState::load_from_path(state_path.clone()).unwrap();
        let first = authority();
        let second = CanonicalServerUrl::parse("quic://other.test:50051").unwrap();
        state
            .authorize(&first, "alpha", verified("alpha", b"alpha", 3))
            .unwrap();
        state
            .authorize(&first, "beta", verified("beta", b"beta", 4))
            .unwrap();
        state
            .authorize(&second, "alpha", verified("alpha", b"other alpha", 1))
            .unwrap();

        let snapshot = state.snapshot();
        assert_eq!(snapshot.len(), 3);
        let payload = read_enveloped_state(
            &state_path,
            DurableStateKind::ClientRollback,
            ROLLBACK_SCHEMA,
            ROLLBACK_MAX_PAYLOAD,
        )
        .unwrap();
        assert_eq!(&payload[..8], ROLLBACK_MAGIC);
        assert_eq!(decode_payload(&payload).unwrap().len(), 3);
        assert_eq!(
            blake3::hash(&payload).to_hex().as_str(),
            "4c04189676314ee7f441503c1357db54db6ab23927947001030cb11415596925"
        );
        let mut trailing = payload;
        trailing.push(0);
        assert!(decode_payload(&trailing).is_err());
    }

    #[test]
    fn malformed_current_state_fails_closed_without_rewrite() {
        fn envelope(kind: DurableStateKind, schema: u16, payload: &[u8]) -> Vec<u8> {
            distd_core::durable_state::encode_envelope(kind, schema, payload, ROLLBACK_MAX_PAYLOAD)
                .unwrap()
        }

        let mut noncanonical_url = Vec::from(ROLLBACK_MAGIC.as_slice());
        noncanonical_url.extend_from_slice(&1_u32.to_le_bytes());
        push_string(
            &mut noncanonical_url,
            "quic://EXAMPLE.test:50051",
            MAX_CANONICAL_SERVER_URL_BYTES,
        )
        .unwrap();
        push_string(&mut noncanonical_url, "artifact", MAX_ARTIFACT_ID_BYTES).unwrap();
        noncanonical_url.extend_from_slice(&1_u64.to_le_bytes());
        noncanonical_url.extend_from_slice(&[7; 32]);

        let mut excessive_count = Vec::from(ROLLBACK_MAGIC.as_slice());
        excessive_count.extend_from_slice(
            &u32::try_from(ROLLBACK_MAX_RECORDS + 1)
                .unwrap()
                .to_le_bytes(),
        );
        let cases = [
            envelope(DurableStateKind::FsStorage, ROLLBACK_SCHEMA, b"wrong kind"),
            envelope(
                DurableStateKind::ClientRollback,
                ROLLBACK_SCHEMA + 1,
                b"future schema",
            ),
            envelope(
                DurableStateKind::ClientRollback,
                ROLLBACK_SCHEMA,
                b"truncated",
            ),
            envelope(
                DurableStateKind::ClientRollback,
                ROLLBACK_SCHEMA,
                &noncanonical_url,
            ),
            envelope(
                DurableStateKind::ClientRollback,
                ROLLBACK_SCHEMA,
                &excessive_count,
            ),
        ];
        for bytes in cases {
            let state_path = path();
            std::fs::write(&state_path, &bytes).unwrap();
            assert!(ClientRollbackState::load_from_path(state_path.clone()).is_err());
            assert_eq!(std::fs::read(state_path).unwrap(), bytes);
        }
    }

    #[test]
    fn record_and_artifact_bounds_are_exact() {
        let authority = authority();
        let mut records = BTreeMap::new();
        for index in 0..ROLLBACK_MAX_RECORDS {
            records.insert(
                RollbackKey {
                    authority: authority.clone(),
                    artifact_id: format!("artifact-{index:05}"),
                },
                RollbackRecord {
                    sequence: PublisherReleaseSequence::new(index as u64),
                    identity: PublisherReleaseIdentity::from_bytes([index as u8; 32]),
                },
            );
        }
        let payload = encode_payload(&records).unwrap();
        assert_eq!(decode_payload(&payload).unwrap(), records);
        records.insert(
            RollbackKey {
                authority,
                artifact_id: "overflow-record".to_string(),
            },
            RollbackRecord {
                sequence: PublisherReleaseSequence::new(u64::MAX),
                identity: PublisherReleaseIdentity::from_bytes([0xff; 32]),
            },
        );
        assert!(encode_payload(&records).is_err());

        assert!(validate_artifact_id(&"a".repeat(MAX_ARTIFACT_ID_BYTES)).is_ok());
        assert!(validate_artifact_id(&"a".repeat(MAX_ARTIFACT_ID_BYTES + 1)).is_err());
        assert!(validate_artifact_id("").is_err());
        assert!(validate_artifact_id("e\u{301}").is_err());
    }

    #[test]
    fn every_durable_replace_stage_restarts_to_exact_old_or_new_state() {
        for stage in [
            DurableReplaceStage::AfterEncode,
            DurableReplaceStage::AfterCreate,
            DurableReplaceStage::AfterWrite,
            DurableReplaceStage::AfterFileSync,
            DurableReplaceStage::AfterRename,
            DurableReplaceStage::AfterDirectorySync,
        ] {
            let state_path = path();
            let state = ClientRollbackState::load_from_path(state_path.clone()).unwrap();
            state
                .authorize(
                    &authority(),
                    "artifact",
                    verified("artifact", b"prior release", 8),
                )
                .unwrap();
            let result = state.authorize_with_hook(
                &authority(),
                "artifact",
                verified("artifact", b"fault candidate", 9),
                |current| {
                    if current == stage {
                        Err(std::io::Error::other("injected rollback-state failure"))
                    } else {
                        Ok(())
                    }
                },
            );
            assert!(result.is_err(), "stage {stage:?} unexpectedly succeeded");
            let reopened = ClientRollbackState::load_from_path(state_path.clone()).unwrap();
            let expected_sequence = if matches!(
                stage,
                DurableReplaceStage::AfterRename | DurableReplaceStage::AfterDirectorySync
            ) {
                9
            } else {
                8
            };
            assert_eq!(
                reopened.snapshot()[&(authority().as_str().to_owned(), "artifact".to_owned())].0,
                expected_sequence,
                "stage {stage:?} recovered the wrong side of the commit boundary"
            );
            let second = ClientRollbackState::load_from_path(state_path).unwrap();
            assert_eq!(second.snapshot(), reopened.snapshot());
        }
    }

    #[test]
    fn concurrent_instances_merge_through_the_shared_durable_file() {
        let state_path = path();
        let first = ClientRollbackState::load_from_path(state_path.clone()).unwrap();
        let second = ClientRollbackState::load_from_path(state_path.clone()).unwrap();
        let authority = authority();
        let first_authority = authority.clone();
        let second_authority = authority.clone();
        let low = std::thread::spawn(move || {
            first.authorize(
                &first_authority,
                "artifact",
                verified("artifact", b"low", 10),
            )
        });
        let high = std::thread::spawn(move || {
            second.authorize(
                &second_authority,
                "artifact",
                verified("artifact", b"high", 11),
            )
        });
        let low = low.join().unwrap();
        let high = high.join().unwrap();
        assert!(high.is_ok());
        assert!(low.is_ok() || matches!(low, Err(RollbackError::RollbackDetected)));

        let reopened = ClientRollbackState::load_from_path(state_path).unwrap();
        let record = reopened
            .snapshot()
            .remove(&(authority.as_str().to_owned(), "artifact".to_owned()))
            .unwrap();
        assert_eq!(record.0, 11);
    }

    #[test]
    fn fresh_state_accepts_first_observation_key_rotation_cannot_reset_and_max_is_final() {
        let state_path = path();
        let state = ClientRollbackState::load_from_path(state_path.clone()).unwrap();
        let authority = authority();
        assert_eq!(
            state
                .authorize(
                    &authority,
                    "artifact",
                    verified_with_seed("artifact", b"initial", 4, [31; 32]),
                )
                .unwrap()
                .outcome(),
            RollbackOutcome::Advanced
        );
        assert_eq!(
            state
                .authorize(
                    &authority,
                    "artifact",
                    verified_with_seed("artifact", b"rotated key", 5, [33; 32]),
                )
                .unwrap()
                .outcome(),
            RollbackOutcome::Advanced
        );
        assert!(matches!(
            state.authorize(
                &authority,
                "artifact",
                verified_with_seed("artifact", b"old key rollback", 4, [31; 32]),
            ),
            Err(RollbackError::RollbackDetected)
        ));
        let maximum = verified_with_seed("artifact", b"maximum", u64::MAX, [33; 32]);
        state.authorize(&authority, "artifact", maximum).unwrap();
        assert_eq!(
            state
                .authorize(&authority, "artifact", maximum)
                .unwrap()
                .outcome(),
            RollbackOutcome::Unchanged
        );
        assert!(matches!(
            state.authorize(
                &authority,
                "artifact",
                verified_with_seed("artifact", b"any lower", u64::MAX - 1, [33; 32]),
            ),
            Err(RollbackError::RollbackDetected)
        ));
        let reopened = ClientRollbackState::load_from_path(state_path).unwrap();
        assert_eq!(
            reopened.snapshot()[&(authority.as_str().to_owned(), "artifact".to_owned())].0,
            u64::MAX
        );
    }

    #[test]
    #[ignore = "release-only local rollback-watermark reference"]
    fn rollback_watermark_reference_performance() {
        assert!(!cfg!(debug_assertions), "run this reference with --release");

        fn p95(samples: &[std::time::Duration]) -> std::time::Duration {
            let mut sorted = samples.to_vec();
            sorted.sort_unstable();
            sorted[(sorted.len() * 95).div_ceil(100).saturating_sub(1)]
        }

        fn campaign(state: &ClientRollbackState, start: u64) -> Vec<std::time::Duration> {
            let authority = authority();
            let releases = (0..25)
                .map(|offset| {
                    verified_with_seed("target", b"watermark performance", start + offset, [73; 32])
                })
                .collect::<Vec<_>>();
            releases
                .into_iter()
                .enumerate()
                .filter_map(|(index, release)| {
                    let started = std::time::Instant::now();
                    state.authorize(&authority, "target", release).unwrap();
                    (index >= 5).then(|| started.elapsed())
                })
                .collect()
        }

        let small_path = path();
        let small = ClientRollbackState::load_from_path(small_path).unwrap();
        let small_samples = campaign(&small, 1);

        let large_path = path();
        let authority = authority();
        let mut records = BTreeMap::new();
        for index in 0..10_000_u64 {
            let artifact_id = if index == 0 {
                "target".to_string()
            } else {
                format!("artifact-{index:05}")
            };
            records.insert(
                RollbackKey {
                    authority: authority.clone(),
                    artifact_id,
                },
                RollbackRecord {
                    sequence: PublisherReleaseSequence::new(0),
                    identity: PublisherReleaseIdentity::from_bytes([index as u8; 32]),
                },
            );
        }
        let payload = encode_payload(&records).unwrap();
        replace_enveloped_state_with_hook(
            &large_path,
            DurableStateKind::ClientRollback,
            ROLLBACK_SCHEMA,
            &payload,
            ROLLBACK_MAX_PAYLOAD,
            |_| Ok(()),
        )
        .unwrap();
        let large = ClientRollbackState::load_from_path(large_path).unwrap();
        let large_samples = campaign(&large, 1);

        let small_p95 = p95(&small_samples);
        let small_max = *small_samples.iter().max().unwrap();
        let large_p95 = p95(&large_samples);
        let large_max = *large_samples.iter().max().unwrap();
        println!(
            "rollback_watermark_reference records=1 raw_ns={:?} p95_ns={} max_ns={} records=10000 payload_bytes={} raw_large_ns={:?} large_p95_ns={} large_max_ns={}",
            small_samples
                .iter()
                .map(std::time::Duration::as_nanos)
                .collect::<Vec<_>>(),
            small_p95.as_nanos(),
            small_max.as_nanos(),
            payload.len(),
            large_samples
                .iter()
                .map(std::time::Duration::as_nanos)
                .collect::<Vec<_>>(),
            large_p95.as_nanos(),
            large_max.as_nanos(),
        );
        assert!(small_p95 <= std::time::Duration::from_millis(30));
        assert!(small_max <= std::time::Duration::from_millis(50));
        assert!(large_p95 <= std::time::Duration::from_millis(30));
        assert!(large_max <= std::time::Duration::from_millis(50));
    }
}

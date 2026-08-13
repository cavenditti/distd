use std::{
    collections::{BTreeMap, HashMap},
    fmt::Debug,
    io,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use distd_core::{
    chunk_storage::validate_revision_metadata,
    chunks::{ChunkAlgorithm, ChunkInfo},
    durable_state::{
        read_bounded_file, read_enveloped_state, replace_enveloped_state, DurableStateError,
        DurableStateKind,
    },
    feed::{Feed, Name as FeedName},
    hash::Hash,
    item::{ArtifactId, FileEntry, Format as ItemFormat, Item, Manifest},
    metadata::Item as ItemMetadata,
    version::Version,
};
use serde::{Deserialize, Serialize};

use crate::{error::Server as ServerError, server::InternalMetadata};

pub const SERVER_PUBLICATION_SCHEMA: u16 = 1;
pub const MAX_SERVER_PUBLICATION_PAYLOAD: u64 = 64 * 1024 * 1024;
const MAX_FEEDS: usize = 65_536;
const MAX_ARTIFACTS: usize = 65_536;
const MAX_REVISIONS: usize = 1_048_576;

#[derive(Clone, Debug)]
pub struct MetadataPaths {
    pub current: PathBuf,
    pub legacy: PathBuf,
}

impl MetadataPaths {
    #[must_use]
    pub fn in_state_directory(directory: &Path) -> Self {
        Self {
            current: directory.join("metadata.state"),
            legacy: directory.join("metadata.bitcode"),
        }
    }
}

pub trait MetadataReplace: Debug + Send + Sync {
    fn replace(&self, path: &Path, payload: &[u8]) -> Result<(), DurableStateError>;
}

#[cfg(test)]
pub(crate) struct StageFailingMetadataReplace {
    stage: distd_core::durable_state::DurableReplaceStage,
}

#[cfg(test)]
impl Debug for StageFailingMetadataReplace {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StageFailingMetadataReplace")
            .field("stage", &self.stage)
            .finish()
    }
}

#[cfg(test)]
impl StageFailingMetadataReplace {
    pub(crate) fn new(stage: distd_core::durable_state::DurableReplaceStage) -> Arc<Self> {
        Arc::new(Self { stage })
    }
}

#[cfg(test)]
impl MetadataReplace for StageFailingMetadataReplace {
    fn replace(&self, path: &Path, payload: &[u8]) -> Result<(), DurableStateError> {
        distd_core::durable_state::replace_enveloped_state_with_hook(
            path,
            DurableStateKind::ServerPublication,
            SERVER_PUBLICATION_SCHEMA,
            payload,
            MAX_SERVER_PUBLICATION_PAYLOAD,
            |stage| {
                if stage == self.stage {
                    Err(io::Error::other(format!("injected {stage:?} failure")))
                } else {
                    Ok(())
                }
            },
        )
    }
}

#[derive(Debug)]
struct DurableMetadataReplace;

impl MetadataReplace for DurableMetadataReplace {
    fn replace(&self, path: &Path, payload: &[u8]) -> Result<(), DurableStateError> {
        replace_enveloped_state(
            path,
            DurableStateKind::ServerPublication,
            SERVER_PUBLICATION_SCHEMA,
            payload,
            MAX_SERVER_PUBLICATION_PAYLOAD,
        )
    }
}

#[derive(Debug)]
pub struct MetadataStateStore {
    paths: MetadataPaths,
    replacer: Arc<dyn MetadataReplace>,
    poisoned: AtomicBool,
}

#[derive(Debug)]
pub struct DurableMetadataReceipt {
    _private: (),
}

impl DurableMetadataReceipt {
    pub(crate) fn acknowledge(self) {}
}

impl MetadataStateStore {
    pub fn open_with_validator<F>(
        paths: MetadataPaths,
        validate: F,
    ) -> Result<(Arc<Self>, InternalMetadata), ServerError>
    where
        F: FnOnce(&InternalMetadata) -> Result<(), ServerError>,
    {
        Self::open_with_replacer(paths, Arc::new(DurableMetadataReplace), validate)
    }

    pub(crate) fn open_with_replacer<F>(
        paths: MetadataPaths,
        replacer: Arc<dyn MetadataReplace>,
        validate: F,
    ) -> Result<(Arc<Self>, InternalMetadata), ServerError>
    where
        F: FnOnce(&InternalMetadata) -> Result<(), ServerError>,
    {
        let store = Arc::new(Self {
            paths,
            replacer,
            poisoned: AtomicBool::new(false),
        });
        let current_present = path_entry_present(&store.paths.current)?;
        let legacy_present = path_entry_present(&store.paths.legacy)?;
        let (metadata, needs_current_write) = if current_present {
            (read_current(&store.paths.current)?, false)
        } else if legacy_present {
            (read_legacy(&store.paths.legacy)?, true)
        } else {
            (InternalMetadata::default(), true)
        };
        validate_runtime_metadata(&metadata)?;
        validate(&metadata)?;
        if needs_current_write {
            store.replace_and_verify(&metadata)?;
        }
        Ok((store, metadata))
    }

    #[must_use]
    pub fn for_current_path(path: PathBuf) -> Arc<Self> {
        Arc::new(Self {
            paths: MetadataPaths {
                legacy: path.with_extension("bitcode"),
                current: path,
            },
            replacer: Arc::new(DurableMetadataReplace),
            poisoned: AtomicBool::new(false),
        })
    }

    pub(crate) fn commit(
        &self,
        previous: &InternalMetadata,
        candidate: &InternalMetadata,
    ) -> Result<DurableMetadataReceipt, ServerError> {
        if self.poisoned.load(Ordering::Acquire) {
            return Err(ServerError::State(
                "metadata publication is read-only until restart".to_string(),
            ));
        }
        let payload = canonical_payload(candidate)?;
        match self.replacer.replace(&self.paths.current, &payload) {
            Ok(()) => {
                let reread = match read_current(&self.paths.current) {
                    Ok(reread) => reread,
                    Err(error) => {
                        self.poisoned.store(true, Ordering::Release);
                        return Err(ServerError::State(format!(
                            "metadata replacement completed but reread failed; restart required: {error}"
                        )));
                    }
                };
                if &reread != candidate {
                    self.poisoned.store(true, Ordering::Release);
                    return Err(ServerError::State(
                        "durable metadata reread disagrees with candidate; restart required"
                            .to_string(),
                    ));
                }
                Ok(DurableMetadataReceipt { _private: () })
            }
            Err(error @ DurableStateError::CommitIndeterminate { .. }) => {
                let reconciled = read_current(&self.paths.current);
                self.poisoned.store(true, Ordering::Release);
                match reconciled {
                    Ok(actual) if actual == *candidate || actual == *previous => {
                        Err(ServerError::DurableState(error))
                    }
                    Ok(_) | Err(_) => Err(ServerError::State(
                        "metadata commit is indeterminate and target reconciliation failed; restart required"
                            .to_string(),
                    )),
                }
            }
            Err(error) => {
                if path_entry_present(&self.paths.current)? {
                    let actual = read_current(&self.paths.current)?;
                    if &actual != previous {
                        self.poisoned.store(true, Ordering::Release);
                        return Err(ServerError::State(
                            "metadata pre-commit failure changed durable state; restart required"
                                .to_string(),
                        ));
                    }
                }
                Err(ServerError::DurableState(error))
            }
        }
    }

    #[must_use]
    pub fn is_poisoned(&self) -> bool {
        self.poisoned.load(Ordering::Acquire)
    }

    fn replace_and_verify(&self, metadata: &InternalMetadata) -> Result<(), ServerError> {
        if let Some(parent) = self.paths.current.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let payload = canonical_payload(metadata)?;
        self.replacer.replace(&self.paths.current, &payload)?;
        let reread = read_current(&self.paths.current)?;
        if &reread != metadata {
            return Err(ServerError::State(
                "bootstrapped metadata reread disagrees with source".to_string(),
            ));
        }
        Ok(())
    }
}

const CURRENT_PAYLOAD_MAGIC: &[u8; 8] = b"DSPUBV1\0";
const MAX_STRING_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug, Serialize, Deserialize)]
struct LegacyMetadataV0 {
    version: Version,
    feeds: HashMap<FeedName, Feed>,
    items: HashMap<ArtifactId, Item>,
}

#[derive(Debug, Serialize, Deserialize)]
struct LegacyMetadataV1 {
    version: Version,
    feeds: HashMap<FeedName, Feed>,
    items: HashMap<ArtifactId, Item>,
    #[serde(default)]
    versions: HashMap<ArtifactId, BTreeMap<u64, Item>>,
}

fn canonical_payload(metadata: &InternalMetadata) -> Result<Vec<u8>, ServerError> {
    let mut writer = WireWriterV1::new();
    writer.bytes(CURRENT_PAYLOAD_MAGIC)?;
    writer.version(metadata.version)?;

    let mut feeds = metadata.feeds.iter().collect::<Vec<_>>();
    feeds.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
    writer.count(feeds.len())?;
    for (name, feed) in feeds {
        writer.string(name)?;
        writer.feed(feed)?;
    }

    let mut artifacts = metadata.versions.iter().collect::<Vec<_>>();
    artifacts.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
    writer.count(artifacts.len())?;
    for (artifact_id, revisions) in artifacts {
        writer.string(artifact_id)?;
        writer.count(revisions.len())?;
        for (version, item) in revisions {
            writer.u64(*version)?;
            writer.item(item)?;
        }
    }
    Ok(writer.finish())
}

/// Frozen schema-1 encoder. Every integer is little-endian, every string is a
/// u32 byte length followed by UTF-8, and every collection is a u32 count.
/// Variant tags and field order below are part of the durable wire contract.
struct WireWriterV1 {
    bytes: Vec<u8>,
}

impl WireWriterV1 {
    fn new() -> Self {
        Self { bytes: Vec::new() }
    }

    fn finish(self) -> Vec<u8> {
        self.bytes
    }

    fn ensure_capacity(&self, additional: usize) -> Result<(), ServerError> {
        if self
            .bytes
            .len()
            .checked_add(additional)
            .is_none_or(|length| length as u64 > MAX_SERVER_PUBLICATION_PAYLOAD)
        {
            return Err(ServerError::State(
                "server publication payload exceeds size limit".to_string(),
            ));
        }
        Ok(())
    }

    fn bytes(&mut self, value: &[u8]) -> Result<(), ServerError> {
        self.ensure_capacity(value.len())?;
        self.bytes.extend_from_slice(value);
        Ok(())
    }

    fn u8(&mut self, value: u8) -> Result<(), ServerError> {
        self.bytes(&[value])
    }

    fn u16(&mut self, value: u16) -> Result<(), ServerError> {
        self.bytes(&value.to_le_bytes())
    }

    fn u32(&mut self, value: u32) -> Result<(), ServerError> {
        self.bytes(&value.to_le_bytes())
    }

    fn u64(&mut self, value: u64) -> Result<(), ServerError> {
        self.bytes(&value.to_le_bytes())
    }

    fn count(&mut self, value: usize) -> Result<(), ServerError> {
        let value = u32::try_from(value)
            .map_err(|_| ServerError::State("collection count exceeds u32".to_string()))?;
        self.u32(value)
    }

    fn string(&mut self, value: &str) -> Result<(), ServerError> {
        if value.len() > MAX_STRING_BYTES {
            return Err(ServerError::State(
                "server publication string exceeds size limit".to_string(),
            ));
        }
        self.count(value.len())?;
        self.bytes(value.as_bytes())
    }

    fn path(&mut self, value: &Path) -> Result<(), ServerError> {
        let value = value.to_str().ok_or_else(|| {
            ServerError::State("server publication path is not valid UTF-8".to_string())
        })?;
        self.string(value)
    }

    fn version(&mut self, value: Version) -> Result<(), ServerError> {
        let text = value.to_string();
        let mut components = text.split('.');
        for _ in 0..3 {
            let component = components
                .next()
                .ok_or_else(|| ServerError::State("invalid server version".to_string()))?
                .parse::<u16>()
                .map_err(|_| ServerError::State("invalid server version".to_string()))?;
            self.u16(component)?;
        }
        if components.next().is_some() {
            return Err(ServerError::State("invalid server version".to_string()));
        }
        Ok(())
    }

    fn timestamp(&mut self, value: SystemTime) -> Result<(), ServerError> {
        let (side, duration) = match value.duration_since(UNIX_EPOCH) {
            Ok(duration) => (0, duration),
            Err(error) => (1, error.duration()),
        };
        self.u8(side)?;
        self.u64(duration.as_secs())?;
        self.u32(duration.subsec_nanos())
    }

    fn chunk_info(&mut self, value: &ChunkInfo) -> Result<(), ServerError> {
        self.u64(value.size)?;
        self.bytes(value.hash.as_bytes())
    }

    fn chunk_algorithm(&mut self, value: ChunkAlgorithm) -> Result<(), ServerError> {
        match value {
            ChunkAlgorithm::Fixed256K => self.u8(0),
            ChunkAlgorithm::FixedAligned {
                chunk_size,
                alignment,
                header_size,
                profile_version,
            } => {
                self.u8(1)?;
                self.u32(chunk_size)?;
                self.u32(alignment)?;
                self.u32(header_size)?;
                self.u32(profile_version)
            }
            ChunkAlgorithm::FastCdc {
                min_size,
                avg_size,
                max_size,
                normalization_level,
                seed,
                profile_version,
            } => {
                self.u8(2)?;
                self.u32(min_size)?;
                self.u32(avg_size)?;
                self.u32(max_size)?;
                self.u32(normalization_level)?;
                self.u64(seed)?;
                self.u32(profile_version)
            }
            ChunkAlgorithm::BlockAlignedFastCdc {
                min_size,
                avg_size,
                max_size,
                alignment,
                zero_run_cutoff,
                normalization_level,
                seed,
                profile_version,
            } => {
                self.u8(3)?;
                self.u32(min_size)?;
                self.u32(avg_size)?;
                self.u32(max_size)?;
                self.u32(alignment)?;
                self.u32(zero_run_cutoff)?;
                self.u32(normalization_level)?;
                self.u64(seed)?;
                self.u32(profile_version)
            }
            ChunkAlgorithm::ArchiveAwareFastCdc {
                min_size,
                avg_size,
                max_size,
                record_size,
                alignment,
                normalization_level,
                seed,
                profile_version,
            } => {
                self.u8(4)?;
                self.u32(min_size)?;
                self.u32(avg_size)?;
                self.u32(max_size)?;
                self.u32(record_size)?;
                self.u32(alignment)?;
                self.u32(normalization_level)?;
                self.u64(seed)?;
                self.u32(profile_version)
            }
        }
    }

    fn item(&mut self, item: &Item) -> Result<(), ServerError> {
        let metadata = &item.metadata;
        self.string(&metadata.artifact_id)?;
        self.string(&metadata.name)?;
        match &metadata.description {
            None => self.u8(0)?,
            Some(description) => {
                self.u8(1)?;
                self.string(description)?;
            }
        }
        self.u32(metadata.revision)?;
        self.path(&metadata.path)?;
        self.chunk_info(&metadata.root)?;
        self.timestamp(metadata.created)?;
        self.timestamp(metadata.updated)?;
        self.string(&metadata.created_by)?;
        match metadata.format {
            ItemFormat::V1 => self.u8(1)?,
        }

        let manifest = &item.manifest;
        self.string(&manifest.artifact_id)?;
        self.u64(manifest.version)?;
        self.bytes(manifest.root_hash.as_bytes())?;
        self.u64(manifest.total_size)?;
        self.u32(manifest.chunk_count)?;
        self.u32(manifest.chunk_size)?;
        self.chunk_algorithm(manifest.chunk_algorithm)?;
        self.count(manifest.entries.len())?;
        for entry in &manifest.entries {
            self.string(&entry.relative_path)?;
            self.u64(entry.size)?;
            self.u32(entry.chunk_range.0)?;
            self.u32(entry.chunk_range.1)?;
        }

        self.count(item.chunks.len())?;
        for chunk in &item.chunks {
            self.chunk_info(chunk)?;
        }
        Ok(())
    }

    fn feed(&mut self, feed: &Feed) -> Result<(), ServerError> {
        self.string(&feed.name)?;
        self.count(feed.paths.len())?;
        for (path, item) in &feed.paths {
            self.path(path)?;
            self.item(item)?;
        }
        Ok(())
    }
}

struct WireReaderV1<'a> {
    remaining: &'a [u8],
}

impl<'a> WireReaderV1<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { remaining: bytes }
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], ServerError> {
        if length > self.remaining.len() {
            return Err(ServerError::State(
                "truncated server publication payload".to_string(),
            ));
        }
        let (value, remaining) = self.remaining.split_at(length);
        self.remaining = remaining;
        Ok(value)
    }

    fn expect_bytes(&mut self, expected: &[u8]) -> Result<(), ServerError> {
        if self.take(expected.len())? != expected {
            return Err(ServerError::State(
                "server publication payload has invalid schema marker".to_string(),
            ));
        }
        Ok(())
    }

    fn u8(&mut self) -> Result<u8, ServerError> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16, ServerError> {
        Ok(u16::from_le_bytes(
            self.take(2)?.try_into().expect("fixed length"),
        ))
    }

    fn u32(&mut self) -> Result<u32, ServerError> {
        Ok(u32::from_le_bytes(
            self.take(4)?.try_into().expect("fixed length"),
        ))
    }

    fn u64(&mut self) -> Result<u64, ServerError> {
        Ok(u64::from_le_bytes(
            self.take(8)?.try_into().expect("fixed length"),
        ))
    }

    fn count(&mut self, maximum: usize, label: &str) -> Result<usize, ServerError> {
        let count = usize::try_from(self.u32()?).expect("u32 fits usize");
        if count > maximum {
            return Err(ServerError::State(format!(
                "server publication {label} count exceeds limit"
            )));
        }
        Ok(count)
    }

    fn string(&mut self) -> Result<String, ServerError> {
        let length = self.count(MAX_STRING_BYTES, "string byte")?;
        let bytes = self.take(length)?;
        let value = std::str::from_utf8(bytes).map_err(|_| {
            ServerError::State("server publication string is not UTF-8".to_string())
        })?;
        Ok(value.to_owned())
    }

    fn path(&mut self) -> Result<PathBuf, ServerError> {
        Ok(PathBuf::from(self.string()?))
    }

    fn version(&mut self) -> Result<Version, ServerError> {
        Ok(Version::from((self.u16()?, self.u16()?, self.u16()?)))
    }

    fn timestamp(&mut self) -> Result<SystemTime, ServerError> {
        let side = self.u8()?;
        let seconds = self.u64()?;
        let nanos = self.u32()?;
        if nanos >= 1_000_000_000 {
            return Err(ServerError::State(
                "server publication timestamp nanoseconds are invalid".to_string(),
            ));
        }
        let duration = Duration::new(seconds, nanos);
        match side {
            0 => UNIX_EPOCH.checked_add(duration),
            1 => UNIX_EPOCH.checked_sub(duration),
            _ => {
                return Err(ServerError::State(
                    "server publication timestamp tag is invalid".to_string(),
                ));
            }
        }
        .ok_or_else(|| ServerError::State("server publication timestamp overflows".to_string()))
    }

    fn hash(&mut self) -> Result<Hash, ServerError> {
        Ok(Hash::from_bytes(
            self.take(32)?.try_into().expect("fixed length"),
        ))
    }

    fn chunk_info(&mut self) -> Result<ChunkInfo, ServerError> {
        Ok(ChunkInfo {
            size: self.u64()?,
            hash: self.hash()?,
        })
    }

    fn chunk_algorithm(&mut self) -> Result<ChunkAlgorithm, ServerError> {
        match self.u8()? {
            0 => Ok(ChunkAlgorithm::Fixed256K),
            1 => Ok(ChunkAlgorithm::FixedAligned {
                chunk_size: self.u32()?,
                alignment: self.u32()?,
                header_size: self.u32()?,
                profile_version: self.u32()?,
            }),
            2 => Ok(ChunkAlgorithm::FastCdc {
                min_size: self.u32()?,
                avg_size: self.u32()?,
                max_size: self.u32()?,
                normalization_level: self.u32()?,
                seed: self.u64()?,
                profile_version: self.u32()?,
            }),
            3 => Ok(ChunkAlgorithm::BlockAlignedFastCdc {
                min_size: self.u32()?,
                avg_size: self.u32()?,
                max_size: self.u32()?,
                alignment: self.u32()?,
                zero_run_cutoff: self.u32()?,
                normalization_level: self.u32()?,
                seed: self.u64()?,
                profile_version: self.u32()?,
            }),
            4 => Ok(ChunkAlgorithm::ArchiveAwareFastCdc {
                min_size: self.u32()?,
                avg_size: self.u32()?,
                max_size: self.u32()?,
                record_size: self.u32()?,
                alignment: self.u32()?,
                normalization_level: self.u32()?,
                seed: self.u64()?,
                profile_version: self.u32()?,
            }),
            _ => Err(ServerError::State(
                "server publication chunk algorithm tag is invalid".to_string(),
            )),
        }
    }

    fn item(&mut self) -> Result<Item, ServerError> {
        let artifact_id = self.string()?;
        let name = self.string()?;
        let description = match self.u8()? {
            0 => None,
            1 => Some(self.string()?),
            _ => {
                return Err(ServerError::State(
                    "server publication option tag is invalid".to_string(),
                ));
            }
        };
        let revision = self.u32()?;
        let path = self.path()?;
        let root = self.chunk_info()?;
        let created = self.timestamp()?;
        let updated = self.timestamp()?;
        let created_by = self.string()?;
        let format = match self.u8()? {
            1 => ItemFormat::V1,
            _ => {
                return Err(ServerError::State(
                    "server publication Item format tag is invalid".to_string(),
                ));
            }
        };

        let manifest_artifact_id = self.string()?;
        let version = self.u64()?;
        let root_hash = self.hash()?;
        let total_size = self.u64()?;
        let chunk_count = self.u32()?;
        let chunk_size = self.u32()?;
        let chunk_algorithm = self.chunk_algorithm()?;
        let entry_count = self.count(MAX_REVISIONS, "file entry")?;
        let mut entries = Vec::with_capacity(entry_count.min(4096));
        for _ in 0..entry_count {
            entries.push(FileEntry {
                relative_path: self.string()?,
                size: self.u64()?,
                chunk_range: (self.u32()?, self.u32()?),
            });
        }
        let encoded_chunk_count = self.count(MAX_REVISIONS, "chunk")?;
        let mut chunks = Vec::with_capacity(encoded_chunk_count.min(4096));
        for _ in 0..encoded_chunk_count {
            chunks.push(self.chunk_info()?);
        }
        Ok(Item {
            metadata: ItemMetadata {
                artifact_id,
                name,
                description,
                revision,
                path,
                root,
                created,
                updated,
                created_by,
                format,
            },
            manifest: Manifest {
                artifact_id: manifest_artifact_id,
                version,
                root_hash,
                total_size,
                chunk_count,
                chunk_size,
                chunk_algorithm,
                entries,
            },
            chunks,
        })
    }

    fn feed(&mut self) -> Result<Feed, ServerError> {
        let name = self.string()?;
        let path_count = self.count(MAX_REVISIONS, "feed path")?;
        let mut paths = BTreeMap::new();
        let mut previous_path: Option<PathBuf> = None;
        for _ in 0..path_count {
            let path = self.path()?;
            if previous_path.as_ref().is_some_and(|prior| prior >= &path) {
                return Err(ServerError::State(
                    "feed paths are not strictly ordered".to_string(),
                ));
            }
            let item = self.item()?;
            previous_path = Some(path.clone());
            paths.insert(path, item);
        }
        Ok(Feed { name, paths })
    }

    fn finish(self) -> Result<(), ServerError> {
        if self.remaining.is_empty() {
            Ok(())
        } else {
            Err(ServerError::State(
                "server publication payload has trailing bytes".to_string(),
            ))
        }
    }
}

fn read_current(path: &Path) -> Result<InternalMetadata, ServerError> {
    let payload = read_enveloped_state(
        path,
        DurableStateKind::ServerPublication,
        SERVER_PUBLICATION_SCHEMA,
        MAX_SERVER_PUBLICATION_PAYLOAD,
    )?;
    decode_current_payload(&payload)
}

fn read_legacy(path: &Path) -> Result<InternalMetadata, ServerError> {
    let bytes = read_bounded_file(path, MAX_SERVER_PUBLICATION_PAYLOAD)?;
    let four = bitcode::deserialize::<LegacyMetadataV1>(&bytes)
        .ok()
        .map(legacy_v1_into_runtime)
        .transpose()?;
    let three = bitcode::deserialize::<LegacyMetadataV0>(&bytes)
        .ok()
        .map(legacy_v0_into_runtime)
        .transpose()?;
    match (four, three) {
        (Some(left), Some(right)) if left == right => Ok(left),
        (Some(_), Some(_)) => Err(ServerError::State(
            "legacy metadata matches multiple layouts with different meanings".to_string(),
        )),
        (Some(metadata), None) | (None, Some(metadata)) => Ok(metadata),
        (None, None) => Err(ServerError::State(
            "legacy metadata does not match a declared supported layout".to_string(),
        )),
    }
}

fn legacy_v0_into_runtime(legacy: LegacyMetadataV0) -> Result<InternalMetadata, ServerError> {
    normalize_runtime(InternalMetadata {
        version: legacy.version,
        feeds: legacy.feeds,
        items: legacy.items,
        versions: HashMap::new(),
    })
}

fn legacy_v1_into_runtime(legacy: LegacyMetadataV1) -> Result<InternalMetadata, ServerError> {
    normalize_runtime(InternalMetadata {
        version: legacy.version,
        feeds: legacy.feeds,
        items: legacy.items,
        versions: legacy.versions,
    })
}

fn normalize_runtime(mut metadata: InternalMetadata) -> Result<InternalMetadata, ServerError> {
    for (artifact_id, item) in metadata.items.clone() {
        let versions = metadata.versions.entry(artifact_id).or_default();
        match versions.entry(item.manifest.version) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(item);
            }
            std::collections::btree_map::Entry::Occupied(entry) if entry.get() == &item => {}
            std::collections::btree_map::Entry::Occupied(_) => {
                return Err(ServerError::State(
                    "legacy current item conflicts with historical revision".to_string(),
                ));
            }
        }
    }
    validate_runtime_metadata(&metadata)?;
    Ok(metadata)
}

fn decode_current_payload(payload: &[u8]) -> Result<InternalMetadata, ServerError> {
    let mut reader = WireReaderV1::new(payload);
    reader.expect_bytes(CURRENT_PAYLOAD_MAGIC)?;
    let version = reader.version()?;

    let feed_count = reader.count(MAX_FEEDS, "feed")?;
    let mut feeds = HashMap::with_capacity(feed_count.min(4096));
    let mut previous_feed: Option<String> = None;
    for _ in 0..feed_count {
        let name = reader.string()?;
        require_next_string(&previous_feed, &name, "feed")?;
        let feed = reader.feed()?;
        if name != feed.name {
            return Err(ServerError::State("feed key and name differ".to_string()));
        }
        previous_feed = Some(name.clone());
        feeds.insert(name, feed);
    }

    let artifact_count = reader.count(MAX_ARTIFACTS, "artifact")?;
    let mut versions = HashMap::with_capacity(artifact_count.min(4096));
    let mut items = HashMap::with_capacity(artifact_count.min(4096));
    let mut previous_artifact: Option<String> = None;
    let mut total_revisions = 0usize;
    for _ in 0..artifact_count {
        let artifact_id = reader.string()?;
        require_next_string(&previous_artifact, &artifact_id, "artifact")?;
        let revision_count = reader.count(MAX_REVISIONS, "revision")?;
        if revision_count == 0 {
            return Err(ServerError::State("artifact history is empty".to_string()));
        }
        total_revisions = total_revisions
            .checked_add(revision_count)
            .ok_or_else(|| ServerError::State("revision count overflows".to_string()))?;
        if total_revisions > MAX_REVISIONS {
            return Err(ServerError::State(
                "too many metadata revisions".to_string(),
            ));
        }
        let mut history = BTreeMap::new();
        let mut previous_revision = None;
        for _ in 0..revision_count {
            let revision = reader.u64()?;
            if previous_revision.is_some_and(|prior| prior >= revision) {
                return Err(ServerError::State(
                    "revision records are not strictly increasing".to_string(),
                ));
            }
            let item = reader.item()?;
            if revision != item.manifest.version {
                return Err(ServerError::State(
                    "revision key disagrees with Item version".to_string(),
                ));
            }
            previous_revision = Some(revision);
            history.insert(revision, item);
        }
        let current = history
            .last_key_value()
            .expect("nonempty checked")
            .1
            .clone();
        previous_artifact = Some(artifact_id.clone());
        items.insert(artifact_id.clone(), current);
        versions.insert(artifact_id, history);
    }
    reader.finish()?;
    let metadata = InternalMetadata {
        version,
        feeds,
        items,
        versions,
    };
    validate_runtime_metadata(&metadata)?;
    Ok(metadata)
}

fn require_next_string(
    previous: &Option<String>,
    value: &str,
    label: &str,
) -> Result<(), ServerError> {
    if previous.as_deref().is_some_and(|prior| prior >= value) {
        return Err(ServerError::State(format!(
            "{label} records are not strictly ordered"
        )));
    }
    Ok(())
}

pub(crate) fn validate_runtime_metadata(metadata: &InternalMetadata) -> Result<(), ServerError> {
    if metadata.feeds.len() > MAX_FEEDS
        || metadata.items.len() > MAX_ARTIFACTS
        || metadata.versions.len() > MAX_ARTIFACTS
    {
        return Err(ServerError::State(
            "server publication state exceeds collection limits".to_string(),
        ));
    }
    let total_revisions = metadata
        .versions
        .values()
        .try_fold(0usize, |total, revisions| {
            total.checked_add(revisions.len())
        });
    if total_revisions.is_none_or(|count| count > MAX_REVISIONS) {
        return Err(ServerError::State(
            "too many metadata revisions".to_string(),
        ));
    }
    for (name, feed) in &metadata.feeds {
        if name != &feed.name {
            return Err(ServerError::State("feed key and name differ".to_string()));
        }
        for (path, item) in &feed.paths {
            if path != &item.metadata.path {
                return Err(ServerError::State(
                    "feed path and nested Item path differ".to_string(),
                ));
            }
            validate_revision_metadata(item).map_err(|error| {
                ServerError::State(format!(
                    "feed {name} contains an invalid published revision: {error}"
                ))
            })?;
        }
    }
    for (artifact_id, current) in &metadata.items {
        validate_item_identity(artifact_id, current)?;
        validate_revision_metadata(current).map_err(|error| {
            ServerError::State(format!(
                "artifact {artifact_id} current revision is invalid: {error}"
            ))
        })?;
        let versions = metadata.versions.get(artifact_id).ok_or_else(|| {
            ServerError::State(format!("artifact {artifact_id} has no revision history"))
        })?;
        for (version, item) in versions {
            validate_item_identity(artifact_id, item)?;
            validate_revision_metadata(item).map_err(|error| {
                ServerError::State(format!(
                    "artifact {artifact_id} version {version} is invalid: {error}"
                ))
            })?;
            if version != &item.manifest.version {
                return Err(ServerError::State(
                    "history key disagrees with Item version".to_string(),
                ));
            }
        }
        if versions.last_key_value().map(|(_, item)| item) != Some(current) {
            return Err(ServerError::State(
                "current Item is not the highest historical revision".to_string(),
            ));
        }
    }
    if metadata
        .versions
        .keys()
        .any(|artifact_id| !metadata.items.contains_key(artifact_id))
    {
        return Err(ServerError::State(
            "revision history has no current Item".to_string(),
        ));
    }
    Ok(())
}

fn validate_item_identity(artifact_id: &str, item: &Item) -> Result<(), ServerError> {
    if item.metadata.artifact_id != artifact_id
        || item.manifest.artifact_id != artifact_id
        || item.manifest.version != u64::from(item.metadata.revision)
    {
        return Err(ServerError::State(format!(
            "artifact {artifact_id} Item identity is inconsistent"
        )));
    }
    Ok(())
}

fn path_entry_present(path: &Path) -> Result<bool, ServerError> {
    match std::fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Bytes;
    use distd_core::{
        chunk_storage::{hashmap_storage::HashMapStorage, ChunkStorage},
        chunks::ChunkAlgorithm,
    };
    use std::sync::atomic::{AtomicU64, AtomicUsize};

    static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

    const LEGACY_THREE_FIELD_FIXTURE: &[u8] = &[
        0, 0, 2, 0, 0, 0, 0, 1, 6, 108, 101, 103, 97, 99, 121, 6, 108, 101, 103, 97, 99, 121, 7,
        100, 105, 115, 112, 108, 97, 121, 0, 4, 0, 12, 97, 114, 116, 105, 102, 97, 99, 116, 46, 98,
        105, 110, 6, 14, 249, 133, 234, 102, 201, 247, 36, 236, 39, 205, 138, 255, 11, 97, 4, 30,
        212, 174, 73, 170, 95, 76, 210, 144, 48, 74, 24, 152, 152, 210, 46, 43, 6, 42, 4, 0, 6, 42,
        4, 0, 5, 48, 46, 50, 46, 48, 0, 6, 108, 101, 103, 97, 99, 121, 6, 0, 249, 133, 234, 102,
        201, 247, 36, 236, 39, 205, 138, 255, 11, 97, 4, 30, 212, 174, 73, 170, 95, 76, 210, 144,
        48, 74, 24, 152, 152, 210, 46, 43, 6, 14, 4, 1, 0, 0, 0, 4, 0, 0, 0, 1, 6, 14, 249, 133,
        234, 102, 201, 247, 36, 236, 39, 205, 138, 255, 11, 97, 4, 30, 212, 174, 73, 170, 95, 76,
        210, 144, 48, 74, 24, 152, 152, 210, 46, 43,
    ];
    const LEGACY_FOUR_FIELD_FIXTURE: &[u8] = &[
        0, 0, 2, 0, 0, 0, 0, 1, 6, 108, 101, 103, 97, 99, 121, 6, 108, 101, 103, 97, 99, 121, 7,
        100, 105, 115, 112, 108, 97, 121, 0, 4, 0, 12, 97, 114, 116, 105, 102, 97, 99, 116, 46, 98,
        105, 110, 6, 14, 249, 133, 234, 102, 201, 247, 36, 236, 39, 205, 138, 255, 11, 97, 4, 30,
        212, 174, 73, 170, 95, 76, 210, 144, 48, 74, 24, 152, 152, 210, 46, 43, 6, 42, 4, 0, 6, 42,
        4, 0, 5, 48, 46, 50, 46, 48, 0, 6, 108, 101, 103, 97, 99, 121, 6, 0, 249, 133, 234, 102,
        201, 247, 36, 236, 39, 205, 138, 255, 11, 97, 4, 30, 212, 174, 73, 170, 95, 76, 210, 144,
        48, 74, 24, 152, 152, 210, 46, 43, 6, 14, 4, 1, 0, 0, 0, 4, 0, 0, 0, 1, 6, 14, 249, 133,
        234, 102, 201, 247, 36, 236, 39, 205, 138, 255, 11, 97, 4, 30, 212, 174, 73, 170, 95, 76,
        210, 144, 48, 74, 24, 152, 152, 210, 46, 43, 1, 6, 108, 101, 103, 97, 99, 121, 1, 6, 0, 6,
        108, 101, 103, 97, 99, 121, 7, 100, 105, 115, 112, 108, 97, 121, 0, 4, 0, 12, 97, 114, 116,
        105, 102, 97, 99, 116, 46, 98, 105, 110, 6, 14, 249, 133, 234, 102, 201, 247, 36, 236, 39,
        205, 138, 255, 11, 97, 4, 30, 212, 174, 73, 170, 95, 76, 210, 144, 48, 74, 24, 152, 152,
        210, 46, 43, 6, 42, 4, 0, 6, 42, 4, 0, 5, 48, 46, 50, 46, 48, 0, 6, 108, 101, 103, 97, 99,
        121, 6, 0, 249, 133, 234, 102, 201, 247, 36, 236, 39, 205, 138, 255, 11, 97, 4, 30, 212,
        174, 73, 170, 95, 76, 210, 144, 48, 74, 24, 152, 152, 210, 46, 43, 6, 14, 4, 1, 0, 0, 0, 4,
        0, 0, 0, 1, 6, 14, 249, 133, 234, 102, 201, 247, 36, 236, 39, 205, 138, 255, 11, 97, 4, 30,
        212, 174, 73, 170, 95, 76, 210, 144, 48, 74, 24, 152, 152, 210, 46, 43,
    ];

    fn directory() -> PathBuf {
        let id = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "distd-server-metadata-state-{}-{id}",
            std::process::id()
        ));
        std::fs::create_dir(&path).unwrap();
        path
    }

    fn metadata_with(artifact_id: &str, payload: &'static [u8]) -> InternalMetadata {
        let storage = HashMapStorage::default();
        let item = storage
            .prepare_revision(
                artifact_id.to_string(),
                "display".to_string(),
                "artifact.bin".into(),
                0,
                None,
                Bytes::from_static(payload),
                ChunkAlgorithm::default(),
            )
            .unwrap();
        storage
            .commit_revision(item.clone(), Bytes::from_static(payload))
            .unwrap();
        InternalMetadata {
            items: HashMap::from([(artifact_id.to_string(), item.clone())]),
            versions: HashMap::from([(
                artifact_id.to_string(),
                BTreeMap::from([(item.manifest.version, item)]),
            )]),
            ..InternalMetadata::default()
        }
    }

    fn fixed_legacy_metadata() -> InternalMetadata {
        let mut metadata = metadata_with("legacy", b"legacy payload");
        let fixed_time = std::time::UNIX_EPOCH + std::time::Duration::from_secs(42);
        let fixed_item = metadata
            .versions
            .get_mut("legacy")
            .unwrap()
            .get_mut(&0)
            .unwrap();
        fixed_item.metadata.created = fixed_time;
        fixed_item.metadata.updated = fixed_time;
        metadata
            .items
            .insert("legacy".to_string(), fixed_item.clone());
        metadata
    }

    #[test]
    fn canonical_payload_is_insertion_order_independent_and_empty_golden_is_stable() {
        let alpha = metadata_with("alpha", b"alpha");
        let beta = metadata_with("beta", b"beta");
        let mut forward = InternalMetadata::default();
        let mut reverse = InternalMetadata::default();
        for source in [&alpha, &beta] {
            forward.items.extend(source.items.clone());
            forward.versions.extend(source.versions.clone());
        }
        for source in [&beta, &alpha] {
            reverse.items.extend(source.items.clone());
            reverse.versions.extend(source.versions.clone());
        }
        assert_eq!(
            canonical_payload(&forward).unwrap(),
            canonical_payload(&reverse).unwrap()
        );

        let empty = canonical_payload(&InternalMetadata::default()).unwrap();
        assert_eq!(
            blake3::hash(&empty).to_hex().as_str(),
            "0cee528da2ce51c0c59262c5e30850ff3786c45d221fa7ddbb78a5425bffc409"
        );
    }

    #[test]
    fn nonempty_payload_has_frozen_literal_and_roundtrips_every_nested_wire_kind() {
        let chunk = ChunkInfo {
            size: 7,
            hash: Hash::from_bytes([0x11; 32]),
        };
        let created = UNIX_EPOCH + Duration::new(1_700_000_000, 123_456_789);
        let updated = UNIX_EPOCH - Duration::new(17, 42);
        let item = Item {
            metadata: ItemMetadata {
                artifact_id: "artifact".to_string(),
                name: "artifact".to_string(),
                description: Some("frozen".to_string()),
                revision: 7,
                path: PathBuf::from("deploy/artifact.bin"),
                root: chunk,
                created,
                updated,
                created_by: "distd 0.2.0".to_string(),
                format: ItemFormat::V1,
            },
            manifest: Manifest {
                artifact_id: "artifact".to_string(),
                version: 7,
                root_hash: chunk.hash,
                total_size: 7,
                chunk_count: 1,
                chunk_size: 4096,
                chunk_algorithm: ChunkAlgorithm::FixedAligned {
                    chunk_size: 4096,
                    alignment: 512,
                    header_size: 64,
                    profile_version: 3,
                },
                entries: vec![FileEntry {
                    relative_path: "bin/payload".to_string(),
                    size: 7,
                    chunk_range: (0, 1),
                }],
            },
            chunks: vec![chunk],
        };
        let feed = Feed {
            name: "stable".to_string(),
            paths: BTreeMap::from([(item.metadata.path.clone(), item.clone())]),
        };
        let metadata = InternalMetadata {
            version: Version::from((1, 2, 3)),
            feeds: HashMap::from([("stable".to_string(), feed)]),
            items: HashMap::from([("artifact".to_string(), item.clone())]),
            versions: HashMap::from([("artifact".to_string(), BTreeMap::from([(7, item)]))]),
        };

        let payload = canonical_payload(&metadata).unwrap();
        assert_eq!(&payload[..8], b"DSPUBV1\0");
        assert_eq!(payload.len(), 709);
        assert_eq!(
            blake3::hash(&payload).to_hex().as_str(),
            "4a2449965f978fe65d112121bc48eaa3cf6b26620e57f9c2490f2334e590b435"
        );
        assert_eq!(decode_current_payload(&payload).unwrap(), metadata);
        assert_eq!(
            canonical_payload(&decode_current_payload(&payload).unwrap()).unwrap(),
            payload
        );
    }

    #[test]
    #[ignore = "manual release-mode codec benchmark"]
    fn benchmark_current_codec_with_ten_thousand_artifacts() {
        let template = metadata_with("template", b"benchmark")
            .items
            .remove("template")
            .unwrap();
        let mut metadata = InternalMetadata::default();
        for index in 0..10_000 {
            let artifact_id = format!("artifact-{index:05}");
            let item = template.clone().with_artifact_id(artifact_id.clone());
            metadata.items.insert(artifact_id.clone(), item.clone());
            metadata
                .versions
                .insert(artifact_id, BTreeMap::from([(item.manifest.version, item)]));
        }

        let mut encode_samples = Vec::new();
        let mut decode_samples = Vec::new();
        let mut payload_size = 0;
        for _ in 0..7 {
            let started = std::time::Instant::now();
            let payload = canonical_payload(&metadata).unwrap();
            encode_samples.push(started.elapsed());
            payload_size = payload.len();
            let started = std::time::Instant::now();
            assert_eq!(decode_current_payload(&payload).unwrap(), metadata);
            decode_samples.push(started.elapsed());
        }
        encode_samples.sort_unstable();
        decode_samples.sort_unstable();
        eprintln!(
            "schema1 current codec 10k: payload={payload_size} encode_median={:?} encode_max={:?} decode_median={:?} decode_max={:?}",
            encode_samples[3], encode_samples[6], decode_samples[3], decode_samples[6]
        );
    }

    #[test]
    fn absent_state_bootstraps_and_current_corruption_never_falls_back() {
        let directory = directory();
        let paths = MetadataPaths::in_state_directory(&directory);
        let (_, metadata) =
            MetadataStateStore::open_with_validator(paths.clone(), |_| Ok(())).unwrap();
        assert_eq!(metadata, InternalMetadata::default());
        assert!(paths.current.is_file());

        let legacy = LegacyMetadataV0 {
            version: Version::default(),
            feeds: HashMap::new(),
            items: HashMap::new(),
        };
        std::fs::write(&paths.legacy, bitcode::serialize(&legacy).unwrap()).unwrap();
        std::fs::write(&paths.current, b"corrupt current").unwrap();
        let legacy_before = std::fs::read(&paths.legacy).unwrap();
        assert!(MetadataStateStore::open_with_validator(paths.clone(), |_| Ok(())).is_err());
        assert_eq!(std::fs::read(&paths.legacy).unwrap(), legacy_before);
        assert_eq!(std::fs::read(&paths.current).unwrap(), b"corrupt current");
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn both_declared_legacy_layouts_migrate_and_retain_exact_source() {
        let metadata = fixed_legacy_metadata();
        for bytes in [LEGACY_THREE_FIELD_FIXTURE, LEGACY_FOUR_FIELD_FIXTURE] {
            let directory = directory();
            let paths = MetadataPaths::in_state_directory(&directory);
            std::fs::write(&paths.legacy, bytes).unwrap();
            let (_, migrated) =
                MetadataStateStore::open_with_validator(paths.clone(), |_| Ok(())).unwrap();
            assert_eq!(migrated, metadata);
            assert_eq!(std::fs::read(&paths.legacy).unwrap(), bytes);
            assert_eq!(read_current(&paths.current).unwrap(), metadata);
            std::fs::remove_dir_all(directory).unwrap();
        }
    }

    #[derive(Debug)]
    struct FailingReplace {
        mode: FailureMode,
        calls: AtomicUsize,
    }

    #[derive(Clone, Copy, Debug)]
    enum FailureMode {
        BeforeRename,
        AfterRename,
    }

    impl MetadataReplace for FailingReplace {
        fn replace(&self, path: &Path, payload: &[u8]) -> Result<(), DurableStateError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            match self.mode {
                FailureMode::BeforeRename => Err(DurableStateError::Io(io::Error::other(
                    "injected pre-rename failure",
                ))),
                FailureMode::AfterRename => {
                    replace_enveloped_state(
                        path,
                        DurableStateKind::ServerPublication,
                        SERVER_PUBLICATION_SCHEMA,
                        payload,
                        MAX_SERVER_PUBLICATION_PAYLOAD,
                    )?;
                    Err(DurableStateError::CommitIndeterminate {
                        path: path.to_path_buf(),
                        source: io::Error::other("injected post-rename failure"),
                    })
                }
            }
        }
    }

    #[test]
    fn pre_rename_failure_keeps_old_and_post_rename_failure_poison_reconciles_complete_new() {
        for mode in [FailureMode::BeforeRename, FailureMode::AfterRename] {
            let directory = directory();
            let paths = MetadataPaths::in_state_directory(&directory);
            let (initial, old) =
                MetadataStateStore::open_with_validator(paths.clone(), |_| Ok(())).unwrap();
            drop(initial);
            let replacer = Arc::new(FailingReplace {
                mode,
                calls: AtomicUsize::new(0),
            });
            let store = MetadataStateStore {
                paths: paths.clone(),
                replacer,
                poisoned: AtomicBool::new(false),
            };
            let new = metadata_with("candidate", b"candidate");
            assert!(store.commit(&old, &new).is_err());
            match mode {
                FailureMode::BeforeRename => {
                    assert_eq!(read_current(&paths.current).unwrap(), old);
                    assert!(!store.is_poisoned());
                }
                FailureMode::AfterRename => {
                    assert_eq!(read_current(&paths.current).unwrap(), new);
                    assert!(store.is_poisoned());
                    assert!(store.commit(&old, &new).is_err());
                }
            }
            std::fs::remove_dir_all(directory).unwrap();
        }
    }
}

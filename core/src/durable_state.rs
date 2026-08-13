//! Bounded, versioned durable-state files.
//!
//! The replacement operation assumes a single writer in this process and a
//! non-hostile parent directory. Temporary files are created exclusively in
//! the destination directory, and the destination's final component is
//! replaced rather than followed if it is a symlink.

use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use thiserror::Error;

/// Magic at the start of every durable-state envelope.
pub const MAGIC: &[u8; 8] = b"DISTDSTA";
/// Version of the envelope itself. Payload schemas are versioned separately.
pub const ENVELOPE_VERSION: u16 = 1;
/// Fixed encoded envelope header size.
pub const HEADER_SIZE: usize = 56;
/// Default maximum payload accepted by durable-state consumers (64 MiB).
pub const MAX_DURABLE_STATE_PAYLOAD: u64 = 64 * 1024 * 1024;

const DIGEST_DOMAIN: &[u8] = b"distd durable state envelope v1\0";
const HEADER_PREFIX_SIZE: usize = 24;
const TEMP_ATTEMPTS: usize = 128;
static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(0);

/// Stable identifiers for payload families stored in durable-state envelopes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u16)]
pub enum DurableStateKind {
    FsStorage = 1,
    ClientTrust = 2,
}

impl DurableStateKind {
    fn encoded(self) -> u16 {
        self as u16
    }
}

/// Errors detected before a payload codec is invoked, or while committing it.
#[derive(Debug, Error)]
pub enum DurableStateError {
    #[error("durable-state magic is invalid")]
    BadMagic,
    #[error("durable-state kind {actual} does not match expected kind {expected}")]
    WrongKind { expected: u16, actual: u16 },
    #[error("unsupported durable-state envelope version {actual}; expected {expected}")]
    UnsupportedEnvelopeVersion { expected: u16, actual: u16 },
    #[error("unsupported durable-state payload schema {actual}; expected {expected}")]
    UnsupportedPayloadSchema { expected: u16, actual: u16 },
    #[error("durable-state flags must be zero, got {actual:#06x}")]
    UnsupportedFlags { actual: u16 },
    #[error("durable-state payload length {actual} exceeds limit {maximum}")]
    PayloadTooLarge { actual: u64, maximum: u64 },
    #[error("durable-state file length {actual} exceeds read limit {maximum}")]
    FileTooLarge { actual: u64, maximum: u64 },
    #[error("durable-state encoded length overflows this platform")]
    LengthOverflow,
    #[error("durable-state file is truncated: expected {expected} bytes, got {actual}")]
    Truncated { expected: usize, actual: usize },
    #[error("durable-state file has trailing bytes: expected {expected} bytes, got {actual}")]
    TrailingBytes { expected: usize, actual: usize },
    #[error("durable-state payload integrity check failed")]
    IntegrityMismatch,
    #[error("durable-state I/O failed: {0}")]
    Io(#[from] io::Error),
    #[error("durable-state replacement may have committed at {path}: {source}")]
    CommitIndeterminate {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
}

/// Encode a payload into the stable v1 durable-state envelope.
pub fn encode_envelope(
    kind: DurableStateKind,
    payload_schema: u16,
    payload: &[u8],
    maximum_payload: u64,
) -> Result<Vec<u8>, DurableStateError> {
    let payload_length =
        u64::try_from(payload.len()).map_err(|_| DurableStateError::LengthOverflow)?;
    if payload_length > maximum_payload {
        return Err(DurableStateError::PayloadTooLarge {
            actual: payload_length,
            maximum: maximum_payload,
        });
    }

    let total_length = HEADER_SIZE
        .checked_add(payload.len())
        .ok_or(DurableStateError::LengthOverflow)?;
    let mut encoded = Vec::with_capacity(total_length);
    encoded.extend_from_slice(MAGIC);
    encoded.extend_from_slice(&kind.encoded().to_le_bytes());
    encoded.extend_from_slice(&ENVELOPE_VERSION.to_le_bytes());
    encoded.extend_from_slice(&payload_schema.to_le_bytes());
    encoded.extend_from_slice(&0_u16.to_le_bytes());
    encoded.extend_from_slice(&payload_length.to_le_bytes());

    debug_assert_eq!(encoded.len(), HEADER_PREFIX_SIZE);
    let digest = envelope_digest(&encoded, payload);
    encoded.extend_from_slice(digest.as_bytes());
    encoded.extend_from_slice(payload);
    Ok(encoded)
}

/// Validate an envelope and return its borrowed payload.
///
/// All framing, size, and integrity checks happen before the caller can pass
/// the returned bytes to a payload codec.
pub fn decode_envelope<'a>(
    encoded: &'a [u8],
    expected_kind: DurableStateKind,
    expected_payload_schema: u16,
    maximum_payload: u64,
) -> Result<&'a [u8], DurableStateError> {
    if encoded.len() < MAGIC.len() {
        return Err(DurableStateError::Truncated {
            expected: MAGIC.len(),
            actual: encoded.len(),
        });
    }
    if &encoded[..MAGIC.len()] != MAGIC {
        return Err(DurableStateError::BadMagic);
    }
    if encoded.len() < HEADER_SIZE {
        return Err(DurableStateError::Truncated {
            expected: HEADER_SIZE,
            actual: encoded.len(),
        });
    }

    let actual_kind = read_u16(encoded, 8);
    if actual_kind != expected_kind.encoded() {
        return Err(DurableStateError::WrongKind {
            expected: expected_kind.encoded(),
            actual: actual_kind,
        });
    }
    let envelope_version = read_u16(encoded, 10);
    if envelope_version != ENVELOPE_VERSION {
        return Err(DurableStateError::UnsupportedEnvelopeVersion {
            expected: ENVELOPE_VERSION,
            actual: envelope_version,
        });
    }
    let payload_schema = read_u16(encoded, 12);
    if payload_schema != expected_payload_schema {
        return Err(DurableStateError::UnsupportedPayloadSchema {
            expected: expected_payload_schema,
            actual: payload_schema,
        });
    }
    let flags = read_u16(encoded, 14);
    if flags != 0 {
        return Err(DurableStateError::UnsupportedFlags { actual: flags });
    }

    let payload_length = read_u64(encoded, 16);
    if payload_length > maximum_payload {
        return Err(DurableStateError::PayloadTooLarge {
            actual: payload_length,
            maximum: maximum_payload,
        });
    }
    let payload_length =
        usize::try_from(payload_length).map_err(|_| DurableStateError::LengthOverflow)?;
    let total_length = HEADER_SIZE
        .checked_add(payload_length)
        .ok_or(DurableStateError::LengthOverflow)?;
    match encoded.len().cmp(&total_length) {
        std::cmp::Ordering::Less => {
            return Err(DurableStateError::Truncated {
                expected: total_length,
                actual: encoded.len(),
            });
        }
        std::cmp::Ordering::Greater => {
            return Err(DurableStateError::TrailingBytes {
                expected: total_length,
                actual: encoded.len(),
            });
        }
        std::cmp::Ordering::Equal => {}
    }

    let payload = &encoded[HEADER_SIZE..total_length];
    let expected_digest = envelope_digest(&encoded[..HEADER_PREFIX_SIZE], payload);
    if encoded[HEADER_PREFIX_SIZE..HEADER_SIZE] != expected_digest.as_bytes()[..] {
        return Err(DurableStateError::IntegrityMismatch);
    }
    Ok(payload)
}

/// Read at most `maximum_file_size` bytes, rejecting an oversized file before
/// allocating from its advertised size.
///
/// The capped stream also detects a file that grows after the metadata check,
/// while keeping allocation bounded to the caller's limit plus one byte.
pub fn read_bounded_file(
    path: &Path,
    maximum_file_size: u64,
) -> Result<Vec<u8>, DurableStateError> {
    let path_metadata = fs::symlink_metadata(path)?;
    if path_metadata.file_type().is_symlink() || !path_metadata.is_file() {
        return Err(DurableStateError::Io(io::Error::new(
            io::ErrorKind::InvalidInput,
            "durable-state input must be a regular non-symbolic-link file",
        )));
    }
    let file = File::open(path)?;
    let opened_metadata = file.metadata()?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if opened_metadata.dev() != path_metadata.dev()
            || opened_metadata.ino() != path_metadata.ino()
        {
            return Err(DurableStateError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                "durable-state input changed while it was opened",
            )));
        }
    }
    let advertised_length = opened_metadata.len();
    if advertised_length > maximum_file_size {
        return Err(DurableStateError::FileTooLarge {
            actual: advertised_length,
            maximum: maximum_file_size,
        });
    }

    let initial_capacity =
        usize::try_from(advertised_length).map_err(|_| DurableStateError::LengthOverflow)?;
    let read_limit = maximum_file_size
        .checked_add(1)
        .ok_or(DurableStateError::LengthOverflow)?;
    let mut encoded = Vec::with_capacity(initial_capacity);
    file.take(read_limit).read_to_end(&mut encoded)?;
    let actual_length =
        u64::try_from(encoded.len()).map_err(|_| DurableStateError::LengthOverflow)?;
    if actual_length > maximum_file_size {
        return Err(DurableStateError::FileTooLarge {
            actual: actual_length,
            maximum: maximum_file_size,
        });
    }
    Ok(encoded)
}

/// Read and validate an enveloped state file with bounded allocation.
pub fn read_enveloped_state(
    path: &Path,
    expected_kind: DurableStateKind,
    expected_payload_schema: u16,
    maximum_payload: u64,
) -> Result<Vec<u8>, DurableStateError> {
    let maximum_file_size = u64::try_from(HEADER_SIZE)
        .map_err(|_| DurableStateError::LengthOverflow)?
        .checked_add(maximum_payload)
        .ok_or(DurableStateError::LengthOverflow)?;
    let encoded = read_bounded_file(path, maximum_file_size)?;
    let payload = decode_envelope(
        &encoded,
        expected_kind,
        expected_payload_schema,
        maximum_payload,
    )?;
    Ok(payload.to_vec())
}

/// Encode and durably replace a state file in the same directory.
///
/// A failure before rename leaves the old destination in place and removes
/// only the temporary file created by this call. A failure after rename is
/// reported as [`DurableStateError::CommitIndeterminate`], because the complete
/// new file is visible but the directory entry's crash durability is unknown.
pub fn replace_enveloped_state(
    path: &Path,
    kind: DurableStateKind,
    payload_schema: u16,
    payload: &[u8],
    maximum_payload: u64,
) -> Result<(), DurableStateError> {
    let encoded = encode_envelope(kind, payload_schema, payload, maximum_payload)?;
    atomic_replace(path, &encoded, &mut |_| Ok(()))
}

fn envelope_digest(header_prefix: &[u8], payload: &[u8]) -> blake3::Hash {
    let mut hasher = blake3::Hasher::new();
    hasher.update(DIGEST_DOMAIN);
    hasher.update(header_prefix);
    hasher.update(payload);
    hasher.finalize()
}

fn read_u16(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes(
        bytes[offset..offset + 2]
            .try_into()
            .expect("fixed header slice"),
    )
}

fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(
        bytes[offset..offset + 8]
            .try_into()
            .expect("fixed header slice"),
    )
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AtomicReplaceStage {
    AfterEncode,
    AfterCreate,
    AfterWrite,
    AfterFileSync,
    AfterRename,
    AfterDirectorySync,
}

fn atomic_replace<F>(path: &Path, encoded: &[u8], hook: &mut F) -> Result<(), DurableStateError>
where
    F: FnMut(AtomicReplaceStage) -> io::Result<()>,
{
    hook(AtomicReplaceStage::AfterEncode)?;

    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let file_name = path.file_name().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "durable-state path has no file name",
        )
    })?;
    let (temporary_path, mut temporary_file) = create_temporary(parent, file_name)?;
    let mut temporary_owned = true;

    let result = (|| {
        hook(AtomicReplaceStage::AfterCreate)?;
        temporary_file.write_all(encoded)?;
        hook(AtomicReplaceStage::AfterWrite)?;
        temporary_file.sync_all()?;
        hook(AtomicReplaceStage::AfterFileSync)?;
        drop(temporary_file);

        fs::rename(&temporary_path, path)?;
        temporary_owned = false;
        hook(AtomicReplaceStage::AfterRename).map_err(|source| {
            DurableStateError::CommitIndeterminate {
                path: path.to_path_buf(),
                source,
            }
        })?;

        let parent_directory =
            File::open(parent).map_err(|source| DurableStateError::CommitIndeterminate {
                path: path.to_path_buf(),
                source,
            })?;
        parent_directory
            .sync_all()
            .map_err(|source| DurableStateError::CommitIndeterminate {
                path: path.to_path_buf(),
                source,
            })?;
        hook(AtomicReplaceStage::AfterDirectorySync).map_err(|source| {
            DurableStateError::CommitIndeterminate {
                path: path.to_path_buf(),
                source,
            }
        })?;
        Ok(())
    })();

    if temporary_owned {
        match fs::remove_file(&temporary_path) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(_) => {}
        }
    }
    result
}

fn create_temporary(parent: &Path, file_name: &std::ffi::OsStr) -> io::Result<(PathBuf, File)> {
    for _ in 0..TEMP_ATTEMPTS {
        let id = NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed);
        let name = format!(
            ".{}.distd-tmp-{}-{id}",
            file_name.to_string_lossy(),
            std::process::id()
        );
        let path = parent.join(name);
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        match options.open(&path) {
            Ok(file) => return Ok((path, file)),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        }
    }
    Err(io::Error::new(
        io::ErrorKind::AlreadyExists,
        "could not reserve a unique durable-state temporary file",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{collections::HashSet, sync::atomic::AtomicU64};

    static NEXT_TEST_DIRECTORY: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn golden_envelope_is_stable() {
        let encoded = encode_envelope(DurableStateKind::FsStorage, 7, b"state", 64).unwrap();
        assert_eq!(
            hex(&encoded),
            "444953544453544101000100070000000500000000000000\
             179fc9a22a3d3951e9442dfd50021f75481a4a1771e93ca8042e4e8a243c3a55\
             7374617465"
                .replace(' ', "")
        );
        assert_eq!(
            decode_envelope(&encoded, DurableStateKind::FsStorage, 7, 64).unwrap(),
            b"state"
        );
    }

    #[test]
    fn rejects_every_invalid_frame_class() {
        let valid = encode_envelope(DurableStateKind::FsStorage, 7, b"state", 64).unwrap();

        assert!(matches!(
            decode_envelope(&valid[..4], DurableStateKind::FsStorage, 7, 64),
            Err(DurableStateError::Truncated { .. })
        ));
        let mut bytes = valid.clone();
        bytes[0] ^= 1;
        assert!(matches!(
            decode_envelope(&bytes, DurableStateKind::FsStorage, 7, 64),
            Err(DurableStateError::BadMagic)
        ));
        let mut bytes = valid.clone();
        bytes[8..10].copy_from_slice(&2_u16.to_le_bytes());
        assert!(matches!(
            decode_envelope(&bytes, DurableStateKind::FsStorage, 7, 64),
            Err(DurableStateError::WrongKind { .. })
        ));
        let mut bytes = valid.clone();
        bytes[10..12].copy_from_slice(&2_u16.to_le_bytes());
        assert!(matches!(
            decode_envelope(&bytes, DurableStateKind::FsStorage, 7, 64),
            Err(DurableStateError::UnsupportedEnvelopeVersion { .. })
        ));
        assert!(matches!(
            decode_envelope(&valid, DurableStateKind::FsStorage, 8, 64),
            Err(DurableStateError::UnsupportedPayloadSchema { .. })
        ));
        let mut bytes = valid.clone();
        bytes[14..16].copy_from_slice(&1_u16.to_le_bytes());
        assert!(matches!(
            decode_envelope(&bytes, DurableStateKind::FsStorage, 7, 64),
            Err(DurableStateError::UnsupportedFlags { .. })
        ));
        let mut bytes = valid.clone();
        bytes[16..24].copy_from_slice(&65_u64.to_le_bytes());
        assert!(matches!(
            decode_envelope(&bytes, DurableStateKind::FsStorage, 7, 64),
            Err(DurableStateError::PayloadTooLarge { .. })
        ));
        let mut bytes = valid.clone();
        bytes[16..24].copy_from_slice(&u64::MAX.to_le_bytes());
        assert!(matches!(
            decode_envelope(&bytes, DurableStateKind::FsStorage, 7, u64::MAX),
            Err(DurableStateError::LengthOverflow)
        ));
        assert!(matches!(
            decode_envelope(
                &valid[..valid.len() - 1],
                DurableStateKind::FsStorage,
                7,
                64
            ),
            Err(DurableStateError::Truncated { .. })
        ));
        let mut bytes = valid.clone();
        bytes.push(0);
        assert!(matches!(
            decode_envelope(&bytes, DurableStateKind::FsStorage, 7, 64),
            Err(DurableStateError::TrailingBytes { .. })
        ));
        let mut bytes = valid.clone();
        bytes[HEADER_SIZE] ^= 1;
        assert!(matches!(
            decode_envelope(&bytes, DurableStateKind::FsStorage, 7, 64),
            Err(DurableStateError::IntegrityMismatch)
        ));
    }

    #[test]
    fn encode_rejects_oversize_payload() {
        assert!(matches!(
            encode_envelope(DurableStateKind::FsStorage, 1, b"too long", 2),
            Err(DurableStateError::PayloadTooLarge { .. })
        ));
    }

    #[test]
    fn bounded_reader_rejects_size_before_reading_payload() {
        let directory = test_directory();
        let path = directory.join("state.bin");
        fs::write(&path, [0_u8; 65]).unwrap();
        assert!(matches!(
            read_bounded_file(&path, 64),
            Err(DurableStateError::FileTooLarge {
                actual: 65,
                maximum: 64
            })
        ));
        fs::remove_dir_all(directory).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn bounded_reader_rejects_symlinks_and_directories() {
        use std::os::unix::fs::symlink;

        let directory = test_directory();
        let target = directory.join("target");
        let link = directory.join("link");
        fs::write(&target, b"state").unwrap();
        symlink(&target, &link).unwrap();
        assert!(matches!(
            read_bounded_file(&link, 64),
            Err(DurableStateError::Io(error))
                if error.kind() == io::ErrorKind::InvalidInput
        ));
        assert!(matches!(
            read_bounded_file(&directory, 64),
            Err(DurableStateError::Io(error))
                if error.kind() == io::ErrorKind::InvalidInput
        ));
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn enveloped_reader_returns_only_validated_payload() {
        let directory = test_directory();
        let path = directory.join("state.bin");
        replace_enveloped_state(&path, DurableStateKind::FsStorage, 3, b"new", 64).unwrap();
        assert_eq!(
            read_enveloped_state(&path, DurableStateKind::FsStorage, 3, 64).unwrap(),
            b"new"
        );
        assert!(matches!(
            read_enveloped_state(&path, DurableStateKind::FsStorage, 4, 64),
            Err(DurableStateError::UnsupportedPayloadSchema { .. })
        ));
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn every_fault_boundary_leaves_old_or_complete_new_state() {
        let stages = [
            AtomicReplaceStage::AfterEncode,
            AtomicReplaceStage::AfterCreate,
            AtomicReplaceStage::AfterWrite,
            AtomicReplaceStage::AfterFileSync,
            AtomicReplaceStage::AfterRename,
            AtomicReplaceStage::AfterDirectorySync,
        ];
        for fault_stage in stages {
            let directory = test_directory();
            let path = directory.join("state.bin");
            fs::write(&path, b"old").unwrap();
            let encoded = encode_envelope(DurableStateKind::FsStorage, 1, b"new", 64).unwrap();
            let error = atomic_replace(&path, &encoded, &mut |stage| {
                if stage == fault_stage {
                    Err(io::Error::other("injected fault"))
                } else {
                    Ok(())
                }
            })
            .unwrap_err();

            let actual = fs::read(&path).unwrap();
            if matches!(
                fault_stage,
                AtomicReplaceStage::AfterRename | AtomicReplaceStage::AfterDirectorySync
            ) {
                assert_eq!(actual, encoded, "stage {fault_stage:?}: {error}");
                assert!(matches!(
                    error,
                    DurableStateError::CommitIndeterminate { .. }
                ));
            } else {
                assert_eq!(actual, b"old", "stage {fault_stage:?}: {error}");
            }
            let names: HashSet<_> = fs::read_dir(&directory)
                .unwrap()
                .map(|entry| entry.unwrap().file_name())
                .collect();
            assert_eq!(names, HashSet::from(["state.bin".into()]));
            fs::remove_dir_all(directory).unwrap();
        }
    }

    #[test]
    fn successful_replace_is_complete_and_decodable() {
        let directory = test_directory();
        let path = directory.join("state.bin");
        fs::write(&path, b"old").unwrap();
        replace_enveloped_state(&path, DurableStateKind::FsStorage, 3, b"new", 64).unwrap();
        let encoded = fs::read(&path).unwrap();
        assert_eq!(
            decode_envelope(&encoded, DurableStateKind::FsStorage, 3, 64).unwrap(),
            b"new"
        );
        fs::remove_dir_all(directory).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn replacement_files_are_owner_only() {
        use std::os::unix::fs::PermissionsExt;

        let directory = test_directory();
        let path = directory.join("state.bin");
        replace_enveloped_state(&path, DurableStateKind::FsStorage, 1, b"state", 64).unwrap();
        assert_eq!(
            fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        fs::remove_dir_all(directory).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn replacement_does_not_follow_destination_symlink() {
        use std::os::unix::fs::symlink;

        let directory = test_directory();
        let target = directory.join("unrelated.bin");
        let path = directory.join("state.bin");
        fs::write(&target, b"unrelated").unwrap();
        symlink(&target, &path).unwrap();

        replace_enveloped_state(&path, DurableStateKind::FsStorage, 1, b"state", 64).unwrap();

        assert_eq!(fs::read(&target).unwrap(), b"unrelated");
        assert!(!fs::symlink_metadata(&path)
            .unwrap()
            .file_type()
            .is_symlink());
        let encoded = fs::read(&path).unwrap();
        assert_eq!(
            decode_envelope(&encoded, DurableStateKind::FsStorage, 1, 64).unwrap(),
            b"state"
        );
        fs::remove_dir_all(directory).unwrap();
    }

    fn test_directory() -> PathBuf {
        let id = NEXT_TEST_DIRECTORY.fetch_add(1, Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("distd-durable-state-{}-{id}", std::process::id()));
        fs::create_dir(&path).unwrap();
        path
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }
}

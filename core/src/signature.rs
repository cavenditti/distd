//! Canonical publisher statements and detached Ed25519 signature envelopes.

use std::{fmt, str::FromStr};

use ring::signature;
use thiserror::Error;
use unicode_normalization::UnicodeNormalization;

use crate::{
    chunk_storage::validate_revision_metadata,
    chunks::{ChunkAlgorithm, ChunkInfo},
    hash::{hash, merge_hashes, Hash},
    item::{FileEntry, Item, Manifest},
    utils::frame::MAX_FRAME_PAYLOAD_BYTES,
};

const STATEMENT_DOMAIN: &[u8] = b"distd publisher statement v1\0";
const ENVELOPE_DOMAIN: &[u8] = b"distd publisher signature envelope v1\0";
const FINGERPRINT_DOMAIN: &[u8] = b"distd publisher public key fingerprint v1\0";
const FORMAT_VERSION: u16 = 1;
const MAX_ARTIFACT_ID_BYTES: usize = 256;
const MAX_ENTRY_PATH_BYTES: usize = 4_096;
const MAX_CHUNKS: usize = 65_536;
const MAX_ENTRIES: usize = 65_536;
const PUBLIC_KEY_BYTES: usize = 32;
const SIGNATURE_BYTES: usize = 64;
const ENVELOPE_BYTES: usize = ENVELOPE_DOMAIN.len() + 2 + 2 + PUBLIC_KEY_BYTES + SIGNATURE_BYTES;

/// Errors produced by the canonical publisher statement and signature format.
#[non_exhaustive]
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum SignatureEnvelopeError {
    #[error("publisher statement field {field} exceeds its limit of {limit}")]
    StatementLimit { field: &'static str, limit: usize },
    #[error("invalid publisher statement: {0}")]
    InvalidStatement(&'static str),
    #[error("publisher signature encoding is truncated")]
    Truncated,
    #[error("publisher signature encoding contains trailing bytes")]
    TrailingBytes,
    #[error("publisher signature encoding has the wrong domain")]
    InvalidDomain,
    #[error("unsupported publisher statement version {0}")]
    UnsupportedStatementVersion(u16),
    #[error("unsupported publisher signature envelope version {0}")]
    UnsupportedEnvelopeVersion(u16),
    #[error("invalid Ed25519 public-key length {0}; expected 32")]
    InvalidPublicKeyLength(usize),
    #[error("invalid Ed25519 signature length {0}; expected 64")]
    InvalidSignatureLength(usize),
    #[error("invalid publisher public key")]
    InvalidPublicKey,
    #[error("publisher signature verification failed")]
    InvalidSignature,
    #[error("invalid publisher fingerprint")]
    InvalidFingerprint,
}

/// An exact raw Ed25519 publisher public key.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct PublisherPublicKey([u8; PUBLIC_KEY_BYTES]);

impl PublisherPublicKey {
    #[must_use]
    pub const fn from_bytes(bytes: [u8; PUBLIC_KEY_BYTES]) -> Self {
        Self(bytes)
    }

    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; PUBLIC_KEY_BYTES] {
        &self.0
    }

    #[must_use]
    pub fn fingerprint(&self) -> PublisherKeyFingerprint {
        let mut hasher = blake3::Hasher::new();
        hasher.update(FINGERPRINT_DOMAIN);
        hasher.update(&self.0);
        PublisherKeyFingerprint(*hasher.finalize().as_bytes())
    }
}

impl TryFrom<&[u8]> for PublisherPublicKey {
    type Error = SignatureEnvelopeError;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        Ok(Self(bytes.try_into().map_err(|_| {
            SignatureEnvelopeError::InvalidPublicKeyLength(bytes.len())
        })?))
    }
}

/// An exact raw detached Ed25519 signature.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PublisherSignature([u8; SIGNATURE_BYTES]);

impl PublisherSignature {
    #[must_use]
    pub const fn from_bytes(bytes: [u8; SIGNATURE_BYTES]) -> Self {
        Self(bytes)
    }

    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; SIGNATURE_BYTES] {
        &self.0
    }
}

impl TryFrom<&[u8]> for PublisherSignature {
    type Error = SignatureEnvelopeError;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        Ok(Self(bytes.try_into().map_err(|_| {
            SignatureEnvelopeError::InvalidSignatureLength(bytes.len())
        })?))
    }
}

/// A domain-separated BLAKE3 fingerprint of a publisher public key.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PublisherKeyFingerprint([u8; 32]);

impl PublisherKeyFingerprint {
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Display for PublisherKeyFingerprint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl FromStr for PublisherKeyFingerprint {
    type Err = SignatureEnvelopeError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.len() != 64
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
        {
            return Err(SignatureEnvelopeError::InvalidFingerprint);
        }
        let mut bytes = [0u8; 32];
        for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
            bytes[index] = hex_nibble(pair[0])? << 4 | hex_nibble(pair[1])?;
        }
        Ok(Self(bytes))
    }
}

/// A fully decoded and validated canonical publisher statement.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublisherStatement {
    bytes: Vec<u8>,
}

impl PublisherStatement {
    /// Construct the canonical statement from a typed sync manifest and its
    /// ordered chunk identities.
    pub fn from_manifest(
        manifest: &Manifest,
        chunks: &[ChunkInfo],
    ) -> Result<Self, SignatureEnvelopeError> {
        Self::from_parts(manifest, chunks)
    }

    /// Construct the canonical statement from an authoritative typed item.
    pub fn from_item(item: &Item) -> Result<Self, SignatureEnvelopeError> {
        validate_revision_metadata(item)
            .map_err(|_| SignatureEnvelopeError::InvalidStatement("invalid item metadata"))?;
        let encoded_size = validate_manifest_after_valid_item(&item.manifest, &item.chunks)?;
        Self::encode_validated(&item.manifest, &item.chunks, encoded_size)
    }

    /// Decode exact canonical statement bytes for fixtures and external tooling.
    pub fn decode(bytes: &[u8]) -> Result<Self, SignatureEnvelopeError> {
        if bytes.len() > MAX_FRAME_PAYLOAD_BYTES {
            return Err(SignatureEnvelopeError::StatementLimit {
                field: "statement bytes",
                limit: MAX_FRAME_PAYLOAD_BYTES,
            });
        }
        let mut reader = Reader::new(bytes);
        reader.expect_domain(STATEMENT_DOMAIN)?;
        let version = reader.u16()?;
        if version != FORMAT_VERSION {
            return Err(SignatureEnvelopeError::UnsupportedStatementVersion(version));
        }
        let body_length = usize::try_from(reader.u32()?)
            .map_err(|_| SignatureEnvelopeError::InvalidStatement("body length overflows usize"))?;
        if body_length != reader.remaining() {
            return Err(if body_length > reader.remaining() {
                SignatureEnvelopeError::Truncated
            } else {
                SignatureEnvelopeError::TrailingBytes
            });
        }

        let artifact_id = reader.utf8_u16("artifact ID", MAX_ARTIFACT_ID_BYTES)?;
        let root_hash = Hash::from_bytes(reader.array()?);
        let total_size = reader.u64()?;
        let chunk_algorithm = decode_algorithm(&mut reader)?;
        let chunk_count = reader.bounded_count("chunks", MAX_CHUNKS, 40)?;
        if chunk_count == 0 {
            return Err(SignatureEnvelopeError::InvalidStatement(
                "chunk list is empty",
            ));
        }
        let mut chunks = Vec::with_capacity(chunk_count);
        for _ in 0..chunk_count {
            chunks.push(ChunkInfo {
                hash: Hash::from_bytes(reader.array()?),
                size: reader.u64()?,
            });
        }
        let entry_count = reader.bounded_count("entries", MAX_ENTRIES, 18)?;
        let mut entries = Vec::with_capacity(entry_count);
        for _ in 0..entry_count {
            entries.push(FileEntry {
                relative_path: reader.utf8_u16("entry path", MAX_ENTRY_PATH_BYTES)?,
                size: reader.u64()?,
                chunk_range: (reader.u32()?, reader.u32()?),
            });
        }
        if reader.remaining() != 0 {
            return Err(SignatureEnvelopeError::TrailingBytes);
        }

        let manifest = Manifest {
            artifact_id,
            version: 0,
            root_hash,
            total_size,
            chunk_count: u32::try_from(chunks.len()).expect("bounded chunk count fits u32"),
            chunk_size: chunk_algorithm.nominal_chunk_size(),
            chunk_algorithm,
            entries,
        };
        validate_manifest(&manifest, &chunks)?;
        drop(manifest);
        drop(chunks);
        Ok(Self {
            bytes: bytes.to_vec(),
        })
    }

    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    #[must_use]
    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }

    fn from_parts(
        manifest: &Manifest,
        chunks: &[ChunkInfo],
    ) -> Result<Self, SignatureEnvelopeError> {
        let encoded_size = validate_manifest(manifest, chunks)?;
        Self::encode_validated(manifest, chunks, encoded_size)
    }

    fn encode_validated(
        manifest: &Manifest,
        chunks: &[ChunkInfo],
        encoded_size: usize,
    ) -> Result<Self, SignatureEnvelopeError> {
        let mut bytes = Vec::with_capacity(encoded_size);
        bytes.extend_from_slice(STATEMENT_DOMAIN);
        push_u16(&mut bytes, FORMAT_VERSION);
        let body_length_offset = bytes.len();
        push_u32(&mut bytes, 0);
        let body_start = bytes.len();
        push_string_u16(&mut bytes, &manifest.artifact_id)?;
        bytes.extend_from_slice(manifest.root_hash.as_bytes());
        push_u64(&mut bytes, manifest.total_size);
        encode_algorithm(&mut bytes, manifest.chunk_algorithm);
        push_u32(
            &mut bytes,
            u32::try_from(chunks.len()).expect("validated chunk count fits u32"),
        );
        for chunk in chunks {
            bytes.extend_from_slice(chunk.hash.as_bytes());
            push_u64(&mut bytes, chunk.size);
        }
        push_u32(
            &mut bytes,
            u32::try_from(manifest.entries.len()).expect("validated entry count fits u32"),
        );
        for entry in &manifest.entries {
            push_string_u16(&mut bytes, &entry.relative_path)?;
            push_u64(&mut bytes, entry.size);
            push_u32(&mut bytes, entry.chunk_range.0);
            push_u32(&mut bytes, entry.chunk_range.1);
        }
        if bytes.len() > MAX_FRAME_PAYLOAD_BYTES {
            return Err(SignatureEnvelopeError::StatementLimit {
                field: "statement bytes",
                limit: MAX_FRAME_PAYLOAD_BYTES,
            });
        }
        let body_length = u32::try_from(bytes.len() - body_start).map_err(|_| {
            SignatureEnvelopeError::StatementLimit {
                field: "statement bytes",
                limit: MAX_FRAME_PAYLOAD_BYTES,
            }
        })?;
        bytes[body_length_offset..body_length_offset + 4]
            .copy_from_slice(&body_length.to_le_bytes());
        Ok(Self { bytes })
    }
}

/// A fixed-width detached Ed25519 signature envelope.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PublisherSignatureEnvelope {
    public_key: PublisherPublicKey,
    signature: PublisherSignature,
}

impl PublisherSignatureEnvelope {
    #[must_use]
    pub const fn from_parts(public_key: PublisherPublicKey, signature: PublisherSignature) -> Self {
        Self {
            public_key,
            signature,
        }
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, SignatureEnvelopeError> {
        let mut reader = Reader::new(bytes);
        reader.expect_domain(ENVELOPE_DOMAIN)?;
        let envelope_version = reader.u16()?;
        if envelope_version != FORMAT_VERSION {
            return Err(SignatureEnvelopeError::UnsupportedEnvelopeVersion(
                envelope_version,
            ));
        }
        let statement_version = reader.u16()?;
        if statement_version != FORMAT_VERSION {
            return Err(SignatureEnvelopeError::UnsupportedStatementVersion(
                statement_version,
            ));
        }
        let public_key = PublisherPublicKey::from_bytes(reader.array()?);
        let signature = PublisherSignature::from_bytes(reader.array()?);
        if reader.remaining() != 0 {
            return Err(SignatureEnvelopeError::TrailingBytes);
        }
        Ok(Self::from_parts(public_key, signature))
    }

    #[must_use]
    pub fn encode(&self) -> [u8; ENVELOPE_BYTES] {
        let mut bytes = [0u8; ENVELOPE_BYTES];
        let mut offset = 0;
        copy_to_array(&mut bytes, &mut offset, ENVELOPE_DOMAIN);
        copy_to_array(&mut bytes, &mut offset, &FORMAT_VERSION.to_le_bytes());
        copy_to_array(&mut bytes, &mut offset, &FORMAT_VERSION.to_le_bytes());
        copy_to_array(&mut bytes, &mut offset, self.public_key.as_bytes());
        copy_to_array(&mut bytes, &mut offset, self.signature.as_bytes());
        bytes
    }

    #[must_use]
    pub const fn public_key(&self) -> PublisherPublicKey {
        self.public_key
    }

    #[must_use]
    pub const fn signature(&self) -> PublisherSignature {
        self.signature
    }

    #[must_use]
    pub fn publisher_fingerprint(&self) -> PublisherKeyFingerprint {
        self.public_key.fingerprint()
    }

    /// Reconstruct and verify the statement from a fully validated typed item.
    pub fn verify(&self, item: &Item) -> Result<PublisherKeyFingerprint, SignatureEnvelopeError> {
        let statement = PublisherStatement::from_item(item)?;
        signature::UnparsedPublicKey::new(&signature::ED25519, self.public_key.as_bytes())
            .verify(statement.as_bytes(), self.signature.as_bytes())
            .map_err(|_| SignatureEnvelopeError::InvalidSignature)?;
        Ok(self.publisher_fingerprint())
    }

    /// Validate and verify a typed sync manifest before chunk negotiation.
    pub fn verify_manifest(
        &self,
        manifest: &Manifest,
        chunks: &[ChunkInfo],
    ) -> Result<PublisherKeyFingerprint, SignatureEnvelopeError> {
        let statement = PublisherStatement::from_manifest(manifest, chunks)?;
        signature::UnparsedPublicKey::new(&signature::ED25519, self.public_key.as_bytes())
            .verify(statement.as_bytes(), self.signature.as_bytes())
            .map_err(|_| SignatureEnvelopeError::InvalidSignature)?;
        Ok(self.publisher_fingerprint())
    }
}

impl TryFrom<&[u8]> for PublisherSignatureEnvelope {
    type Error = SignatureEnvelopeError;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        Self::decode(bytes)
    }
}

fn validate_manifest(
    manifest: &Manifest,
    chunks: &[ChunkInfo],
) -> Result<usize, SignatureEnvelopeError> {
    validate_nfc(&manifest.artifact_id, "artifact ID")?;
    if manifest.artifact_id.is_empty() {
        return Err(SignatureEnvelopeError::InvalidStatement(
            "artifact ID is empty",
        ));
    }
    if manifest.artifact_id.len() > MAX_ARTIFACT_ID_BYTES {
        return Err(SignatureEnvelopeError::StatementLimit {
            field: "artifact ID",
            limit: MAX_ARTIFACT_ID_BYTES,
        });
    }
    if chunks.is_empty() {
        return Err(SignatureEnvelopeError::InvalidStatement(
            "chunk list is empty",
        ));
    }
    if chunks.len() > MAX_CHUNKS {
        return Err(SignatureEnvelopeError::StatementLimit {
            field: "chunks",
            limit: MAX_CHUNKS,
        });
    }
    if manifest.entries.len() > MAX_ENTRIES {
        return Err(SignatureEnvelopeError::StatementLimit {
            field: "entries",
            limit: MAX_ENTRIES,
        });
    }
    if usize::try_from(manifest.chunk_count).ok() != Some(chunks.len()) {
        return Err(SignatureEnvelopeError::InvalidStatement(
            "manifest chunk count differs from chunk list",
        ));
    }
    validate_algorithm(manifest.chunk_algorithm)?;
    if manifest.chunk_size != manifest.chunk_algorithm.nominal_chunk_size() {
        return Err(SignatureEnvelopeError::InvalidStatement(
            "manifest chunk size differs from algorithm",
        ));
    }
    let root = reconstruct_root_validated(chunks)?;
    if root.hash != manifest.root_hash || root.size != manifest.total_size {
        return Err(SignatureEnvelopeError::InvalidStatement(
            "chunks do not reconstruct declared root and size",
        ));
    }
    let encoded_entry_bytes = validate_entries(&manifest.entries, chunks)?;
    estimated_statement_size(manifest, chunks, encoded_entry_bytes)
}

fn validate_manifest_after_valid_item(
    manifest: &Manifest,
    chunks: &[ChunkInfo],
) -> Result<usize, SignatureEnvelopeError> {
    // `validate_revision_metadata` already proved the redundant Item identity,
    // chunk tree/root, entry ranges, sizes, coverage, and nominal chunk size.
    // Keep only signing-format constraints here so Item verification does not
    // reconstruct the same potentially large tree twice.
    validate_nfc(&manifest.artifact_id, "artifact ID")?;
    if chunks.len() > MAX_CHUNKS {
        return Err(SignatureEnvelopeError::StatementLimit {
            field: "chunks",
            limit: MAX_CHUNKS,
        });
    }
    if manifest.entries.len() > MAX_ENTRIES {
        return Err(SignatureEnvelopeError::StatementLimit {
            field: "entries",
            limit: MAX_ENTRIES,
        });
    }
    validate_algorithm(manifest.chunk_algorithm)?;

    let empty_hash = hash(&[]);
    for chunk in chunks {
        if chunk.size > u64::from(u32::MAX) {
            return Err(SignatureEnvelopeError::InvalidStatement(
                "chunk size exceeds u32 transport limit",
            ));
        }
        if chunk.size == 0 && chunk.hash != empty_hash {
            return Err(SignatureEnvelopeError::InvalidStatement(
                "zero-size chunk has the wrong hash",
            ));
        }
    }

    let mut previous: Option<&str> = None;
    let mut encoded_entry_bytes = 0usize;
    if manifest.entries.is_empty() {
        if chunks.iter().any(|chunk| chunk.size == 0) && !(chunks.len() == 1 && chunks[0].size == 0)
        {
            return Err(SignatureEnvelopeError::InvalidStatement(
                "single-file artifact has an invalid zero-size chunk",
            ));
        }
    } else {
        for entry in &manifest.entries {
            validate_entry_path(&entry.relative_path)?;
            encoded_entry_bytes = encoded_entry_bytes
                .checked_add(2 + entry.relative_path.len() + 8 + 4 + 4)
                .ok_or(SignatureEnvelopeError::StatementLimit {
                    field: "statement bytes",
                    limit: MAX_FRAME_PAYLOAD_BYTES,
                })?;
            if let Some(previous) = previous {
                if previous.as_bytes() >= entry.relative_path.as_bytes() {
                    return Err(SignatureEnvelopeError::InvalidStatement(
                        "entry paths are not strictly ordered",
                    ));
                }
                if entry.relative_path.starts_with(previous)
                    && entry.relative_path.as_bytes().get(previous.len()) == Some(&b'/')
                {
                    return Err(SignatureEnvelopeError::InvalidStatement(
                        "entry path conflicts with ancestor file",
                    ));
                }
            }
            previous = Some(&entry.relative_path);
            let start = usize::try_from(entry.chunk_range.0)
                .expect("validate_revision_metadata proved entry start fits usize");
            let end = usize::try_from(entry.chunk_range.1)
                .expect("validate_revision_metadata proved entry end fits usize");
            let entry_chunks = &chunks[start..end];
            if entry.size == 0 {
                if entry_chunks.len() != 1 || entry_chunks[0].size != 0 {
                    return Err(SignatureEnvelopeError::InvalidStatement(
                        "empty entry must cover exactly one empty chunk",
                    ));
                }
            } else if entry_chunks.iter().any(|chunk| chunk.size == 0) {
                return Err(SignatureEnvelopeError::InvalidStatement(
                    "nonempty entry contains an empty chunk",
                ));
            }
        }
    }
    estimated_statement_size(manifest, chunks, encoded_entry_bytes)
}

fn validate_entries(
    entries: &[FileEntry],
    chunks: &[ChunkInfo],
) -> Result<usize, SignatureEnvelopeError> {
    if entries.is_empty() {
        if chunks.iter().any(|chunk| chunk.size == 0) && !(chunks.len() == 1 && chunks[0].size == 0)
        {
            return Err(SignatureEnvelopeError::InvalidStatement(
                "single-file artifact has an invalid zero-size chunk",
            ));
        }
        return Ok(0);
    }
    let mut next = 0usize;
    let mut previous: Option<&str> = None;
    let mut encoded_size = 0usize;
    for entry in entries {
        validate_entry_path(&entry.relative_path)?;
        encoded_size = encoded_size
            .checked_add(2 + entry.relative_path.len() + 8 + 4 + 4)
            .ok_or(SignatureEnvelopeError::StatementLimit {
                field: "statement bytes",
                limit: MAX_FRAME_PAYLOAD_BYTES,
            })?;
        if let Some(previous) = previous {
            if previous.as_bytes() >= entry.relative_path.as_bytes() {
                return Err(SignatureEnvelopeError::InvalidStatement(
                    "entry paths are not strictly ordered",
                ));
            }
            if entry.relative_path.starts_with(previous)
                && entry.relative_path.as_bytes().get(previous.len()) == Some(&b'/')
            {
                return Err(SignatureEnvelopeError::InvalidStatement(
                    "entry path conflicts with ancestor file",
                ));
            }
        }
        previous = Some(&entry.relative_path);
        let start = usize::try_from(entry.chunk_range.0)
            .map_err(|_| SignatureEnvelopeError::InvalidStatement("entry start overflows"))?;
        let end = usize::try_from(entry.chunk_range.1)
            .map_err(|_| SignatureEnvelopeError::InvalidStatement("entry end overflows"))?;
        if start != next || end <= start || end > chunks.len() {
            return Err(SignatureEnvelopeError::InvalidStatement(
                "entry chunk range is not contiguous and in bounds",
            ));
        }
        let entry_chunks = &chunks[start..end];
        let mut size = 0u64;
        let mut contains_empty = false;
        for chunk in entry_chunks {
            size = size
                .checked_add(chunk.size)
                .ok_or(SignatureEnvelopeError::InvalidStatement(
                    "entry size overflows u64",
                ))?;
            contains_empty |= chunk.size == 0;
        }
        if size != entry.size {
            return Err(SignatureEnvelopeError::InvalidStatement(
                "entry size differs from chunks",
            ));
        }
        if entry.size == 0 {
            if entry_chunks.len() != 1 || entry_chunks[0].size != 0 {
                return Err(SignatureEnvelopeError::InvalidStatement(
                    "empty entry must cover exactly one empty chunk",
                ));
            }
        } else if contains_empty {
            return Err(SignatureEnvelopeError::InvalidStatement(
                "nonempty entry contains an empty chunk",
            ));
        }
        next = end;
    }
    if next != chunks.len() {
        return Err(SignatureEnvelopeError::InvalidStatement(
            "entries do not cover all chunks",
        ));
    }
    Ok(encoded_size)
}

fn validate_entry_path(path: &str) -> Result<(), SignatureEnvelopeError> {
    if path.is_empty() || path.len() > MAX_ENTRY_PATH_BYTES {
        return Err(if path.len() > MAX_ENTRY_PATH_BYTES {
            SignatureEnvelopeError::StatementLimit {
                field: "entry path",
                limit: MAX_ENTRY_PATH_BYTES,
            }
        } else {
            SignatureEnvelopeError::InvalidStatement("entry path is not canonical")
        });
    }

    let bytes = path.as_bytes();
    let mut component_start = 0usize;
    let mut first_component_end = bytes.len();
    let mut non_ascii = false;
    for (index, byte) in bytes.iter().copied().enumerate() {
        non_ascii |= !byte.is_ascii();
        if matches!(byte, b'\\' | 0) {
            return Err(SignatureEnvelopeError::InvalidStatement(
                "entry path is not canonical",
            ));
        }
        if byte == b'/' {
            if component_start == index
                || &bytes[component_start..index] == b"."
                || &bytes[component_start..index] == b".."
            {
                return Err(SignatureEnvelopeError::InvalidStatement(
                    "entry path is not canonical",
                ));
            }
            if component_start == 0 {
                first_component_end = index;
            }
            component_start = index + 1;
        }
    }
    if component_start == bytes.len()
        || &bytes[component_start..] == b"."
        || &bytes[component_start..] == b".."
    {
        return Err(SignatureEnvelopeError::InvalidStatement(
            "entry path is not canonical",
        ));
    }
    if matches!(
        &bytes[..first_component_end],
        b".distd-revisions-v1" | b".distd-activation-v1"
    ) {
        return Err(SignatureEnvelopeError::InvalidStatement(
            "entry path uses reserved namespace",
        ));
    }
    if non_ascii {
        validate_nfc(path, "entry path")?;
    }
    Ok(())
}

fn validate_nfc(value: &str, field: &'static str) -> Result<(), SignatureEnvelopeError> {
    if value.is_ascii() || value.nfc().eq(value.chars()) {
        Ok(())
    } else {
        Err(SignatureEnvelopeError::InvalidStatement(match field {
            "artifact ID" => "artifact ID is not NFC",
            _ => "entry path is not NFC",
        }))
    }
}

fn validate_algorithm(algorithm: ChunkAlgorithm) -> Result<(), SignatureEnvelopeError> {
    if algorithm.is_canonical_publisher_profile() {
        Ok(())
    } else {
        Err(SignatureEnvelopeError::InvalidStatement(
            "invalid or unsupported chunk algorithm profile",
        ))
    }
}

#[cfg(test)]
fn reconstruct_root(chunks: &[ChunkInfo]) -> Result<ChunkInfo, SignatureEnvelopeError> {
    reconstruct_root_inner(chunks, false)
}

fn reconstruct_root_validated(chunks: &[ChunkInfo]) -> Result<ChunkInfo, SignatureEnvelopeError> {
    reconstruct_root_inner(chunks, true)
}

fn reconstruct_root_inner(
    chunks: &[ChunkInfo],
    validate_chunks: bool,
) -> Result<ChunkInfo, SignatureEnvelopeError> {
    fn merge(left: ChunkInfo, right: ChunkInfo) -> Result<ChunkInfo, SignatureEnvelopeError> {
        Ok(ChunkInfo {
            hash: merge_hashes(&left.hash, &right.hash),
            size: left.size.checked_add(right.size).ok_or(
                SignatureEnvelopeError::InvalidStatement("root size overflows u64"),
            )?,
        })
    }

    // A binary frontier retains at most one complete subtree at each height.
    // Folding the rightmost, lowest subtree into successively larger left
    // subtrees preserves the pair-and-carry tree shape used by the chunker.
    let mut frontier: Vec<Option<ChunkInfo>> = Vec::new();
    let empty_hash = validate_chunks.then(|| hash(&[]));
    for chunk in chunks.iter().copied() {
        if validate_chunks {
            if chunk.size > u64::from(u32::MAX) {
                return Err(SignatureEnvelopeError::InvalidStatement(
                    "chunk size exceeds u32 transport limit",
                ));
            }
            if chunk.size == 0 && Some(chunk.hash) != empty_hash {
                return Err(SignatureEnvelopeError::InvalidStatement(
                    "zero-size chunk has the wrong hash",
                ));
            }
        }
        let mut node = chunk;
        let mut level = 0usize;
        loop {
            if level == frontier.len() {
                frontier.push(Some(node));
                break;
            }
            match frontier[level].take() {
                Some(left) => {
                    node = merge(left, node)?;
                    level += 1;
                }
                None => {
                    frontier[level] = Some(node);
                    break;
                }
            }
        }
    }
    let mut root = None;
    for node in frontier.into_iter().flatten() {
        root = Some(match root {
            Some(right) => merge(node, right)?,
            None => node,
        });
    }
    root.ok_or(SignatureEnvelopeError::InvalidStatement(
        "chunk list is empty",
    ))
}

fn estimated_statement_size(
    manifest: &Manifest,
    chunks: &[ChunkInfo],
    encoded_entry_bytes: usize,
) -> Result<usize, SignatureEnvelopeError> {
    let mut size = STATEMENT_DOMAIN.len()
        + 2
        + 4
        + 2
        + manifest.artifact_id.len()
        + 32
        + 8
        + encoded_algorithm_size(manifest.chunk_algorithm)
        + 4;
    size = size
        .checked_add(chunks.len().checked_mul(40).ok_or(
            SignatureEnvelopeError::StatementLimit {
                field: "statement bytes",
                limit: MAX_FRAME_PAYLOAD_BYTES,
            },
        )?)
        .ok_or(SignatureEnvelopeError::StatementLimit {
            field: "statement bytes",
            limit: MAX_FRAME_PAYLOAD_BYTES,
        })?;
    size = size
        .checked_add(4)
        .ok_or(SignatureEnvelopeError::StatementLimit {
            field: "statement bytes",
            limit: MAX_FRAME_PAYLOAD_BYTES,
        })?;
    size = size
        .checked_add(encoded_entry_bytes)
        .ok_or(SignatureEnvelopeError::StatementLimit {
            field: "statement bytes",
            limit: MAX_FRAME_PAYLOAD_BYTES,
        })?;
    if size > MAX_FRAME_PAYLOAD_BYTES {
        return Err(SignatureEnvelopeError::StatementLimit {
            field: "statement bytes",
            limit: MAX_FRAME_PAYLOAD_BYTES,
        });
    }
    Ok(size)
}

const fn encoded_algorithm_size(algorithm: ChunkAlgorithm) -> usize {
    match algorithm {
        ChunkAlgorithm::Fixed256K => 1,
        ChunkAlgorithm::FixedAligned { .. } => 1 + 4 * 4,
        ChunkAlgorithm::FastCdc { .. } => 1 + 4 * 4 + 8 + 4,
        ChunkAlgorithm::BlockAlignedFastCdc { .. } | ChunkAlgorithm::ArchiveAwareFastCdc { .. } => {
            1 + 6 * 4 + 8 + 4
        }
    }
}

fn encode_algorithm(bytes: &mut Vec<u8>, algorithm: ChunkAlgorithm) {
    match algorithm {
        ChunkAlgorithm::Fixed256K => bytes.push(0),
        ChunkAlgorithm::FixedAligned {
            chunk_size,
            alignment,
            header_size,
            profile_version,
        } => {
            bytes.push(1);
            for value in [chunk_size, alignment, header_size, profile_version] {
                push_u32(bytes, value);
            }
        }
        ChunkAlgorithm::FastCdc {
            min_size,
            avg_size,
            max_size,
            normalization_level,
            seed,
            profile_version,
        } => {
            bytes.push(2);
            for value in [min_size, avg_size, max_size, normalization_level] {
                push_u32(bytes, value);
            }
            push_u64(bytes, seed);
            push_u32(bytes, profile_version);
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
            bytes.push(3);
            for value in [
                min_size,
                avg_size,
                max_size,
                alignment,
                zero_run_cutoff,
                normalization_level,
            ] {
                push_u32(bytes, value);
            }
            push_u64(bytes, seed);
            push_u32(bytes, profile_version);
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
            bytes.push(4);
            for value in [
                min_size,
                avg_size,
                max_size,
                record_size,
                alignment,
                normalization_level,
            ] {
                push_u32(bytes, value);
            }
            push_u64(bytes, seed);
            push_u32(bytes, profile_version);
        }
    }
}

fn decode_algorithm(reader: &mut Reader<'_>) -> Result<ChunkAlgorithm, SignatureEnvelopeError> {
    let algorithm = match reader.u8()? {
        0 => ChunkAlgorithm::Fixed256K,
        1 => ChunkAlgorithm::FixedAligned {
            chunk_size: reader.u32()?,
            alignment: reader.u32()?,
            header_size: reader.u32()?,
            profile_version: reader.u32()?,
        },
        2 => ChunkAlgorithm::FastCdc {
            min_size: reader.u32()?,
            avg_size: reader.u32()?,
            max_size: reader.u32()?,
            normalization_level: reader.u32()?,
            seed: reader.u64()?,
            profile_version: reader.u32()?,
        },
        3 => ChunkAlgorithm::BlockAlignedFastCdc {
            min_size: reader.u32()?,
            avg_size: reader.u32()?,
            max_size: reader.u32()?,
            alignment: reader.u32()?,
            zero_run_cutoff: reader.u32()?,
            normalization_level: reader.u32()?,
            seed: reader.u64()?,
            profile_version: reader.u32()?,
        },
        4 => ChunkAlgorithm::ArchiveAwareFastCdc {
            min_size: reader.u32()?,
            avg_size: reader.u32()?,
            max_size: reader.u32()?,
            record_size: reader.u32()?,
            alignment: reader.u32()?,
            normalization_level: reader.u32()?,
            seed: reader.u64()?,
            profile_version: reader.u32()?,
        },
        _ => {
            return Err(SignatureEnvelopeError::InvalidStatement(
                "unknown chunk algorithm tag",
            ))
        }
    };
    validate_algorithm(algorithm)?;
    Ok(algorithm)
}

struct Reader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Reader<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn remaining(&self) -> usize {
        self.bytes.len() - self.offset
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], SignatureEnvelopeError> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or(SignatureEnvelopeError::Truncated)?;
        let bytes = self
            .bytes
            .get(self.offset..end)
            .ok_or(SignatureEnvelopeError::Truncated)?;
        self.offset = end;
        Ok(bytes)
    }

    fn expect_domain(&mut self, domain: &[u8]) -> Result<(), SignatureEnvelopeError> {
        if self.take(domain.len())? == domain {
            Ok(())
        } else {
            Err(SignatureEnvelopeError::InvalidDomain)
        }
    }

    fn u8(&mut self) -> Result<u8, SignatureEnvelopeError> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16, SignatureEnvelopeError> {
        Ok(u16::from_le_bytes(self.array()?))
    }

    fn u32(&mut self) -> Result<u32, SignatureEnvelopeError> {
        Ok(u32::from_le_bytes(self.array()?))
    }

    fn u64(&mut self) -> Result<u64, SignatureEnvelopeError> {
        Ok(u64::from_le_bytes(self.array()?))
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], SignatureEnvelopeError> {
        self.take(N)?
            .try_into()
            .map_err(|_| SignatureEnvelopeError::Truncated)
    }

    fn utf8_u16(
        &mut self,
        field: &'static str,
        maximum: usize,
    ) -> Result<String, SignatureEnvelopeError> {
        let length = usize::from(self.u16()?);
        if length > maximum {
            return Err(SignatureEnvelopeError::StatementLimit {
                field,
                limit: maximum,
            });
        }
        let value = std::str::from_utf8(self.take(length)?)
            .map_err(|_| SignatureEnvelopeError::InvalidStatement("invalid UTF-8"))?;
        Ok(value.to_string())
    }

    fn bounded_count(
        &mut self,
        field: &'static str,
        maximum: usize,
        minimum_bytes: usize,
    ) -> Result<usize, SignatureEnvelopeError> {
        let count =
            usize::try_from(self.u32()?).map_err(|_| SignatureEnvelopeError::StatementLimit {
                field,
                limit: maximum,
            })?;
        if count > maximum {
            return Err(SignatureEnvelopeError::StatementLimit {
                field,
                limit: maximum,
            });
        }
        if count > self.remaining() / minimum_bytes {
            return Err(SignatureEnvelopeError::Truncated);
        }
        Ok(count)
    }
}

fn push_u16(bytes: &mut Vec<u8>, value: u16) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

fn push_u32(bytes: &mut Vec<u8>, value: u32) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

fn push_u64(bytes: &mut Vec<u8>, value: u64) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

fn push_string_u16(bytes: &mut Vec<u8>, value: &str) -> Result<(), SignatureEnvelopeError> {
    let length =
        u16::try_from(value.len()).map_err(|_| SignatureEnvelopeError::StatementLimit {
            field: "UTF-8 string",
            limit: usize::from(u16::MAX),
        })?;
    push_u16(bytes, length);
    bytes.extend_from_slice(value.as_bytes());
    Ok(())
}

fn copy_to_array<const N: usize>(target: &mut [u8; N], offset: &mut usize, value: &[u8]) {
    let end = *offset + value.len();
    target[*offset..end].copy_from_slice(value);
    *offset = end;
}

fn hex_nibble(byte: u8) -> Result<u8, SignatureEnvelopeError> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        _ => Err(SignatureEnvelopeError::InvalidFingerprint),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        chunks::ChunkingPolicy,
        item::{Format, Item},
        metadata::Item as ItemMetadata,
    };
    use ring::{rand::SystemRandom, signature::KeyPair};
    use std::{path::PathBuf, time::SystemTime};

    fn chunks_for(parts: &[&[u8]]) -> Vec<ChunkInfo> {
        parts
            .iter()
            .map(|bytes| ChunkInfo {
                hash: hash(bytes),
                size: bytes.len() as u64,
            })
            .collect()
    }

    fn item_for(parts: &[&[u8]], entries: Vec<FileEntry>, algorithm: ChunkAlgorithm) -> Item {
        let chunks = chunks_for(parts);
        let root = reconstruct_root(&chunks).unwrap();
        Item {
            metadata: ItemMetadata {
                artifact_id: "org.example/artifact".to_string(),
                name: "Artifact".to_string(),
                description: Some("description".to_string()),
                revision: 7,
                path: PathBuf::from("deploy/artifact"),
                root,
                created: SystemTime::UNIX_EPOCH,
                updated: SystemTime::UNIX_EPOCH,
                created_by: "fixture".to_string(),
                format: Format::V1,
            },
            manifest: Manifest {
                artifact_id: "org.example/artifact".to_string(),
                version: 7,
                root_hash: root.hash,
                total_size: root.size,
                chunk_count: chunks.len() as u32,
                chunk_size: algorithm.nominal_chunk_size(),
                chunk_algorithm: algorithm,
                entries,
            },
            chunks,
        }
    }

    fn signed(item: &Item) -> (PublisherSignatureEnvelope, ring::signature::Ed25519KeyPair) {
        let key = ring::signature::Ed25519KeyPair::from_pkcs8(
            ring::signature::Ed25519KeyPair::generate_pkcs8(&SystemRandom::new())
                .unwrap()
                .as_ref(),
        )
        .unwrap();
        let statement = PublisherStatement::from_item(item).unwrap();
        let envelope = PublisherSignatureEnvelope::from_parts(
            PublisherPublicKey::try_from(key.public_key().as_ref()).unwrap(),
            PublisherSignature::try_from(key.sign(statement.as_bytes()).as_ref()).unwrap(),
        );
        (envelope, key)
    }

    fn directory_item() -> Item {
        item_for(
            &[b"alpha", b"", b"omega"],
            vec![
                FileEntry {
                    relative_path: "a.txt".to_string(),
                    size: 5,
                    chunk_range: (0, 1),
                },
                FileEntry {
                    relative_path: "empty.txt".to_string(),
                    size: 0,
                    chunk_range: (1, 2),
                },
                FileEntry {
                    relative_path: "z.txt".to_string(),
                    size: 5,
                    chunk_range: (2, 3),
                },
            ],
            ChunkAlgorithm::fixed_aligned(4, 1),
        )
    }

    #[test]
    fn canonical_statement_round_trips_and_revision_is_unsigned() {
        let item = directory_item();
        let statement = PublisherStatement::from_item(&item).unwrap();
        const GOLDEN: &str = "6469737464207075626c69736865722073746174656d656e742076310001001801000014006f72672e6578616d706c652f6172746966616374cc6287f7e02fc50ce0c4b7ffe12d090f733e61c204b0ac1539e87299549cba7c0a00000000000000010400000001000000000000000100000003000000644a9bc57c6063e2ba4028fa73ed585170ae7db8ac7723d32be49c021a0225f50500000000000000af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f326200000000000000003c13aeea02a8b9dd4108745b06eaf3ed38e318f084ad97dc7001d82f9b8d19480500000000000000030000000500612e747874050000000000000000000000010000000900656d7074792e7478740000000000000000010000000200000005007a2e74787405000000000000000200000003000000";
        assert_eq!(encode_hex(statement.as_bytes()), GOLDEN);
        assert_eq!(
            blake3::hash(statement.as_bytes()).to_hex().as_str(),
            "9ec249850f9d4d518a792449294ab969ab704fa871b68745ae00cea5d7718f86"
        );
        assert_eq!(
            PublisherStatement::decode(&decode_hex(GOLDEN)).unwrap(),
            statement
        );
        assert_eq!(
            PublisherStatement::decode(statement.as_bytes()).unwrap(),
            statement
        );
        let mut changed = item;
        changed.metadata.revision = 99;
        changed.manifest.version = 99;
        changed.metadata.path = PathBuf::from("another/destination");
        changed.metadata.name = "Another name".to_string();
        changed.metadata.description = None;
        changed.metadata.created = SystemTime::now();
        assert_eq!(
            PublisherStatement::from_item(&changed).unwrap().as_bytes(),
            statement.as_bytes()
        );
    }

    #[test]
    fn independently_signed_item_verifies_and_wrong_key_or_mutation_fails() {
        let item = directory_item();
        let (envelope, _) = signed(&item);
        assert_eq!(
            envelope.verify(&item).unwrap(),
            envelope.publisher_fingerprint()
        );
        assert_eq!(
            envelope
                .verify_manifest(&item.manifest, &item.chunks)
                .unwrap(),
            envelope.publisher_fingerprint()
        );
        let (wrong, _) = signed(&item);
        let wrong =
            PublisherSignatureEnvelope::from_parts(wrong.public_key(), envelope.signature());
        assert_eq!(
            wrong.verify(&item),
            Err(SignatureEnvelopeError::InvalidSignature)
        );

        let mut changed = item.clone();
        changed.metadata.artifact_id.push('x');
        changed.manifest.artifact_id.push('x');
        assert!(envelope.verify(&changed).is_err());
    }

    #[test]
    fn every_covered_item_field_and_algorithm_parameter_invalidates_verification() {
        fn refresh(item: &mut Item) {
            let root = reconstruct_root(&item.chunks).unwrap();
            item.metadata.root = root;
            item.manifest.root_hash = root.hash;
            item.manifest.total_size = root.size;
            item.manifest.chunk_count = item.chunks.len() as u32;
            item.manifest.chunk_size = item.manifest.chunk_algorithm.nominal_chunk_size();
        }

        let original = directory_item();
        let (envelope, _) = signed(&original);
        let mut mutations = Vec::new();

        let mut changed = original.clone();
        changed.metadata.artifact_id.push('x');
        changed.manifest.artifact_id.push('x');
        mutations.push(changed);

        let mut changed = original.clone();
        changed.metadata.root.hash = hash(b"wrong root");
        changed.manifest.root_hash = changed.metadata.root.hash;
        mutations.push(changed);

        let mut changed = original.clone();
        changed.metadata.root.size += 1;
        changed.manifest.total_size += 1;
        mutations.push(changed);

        let mut changed = original.clone();
        changed.chunks.swap(0, 2);
        refresh(&mut changed);
        mutations.push(changed);

        let mut changed = original.clone();
        changed.chunks[0].hash = hash(b"different five bytes");
        refresh(&mut changed);
        mutations.push(changed);

        let mut changed = original.clone();
        changed.chunks[0].size += 1;
        changed.manifest.entries[0].size += 1;
        refresh(&mut changed);
        mutations.push(changed);

        let mut changed = original.clone();
        changed.manifest.entries[0].relative_path = "b.txt".to_string();
        mutations.push(changed);

        let mut changed = original.clone();
        changed.manifest.entries[0].size += 1;
        mutations.push(changed);

        let mut changed = original.clone();
        changed.manifest.entries[0].chunk_range.1 = 2;
        mutations.push(changed);

        for (index, mutation) in mutations.iter().enumerate() {
            assert!(
                envelope.verify(mutation).is_err(),
                "covered mutation {index}"
            );
        }

        let algorithm_mutations = [
            ChunkAlgorithm::Fixed256K,
            ChunkAlgorithm::fixed_aligned_with_header(8_192, 512, 256),
            ChunkAlgorithm::FixedAligned {
                chunk_size: 4_096,
                alignment: 1_024,
                header_size: 0,
                profile_version: 1,
            },
            ChunkAlgorithm::FastCdc {
                min_size: 128,
                avg_size: 512,
                max_size: 1_024,
                normalization_level: 3,
                seed: 9,
                profile_version: 1,
            },
            ChunkAlgorithm::FastCdc {
                min_size: 64,
                avg_size: 256,
                max_size: 2_048,
                normalization_level: 4,
                seed: 10,
                profile_version: 1,
            },
            ChunkAlgorithm::BlockAlignedFastCdc {
                min_size: 128,
                avg_size: 256,
                max_size: 1_024,
                alignment: 64,
                zero_run_cutoff: 128,
                normalization_level: 3,
                seed: 11,
                profile_version: 1,
            },
            ChunkAlgorithm::BlockAlignedFastCdc {
                min_size: 256,
                avg_size: 512,
                max_size: 2_048,
                alignment: 128,
                zero_run_cutoff: 256,
                normalization_level: 4,
                seed: 12,
                profile_version: 1,
            },
            ChunkAlgorithm::ArchiveAwareFastCdc {
                min_size: 128,
                avg_size: 256,
                max_size: 1_024,
                record_size: 32,
                alignment: 64,
                normalization_level: 3,
                seed: 13,
                profile_version: 1,
            },
            ChunkAlgorithm::ArchiveAwareFastCdc {
                min_size: 256,
                avg_size: 512,
                max_size: 2_048,
                record_size: 64,
                alignment: 128,
                normalization_level: 4,
                seed: 14,
                profile_version: 1,
            },
        ];
        let base = item_for(
            &[b"data"],
            Vec::new(),
            ChunkAlgorithm::fixed_aligned(4_096, 512),
        );
        let (envelope, _) = signed(&base);
        for algorithm in algorithm_mutations {
            let mut changed = base.clone();
            changed.manifest.chunk_algorithm = algorithm;
            changed.manifest.chunk_size = algorithm.nominal_chunk_size();
            assert!(envelope.verify(&changed).is_err(), "{algorithm:?}");
        }

        for invalid_version in [
            ChunkAlgorithm::FixedAligned {
                chunk_size: 4_096,
                alignment: 512,
                header_size: 0,
                profile_version: 2,
            },
            ChunkAlgorithm::FastCdc {
                min_size: 64,
                avg_size: 256,
                max_size: 1_024,
                normalization_level: 2,
                seed: 1,
                profile_version: 2,
            },
        ] {
            let mut changed = base.clone();
            changed.manifest.chunk_algorithm = invalid_version;
            changed.manifest.chunk_size = invalid_version.nominal_chunk_size();
            assert!(envelope.verify(&changed).is_err());
        }
    }

    #[test]
    fn envelope_and_fingerprint_are_strict_and_canonical() {
        let (envelope, _) = signed(&directory_item());
        let bytes = envelope.encode();
        assert_eq!(bytes.len(), ENVELOPE_BYTES);
        assert_eq!(
            PublisherSignatureEnvelope::decode(&bytes).unwrap(),
            envelope
        );
        for length in 0..bytes.len() {
            assert!(PublisherSignatureEnvelope::decode(&bytes[..length]).is_err());
        }
        let mut trailing = bytes.to_vec();
        trailing.push(0);
        assert_eq!(
            PublisherSignatureEnvelope::decode(&trailing),
            Err(SignatureEnvelopeError::TrailingBytes)
        );
        let text = envelope.publisher_fingerprint().to_string();
        assert_eq!(text.len(), 64);
        assert_eq!(text.parse(), Ok(envelope.publisher_fingerprint()));
        assert!(text
            .to_uppercase()
            .parse::<PublisherKeyFingerprint>()
            .is_err());
        assert!(PublisherPublicKey::try_from(&[0u8; 31][..]).is_err());
        assert!(PublisherSignature::try_from(&[0u8; 63][..]).is_err());
    }

    #[test]
    fn fixed_envelope_literal_is_hash_pinned_and_verifies() {
        let seed = decode_hex_array::<32>(
            "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60",
        );
        let public = decode_hex_array::<32>(
            "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a",
        );
        let key =
            ring::signature::Ed25519KeyPair::from_seed_and_public_key(&seed, &public).unwrap();
        let statement = PublisherStatement::from_item(&directory_item()).unwrap();
        let envelope = PublisherSignatureEnvelope::from_parts(
            PublisherPublicKey::from_bytes(public),
            PublisherSignature::try_from(key.sign(statement.as_bytes()).as_ref()).unwrap(),
        );
        const GOLDEN: &str = "6469737464207075626c6973686572207369676e617475726520656e76656c6f70652076310001000100d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a248daf68892a129b7eb1c6c4a697f650e1e3aaa54879c81834b8122e122797ca371f514407d7423fe42fa9975025515f91de22eff2b79fe53759fa75409a1b0d";
        assert_eq!(encode_hex(&envelope.encode()), GOLDEN);
        assert_eq!(
            blake3::hash(&envelope.encode()).to_hex().as_str(),
            "983fba4c5a7c4a6e8ad04fca68dc32580fa12185fb10a48fac0b9ee7a42402e0"
        );
        assert_eq!(
            envelope.publisher_fingerprint().to_string(),
            "98827399dc7c250ae2cee9829644e0392839c5ad89be10320d1d5af31599879c"
        );
        assert_eq!(
            PublisherSignatureEnvelope::decode(&decode_hex(GOLDEN)).unwrap(),
            envelope
        );
        assert!(envelope.verify(&directory_item()).is_ok());
    }

    #[test]
    fn malformed_statement_headers_paths_and_limits_fail_closed() {
        let item = directory_item();
        let statement = PublisherStatement::from_item(&item).unwrap();
        for length in 0..statement.as_bytes().len() {
            assert!(PublisherStatement::decode(&statement.as_bytes()[..length]).is_err());
        }
        let mut trailing = statement.as_bytes().to_vec();
        trailing.push(0);
        assert!(PublisherStatement::decode(&trailing).is_err());
        let mut wrong_domain = statement.as_bytes().to_vec();
        wrong_domain[0] ^= 1;
        assert_eq!(
            PublisherStatement::decode(&wrong_domain),
            Err(SignatureEnvelopeError::InvalidDomain)
        );

        for invalid in ["", "/a", "a/", "a//b", "a/./b", "a/../b", "a\\b", "a\0b"] {
            let mut changed = item.clone();
            changed.manifest.entries[0].relative_path = invalid.to_string();
            assert!(
                PublisherStatement::from_item(&changed).is_err(),
                "{invalid:?}"
            );
        }
        let mut non_nfc = item.clone();
        non_nfc.manifest.entries[0].relative_path = "e\u{301}.txt".to_string();
        assert!(PublisherStatement::from_item(&non_nfc).is_err());
        let mut too_long = item;
        too_long.manifest.entries[0].relative_path = "x".repeat(MAX_ENTRY_PATH_BYTES + 1);
        assert!(matches!(
            PublisherStatement::from_item(&too_long),
            Err(SignatureEnvelopeError::InvalidStatement(_))
                | Err(SignatureEnvelopeError::StatementLimit { .. })
        ));
    }

    #[test]
    fn versions_lengths_tags_utf8_and_redundant_fields_are_strict() {
        let statement = PublisherStatement::from_item(&directory_item()).unwrap();
        let mut unknown_version = statement.as_bytes().to_vec();
        unknown_version[STATEMENT_DOMAIN.len()..STATEMENT_DOMAIN.len() + 2]
            .copy_from_slice(&2u16.to_le_bytes());
        assert_eq!(
            PublisherStatement::decode(&unknown_version),
            Err(SignatureEnvelopeError::UnsupportedStatementVersion(2))
        );
        let mut wrong_body = statement.as_bytes().to_vec();
        wrong_body[STATEMENT_DOMAIN.len() + 2..STATEMENT_DOMAIN.len() + 6]
            .copy_from_slice(&0u32.to_le_bytes());
        assert_eq!(
            PublisherStatement::decode(&wrong_body),
            Err(SignatureEnvelopeError::TrailingBytes)
        );
        let artifact_start = STATEMENT_DOMAIN.len() + 2 + 4 + 2;
        let mut invalid_utf8 = statement.as_bytes().to_vec();
        invalid_utf8[artifact_start] = 0xff;
        assert!(PublisherStatement::decode(&invalid_utf8).is_err());
        let algorithm_offset = artifact_start + "org.example/artifact".len() + 32 + 8;
        let mut unknown_tag = statement.as_bytes().to_vec();
        unknown_tag[algorithm_offset] = 255;
        assert!(PublisherStatement::decode(&unknown_tag).is_err());

        let envelope = signed(&directory_item()).0.encode();
        let mut version = envelope;
        version[ENVELOPE_DOMAIN.len()..ENVELOPE_DOMAIN.len() + 2]
            .copy_from_slice(&2u16.to_le_bytes());
        assert_eq!(
            PublisherSignatureEnvelope::decode(&version),
            Err(SignatureEnvelopeError::UnsupportedEnvelopeVersion(2))
        );
        let mut version = envelope;
        version[ENVELOPE_DOMAIN.len() + 2..ENVELOPE_DOMAIN.len() + 4]
            .copy_from_slice(&2u16.to_le_bytes());
        assert_eq!(
            PublisherSignatureEnvelope::decode(&version),
            Err(SignatureEnvelopeError::UnsupportedStatementVersion(2))
        );

        let mut invalid = directory_item();
        invalid.manifest.chunk_count += 1;
        assert!(PublisherStatement::from_item(&invalid).is_err());
        let mut invalid = directory_item();
        invalid.manifest.chunk_size += 1;
        assert!(PublisherStatement::from_item(&invalid).is_err());
        let mut invalid = directory_item();
        invalid.manifest.version += 1;
        assert!(PublisherStatement::from_item(&invalid).is_err());
    }

    #[test]
    fn every_chunk_algorithm_variant_round_trips() {
        let algorithms = [
            ChunkAlgorithm::Fixed256K,
            ChunkAlgorithm::fixed_aligned_with_header(4096, 512, 512),
            ChunkAlgorithm::fastcdc_default(),
            ChunkAlgorithm::image_default(),
            ChunkAlgorithm::archive_default(),
        ];
        for algorithm in algorithms {
            let item = item_for(&[b"data"], Vec::new(), algorithm);
            let statement =
                PublisherStatement::from_manifest(&item.manifest, &item.chunks).unwrap();
            assert_eq!(
                PublisherStatement::decode(statement.as_bytes()).unwrap(),
                statement
            );
        }
    }

    #[test]
    fn detected_bzip2_profile_encodes_decodes_and_verifies() {
        let algorithm = ChunkAlgorithm::from_policy(
            ChunkingPolicy::Auto,
            std::path::Path::new("archive.tar.bz2"),
            &[],
            false,
        );
        assert_eq!(
            algorithm,
            ChunkAlgorithm::fixed_aligned(1024 * 1024, 100 * 1024)
        );
        let item = item_for(&[b"bzip2 payload"], Vec::new(), algorithm);
        let statement = PublisherStatement::from_manifest(&item.manifest, &item.chunks).unwrap();
        assert_eq!(
            PublisherStatement::decode(statement.as_bytes()).unwrap(),
            statement
        );
        let (envelope, _) = signed(&item);
        assert_eq!(
            envelope
                .verify_manifest(&item.manifest, &item.chunks)
                .unwrap(),
            envelope.publisher_fingerprint()
        );
    }

    #[test]
    fn normalization_level_boundary_is_exact() {
        for algorithm in [
            ChunkAlgorithm::FastCdc {
                min_size: 64,
                avg_size: 256,
                max_size: 1_024,
                normalization_level: 62,
                seed: 1,
                profile_version: 1,
            },
            ChunkAlgorithm::BlockAlignedFastCdc {
                min_size: 128,
                avg_size: 256,
                max_size: 1_024,
                alignment: 64,
                zero_run_cutoff: 128,
                normalization_level: 62,
                seed: 1,
                profile_version: 1,
            },
            ChunkAlgorithm::ArchiveAwareFastCdc {
                min_size: 128,
                avg_size: 256,
                max_size: 1_024,
                record_size: 32,
                alignment: 64,
                normalization_level: 62,
                seed: 1,
                profile_version: 1,
            },
        ] {
            let item = item_for(&[b"data"], Vec::new(), algorithm);
            assert!(PublisherStatement::from_manifest(&item.manifest, &item.chunks).is_ok());

            let invalid = match algorithm {
                ChunkAlgorithm::FastCdc {
                    min_size,
                    avg_size,
                    max_size,
                    seed,
                    profile_version,
                    ..
                } => ChunkAlgorithm::FastCdc {
                    min_size,
                    avg_size,
                    max_size,
                    normalization_level: 63,
                    seed,
                    profile_version,
                },
                ChunkAlgorithm::BlockAlignedFastCdc {
                    min_size,
                    avg_size,
                    max_size,
                    alignment,
                    zero_run_cutoff,
                    seed,
                    profile_version,
                    ..
                } => ChunkAlgorithm::BlockAlignedFastCdc {
                    min_size,
                    avg_size,
                    max_size,
                    alignment,
                    zero_run_cutoff,
                    normalization_level: 63,
                    seed,
                    profile_version,
                },
                ChunkAlgorithm::ArchiveAwareFastCdc {
                    min_size,
                    avg_size,
                    max_size,
                    record_size,
                    alignment,
                    seed,
                    profile_version,
                    ..
                } => ChunkAlgorithm::ArchiveAwareFastCdc {
                    min_size,
                    avg_size,
                    max_size,
                    record_size,
                    alignment,
                    normalization_level: 63,
                    seed,
                    profile_version,
                },
                _ => unreachable!(),
            };
            let invalid_item = item_for(&[b"data"], Vec::new(), invalid);
            assert!(PublisherStatement::from_manifest(
                &invalid_item.manifest,
                &invalid_item.chunks
            )
            .is_err());
        }
    }

    #[test]
    fn every_algorithm_parameter_is_independently_covered() {
        fn assert_mutations(base: ChunkAlgorithm, mutations: &[ChunkAlgorithm]) {
            let item = item_for(&[b"data"], Vec::new(), base);
            let original = PublisherStatement::from_manifest(&item.manifest, &item.chunks).unwrap();
            let (envelope, _) = signed(&item);
            for mutation in mutations {
                let mut changed = item.clone();
                changed.manifest.chunk_algorithm = *mutation;
                changed.manifest.chunk_size = mutation.nominal_chunk_size();
                match PublisherStatement::from_manifest(&changed.manifest, &changed.chunks) {
                    Ok(statement) => {
                        assert_ne!(statement.as_bytes(), original.as_bytes(), "{mutation:?}");
                        assert_eq!(
                            envelope.verify_manifest(&changed.manifest, &changed.chunks),
                            Err(SignatureEnvelopeError::InvalidSignature),
                            "{mutation:?}"
                        );
                    }
                    Err(_) => assert!(matches!(
                        mutation,
                        ChunkAlgorithm::FixedAligned {
                            profile_version: 2,
                            ..
                        } | ChunkAlgorithm::FastCdc {
                            profile_version: 2,
                            ..
                        } | ChunkAlgorithm::BlockAlignedFastCdc {
                            profile_version: 2,
                            ..
                        } | ChunkAlgorithm::ArchiveAwareFastCdc {
                            profile_version: 2,
                            ..
                        }
                    )),
                }
            }
        }

        let fixed_variant =
            |chunk_size, alignment, header_size, profile_version| ChunkAlgorithm::FixedAligned {
                chunk_size,
                alignment,
                header_size,
                profile_version,
            };
        let fixed = fixed_variant(4_096, 512, 0, 1);
        assert_mutations(
            fixed,
            &[
                fixed_variant(8_192, 512, 0, 1),
                fixed_variant(4_096, 1_024, 0, 1),
                fixed_variant(4_096, 512, 512, 1),
                fixed_variant(4_096, 512, 0, 2),
            ],
        );

        let fast_variant =
            |min_size, avg_size, max_size, normalization_level, seed, profile_version| {
                ChunkAlgorithm::FastCdc {
                    min_size,
                    avg_size,
                    max_size,
                    normalization_level,
                    seed,
                    profile_version,
                }
            };
        let fast = fast_variant(64, 256, 1_024, 2, 1, 1);
        assert_mutations(
            fast,
            &[
                fast_variant(128, 256, 1_024, 2, 1, 1),
                fast_variant(64, 512, 1_024, 2, 1, 1),
                fast_variant(64, 256, 2_048, 2, 1, 1),
                fast_variant(64, 256, 1_024, 3, 1, 1),
                fast_variant(64, 256, 1_024, 2, 2, 1),
                fast_variant(64, 256, 1_024, 2, 1, 2),
            ],
        );

        let block_variant = |min_size,
                             avg_size,
                             max_size,
                             alignment,
                             zero_run_cutoff,
                             normalization_level,
                             seed,
                             profile_version| {
            ChunkAlgorithm::BlockAlignedFastCdc {
                min_size,
                avg_size,
                max_size,
                alignment,
                zero_run_cutoff,
                normalization_level,
                seed,
                profile_version,
            }
        };
        let block = block_variant(128, 256, 1_024, 64, 128, 2, 1, 1);
        assert_mutations(
            block,
            &[
                block_variant(192, 256, 1_024, 64, 128, 2, 1, 1),
                block_variant(128, 512, 1_024, 64, 128, 2, 1, 1),
                block_variant(128, 256, 2_048, 64, 128, 2, 1, 1),
                block_variant(128, 256, 1_024, 32, 128, 2, 1, 1),
                block_variant(128, 256, 1_024, 64, 256, 2, 1, 1),
                block_variant(128, 256, 1_024, 64, 128, 3, 1, 1),
                block_variant(128, 256, 1_024, 64, 128, 2, 2, 1),
                block_variant(128, 256, 1_024, 64, 128, 2, 1, 2),
            ],
        );

        let archive_variant = |min_size,
                               avg_size,
                               max_size,
                               record_size,
                               alignment,
                               normalization_level,
                               seed,
                               profile_version| {
            ChunkAlgorithm::ArchiveAwareFastCdc {
                min_size,
                avg_size,
                max_size,
                record_size,
                alignment,
                normalization_level,
                seed,
                profile_version,
            }
        };
        let archive = archive_variant(128, 256, 1_024, 32, 64, 2, 1, 1);
        assert_mutations(
            archive,
            &[
                archive_variant(192, 256, 1_024, 32, 64, 2, 1, 1),
                archive_variant(128, 512, 1_024, 32, 64, 2, 1, 1),
                archive_variant(128, 256, 2_048, 32, 64, 2, 1, 1),
                archive_variant(128, 256, 1_024, 16, 64, 2, 1, 1),
                archive_variant(128, 256, 1_024, 32, 128, 2, 1, 1),
                archive_variant(128, 256, 1_024, 32, 64, 3, 1, 1),
                archive_variant(128, 256, 1_024, 32, 64, 2, 2, 1),
                archive_variant(128, 256, 1_024, 32, 64, 2, 1, 2),
            ],
        );

        let item = item_for(&[b"data"], Vec::new(), ChunkAlgorithm::Fixed256K);
        let (envelope, _) = signed(&item);
        let mut changed = item.clone();
        changed.manifest.chunk_algorithm = fixed;
        changed.manifest.chunk_size = fixed.nominal_chunk_size();
        assert_eq!(
            envelope.verify_manifest(&changed.manifest, &changed.chunks),
            Err(SignatureEnvelopeError::InvalidSignature)
        );
    }

    #[test]
    fn exact_artifact_path_and_count_bounds_are_enforced() {
        let mut item = item_for(&[b"data"], Vec::new(), ChunkAlgorithm::Fixed256K);
        item.metadata.artifact_id = "a".repeat(MAX_ARTIFACT_ID_BYTES);
        item.manifest
            .artifact_id
            .clone_from(&item.metadata.artifact_id);
        assert!(PublisherStatement::from_item(&item).is_ok());
        item.metadata.artifact_id.push('a');
        item.manifest.artifact_id.push('a');
        assert!(PublisherStatement::from_item(&item).is_err());

        let mut chunks = vec![
            ChunkInfo {
                hash: hash(b"x"),
                size: 1
            };
            MAX_CHUNKS + 1
        ];
        let root = reconstruct_root(&chunks).unwrap();
        let manifest = Manifest {
            artifact_id: "a".to_string(),
            version: 0,
            root_hash: root.hash,
            total_size: root.size,
            chunk_count: chunks.len() as u32,
            chunk_size: ChunkAlgorithm::Fixed256K.nominal_chunk_size(),
            chunk_algorithm: ChunkAlgorithm::Fixed256K,
            entries: Vec::new(),
        };
        assert!(matches!(
            PublisherStatement::from_parts(&manifest, &chunks),
            Err(SignatureEnvelopeError::StatementLimit {
                field: "chunks",
                ..
            })
        ));
        chunks.truncate(MAX_CHUNKS);
    }

    #[test]
    fn exact_path_count_and_total_statement_bounds_round_trip() {
        let chunk = ChunkInfo {
            hash: hash(b"x"),
            size: 1,
        };
        let mut path_manifest = Manifest {
            artifact_id: "a".to_string(),
            version: 0,
            root_hash: chunk.hash,
            total_size: 1,
            chunk_count: 1,
            chunk_size: ChunkAlgorithm::Fixed256K.nominal_chunk_size(),
            chunk_algorithm: ChunkAlgorithm::Fixed256K,
            entries: vec![FileEntry {
                relative_path: "x".repeat(MAX_ENTRY_PATH_BYTES),
                size: 1,
                chunk_range: (0, 1),
            }],
        };
        assert!(PublisherStatement::from_parts(&path_manifest, &[chunk]).is_ok());
        path_manifest.entries[0].relative_path.push('x');
        assert!(matches!(
            PublisherStatement::from_parts(&path_manifest, &[chunk]),
            Err(SignatureEnvelopeError::StatementLimit {
                field: "entry path",
                ..
            })
        ));

        let chunks = vec![chunk; MAX_CHUNKS];
        let root = reconstruct_root(&chunks).unwrap();
        let base_path_bytes = MAX_ENTRIES * 7;
        let fixed_bytes = STATEMENT_DOMAIN.len()
            + 2
            + 4
            + 2
            + 1
            + 32
            + 8
            + encoded_algorithm_size(ChunkAlgorithm::Fixed256K)
            + 4
            + MAX_CHUNKS * 40
            + 4
            + MAX_ENTRIES * 18;
        let required_path_bytes = MAX_FRAME_PAYLOAD_BYTES - fixed_bytes;
        let filler_bytes = required_path_bytes - base_path_bytes;
        let filler_per_entry = filler_bytes / MAX_ENTRIES;
        let entries_with_extra = filler_bytes % MAX_ENTRIES;
        let entries: Vec<FileEntry> = (0..MAX_ENTRIES)
            .map(|index| FileEntry {
                relative_path: format!(
                    "p{index:05}/{}",
                    "x".repeat(filler_per_entry + usize::from(index < entries_with_extra))
                ),
                size: 1,
                chunk_range: (index as u32, index as u32 + 1),
            })
            .collect();
        let manifest = Manifest {
            artifact_id: "a".to_string(),
            version: 0,
            root_hash: root.hash,
            total_size: root.size,
            chunk_count: MAX_CHUNKS as u32,
            chunk_size: ChunkAlgorithm::Fixed256K.nominal_chunk_size(),
            chunk_algorithm: ChunkAlgorithm::Fixed256K,
            entries,
        };
        let statement = PublisherStatement::from_parts(&manifest, &chunks).unwrap();
        assert_eq!(statement.as_bytes().len(), MAX_FRAME_PAYLOAD_BYTES);
        assert_eq!(
            PublisherStatement::decode(statement.as_bytes()).unwrap(),
            statement
        );

        let mut oversized = manifest;
        oversized.entries[0].relative_path.push('x');
        assert!(matches!(
            PublisherStatement::from_parts(&oversized, &chunks),
            Err(SignatureEnvelopeError::StatementLimit {
                field: "statement bytes",
                ..
            })
        ));

        let fixed_typed_overhead = MAX_CHUNKS * std::mem::size_of::<ChunkInfo>()
            + MAX_ENTRIES * std::mem::size_of::<FileEntry>()
            + (usize::BITS as usize + 1) * std::mem::size_of::<Option<ChunkInfo>>();
        assert!(fixed_typed_overhead <= 8 * 1024 * 1024);
    }

    #[test]
    fn entry_count_chunk_size_and_directory_semantic_boundaries_are_exact() {
        let maximum_chunk = ChunkInfo {
            hash: hash(b"maximum-size-identity"),
            size: u64::from(u32::MAX),
        };
        let maximum_manifest = Manifest {
            artifact_id: "a".to_string(),
            version: 0,
            root_hash: maximum_chunk.hash,
            total_size: maximum_chunk.size,
            chunk_count: 1,
            chunk_size: ChunkAlgorithm::Fixed256K.nominal_chunk_size(),
            chunk_algorithm: ChunkAlgorithm::Fixed256K,
            entries: Vec::new(),
        };
        assert!(PublisherStatement::from_manifest(&maximum_manifest, &[maximum_chunk]).is_ok());
        let oversized_chunk = ChunkInfo {
            size: u64::from(u32::MAX) + 1,
            ..maximum_chunk
        };
        let mut oversized_manifest = maximum_manifest;
        oversized_manifest.total_size = oversized_chunk.size;
        assert!(
            PublisherStatement::from_manifest(&oversized_manifest, &[oversized_chunk]).is_err()
        );

        let chunk = ChunkInfo {
            hash: hash(b"x"),
            size: 1,
        };
        let mut too_many_entries = Manifest {
            artifact_id: "a".to_string(),
            version: 0,
            root_hash: chunk.hash,
            total_size: 1,
            chunk_count: 1,
            chunk_size: ChunkAlgorithm::Fixed256K.nominal_chunk_size(),
            chunk_algorithm: ChunkAlgorithm::Fixed256K,
            entries: vec![
                FileEntry {
                    relative_path: "a".to_string(),
                    size: 1,
                    chunk_range: (0, 1),
                };
                MAX_ENTRIES + 1
            ],
        };
        assert!(matches!(
            PublisherStatement::from_manifest(&too_many_entries, &[chunk]),
            Err(SignatureEnvelopeError::StatementLimit {
                field: "entries",
                ..
            })
        ));
        too_many_entries.entries.clear();

        let wrong_empty = ChunkInfo {
            hash: hash(b"not empty"),
            size: 0,
        };
        let wrong_empty_manifest = Manifest {
            root_hash: wrong_empty.hash,
            total_size: 0,
            ..too_many_entries.clone()
        };
        assert!(PublisherStatement::from_manifest(&wrong_empty_manifest, &[wrong_empty]).is_err());

        let empty = ChunkInfo {
            hash: hash(&[]),
            size: 0,
        };
        let two_empty = [empty, empty];
        let empty_root = reconstruct_root(&two_empty).unwrap();
        let invalid_empty_entry = Manifest {
            root_hash: empty_root.hash,
            total_size: 0,
            chunk_count: 2,
            entries: vec![FileEntry {
                relative_path: "empty".to_string(),
                size: 0,
                chunk_range: (0, 2),
            }],
            ..too_many_entries.clone()
        };
        assert!(PublisherStatement::from_manifest(&invalid_empty_entry, &two_empty).is_err());

        let two_chunks = [chunk, chunk];
        let root = reconstruct_root(&two_chunks).unwrap();
        for paths in [
            ["a", "a/b"],
            ["b", "a"],
            [".distd-revisions-v1/a", "z"],
            [".distd-activation-v1/a", "z"],
        ] {
            let manifest = Manifest {
                root_hash: root.hash,
                total_size: root.size,
                chunk_count: 2,
                entries: paths
                    .into_iter()
                    .enumerate()
                    .map(|(index, path)| FileEntry {
                        relative_path: path.to_string(),
                        size: 1,
                        chunk_range: (index as u32, index as u32 + 1),
                    })
                    .collect(),
                ..too_many_entries.clone()
            };
            assert!(
                PublisherStatement::from_manifest(&manifest, &two_chunks).is_err(),
                "{paths:?}"
            );
        }

        let mixed = [empty, chunk];
        let mixed_root = reconstruct_root(&mixed).unwrap();
        let nonempty_covers_zero = Manifest {
            root_hash: mixed_root.hash,
            total_size: mixed_root.size,
            chunk_count: 2,
            entries: vec![FileEntry {
                relative_path: "nonempty".to_string(),
                size: 1,
                chunk_range: (0, 2),
            }],
            ..too_many_entries
        };
        assert!(PublisherStatement::from_manifest(&nonempty_covers_zero, &mixed).is_err());
    }

    #[test]
    fn deterministic_generated_manifests_round_trip_and_verify() {
        for count in [1usize, 2, 3, 17, 64, 257] {
            let bytes: Vec<Vec<u8>> = (0..count)
                .map(|index| {
                    (0..(index % 31 + 1))
                        .map(|offset| ((index * 17 + offset * 13) % 251) as u8)
                        .collect()
                })
                .collect();
            let parts: Vec<&[u8]> = bytes.iter().map(Vec::as_slice).collect();
            let item = item_for(&parts, Vec::new(), ChunkAlgorithm::Fixed256K);
            let statement = PublisherStatement::from_item(&item).unwrap();
            assert_eq!(
                PublisherStatement::decode(statement.as_bytes()).unwrap(),
                statement
            );
            assert!(signed(&item).0.verify(&item).is_ok());
        }
    }

    #[test]
    fn one_byte_statement_mutations_do_not_verify() {
        let item = directory_item();
        let (envelope, _) = signed(&item);
        let statement = PublisherStatement::from_item(&item).unwrap();
        for index in 0..statement.as_bytes().len() {
            let mut bytes = statement.as_bytes().to_vec();
            bytes[index] ^= 1;
            assert!(signature::UnparsedPublicKey::new(
                &signature::ED25519,
                envelope.public_key().as_bytes()
            )
            .verify(&bytes, envelope.signature().as_bytes())
            .is_err());
        }
    }

    #[test]
    #[ignore = "local release publisher signature performance reference"]
    fn publisher_signature_reference_performance() {
        use std::{
            process::Command,
            time::{Duration, Instant},
        };

        assert!(
            !cfg!(debug_assertions),
            "publisher signature reference must run with --release"
        );
        const WARMUPS: usize = 5;
        const SAMPLES: usize = 20;
        const FINGERPRINT_SAMPLES: usize = 1_000;

        fn item_with_chunks(count: usize, directory: bool) -> Item {
            let chunks: Vec<ChunkInfo> = (0..count)
                .map(|index| {
                    let bytes = (index as u64).to_le_bytes();
                    ChunkInfo {
                        hash: hash(&bytes),
                        size: 8,
                    }
                })
                .collect();
            let root = reconstruct_root(&chunks).unwrap();
            let entries = directory
                .then(|| {
                    (0..count)
                        .map(|index| FileEntry {
                            relative_path: format!("f{index:05}"),
                            size: 8,
                            chunk_range: (index as u32, index as u32 + 1),
                        })
                        .collect()
                })
                .unwrap_or_default();
            Item {
                metadata: ItemMetadata {
                    artifact_id: "org.example/performance".to_string(),
                    name: "Performance".to_string(),
                    description: None,
                    revision: 1,
                    path: PathBuf::from("performance.bin"),
                    root,
                    created: SystemTime::UNIX_EPOCH,
                    updated: SystemTime::UNIX_EPOCH,
                    created_by: "reference".to_string(),
                    format: Format::V1,
                },
                manifest: Manifest {
                    artifact_id: "org.example/performance".to_string(),
                    version: 1,
                    root_hash: root.hash,
                    total_size: root.size,
                    chunk_count: count as u32,
                    chunk_size: ChunkAlgorithm::Fixed256K.nominal_chunk_size(),
                    chunk_algorithm: ChunkAlgorithm::Fixed256K,
                    entries,
                },
                chunks,
            }
        }

        fn p95(samples: &[Duration]) -> Duration {
            let mut sorted = samples.to_vec();
            sorted.sort_unstable();
            sorted[(sorted.len() * 95).div_ceil(100) - 1]
        }

        let seed = decode_hex_array::<32>(
            "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60",
        );
        let public = decode_hex_array::<32>(
            "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a",
        );
        let key =
            ring::signature::Ed25519KeyPair::from_seed_and_public_key(&seed, &public).unwrap();
        let publisher_key = PublisherPublicKey::from_bytes(public);

        let fingerprint_samples: Vec<Duration> = (0..FINGERPRINT_SAMPLES)
            .map(|_| {
                let started = Instant::now();
                std::hint::black_box(publisher_key.fingerprint());
                started.elapsed()
            })
            .collect();
        println!("fingerprint_samples={fingerprint_samples:?}");
        assert!(p95(&fingerprint_samples) <= Duration::from_micros(10));

        for directory in [false, true] {
            let workload = if directory { "directory" } else { "single" };
            for (count, construction_limit, verification_limit) in [
                (1, Duration::from_micros(100), Duration::from_micros(100)),
                (64, Duration::from_micros(100), Duration::from_micros(100)),
                (4_096, Duration::from_millis(1), Duration::from_millis(1)),
                (
                    MAX_CHUNKS,
                    Duration::from_millis(10),
                    Duration::from_millis(15),
                ),
            ] {
                let item = item_with_chunks(count, directory);
                let statement = PublisherStatement::from_item(&item).unwrap();
                let envelope = PublisherSignatureEnvelope::from_parts(
                    publisher_key,
                    PublisherSignature::try_from(key.sign(statement.as_bytes()).as_ref()).unwrap(),
                );
                for _ in 0..WARMUPS {
                    std::hint::black_box(
                        PublisherStatement::from_manifest(&item.manifest, &item.chunks).unwrap(),
                    );
                    std::hint::black_box(
                        envelope
                            .verify_manifest(&item.manifest, &item.chunks)
                            .unwrap(),
                    );
                }
                let construction: Vec<Duration> = (0..SAMPLES)
                    .map(|_| {
                        let started = Instant::now();
                        std::hint::black_box(
                            PublisherStatement::from_manifest(&item.manifest, &item.chunks)
                                .unwrap(),
                        );
                        started.elapsed()
                    })
                    .collect();
                let verification: Vec<Duration> = (0..SAMPLES)
                    .map(|_| {
                        let started = Instant::now();
                        std::hint::black_box(
                            envelope
                                .verify_manifest(&item.manifest, &item.chunks)
                                .unwrap(),
                        );
                        started.elapsed()
                    })
                    .collect();
                println!(
                    "workload={workload} chunks={count} construction_samples={construction:?} construction_p95={:?} verification_samples={verification:?} verification_p95={:?}",
                    p95(&construction),
                    p95(&verification)
                );
                assert!(p95(&construction) <= construction_limit);
                assert!(p95(&verification) <= verification_limit);
            }
        }

        let command_output = |program: &str, arguments: &[&str]| {
            let workspace = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .parent()
                .expect("core crate must have a workspace parent");
            Command::new(program)
                .args(arguments)
                .current_dir(workspace)
                .output()
                .ok()
                .filter(|output| output.status.success())
                .map_or_else(
                    || "unavailable".to_string(),
                    |output| {
                        String::from_utf8_lossy(&output.stdout)
                            .trim()
                            .replace('\n', "; ")
                    },
                )
        };
        let workspace = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("core crate must have a workspace parent");
        let mut source_snapshot = Command::new("git")
            .args([
                "diff",
                "--",
                "Cargo.lock",
                "Cargo.toml",
                "core/Cargo.toml",
                "core/src/lib.rs",
                "core/src/signature.rs",
            ])
            .current_dir(workspace)
            .output()
            .ok()
            .map_or_else(Vec::new, |output| output.stdout);
        // `signature.rs` remains untracked while this implementation track is
        // under review, so include its exact bytes independently of Git index
        // state. Once tracked this remains deterministic, albeit redundant.
        source_snapshot.extend_from_slice(
            &std::fs::read(workspace.join("core/src/signature.rs")).unwrap_or_default(),
        );
        let lock_bytes = std::fs::read(workspace.join("Cargo.lock")).unwrap_or_default();
        println!(
            "reference_identity git_head={} source_snapshot_blake3={} cargo_lock_blake3={} package={} arch={} os={} rustc={} warmups={WARMUPS} samples={SAMPLES} fingerprint_samples={FINGERPRINT_SAMPLES}",
            command_output("git", &["rev-parse", "HEAD"]),
            blake3::hash(&source_snapshot),
            blake3::hash(&lock_bytes),
            env!("CARGO_PKG_VERSION"),
            std::env::consts::ARCH,
            command_output("uname", &["-a"]),
            command_output("rustc", &["-Vv"]),
        );
    }

    fn encode_hex(bytes: &[u8]) -> String {
        use std::fmt::Write as _;
        bytes.iter().fold(String::new(), |mut encoded, byte| {
            write!(encoded, "{byte:02x}").unwrap();
            encoded
        })
    }

    fn decode_hex_array<const N: usize>(value: &str) -> [u8; N] {
        let mut bytes = [0u8; N];
        for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
            bytes[index] = hex_nibble(pair[0]).unwrap() << 4 | hex_nibble(pair[1]).unwrap();
        }
        bytes
    }

    fn decode_hex(value: &str) -> Vec<u8> {
        value
            .as_bytes()
            .chunks_exact(2)
            .map(|pair| hex_nibble(pair[0]).unwrap() << 4 | hex_nibble(pair[1]).unwrap())
            .collect()
    }
}

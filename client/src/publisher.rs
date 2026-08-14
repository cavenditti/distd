//! Client-side publisher trust policy and authenticity errors.

use std::{collections::HashSet, fmt, sync::Arc};

use distd_core::{
    chunks::ChunkInfo,
    item::Manifest,
    signature::{
        PublisherKeyFingerprint, PublisherPublicKey, PublisherReleaseEnvelope,
        PublisherReleaseSequence, PublisherSignatureEnvelope, PublisherStatement,
        VerifiedPublisherRelease,
    },
};
use thiserror::Error;

/// Maximum number of exact publisher keys accepted from client configuration.
pub const MAX_PUBLISHER_KEYS: usize = 1_024;

/// Stable, non-secret publisher trust configuration failures.
#[non_exhaustive]
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum PublisherConfigError {
    #[error("publisher trust requires at least one public key or explicit unsigned mode")]
    MissingPolicy,
    #[error("publisher public keys cannot be combined with explicit unsigned mode")]
    MixedPolicy,
    #[error("publisher trust supports at most {maximum} public keys")]
    TooManyKeys { maximum: usize },
    #[error("publisher public key entry {index} is invalid")]
    InvalidKey { index: usize },
    #[error("publisher public key entry {index} duplicates an earlier key")]
    DuplicateKey { index: usize },
}

/// Stable, non-secret failures at the publisher-authenticity boundary.
#[non_exhaustive]
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum PublisherAuthenticityError {
    #[error("publisher attestation is missing")]
    MissingAttestation,
    #[error("publisher attestation is malformed")]
    MalformedAttestation,
    #[error("publisher artifact identity does not match the request")]
    IdentityMismatch,
    #[error("publisher public key is not trusted")]
    UntrustedPublisher,
    #[error("publisher signature or signed manifest is invalid")]
    InvalidSignature,
}

/// Strictly parsed publisher tuple carried by one sync manifest.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PublisherTransportAttestation {
    ReleaseV2 {
        sequence: PublisherReleaseSequence,
        envelope: PublisherReleaseEnvelope,
    },
    ReplayableV1(PublisherSignatureEnvelope),
    Unsigned,
}

/// Result of client policy and cryptographic authorization.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PublisherAuthorization {
    fingerprint: Option<PublisherKeyFingerprint>,
    rollback: Option<VerifiedPublisherRelease>,
}

impl PublisherAuthorization {
    #[must_use]
    pub const fn fingerprint(self) -> Option<PublisherKeyFingerprint> {
        self.fingerprint
    }

    #[must_use]
    pub const fn rollback(self) -> Option<VerifiedPublisherRelease> {
        self.rollback
    }
}

#[derive(Clone)]
enum PublisherPolicyMode {
    Required(Arc<HashSet<PublisherPublicKey>>),
    ExplicitlyInsecureUnsigned,
}

/// Immutable client policy for publisher-authenticated sync manifests.
#[derive(Clone)]
pub struct PublisherPolicy {
    mode: PublisherPolicyMode,
}

impl PublisherPolicy {
    /// Authorize one strict transport attestation. Required policy accepts
    /// only V2 under an exact configured key; explicit insecure mode still
    /// validates every supplied signature but creates no rollback authority.
    pub fn authorize_transport_manifest(
        &self,
        attestation: PublisherTransportAttestation,
        manifest: &Manifest,
        chunks: &[ChunkInfo],
    ) -> Result<PublisherAuthorization, PublisherAuthenticityError> {
        match (&self.mode, attestation) {
            (
                PublisherPolicyMode::Required(keys),
                PublisherTransportAttestation::ReleaseV2 { sequence, envelope },
            ) => {
                if !keys.contains(&envelope.public_key()) {
                    return Err(PublisherAuthenticityError::UntrustedPublisher);
                }
                let verified = envelope
                    .verify_manifest(manifest, chunks, sequence)
                    .map_err(|_| PublisherAuthenticityError::InvalidSignature)?;
                Ok(PublisherAuthorization {
                    fingerprint: Some(verified.publisher_fingerprint()),
                    rollback: Some(verified),
                })
            }
            (PublisherPolicyMode::Required(_), PublisherTransportAttestation::ReplayableV1(_))
            | (PublisherPolicyMode::Required(_), PublisherTransportAttestation::Unsigned) => {
                Err(PublisherAuthenticityError::MissingAttestation)
            }
            (
                PublisherPolicyMode::ExplicitlyInsecureUnsigned,
                PublisherTransportAttestation::ReleaseV2 { sequence, envelope },
            ) => {
                let verified = envelope
                    .verify_manifest(manifest, chunks, sequence)
                    .map_err(|_| PublisherAuthenticityError::InvalidSignature)?;
                Ok(PublisherAuthorization {
                    fingerprint: Some(verified.publisher_fingerprint()),
                    rollback: None,
                })
            }
            (
                PublisherPolicyMode::ExplicitlyInsecureUnsigned,
                PublisherTransportAttestation::ReplayableV1(envelope),
            ) => envelope
                .verify_manifest(manifest, chunks)
                .map(|fingerprint| PublisherAuthorization {
                    fingerprint: Some(fingerprint),
                    rollback: None,
                })
                .map_err(|_| PublisherAuthenticityError::InvalidSignature),
            (
                PublisherPolicyMode::ExplicitlyInsecureUnsigned,
                PublisherTransportAttestation::Unsigned,
            ) => {
                PublisherStatement::from_manifest(manifest, chunks)
                    .map_err(|_| PublisherAuthenticityError::InvalidSignature)?;
                Ok(PublisherAuthorization {
                    fingerprint: None,
                    rollback: None,
                })
            }
        }
    }

    /// Parse one exact policy from the raw client configuration fields.
    pub fn from_config(
        configured_keys: &[String],
        allow_unsigned_publication: bool,
    ) -> Result<Self, PublisherConfigError> {
        if allow_unsigned_publication {
            if !configured_keys.is_empty() {
                return Err(PublisherConfigError::MixedPolicy);
            }
            return Ok(Self {
                mode: PublisherPolicyMode::ExplicitlyInsecureUnsigned,
            });
        }
        if configured_keys.is_empty() {
            return Err(PublisherConfigError::MissingPolicy);
        }
        if configured_keys.len() > MAX_PUBLISHER_KEYS {
            return Err(PublisherConfigError::TooManyKeys {
                maximum: MAX_PUBLISHER_KEYS,
            });
        }

        let mut keys = HashSet::with_capacity(configured_keys.len());
        for (offset, configured) in configured_keys.iter().enumerate() {
            let index = offset + 1;
            let key =
                decode_public_key(configured).ok_or(PublisherConfigError::InvalidKey { index })?;
            if !keys.insert(key) {
                return Err(PublisherConfigError::DuplicateKey { index });
            }
        }
        Ok(Self {
            mode: PublisherPolicyMode::Required(Arc::new(keys)),
        })
    }

    /// Compatibility-only V1 manifest verification. Rollback-enforcing mode
    /// rejects every V1 envelope; explicit insecure mode still validates a
    /// supplied signature and canonicalizes unsigned manifests.
    pub fn authorize_manifest(
        &self,
        envelope: Option<&PublisherSignatureEnvelope>,
        manifest: &Manifest,
        chunks: &[ChunkInfo],
    ) -> Result<Option<PublisherKeyFingerprint>, PublisherAuthenticityError> {
        match (&self.mode, envelope) {
            (PublisherPolicyMode::Required(_), None | Some(_)) => {
                Err(PublisherAuthenticityError::MissingAttestation)
            }
            (PublisherPolicyMode::ExplicitlyInsecureUnsigned, None) => {
                PublisherStatement::from_manifest(manifest, chunks)
                    .map_err(|_| PublisherAuthenticityError::InvalidSignature)?;
                Ok(None)
            }
            (PublisherPolicyMode::ExplicitlyInsecureUnsigned, Some(envelope)) => envelope
                .verify_manifest(manifest, chunks)
                .map(Some)
                .map_err(|_| PublisherAuthenticityError::InvalidSignature),
        }
    }

    /// Whether an exact raw publisher key is an explicit trust anchor.
    #[must_use]
    pub fn contains_exact_key(&self, key: PublisherPublicKey) -> bool {
        match &self.mode {
            PublisherPolicyMode::Required(keys) => keys.contains(&key),
            PublisherPolicyMode::ExplicitlyInsecureUnsigned => false,
        }
    }

    /// Whether the policy explicitly permits an unsigned manifest.
    #[must_use]
    pub const fn permits_unsigned(&self) -> bool {
        matches!(self.mode, PublisherPolicyMode::ExplicitlyInsecureUnsigned)
    }

    /// Number of exact configured publisher trust anchors.
    #[must_use]
    pub fn key_count(&self) -> usize {
        match &self.mode {
            PublisherPolicyMode::Required(keys) => keys.len(),
            PublisherPolicyMode::ExplicitlyInsecureUnsigned => 0,
        }
    }

    fn sorted_fingerprints(&self) -> Vec<String> {
        let mut fingerprints = match &self.mode {
            PublisherPolicyMode::Required(keys) => keys
                .iter()
                .map(|key| key.fingerprint().to_string())
                .collect::<Vec<_>>(),
            PublisherPolicyMode::ExplicitlyInsecureUnsigned => Vec::new(),
        };
        fingerprints.sort_unstable();
        fingerprints
    }
}

impl fmt::Debug for PublisherPolicy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.mode {
            PublisherPolicyMode::Required(_) => formatter
                .debug_struct("RequiredPublisherPolicy")
                .field("key_count", &self.key_count())
                .field("fingerprints", &self.sorted_fingerprints())
                .finish(),
            PublisherPolicyMode::ExplicitlyInsecureUnsigned => {
                formatter.write_str("ExplicitlyInsecureUnsignedPublisherPolicy")
            }
        }
    }
}

fn decode_public_key(value: &str) -> Option<PublisherPublicKey> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        return None;
    }
    let mut decoded = [0u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        decoded[index] = decode_nibble(pair[0])? << 4 | decode_nibble(pair[1])?;
    }
    Some(PublisherPublicKey::from_bytes(decoded))
}

fn decode_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use distd_core::{
        chunks::ChunkAlgorithm,
        hash::hash,
        signature::{PublisherReleaseStatement, PublisherSignature},
    };
    use ring::signature::{Ed25519KeyPair, KeyPair};

    fn key_text(index: usize) -> String {
        format!("{index:064x}")
    }

    fn manifest() -> (Manifest, Vec<ChunkInfo>) {
        let bytes = b"publisher-authenticated client manifest";
        let chunk = ChunkInfo {
            hash: hash(bytes),
            size: bytes.len() as u64,
        };
        (
            Manifest {
                artifact_id: "client-artifact".to_string(),
                version: 7,
                root_hash: chunk.hash,
                total_size: chunk.size,
                chunk_count: 1,
                chunk_size: ChunkAlgorithm::Fixed256K.nominal_chunk_size(),
                chunk_algorithm: ChunkAlgorithm::Fixed256K,
                entries: Vec::new(),
            },
            vec![chunk],
        )
    }

    fn envelope(
        manifest: &Manifest,
        chunks: &[ChunkInfo],
        seed: [u8; 32],
    ) -> PublisherSignatureEnvelope {
        let pair = Ed25519KeyPair::from_seed_unchecked(&seed).unwrap();
        let statement = PublisherStatement::from_manifest(manifest, chunks).unwrap();
        PublisherSignatureEnvelope::from_parts(
            PublisherPublicKey::try_from(pair.public_key().as_ref()).unwrap(),
            PublisherSignature::try_from(pair.sign(statement.as_bytes()).as_ref()).unwrap(),
        )
    }

    fn release_envelope(
        manifest: &Manifest,
        chunks: &[ChunkInfo],
        seed: [u8; 32],
        sequence: PublisherReleaseSequence,
    ) -> PublisherReleaseEnvelope {
        let pair = Ed25519KeyPair::from_seed_unchecked(&seed).unwrap();
        let statement =
            PublisherReleaseStatement::from_manifest(manifest, chunks, sequence).unwrap();
        PublisherReleaseEnvelope::from_parts(
            PublisherPublicKey::try_from(pair.public_key().as_ref()).unwrap(),
            PublisherSignature::try_from(pair.sign(statement.as_bytes()).as_ref()).unwrap(),
        )
    }

    #[test]
    fn configuration_is_exact_bounded_and_mutually_exclusive() {
        assert_eq!(
            PublisherPolicy::from_config(&[], false).unwrap_err(),
            PublisherConfigError::MissingPolicy
        );
        assert!(PublisherPolicy::from_config(&[], true)
            .unwrap()
            .permits_unsigned());
        assert_eq!(
            PublisherPolicy::from_config(&[key_text(1)], true).unwrap_err(),
            PublisherConfigError::MixedPolicy
        );

        let exact = (0..MAX_PUBLISHER_KEYS).map(key_text).collect::<Vec<_>>();
        assert_eq!(
            PublisherPolicy::from_config(&exact, false)
                .unwrap()
                .key_count(),
            MAX_PUBLISHER_KEYS
        );
        let over = (0..=MAX_PUBLISHER_KEYS).map(key_text).collect::<Vec<_>>();
        assert_eq!(
            PublisherPolicy::from_config(&over, false).unwrap_err(),
            PublisherConfigError::TooManyKeys {
                maximum: MAX_PUBLISHER_KEYS
            }
        );
    }

    #[test]
    fn configuration_rejects_noncanonical_and_duplicate_keys() {
        for invalid in [
            String::new(),
            "0".repeat(63),
            "0".repeat(65),
            "A".repeat(64),
            "g".repeat(64),
        ] {
            assert_eq!(
                PublisherPolicy::from_config(&[invalid], false).unwrap_err(),
                PublisherConfigError::InvalidKey { index: 1 }
            );
        }
        assert_eq!(
            PublisherPolicy::from_config(&[key_text(9), key_text(9)], false).unwrap_err(),
            PublisherConfigError::DuplicateKey { index: 2 }
        );
    }

    #[test]
    fn required_policy_accepts_only_v2_and_checks_exact_key_before_signature() {
        let (manifest, chunks) = manifest();
        let sequence = PublisherReleaseSequence::new(7);
        let allowed = release_envelope(&manifest, &chunks, [7; 32], sequence);
        let other = release_envelope(&manifest, &chunks, [9; 32], sequence);
        let policy =
            PublisherPolicy::from_config(&[encode_key(allowed.public_key())], false).unwrap();
        let authorization = policy
            .authorize_transport_manifest(
                PublisherTransportAttestation::ReleaseV2 {
                    sequence,
                    envelope: allowed,
                },
                &manifest,
                &chunks,
            )
            .unwrap();
        assert_eq!(
            authorization.fingerprint(),
            Some(allowed.publisher_fingerprint())
        );
        assert_eq!(authorization.rollback().unwrap().sequence(), sequence);
        assert_eq!(
            policy.authorize_manifest(
                Some(&envelope(&manifest, &chunks, [7; 32])),
                &manifest,
                &chunks
            ),
            Err(PublisherAuthenticityError::MissingAttestation)
        );

        let unknown_with_invalid_signature = PublisherReleaseEnvelope::from_parts(
            other.public_key(),
            PublisherSignature::from_bytes([0; 64]),
        );
        assert_eq!(
            policy.authorize_transport_manifest(
                PublisherTransportAttestation::ReleaseV2 {
                    sequence,
                    envelope: unknown_with_invalid_signature,
                },
                &manifest,
                &chunks,
            ),
            Err(PublisherAuthenticityError::UntrustedPublisher)
        );
        let invalid_allowed = PublisherReleaseEnvelope::from_parts(
            allowed.public_key(),
            PublisherSignature::from_bytes([0; 64]),
        );
        assert_eq!(
            policy.authorize_transport_manifest(
                PublisherTransportAttestation::ReleaseV2 {
                    sequence,
                    envelope: invalid_allowed,
                },
                &manifest,
                &chunks,
            ),
            Err(PublisherAuthenticityError::InvalidSignature)
        );
    }

    #[test]
    fn explicit_unsigned_policy_never_ignores_a_present_invalid_signature() {
        let (manifest, chunks) = manifest();
        let policy = PublisherPolicy::from_config(&[], true).unwrap();
        assert_eq!(
            policy.authorize_manifest(None, &manifest, &chunks),
            Ok(None)
        );
        let signed = envelope(&manifest, &chunks, [13; 32]);
        assert_eq!(
            policy
                .authorize_manifest(Some(&signed), &manifest, &chunks)
                .unwrap(),
            Some(signed.publisher_fingerprint())
        );
        let invalid = PublisherSignatureEnvelope::from_parts(
            signed.public_key(),
            PublisherSignature::from_bytes([0; 64]),
        );
        assert_eq!(
            policy.authorize_manifest(Some(&invalid), &manifest, &chunks),
            Err(PublisherAuthenticityError::InvalidSignature)
        );

        let mut invalid_manifest = manifest.clone();
        invalid_manifest.total_size += 1;
        assert_eq!(
            policy.authorize_manifest(None, &invalid_manifest, &chunks),
            Err(PublisherAuthenticityError::InvalidSignature)
        );
    }

    #[test]
    fn debug_is_redacted_and_fingerprints_are_sorted() {
        let policy = PublisherPolicy::from_config(&[key_text(2), key_text(1)], false).unwrap();
        let mut expected = [key_text(2), key_text(1)]
            .map(|key| decode_public_key(&key).unwrap().fingerprint().to_string());
        expected.sort_unstable();
        let debug = format!("{policy:?}");
        assert!(debug.contains("key_count: 2"));
        let first = debug.find(&expected[0]).unwrap();
        let second = debug.find(&expected[1]).unwrap();
        assert!(first < second);
        assert!(!debug.contains(&key_text(1)));
        assert!(!debug.contains(&key_text(2)));
    }

    #[test]
    fn error_text_is_stable_and_contains_no_configured_material() {
        assert_eq!(
            PublisherAuthenticityError::MalformedAttestation.to_string(),
            "publisher attestation is malformed"
        );
        assert_eq!(
            PublisherAuthenticityError::IdentityMismatch.to_string(),
            "publisher artifact identity does not match the request"
        );
        assert_eq!(
            PublisherConfigError::InvalidKey { index: 3 }.to_string(),
            "publisher public key entry 3 is invalid"
        );
    }

    fn encode_key(key: PublisherPublicKey) -> String {
        key.as_bytes()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect()
    }
}

//! Publisher-authenticated immutable publication records and server policy.

use std::{collections::HashSet, fmt, sync::Arc};

use distd_core::{
    item::Item,
    signature::{
        PublisherKeyFingerprint, PublisherPublicKey, PublisherReleaseEnvelope,
        PublisherReleaseSequence, PublisherSignatureEnvelope, SignatureEnvelopeError,
    },
};
use thiserror::Error;

/// A publisher attestation retained with one immutable artifact revision.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PublicationAttestation {
    /// A rollback-protected V2 publisher release.
    ReleaseV2 {
        sequence: PublisherReleaseSequence,
        envelope: PublisherReleaseEnvelope,
    },
    /// A cryptographically valid but replayable V1 compatibility envelope.
    ReplayableV1(PublisherSignatureEnvelope),
    /// An explicitly insecure unsigned compatibility record.
    UnsignedLegacy,
}

/// Attestation supplied with one publication request before content
/// reconstruction makes it a verified immutable [`PublicationAttestation`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PublicationRequestAttestation {
    ReleaseV2 {
        sequence: PublisherReleaseSequence,
        envelope: PublisherReleaseEnvelope,
    },
    ReplayableV1(PublisherSignatureEnvelope),
    Unsigned,
}

impl PublicationAttestation {
    /// Return the rollback-protected V2 sequence and envelope, when present.
    #[must_use]
    pub const fn release(&self) -> Option<(PublisherReleaseSequence, &PublisherReleaseEnvelope)> {
        match self {
            Self::ReleaseV2 { sequence, envelope } => Some((*sequence, envelope)),
            Self::ReplayableV1(_) | Self::UnsignedLegacy => None,
        }
    }

    /// Return the replayable V1 envelope, when present.
    #[must_use]
    pub const fn replayable_envelope(&self) -> Option<&PublisherSignatureEnvelope> {
        match self {
            Self::ReplayableV1(envelope) => Some(envelope),
            Self::ReleaseV2 { .. } | Self::UnsignedLegacy => None,
        }
    }

    /// Return the stable publisher fingerprint when this record is signed.
    #[must_use]
    pub fn publisher_fingerprint(&self) -> Option<PublisherKeyFingerprint> {
        match self {
            Self::ReleaseV2 { envelope, .. } => Some(envelope.publisher_fingerprint()),
            Self::ReplayableV1(envelope) => Some(envelope.publisher_fingerprint()),
            Self::UnsignedLegacy => None,
        }
    }

    /// Whether this record carries rollback-protected publisher authority.
    #[must_use]
    pub const fn is_rollback_protected(&self) -> bool {
        matches!(self, Self::ReleaseV2 { .. })
    }

    /// Whether this is the explicit unsigned compatibility variant.
    #[must_use]
    pub const fn is_unsigned(&self) -> bool {
        matches!(self, Self::UnsignedLegacy)
    }
}

/// One immutable artifact revision and its inseparable publisher attestation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublishedRevision {
    item: Item,
    attestation: PublicationAttestation,
}

impl PublishedRevision {
    /// Construct a record while verifying any signed attestation against the
    /// complete typed Item. Unsigned construction remains explicit.
    pub fn new(item: Item, attestation: PublicationAttestation) -> Result<Self, PublicationError> {
        match attestation {
            PublicationAttestation::ReleaseV2 { sequence, envelope } => {
                envelope.verify(&item, sequence)?;
            }
            PublicationAttestation::ReplayableV1(envelope) => {
                envelope.verify(&item)?;
            }
            PublicationAttestation::UnsignedLegacy => {}
        }
        Ok(Self { item, attestation })
    }

    /// Construct an explicitly insecure unsigned compatibility record.
    #[must_use]
    pub const fn unsigned(item: Item) -> Self {
        Self {
            item,
            attestation: PublicationAttestation::UnsignedLegacy,
        }
    }

    /// Bind an already verified attestation to the storage-committed Item.
    ///
    /// The publication coordinator may use this only after policy verified
    /// the same prepared content and the storage commit changed solely the
    /// server-assigned revision fields, which are deliberately unsigned.
    #[must_use]
    pub(crate) const fn from_verified_parts(
        item: Item,
        attestation: PublicationAttestation,
    ) -> Self {
        Self { item, attestation }
    }

    /// The immutable typed artifact revision.
    #[must_use]
    pub const fn item(&self) -> &Item {
        &self.item
    }

    /// The immutable publisher attestation paired with this revision.
    #[must_use]
    pub const fn attestation(&self) -> &PublicationAttestation {
        &self.attestation
    }

    /// Consume the wrapper without exposing mutable access to either field.
    #[must_use]
    pub fn into_parts(self) -> (Item, PublicationAttestation) {
        (self.item, self.attestation)
    }
}

/// Server policy errors kept distinct from cryptographic envelope parsing.
#[non_exhaustive]
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum PublicationError {
    /// The exact raw publisher key is not in the required allowlist.
    #[error("publisher public key is not authorized")]
    UntrustedPublisher,
    /// Secure policy does not admit unsigned publication or metadata.
    #[error("unsigned publication is forbidden by publisher policy")]
    UnsignedForbidden,
    /// Secure rollback policy does not admit replayable V1 publication.
    #[error("replayable V1 publication is forbidden by publisher policy")]
    ReplayableForbidden,
    /// Immutable signed content already exists under a different envelope.
    #[error("publication attestation conflicts with the existing immutable revision")]
    AttestationConflict,
    /// The typed Item or signature does not form a valid publisher attestation.
    #[error("publisher signature is invalid: {0}")]
    InvalidSignature(#[from] SignatureEnvelopeError),
}

/// Exact-key publisher policy for new publication and durable compatibility.
#[derive(Clone)]
pub enum PublisherPolicy {
    /// Require a valid signature under one of these exact raw public keys.
    Required {
        keys: Arc<HashSet<PublisherPublicKey>>,
    },
    /// Explicit research compatibility mode permitting unsigned revisions.
    ExplicitlyInsecureUnsigned,
}

impl fmt::Debug for PublisherPolicy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Required { keys } => formatter
                .debug_struct("Required")
                .field("publisher_key_count", &keys.len())
                .finish_non_exhaustive(),
            Self::ExplicitlyInsecureUnsigned => formatter.write_str("ExplicitlyInsecureUnsigned"),
        }
    }
}

impl PublisherPolicy {
    /// Build a required policy from exact raw Ed25519 public keys.
    #[must_use]
    pub fn required(keys: impl IntoIterator<Item = PublisherPublicKey>) -> Self {
        Self::Required {
            keys: Arc::new(keys.into_iter().collect()),
        }
    }

    /// Build the visibly insecure unsigned compatibility policy.
    #[must_use]
    pub const fn explicitly_insecure_unsigned() -> Self {
        Self::ExplicitlyInsecureUnsigned
    }

    /// Verify and bind a rollback-protected V2 release. Required mode checks
    /// exact raw-key membership before cryptographic verification.
    pub fn authorize_release_new(
        &self,
        sequence: PublisherReleaseSequence,
        envelope: PublisherReleaseEnvelope,
        item: Item,
    ) -> Result<PublishedRevision, PublicationError> {
        if let Self::Required { keys } = self {
            if !keys.contains(&envelope.public_key()) {
                return Err(PublicationError::UntrustedPublisher);
            }
        }
        PublishedRevision::new(
            item,
            PublicationAttestation::ReleaseV2 { sequence, envelope },
        )
    }

    /// Verify and bind a replayable V1 publication. Required policy rejects
    /// this compatibility form.
    pub fn authorize_new(
        &self,
        envelope: PublisherSignatureEnvelope,
        item: Item,
    ) -> Result<PublishedRevision, PublicationError> {
        if matches!(self, Self::Required { .. }) {
            return Err(PublicationError::ReplayableForbidden);
        }
        PublishedRevision::new(item, PublicationAttestation::ReplayableV1(envelope))
    }

    /// Bind an unsigned new publication only in explicitly insecure mode.
    pub fn authorize_unsigned_new(
        &self,
        item: Item,
    ) -> Result<PublishedRevision, PublicationError> {
        match self {
            Self::Required { .. } => Err(PublicationError::UnsignedForbidden),
            Self::ExplicitlyInsecureUnsigned => Ok(PublishedRevision::unsigned(item)),
        }
    }

    /// Validate one loaded metadata record. Required policy binds every
    /// historical signed record to the exact current allowlist and rejects
    /// every unsigned record.
    pub fn validate_metadata_compatibility(
        &self,
        revision: &PublishedRevision,
    ) -> Result<(), PublicationError> {
        match revision.attestation() {
            PublicationAttestation::ReleaseV2 { sequence, envelope } => {
                if let Self::Required { keys } = self {
                    if !keys.contains(&envelope.public_key()) {
                        return Err(PublicationError::UntrustedPublisher);
                    }
                }
                envelope.verify(revision.item(), *sequence)?;
                Ok(())
            }
            PublicationAttestation::ReplayableV1(envelope) => match self {
                Self::Required { .. } => Err(PublicationError::ReplayableForbidden),
                Self::ExplicitlyInsecureUnsigned => {
                    envelope.verify(revision.item())?;
                    Ok(())
                }
            },
            PublicationAttestation::UnsignedLegacy => match self {
                Self::Required { .. } => Err(PublicationError::UnsignedForbidden),
                Self::ExplicitlyInsecureUnsigned => Ok(()),
            },
        }
    }

    /// Fail fast on request authority before reading or preparing a body.
    pub fn precheck_request(
        &self,
        attestation: &PublicationRequestAttestation,
    ) -> Result<(), PublicationError> {
        match (self, attestation) {
            (
                Self::Required { keys },
                PublicationRequestAttestation::ReleaseV2 { envelope, .. },
            ) => {
                if keys.contains(&envelope.public_key()) {
                    Ok(())
                } else {
                    Err(PublicationError::UntrustedPublisher)
                }
            }
            (Self::Required { .. }, PublicationRequestAttestation::ReplayableV1(_)) => {
                Err(PublicationError::ReplayableForbidden)
            }
            (Self::Required { .. }, PublicationRequestAttestation::Unsigned) => {
                Err(PublicationError::UnsignedForbidden)
            }
            (Self::ExplicitlyInsecureUnsigned, _) => Ok(()),
        }
    }

    /// Verify one request against its reconstructed typed Item.
    pub fn authorize_request(
        &self,
        attestation: PublicationRequestAttestation,
        item: Item,
    ) -> Result<PublishedRevision, PublicationError> {
        match attestation {
            PublicationRequestAttestation::ReleaseV2 { sequence, envelope } => {
                self.authorize_release_new(sequence, envelope, item)
            }
            PublicationRequestAttestation::ReplayableV1(envelope) => {
                self.authorize_new(envelope, item)
            }
            PublicationRequestAttestation::Unsigned => self.authorize_unsigned_new(item),
        }
    }

    /// Test exact raw-key membership without granting authority by fingerprint.
    #[must_use]
    pub fn contains_exact_key(&self, key: PublisherPublicKey) -> bool {
        match self {
            Self::Required { keys } => keys.contains(&key),
            Self::ExplicitlyInsecureUnsigned => false,
        }
    }

    /// Whether this policy visibly permits unsigned compatibility records.
    #[must_use]
    pub const fn permits_unsigned(&self) -> bool {
        matches!(self, Self::ExplicitlyInsecureUnsigned)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Bytes;
    use distd_core::{
        chunk_storage::{hashmap_storage::HashMapStorage, ChunkStorage},
        chunks::ChunkAlgorithm,
        signature::{PublisherReleaseStatement, PublisherSignature, PublisherStatement},
    };
    use ring::signature::{Ed25519KeyPair, KeyPair};
    use std::path::PathBuf;

    fn item() -> Item {
        HashMapStorage::default()
            .prepare_revision(
                "signed".to_string(),
                "Signed".to_string(),
                PathBuf::from("signed.bin"),
                0,
                None,
                Bytes::from_static(b"signed publication"),
                ChunkAlgorithm::Fixed256K,
            )
            .unwrap()
    }

    fn envelope(item: &Item, seed: [u8; 32]) -> PublisherSignatureEnvelope {
        let pair = Ed25519KeyPair::from_seed_unchecked(&seed).unwrap();
        let statement = PublisherStatement::from_item(item).unwrap();
        PublisherSignatureEnvelope::from_parts(
            PublisherPublicKey::try_from(pair.public_key().as_ref()).unwrap(),
            PublisherSignature::try_from(pair.sign(statement.as_bytes()).as_ref()).unwrap(),
        )
    }

    fn release_envelope(
        item: &Item,
        seed: [u8; 32],
        sequence: PublisherReleaseSequence,
    ) -> PublisherReleaseEnvelope {
        let pair = Ed25519KeyPair::from_seed_unchecked(&seed).unwrap();
        let statement = PublisherReleaseStatement::from_item(item, sequence).unwrap();
        PublisherReleaseEnvelope::from_parts(
            PublisherPublicKey::try_from(pair.public_key().as_ref()).unwrap(),
            PublisherSignature::try_from(pair.sign(statement.as_bytes()).as_ref()).unwrap(),
        )
    }

    #[test]
    fn required_policy_uses_exact_keys_for_new_and_historical_revisions() {
        let item = item();
        let sequence = PublisherReleaseSequence::new(7);
        let allowed = release_envelope(&item, [7; 32], sequence);
        let other = release_envelope(&item, [9; 32], sequence);
        let policy = PublisherPolicy::required([allowed.public_key()]);
        let published = policy
            .authorize_release_new(sequence, allowed, item.clone())
            .unwrap();
        assert_eq!(published.item(), &item);
        assert_eq!(
            published.attestation().release(),
            Some((sequence, &allowed))
        );
        assert_eq!(
            policy.authorize_release_new(sequence, other, item.clone()),
            Err(PublicationError::UntrustedPublisher)
        );

        let rotated_policy = PublisherPolicy::required([other.public_key()]);
        assert_eq!(
            rotated_policy.validate_metadata_compatibility(&published),
            Err(PublicationError::UntrustedPublisher)
        );
        policy.validate_metadata_compatibility(&published).unwrap();

        assert_eq!(
            policy.authorize_new(envelope(&item, [7; 32]), item),
            Err(PublicationError::ReplayableForbidden)
        );
    }

    #[test]
    fn unsigned_records_require_the_explicit_insecure_policy() {
        let item = item();
        let secure = PublisherPolicy::required([]);
        assert_eq!(
            secure.authorize_unsigned_new(item.clone()),
            Err(PublicationError::UnsignedForbidden)
        );
        assert_eq!(
            secure.validate_metadata_compatibility(&PublishedRevision::unsigned(item.clone())),
            Err(PublicationError::UnsignedForbidden)
        );

        let insecure = PublisherPolicy::explicitly_insecure_unsigned();
        let published = insecure.authorize_unsigned_new(item).unwrap();
        assert!(published.attestation().is_unsigned());
        insecure
            .validate_metadata_compatibility(&published)
            .unwrap();
    }

    #[test]
    fn attestation_conflict_message_does_not_disclose_signature_material() {
        assert_eq!(
            PublicationError::AttestationConflict.to_string(),
            "publication attestation conflicts with the existing immutable revision"
        );
    }

    #[test]
    fn required_policy_debug_is_redacted() {
        let item = item();
        let envelope = envelope(&item, [0xabu8; 32]);
        let debug = format!("{:?}", PublisherPolicy::required([envelope.public_key()]));
        assert_eq!(debug, "Required { publisher_key_count: 1, .. }");
        assert!(!debug.contains(&format!("{:?}", envelope.public_key().as_bytes())));
    }
}

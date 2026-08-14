//! Publisher-authenticated immutable publication records and server policy.

use std::{collections::HashSet, fmt, sync::Arc};

use distd_core::{
    item::Item,
    signature::{
        PublisherKeyFingerprint, PublisherPublicKey, PublisherSignatureEnvelope,
        SignatureEnvelopeError,
    },
};
use thiserror::Error;

/// A publisher attestation retained with one immutable artifact revision.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PublicationAttestation {
    /// A cryptographically valid V1 publisher signature envelope.
    Signed(PublisherSignatureEnvelope),
    /// An explicitly insecure unsigned compatibility record.
    Unsigned,
}

impl PublicationAttestation {
    /// Return the signed V1 envelope, if this is an authenticated record.
    #[must_use]
    pub const fn envelope(&self) -> Option<&PublisherSignatureEnvelope> {
        match self {
            Self::Signed(envelope) => Some(envelope),
            Self::Unsigned => None,
        }
    }

    /// Return the stable publisher fingerprint when this record is signed.
    #[must_use]
    pub fn publisher_fingerprint(&self) -> Option<PublisherKeyFingerprint> {
        self.envelope()
            .map(PublisherSignatureEnvelope::publisher_fingerprint)
    }

    /// Whether this is the explicit unsigned compatibility variant.
    #[must_use]
    pub const fn is_unsigned(&self) -> bool {
        matches!(self, Self::Unsigned)
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
        if let PublicationAttestation::Signed(envelope) = attestation {
            envelope.verify(&item)?;
        }
        Ok(Self { item, attestation })
    }

    /// Construct an explicitly insecure unsigned compatibility record.
    #[must_use]
    pub const fn unsigned(item: Item) -> Self {
        Self {
            item,
            attestation: PublicationAttestation::Unsigned,
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
    /// Immutable signed content already exists under a different envelope.
    #[error("publication attestation conflicts with the existing immutable revision")]
    AttestationConflict,
    /// The typed Item or signature does not form a valid V1 attestation.
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

    /// Verify and bind a signed new publication. Required mode checks exact
    /// raw-key membership before performing cryptographic verification.
    pub fn authorize_new(
        &self,
        envelope: PublisherSignatureEnvelope,
        item: Item,
    ) -> Result<PublishedRevision, PublicationError> {
        if let Self::Required { keys } = self {
            if !keys.contains(&envelope.public_key()) {
                return Err(PublicationError::UntrustedPublisher);
            }
        }
        PublishedRevision::new(item, PublicationAttestation::Signed(envelope))
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
            PublicationAttestation::Signed(envelope) => {
                if let Self::Required { keys } = self {
                    if !keys.contains(&envelope.public_key()) {
                        return Err(PublicationError::UntrustedPublisher);
                    }
                }
                envelope.verify(revision.item())?;
                Ok(())
            }
            PublicationAttestation::Unsigned => match self {
                Self::Required { .. } => Err(PublicationError::UnsignedForbidden),
                Self::ExplicitlyInsecureUnsigned => Ok(()),
            },
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
        signature::{PublisherSignature, PublisherStatement},
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

    #[test]
    fn required_policy_uses_exact_keys_for_new_and_historical_revisions() {
        let item = item();
        let allowed = envelope(&item, [7; 32]);
        let other = envelope(&item, [9; 32]);
        let policy = PublisherPolicy::required([allowed.public_key()]);
        let published = policy.authorize_new(allowed, item.clone()).unwrap();
        assert_eq!(published.item(), &item);
        assert_eq!(published.attestation().envelope(), Some(&allowed));
        assert_eq!(
            policy.authorize_new(other, item.clone()),
            Err(PublicationError::UntrustedPublisher)
        );

        let rotated_policy = PublisherPolicy::required([other.public_key()]);
        assert_eq!(
            rotated_policy.validate_metadata_compatibility(&published),
            Err(PublicationError::UntrustedPublisher)
        );
        policy.validate_metadata_compatibility(&published).unwrap();
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

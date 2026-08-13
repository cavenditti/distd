//! Shared, fail-closed provenance validation for every driver phase.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Component, Path};

use serde::{de::DeserializeOwned, Serialize};

use super::evidence::{self, canonical_json_line};
use super::model::{
    ArtifactInput, ArtifactOrigin, ArtifactProvenance, ArtifactRole, FileInput, HarnessContract,
    LegacyEvidence, Plan, ProvenancePolicy, ProvenanceStatus, ProvenanceStatusPolicy,
    ReleaseProfileIdentity, RepositoryIdentity, Request, ReviewedPatch, ReviewedPatchFile,
    ReviewedPatchFileIdentity, ReviewedPatchManifest, ReviewedPatchOperation, SourceBundle,
    SourceManifest, SourceManifestEntry, ARTIFACT_PROVENANCE_SCHEMA, BASELINE_ARTIFACT_SHA256,
    BASELINE_LEGACY_BUILD_RESULT_SHA256, BASELINE_LEGACY_ENVIRONMENT_SHA256,
    BASELINE_LEGACY_PREREGISTRATION_SHA256, BASELINE_LEGACY_PRIMARY_SHA256,
    BASELINE_LEGACY_SECONDARY_SHA256, CANDIDATE_ARTIFACT_SHA256, CANDIDATE_LEGACY_PRIMARY_SHA256,
    CANDIDATE_LEGACY_SECONDARY_SHA256, CANDIDATE_SOURCE_BUNDLE_SHA256, PLAN_SCHEMA,
    RELEASE_PROFILE_SCHEMA, REQUEST_SCHEMA, REVIEWED_PATCH_SCHEMA, SOURCE_BUNDLE_SCHEMA,
    SOURCE_MANIFEST_SCHEMA,
};

const HISTORICAL_HEAD: &str = "4dbe52ca6f6b572eef05f89f571e627232a7b2f6";
const HISTORICAL_LOCK_SHA256: &str =
    "ad953daf188f5f11c633a7efb8a8f548310a44af9b9b5893872058db8bfcd356";
const HISTORICAL_QUIC_SHA256: &str =
    "b39c61c2592770ee50a504ce2316135a38c527c343d93862d6b7ad53da4393db";
const HISTORICAL_RUSTC_RECEIPT_SHA256: &str =
    "aea7c77c2b0d24444d07240e4d785e4ba73b6c9e786748af2ddc22bbf4c104bf";
const HISTORICAL_CARGO_RECEIPT_SHA256: &str =
    "9db411a77dc69b743e7aa51da9aafff8b31e7aaf1afc54ae1fdb5635fc8dc7cd";
const BASELINE_DIFF_SHA256: &str =
    "246c8ad9efecff0267b72b50f2aabff8f3742139c587c4e64478757fe60aab91";
const CANDIDATE_DIFF_SHA256: &str =
    "fb536d79314f0b2d6632c0c7c3ba19586028b0e72efc547e540751bd1e64ffcf";
const HISTORICAL_RUSTC_RELEASE: &str = "1.96.0";
const HISTORICAL_RUSTC_COMMIT: &str = "ac68faa20c58cbccd01ee7208bf3b6e93a7d7f96";
const HISTORICAL_CARGO_RELEASE: &str = "1.96.0";
const HISTORICAL_TARGET: &str = "aarch64-apple-darwin";
const BENCHMARK_TEST: &str = "quic::tests::manual_quic_authenticated_fetch_handler_5000";
const CANDIDATE_ACCEPTED_C3_SHA256: &str =
    "59cab348c6cb7b07e51b6cc51a040d0beba06e475901e7b21b6390b619ae8208";
const ALLOWED_BUILD_ENV: [&str; 5] = [
    "CARGO_HOME",
    "CARGO_TARGET_DIR",
    "RUSTFLAGS",
    "CARGO_ENCODED_RUSTFLAGS",
    "RUSTUP_TOOLCHAIN",
];
pub const MAX_PROVENANCE_INPUT_BYTES: u64 = 64 * 1024 * 1024;
pub const MAX_SOURCE_BUNDLE_ENTRIES: usize = 4_096;
pub const MAX_SOURCE_ENTRY_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_SOURCE_BUNDLE_CONTENT_BYTES: usize = 32 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ValidationClass {
    Schema,
    Harness,
    Path,
    Input,
    Anchor,
    Source,
    Patch,
    Toolchain,
    Build,
    RepositoryRelation,
}

#[derive(Debug, thiserror::Error)]
#[error("{class:?}: {detail}")]
pub struct ProvenanceError {
    pub class: ValidationClass,
    pub detail: String,
}

impl ProvenanceError {
    fn new(class: ValidationClass, detail: impl Into<String>) -> Self {
        Self {
            class,
            detail: detail.into(),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct ProvenanceValidationPolicy<'a> {
    baseline_artifact_sha256: &'a str,
    candidate_artifact_sha256: &'a str,
    baseline_legacy_primary_sha256: &'a str,
    baseline_legacy_secondary_sha256: &'a str,
    baseline_legacy_environment_sha256: Option<&'a str>,
    baseline_legacy_preregistration_sha256: Option<&'a str>,
    baseline_legacy_build_result_sha256: Option<&'a str>,
    candidate_legacy_primary_sha256: &'a str,
    candidate_legacy_secondary_sha256: &'a str,
    baseline_source_bundle_sha256: Option<&'a str>,
    candidate_source_bundle_sha256: &'a str,
    require_historical_semantics: bool,
}

impl ProvenanceValidationPolicy<'static> {
    #[must_use]
    pub const fn production() -> Self {
        Self {
            baseline_artifact_sha256: BASELINE_ARTIFACT_SHA256,
            candidate_artifact_sha256: CANDIDATE_ARTIFACT_SHA256,
            baseline_legacy_primary_sha256: BASELINE_LEGACY_PRIMARY_SHA256,
            baseline_legacy_secondary_sha256: BASELINE_LEGACY_SECONDARY_SHA256,
            baseline_legacy_environment_sha256: Some(BASELINE_LEGACY_ENVIRONMENT_SHA256),
            baseline_legacy_preregistration_sha256: Some(BASELINE_LEGACY_PREREGISTRATION_SHA256),
            baseline_legacy_build_result_sha256: Some(BASELINE_LEGACY_BUILD_RESULT_SHA256),
            candidate_legacy_primary_sha256: CANDIDATE_LEGACY_PRIMARY_SHA256,
            candidate_legacy_secondary_sha256: CANDIDATE_LEGACY_SECONDARY_SHA256,
            baseline_source_bundle_sha256: None,
            candidate_source_bundle_sha256: CANDIDATE_SOURCE_BUNDLE_SHA256,
            require_historical_semantics: true,
        }
    }
}

impl<'a> ProvenanceValidationPolicy<'a> {
    /// Explicit test-policy injection; production never derives anchors from the request.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub const fn synthetic(
        baseline_artifact_sha256: &'a str,
        candidate_artifact_sha256: &'a str,
        baseline_legacy_primary_sha256: &'a str,
        baseline_legacy_secondary_sha256: &'a str,
        baseline_legacy_environment_sha256: Option<&'a str>,
        baseline_legacy_preregistration_sha256: Option<&'a str>,
        baseline_legacy_build_result_sha256: Option<&'a str>,
        candidate_legacy_primary_sha256: &'a str,
        candidate_legacy_secondary_sha256: &'a str,
        baseline_source_bundle_sha256: &'a str,
        candidate_source_bundle_sha256: &'a str,
    ) -> Self {
        Self {
            baseline_artifact_sha256,
            candidate_artifact_sha256,
            baseline_legacy_primary_sha256,
            baseline_legacy_secondary_sha256,
            baseline_legacy_environment_sha256,
            baseline_legacy_preregistration_sha256,
            baseline_legacy_build_result_sha256,
            candidate_legacy_primary_sha256,
            candidate_legacy_secondary_sha256,
            baseline_source_bundle_sha256: Some(baseline_source_bundle_sha256),
            candidate_source_bundle_sha256,
            require_historical_semantics: false,
        }
    }

    #[must_use]
    pub const fn baseline_artifact_sha256(self) -> &'a str {
        self.baseline_artifact_sha256
    }

    #[must_use]
    pub const fn candidate_artifact_sha256(self) -> &'a str {
        self.candidate_artifact_sha256
    }

    #[must_use]
    pub const fn baseline_legacy_primary_sha256(self) -> &'a str {
        self.baseline_legacy_primary_sha256
    }

    #[must_use]
    pub const fn baseline_legacy_secondary_sha256(self) -> &'a str {
        self.baseline_legacy_secondary_sha256
    }

    #[must_use]
    pub const fn candidate_legacy_primary_sha256(self) -> &'a str {
        self.candidate_legacy_primary_sha256
    }

    #[must_use]
    pub const fn candidate_legacy_secondary_sha256(self) -> &'a str {
        self.candidate_legacy_secondary_sha256
    }

    #[must_use]
    pub const fn baseline_legacy_environment_sha256(self) -> Option<&'a str> {
        self.baseline_legacy_environment_sha256
    }

    #[must_use]
    pub const fn baseline_legacy_preregistration_sha256(self) -> Option<&'a str> {
        self.baseline_legacy_preregistration_sha256
    }

    #[must_use]
    pub const fn baseline_legacy_build_result_sha256(self) -> Option<&'a str> {
        self.baseline_legacy_build_result_sha256
    }

    #[must_use]
    pub const fn baseline_source_bundle_sha256(self) -> Option<&'a str> {
        self.baseline_source_bundle_sha256
    }

    #[must_use]
    pub const fn candidate_source_bundle_sha256(self) -> &'a str {
        self.candidate_source_bundle_sha256
    }
}

#[derive(Clone, Debug)]
pub struct ValidatedProvenance {
    pub baseline_manifest: SourceManifest,
    pub candidate_manifest: SourceManifest,
    pub current_manifest: SourceManifest,
}

pub fn parse_canonical_request(bytes: &[u8]) -> Result<Request, ProvenanceError> {
    parse_canonical_json(bytes, "request")
}

pub fn canonical_request_bytes(request: &Request) -> Result<Vec<u8>, ProvenanceError> {
    canonical_json_line(request)
        .map_err(|error| ProvenanceError::new(ValidationClass::Schema, error.to_string()))
}

pub fn validate_request(
    request: &Request,
    policy: ProvenanceValidationPolicy<'_>,
) -> Result<ValidatedProvenance, ProvenanceError> {
    if request.schema != REQUEST_SCHEMA {
        return Err(ProvenanceError::new(
            ValidationClass::Schema,
            "unsupported request schema",
        ));
    }
    validate_common(
        &request.repository,
        &request.baseline,
        &request.candidate,
        &request.provenance_policy,
        &request.reviewed_patch,
        &request.harness,
        policy,
    )
}

pub fn validate_plan(
    plan: &Plan,
    policy: ProvenanceValidationPolicy<'_>,
) -> Result<ValidatedProvenance, ProvenanceError> {
    if plan.schema != PLAN_SCHEMA {
        return Err(ProvenanceError::new(
            ValidationClass::Schema,
            "unsupported plan schema",
        ));
    }
    let validated = validate_common(
        &plan.repository,
        &plan.baseline,
        &plan.candidate,
        &plan.provenance_policy,
        &plan.reviewed_patch,
        &plan.harness,
        policy,
    )?;
    let request: Request = read_canonical_input(&plan.composed_request, "composed request")?;
    if request.schema != REQUEST_SCHEMA || request.harness != HarnessContract::fixed() {
        return Err(error(
            ValidationClass::Schema,
            "composed request schema or harness is invalid",
        ));
    }
    validate_request_policy(&request.provenance_policy, policy)?;
    validate_plan_request_relation(plan, &request)?;
    Ok(validated)
}

fn validate_plan_request_relation(plan: &Plan, request: &Request) -> Result<(), ProvenanceError> {
    if plan.repository.root != request.repository.root
        || plan.repository.head != request.repository.head
        || repository_hashes(&plan.repository) != repository_hashes(&request.repository)
        || !artifact_semantics_equal(&plan.baseline, &request.baseline)
        || !artifact_semantics_equal(&plan.candidate, &request.candidate)
        || plan.provenance_policy != request.provenance_policy
        || plan.reviewed_patch.record != request.reviewed_patch.record
        || plan.reviewed_patch.manifest.sha256 != request.reviewed_patch.manifest.sha256
        || plan.harness != request.harness
        || plan.blocked_evidence != request.blocked_evidence
        || plan.evidence_root != request.evidence_root
    {
        return Err(error(
            ValidationClass::Input,
            "sealed plan is not a hash-preserving relocation of the composed request",
        ));
    }
    Ok(())
}

fn repository_hashes(repository: &RepositoryIdentity) -> [&str; 6] {
    [
        &repository.source_diff_sha256,
        &repository.source_status_sha256,
        &repository.source_manifest_sha256,
        &repository.cargo_lock_sha256,
        &repository.quic_source_sha256,
        &repository.task_spec_sha256,
    ]
}

fn artifact_semantics_equal(left: &ArtifactInput, right: &ArtifactInput) -> bool {
    left.provenance == right.provenance
        && left.cargo_config.is_some() == right.cargo_config.is_some()
        && artifact_inputs(left)
            .iter()
            .map(|(_, input)| input.sha256.as_str())
            .eq(artifact_inputs(right)
                .iter()
                .map(|(_, input)| input.sha256.as_str()))
}

fn validate_common(
    repository: &RepositoryIdentity,
    baseline: &ArtifactInput,
    candidate: &ArtifactInput,
    request_policy: &ProvenancePolicy,
    reviewed_patch: &ReviewedPatch,
    harness: &HarnessContract,
    policy: ProvenanceValidationPolicy<'_>,
) -> Result<ValidatedProvenance, ProvenanceError> {
    require_anchor_availability(policy)?;
    if harness != &HarnessContract::fixed() {
        return Err(ProvenanceError::new(
            ValidationClass::Harness,
            "harness differs from the compiled contract",
        ));
    }
    validate_request_policy(request_policy, policy)?;
    let baseline_manifest =
        validate_artifact(baseline, ArtifactRole::Baseline, request_policy, policy)?;
    let candidate_manifest =
        validate_artifact(candidate, ArtifactRole::Candidate, request_policy, policy)?;
    let patch: ReviewedPatchManifest =
        read_canonical_input(&reviewed_patch.manifest, "reviewed patch")?;
    if patch != reviewed_patch.record {
        return Err(error(
            ValidationClass::Patch,
            "reviewed patch bytes differ from embedded record",
        ));
    }
    validate_patch(
        &patch,
        baseline,
        candidate,
        &baseline_manifest,
        &candidate_manifest,
    )?;
    let current_manifest = read_repository_manifest(repository)?;
    validate_product_projection(&current_manifest, &candidate_manifest)?;
    Ok(ValidatedProvenance {
        baseline_manifest,
        candidate_manifest,
        current_manifest,
    })
}

fn require_anchor_availability(
    policy: ProvenanceValidationPolicy<'_>,
) -> Result<(), ProvenanceError> {
    if policy.baseline_source_bundle_sha256.is_none() {
        return Err(error(
            ValidationClass::Anchor,
            "baseline build-source bundle anchor is unavailable; production remains blocked",
        ));
    }
    Ok(())
}

fn validate_request_policy(
    request: &ProvenancePolicy,
    policy: ProvenanceValidationPolicy<'_>,
) -> Result<(), ProvenanceError> {
    let expected = ProvenancePolicy {
        baseline_origin: ArtifactOrigin::RecoveredFrozen,
        candidate_origin: ArtifactOrigin::RecoveredFrozen,
        status: ProvenanceStatusPolicy::EvidenceDerivedOnly,
        baseline_artifact_sha256: policy.baseline_artifact_sha256.to_owned(),
        candidate_artifact_sha256: policy.candidate_artifact_sha256.to_owned(),
        baseline_legacy_primary_sha256: policy.baseline_legacy_primary_sha256.to_owned(),
        baseline_legacy_secondary_sha256: policy.baseline_legacy_secondary_sha256.to_owned(),
        baseline_legacy_environment_sha256: policy
            .baseline_legacy_environment_sha256
            .map(str::to_owned),
        baseline_legacy_preregistration_sha256: policy
            .baseline_legacy_preregistration_sha256
            .map(str::to_owned),
        baseline_legacy_build_result_sha256: policy
            .baseline_legacy_build_result_sha256
            .map(str::to_owned),
        candidate_legacy_primary_sha256: policy.candidate_legacy_primary_sha256.to_owned(),
        candidate_legacy_secondary_sha256: policy.candidate_legacy_secondary_sha256.to_owned(),
        baseline_source_bundle_sha256: policy.baseline_source_bundle_sha256.map(str::to_owned),
        candidate_source_bundle_sha256: policy.candidate_source_bundle_sha256.to_owned(),
    };
    if request != &expected {
        return Err(error(
            ValidationClass::Anchor,
            "request policy differs from the independently supplied anchors",
        ));
    }
    Ok(())
}

#[allow(clippy::too_many_lines)]
fn validate_artifact(
    artifact: &ArtifactInput,
    role: ArtifactRole,
    request_policy: &ProvenancePolicy,
    policy: ProvenanceValidationPolicy<'_>,
) -> Result<SourceManifest, ProvenanceError> {
    for (name, input) in artifact_inputs(artifact) {
        validate_file_input(input, name)?;
    }
    let receipt: ArtifactProvenance =
        read_canonical_input(&artifact.provenance_receipt, "provenance receipt")?;
    if receipt != artifact.provenance
        || receipt.schema != ARTIFACT_PROVENANCE_SCHEMA
        || receipt.role != role
    {
        return Err(error(
            ValidationClass::Schema,
            "provenance receipt schema, role, or embedded record differs",
        ));
    }
    let (
        expected_artifact,
        expected_primary,
        expected_secondary,
        expected_environment,
        expected_preregistration,
        expected_build_result,
    ) = match role {
        ArtifactRole::Baseline => (
            policy.baseline_artifact_sha256,
            policy.baseline_legacy_primary_sha256,
            policy.baseline_legacy_secondary_sha256,
            policy.baseline_legacy_environment_sha256,
            policy.baseline_legacy_preregistration_sha256,
            policy.baseline_legacy_build_result_sha256,
        ),
        ArtifactRole::Candidate => (
            policy.candidate_artifact_sha256,
            policy.candidate_legacy_primary_sha256,
            policy.candidate_legacy_secondary_sha256,
            None,
            None,
            None,
        ),
    };
    let status_matches_policy = match request_policy.status {
        ProvenanceStatusPolicy::EvidenceDerivedOnly => {
            matches!(&receipt.status, ProvenanceStatus::EvidenceDerived { .. })
        }
        ProvenanceStatusPolicy::FutureReconstructedWithAuthority => {
            matches!(&receipt.status, ProvenanceStatus::Reconstructed { .. })
        }
        ProvenanceStatusPolicy::OriginalOnly => false,
    };
    if receipt.artifact_sha256 != expected_artifact
        || artifact.executable.sha256 != expected_artifact
        || receipt.origin != ArtifactOrigin::RecoveredFrozen
        || !status_matches_policy
        || (policy.require_historical_semantics
            && !matches!(
                &receipt.status,
                ProvenanceStatus::EvidenceDerived {
                    derivation: super::model::EvidenceDerivation::ByteExactPayloadsFromDurableSessionLogOriginalPathsAndInodesUnavailable
                }
            ))
    {
        return Err(error(
            ValidationClass::Anchor,
            "artifact, origin, or evidence-derived status differs from policy",
        ));
    }
    if artifact.legacy_evidence.primary.sha256 != expected_primary
        || artifact.legacy_evidence.secondary.sha256 != expected_secondary
        || artifact
            .legacy_evidence
            .environment
            .as_ref()
            .map(|input| input.sha256.as_str())
            != expected_environment
        || artifact
            .legacy_evidence
            .preregistration
            .as_ref()
            .map(|input| input.sha256.as_str())
            != expected_preregistration
        || artifact
            .legacy_evidence
            .build_result
            .as_ref()
            .map(|input| input.sha256.as_str())
            != expected_build_result
    {
        return Err(error(
            ValidationClass::Anchor,
            "legacy evidence differs from independent immutable anchors",
        ));
    }
    if role == ArtifactRole::Baseline {
        let Some(expected_bundle) = policy.baseline_source_bundle_sha256 else {
            return Err(error(
                ValidationClass::Anchor,
                "baseline build-source bundle anchor is unavailable; production remains blocked",
            ));
        };
        if artifact.source_bundle.sha256 != expected_bundle {
            return Err(error(
                ValidationClass::Anchor,
                "baseline build-source bundle differs from its independent anchor",
            ));
        }
    } else if artifact.source_bundle.sha256 != policy.candidate_source_bundle_sha256 {
        return Err(error(
            ValidationClass::Anchor,
            "candidate build-source bundle differs from its independent commit-derived anchor",
        ));
    }
    let source = derive_manifest_from_bundle(&artifact.source_bundle)?;
    if source.repository_head != receipt.source.repository_head {
        return Err(error(
            ValidationClass::Source,
            "source bundle HEAD differs from the structured provenance receipt",
        ));
    }
    let recorded: SourceManifest =
        read_canonical_input(&artifact.source_manifest, "source manifest")?;
    if source != recorded
        || artifact.source_bundle.sha256 != receipt.source.source_bundle_sha256
        || artifact.source_manifest.sha256 != receipt.source.source_manifest_sha256
        || artifact.source_snapshot.sha256 != receipt.source.source_snapshot_sha256
        || artifact.benchmark_source.sha256 != receipt.source.benchmark_source_sha256
        || artifact.cargo_lock.sha256 != receipt.dependencies.cargo_lock_sha256
        || artifact.workspace_manifest.sha256 != receipt.dependencies.workspace_manifest_sha256
        || artifact.package_manifest.sha256 != receipt.dependencies.package_manifest_sha256
        || artifact.cargo_config.as_ref().map(|file| &file.sha256)
            != receipt.dependencies.cargo_config_sha256.as_ref()
        || artifact.rustc_verbose.sha256 != receipt.toolchain.rustc_verbose_sha256
        || artifact.cargo_verbose.sha256 != receipt.toolchain.cargo_verbose_sha256
        || artifact.build_environment.sha256 != receipt.build.environment_sha256
        || artifact.profile_identity.sha256 != receipt.build.profile_identity_sha256
    {
        return Err(error(
            ValidationClass::Input,
            "supporting inputs do not match structured provenance or derived source manifest",
        ));
    }
    require_bundle_input(&source, "server/src/quic.rs", &artifact.benchmark_source)?;
    require_bundle_input(&source, "Cargo.toml", &artifact.workspace_manifest)?;
    require_bundle_input(&source, "server/Cargo.toml", &artifact.package_manifest)?;
    validate_cargo_config_binding(
        &source,
        artifact.cargo_config.as_ref(),
        policy.require_historical_semantics,
    )?;
    if artifact.cargo_lock.sha256 != HISTORICAL_LOCK_SHA256 && policy.require_historical_semantics {
        return Err(error(
            ValidationClass::Anchor,
            "historical Cargo.lock differs from legacy receipt anchor",
        ));
    }
    if policy.require_historical_semantics
        && (artifact.rustc_verbose.sha256 != HISTORICAL_RUSTC_RECEIPT_SHA256
            || artifact.cargo_verbose.sha256 != HISTORICAL_CARGO_RECEIPT_SHA256)
    {
        return Err(error(
            ValidationClass::Anchor,
            "toolchain receipt bytes differ from immutable historical anchors",
        ));
    }
    validate_release_profile(&artifact.profile_identity)?;
    if policy.require_historical_semantics && role == ArtifactRole::Candidate {
        validate_candidate_source_profile(&artifact.source_bundle)?;
    }
    validate_toolchain_and_build(artifact, role, policy)?;
    validate_legacy_evidence(
        role,
        &artifact.legacy_evidence,
        &receipt,
        request_policy,
        &source,
        policy,
    )?;
    Ok(source)
}

fn artifact_inputs(artifact: &ArtifactInput) -> Vec<(&'static str, &FileInput)> {
    let mut result = vec![
        ("executable", &artifact.executable),
        ("provenance receipt", &artifact.provenance_receipt),
        ("source snapshot", &artifact.source_snapshot),
        ("source manifest", &artifact.source_manifest),
        ("source bundle", &artifact.source_bundle),
        ("legacy primary", &artifact.legacy_evidence.primary),
        ("legacy secondary", &artifact.legacy_evidence.secondary),
        ("benchmark source", &artifact.benchmark_source),
        ("Cargo.lock", &artifact.cargo_lock),
        ("workspace manifest", &artifact.workspace_manifest),
        ("package manifest", &artifact.package_manifest),
        ("rustc receipt", &artifact.rustc_verbose),
        ("cargo receipt", &artifact.cargo_verbose),
        ("build environment", &artifact.build_environment),
        ("release profile", &artifact.profile_identity),
    ];
    if let Some(config) = &artifact.cargo_config {
        result.push(("cargo config", config));
    }
    if let Some(environment) = &artifact.legacy_evidence.environment {
        result.push(("legacy environment", environment));
    }
    if let Some(preregistration) = &artifact.legacy_evidence.preregistration {
        result.push(("legacy preregistration", preregistration));
    }
    if let Some(build_result) = &artifact.legacy_evidence.build_result {
        result.push(("legacy build result", build_result));
    }
    result
}

fn derive_manifest_from_bundle(input: &FileInput) -> Result<SourceManifest, ProvenanceError> {
    let bundle: SourceBundle = read_canonical_input(input, "source bundle")?;
    if bundle.schema != SOURCE_BUNDLE_SCHEMA {
        return Err(error(
            ValidationClass::Schema,
            "unsupported source bundle schema",
        ));
    }
    if bundle.entries.len() > MAX_SOURCE_BUNDLE_ENTRIES {
        return Err(error(
            ValidationClass::Source,
            "source bundle exceeds the entry-count limit",
        ));
    }
    validate_git_oid(&bundle.repository_head, "bundle HEAD")?;
    let mut prior: Option<&str> = None;
    let mut entries = Vec::with_capacity(bundle.entries.len());
    let mut content_bytes = 0usize;
    for entry in &bundle.entries {
        validate_relative_path(&entry.path)?;
        if !is_bundle_source_path(&entry.path) && entry.path != ".cargo/config.toml" {
            return Err(error(
                ValidationClass::Source,
                "source bundle contains a path outside the build-source closure",
            ));
        }
        if prior.is_some_and(|value| value >= entry.path.as_str()) {
            return Err(error(
                ValidationClass::Source,
                "source bundle paths are not unique and strictly sorted",
            ));
        }
        if !matches!(entry.mode.as_str(), "100644" | "100755") {
            return Err(error(
                ValidationClass::Source,
                "source bundle contains an unsupported mode",
            ));
        }
        if entry.contents.len() > MAX_SOURCE_ENTRY_BYTES {
            return Err(error(
                ValidationClass::Source,
                "source bundle entry exceeds the byte limit",
            ));
        }
        content_bytes = content_bytes
            .checked_add(entry.contents.len())
            .ok_or_else(|| error(ValidationClass::Source, "source bundle size overflow"))?;
        if content_bytes > MAX_SOURCE_BUNDLE_CONTENT_BYTES {
            return Err(error(
                ValidationClass::Source,
                "source bundle exceeds the aggregate content-byte limit",
            ));
        }
        entries.push(SourceManifestEntry {
            path: entry.path.clone(),
            mode: entry.mode.clone(),
            length: u64::try_from(entry.contents.len())
                .map_err(|_| error(ValidationClass::Source, "source entry length overflow"))?,
            sha256: evidence::sha256_bytes(&entry.contents),
        });
        prior = Some(&entry.path);
    }
    if entries.is_empty()
        || !entries.iter().any(|entry| entry.path == "Cargo.toml")
        || !entries
            .iter()
            .any(|entry| entry.path == "server/Cargo.toml")
        || !entries
            .iter()
            .any(|entry| entry.path == "server/src/quic.rs")
        || !entries.iter().any(|entry| entry.path == "core/Cargo.toml")
        || !entries.iter().any(|entry| entry.path == "bench/Cargo.toml")
        || !entries
            .iter()
            .any(|entry| entry.path == "client/Cargo.toml")
        || !entries
            .iter()
            .any(|entry| entry.path == "rust-toolchain.toml")
    {
        return Err(error(
            ValidationClass::Source,
            "source bundle omits required build-source closure roots",
        ));
    }
    Ok(SourceManifest {
        schema: SOURCE_MANIFEST_SCHEMA.to_owned(),
        repository_head: bundle.repository_head,
        entries,
    })
}

fn is_product_projection_path(path: &str) -> bool {
    path == "Cargo.toml" || path.starts_with("server/") || path.starts_with("core/")
}

fn is_bundle_source_path(path: &str) -> bool {
    is_product_projection_path(path)
        || matches!(
            path,
            "rust-toolchain.toml" | "bench/Cargo.toml" | "client/Cargo.toml"
        )
}

fn require_bundle_input(
    source: &SourceManifest,
    path: &str,
    input: &FileInput,
) -> Result<(), ProvenanceError> {
    match source
        .entries
        .binary_search_by_key(&path, |entry| entry.path.as_str())
    {
        Ok(index)
            if source.entries[index].sha256 == input.sha256
                && source.entries[index].length
                    == evidence::stable_file_identity_bounded(
                        &input.path,
                        MAX_PROVENANCE_INPUT_BYTES,
                    )
                    .map_err(|cause| error(ValidationClass::Input, cause.to_string()))?
                    .length =>
        {
            Ok(())
        }
        _ => Err(error(
            ValidationClass::Source,
            format!("support input does not match bundled path {path}"),
        )),
    }
}

fn validate_cargo_config_binding(
    source: &SourceManifest,
    config: Option<&FileInput>,
    require_historical_semantics: bool,
) -> Result<(), ProvenanceError> {
    match config {
        Some(_) if require_historical_semantics => Err(error(
            ValidationClass::Source,
            "historical evidence records no workspace Cargo configuration",
        )),
        Some(config) => require_bundle_input(source, ".cargo/config.toml", config),
        None if source
            .entries
            .iter()
            .any(|entry| entry.path == ".cargo/config.toml") =>
        {
            Err(error(
                ValidationClass::Source,
                "source bundle contains undeclared Cargo configuration",
            ))
        }
        None => Ok(()),
    }
}

#[allow(clippy::too_many_lines)]
fn validate_legacy_evidence(
    role: ArtifactRole,
    legacy: &LegacyEvidence,
    receipt: &ArtifactProvenance,
    request_policy: &ProvenancePolicy,
    source: &SourceManifest,
    policy: ProvenanceValidationPolicy<'_>,
) -> Result<(), ProvenanceError> {
    let (_, primary_bytes) = stable_input_bytes(&legacy.primary, "legacy primary")?;
    let (_, secondary_bytes) = stable_input_bytes(&legacy.secondary, "legacy secondary")?;
    let expected_source_identity = match role {
        ArtifactRole::Baseline => &legacy.secondary.sha256,
        ArtifactRole::Candidate => &legacy.primary.sha256,
    };
    if receipt.source.repository_head != source.repository_head {
        return Err(error(
            ValidationClass::Source,
            "legacy provenance HEAD differs from the source bundle",
        ));
    }
    if &receipt.source.source_identity_sha256 != expected_source_identity {
        return Err(error(
            ValidationClass::Anchor,
            "role-specific source identity differs from the sealed legacy evidence",
        ));
    }
    if !policy.require_historical_semantics {
        return Ok(());
    }
    let primary = parse_key_values(text(&primary_bytes, "legacy primary")?, '=')?;
    let secondary_text = text(&secondary_bytes, "legacy secondary")?;
    match role {
        ArtifactRole::Baseline => {
            let environment = legacy.environment.as_ref().ok_or_else(|| {
                error(
                    ValidationClass::Anchor,
                    "baseline recovered environment receipt is absent",
                )
            })?;
            let (_, environment_bytes) =
                stable_input_bytes(environment, "baseline legacy environment")?;
            validate_baseline_environment(
                text(&environment_bytes, "baseline legacy environment")?,
                receipt,
            )?;
            let preregistration = legacy.preregistration.as_ref().ok_or_else(|| {
                error(
                    ValidationClass::Anchor,
                    "baseline recovered preregistration receipt is absent",
                )
            })?;
            let build_result = legacy.build_result.as_ref().ok_or_else(|| {
                error(
                    ValidationClass::Anchor,
                    "baseline recovered build-result receipt is absent",
                )
            })?;
            let (_, preregistration_bytes) =
                stable_input_bytes(preregistration, "baseline legacy preregistration")?;
            let (_, build_result_bytes) =
                stable_input_bytes(build_result, "baseline legacy build result")?;
            validate_baseline_build_evidence(
                text(&preregistration_bytes, "baseline legacy preregistration")?,
                text(&build_result_bytes, "baseline legacy build result")?,
                receipt,
            )?;
            let secondary = parse_key_values(secondary_text, ' ')?;
            require_value(
                &primary,
                "source_sha256",
                request_policy.baseline_artifact_sha256.as_str(),
            )?;
            require_value(
                &primary,
                "frozen_sha256",
                request_policy.baseline_artifact_sha256.as_str(),
            )?;
            require_value(&primary, "selected_test", BENCHMARK_TEST)?;
            require_value(&primary, "selection_count", "1")?;
            require_value(&primary, "benchmark_executed", "false")?;
            require_value(&secondary, "HEAD", &receipt.source.repository_head)?;
            require_value(
                &secondary,
                "tracked_diff_sha256",
                &receipt.source.source_snapshot_sha256,
            )?;
            require_value(
                &secondary,
                "cargo_lock_sha256",
                &receipt.dependencies.cargo_lock_sha256,
            )?;
            if receipt.source.source_identity_sha256 != legacy.secondary.sha256 {
                return Err(error(
                    ValidationClass::Anchor,
                    "baseline source status is not the exact recovered legacy receipt",
                ));
            }
            if policy.require_historical_semantics {
                require_value(&primary, "size_bytes", "3137232")?;
                require_value(&secondary, "HEAD", HISTORICAL_HEAD)?;
                require_value(&secondary, "tracked_diff_sha256", BASELINE_DIFF_SHA256)?;
                require_value(&secondary, "cargo_lock_sha256", HISTORICAL_LOCK_SHA256)?;
            }
        }
        ArtifactRole::Candidate => {
            require_value(&primary, "head", &receipt.source.repository_head)?;
            require_value(
                &primary,
                "tracked_diff_sha256",
                &receipt.source.source_snapshot_sha256,
            )?;
            require_value(
                &primary,
                "cargo_lock_sha256",
                &receipt.dependencies.cargo_lock_sha256,
            )?;
            require_value(
                &primary,
                "server_quic_sha256",
                &receipt.source.benchmark_source_sha256,
            )?;
            require_value(
                &primary,
                "selected_source_sha256",
                request_policy.candidate_artifact_sha256.as_str(),
            )?;
            require_value(
                &primary,
                "frozen_copy_sha256",
                request_policy.candidate_artifact_sha256.as_str(),
            )?;
            require_value(&primary, "benchmark_test", BENCHMARK_TEST)?;
            require_value(&primary, "final_executable_selection_count", "1")?;
            require_value(&primary, "source_vs_frozen_copy", "IDENTICAL")?;
            if receipt.source.source_identity_sha256 != legacy.primary.sha256 {
                return Err(error(
                    ValidationClass::Anchor,
                    "candidate source identity is not the exact recovered legacy receipt",
                ));
            }
            validate_candidate_build_receipt(secondary_text, receipt)?;
            if policy.require_historical_semantics {
                require_value(&primary, "head", HISTORICAL_HEAD)?;
                require_value(&primary, "tracked_diff_sha256", CANDIDATE_DIFF_SHA256)?;
                require_value(&primary, "cargo_lock_sha256", HISTORICAL_LOCK_SHA256)?;
                require_value(&primary, "server_quic_sha256", HISTORICAL_QUIC_SHA256)?;
                require_value(&primary, "accepted_c3_sha256", CANDIDATE_ACCEPTED_C3_SHA256)?;
                require_value(&primary, "all_executable_sizes_bytes", "3435104")?;
                require_value(&primary, "frozen_copy_vs_accepted_c3", "DIFFERENT")?;
                if source.repository_head != HISTORICAL_HEAD {
                    return Err(error(
                        ValidationClass::Source,
                        "candidate source bundle HEAD differs from recovered legacy identity",
                    ));
                }
            }
        }
    }
    Ok(())
}

fn validate_baseline_environment(
    text: &str,
    receipt: &ArtifactProvenance,
) -> Result<(), ProvenanceError> {
    let values = parse_key_values(text, '=')?;
    require_value(
        &values,
        "rustc_commit",
        &receipt.toolchain.rustc_commit_hash,
    )?;
    require_value(&values, "rustc_host", &receipt.toolchain.host)?;
    require_value(
        &values,
        "cargo_lock_sha256",
        &receipt.dependencies.cargo_lock_sha256,
    )?;
    require_value(&values, "workspace_cargo_config", "none found under .cargo")?;
    require_value(&values, "release_lto", "true")?;
    require_value(&values, "release_codegen_units", "1")?;
    require_value(&values, "release_panic", "abort")?;
    require_value(&values, "release_strip", "true")?;
    require_value(
        &values,
        "release_opt_level",
        "Cargo default for release profile",
    )?;
    let rustc = values
        .get("rustc")
        .ok_or_else(|| error(ValidationClass::Toolchain, "baseline rustc field is absent"))?;
    let cargo = values
        .get("cargo")
        .ok_or_else(|| error(ValidationClass::Toolchain, "baseline cargo field is absent"))?;
    if !rustc.contains(&receipt.toolchain.rustc_release)
        || !cargo.contains(&receipt.toolchain.cargo_release)
    {
        return Err(error(
            ValidationClass::Toolchain,
            "baseline environment does not support structured toolchain releases",
        ));
    }
    Ok(())
}

fn validate_baseline_build_evidence(
    preregistration: &str,
    build_result: &str,
    receipt: &ArtifactProvenance,
) -> Result<(), ProvenanceError> {
    let command = receipt.build.command_argv.join(" ");
    for (text, field, expected) in [
        (
            preregistration,
            "source_head",
            receipt.source.repository_head.as_str(),
        ),
        (
            preregistration,
            "source_tracked_diff_sha256",
            receipt.source.source_snapshot_sha256.as_str(),
        ),
        (
            preregistration,
            "cargo_lock_sha256",
            receipt.dependencies.cargo_lock_sha256.as_str(),
        ),
        (preregistration, "build_command", command.as_str()),
        (preregistration, "benchmark_test", BENCHMARK_TEST),
        (build_result, "command", command.as_str()),
        (build_result, "exit_code", "0"),
        (build_result, "profile", "release optimized"),
    ] {
        require_exact_line(text, &format!("{field}={expected}"), field)?;
    }
    require_exact_line(
        preregistration,
        "No warmup or measured benchmark execution is authorized in this freeze phase.",
        "baseline preregistration no-product authorization",
    )?;
    require_exact_line(
        build_result,
        "No test, ignored benchmark, warmup, or measured batch was run by this build command.",
        "baseline build-result no-product outcome",
    )?;
    Ok(())
}

fn require_exact_line(text: &str, expected: &str, field: &str) -> Result<(), ProvenanceError> {
    if text.lines().filter(|line| *line == expected).count() != 1 {
        return Err(error(
            ValidationClass::Anchor,
            format!("legacy evidence field {field} is absent or ambiguous"),
        ));
    }
    Ok(())
}

fn validate_candidate_build_receipt(
    text: &str,
    receipt: &ArtifactProvenance,
) -> Result<(), ProvenanceError> {
    let values = parse_key_values(text, '=')?;
    require_value(&values, "classification", "EQUIVALENCE-BUILD-NOT-BENCHMARK")?;
    require_value(&values, "build_count", "1")?;
    require_value(&values, "benchmark_invocation_count", "0")?;
    require_value(&values, "warmup_invocation_count", "0")?;
    require_value(&values, "timing_run_count", "0")?;
    let command = values
        .get("command")
        .ok_or_else(|| error(ValidationClass::Build, "legacy build command is absent"))?;
    let tokens: Vec<_> = command.split_ascii_whitespace().collect();
    if tokens.len() < 2 {
        return Err(error(
            ValidationClass::Build,
            "legacy build command is incomplete",
        ));
    }
    let (environment, program_index) =
        if let Some(value) = tokens[0].strip_prefix("CARGO_TARGET_DIR=") {
            (Some(value), 1)
        } else {
            (None, 0)
        };
    let expected_argv: Vec<_> = receipt
        .build
        .command_argv
        .iter()
        .map(String::as_str)
        .collect();
    if tokens[program_index..] != expected_argv {
        return Err(error(
            ValidationClass::Build,
            "structured build argv differs from exact recovered command",
        ));
    }
    if environment
        != receipt
            .build
            .environment
            .get("CARGO_TARGET_DIR")
            .map(String::as_str)
        || receipt.build.environment.len() != 1
    {
        return Err(error(
            ValidationClass::Build,
            "structured CARGO_TARGET_DIR differs from exact recovered command",
        ));
    }
    Ok(())
}

fn parse_key_values(
    text: &str,
    separator: char,
) -> Result<BTreeMap<String, String>, ProvenanceError> {
    let mut values = BTreeMap::new();
    for line in text.lines().filter(|line| !line.is_empty()) {
        let pair = if separator == '=' {
            line.split_once('=')
        } else {
            line.split_once(char::is_whitespace)
        };
        let Some((key, value)) = pair else {
            return Err(error(
                ValidationClass::Anchor,
                "legacy evidence contains a malformed line",
            ));
        };
        if key.is_empty()
            || value.is_empty()
            || values
                .insert(key.to_owned(), value.trim_start().to_owned())
                .is_some()
        {
            return Err(error(
                ValidationClass::Anchor,
                "legacy evidence contains an empty or duplicate field",
            ));
        }
    }
    Ok(values)
}

fn require_value(
    values: &BTreeMap<String, String>,
    key: &str,
    expected: &str,
) -> Result<(), ProvenanceError> {
    if values.get(key).map(String::as_str) != Some(expected) {
        return Err(error(
            ValidationClass::Anchor,
            format!("legacy evidence field {key} differs from its structured claim"),
        ));
    }
    Ok(())
}

fn validate_release_profile(input: &FileInput) -> Result<(), ProvenanceError> {
    let profile: ReleaseProfileIdentity = read_canonical_input(input, "release profile")?;
    if profile.schema != RELEASE_PROFILE_SCHEMA
        || profile.profile != "release"
        || profile.opt_level != 3
        || !profile.lto
        || profile.codegen_units != 1
        || profile.panic != "abort"
        || !profile.strip
    {
        return Err(error(
            ValidationClass::Build,
            "release profile receipt differs from historical profile",
        ));
    }
    Ok(())
}

fn validate_candidate_source_profile(input: &FileInput) -> Result<(), ProvenanceError> {
    let bundle: SourceBundle = read_canonical_input(input, "candidate source bundle")?;
    let cargo = bundle
        .entries
        .iter()
        .find(|entry| entry.path == "Cargo.toml")
        .ok_or_else(|| {
            error(
                ValidationClass::Build,
                "candidate workspace manifest is absent",
            )
        })?;
    let text = text(&cargo.contents, "candidate workspace manifest")?;
    let mut in_release = false;
    let mut profile = BTreeMap::new();
    for raw in text.lines() {
        let line = raw.split_once('#').map_or(raw, |(before, _)| before).trim();
        if line.starts_with('[') {
            in_release = line == "[profile.release]";
            continue;
        }
        if in_release && !line.is_empty() {
            let (name, value) = line.split_once('=').ok_or_else(|| {
                error(
                    ValidationClass::Build,
                    "candidate release profile contains malformed TOML",
                )
            })?;
            profile.insert(name.trim(), value.trim().trim_matches(['\'', '"']));
        }
    }
    if profile.get("lto") != Some(&"true")
        || profile.get("codegen-units") != Some(&"1")
        || profile.get("panic") != Some(&"abort")
        || profile.get("strip") != Some(&"true")
        || profile.contains_key("opt-level")
    {
        return Err(error(
            ValidationClass::Build,
            "candidate source release profile differs from historical Cargo semantics",
        ));
    }
    Ok(())
}

fn validate_toolchain_and_build(
    artifact: &ArtifactInput,
    role: ArtifactRole,
    policy: ProvenanceValidationPolicy<'_>,
) -> Result<(), ProvenanceError> {
    let receipt = &artifact.provenance;
    let (_, rustc_bytes) = stable_input_bytes(&artifact.rustc_verbose, "rustc receipt")?;
    let (_, cargo_bytes) = stable_input_bytes(&artifact.cargo_verbose, "cargo receipt")?;
    let (_, environment_bytes) =
        stable_input_bytes(&artifact.build_environment, "build environment")?;
    let rustc = text(&rustc_bytes, "rustc receipt")?;
    let cargo = text(&cargo_bytes, "cargo receipt")?;
    if policy.require_historical_semantics
        && (!rustc.contains(&format!("release: {}", receipt.toolchain.rustc_release))
            || !rustc.contains(&format!(
                "commit-hash: {}",
                receipt.toolchain.rustc_commit_hash
            ))
            || !rustc.contains(&format!("host: {}", receipt.toolchain.host))
            || !cargo.contains(&format!("release: {}", receipt.toolchain.cargo_release))
            || !cargo.contains(&format!("host: {}", receipt.toolchain.host)))
    {
        return Err(error(
            ValidationClass::Toolchain,
            "toolchain receipt text does not support structured claims",
        ));
    }
    let environment: BTreeMap<String, String> =
        parse_canonical_json(&environment_bytes, "build environment")?;
    if environment != receipt.build.environment {
        return Err(error(
            ValidationClass::Build,
            "build environment bytes differ from structured claims",
        ));
    }
    validate_explicit_environment_overrides(
        role,
        &environment,
        policy.require_historical_semantics,
    )?;
    for (name, value) in &environment {
        if !ALLOWED_BUILD_ENV.contains(&name.as_str()) || value.is_empty() {
            return Err(error(
                ValidationClass::Build,
                "build environment contains unsupported or empty data",
            ));
        }
        if name == "CARGO_TARGET_DIR" && !Path::new(value).is_absolute() {
            return Err(error(
                ValidationClass::Build,
                "CARGO_TARGET_DIR must be absolute",
            ));
        }
    }
    let Some((program, arguments)) = receipt.build.command_argv.split_first() else {
        return Err(error(ValidationClass::Build, "build command is empty"));
    };
    let exact_program = match role {
        ArtifactRole::Baseline => "cargo",
        ArtifactRole::Candidate => "/Volumes/SD-2T/Caches/cargo/bin/cargo",
    };
    if (policy.require_historical_semantics && program != exact_program)
        || (!policy.require_historical_semantics
            && Path::new(program)
                .file_name()
                .and_then(|name| name.to_str())
                != Some("cargo"))
        || arguments
            != [
                "test",
                "--profile",
                "release",
                "-p",
                "distd_server",
                "--no-run",
            ]
    {
        return Err(error(
            ValidationClass::Build,
            "build command differs from exact one-build/no-run argv",
        ));
    }
    if receipt.build.profile != "release" || receipt.build.features != ["redb"] {
        return Err(error(
            ValidationClass::Build,
            "build profile or feature set differs from historical build",
        ));
    }
    if policy.require_historical_semantics
        && (receipt.toolchain.rustc_release != HISTORICAL_RUSTC_RELEASE
            || receipt.toolchain.rustc_commit_hash != HISTORICAL_RUSTC_COMMIT
            || receipt.toolchain.cargo_release != HISTORICAL_CARGO_RELEASE
            || receipt.toolchain.host != HISTORICAL_TARGET
            || receipt.toolchain.target != HISTORICAL_TARGET)
    {
        return Err(error(
            ValidationClass::Toolchain,
            "toolchain differs from historical anchors",
        ));
    }
    Ok(())
}

fn validate_explicit_environment_overrides(
    role: ArtifactRole,
    environment: &BTreeMap<String, String>,
    require_historical_semantics: bool,
) -> Result<(), ProvenanceError> {
    if require_historical_semantics
        && match role {
            ArtifactRole::Baseline => !environment.is_empty(),
            ArtifactRole::Candidate => {
                environment.len() != 1 || !environment.contains_key("CARGO_TARGET_DIR")
            }
        }
    {
        return Err(error(
            ValidationClass::Build,
            "explicit command environment overrides differ from recovered invocation",
        ));
    }
    Ok(())
}

fn read_repository_manifest(
    repository: &RepositoryIdentity,
) -> Result<SourceManifest, ProvenanceError> {
    let input = FileInput {
        path: repository.source_manifest.clone(),
        sha256: repository.source_manifest_sha256.clone(),
    };
    let manifest: SourceManifest = read_canonical_input(&input, "current source manifest")?;
    validate_source_manifest(&manifest)?;
    if manifest.repository_head != repository.head {
        return Err(error(
            ValidationClass::RepositoryRelation,
            "current source manifest HEAD differs from repository capture",
        ));
    }
    Ok(manifest)
}

fn validate_product_projection(
    current: &SourceManifest,
    candidate: &SourceManifest,
) -> Result<(), ProvenanceError> {
    let projection = |manifest: &SourceManifest| {
        manifest
            .entries
            .iter()
            .filter(|entry| is_product_projection_path(&entry.path))
            .map(|entry| {
                (
                    entry.path.clone(),
                    (entry.mode.clone(), entry.length, entry.sha256.clone()),
                )
            })
            .collect::<BTreeMap<_, _>>()
    };
    let current = projection(current);
    let candidate = projection(candidate);
    if current.is_empty() || current != candidate {
        return Err(error(
            ValidationClass::RepositoryRelation,
            "candidate build-source closure differs from current server/core source",
        ));
    }
    Ok(())
}

fn validate_patch(
    patch: &ReviewedPatchManifest,
    baseline: &ArtifactInput,
    candidate: &ArtifactInput,
    baseline_manifest: &SourceManifest,
    candidate_manifest: &SourceManifest,
) -> Result<(), ProvenanceError> {
    if patch.schema != REVIEWED_PATCH_SCHEMA
        || patch.baseline != baseline.provenance.source
        || patch.candidate != candidate.provenance.source
        || patch.files != derive_patch(baseline_manifest, candidate_manifest)
    {
        return Err(error(
            ValidationClass::Patch,
            "reviewed patch is not the complete ordered mechanical bundle delta",
        ));
    }
    Ok(())
}

#[must_use]
pub fn derive_patch(
    baseline: &SourceManifest,
    candidate: &SourceManifest,
) -> Vec<ReviewedPatchFile> {
    let left: BTreeMap<_, _> = baseline
        .entries
        .iter()
        .map(|entry| (&entry.path, entry))
        .collect();
    let right: BTreeMap<_, _> = candidate
        .entries
        .iter()
        .map(|entry| (&entry.path, entry))
        .collect();
    let paths: BTreeSet<_> = left.keys().chain(right.keys()).copied().collect();
    paths
        .into_iter()
        .filter_map(|path| {
            let baseline = left.get(path).map(|entry| ReviewedPatchFileIdentity {
                mode: entry.mode.clone(),
                length: entry.length,
                sha256: entry.sha256.clone(),
            });
            let candidate = right.get(path).map(|entry| ReviewedPatchFileIdentity {
                mode: entry.mode.clone(),
                length: entry.length,
                sha256: entry.sha256.clone(),
            });
            let operation = match (&baseline, &candidate) {
                (None, Some(_)) => ReviewedPatchOperation::Added,
                (Some(_), None) => ReviewedPatchOperation::Deleted,
                (Some(before), Some(after)) if before != after => ReviewedPatchOperation::Modified,
                _ => return None,
            };
            Some(ReviewedPatchFile {
                path: path.clone(),
                operation,
                baseline,
                candidate,
            })
        })
        .collect()
}

pub fn validate_source_manifest(manifest: &SourceManifest) -> Result<(), ProvenanceError> {
    if manifest.schema != SOURCE_MANIFEST_SCHEMA {
        return Err(error(
            ValidationClass::Schema,
            "unsupported source manifest schema",
        ));
    }
    validate_git_oid(&manifest.repository_head, "source HEAD")?;
    let mut prior: Option<&str> = None;
    for entry in &manifest.entries {
        validate_relative_path(&entry.path)?;
        if prior.is_some_and(|value| value >= entry.path.as_str()) {
            return Err(error(
                ValidationClass::Source,
                "source paths are not unique and strictly sorted",
            ));
        }
        if !matches!(entry.mode.as_str(), "100644" | "100755") {
            return Err(error(
                ValidationClass::Source,
                "source manifest contains an unsupported mode",
            ));
        }
        validate_hash(&entry.sha256)?;
        prior = Some(&entry.path);
    }
    Ok(())
}

pub fn validate_relative_path(path: &str) -> Result<(), ProvenanceError> {
    let candidate = Path::new(path);
    if path.is_empty()
        || path.starts_with('/')
        || path.ends_with('/')
        || path.contains("//")
        || path.contains('\u{5c}')
        || candidate.is_absolute()
        || candidate
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(error(
            ValidationClass::Path,
            "path is not canonical repository-relative UTF-8",
        ));
    }
    Ok(())
}

pub fn validate_file_input(input: &FileInput, role: &str) -> Result<(), ProvenanceError> {
    stable_input_bytes(input, role).map(|_| ())
}

fn stable_input_bytes(
    input: &FileInput,
    role: &str,
) -> Result<(evidence::FileIdentity, Vec<u8>), ProvenanceError> {
    validate_hash(&input.sha256)?;
    if !input.path.is_absolute() {
        return Err(error(
            ValidationClass::Path,
            format!("{role} path is not absolute"),
        ));
    }
    let (identity, bytes) =
        evidence::stable_file_bytes_bounded(&input.path, MAX_PROVENANCE_INPUT_BYTES)
            .map_err(|cause| error(ValidationClass::Input, cause.to_string()))?;
    if identity.canonical_path != input.path || identity.sha256 != input.sha256 {
        return Err(error(
            ValidationClass::Input,
            format!("{role} is noncanonical, changed, or hash mismatched"),
        ));
    }
    Ok((identity, bytes))
}

pub fn read_canonical_input<T: DeserializeOwned + Serialize>(
    input: &FileInput,
    role: &str,
) -> Result<T, ProvenanceError> {
    let (_, bytes) = stable_input_bytes(input, role)?;
    parse_canonical_json(&bytes, role)
}

fn parse_canonical_json<T: DeserializeOwned + Serialize>(
    bytes: &[u8],
    role: &str,
) -> Result<T, ProvenanceError> {
    let value: T = serde_json::from_slice(bytes)
        .map_err(|cause| error(ValidationClass::Schema, format!("{role}: {cause}")))?;
    if canonical_json_line(&value)
        .map_err(|cause| error(ValidationClass::Schema, cause.to_string()))?
        != bytes
    {
        return Err(error(
            ValidationClass::Schema,
            format!("{role} is not canonical one-line JSON"),
        ));
    }
    Ok(value)
}

fn validate_hash(hash: &str) -> Result<(), ProvenanceError> {
    if hash.len() != 64
        || !hash
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(error(
            ValidationClass::Anchor,
            "SHA-256 must be 64 lowercase hexadecimal characters",
        ));
    }
    Ok(())
}

fn validate_git_oid(value: &str, role: &str) -> Result<(), ProvenanceError> {
    if value.len() != 40
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(error(
            ValidationClass::Source,
            format!("{role} must be 40 lowercase hexadecimal characters"),
        ));
    }
    Ok(())
}

fn text<'a>(bytes: &'a [u8], role: &str) -> Result<&'a str, ProvenanceError> {
    std::str::from_utf8(bytes)
        .map_err(|_| error(ValidationClass::Input, format!("{role} is not UTF-8")))
}

fn error(class: ValidationClass, detail: impl Into<String>) -> ProvenanceError {
    ProvenanceError::new(class, detail)
}

#[cfg(test)]
mod tests {
    use super::super::model::{EvidenceDerivation, SourceBundleEntry};
    use super::*;
    use std::fs;
    use std::path::PathBuf;

    fn input(path: PathBuf) -> FileInput {
        let path = fs::canonicalize(path).unwrap();
        FileInput {
            sha256: evidence::sha256_file(&path).unwrap(),
            path,
        }
    }

    fn baseline_receipt() -> ArtifactProvenance {
        ArtifactProvenance {
            schema: ARTIFACT_PROVENANCE_SCHEMA.to_owned(),
            role: ArtifactRole::Baseline,
            origin: ArtifactOrigin::RecoveredFrozen,
            status: ProvenanceStatus::EvidenceDerived {
                derivation: EvidenceDerivation::SyntheticFixture,
            },
            artifact_sha256: BASELINE_ARTIFACT_SHA256.to_owned(),
            source: super::super::model::HistoricalSourceIdentity {
                repository_head: HISTORICAL_HEAD.to_owned(),
                source_snapshot_sha256: BASELINE_DIFF_SHA256.to_owned(),
                source_identity_sha256: BASELINE_LEGACY_SECONDARY_SHA256.to_owned(),
                source_manifest_sha256: "a".repeat(64),
                source_bundle_sha256: "b".repeat(64),
                benchmark_source_sha256: HISTORICAL_QUIC_SHA256.to_owned(),
            },
            dependencies: super::super::model::DependencyIdentity {
                cargo_lock_sha256: HISTORICAL_LOCK_SHA256.to_owned(),
                workspace_manifest_sha256: "c".repeat(64),
                package_manifest_sha256: "d".repeat(64),
                cargo_config_sha256: None,
            },
            toolchain: super::super::model::ToolchainIdentity {
                rustc_release: HISTORICAL_RUSTC_RELEASE.to_owned(),
                rustc_commit_hash: HISTORICAL_RUSTC_COMMIT.to_owned(),
                cargo_release: HISTORICAL_CARGO_RELEASE.to_owned(),
                host: HISTORICAL_TARGET.to_owned(),
                target: HISTORICAL_TARGET.to_owned(),
                rustc_verbose_sha256: HISTORICAL_RUSTC_RECEIPT_SHA256.to_owned(),
                cargo_verbose_sha256: HISTORICAL_CARGO_RECEIPT_SHA256.to_owned(),
            },
            build: super::super::model::BuildIdentity {
                profile: "release".to_owned(),
                profile_identity_sha256: "e".repeat(64),
                features: vec!["redb".to_owned()],
                command_argv: [
                    "cargo",
                    "test",
                    "--profile",
                    "release",
                    "-p",
                    "distd_server",
                    "--no-run",
                ]
                .map(str::to_owned)
                .to_vec(),
                environment: BTreeMap::new(),
                environment_sha256: "f".repeat(64),
            },
        }
    }

    #[test]
    fn production_policy_pins_all_independent_artifact_and_legacy_hashes() {
        let policy = ProvenanceValidationPolicy::production();
        assert_eq!(policy.baseline_artifact_sha256, BASELINE_ARTIFACT_SHA256);
        assert_eq!(policy.candidate_artifact_sha256, CANDIDATE_ARTIFACT_SHA256);
        assert_eq!(
            policy.baseline_legacy_primary_sha256,
            BASELINE_LEGACY_PRIMARY_SHA256
        );
        assert_eq!(
            policy.baseline_legacy_secondary_sha256,
            BASELINE_LEGACY_SECONDARY_SHA256
        );
        assert_eq!(
            policy.baseline_legacy_environment_sha256,
            Some(BASELINE_LEGACY_ENVIRONMENT_SHA256)
        );
        assert_eq!(
            policy.baseline_legacy_preregistration_sha256,
            Some(BASELINE_LEGACY_PREREGISTRATION_SHA256)
        );
        assert_eq!(
            policy.baseline_legacy_build_result_sha256,
            Some(BASELINE_LEGACY_BUILD_RESULT_SHA256)
        );
        assert_eq!(
            policy.candidate_legacy_primary_sha256,
            CANDIDATE_LEGACY_PRIMARY_SHA256
        );
        assert_eq!(
            policy.candidate_legacy_secondary_sha256,
            CANDIDATE_LEGACY_SECONDARY_SHA256
        );
        assert!(policy.require_historical_semantics);
        assert!(require_anchor_availability(policy).is_err());
    }

    #[test]
    fn source_bundle_manifest_is_derived_from_exact_bytes() {
        let temporary = tempfile::tempdir().unwrap();
        let path = temporary.path().join("bundle.json");
        let entries = [
            ("Cargo.toml", b"workspace".as_slice()),
            ("bench/Cargo.toml", b"bench".as_slice()),
            ("client/Cargo.toml", b"client".as_slice()),
            ("core/Cargo.toml", b"core".as_slice()),
            ("rust-toolchain.toml", b"toolchain".as_slice()),
            ("server/Cargo.toml", b"server".as_slice()),
            ("server/src/quic.rs", b"quic".as_slice()),
        ]
        .into_iter()
        .map(|(path, contents)| SourceBundleEntry {
            path: path.to_owned(),
            mode: "100644".to_owned(),
            contents: contents.to_vec(),
        })
        .collect();
        let bundle = SourceBundle {
            schema: SOURCE_BUNDLE_SCHEMA.to_owned(),
            repository_head: "a".repeat(40),
            entries,
        };
        fs::write(&path, canonical_json_line(&bundle).unwrap()).unwrap();

        let manifest = derive_manifest_from_bundle(&input(path)).unwrap();

        let quic = manifest
            .entries
            .iter()
            .find(|entry| entry.path == "server/src/quic.rs")
            .expect("derived QUIC source entry");
        assert_eq!(quic.sha256, evidence::sha256_bytes(b"quic"));
        assert_eq!(quic.length, 4);
    }

    #[test]
    fn source_bundle_entry_count_is_bounded() {
        let temporary = tempfile::tempdir().unwrap();
        let path = temporary.path().join("bundle.json");
        let entries = (0..=MAX_SOURCE_BUNDLE_ENTRIES)
            .map(|index| SourceBundleEntry {
                path: format!("server/src/generated/{index:04}.rs"),
                mode: "100644".to_owned(),
                contents: Vec::new(),
            })
            .collect();
        let bundle = SourceBundle {
            schema: SOURCE_BUNDLE_SCHEMA.to_owned(),
            repository_head: "a".repeat(40),
            entries,
        };
        fs::write(&path, canonical_json_line(&bundle).unwrap()).unwrap();

        let error = derive_manifest_from_bundle(&input(path)).unwrap_err();

        assert_eq!(error.class, ValidationClass::Source);
        assert!(error.detail.contains("entry-count limit"));
    }

    #[test]
    fn cargo_config_is_absent_historically_and_exactly_bound_synthetically() {
        let temporary = tempfile::tempdir().unwrap();
        let path = temporary.path().join("config.toml");
        fs::write(&path, b"[net]\noffline = true\n").unwrap();
        let config = input(path);
        let source = SourceManifest {
            schema: SOURCE_MANIFEST_SCHEMA.to_owned(),
            repository_head: "a".repeat(40),
            entries: vec![SourceManifestEntry {
                path: ".cargo/config.toml".to_owned(),
                mode: "100644".to_owned(),
                length: 21,
                sha256: config.sha256.clone(),
            }],
        };

        assert!(validate_cargo_config_binding(&source, None, false).is_err());
        assert!(validate_cargo_config_binding(&source, Some(&config), false).is_ok());
        assert!(validate_cargo_config_binding(&source, Some(&config), true).is_err());
    }

    #[test]
    fn mechanical_patch_includes_additions_and_deletions() {
        let entry = |path: &str, sha: char| SourceManifestEntry {
            path: path.to_owned(),
            mode: "100644".to_owned(),
            length: 1,
            sha256: sha.to_string().repeat(64),
        };
        let baseline = SourceManifest {
            schema: SOURCE_MANIFEST_SCHEMA.to_owned(),
            repository_head: "a".repeat(40),
            entries: vec![entry("core/deleted.rs", 'a')],
        };
        let candidate = SourceManifest {
            schema: SOURCE_MANIFEST_SCHEMA.to_owned(),
            repository_head: "a".repeat(40),
            entries: vec![entry("server/added.rs", 'b')],
        };

        let patch = derive_patch(&baseline, &candidate);
        assert_eq!(patch.len(), 2);
        assert_eq!(patch[0].operation, ReviewedPatchOperation::Deleted);
        assert_eq!(patch[1].operation, ReviewedPatchOperation::Added);
    }

    #[test]
    fn historical_command_environment_means_only_explicit_overrides() {
        let mut nonempty = BTreeMap::new();
        nonempty.insert("CARGO_TARGET_DIR".to_owned(), "/tmp/target".to_owned());
        assert!(validate_explicit_environment_overrides(
            ArtifactRole::Baseline,
            &BTreeMap::new(),
            true,
        )
        .is_ok());
        assert!(
            validate_explicit_environment_overrides(ArtifactRole::Baseline, &nonempty, true,)
                .is_err()
        );
        assert!(
            validate_explicit_environment_overrides(ArtifactRole::Candidate, &nonempty, true,)
                .is_ok()
        );
        nonempty.insert("RUSTFLAGS".to_owned(), "-Ctarget-cpu=native".to_owned());
        assert!(
            validate_explicit_environment_overrides(ArtifactRole::Candidate, &nonempty, true,)
                .is_err()
        );
    }

    #[test]
    fn baseline_build_receipts_derive_exact_no_run_build_semantics() {
        let receipt = baseline_receipt();
        let preregistration = format!(
            "source_head={HISTORICAL_HEAD}\nsource_tracked_diff_sha256={BASELINE_DIFF_SHA256}\n\
cargo_lock_sha256={HISTORICAL_LOCK_SHA256}\nbuild_command=cargo test --profile release -p \
distd_server --no-run\nbenchmark_test={BENCHMARK_TEST}\n\
No warmup or measured benchmark execution is authorized in this freeze phase.\n"
        );
        let build_result = "command=cargo test --profile release -p distd_server --no-run\n\
exit_code=0\nprofile=release optimized\n\
No test, ignored benchmark, warmup, or measured batch was run by this build command.\n";

        assert!(
            validate_baseline_build_evidence(&preregistration, build_result, &receipt,).is_ok()
        );
        assert!(validate_baseline_build_evidence(
            &preregistration,
            &build_result.replace("exit_code=0", "exit_code=1"),
            &receipt,
        )
        .is_err());
    }

    #[test]
    fn lexical_paths_reject_alias_spellings() {
        for path in ["a//b", "a/", "./a", "a/../b", "/a", r"a\b"] {
            assert!(validate_relative_path(path).is_err(), "{path}");
        }
        assert!(validate_relative_path("server/src/quic.rs").is_ok());
    }

    #[test]
    fn evidence_derivation_has_a_fixed_non_narrative_wire_value() {
        let value = serde_json::to_value(ProvenanceStatus::EvidenceDerived {
            derivation:
                EvidenceDerivation::ByteExactPayloadsFromDurableSessionLogOriginalPathsAndInodesUnavailable,
        })
        .unwrap();
        assert_eq!(
            value["derivation"],
            "byte_exact_payloads_from_durable_session_log_original_paths_and_inodes_unavailable"
        );
    }

    #[test]
    fn synthetic_policy_is_explicit_and_never_enables_historical_claims() {
        let policy = ProvenanceValidationPolicy::synthetic(
            "a", "b", "c", "d", None, None, None, "e", "f", "g", "h",
        );
        assert_eq!(policy.baseline_artifact_sha256, "a");
        assert!(!policy.require_historical_semantics);
    }

    #[test]
    fn recovered_baseline_environment_payload_has_exact_hash_and_semantics() {
        let bytes = br"captured_utc=2026-08-12T15:14:44Z
rustc=rustc 1.96.0 (ac68faa20 2026-05-25)
rustc_commit=ac68faa20c58cbccd01ee7208bf3b6e93a7d7f96
rustc_host=aarch64-apple-darwin
llvm=22.1.2
cargo=cargo 1.96.0 (30a34c682 2026-05-25)
os=macOS 26.5.2 build 25F84
kernel=Darwin 25.5.0 RELEASE_ARM64_T6041 arm64
cpu=Apple M4 Pro
hardware_model=Mac16,8
memory_bytes=25769803776
power=AC Power; internal battery 100 percent charged
thermal=No thermal warning level recorded
performance=No performance warning level recorded
cpu_power=No CPU power status recorded
cargo_lock_sha256=ad953daf188f5f11c633a7efb8a8f548310a44af9b9b5893872058db8bfcd356
workspace_cargo_config=none found under .cargo
release_lto=true
release_codegen_units=1
release_panic=abort
release_strip=true
release_opt_level=Cargo default for release profile
";
        assert_eq!(
            evidence::sha256_bytes(bytes),
            BASELINE_LEGACY_ENVIRONMENT_SHA256
        );
        let values = parse_key_values(std::str::from_utf8(bytes).unwrap(), '=').unwrap();
        assert_eq!(values["release_codegen_units"], "1");
        assert_eq!(values["release_panic"], "abort");
        assert_eq!(values["rustc_commit"], HISTORICAL_RUSTC_COMMIT);
    }
}

//! Stable, non-product composition of a provenance-complete v3 request.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use super::evidence::{self, canonical_json_line, pin_absent_root, AbsentRoot};
use super::model::{
    ArtifactInput, ArtifactProvenance, ArtifactRole, FileInput, HarnessContract, LegacyEvidence,
    ProvenancePolicy, Request, ReviewedPatch, ReviewedPatchManifest, REQUEST_SCHEMA,
};
use super::prepare::RepositoryCaptureReceipt;
use super::provenance::{
    self, ProvenanceValidationPolicy, ValidationClass, MAX_PROVENANCE_INPUT_BYTES,
};

pub const COMPOSE_INPUT_SCHEMA: &str = "distd.quic.fetch.compose-input.v3";
const CAPTURE_SCHEMA: &str = "distd.quic.fetch.repository-capture.v2";

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ComposeInput {
    pub schema: String,
    pub repository_capture: FileInput,
    pub baseline: ArtifactFiles,
    pub candidate: ArtifactFiles,
    pub reviewed_patch_manifest: FileInput,
    pub blocked_evidence: PathBuf,
    pub evidence_root: PathBuf,
    pub output: PathBuf,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactFiles {
    pub executable: FileInput,
    pub provenance_receipt: FileInput,
    pub source_snapshot: FileInput,
    pub source_manifest: FileInput,
    pub source_bundle: FileInput,
    pub legacy_evidence: LegacyEvidence,
    pub benchmark_source: FileInput,
    pub cargo_lock: FileInput,
    pub workspace_manifest: FileInput,
    pub package_manifest: FileInput,
    pub cargo_config: Option<FileInput>,
    pub rustc_verbose: FileInput,
    pub cargo_verbose: FileInput,
    pub build_environment: FileInput,
    pub profile_identity: FileInput,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ComposeReceipt {
    pub schema: String,
    pub request_path: PathBuf,
    pub request_sha256: String,
    pub input_identities: BTreeMap<PathBuf, String>,
    pub product_invocations: u8,
}

#[derive(Debug, thiserror::Error)]
pub enum ComposeError {
    #[error(transparent)]
    Evidence(#[from] evidence::EvidenceError),
    #[error("compose input is invalid: {0}")]
    Invalid(String),
    #[error("compose JSON is invalid: {0}")]
    Json(#[from] serde_json::Error),
}

pub fn parse_canonical_compose_input(bytes: &[u8]) -> Result<ComposeInput, ComposeError> {
    let input: ComposeInput = serde_json::from_slice(bytes)?;
    if canonical_json_line(&input)? != bytes {
        return Err(invalid("compose input is not canonical one-line JSON"));
    }
    Ok(input)
}

/// Create one immutable canonical request without running or rebuilding either product arm.
pub fn compose(input: &ComposeInput) -> Result<ComposeReceipt, ComposeError> {
    compose_with_policy(input, ProvenanceValidationPolicy::production())
}

pub(crate) fn compose_with_policy(
    input: &ComposeInput,
    policy: ProvenanceValidationPolicy<'_>,
) -> Result<ComposeReceipt, ComposeError> {
    if input.schema != COMPOSE_INPUT_SCHEMA {
        return Err(invalid("unsupported compose-input schema"));
    }
    require_available_production_anchors(policy)?;
    let output = pin_absent_root(&input.output)?;
    let blocked = pin_absent_root(&input.blocked_evidence)?;
    let execution = pin_absent_root(&input.evidence_root)?;
    validate_output_roots(input, output.absent(), blocked.absent(), execution.absent())?;
    let first = capture_all_inputs(input)?;
    let request =
        build_request_with_policy_and_roots(input, policy, blocked.absent(), execution.absent())?;
    provenance::validate_request(&request, policy).map_err(|error| map_provenance(&error))?;
    let bytes = canonical_json_line(&request)?;
    let request_sha256 = evidence::sha256_bytes(&bytes);
    let second = capture_all_inputs(input)?;
    if first != second {
        return Err(invalid(
            "one or more inputs changed between composition and publication",
        ));
    }
    // The semantic validator reads every input again immediately before this create-new commit.
    provenance::validate_request(&request, policy).map_err(|error| map_provenance(&error))?;
    let request_path = output.absent().canonical_path.clone();
    output.atomic_create_new_synced_mode(&bytes, 0o444)?;
    let published = evidence::stable_file_identity(&request_path)?;
    if published.sha256 != request_sha256 {
        return Err(invalid("published request differs from composed bytes"));
    }
    Ok(ComposeReceipt {
        schema: "distd.quic.fetch.compose-receipt.v3".to_owned(),
        request_path,
        request_sha256,
        input_identities: first
            .into_iter()
            .map(|(path, identity)| (path, identity.sha256))
            .collect(),
        product_invocations: 0,
    })
}

pub fn build_request(input: &ComposeInput) -> Result<Request, ComposeError> {
    build_request_with_policy(input, ProvenanceValidationPolicy::production())
}

pub(crate) fn build_request_with_policy(
    input: &ComposeInput,
    policy: ProvenanceValidationPolicy<'_>,
) -> Result<Request, ComposeError> {
    if input.schema != COMPOSE_INPUT_SCHEMA {
        return Err(invalid("unsupported compose-input schema"));
    }
    require_available_production_anchors(policy)?;
    let output = pin_absent_root(&input.output)?;
    let blocked = pin_absent_root(&input.blocked_evidence)?;
    let execution = pin_absent_root(&input.evidence_root)?;
    validate_output_roots(input, output.absent(), blocked.absent(), execution.absent())?;
    build_request_with_policy_and_roots(input, policy, blocked.absent(), execution.absent())
}

fn build_request_with_policy_and_roots(
    input: &ComposeInput,
    policy: ProvenanceValidationPolicy<'_>,
    blocked: &AbsentRoot,
    execution: &AbsentRoot,
) -> Result<Request, ComposeError> {
    let capture: RepositoryCaptureReceipt =
        provenance::read_canonical_input(&input.repository_capture, "repository capture")
            .map_err(|error| map_provenance(&error))?;
    if capture.schema != CAPTURE_SCHEMA || capture.product_invocations != 0 {
        return Err(invalid(
            "repository capture is not a non-product v2 capture",
        ));
    }
    let baseline = bind_artifact(&input.baseline, ArtifactRole::Baseline)?;
    let candidate = bind_artifact(&input.candidate, ArtifactRole::Candidate)?;
    let patch: ReviewedPatchManifest =
        provenance::read_canonical_input(&input.reviewed_patch_manifest, "reviewed patch")
            .map_err(|error| map_provenance(&error))?;
    let request_policy = ProvenancePolicy {
        baseline_origin: super::model::ArtifactOrigin::RecoveredFrozen,
        candidate_origin: super::model::ArtifactOrigin::RecoveredFrozen,
        status: super::model::ProvenanceStatusPolicy::EvidenceDerivedOnly,
        baseline_artifact_sha256: policy.baseline_artifact_sha256().to_owned(),
        candidate_artifact_sha256: policy.candidate_artifact_sha256().to_owned(),
        baseline_legacy_primary_sha256: policy.baseline_legacy_primary_sha256().to_owned(),
        baseline_legacy_secondary_sha256: policy.baseline_legacy_secondary_sha256().to_owned(),
        baseline_legacy_environment_sha256: policy
            .baseline_legacy_environment_sha256()
            .map(str::to_owned),
        baseline_legacy_preregistration_sha256: policy
            .baseline_legacy_preregistration_sha256()
            .map(str::to_owned),
        baseline_legacy_build_result_sha256: policy
            .baseline_legacy_build_result_sha256()
            .map(str::to_owned),
        candidate_legacy_primary_sha256: policy.candidate_legacy_primary_sha256().to_owned(),
        candidate_legacy_secondary_sha256: policy.candidate_legacy_secondary_sha256().to_owned(),
        baseline_source_bundle_sha256: policy.baseline_source_bundle_sha256().map(str::to_owned),
        candidate_source_bundle_sha256: policy.candidate_source_bundle_sha256().to_owned(),
    };
    let request = Request {
        schema: REQUEST_SCHEMA.to_owned(),
        repository: capture.repository,
        baseline,
        candidate,
        provenance_policy: request_policy,
        reviewed_patch: ReviewedPatch {
            manifest: canonical_file_input(&input.reviewed_patch_manifest)?,
            record: patch,
        },
        harness: HarnessContract::fixed(),
        blocked_evidence: blocked.canonical_path.clone(),
        evidence_root: execution.canonical_path.clone(),
    };
    provenance::validate_request(&request, policy).map_err(|error| map_provenance(&error))?;
    Ok(request)
}

fn bind_artifact(files: &ArtifactFiles, role: ArtifactRole) -> Result<ArtifactInput, ComposeError> {
    let provenance: ArtifactProvenance =
        provenance::read_canonical_input(&files.provenance_receipt, "provenance receipt")
            .map_err(|error| map_provenance(&error))?;
    if provenance.role != role {
        return Err(invalid("artifact provenance has the wrong role"));
    }
    Ok(ArtifactInput {
        executable: canonical_file_input(&files.executable)?,
        provenance_receipt: canonical_file_input(&files.provenance_receipt)?,
        source_snapshot: canonical_file_input(&files.source_snapshot)?,
        source_manifest: canonical_file_input(&files.source_manifest)?,
        source_bundle: canonical_file_input(&files.source_bundle)?,
        legacy_evidence: LegacyEvidence {
            primary: canonical_file_input(&files.legacy_evidence.primary)?,
            secondary: canonical_file_input(&files.legacy_evidence.secondary)?,
            environment: files
                .legacy_evidence
                .environment
                .as_ref()
                .map(canonical_file_input)
                .transpose()?,
            preregistration: files
                .legacy_evidence
                .preregistration
                .as_ref()
                .map(canonical_file_input)
                .transpose()?,
            build_result: files
                .legacy_evidence
                .build_result
                .as_ref()
                .map(canonical_file_input)
                .transpose()?,
        },
        benchmark_source: canonical_file_input(&files.benchmark_source)?,
        cargo_lock: canonical_file_input(&files.cargo_lock)?,
        workspace_manifest: canonical_file_input(&files.workspace_manifest)?,
        package_manifest: canonical_file_input(&files.package_manifest)?,
        cargo_config: files
            .cargo_config
            .as_ref()
            .map(canonical_file_input)
            .transpose()?,
        rustc_verbose: canonical_file_input(&files.rustc_verbose)?,
        cargo_verbose: canonical_file_input(&files.cargo_verbose)?,
        build_environment: canonical_file_input(&files.build_environment)?,
        profile_identity: canonical_file_input(&files.profile_identity)?,
        provenance,
    })
}

fn validate_output_roots(
    input: &ComposeInput,
    output: &AbsentRoot,
    blocked: &AbsentRoot,
    evidence: &AbsentRoot,
) -> Result<(), ComposeError> {
    let roots = [
        &output.canonical_path,
        &blocked.canonical_path,
        &evidence.canonical_path,
    ];
    for (index, left) in roots.iter().enumerate() {
        for right in &roots[index + 1..] {
            if overlaps(left, right) {
                return Err(invalid("compose, blocked, and evidence outputs overlap"));
            }
        }
    }
    let capture: RepositoryCaptureReceipt =
        provenance::read_canonical_input(&input.repository_capture, "repository capture")
            .map_err(|error| map_provenance(&error))?;
    let repository = std::fs::canonicalize(&capture.repository.root)
        .map_err(|cause| invalid(format!("cannot canonicalize repository: {cause}")))?;
    for root in roots {
        if root.starts_with(&repository) || repository.starts_with(root) {
            return Err(invalid("compose outputs overlap the repository"));
        }
    }
    for file in all_file_inputs(input) {
        let canonical = canonical_file_input(file)?;
        let parent = canonical
            .path
            .parent()
            .ok_or_else(|| invalid("input has no parent"))?;
        for root in roots {
            if root.starts_with(parent) || canonical.path.starts_with(root) {
                return Err(invalid("compose output overlaps an input tree"));
            }
        }
    }
    Ok(())
}

fn overlaps(left: &Path, right: &Path) -> bool {
    left == right || left.starts_with(right) || right.starts_with(left)
}

fn capture_all_inputs(
    input: &ComposeInput,
) -> Result<BTreeMap<PathBuf, evidence::FileIdentity>, ComposeError> {
    let mut identities = BTreeMap::new();
    for file in all_file_inputs(input) {
        let file = canonical_file_input(file)?;
        let identity =
            evidence::stable_file_identity_bounded(&file.path, MAX_PROVENANCE_INPUT_BYTES)?;
        if identities.insert(file.path.clone(), identity).is_some() {
            return Err(invalid("the same canonical input path was supplied twice"));
        }
    }
    Ok(identities)
}

fn require_available_production_anchors(
    policy: ProvenanceValidationPolicy<'_>,
) -> Result<(), ComposeError> {
    if policy.baseline_source_bundle_sha256().is_none() {
        return Err(invalid(
            "baseline build-source bundle anchor is unavailable; production remains blocked",
        ));
    }
    Ok(())
}

fn all_file_inputs(input: &ComposeInput) -> Vec<&FileInput> {
    let mut inputs = vec![&input.repository_capture, &input.reviewed_patch_manifest];
    for artifact in [&input.baseline, &input.candidate] {
        inputs.extend([
            &artifact.executable,
            &artifact.provenance_receipt,
            &artifact.source_snapshot,
            &artifact.source_manifest,
            &artifact.source_bundle,
            &artifact.legacy_evidence.primary,
            &artifact.legacy_evidence.secondary,
            &artifact.benchmark_source,
            &artifact.cargo_lock,
            &artifact.workspace_manifest,
            &artifact.package_manifest,
            &artifact.rustc_verbose,
            &artifact.cargo_verbose,
            &artifact.build_environment,
            &artifact.profile_identity,
        ]);
        if let Some(environment) = &artifact.legacy_evidence.environment {
            inputs.push(environment);
        }
        if let Some(preregistration) = &artifact.legacy_evidence.preregistration {
            inputs.push(preregistration);
        }
        if let Some(build_result) = &artifact.legacy_evidence.build_result {
            inputs.push(build_result);
        }
        if let Some(config) = &artifact.cargo_config {
            inputs.push(config);
        }
    }
    inputs
}

fn canonical_file_input(input: &FileInput) -> Result<FileInput, ComposeError> {
    provenance::validate_file_input(input, "compose input")
        .map_err(|error| map_provenance(&error))?;
    Ok(input.clone())
}

fn map_provenance(error: &provenance::ProvenanceError) -> ComposeError {
    let prefix = match error.class {
        ValidationClass::Schema => "schema",
        ValidationClass::Harness => "harness",
        ValidationClass::Path => "path",
        ValidationClass::Input => "input",
        ValidationClass::Anchor => "anchor",
        ValidationClass::Source => "source",
        ValidationClass::Patch => "patch",
        ValidationClass::Toolchain => "toolchain",
        ValidationClass::Build => "build",
        ValidationClass::RepositoryRelation => "repository relation",
    };
    invalid(format!("{prefix}: {}", error.detail))
}

fn invalid(detail: impl Into<String>) -> ComposeError {
    ComposeError::Invalid(detail.into())
}

#[cfg(test)]
mod tests {
    use super::super::model::*;
    use super::*;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;

    struct Fixture {
        _temporary: tempfile::TempDir,
        input: ComposeInput,
        policy_hashes: [String; 6],
    }

    impl Fixture {
        fn new() -> Self {
            let temporary = tempfile::tempdir().unwrap();
            let root = fs::canonicalize(temporary.path()).unwrap();
            let repository = root.join("repository");
            let capture_root = root.join("capture");
            let inputs = root.join("inputs");
            let outputs = root.join("outputs");
            for directory in [&repository, &capture_root, &inputs, &outputs] {
                fs::create_dir(directory).unwrap();
            }
            let bundle = SourceBundle {
                schema: SOURCE_BUNDLE_SCHEMA.to_owned(),
                repository_head: "a".repeat(40),
                entries: [
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
                .collect(),
            };
            let manifest = manifest(&bundle);
            let current_manifest =
                canonical_file(&capture_root.join("current-manifest.json"), &manifest);
            let repository_diff = file(&capture_root.join("diff"), b"diff");
            let repository_status = file(&capture_root.join("status"), b"status");
            let repository_lock = file(&repository.join("Cargo.lock"), b"current-lock");
            let repository_quic = file(&repository.join("quic.rs"), b"quic");
            let task = file(&repository.join("task.md"), b"task");
            let identity = RepositoryIdentity {
                root: repository,
                head: "a".repeat(40),
                source_diff: repository_diff.path.clone(),
                source_diff_sha256: repository_diff.sha256.clone(),
                source_status: repository_status.path.clone(),
                source_status_sha256: repository_status.sha256.clone(),
                source_manifest: current_manifest.path.clone(),
                source_manifest_sha256: current_manifest.sha256.clone(),
                cargo_lock: repository_lock.path.clone(),
                cargo_lock_sha256: repository_lock.sha256.clone(),
                quic_source: repository_quic.path.clone(),
                quic_source_sha256: repository_quic.sha256.clone(),
                task_spec: task.path.clone(),
                task_spec_sha256: task.sha256.clone(),
            };
            let capture = RepositoryCaptureReceipt {
                schema: CAPTURE_SCHEMA.to_owned(),
                repository: identity,
                source_manifest: current_manifest,
                output_root: capture_root.clone(),
                product_invocations: 0,
            };
            let repository_capture = canonical_file(&capture_root.join("capture.json"), &capture);
            let baseline = artifact(&inputs.join("baseline"), ArtifactRole::Baseline, &bundle);
            let candidate = artifact(&inputs.join("candidate"), ArtifactRole::Candidate, &bundle);
            let baseline_source = read_provenance(&baseline).source;
            let candidate_source = read_provenance(&candidate).source;
            let patch = ReviewedPatchManifest {
                schema: REVIEWED_PATCH_SCHEMA.to_owned(),
                baseline: baseline_source,
                candidate: candidate_source,
                files: Vec::new(),
            };
            let reviewed_patch_manifest =
                canonical_file(&inputs.join("reviewed-patch.json"), &patch);
            let policy_hashes = [
                baseline.executable.sha256.clone(),
                candidate.executable.sha256.clone(),
                baseline.legacy_evidence.primary.sha256.clone(),
                baseline.legacy_evidence.secondary.sha256.clone(),
                candidate.legacy_evidence.primary.sha256.clone(),
                candidate.legacy_evidence.secondary.sha256.clone(),
            ];
            Self {
                _temporary: temporary,
                input: ComposeInput {
                    schema: COMPOSE_INPUT_SCHEMA.to_owned(),
                    repository_capture,
                    baseline,
                    candidate,
                    reviewed_patch_manifest,
                    blocked_evidence: outputs.join("BLOCKED.json"),
                    evidence_root: outputs.join("evidence"),
                    output: outputs.join("request.json"),
                },
                policy_hashes,
            }
        }

        fn policy(&self) -> ProvenanceValidationPolicy<'_> {
            ProvenanceValidationPolicy::synthetic(
                &self.policy_hashes[0],
                &self.policy_hashes[1],
                &self.policy_hashes[2],
                &self.policy_hashes[3],
                None,
                None,
                None,
                &self.policy_hashes[4],
                &self.policy_hashes[5],
                &self.input.baseline.source_bundle.sha256,
                &self.input.candidate.source_bundle.sha256,
            )
        }
    }

    fn manifest(bundle: &SourceBundle) -> SourceManifest {
        SourceManifest {
            schema: SOURCE_MANIFEST_SCHEMA.to_owned(),
            repository_head: bundle.repository_head.clone(),
            entries: bundle
                .entries
                .iter()
                .map(|entry| SourceManifestEntry {
                    path: entry.path.clone(),
                    mode: entry.mode.clone(),
                    length: u64::try_from(entry.contents.len()).unwrap(),
                    sha256: evidence::sha256_bytes(&entry.contents),
                })
                .collect(),
        }
    }

    #[allow(clippy::too_many_lines)]
    fn artifact(root: &Path, role: ArtifactRole, bundle: &SourceBundle) -> ArtifactFiles {
        fs::create_dir(root).unwrap();
        let executable = file(
            &root.join("executable"),
            match role {
                ArtifactRole::Baseline => b"baseline",
                ArtifactRole::Candidate => b"candidate",
            },
        );
        let mut permissions = fs::metadata(&executable.path).unwrap().permissions();
        permissions.set_mode(0o500);
        fs::set_permissions(&executable.path, permissions).unwrap();
        let source_snapshot = file(&root.join("source.snapshot"), b"snapshot");
        let source_bundle = canonical_file(&root.join("source-bundle.json"), bundle);
        let source_manifest = canonical_file(&root.join("source-manifest.json"), &manifest(bundle));
        let legacy_primary = file(&root.join("legacy-primary"), b"primary");
        let legacy_secondary = file(&root.join("legacy-secondary"), b"secondary");
        let benchmark_source = file(&root.join("quic.rs"), b"quic");
        let cargo_lock = file(&root.join("Cargo.lock"), b"lock");
        let workspace_manifest = file(&root.join("Cargo.toml"), b"workspace");
        let package_manifest = file(&root.join("server-Cargo.toml"), b"server");
        let rustc_verbose = file(&root.join("rustc.txt"), b"rustc");
        let cargo_verbose = file(&root.join("cargo.txt"), b"cargo");
        let environment = BTreeMap::<String, String>::new();
        let build_environment = canonical_file(&root.join("environment.json"), &environment);
        let profile_identity = canonical_file(
            &root.join("profile.json"),
            &ReleaseProfileIdentity {
                schema: RELEASE_PROFILE_SCHEMA.to_owned(),
                profile: "release".to_owned(),
                opt_level: 3,
                lto: true,
                codegen_units: 1,
                panic: "abort".to_owned(),
                strip: true,
            },
        );
        let source = HistoricalSourceIdentity {
            repository_head: bundle.repository_head.clone(),
            source_snapshot_sha256: source_snapshot.sha256.clone(),
            source_identity_sha256: match role {
                ArtifactRole::Baseline => legacy_secondary.sha256.clone(),
                ArtifactRole::Candidate => legacy_primary.sha256.clone(),
            },
            source_manifest_sha256: source_manifest.sha256.clone(),
            source_bundle_sha256: source_bundle.sha256.clone(),
            benchmark_source_sha256: benchmark_source.sha256.clone(),
        };
        let provenance = ArtifactProvenance {
            schema: ARTIFACT_PROVENANCE_SCHEMA.to_owned(),
            role,
            origin: ArtifactOrigin::RecoveredFrozen,
            status: ProvenanceStatus::EvidenceDerived {
                derivation: EvidenceDerivation::SyntheticFixture,
            },
            artifact_sha256: executable.sha256.clone(),
            source,
            dependencies: DependencyIdentity {
                cargo_lock_sha256: cargo_lock.sha256.clone(),
                workspace_manifest_sha256: workspace_manifest.sha256.clone(),
                package_manifest_sha256: package_manifest.sha256.clone(),
                cargo_config_sha256: None,
            },
            toolchain: ToolchainIdentity {
                rustc_release: "fixture".to_owned(),
                rustc_commit_hash: "c".repeat(40),
                cargo_release: "fixture".to_owned(),
                host: "fixture".to_owned(),
                target: "fixture".to_owned(),
                rustc_verbose_sha256: rustc_verbose.sha256.clone(),
                cargo_verbose_sha256: cargo_verbose.sha256.clone(),
            },
            build: BuildIdentity {
                profile: "release".to_owned(),
                profile_identity_sha256: profile_identity.sha256.clone(),
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
                environment,
                environment_sha256: build_environment.sha256.clone(),
            },
        };
        let provenance_receipt = canonical_file(&root.join("provenance.json"), &provenance);
        ArtifactFiles {
            executable,
            provenance_receipt,
            source_snapshot,
            source_manifest,
            source_bundle,
            legacy_evidence: LegacyEvidence {
                primary: legacy_primary,
                secondary: legacy_secondary,
                environment: None,
                preregistration: None,
                build_result: None,
            },
            benchmark_source,
            cargo_lock,
            workspace_manifest,
            package_manifest,
            cargo_config: None,
            rustc_verbose,
            cargo_verbose,
            build_environment,
            profile_identity,
        }
    }

    fn read_provenance(artifact: &ArtifactFiles) -> ArtifactProvenance {
        let bytes = fs::read(&artifact.provenance_receipt.path).unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    fn canonical_file<T: Serialize>(path: &Path, value: &T) -> FileInput {
        file(path, &canonical_json_line(value).unwrap())
    }

    fn file(path: &Path, bytes: &[u8]) -> FileInput {
        fs::write(path, bytes).unwrap();
        let path = fs::canonicalize(path).unwrap();
        FileInput {
            sha256: evidence::sha256_file(&path).unwrap(),
            path,
        }
    }

    #[test]
    fn unknown_compose_field_is_rejected() {
        let bytes = br#"{"schema":"distd.quic.fetch.compose-input.v3","unknown":true}
"#;
        assert!(parse_canonical_compose_input(bytes).is_err());
    }

    #[test]
    fn overlapping_paths_are_detected_lexically() {
        assert!(overlaps(Path::new("/a/b"), Path::new("/a/b/c")));
        assert!(!overlaps(Path::new("/a/b"), Path::new("/a/c")));
    }

    #[test]
    fn valid_synthetic_compose_publishes_read_only_canonical_request_once() {
        let fixture = Fixture::new();
        let receipt = compose_with_policy(&fixture.input, fixture.policy()).unwrap();
        let (_, bytes) = evidence::stable_file_bytes(&receipt.request_path).unwrap();
        let request = provenance::parse_canonical_request(&bytes).unwrap();
        provenance::validate_request(&request, fixture.policy()).unwrap();
        assert_eq!(receipt.product_invocations, 0);
        assert_eq!(
            fs::metadata(&receipt.request_path)
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o444
        );
        assert!(compose_with_policy(&fixture.input, fixture.policy()).is_err());
    }

    #[test]
    fn wrong_legacy_anchor_or_current_product_source_creates_no_output() {
        let mut fixture = Fixture::new();
        fixture.policy_hashes[2] = "0".repeat(64);
        assert!(compose_with_policy(&fixture.input, fixture.policy()).is_err());
        assert!(!fixture.input.output.exists());

        let mut fixture = Fixture::new();
        let mut capture: RepositoryCaptureReceipt =
            provenance::read_canonical_input(&fixture.input.repository_capture, "capture").unwrap();
        let mut current: SourceManifest =
            provenance::read_canonical_input(&capture.source_manifest, "manifest").unwrap();
        current.entries.last_mut().unwrap().sha256 = "f".repeat(64);
        capture.source_manifest =
            canonical_file(&capture.output_root.join("drifted-manifest.json"), &current);
        capture.repository.source_manifest = capture.source_manifest.path.clone();
        capture.repository.source_manifest_sha256 = capture.source_manifest.sha256.clone();
        fixture.input.repository_capture =
            canonical_file(&capture.output_root.join("drifted-capture.json"), &capture);
        assert!(compose_with_policy(&fixture.input, fixture.policy()).is_err());
        assert!(!fixture.input.output.exists());
    }
}

#![cfg(unix)]

use std::fs;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use tempfile::TempDir;

use super::compose::{self, ArtifactFiles, ComposeInput, COMPOSE_INPUT_SCHEMA};
use super::environment::{
    EnvironmentBlocked, EnvironmentExpectations, EnvironmentPass, EnvironmentProbe,
    EnvironmentProbeError, LoopbackFacts, OsFacts, PowerFacts,
};
use super::evidence::{canonical_json_line, sha256_bytes};
use super::execute::{
    execute, execute_with_dependencies_and_policy, verify_with_policy, ExecuteError,
    ExecutionDependencies, ExecutionPreflight,
};
use super::model::{
    ArtifactInput, ArtifactOrigin, ArtifactProvenance, ArtifactRole, BuildIdentity,
    DependencyIdentity, DriverIdentity, EvidenceDerivation, FileInput, HarnessContract,
    HarnessRecord, HistoricalSourceIdentity, LegacyEvidence, Plan, PreflightIdentity,
    ProvenancePolicy, ProvenanceStatus, ProvenanceStatusPolicy, ReleaseProfileIdentity,
    RepositoryIdentity, Request, ReviewedPatch, ReviewedPatchFile, ReviewedPatchFileIdentity,
    ReviewedPatchManifest, ReviewedPatchOperation, SourceBundle, SourceBundleEntry, SourceManifest,
    SourceManifestEntry, TerminalReceipt, TerminalStatus, ToolchainIdentity,
    ARTIFACT_PROVENANCE_SCHEMA, HARNESS_SCHEMA, OPERATIONS_PER_RUN, PLAN_SCHEMA,
    RELEASE_PROFILE_SCHEMA, RESPONSE_BLAKE3, RESPONSE_FRAME_BYTES, REVIEWED_PATCH_SCHEMA,
    SOURCE_BUNDLE_SCHEMA, SOURCE_MANIFEST_SCHEMA,
};
use super::prepare::{
    self, LiveRepositoryState, PreflightBlockCode, PrepareOutcome, PreparePaths,
    RepositoryCaptureReceipt, RepositoryProbe, RepositoryVerificationError,
};
use super::process::{ProcessError, TEST_EXECUTION_LOCK};
use super::provenance::ProvenanceValidationPolicy;

const EXECUTION_PREFLIGHT_SCHEMA: &str = "distd.quic.fetch.execution-preflight.v1";

#[derive(Clone, Copy)]
enum ChildBehavior {
    Valid { ns_per_op: u64 },
    SemanticInvalid,
    Nonzero,
    Malformed,
    Multiple,
    Missing,
}

struct Fixture {
    temporary: TempDir,
    plan_path: PathBuf,
    evidence_root: PathBuf,
    spawn_marker: PathBuf,
    baseline_sha256: String,
    candidate_sha256: String,
    baseline_legacy_primary_sha256: String,
    baseline_legacy_secondary_sha256: String,
    baseline_legacy_environment_sha256: Option<String>,
    baseline_legacy_preregistration_sha256: Option<String>,
    baseline_legacy_build_result_sha256: Option<String>,
    candidate_legacy_primary_sha256: String,
    candidate_legacy_secondary_sha256: String,
    baseline_source_bundle_sha256: String,
    candidate_source_bundle_sha256: String,
}

struct ArmBundle {
    input: ArtifactInput,
    source: HistoricalSourceIdentity,
    benchmark_identity: ReviewedPatchFileIdentity,
}

impl Fixture {
    #[allow(clippy::too_many_lines)]
    fn new(baseline: ChildBehavior, candidate: ChildBehavior) -> Self {
        let temporary = tempfile::tempdir().expect("create fixture directory");
        let fixture_root = fs::canonicalize(temporary.path()).expect("canonical fixture root");
        let repository_root = fixture_root.join("repository-worktree");
        fs::create_dir(&repository_root).expect("create repository worktree");
        let spawn_marker = fixture_root.join("child-spawns.log");
        let plan_root = fixture_root.join("plan-root");
        let artifacts = plan_root.join("artifacts");
        let driver_repository = artifacts.join("repository");
        fs::create_dir_all(&driver_repository).expect("create driver repository artifacts");

        let driver_path = write_file(&fixture_root, "driver", b"acceptance driver");
        let environment_path = write_file(&plan_root, "environment.json", b"prepared\n");
        let source_diff = write_file(&driver_repository, "source.snapshot", b"diff\n");
        let source_status = write_file(&driver_repository, "source-status.txt", b"status\n");
        let cargo_lock = write_file(&driver_repository, "Cargo.lock", b"lock\n");
        let quic_source = write_file(
            &driver_repository,
            "quic.rs",
            b"candidate benchmark source\n",
        );
        let task_spec = write_file(&driver_repository, "task-spec.md", b"spec\n");
        let repository_head = "2".repeat(40);
        let repository_source_manifest_record = SourceManifest {
            schema: SOURCE_MANIFEST_SCHEMA.to_string(),
            repository_head: repository_head.clone(),
            entries: source_entries(b"candidate benchmark source\n"),
        };
        let repository_source_manifest = driver_repository.join("source-manifest.json");
        fs::write(
            &repository_source_manifest,
            canonical_json_line(&repository_source_manifest_record)
                .expect("serialize driver repository source manifest"),
        )
        .expect("write driver repository source manifest");

        let reviewed_patch_path = artifacts.join("reviewed-patch.json");
        let baseline_root = artifacts.join("baseline");
        let candidate_root = artifacts.join("candidate");
        fs::create_dir(&baseline_root).expect("create baseline bundle");
        fs::create_dir(&candidate_root).expect("create candidate bundle");
        let baseline = write_arm_bundle(
            &baseline_root,
            &spawn_marker,
            baseline,
            ArtifactRole::Baseline,
            b"baseline benchmark source\n",
        );
        let candidate = write_arm_bundle(
            &candidate_root,
            &spawn_marker,
            candidate,
            ArtifactRole::Candidate,
            b"candidate benchmark source\n",
        );
        let reviewed_patch_record = ReviewedPatchManifest {
            schema: REVIEWED_PATCH_SCHEMA.to_string(),
            baseline: baseline.source.clone(),
            candidate: candidate.source.clone(),
            files: vec![ReviewedPatchFile {
                path: "server/src/quic.rs".to_string(),
                operation: ReviewedPatchOperation::Modified,
                baseline: Some(baseline.benchmark_identity.clone()),
                candidate: Some(candidate.benchmark_identity.clone()),
            }],
        };
        let reviewed_patch_bytes =
            canonical_json_line(&reviewed_patch_record).expect("serialize reviewed patch");
        fs::write(&reviewed_patch_path, &reviewed_patch_bytes).expect("write reviewed patch");
        let reviewed_patch = ReviewedPatch {
            manifest: file_input(reviewed_patch_path),
            record: reviewed_patch_record,
        };
        let evidence_root = fixture_root.join("evidence");

        let baseline_sha256 = baseline.input.executable.sha256.clone();
        let candidate_sha256 = candidate.input.executable.sha256.clone();
        let provenance_policy = ProvenancePolicy {
            baseline_origin: ArtifactOrigin::RecoveredFrozen,
            candidate_origin: ArtifactOrigin::RecoveredFrozen,
            status: ProvenanceStatusPolicy::EvidenceDerivedOnly,
            baseline_artifact_sha256: baseline_sha256.clone(),
            candidate_artifact_sha256: candidate_sha256.clone(),
            baseline_legacy_primary_sha256: baseline.input.legacy_evidence.primary.sha256.clone(),
            baseline_legacy_secondary_sha256: baseline
                .input
                .legacy_evidence
                .secondary
                .sha256
                .clone(),
            baseline_legacy_environment_sha256: baseline
                .input
                .legacy_evidence
                .environment
                .as_ref()
                .map(|input| input.sha256.clone()),
            baseline_legacy_preregistration_sha256: baseline
                .input
                .legacy_evidence
                .preregistration
                .as_ref()
                .map(|input| input.sha256.clone()),
            baseline_legacy_build_result_sha256: baseline
                .input
                .legacy_evidence
                .build_result
                .as_ref()
                .map(|input| input.sha256.clone()),
            candidate_legacy_primary_sha256: candidate.input.legacy_evidence.primary.sha256.clone(),
            candidate_legacy_secondary_sha256: candidate
                .input
                .legacy_evidence
                .secondary
                .sha256
                .clone(),
            baseline_source_bundle_sha256: Some(baseline.input.source_bundle.sha256.clone()),
            candidate_source_bundle_sha256: candidate.input.source_bundle.sha256.clone(),
        };
        let repository = RepositoryIdentity {
            root: repository_root,
            head: repository_head,
            source_diff_sha256: file_sha256(&source_diff),
            source_diff,
            source_status_sha256: file_sha256(&source_status),
            source_status,
            source_manifest_sha256: file_sha256(&repository_source_manifest),
            source_manifest: repository_source_manifest,
            cargo_lock_sha256: file_sha256(&cargo_lock),
            cargo_lock,
            quic_source_sha256: file_sha256(&quic_source),
            quic_source,
            task_spec_sha256: file_sha256(&task_spec),
            task_spec,
        };
        let blocked_evidence = fixture_root.join("blocked.json");
        let request = Request {
            schema: super::model::REQUEST_SCHEMA.to_owned(),
            repository: repository.clone(),
            baseline: baseline.input.clone(),
            candidate: candidate.input.clone(),
            provenance_policy: provenance_policy.clone(),
            reviewed_patch: reviewed_patch.clone(),
            harness: HarnessContract::fixed(),
            blocked_evidence: blocked_evidence.clone(),
            evidence_root: evidence_root.clone(),
        };
        let composed_request_path = plan_root.join("request.json");
        fs::write(
            &composed_request_path,
            canonical_json_line(&request).expect("serialize composed request"),
        )
        .expect("write composed request");
        let plan = Plan {
            schema: PLAN_SCHEMA.to_string(),
            driver: DriverIdentity {
                sha256: file_sha256(&driver_path),
                device: fs::metadata(&driver_path).expect("driver metadata").dev(),
                inode: fs::metadata(&driver_path).expect("driver metadata").ino(),
                path: driver_path,
            },
            preflight: PreflightIdentity {
                environment_sha256: file_sha256(&environment_path),
                environment_path,
            },
            repository,
            provenance_policy,
            baseline: baseline.input,
            candidate: candidate.input,
            reviewed_patch,
            harness: HarnessContract::fixed(),
            blocked_evidence,
            evidence_root: evidence_root.clone(),
            composed_request: file_input(composed_request_path),
        };
        write_file(&plan_root, "input-identities.json", b"{}\n");
        write_file(&plan_root, "prepare-receipt.json", b"{}\n");
        let plan_path = plan_root.join("plan.json");
        let plan_bytes = canonical_json_line(&plan).expect("serialize sealed plan");
        fs::write(&plan_path, &plan_bytes).expect("write sealed plan");
        fs::write(
            plan_root.join("plan.sha256"),
            format!("{}  plan.json\n", sha256_bytes(&plan_bytes)),
        )
        .expect("write plan seal");
        seal_plan_tree(&plan_root);
        for name in [
            "environment.json",
            "input-identities.json",
            "prepare-receipt.json",
            "request.json",
            "plan.json",
            "plan.sha256",
        ] {
            set_mode(&plan_root.join(name), 0o444);
        }
        set_mode(&plan_root, 0o555);

        Self {
            temporary,
            plan_path,
            evidence_root,
            spawn_marker,
            baseline_sha256,
            candidate_sha256,
            baseline_legacy_primary_sha256: plan.baseline.legacy_evidence.primary.sha256.clone(),
            baseline_legacy_secondary_sha256: plan
                .baseline
                .legacy_evidence
                .secondary
                .sha256
                .clone(),
            baseline_legacy_environment_sha256: plan
                .baseline
                .legacy_evidence
                .environment
                .as_ref()
                .map(|input| input.sha256.clone()),
            baseline_legacy_preregistration_sha256: plan
                .baseline
                .legacy_evidence
                .preregistration
                .as_ref()
                .map(|input| input.sha256.clone()),
            baseline_legacy_build_result_sha256: plan
                .baseline
                .legacy_evidence
                .build_result
                .as_ref()
                .map(|input| input.sha256.clone()),
            candidate_legacy_primary_sha256: plan.candidate.legacy_evidence.primary.sha256.clone(),
            candidate_legacy_secondary_sha256: plan
                .candidate
                .legacy_evidence
                .secondary
                .sha256
                .clone(),
            baseline_source_bundle_sha256: plan.baseline.source_bundle.sha256.clone(),
            candidate_source_bundle_sha256: plan.candidate.source_bundle.sha256.clone(),
        }
    }

    fn terminal(&self) -> TerminalReceipt {
        serde_json::from_slice(
            &fs::read(self.evidence_root.join("terminal.json")).expect("read terminal receipt"),
        )
        .expect("decode terminal receipt")
    }

    fn spawn_count(&self) -> usize {
        fs::read_to_string(&self.spawn_marker).map_or(0, |contents| contents.lines().count())
    }

    fn validation_policy(&self) -> ProvenanceValidationPolicy<'_> {
        ProvenanceValidationPolicy::synthetic(
            &self.baseline_sha256,
            &self.candidate_sha256,
            &self.baseline_legacy_primary_sha256,
            &self.baseline_legacy_secondary_sha256,
            self.baseline_legacy_environment_sha256.as_deref(),
            self.baseline_legacy_preregistration_sha256.as_deref(),
            self.baseline_legacy_build_result_sha256.as_deref(),
            &self.candidate_legacy_primary_sha256,
            &self.candidate_legacy_secondary_sha256,
            &self.baseline_source_bundle_sha256,
            &self.candidate_source_bundle_sha256,
        )
    }

    fn plan(&self) -> Plan {
        serde_json::from_slice(&fs::read(&self.plan_path).expect("read fixture plan"))
            .expect("decode fixture plan")
    }
}

#[allow(clippy::too_many_lines)]
fn write_arm_bundle(
    root: &Path,
    spawn_marker: &Path,
    behavior: ChildBehavior,
    role: ArtifactRole,
    benchmark_bytes: &[u8],
) -> ArmBundle {
    let executable = root.join("executable");
    write_child(&executable, spawn_marker, behavior, role);
    let source_snapshot = write_file(root, "source.snapshot", b"source snapshot\n");
    let benchmark_source = write_file(root, "benchmark-source.rs", benchmark_bytes);
    let cargo_lock = write_file(root, "Cargo.lock", b"historical lock\n");
    let workspace_manifest = write_file(root, "workspace-Cargo.toml", b"[workspace]\n");
    let package_manifest = write_file(
        root,
        "package-Cargo.toml",
        b"[package]\nname = \"distd_server\"\n",
    );
    let rustc_verbose = write_file(
        root,
        "rustc-verbose.txt",
        b"rustc 1.96.0\nrelease: 1.96.0\ncommit-hash: 5555555555555555555555555555555555555555\nhost: aarch64-apple-darwin\n",
    );
    let cargo_verbose = write_file(
        root,
        "cargo-verbose.txt",
        b"cargo 1.96.0 (fixture)\nrelease: 1.96.0\nhost: aarch64-apple-darwin\n",
    );
    let build_environment = write_file(root, "build-environment.json", b"{}\n");
    let profile_identity_record = ReleaseProfileIdentity {
        schema: RELEASE_PROFILE_SCHEMA.to_owned(),
        profile: "release".to_owned(),
        opt_level: 3,
        lto: true,
        codegen_units: 1,
        panic: "abort".to_owned(),
        strip: true,
    };
    let profile_identity_bytes = canonical_json_line(&profile_identity_record).unwrap();
    let profile_identity = write_file(root, "profile-identity.json", &profile_identity_bytes);

    let repository_head = match role {
        ArtifactRole::Baseline => "1".repeat(40),
        ArtifactRole::Candidate => "2".repeat(40),
    };
    let source_bundle_record = SourceBundle {
        schema: SOURCE_BUNDLE_SCHEMA.to_owned(),
        repository_head: repository_head.clone(),
        entries: source_bundle_entries(benchmark_bytes),
    };
    let source_bundle = root.join("source-bundle.json");
    fs::write(
        &source_bundle,
        canonical_json_line(&source_bundle_record).expect("serialize source bundle"),
    )
    .expect("write source bundle");
    let source_manifest_record = SourceManifest {
        schema: SOURCE_MANIFEST_SCHEMA.to_string(),
        repository_head,
        entries: source_entries(benchmark_bytes),
    };
    let source_manifest_bytes =
        canonical_json_line(&source_manifest_record).expect("serialize source manifest");
    let source_manifest = root.join("source-manifest.json");
    fs::write(&source_manifest, source_manifest_bytes).expect("write source manifest");

    let executable_sha256 = file_sha256(&executable);
    let snapshot_sha256 = file_sha256(&source_snapshot);
    let (legacy_primary_bytes, legacy_secondary_bytes) = match role {
        ArtifactRole::Baseline => (
            format!(
                "source_sha256={executable_sha256}\nfrozen_sha256={executable_sha256}\nselected_test=quic::tests::manual_quic_authenticated_fetch_handler_5000\nselection_count=1\nbenchmark_executed=false\n"
            ),
            format!(
                "HEAD {}\ntracked_diff_sha256 {}\ncargo_lock_sha256 {}\n",
                source_manifest_record.repository_head,
                snapshot_sha256,
                file_sha256(&cargo_lock)
            ),
        ),
        ArtifactRole::Candidate => (
            format!(
                "head={}\ntracked_diff_sha256={}\ncargo_lock_sha256={}\nserver_quic_sha256={}\nselected_source_sha256={executable_sha256}\nfrozen_copy_sha256={executable_sha256}\nbenchmark_test=quic::tests::manual_quic_authenticated_fetch_handler_5000\nfinal_executable_selection_count=1\nsource_vs_frozen_copy=IDENTICAL\n",
                source_manifest_record.repository_head,
                snapshot_sha256,
                file_sha256(&cargo_lock),
                file_sha256(&benchmark_source)
            ),
            "classification=EQUIVALENCE-BUILD-NOT-BENCHMARK\ncommand=cargo test --profile release -p distd_server --no-run\nbuild_count=1\nbenchmark_invocation_count=0\nwarmup_invocation_count=0\ntiming_run_count=0\n".to_owned(),
        ),
    };
    let legacy_primary = write_file(root, "legacy-primary.txt", legacy_primary_bytes.as_bytes());
    let legacy_secondary = write_file(
        root,
        "legacy-secondary.txt",
        legacy_secondary_bytes.as_bytes(),
    );
    let legacy_environment = if role == ArtifactRole::Baseline {
        Some(write_file(
            root,
            "legacy-environment.txt",
            b"fixture baseline environment\n",
        ))
    } else {
        None
    };
    let legacy_preregistration = if role == ArtifactRole::Baseline {
        Some(write_file(
            root,
            "legacy-preregistration.txt",
            b"fixture baseline preregistration\n",
        ))
    } else {
        None
    };
    let legacy_build_result = if role == ArtifactRole::Baseline {
        Some(write_file(
            root,
            "legacy-build-result.txt",
            b"fixture baseline build result\n",
        ))
    } else {
        None
    };
    let source_identity = match role {
        ArtifactRole::Baseline => &legacy_secondary,
        ArtifactRole::Candidate => &legacy_primary,
    };
    let source = HistoricalSourceIdentity {
        repository_head: source_manifest_record.repository_head,
        source_snapshot_sha256: snapshot_sha256,
        source_identity_sha256: file_sha256(source_identity),
        source_manifest_sha256: file_sha256(&source_manifest),
        source_bundle_sha256: file_sha256(&source_bundle),
        benchmark_source_sha256: file_sha256(&benchmark_source),
    };
    let provenance = ArtifactProvenance {
        schema: ARTIFACT_PROVENANCE_SCHEMA.to_string(),
        role,
        origin: ArtifactOrigin::RecoveredFrozen,
        status: ProvenanceStatus::EvidenceDerived {
            derivation: EvidenceDerivation::SyntheticFixture,
        },
        artifact_sha256: executable_sha256,
        source: source.clone(),
        dependencies: DependencyIdentity {
            cargo_lock_sha256: file_sha256(&cargo_lock),
            workspace_manifest_sha256: file_sha256(&workspace_manifest),
            package_manifest_sha256: file_sha256(&package_manifest),
            cargo_config_sha256: None,
        },
        toolchain: ToolchainIdentity {
            rustc_release: "1.96.0".to_string(),
            rustc_commit_hash: "5".repeat(40),
            cargo_release: "1.96.0".to_string(),
            host: "aarch64-apple-darwin".to_string(),
            target: "aarch64-apple-darwin".to_string(),
            rustc_verbose_sha256: file_sha256(&rustc_verbose),
            cargo_verbose_sha256: file_sha256(&cargo_verbose),
        },
        build: BuildIdentity {
            profile: "release".to_string(),
            profile_identity_sha256: file_sha256(&profile_identity),
            features: vec!["redb".to_owned()],
            command_argv: vec![
                "cargo".to_string(),
                "test".to_string(),
                "--profile".to_string(),
                "release".to_string(),
                "-p".to_string(),
                "distd_server".to_string(),
                "--no-run".to_string(),
            ],
            environment: std::collections::BTreeMap::new(),
            environment_sha256: file_sha256(&build_environment),
        },
    };
    let provenance_receipt = root.join("provenance.json");
    fs::write(
        &provenance_receipt,
        canonical_json_line(&provenance).expect("serialize provisional provenance"),
    )
    .expect("write provisional provenance");

    let benchmark_identity = ReviewedPatchFileIdentity {
        mode: "100644".to_string(),
        length: u64::try_from(benchmark_bytes.len()).expect("benchmark fixture length"),
        sha256: file_sha256(&benchmark_source),
    };
    ArmBundle {
        input: ArtifactInput {
            executable: file_input(executable),
            provenance_receipt: file_input(provenance_receipt),
            source_snapshot: file_input(source_snapshot),
            source_manifest: file_input(source_manifest),
            source_bundle: file_input(source_bundle),
            legacy_evidence: LegacyEvidence {
                primary: file_input(legacy_primary),
                secondary: file_input(legacy_secondary),
                environment: legacy_environment.map(file_input),
                preregistration: legacy_preregistration.map(file_input),
                build_result: legacy_build_result.map(file_input),
            },
            benchmark_source: file_input(benchmark_source),
            cargo_lock: file_input(cargo_lock),
            workspace_manifest: file_input(workspace_manifest),
            package_manifest: file_input(package_manifest),
            cargo_config: None,
            rustc_verbose: file_input(rustc_verbose),
            cargo_verbose: file_input(cargo_verbose),
            build_environment: file_input(build_environment),
            profile_identity: file_input(profile_identity),
            provenance,
        },
        source,
        benchmark_identity,
    }
}

fn source_bundle_entries(quic: &[u8]) -> Vec<SourceBundleEntry> {
    [
        ("Cargo.toml", b"[workspace]\n".as_slice()),
        (
            "bench/Cargo.toml",
            b"[package]\nname = \"bench\"\n".as_slice(),
        ),
        (
            "client/Cargo.toml",
            b"[package]\nname = \"client\"\n".as_slice(),
        ),
        (
            "core/Cargo.toml",
            b"[package]\nname = \"core\"\n".as_slice(),
        ),
        (
            "rust-toolchain.toml",
            b"[toolchain]\nchannel = \"stable\"\n".as_slice(),
        ),
        (
            "server/Cargo.toml",
            b"[package]\nname = \"distd_server\"\n".as_slice(),
        ),
        ("server/src/quic.rs", quic),
    ]
    .into_iter()
    .map(|(path, contents)| SourceBundleEntry {
        path: path.to_owned(),
        mode: "100644".to_owned(),
        contents: contents.to_vec(),
    })
    .collect()
}

fn source_entries(quic: &[u8]) -> Vec<SourceManifestEntry> {
    source_bundle_entries(quic)
        .into_iter()
        .map(|entry| SourceManifestEntry {
            path: entry.path,
            mode: entry.mode,
            length: u64::try_from(entry.contents.len()).expect("source length"),
            sha256: sha256_bytes(&entry.contents),
        })
        .collect()
}

fn file_input(path: PathBuf) -> FileInput {
    FileInput {
        sha256: file_sha256(&path),
        path,
    }
}

fn seal_plan_tree(plan_root: &Path) {
    for directory in [
        plan_root.join("artifacts/repository"),
        plan_root.join("artifacts/baseline"),
        plan_root.join("artifacts/candidate"),
    ] {
        for entry in fs::read_dir(&directory).expect("list plan bundle") {
            let path = entry.expect("bundle entry").path();
            let mode = if path.file_name().and_then(|name| name.to_str()) == Some("executable") {
                0o555
            } else {
                0o444
            };
            set_mode(&path, mode);
        }
        set_mode(&directory, 0o555);
    }
    set_mode(&plan_root.join("artifacts/reviewed-patch.json"), 0o444);
    set_mode(&plan_root.join("artifacts"), 0o555);
}

fn rewrite_sealed_plan(fixture: &Fixture, plan: &Plan) {
    let plan_root = fixture.plan_path.parent().expect("plan root");
    let seal_path = plan_root.join("plan.sha256");
    set_mode(plan_root, 0o755);
    set_mode(&fixture.plan_path, 0o644);
    set_mode(&seal_path, 0o644);
    let bytes = canonical_json_line(plan).expect("serialize rewritten plan");
    fs::write(&fixture.plan_path, &bytes).expect("rewrite plan");
    fs::write(&seal_path, format!("{}  plan.json\n", sha256_bytes(&bytes)))
        .expect("rewrite plan seal");
    set_mode(&fixture.plan_path, 0o444);
    set_mode(&seal_path, 0o444);
    set_mode(plan_root, 0o555);
}

fn rewrite_candidate_provenance(fixture: &Fixture, mutate: impl FnOnce(&mut ArtifactProvenance)) {
    let mut plan = fixture.plan();
    mutate(&mut plan.candidate.provenance);
    let receipt_path = plan.candidate.provenance_receipt.path.clone();
    let arm_root = receipt_path.parent().expect("candidate arm root");
    set_mode(arm_root, 0o755);
    set_mode(&receipt_path, 0o644);
    let bytes = canonical_json_line(&plan.candidate.provenance)
        .expect("serialize rewritten candidate provenance");
    fs::write(&receipt_path, &bytes).expect("rewrite candidate provenance");
    plan.candidate.provenance_receipt.sha256 = sha256_bytes(&bytes);
    set_mode(&receipt_path, 0o444);
    set_mode(arm_root, 0o555);
    rewrite_sealed_plan(fixture, &plan);
}

fn tamper_read_only_file(path: &Path, bytes: &[u8]) {
    set_mode(path, 0o644);
    fs::write(path, bytes).expect("tamper read-only fixture input");
    set_mode(path, 0o444);
}

struct FakeDependencies {
    verify_calls: AtomicUsize,
    fail_verify_call: Option<usize>,
    race_root: Option<PathBuf>,
    block_environment: bool,
    signal_verify_call: Option<usize>,
}

impl FakeDependencies {
    fn passing() -> Self {
        Self {
            verify_calls: AtomicUsize::new(0),
            fail_verify_call: None,
            race_root: None,
            block_environment: false,
            signal_verify_call: None,
        }
    }

    fn drift_on(call: usize) -> Self {
        Self {
            verify_calls: AtomicUsize::new(0),
            fail_verify_call: Some(call),
            race_root: None,
            block_environment: false,
            signal_verify_call: None,
        }
    }

    fn racing(root: PathBuf) -> Self {
        Self {
            verify_calls: AtomicUsize::new(0),
            fail_verify_call: None,
            race_root: Some(root),
            block_environment: false,
            signal_verify_call: None,
        }
    }

    fn environment_blocked() -> Self {
        Self {
            verify_calls: AtomicUsize::new(0),
            fail_verify_call: None,
            race_root: None,
            block_environment: true,
            signal_verify_call: None,
        }
    }

    fn signal_on_verify(call: usize) -> Self {
        Self {
            verify_calls: AtomicUsize::new(0),
            fail_verify_call: None,
            race_root: None,
            block_environment: false,
            signal_verify_call: Some(call),
        }
    }
}

impl ExecutionDependencies for FakeDependencies {
    fn verify_plan_inputs(&self, _plan: &Plan) -> Result<(), ExecuteError> {
        let call = self.verify_calls.fetch_add(1, Ordering::SeqCst) + 1;
        if self.signal_verify_call == Some(call) {
            unsafe {
                libc::raise(libc::SIGHUP);
            }
        }
        if self.fail_verify_call == Some(call) {
            return Err(ExecuteError::Plan(
                "input drifted before product spawn".to_string(),
            ));
        }
        Ok(())
    }

    fn rerun_environment_preflight(
        &self,
        plan: &Plan,
    ) -> Result<ExecutionPreflight, EnvironmentBlocked> {
        if self.block_environment {
            return Err(EnvironmentBlocked {
                schema: "distd.quic.fetch.environment-blocked.v1".to_string(),
                status: "BLOCKED".to_string(),
                stage: "environment_preflight".to_string(),
                code: super::environment::EnvironmentBlockCode::ThermalStateNotNominal,
                detail: "fixture thermal block".to_string(),
                product_invocations: 0,
            });
        }
        if let Some(root) = &self.race_root {
            fs::create_dir(root).expect("racer claims exact evidence root");
            fs::write(root.join("racer-owned"), b"do not overwrite\n")
                .expect("write racer sentinel");
        }
        Ok(ExecutionPreflight {
            schema: EXECUTION_PREFLIGHT_SCHEMA.to_string(),
            prepared_environment_sha256: plan.preflight.environment_sha256.clone(),
            current: passing_environment(),
        })
    }
}

struct PassingPrepareEnvironment;

impl EnvironmentProbe for PassingPrepareEnvironment {
    fn observe_os(&self) -> Result<OsFacts, EnvironmentProbeError> {
        Ok(passing_environment().os)
    }

    fn observe_power(&self) -> Result<PowerFacts, EnvironmentProbeError> {
        Ok(passing_environment().power)
    }

    fn bind_close_ipv4_loopback(&self) -> Result<LoopbackFacts, EnvironmentProbeError> {
        Ok(passing_environment().loopback)
    }
}

struct StaticRepositoryProbe {
    state: LiveRepositoryState,
}

impl RepositoryProbe for StaticRepositoryProbe {
    fn observe(
        &self,
        _canonical_root: &Path,
    ) -> Result<LiveRepositoryState, RepositoryVerificationError> {
        Ok(self.state.clone())
    }
}

fn artifact_files(artifact: &ArtifactInput) -> ArtifactFiles {
    ArtifactFiles {
        executable: artifact.executable.clone(),
        provenance_receipt: artifact.provenance_receipt.clone(),
        source_snapshot: artifact.source_snapshot.clone(),
        source_manifest: artifact.source_manifest.clone(),
        source_bundle: artifact.source_bundle.clone(),
        legacy_evidence: artifact.legacy_evidence.clone(),
        benchmark_source: artifact.benchmark_source.clone(),
        cargo_lock: artifact.cargo_lock.clone(),
        workspace_manifest: artifact.workspace_manifest.clone(),
        package_manifest: artifact.package_manifest.clone(),
        cargo_config: artifact.cargo_config.clone(),
        rustc_verbose: artifact.rustc_verbose.clone(),
        cargo_verbose: artifact.cargo_verbose.clone(),
        build_environment: artifact.build_environment.clone(),
        profile_identity: artifact.profile_identity.clone(),
    }
}

#[test]
#[allow(clippy::too_many_lines)]
fn compose_prepare_execute_verify_round_trip_uses_one_sealed_validator() {
    let fixture = Fixture::new(
        ChildBehavior::Valid { ns_per_op: 100 },
        ChildBehavior::Valid { ns_per_op: 104 },
    );
    let source_plan = fixture.plan();
    let input_root = tempfile::tempdir().expect("create compose input root");
    let output_root = tempfile::tempdir().expect("create compose output root");
    let input_path = fs::canonicalize(input_root.path()).expect("canonical compose input root");
    let output_path = fs::canonicalize(output_root.path()).expect("canonical compose output root");
    let repository_root = input_path.join("repository");
    fs::create_dir(&repository_root).expect("create compose repository");
    let repository_cargo = write_file(
        &repository_root,
        "Cargo.lock",
        &fs::read(&source_plan.repository.cargo_lock).expect("read captured lock"),
    );
    let repository_quic = write_file(
        &repository_root,
        "quic.rs",
        &fs::read(&source_plan.candidate.benchmark_source.path).expect("read candidate source"),
    );
    let repository_task = write_file(&repository_root, "task.md", b"fixture task\n");
    let source_diff = write_file(&input_path, "source.snapshot", b"fixture diff\n");
    let source_status = write_file(&input_path, "source-status.txt", b"fixture status\n");
    let source_manifest = write_file(
        &input_path,
        "source-manifest.json",
        &fs::read(&source_plan.candidate.source_manifest.path).expect("read candidate manifest"),
    );
    let repository = RepositoryIdentity {
        root: fs::canonicalize(&repository_root).expect("canonical compose repository"),
        head: source_plan
            .candidate
            .provenance
            .source
            .repository_head
            .clone(),
        source_diff: source_diff.clone(),
        source_diff_sha256: file_sha256(&source_diff),
        source_status: source_status.clone(),
        source_status_sha256: file_sha256(&source_status),
        source_manifest: source_manifest.clone(),
        source_manifest_sha256: file_sha256(&source_manifest),
        cargo_lock: repository_cargo.clone(),
        cargo_lock_sha256: file_sha256(&repository_cargo),
        quic_source: repository_quic.clone(),
        quic_source_sha256: file_sha256(&repository_quic),
        task_spec: repository_task.clone(),
        task_spec_sha256: file_sha256(&repository_task),
    };
    let capture_manifest = file_input(source_manifest.clone());
    let capture = RepositoryCaptureReceipt {
        schema: "distd.quic.fetch.repository-capture.v2".to_owned(),
        repository: repository.clone(),
        source_manifest: capture_manifest,
        output_root: input_path.clone(),
        product_invocations: 0,
    };
    let capture_path = input_path.join("capture.json");
    fs::write(
        &capture_path,
        canonical_json_line(&capture).expect("serialize repository capture"),
    )
    .expect("write repository capture");
    let composed_path = output_path.join("request.json");
    let blocked_path = output_path.join("blocked.json");
    let evidence_path = output_path.join("evidence");
    let compose_input = ComposeInput {
        schema: COMPOSE_INPUT_SCHEMA.to_owned(),
        repository_capture: file_input(capture_path),
        baseline: artifact_files(&source_plan.baseline),
        candidate: artifact_files(&source_plan.candidate),
        reviewed_patch_manifest: source_plan.reviewed_patch.manifest.clone(),
        blocked_evidence: blocked_path.clone(),
        evidence_root: evidence_path.clone(),
        output: composed_path.clone(),
    };
    let policy = fixture.validation_policy();
    let compose_receipt = compose::compose_with_policy(&compose_input, policy)
        .expect("compose canonical synthetic request");
    assert_eq!(compose_receipt.product_invocations, 0);
    let (request_identity, request_bytes) =
        super::evidence::stable_file_bytes(&composed_path).expect("read composed request");
    let request = super::provenance::parse_canonical_request(&request_bytes)
        .expect("parse canonical composed request");
    let prepare_paths = PreparePaths {
        child_working_directory: repository.root.clone(),
        plan_root: output_path.join("prepared-plan"),
        blocked_receipt_path: blocked_path,
        driver_executable: fs::canonicalize(std::env::current_exe().expect("current test driver"))
            .expect("canonical test driver"),
        environment_expectations: EnvironmentExpectations {
            product_version: Some("acceptance-os".to_owned()),
            kernel_identity: Some("acceptance-kernel".to_owned()),
            boot_record: Some("acceptance-boot".to_owned()),
        },
    };
    let repository_probe = StaticRepositoryProbe {
        state: LiveRepositoryState {
            head: repository.head,
            source_diff: fs::read(repository.source_diff).expect("read source diff"),
            source_status: fs::read(repository.source_status).expect("read source status"),
            source_manifest: fs::read(repository.source_manifest).expect("read source manifest"),
        },
    };
    let PrepareOutcome::Prepared { plan_root, .. } =
        prepare::prepare_canonical_request_with_probes(
            &request,
            &request_bytes,
            Some(request_identity),
            &prepare_paths,
            &PassingPrepareEnvironment,
            &repository_probe,
            policy,
        )
        .expect("prepare composed request")
    else {
        panic!("composed request did not prepare");
    };
    assert_eq!(
        fixture.spawn_count(),
        0,
        "compose and prepare must remain outside the product boundary"
    );
    let plan_path = plan_root.join("plan.json");
    let prepared_fixture = Fixture {
        temporary: fixture.temporary,
        plan_path,
        evidence_root: evidence_path,
        spawn_marker: fixture.spawn_marker,
        baseline_sha256: fixture.baseline_sha256,
        candidate_sha256: fixture.candidate_sha256,
        baseline_legacy_primary_sha256: fixture.baseline_legacy_primary_sha256,
        baseline_legacy_secondary_sha256: fixture.baseline_legacy_secondary_sha256,
        baseline_legacy_environment_sha256: fixture.baseline_legacy_environment_sha256,
        baseline_legacy_preregistration_sha256: fixture.baseline_legacy_preregistration_sha256,
        baseline_legacy_build_result_sha256: fixture.baseline_legacy_build_result_sha256,
        candidate_legacy_primary_sha256: fixture.candidate_legacy_primary_sha256,
        candidate_legacy_secondary_sha256: fixture.candidate_legacy_secondary_sha256,
        baseline_source_bundle_sha256: fixture.baseline_source_bundle_sha256,
        candidate_source_bundle_sha256: fixture.candidate_source_bundle_sha256,
    };
    execute_retry(&prepared_fixture, &FakeDependencies::passing())
        .expect("execute prepared fake-child plan");
    assert_eq!(prepared_fixture.spawn_count(), 12);
    fs::remove_dir_all(&input_path)
        .expect("remove original capture and repository inputs after preparation");
    assert_eq!(
        verify_fixture(&prepared_fixture, &prepared_fixture.evidence_root)
            .expect("verify composed and prepared evidence"),
        TerminalStatus::Pass
    );
}

#[test]
fn unknown_request_fields_and_composed_request_drift_fail_before_spawn() {
    let fixture = Fixture::new(
        ChildBehavior::Valid { ns_per_op: 100 },
        ChildBehavior::Valid { ns_per_op: 104 },
    );
    let request = fixture.plan().composed_request.path;
    let mut unknown: serde_json::Value = serde_json::from_slice(
        &fs::read(&request).expect("read canonical request for unknown-field test"),
    )
    .expect("decode canonical request value");
    unknown
        .as_object_mut()
        .expect("request is an object")
        .insert("extra".to_owned(), serde_json::Value::Bool(true));
    let unknown_bytes = canonical_json_line(&unknown).expect("serialize unknown-field request");
    assert!(super::provenance::parse_canonical_request(&unknown_bytes).is_err());

    tamper_read_only_file(&request, b"{}\n");

    assert!(matches!(
        execute_with_dependencies_and_policy(
            &fixture.plan_path,
            &FakeDependencies::passing(),
            fixture.validation_policy(),
        ),
        Err(ExecuteError::Plan(_))
    ));
    assert_eq!(fixture.spawn_count(), 0);
    assert!(!fixture.evidence_root.exists());
}

#[test]
fn v1_request_and_plan_fail_closed_before_product_execution() {
    let fixture = Fixture::new(
        ChildBehavior::Valid { ns_per_op: 100 },
        ChildBehavior::Valid { ns_per_op: 104 },
    );
    let plan = fixture.plan();
    let request = Request {
        schema: "distd.quic.fetch.driver.request.v1".to_string(),
        repository: plan.repository.clone(),
        baseline: plan.baseline.clone(),
        candidate: plan.candidate.clone(),
        provenance_policy: plan.provenance_policy.clone(),
        reviewed_patch: plan.reviewed_patch.clone(),
        harness: plan.harness.clone(),
        blocked_evidence: plan.blocked_evidence.clone(),
        evidence_root: plan.evidence_root.clone(),
    };
    let request_path = fixture
        .plan_path
        .parent()
        .expect("plan root")
        .parent()
        .expect("fixture root")
        .join("v1-request.json");
    let request_bytes = canonical_json_line(&request).expect("serialize v1 request");
    fs::write(&request_path, &request_bytes).expect("write v1 request");
    let request_identity =
        super::evidence::stable_file_identity(&request_path).expect("identify v1 request");
    let outcome = prepare::prepare_canonical_request(
        &request,
        &request_bytes,
        request_identity,
        &PreparePaths {
            child_working_directory: request.repository.root.clone(),
            plan_root: fixture
                .plan_path
                .parent()
                .expect("fixture plan root")
                .parent()
                .expect("fixture root")
                .join("v1-request-plan"),
            blocked_receipt_path: request.blocked_evidence.clone(),
            driver_executable: plan.driver.path.clone(),
            environment_expectations: EnvironmentExpectations::default(),
        },
    )
    .expect("v1 request produces typed BLOCKED evidence");
    assert!(matches!(
        outcome,
        PrepareOutcome::Blocked(receipt) if receipt.code == PreflightBlockCode::RequestSchema
    ));
    assert_eq!(fixture.spawn_count(), 0);

    let mut v1_plan = plan;
    v1_plan.schema = "distd.quic.fetch.driver.plan.v1".to_string();
    rewrite_sealed_plan(&fixture, &v1_plan);
    assert!(matches!(
        execute_with_dependencies_and_policy(
            &fixture.plan_path,
            &FakeDependencies::passing(),
            fixture.validation_policy(),
        ),
        Err(ExecuteError::Plan(_))
    ));
    assert_eq!(fixture.spawn_count(), 0);
}

#[test]
fn production_policy_rejects_synthetic_arm_hashes_before_spawn() {
    let fixture = Fixture::new(
        ChildBehavior::Valid { ns_per_op: 100 },
        ChildBehavior::Valid { ns_per_op: 104 },
    );

    assert!(matches!(
        execute(&fixture.plan_path),
        Err(ExecuteError::Plan(_))
    ));
    assert_eq!(fixture.spawn_count(), 0);
    assert!(!fixture.evidence_root.exists());
}

#[test]
fn supporting_file_drift_is_rejected_by_execute_and_verify_before_spawn() {
    let fixture = Fixture::new(
        ChildBehavior::Valid { ns_per_op: 100 },
        ChildBehavior::Valid { ns_per_op: 104 },
    );
    let cargo_lock = fixture.plan().candidate.cargo_lock.path;
    tamper_read_only_file(&cargo_lock, b"drifted historical lock\n");

    assert!(matches!(
        execute_with_dependencies_and_policy(
            &fixture.plan_path,
            &FakeDependencies::passing(),
            fixture.validation_policy(),
        ),
        Err(ExecuteError::Plan(_))
    ));
    assert!(matches!(
        verify_fixture(&fixture, &fixture.evidence_root),
        Err(ExecuteError::Plan(_))
    ));
    assert_eq!(fixture.spawn_count(), 0);
}

#[test]
fn wrong_arm_semantic_provenance_is_rejected_by_execute_and_verify() {
    let fixture = Fixture::new(
        ChildBehavior::Valid { ns_per_op: 100 },
        ChildBehavior::Valid { ns_per_op: 104 },
    );
    rewrite_candidate_provenance(&fixture, |provenance| {
        provenance.role = ArtifactRole::Baseline;
    });

    assert!(matches!(
        execute_with_dependencies_and_policy(
            &fixture.plan_path,
            &FakeDependencies::passing(),
            fixture.validation_policy(),
        ),
        Err(ExecuteError::Plan(_))
    ));
    assert!(matches!(
        verify_fixture(&fixture, &fixture.evidence_root),
        Err(ExecuteError::Plan(_))
    ));
    assert_eq!(fixture.spawn_count(), 0);
}

#[test]
fn synthetic_policy_rejects_reconstructed_status_before_product_spawn() {
    let fixture = Fixture::new(
        ChildBehavior::Valid { ns_per_op: 100 },
        ChildBehavior::Valid { ns_per_op: 104 },
    );
    rewrite_candidate_provenance(&fixture, |provenance| {
        provenance.status = ProvenanceStatus::Reconstructed {
            authority: "synthetic authority".to_owned(),
            evidence: Vec::new(),
            limitations: vec!["not evidence-derived".to_owned()],
        };
    });

    assert!(matches!(
        execute_with_dependencies_and_policy(
            &fixture.plan_path,
            &FakeDependencies::passing(),
            fixture.validation_policy(),
        ),
        Err(ExecuteError::Plan(_))
    ));
    assert!(matches!(
        verify_fixture(&fixture, &fixture.evidence_root),
        Err(ExecuteError::Plan(_))
    ));
    assert_eq!(fixture.spawn_count(), 0);
}

#[test]
fn synthetic_policy_rejects_source_head_and_legacy_identity_mismatch() {
    for mismatch in ["head", "identity"] {
        let fixture = Fixture::new(
            ChildBehavior::Valid { ns_per_op: 100 },
            ChildBehavior::Valid { ns_per_op: 104 },
        );
        rewrite_candidate_provenance(&fixture, |provenance| match mismatch {
            "head" => provenance.source.repository_head = "f".repeat(40),
            "identity" => provenance.source.source_identity_sha256 = "f".repeat(64),
            _ => unreachable!(),
        });

        assert!(matches!(
            execute_with_dependencies_and_policy(
                &fixture.plan_path,
                &FakeDependencies::passing(),
                fixture.validation_policy(),
            ),
            Err(ExecuteError::Plan(_))
        ));
        assert!(matches!(
            verify_fixture(&fixture, &fixture.evidence_root),
            Err(ExecuteError::Plan(_))
        ));
        assert_eq!(fixture.spawn_count(), 0);
    }
}

#[test]
fn successful_twelve_run_execution_is_pass_and_verifies() {
    let fixture = Fixture::new(
        ChildBehavior::Valid { ns_per_op: 100 },
        ChildBehavior::Valid { ns_per_op: 104 },
    );

    execute_retry(&fixture, &FakeDependencies::passing()).expect("execute PASS fixture");

    assert_eq!(fixture.spawn_count(), 12);
    assert_eq!(fixture.terminal().status, TerminalStatus::Pass);
    verify_fixture(&fixture, &fixture.evidence_root).expect("verify PASS evidence");
}

#[test]
fn threshold_failure_executes_all_runs_and_verifies() {
    let fixture = Fixture::new(
        ChildBehavior::Valid { ns_per_op: 100 },
        ChildBehavior::Valid { ns_per_op: 106 },
    );

    execute_retry(&fixture, &FakeDependencies::passing()).expect("execute FAIL fixture");

    assert_eq!(fixture.spawn_count(), 12);
    assert_eq!(fixture.terminal().status, TerminalStatus::Fail);
    verify_fixture(&fixture, &fixture.evidence_root).expect("verify FAIL evidence");
}

#[test]
fn nonzero_child_produces_a_verifiable_invalid_partial_run() {
    let fixture = Fixture::new(
        ChildBehavior::Nonzero,
        ChildBehavior::Valid { ns_per_op: 100 },
    );

    let error = execute_retry(&fixture, &FakeDependencies::passing()).unwrap_err();

    assert!(matches!(
        error,
        ExecuteError::ChildFailed {
            exit_code: 7,
            signal: None,
            ..
        }
    ));
    assert_eq!(fixture.spawn_count(), 1);
    assert!(!fixture
        .evidence_root
        .join("runs/00-warmup-B/record.json")
        .exists());
    let terminal = fixture.terminal();
    assert_eq!(terminal.status, TerminalStatus::Invalid);
    assert_eq!(terminal.completed_runs, 0);
    verify_fixture(&fixture, &fixture.evidence_root).expect("verify INVALID partial evidence");
}

#[test]
fn malformed_multiple_and_missing_records_each_finalize_as_invalid() {
    for (name, behavior, expected_detail) in [
        ("malformed", ChildBehavior::Malformed, "malformed"),
        ("multiple", ChildBehavior::Multiple, "found 2"),
        ("missing", ChildBehavior::Missing, "found 0"),
    ] {
        let fixture = Fixture::new(behavior, ChildBehavior::Valid { ns_per_op: 100 });
        let error = execute_retry(&fixture, &FakeDependencies::passing()).unwrap_err();

        assert!(
            matches!(&error, ExecuteError::HarnessOutput { detail, .. } if detail.contains(expected_detail)),
            "{name} fixture returned {error:?}"
        );
        assert_eq!(fixture.spawn_count(), 1, "{name} child count");
        assert_eq!(fixture.terminal().status, TerminalStatus::Invalid);
        verify_fixture(&fixture, &fixture.evidence_root)
            .unwrap_or_else(|error| panic!("verify {name} INVALID evidence: {error}"));
    }
}

#[test]
fn semantic_invalidity_after_twelve_parseable_runs_is_verifiable() {
    let fixture = Fixture::new(
        ChildBehavior::SemanticInvalid,
        ChildBehavior::SemanticInvalid,
    );

    let error = execute_retry(&fixture, &FakeDependencies::passing()).unwrap_err();

    assert!(matches!(&error, ExecuteError::Audit(_)), "{error:?}");
    assert_eq!(fixture.spawn_count(), 12);
    let terminal = fixture.terminal();
    assert_eq!(terminal.status, TerminalStatus::Invalid);
    assert_eq!(terminal.completed_runs, 12);
    verify_fixture(&fixture, &fixture.evidence_root)
        .expect("verify twelve-run semantic INVALID evidence");
}

#[test]
fn full_plan_verification_runs_once_before_the_fixed_series() {
    let fixture = Fixture::new(
        ChildBehavior::Valid { ns_per_op: 100 },
        ChildBehavior::Valid { ns_per_op: 100 },
    );
    let dependencies = FakeDependencies::passing();

    let status = execute_retry(&fixture, &dependencies).expect("execute fixed series");

    assert_eq!(status, TerminalStatus::Pass);
    assert_eq!(dependencies.verify_calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.spawn_count(), 12);
}

#[test]
fn initial_input_drift_is_blocked_with_zero_product_and_no_execution_root() {
    let fixture = Fixture::new(
        ChildBehavior::Valid { ns_per_op: 100 },
        ChildBehavior::Valid { ns_per_op: 100 },
    );
    let dependencies = FakeDependencies::drift_on(1);

    let error = execute_retry(&fixture, &dependencies).unwrap_err();

    assert!(matches!(error, ExecuteError::Blocked(detail) if detail.contains("input drifted")));
    assert_eq!(dependencies.verify_calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.spawn_count(), 0);
    assert!(!fixture.evidence_root.exists());
    assert!(fixture
        .evidence_root
        .parent()
        .expect("evidence parent")
        .join("blocked.json")
        .is_file());
}

#[test]
fn writable_plan_root_is_rejected_before_product_execution() {
    let fixture = Fixture::new(
        ChildBehavior::Valid { ns_per_op: 100 },
        ChildBehavior::Valid { ns_per_op: 100 },
    );
    let plan_root = fixture.plan_path.parent().expect("plan root");
    set_mode(plan_root, 0o755);

    let error = execute_retry(&fixture, &FakeDependencies::passing()).unwrap_err();

    assert!(matches!(error, ExecuteError::Plan(detail) if detail.contains("mode 555")));
    assert_eq!(fixture.spawn_count(), 0);
    assert!(!fixture.evidence_root.exists());
}

#[test]
fn environment_block_is_distinct_and_has_zero_product() {
    let fixture = Fixture::new(
        ChildBehavior::Valid { ns_per_op: 100 },
        ChildBehavior::Valid { ns_per_op: 100 },
    );

    let error = execute_retry(&fixture, &FakeDependencies::environment_blocked()).unwrap_err();

    assert!(
        matches!(error, ExecuteError::Blocked(ref detail) if detail == "fixture thermal block"),
        "unexpected error: {error:?}"
    );
    assert_eq!(fixture.spawn_count(), 0);
    assert!(!fixture.evidence_root.exists());
    let blocked: EnvironmentBlocked = serde_json::from_slice(
        &fs::read(
            fixture
                .evidence_root
                .parent()
                .expect("evidence parent")
                .join("blocked.json"),
        )
        .expect("read environment BLOCKED receipt"),
    )
    .expect("decode environment BLOCKED receipt");
    assert_eq!(blocked.product_invocations, 0);
}

#[test]
fn pre_root_managed_signal_is_propagated_without_blocked_or_product_evidence() {
    let fixture = Fixture::new(
        ChildBehavior::Valid { ns_per_op: 100 },
        ChildBehavior::Valid { ns_per_op: 100 },
    );

    let error = execute_retry(&fixture, &FakeDependencies::signal_on_verify(1)).unwrap_err();

    assert!(matches!(error, ExecuteError::Interrupted { signal } if signal == libc::SIGHUP));
    assert_eq!(fixture.spawn_count(), 0);
    assert!(!fixture.evidence_root.exists());
    assert!(!fixture
        .evidence_root
        .parent()
        .expect("evidence parent")
        .join("blocked.json")
        .exists());
}

#[test]
fn completed_root_rejects_second_execution_and_preserves_first_terminal() {
    let fixture = Fixture::new(
        ChildBehavior::Valid { ns_per_op: 100 },
        ChildBehavior::Valid { ns_per_op: 104 },
    );
    let dependencies = FakeDependencies::passing();
    execute_retry(&fixture, &dependencies).expect("first execution");
    let terminal_before = fs::read(fixture.evidence_root.join("terminal.json")).unwrap();
    let spawns_before = fixture.spawn_count();

    let second = execute_retry(&fixture, &dependencies).unwrap_err();

    assert!(matches!(second, ExecuteError::RootCreate(_)));
    assert_eq!(fixture.spawn_count(), spawns_before);
    assert_eq!(
        fs::read(fixture.evidence_root.join("terminal.json")).unwrap(),
        terminal_before
    );
}

#[test]
fn exact_root_claim_between_preflight_and_creation_is_not_overwritten() {
    let fixture = Fixture::new(
        ChildBehavior::Valid { ns_per_op: 100 },
        ChildBehavior::Valid { ns_per_op: 100 },
    );
    let dependencies = FakeDependencies::racing(fixture.evidence_root.clone());

    let error = execute_retry(&fixture, &dependencies).unwrap_err();

    assert!(matches!(error, ExecuteError::RootCreate(ref root) if root == &fixture.evidence_root));
    assert_eq!(
        fs::read(fixture.evidence_root.join("racer-owned")).expect("read racer sentinel"),
        b"do not overwrite\n"
    );
    assert_eq!(fixture.spawn_count(), 0);
    assert!(!fixture.evidence_root.join("terminal.json").exists());
}

#[test]
fn verifier_rejects_extra_and_tampered_evidence() {
    let extra_fixture = Fixture::new(
        ChildBehavior::Valid { ns_per_op: 100 },
        ChildBehavior::Valid { ns_per_op: 104 },
    );
    execute_retry(&extra_fixture, &FakeDependencies::passing()).expect("execute extra fixture");
    fs::write(
        extra_fixture.evidence_root.join("unsealed-extra"),
        b"extra\n",
    )
    .expect("add extra evidence");
    assert!(verify_fixture(&extra_fixture, &extra_fixture.evidence_root).is_err());

    let tampered_fixture = Fixture::new(
        ChildBehavior::Valid { ns_per_op: 100 },
        ChildBehavior::Valid { ns_per_op: 104 },
    );
    execute_retry(&tampered_fixture, &FakeDependencies::passing()).expect("execute tamper fixture");
    fs::write(
        tampered_fixture
            .evidence_root
            .join("runs/00-warmup-B/stdout.log"),
        b"tampered\n",
    )
    .expect("tamper sealed evidence");
    assert!(verify_fixture(&tampered_fixture, &tampered_fixture.evidence_root).is_err());

    let empty_directory_fixture = Fixture::new(
        ChildBehavior::Valid { ns_per_op: 100 },
        ChildBehavior::Valid { ns_per_op: 104 },
    );
    execute_retry(&empty_directory_fixture, &FakeDependencies::passing())
        .expect("execute empty-directory fixture");
    fs::create_dir(empty_directory_fixture.evidence_root.join("unsealed-empty"))
        .expect("add empty evidence directory");
    assert!(verify_fixture(
        &empty_directory_fixture,
        &empty_directory_fixture.evidence_root
    )
    .is_err());

    let manifest_bytes_fixture = Fixture::new(
        ChildBehavior::Valid { ns_per_op: 100 },
        ChildBehavior::Valid { ns_per_op: 104 },
    );
    execute_retry(&manifest_bytes_fixture, &FakeDependencies::passing())
        .expect("execute manifest-byte fixture");
    let manifest_path = manifest_bytes_fixture
        .evidence_root
        .join("evidence-manifest.json");
    let mut noncanonical_manifest = vec![b' '];
    noncanonical_manifest.extend(fs::read(&manifest_path).expect("read manifest"));
    fs::write(&manifest_path, noncanonical_manifest).expect("rewrite manifest noncanonically");
    assert!(verify_fixture(
        &manifest_bytes_fixture,
        &manifest_bytes_fixture.evidence_root
    )
    .is_err());

    let terminal_bytes_fixture = Fixture::new(
        ChildBehavior::Valid { ns_per_op: 100 },
        ChildBehavior::Valid { ns_per_op: 104 },
    );
    execute_retry(&terminal_bytes_fixture, &FakeDependencies::passing())
        .expect("execute terminal-byte fixture");
    let terminal_path = terminal_bytes_fixture.evidence_root.join("terminal.json");
    let mut noncanonical_terminal = vec![b' '];
    noncanonical_terminal.extend(fs::read(&terminal_path).expect("read terminal"));
    fs::write(&terminal_path, noncanonical_terminal).expect("rewrite terminal noncanonically");
    assert!(verify_fixture(
        &terminal_bytes_fixture,
        &terminal_bytes_fixture.evidence_root
    )
    .is_err());
}

#[test]
fn verifier_accepts_a_relative_equivalent_evidence_path() {
    let fixture = Fixture::new(
        ChildBehavior::Valid { ns_per_op: 100 },
        ChildBehavior::Valid { ns_per_op: 104 },
    );
    execute_retry(&fixture, &FakeDependencies::passing()).expect("execute relative-path fixture");
    let parent = fixture.evidence_root.parent().expect("evidence parent");
    fs::create_dir(parent.join("path-alias-component")).expect("create path alias component");
    let equivalent = parent
        .join("path-alias-component")
        .join("..")
        .join("evidence");

    verify_fixture(&fixture, &equivalent).expect("verify equivalent evidence path");
}

fn execute_retry(
    fixture: &Fixture,
    dependencies: &dyn ExecutionDependencies,
) -> Result<TerminalStatus, ExecuteError> {
    // Other unit tests exercise the process-wide signal guard. Waiting here keeps this
    // acceptance module reliable under the default parallel Rust test runner.
    let _execution_lock = TEST_EXECUTION_LOCK.lock().expect("shared execution lock");
    let deadline = Instant::now() + Duration::from_mins(1);
    loop {
        match execute_with_dependencies_and_policy(
            &fixture.plan_path,
            dependencies,
            fixture.validation_policy(),
        ) {
            Err(ExecuteError::Process(ProcessError::ConcurrentGuard))
                if Instant::now() < deadline =>
            {
                thread::sleep(Duration::from_millis(10));
            }
            result => return result,
        }
    }
}

fn verify_fixture(fixture: &Fixture, evidence_root: &Path) -> Result<TerminalStatus, ExecuteError> {
    verify_with_policy(
        &fixture.plan_path,
        evidence_root,
        fixture.validation_policy(),
    )
}

fn passing_environment() -> EnvironmentPass {
    EnvironmentPass {
        schema: "distd.quic.fetch.environment-pass.v1".to_string(),
        status: "PASS".to_string(),
        os: OsFacts {
            product_version: "acceptance-os".to_string(),
            kernel_identity: "acceptance-kernel".to_string(),
            boot_record: "acceptance-boot".to_string(),
        },
        power: PowerFacts {
            ac_power_active: true,
            low_power_mode: false,
            thermal_state: 0,
        },
        loopback: LoopbackFacts {
            address: "127.0.0.1".to_string(),
            port: 49_152,
            closed_before_return: true,
        },
    }
}

fn write_child(path: &Path, spawn_marker: &Path, behavior: ChildBehavior, role: ArtifactRole) {
    let marker = shell_single_quote(spawn_marker.as_os_str().to_string_lossy().as_ref());
    let role = match role {
        ArtifactRole::Baseline => "baseline",
        ArtifactRole::Candidate => "candidate",
    };
    let mut script = format!("#!/bin/sh\n# synthetic {role} arm\nprintf 'spawn\\n' >> {marker}\n");
    match behavior {
        ChildBehavior::Valid { ns_per_op } => {
            script.push_str(&valid_record_command(ns_per_op));
        }
        ChildBehavior::SemanticInvalid => {
            script.push_str(&semantic_invalid_record_command());
        }
        ChildBehavior::Nonzero => {
            script.push_str("printf 'child failed after partial output\\n'\nexit 7\n");
        }
        ChildBehavior::Malformed => {
            script.push_str("printf 'DISTD_QUIC_FETCH_BENCH {not-json}\\n'\n");
        }
        ChildBehavior::Multiple => {
            let command = valid_record_command(100);
            script.push_str(&command);
            script.push_str(&command);
        }
        ChildBehavior::Missing => {
            script.push_str("printf 'ordinary libtest chatter\\n'\n");
        }
    }
    fs::write(path, script).expect("write fake benchmark child");
    let mut permissions = fs::metadata(path)
        .expect("fake child metadata")
        .permissions();
    permissions.set_mode(0o700);
    fs::set_permissions(path, permissions).expect("make fake child executable");
}

fn semantic_invalid_record_command() -> String {
    let mut command = valid_record_command(100);
    command = command.replace("\"operations\":5000", "\"operations\":4999");
    command
}

fn valid_record_command(ns_per_op: u64) -> String {
    let placeholder = "__ACCEPTANCE_RUN_ID__";
    let record = HarnessRecord {
        schema: HARNESS_SCHEMA.to_string(),
        run_id: placeholder.to_string(),
        debug_assertions: false,
        operations: OPERATIONS_PER_RUN,
        elapsed_ns: ns_per_op * OPERATIONS_PER_RUN,
        ns_per_op,
        per_stream_ns: vec![
            1;
            usize::try_from(OPERATIONS_PER_RUN).expect("operation count fits usize")
        ],
        response_frame_bytes: RESPONSE_FRAME_BYTES,
        response_blake3: RESPONSE_BLAKE3.to_string(),
    };
    let json = serde_json::to_string(&record).expect("serialize child record");
    let (before_run_id, after_run_id) = json
        .split_once(placeholder)
        .expect("record contains run ID placeholder");
    format!(
        "printf '%s%s%s\\n' {} \"$DISTD_QUIC_FETCH_BENCH_RUN_ID\" {}\n",
        shell_single_quote(&format!("DISTD_QUIC_FETCH_BENCH {before_run_id}")),
        shell_single_quote(after_run_id),
    )
}

fn shell_single_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\\"'\\\"'"))
}

fn write_file(root: &Path, name: &str, bytes: &[u8]) -> PathBuf {
    let path = root.join(name);
    fs::write(&path, bytes).unwrap_or_else(|error| panic!("write {name}: {error}"));
    path
}

fn set_mode(path: &Path, mode: u32) {
    let mut permissions = fs::metadata(path).expect("read fixture mode").permissions();
    permissions.set_mode(mode);
    fs::set_permissions(path, permissions).expect("set fixture mode");
}

fn file_sha256(path: &Path) -> String {
    sha256_bytes(&fs::read(path).expect("hash fixture file"))
}

use std::collections::BTreeMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

pub const REQUEST_SCHEMA: &str = "distd.quic.fetch.driver.request.v3";
pub const PLAN_SCHEMA: &str = "distd.quic.fetch.driver.plan.v3";
pub const ARTIFACT_PROVENANCE_SCHEMA: &str = "distd.quic.fetch.artifact-provenance.v1";
pub const REVIEWED_PATCH_SCHEMA: &str = "distd.quic.fetch.reviewed-patch.v1";
pub const SOURCE_MANIFEST_SCHEMA: &str = "distd.quic.fetch.source-manifest.v1";
pub const SOURCE_BUNDLE_SCHEMA: &str = "distd.quic.fetch.source-bundle.v1";
pub const RELEASE_PROFILE_SCHEMA: &str = "distd.quic.fetch.release-profile.v1";
pub const CHILD_RECEIPT_SCHEMA: &str = "distd.quic.fetch.driver.child.v1";
pub const RUN_RECORD_SCHEMA: &str = "distd.quic.fetch.driver.run.v1";
pub const AUDIT_SCHEMA: &str = "distd.quic.fetch.driver.audit.v1";
pub const EVENT_SCHEMA: &str = "distd.quic.fetch.driver.event.v1";
pub const HARNESS_SCHEMA: &str = "distd.quic.fetch.v1";

pub const BENCHMARK_TEST_NAME: &str = "quic::tests::manual_quic_authenticated_fetch_handler_5000";
pub const OPERATIONS_PER_RUN: u64 = 5_000;
pub const RESPONSE_FRAME_BYTES: u64 = 217;
pub const RESPONSE_BLAKE3: &str =
    "9b66dc2d89048d154be69e8e43be058df685224c58234a4f1fd9b86320122bb5";
pub const THRESHOLD_NUMERATOR: u64 = 105;
pub const THRESHOLD_DENOMINATOR: u64 = 100;
pub const RUN_ID_ENVIRONMENT_VARIABLE: &str = "DISTD_QUIC_FETCH_BENCH_RUN_ID";
pub const MAX_CHILD_DURATION_SECONDS: u64 = 300;
pub const MAX_CHILD_STDOUT_BYTES: u64 = 4 * 1024 * 1024;
pub const MAX_CHILD_STDERR_BYTES: u64 = 1024 * 1024;
pub const RUN_COUNT: usize = 12;
pub const MEASURED_RUNS_PER_ARM: usize = 5;
pub const BASELINE_ARTIFACT_SHA256: &str =
    "d60c56edf574a9980b3ba9e7572f9da95ca80260e530781495bdff4604979478";
pub const CANDIDATE_ARTIFACT_SHA256: &str =
    "6cb669536efea5b3055c6f4487624b96abc136cc5d036cbfaae672a651fcb327";
pub const BASELINE_LEGACY_PRIMARY_SHA256: &str =
    "098a8af95d9dfae1b07a73939928f2d57c8f951fa8066d32c5776813b0cf8bf9";
pub const BASELINE_LEGACY_SECONDARY_SHA256: &str =
    "fcecacdd6f0f71a3b515616ecb144efd91ade2d09f10428b2c24bac4ba309941";
pub const BASELINE_LEGACY_ENVIRONMENT_SHA256: &str =
    "53e526b066818d82355edd6f7dac54f43a80b0e13c43fdead520cf38b014f3ac";
pub const BASELINE_LEGACY_PREREGISTRATION_SHA256: &str =
    "9fc81d187c3c9c3a0931106ae58a35c90a90efa7331bc444b02b71fb31f8725a";
pub const BASELINE_LEGACY_BUILD_RESULT_SHA256: &str =
    "7f518a88b4b47c79d12b9090d285665aa53a6327ebde8efb34dfcd2bf5b96c99";
pub const CANDIDATE_LEGACY_PRIMARY_SHA256: &str =
    "90ae2c529fb6cd0bb959d485eefa958728a0cd81b3d888b3d2b334a2008c2330";
pub const CANDIDATE_LEGACY_SECONDARY_SHA256: &str =
    "98a55d72755da6e78cca11163f09b20812d0297c185830573de8474fa6f60eaa";
pub const CANDIDATE_SOURCE_BUNDLE_SHA256: &str =
    "fbbb585bfb4404c9abfb01e807451045becf669361f9275c7450b21e88e87c13";

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub schema: String,
    pub repository: RepositoryIdentity,
    pub baseline: ArtifactInput,
    pub candidate: ArtifactInput,
    pub provenance_policy: ProvenancePolicy,
    pub reviewed_patch: ReviewedPatch,
    pub harness: HarnessContract,
    pub blocked_evidence: PathBuf,
    pub evidence_root: PathBuf,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Plan {
    pub schema: String,
    pub driver: DriverIdentity,
    pub preflight: PreflightIdentity,
    pub repository: RepositoryIdentity,
    pub baseline: ArtifactInput,
    pub candidate: ArtifactInput,
    pub provenance_policy: ProvenancePolicy,
    pub reviewed_patch: ReviewedPatch,
    pub harness: HarnessContract,
    pub blocked_evidence: PathBuf,
    pub evidence_root: PathBuf,
    pub composed_request: FileInput,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DriverIdentity {
    pub path: PathBuf,
    pub sha256: String,
    pub device: u64,
    pub inode: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PreflightIdentity {
    pub environment_path: PathBuf,
    pub environment_sha256: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RepositoryIdentity {
    pub root: PathBuf,
    pub head: String,
    pub source_diff: PathBuf,
    pub source_diff_sha256: String,
    pub source_status: PathBuf,
    pub source_status_sha256: String,
    pub source_manifest: PathBuf,
    pub source_manifest_sha256: String,
    pub cargo_lock: PathBuf,
    pub cargo_lock_sha256: String,
    pub quic_source: PathBuf,
    pub quic_source_sha256: String,
    pub task_spec: PathBuf,
    pub task_spec_sha256: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FileInput {
    pub path: PathBuf,
    pub sha256: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactInput {
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
    pub provenance: ArtifactProvenance,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LegacyEvidence {
    pub primary: FileInput,
    pub secondary: FileInput,
    pub environment: Option<FileInput>,
    pub preregistration: Option<FileInput>,
    pub build_result: Option<FileInput>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactProvenance {
    pub schema: String,
    pub role: ArtifactRole,
    pub origin: ArtifactOrigin,
    pub status: ProvenanceStatus,
    pub artifact_sha256: String,
    pub source: HistoricalSourceIdentity,
    pub dependencies: DependencyIdentity,
    pub toolchain: ToolchainIdentity,
    pub build: BuildIdentity,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactRole {
    Baseline,
    Candidate,
}

/// How the executable bytes were obtained, independently of how their provenance was recorded.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ArtifactOrigin {
    RecoveredFrozen,
    ReproducedExact { expected_historical_sha256: String },
}

/// Whether the provenance receipt is the original record or a later reconstruction.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ProvenanceStatus {
    EvidenceDerived {
        derivation: EvidenceDerivation,
    },
    Reconstructed {
        authority: String,
        evidence: Vec<FileInput>,
        limitations: Vec<String>,
    },
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceDerivation {
    ByteExactPayloadsFromDurableSessionLogOriginalPathsAndInodesUnavailable,
    SyntheticFixture,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HistoricalSourceIdentity {
    pub repository_head: String,
    pub source_snapshot_sha256: String,
    /// Role-specific recovered source identity receipt (baseline status; candidate identity).
    pub source_identity_sha256: String,
    pub source_manifest_sha256: String,
    pub source_bundle_sha256: String,
    pub benchmark_source_sha256: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DependencyIdentity {
    pub cargo_lock_sha256: String,
    pub workspace_manifest_sha256: String,
    pub package_manifest_sha256: String,
    pub cargo_config_sha256: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ToolchainIdentity {
    pub rustc_release: String,
    pub rustc_commit_hash: String,
    pub cargo_release: String,
    pub host: String,
    pub target: String,
    pub rustc_verbose_sha256: String,
    pub cargo_verbose_sha256: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BuildIdentity {
    pub profile: String,
    pub profile_identity_sha256: String,
    pub features: Vec<String>,
    pub command_argv: Vec<String>,
    pub environment: BTreeMap<String, String>,
    pub environment_sha256: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReleaseProfileIdentity {
    pub schema: String,
    pub profile: String,
    pub opt_level: u8,
    pub lto: bool,
    pub codegen_units: u32,
    pub panic: String,
    pub strip: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SourceManifest {
    pub schema: String,
    pub repository_head: String,
    pub entries: Vec<SourceManifestEntry>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SourceManifestEntry {
    /// Canonical UTF-8 repository-relative path using `/` separators.
    pub path: String,
    /// Exact Git-compatible mode such as `100644` or `100755`.
    pub mode: String,
    pub length: u64,
    pub sha256: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SourceBundle {
    pub schema: String,
    pub repository_head: String,
    pub entries: Vec<SourceBundleEntry>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SourceBundleEntry {
    pub path: String,
    pub mode: String,
    pub contents: Vec<u8>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReviewedPatch {
    pub manifest: FileInput,
    pub record: ReviewedPatchManifest,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReviewedPatchManifest {
    pub schema: String,
    pub baseline: HistoricalSourceIdentity,
    pub candidate: HistoricalSourceIdentity,
    pub files: Vec<ReviewedPatchFile>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReviewedPatchFile {
    pub path: String,
    pub operation: ReviewedPatchOperation,
    pub baseline: Option<ReviewedPatchFileIdentity>,
    pub candidate: Option<ReviewedPatchFileIdentity>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReviewedPatchFileIdentity {
    pub mode: String,
    pub length: u64,
    pub sha256: String,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReviewedPatchOperation {
    Added,
    Modified,
    Deleted,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProvenancePolicy {
    pub baseline_origin: ArtifactOrigin,
    pub candidate_origin: ArtifactOrigin,
    pub status: ProvenanceStatusPolicy,
    pub baseline_artifact_sha256: String,
    pub candidate_artifact_sha256: String,
    pub baseline_legacy_primary_sha256: String,
    pub baseline_legacy_secondary_sha256: String,
    pub baseline_legacy_environment_sha256: Option<String>,
    pub baseline_legacy_preregistration_sha256: Option<String>,
    pub baseline_legacy_build_result_sha256: Option<String>,
    pub candidate_legacy_primary_sha256: String,
    pub candidate_legacy_secondary_sha256: String,
    /// Absent until the exact baseline build-source bundle is independently recovered.
    pub baseline_source_bundle_sha256: Option<String>,
    pub candidate_source_bundle_sha256: String,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProvenanceStatusPolicy {
    OriginalOnly,
    EvidenceDerivedOnly,
    FutureReconstructedWithAuthority,
}

impl ProvenancePolicy {
    #[must_use]
    pub fn recovered_frozen_evidence_derived() -> Self {
        Self {
            baseline_origin: ArtifactOrigin::RecoveredFrozen,
            candidate_origin: ArtifactOrigin::RecoveredFrozen,
            status: ProvenanceStatusPolicy::EvidenceDerivedOnly,
            baseline_artifact_sha256: BASELINE_ARTIFACT_SHA256.to_owned(),
            candidate_artifact_sha256: CANDIDATE_ARTIFACT_SHA256.to_owned(),
            baseline_legacy_primary_sha256: BASELINE_LEGACY_PRIMARY_SHA256.to_owned(),
            baseline_legacy_secondary_sha256: BASELINE_LEGACY_SECONDARY_SHA256.to_owned(),
            baseline_legacy_environment_sha256: Some(BASELINE_LEGACY_ENVIRONMENT_SHA256.to_owned()),
            baseline_legacy_preregistration_sha256: Some(
                BASELINE_LEGACY_PREREGISTRATION_SHA256.to_owned(),
            ),
            baseline_legacy_build_result_sha256: Some(
                BASELINE_LEGACY_BUILD_RESULT_SHA256.to_owned(),
            ),
            candidate_legacy_primary_sha256: CANDIDATE_LEGACY_PRIMARY_SHA256.to_owned(),
            candidate_legacy_secondary_sha256: CANDIDATE_LEGACY_SECONDARY_SHA256.to_owned(),
            baseline_source_bundle_sha256: None,
            candidate_source_bundle_sha256: CANDIDATE_SOURCE_BUNDLE_SHA256.to_owned(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HarnessContract {
    pub schema: String,
    pub test_name: String,
    pub operations_per_run: u64,
    pub response_frame_bytes: u64,
    pub response_blake3: String,
    pub threshold_numerator: u64,
    pub threshold_denominator: u64,
    pub child_argv_suffix: [String; 4],
    pub child_environment_policy: ChildEnvironmentPolicy,
    pub run_id_environment_variable: String,
    pub max_child_duration_seconds: u64,
    pub max_child_stdout_bytes: u64,
    pub max_child_stderr_bytes: u64,
    pub assignments: [RunAssignment; RUN_COUNT],
}

impl HarnessContract {
    #[must_use]
    pub fn fixed() -> Self {
        Self {
            schema: HARNESS_SCHEMA.to_string(),
            test_name: BENCHMARK_TEST_NAME.to_string(),
            operations_per_run: OPERATIONS_PER_RUN,
            response_frame_bytes: RESPONSE_FRAME_BYTES,
            response_blake3: RESPONSE_BLAKE3.to_string(),
            threshold_numerator: THRESHOLD_NUMERATOR,
            threshold_denominator: THRESHOLD_DENOMINATOR,
            child_argv_suffix: [
                BENCHMARK_TEST_NAME.to_string(),
                "--ignored".to_string(),
                "--exact".to_string(),
                "--nocapture".to_string(),
            ],
            child_environment_policy: ChildEnvironmentPolicy::ClearAndSetFixedWithRunId,
            run_id_environment_variable: RUN_ID_ENVIRONMENT_VARIABLE.to_string(),
            max_child_duration_seconds: MAX_CHILD_DURATION_SECONDS,
            max_child_stdout_bytes: MAX_CHILD_STDOUT_BYTES,
            max_child_stderr_bytes: MAX_CHILD_STDERR_BYTES,
            assignments: fixed_run_assignments(),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ChildEnvironmentPolicy {
    /// Clear the inherited environment, then set `LC_ALL=C`, `TZ=UTC`,
    /// `RUST_BACKTRACE=0`, and the assignment's sealed run ID.
    ClearAndSetFixedWithRunId,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RunAssignment {
    pub sequence: u8,
    pub run_id: String,
    pub arm: Arm,
    pub kind: RunKind,
    pub pair: Option<u8>,
}

impl RunAssignment {
    fn warmup(sequence: u8, run_id: &str, arm: Arm) -> Self {
        Self {
            sequence,
            run_id: run_id.to_string(),
            arm,
            kind: RunKind::Warmup,
            pair: None,
        }
    }

    fn measured(sequence: u8, run_id: &str, arm: Arm, pair: u8) -> Self {
        Self {
            sequence,
            run_id: run_id.to_string(),
            arm,
            kind: RunKind::Measured,
            pair: Some(pair),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Arm {
    Baseline,
    Candidate,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RunKind {
    Warmup,
    Measured,
}

#[must_use]
pub fn fixed_run_assignments() -> [RunAssignment; RUN_COUNT] {
    [
        RunAssignment::warmup(0, "warmup-B", Arm::Baseline),
        RunAssignment::warmup(1, "warmup-C", Arm::Candidate),
        RunAssignment::measured(2, "pair1-B", Arm::Baseline, 1),
        RunAssignment::measured(3, "pair1-C", Arm::Candidate, 1),
        RunAssignment::measured(4, "pair2-C", Arm::Candidate, 2),
        RunAssignment::measured(5, "pair2-B", Arm::Baseline, 2),
        RunAssignment::measured(6, "pair3-B", Arm::Baseline, 3),
        RunAssignment::measured(7, "pair3-C", Arm::Candidate, 3),
        RunAssignment::measured(8, "pair4-C", Arm::Candidate, 4),
        RunAssignment::measured(9, "pair4-B", Arm::Baseline, 4),
        RunAssignment::measured(10, "pair5-B", Arm::Baseline, 5),
        RunAssignment::measured(11, "pair5-C", Arm::Candidate, 5),
    ]
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HarnessRecord {
    pub schema: String,
    pub run_id: String,
    pub debug_assertions: bool,
    pub operations: u64,
    pub elapsed_ns: u64,
    pub ns_per_op: u64,
    pub per_stream_ns: Vec<u64>,
    pub response_frame_bytes: u64,
    pub response_blake3: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ChildReceipt {
    pub schema: String,
    pub status_available: bool,
    pub exit_code: i32,
    pub signal: Option<i32>,
    pub termination: ChildTerminationReceipt,
    pub stdout_bytes: u64,
    pub stderr_bytes: u64,
    pub stdout_observed_bytes: u64,
    pub stderr_observed_bytes: u64,
    pub cleanup_issues: Vec<ChildCleanupIssueReceipt>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ChildTerminationReceipt {
    Exited,
    ManagedSignal {
        signal: i32,
        escalated: bool,
    },
    RuntimeLimit {
        seconds: u64,
    },
    OutputLimit {
        stdout_limit: u64,
        stderr_limit: u64,
    },
    DrainFailure,
    WaitFailure,
    OutputSetupFailure,
    StartedCallbackFailure,
    StatusUnavailable,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ChildCleanupIssueReceipt {
    Setup {
        stage: String,
        error_kind: String,
        message: String,
    },
    Wait {
        operation: String,
        error_kind: String,
        message: String,
    },
    Drain {
        stream: String,
        error_kind: String,
        message: String,
    },
    DrainPanic {
        stream: String,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RunRecord {
    pub schema: String,
    pub assignment: RunAssignment,
    pub executable_sha256: String,
    pub child: ChildReceipt,
    pub harness_records: Vec<HarnessRecord>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Event {
    pub schema: String,
    pub sequence: u64,
    pub phase: EventPhase,
    pub run_id: Option<String>,
    pub pid: Option<u32>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum EventPhase {
    Starting,
    Started,
    Finished,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AuditRecord {
    pub schema: String,
    pub status: TerminalStatus,
    pub baseline_ns_per_op: [u64; MEASURED_RUNS_PER_ARM],
    pub candidate_ns_per_op: [u64; MEASURED_RUNS_PER_ARM],
    pub baseline_median_ns_per_op: u64,
    pub candidate_median_ns_per_op: u64,
    pub primary_ratio_numerator: u64,
    pub primary_ratio_denominator: u64,
    pub threshold_numerator: u64,
    pub threshold_denominator: u64,
    pub paired_deltas: [PairedDelta; MEASURED_RUNS_PER_ARM],
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TerminalReceipt {
    pub schema: String,
    pub status: TerminalStatus,
    pub stage: String,
    pub detail: String,
    pub manifest_sha256: String,
    /// Number of fully authenticated run records present, in `0..=RUN_COUNT`.
    pub completed_runs: u8,
    pub audit: Option<AuditRecord>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PairedDelta {
    pub pair: u8,
    pub baseline_ns_per_op: u64,
    pub candidate_ns_per_op: u64,
    pub candidate_minus_baseline_ns_per_op: i128,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TerminalStatus {
    Ready,
    Blocked,
    Pass,
    Fail,
    Invalid,
}

#[cfg(test)]
mod tests {
    use super::{
        fixed_run_assignments, Arm, ArtifactOrigin, ChildEnvironmentPolicy, Event, EventPhase,
        HarnessContract, ProvenancePolicy, ProvenanceStatusPolicy, RunKind, TerminalReceipt,
        TerminalStatus, BASELINE_ARTIFACT_SHA256, BASELINE_LEGACY_PRIMARY_SHA256,
        BENCHMARK_TEST_NAME, CANDIDATE_ARTIFACT_SHA256, CANDIDATE_LEGACY_SECONDARY_SHA256,
        EVENT_SCHEMA, HARNESS_SCHEMA, MAX_CHILD_DURATION_SECONDS, MAX_CHILD_STDERR_BYTES,
        MAX_CHILD_STDOUT_BYTES, RUN_COUNT, RUN_ID_ENVIRONMENT_VARIABLE,
    };

    #[test]
    fn fixed_contract_has_the_preregistered_counterbalanced_order() {
        let assignments = fixed_run_assignments();
        let ids: Vec<&str> = assignments
            .iter()
            .map(|assignment| assignment.run_id.as_str())
            .collect();

        assert_eq!(assignments.len(), RUN_COUNT);
        assert_eq!(
            ids,
            [
                "warmup-B", "warmup-C", "pair1-B", "pair1-C", "pair2-C", "pair2-B", "pair3-B",
                "pair3-C", "pair4-C", "pair4-B", "pair5-B", "pair5-C",
            ]
        );
        assert_eq!(assignments[0].kind, RunKind::Warmup);
        assert_eq!(assignments[1].kind, RunKind::Warmup);
        assert_eq!(assignments[0].arm, Arm::Baseline);
        assert_eq!(assignments[1].arm, Arm::Candidate);
        assert!(assignments[2..]
            .iter()
            .all(|assignment| assignment.kind == RunKind::Measured));
    }

    #[test]
    fn fixed_contract_round_trips_through_json() {
        let contract = HarnessContract::fixed();
        let encoded = serde_json::to_vec(&contract).expect("serialize fixed contract");
        let decoded: HarnessContract =
            serde_json::from_slice(&encoded).expect("deserialize fixed contract");

        assert_eq!(decoded, contract);
        assert_eq!(decoded.schema, HARNESS_SCHEMA);
        assert_eq!(
            decoded.child_argv_suffix,
            [BENCHMARK_TEST_NAME, "--ignored", "--exact", "--nocapture"]
        );
        assert_eq!(
            decoded.child_environment_policy,
            ChildEnvironmentPolicy::ClearAndSetFixedWithRunId
        );
        assert_eq!(
            decoded.run_id_environment_variable,
            RUN_ID_ENVIRONMENT_VARIABLE
        );
        assert_eq!(
            decoded.max_child_duration_seconds,
            MAX_CHILD_DURATION_SECONDS
        );
        assert_eq!(decoded.max_child_stdout_bytes, MAX_CHILD_STDOUT_BYTES);
        assert_eq!(decoded.max_child_stderr_bytes, MAX_CHILD_STDERR_BYTES);
    }

    #[test]
    fn event_phases_have_exact_stable_wire_names() {
        for (phase, expected) in [
            (EventPhase::Starting, "STARTING"),
            (EventPhase::Started, "STARTED"),
            (EventPhase::Finished, "FINISHED"),
        ] {
            let event = Event {
                schema: EVENT_SCHEMA.to_string(),
                sequence: 7,
                phase,
                run_id: Some("pair1-B".to_string()),
                pid: Some(42),
            };
            let value = serde_json::to_value(event).expect("serialize event");
            assert_eq!(value["schema"], EVENT_SCHEMA);
            assert_eq!(value["phase"], expected);
        }
    }

    #[test]
    fn acceptance_records_reject_unknown_fields() {
        let event = Event {
            schema: EVENT_SCHEMA.to_owned(),
            sequence: 7,
            phase: EventPhase::Started,
            run_id: Some("pair1-B".to_owned()),
            pid: Some(42),
        };
        let mut event = serde_json::to_value(event).unwrap();
        event["unreviewed"] = serde_json::json!(true);
        assert!(serde_json::from_value::<Event>(event).is_err());

        let terminal = TerminalReceipt {
            schema: "distd.quic.fetch.driver.terminal.v1".to_owned(),
            status: TerminalStatus::Blocked,
            stage: "preflight".to_owned(),
            detail: "fixture".to_owned(),
            manifest_sha256: "a".repeat(64),
            completed_runs: 0,
            audit: None,
        };
        let mut terminal = serde_json::to_value(terminal).unwrap();
        terminal["unreviewed"] = serde_json::json!(true);
        assert!(serde_json::from_value::<TerminalReceipt>(terminal).is_err());
    }

    #[test]
    fn recovery_policy_pins_evidence_derived_frozen_artifacts() {
        let policy = ProvenancePolicy::recovered_frozen_evidence_derived();

        assert_eq!(policy.baseline_origin, ArtifactOrigin::RecoveredFrozen);
        assert_eq!(policy.candidate_origin, ArtifactOrigin::RecoveredFrozen);
        assert_eq!(policy.status, ProvenanceStatusPolicy::EvidenceDerivedOnly);
        assert_eq!(policy.baseline_artifact_sha256, BASELINE_ARTIFACT_SHA256);
        assert_eq!(policy.candidate_artifact_sha256, CANDIDATE_ARTIFACT_SHA256);
        assert_eq!(
            policy.baseline_legacy_primary_sha256,
            BASELINE_LEGACY_PRIMARY_SHA256
        );
        assert_eq!(
            policy.candidate_legacy_secondary_sha256,
            CANDIDATE_LEGACY_SECONDARY_SHA256
        );
    }
}

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

pub const REQUEST_SCHEMA: &str = "distd.quic.fetch.driver.request.v1";
pub const PLAN_SCHEMA: &str = "distd.quic.fetch.driver.plan.v1";
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

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Request {
    pub schema: String,
    pub repository: RepositoryIdentity,
    pub baseline: ExecutableIdentity,
    pub candidate: ExecutableIdentity,
    pub harness: HarnessContract,
    pub blocked_evidence: PathBuf,
    pub evidence_root: PathBuf,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Plan {
    pub schema: String,
    pub driver: DriverIdentity,
    pub preflight: PreflightIdentity,
    pub repository: RepositoryIdentity,
    pub baseline: ExecutableIdentity,
    pub candidate: ExecutableIdentity,
    pub harness: HarnessContract,
    pub blocked_evidence: PathBuf,
    pub evidence_root: PathBuf,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DriverIdentity {
    pub path: PathBuf,
    pub sha256: String,
    pub device: u64,
    pub inode: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PreflightIdentity {
    pub environment_path: PathBuf,
    pub environment_sha256: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RepositoryIdentity {
    pub root: PathBuf,
    pub head: String,
    pub source_diff: PathBuf,
    pub source_diff_sha256: String,
    pub source_status: PathBuf,
    pub source_status_sha256: String,
    pub cargo_lock: PathBuf,
    pub cargo_lock_sha256: String,
    pub quic_source: PathBuf,
    pub quic_source_sha256: String,
    pub task_spec: PathBuf,
    pub task_spec_sha256: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ExecutableIdentity {
    pub path: PathBuf,
    pub sha256: String,
    pub build_identity: String,
    pub source_identity_path: PathBuf,
    pub source_identity_sha256: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
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
#[serde(tag = "kind", rename_all = "snake_case")]
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
#[serde(tag = "kind", rename_all = "snake_case")]
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
pub struct RunRecord {
    pub schema: String,
    pub assignment: RunAssignment,
    pub executable_sha256: String,
    pub child: ChildReceipt,
    pub harness_records: Vec<HarnessRecord>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
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
        fixed_run_assignments, Arm, ChildEnvironmentPolicy, Event, EventPhase, HarnessContract,
        RunKind, BENCHMARK_TEST_NAME, EVENT_SCHEMA, HARNESS_SCHEMA, MAX_CHILD_DURATION_SECONDS,
        MAX_CHILD_STDERR_BYTES, MAX_CHILD_STDOUT_BYTES, RUN_COUNT, RUN_ID_ENVIRONMENT_VARIABLE,
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
}

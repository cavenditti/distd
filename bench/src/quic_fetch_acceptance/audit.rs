use thiserror::Error;

use super::model::{
    fixed_run_assignments, Arm, AuditRecord, ChildTerminationReceipt, HarnessContract,
    HarnessRecord, PairedDelta, Plan, RunAssignment, RunKind, RunRecord, TerminalStatus,
    AUDIT_SCHEMA, CHILD_RECEIPT_SCHEMA, HARNESS_SCHEMA, MEASURED_RUNS_PER_ARM, OPERATIONS_PER_RUN,
    PLAN_SCHEMA, RESPONSE_BLAKE3, RESPONSE_FRAME_BYTES, RUN_COUNT, RUN_RECORD_SCHEMA,
    THRESHOLD_DENOMINATOR, THRESHOLD_NUMERATOR,
};

#[derive(Debug, Error, Eq, PartialEq)]
pub enum AuditError {
    #[error("unsupported plan schema {actual:?}")]
    PlanSchema { actual: String },
    #[error("sealed plan does not contain the exact fixed harness contract")]
    HarnessContractMismatch,
    #[error("plan {arm:?} executable hash is not lowercase SHA-256")]
    PlanExecutableHash { arm: Arm },
    #[error("expected exactly {expected} run records, found {actual}")]
    RunCount { expected: usize, actual: usize },
    #[error("run {index} assignment differs from the fixed plan")]
    AssignmentMismatch {
        index: usize,
        expected: RunAssignment,
        actual: RunAssignment,
    },
    #[error("run {index} has unsupported record schema {actual:?}")]
    RunRecordSchema { index: usize, actual: String },
    #[error("run {index} executable hash is not lowercase SHA-256")]
    ExecutableHash { index: usize },
    #[error("run {index} executable hash does not match its sealed {arm:?} artifact")]
    ExecutableHashMismatch { index: usize, arm: Arm },
    #[error("run {index} has unsupported child receipt schema {actual:?}")]
    ChildReceiptSchema { index: usize, actual: String },
    #[error("run {index} child did not exit normally with status zero")]
    ChildFailed {
        index: usize,
        exit_code: i32,
        signal: Option<i32>,
    },
    #[error("run {index} contains {actual} parsed harness records instead of exactly one")]
    HarnessRecordCount { index: usize, actual: usize },
    #[error("run {index} has unsupported harness schema {actual:?}")]
    HarnessSchema { index: usize, actual: String },
    #[error("run {index} harness run ID {actual:?} does not match {expected:?}")]
    HarnessRunId {
        index: usize,
        expected: String,
        actual: String,
    },
    #[error("run {index} was built with debug assertions")]
    DebugAssertions { index: usize },
    #[error("run {index} reported {actual} operations instead of {expected}")]
    Operations {
        index: usize,
        expected: u64,
        actual: u64,
    },
    #[error("run {index} contains {actual} samples instead of {expected}")]
    SampleCount {
        index: usize,
        expected: usize,
        actual: usize,
    },
    #[error("run {index} sample {sample_index} is zero")]
    ZeroSample { index: usize, sample_index: usize },
    #[error("run {index} reported zero elapsed time")]
    ZeroElapsed { index: usize },
    #[error("run {index} ns/op does not equal floor(elapsed/operations)")]
    NsPerOp {
        index: usize,
        expected: u64,
        actual: u64,
    },
    #[error("run {index} sample sum overflowed")]
    SampleSumOverflow { index: usize },
    #[error("run {index} sample sum exceeds elapsed time")]
    SampleSumExceedsElapsed {
        index: usize,
        sample_sum: u128,
        elapsed_ns: u64,
    },
    #[error("run {index} response has {actual} bytes instead of {expected}")]
    ResponseFrameBytes {
        index: usize,
        expected: u64,
        actual: u64,
    },
    #[error("run {index} response BLAKE3 does not match the fixed fixture")]
    ResponseHash { index: usize },
    #[error("fixed assignments did not yield five measured runs for each arm")]
    MeasuredRunCount,
    #[error("fixed assignments did not yield exactly one record for pair {pair} and arm {arm:?}")]
    PairRecord { pair: u8, arm: Arm },
    #[error("primary ratio comparison overflowed")]
    RatioOverflow,
}

/// Validates already-derived harness records and computes the preregistered metrics.
///
/// This does not authenticate evidence files or prove the absence of extra child invocations. The
/// execution verifier must derive `runs` from sealed append-only evidence and validate its event
/// sequence before calling this metric validator.
pub fn audit_run_metrics(plan: &Plan, runs: &[RunRecord]) -> Result<AuditRecord, AuditError> {
    validate_plan(plan)?;
    if runs.len() != RUN_COUNT {
        return Err(AuditError::RunCount {
            expected: RUN_COUNT,
            actual: runs.len(),
        });
    }

    let assignments = fixed_run_assignments();
    let mut baseline = Vec::with_capacity(MEASURED_RUNS_PER_ARM);
    let mut candidate = Vec::with_capacity(MEASURED_RUNS_PER_ARM);
    let mut baseline_by_pair = [None; MEASURED_RUNS_PER_ARM];
    let mut candidate_by_pair = [None; MEASURED_RUNS_PER_ARM];

    for (index, (run, expected_assignment)) in runs.iter().zip(assignments.iter()).enumerate() {
        if run.assignment != *expected_assignment {
            return Err(AuditError::AssignmentMismatch {
                index,
                expected: expected_assignment.clone(),
                actual: run.assignment.clone(),
            });
        }
        validate_run(plan, index, run)?;

        if run.assignment.kind == RunKind::Measured {
            let value = run.harness_records[0].ns_per_op;
            let pair = run.assignment.pair.ok_or(AuditError::MeasuredRunCount)?;
            let pair_index = usize::from(pair.checked_sub(1).ok_or(AuditError::MeasuredRunCount)?);
            match run.assignment.arm {
                Arm::Baseline => {
                    baseline.push(value);
                    baseline_by_pair[pair_index] = Some(value);
                }
                Arm::Candidate => {
                    candidate.push(value);
                    candidate_by_pair[pair_index] = Some(value);
                }
            }
        }
    }

    let baseline: [u64; MEASURED_RUNS_PER_ARM] = baseline
        .try_into()
        .map_err(|_| AuditError::MeasuredRunCount)?;
    let candidate: [u64; MEASURED_RUNS_PER_ARM] = candidate
        .try_into()
        .map_err(|_| AuditError::MeasuredRunCount)?;

    let baseline_median = median(baseline);
    let candidate_median = median(candidate);
    let candidate_scaled = u128::from(candidate_median)
        .checked_mul(u128::from(THRESHOLD_DENOMINATOR))
        .ok_or(AuditError::RatioOverflow)?;
    let baseline_scaled = u128::from(baseline_median)
        .checked_mul(u128::from(THRESHOLD_NUMERATOR))
        .ok_or(AuditError::RatioOverflow)?;
    let status = if candidate_scaled <= baseline_scaled {
        TerminalStatus::Pass
    } else {
        TerminalStatus::Fail
    };

    let mut paired_deltas = Vec::with_capacity(MEASURED_RUNS_PER_ARM);
    for pair_index in 0..MEASURED_RUNS_PER_ARM {
        let pair = u8::try_from(pair_index + 1).map_err(|_| AuditError::MeasuredRunCount)?;
        let baseline_value = baseline_by_pair[pair_index].ok_or(AuditError::PairRecord {
            pair,
            arm: Arm::Baseline,
        })?;
        let candidate_value = candidate_by_pair[pair_index].ok_or(AuditError::PairRecord {
            pair,
            arm: Arm::Candidate,
        })?;
        paired_deltas.push(PairedDelta {
            pair,
            baseline_ns_per_op: baseline_value,
            candidate_ns_per_op: candidate_value,
            candidate_minus_baseline_ns_per_op: i128::from(candidate_value)
                - i128::from(baseline_value),
        });
    }
    let paired_deltas: [PairedDelta; MEASURED_RUNS_PER_ARM] = paired_deltas
        .try_into()
        .map_err(|_| AuditError::MeasuredRunCount)?;

    Ok(AuditRecord {
        schema: AUDIT_SCHEMA.to_string(),
        status,
        baseline_ns_per_op: baseline,
        candidate_ns_per_op: candidate,
        baseline_median_ns_per_op: baseline_median,
        candidate_median_ns_per_op: candidate_median,
        primary_ratio_numerator: candidate_median,
        primary_ratio_denominator: baseline_median,
        threshold_numerator: THRESHOLD_NUMERATOR,
        threshold_denominator: THRESHOLD_DENOMINATOR,
        paired_deltas,
    })
}

/// Semantically validates an authenticated prefix of the fixed run sequence.
///
/// Unlike [`audit_run_metrics`], this does not require all twelve assignments or compute a
/// performance verdict. It is used to authenticate partial `INVALID` evidence.
pub fn validate_run_prefix(plan: &Plan, runs: &[RunRecord]) -> Result<(), AuditError> {
    validate_plan(plan)?;
    if runs.len() > RUN_COUNT {
        return Err(AuditError::RunCount {
            expected: RUN_COUNT,
            actual: runs.len(),
        });
    }
    let assignments = fixed_run_assignments();
    for (index, (run, expected_assignment)) in runs.iter().zip(assignments.iter()).enumerate() {
        if run.assignment != *expected_assignment {
            return Err(AuditError::AssignmentMismatch {
                index,
                expected: expected_assignment.clone(),
                actual: run.assignment.clone(),
            });
        }
        validate_run(plan, index, run)?;
    }
    Ok(())
}

fn validate_plan(plan: &Plan) -> Result<(), AuditError> {
    if plan.schema != PLAN_SCHEMA {
        return Err(AuditError::PlanSchema {
            actual: plan.schema.clone(),
        });
    }
    if plan.harness != HarnessContract::fixed() {
        return Err(AuditError::HarnessContractMismatch);
    }
    for (arm, sha256) in [
        (Arm::Baseline, plan.baseline.sha256.as_str()),
        (Arm::Candidate, plan.candidate.sha256.as_str()),
    ] {
        if !is_lowercase_sha256(sha256) {
            return Err(AuditError::PlanExecutableHash { arm });
        }
    }
    Ok(())
}

fn validate_run(plan: &Plan, index: usize, run: &RunRecord) -> Result<(), AuditError> {
    if run.schema != RUN_RECORD_SCHEMA {
        return Err(AuditError::RunRecordSchema {
            index,
            actual: run.schema.clone(),
        });
    }
    if !is_lowercase_sha256(&run.executable_sha256) {
        return Err(AuditError::ExecutableHash { index });
    }
    let expected_executable_hash = match run.assignment.arm {
        Arm::Baseline => &plan.baseline.sha256,
        Arm::Candidate => &plan.candidate.sha256,
    };
    if run.executable_sha256 != *expected_executable_hash {
        return Err(AuditError::ExecutableHashMismatch {
            index,
            arm: run.assignment.arm,
        });
    }
    if run.child.schema != CHILD_RECEIPT_SCHEMA {
        return Err(AuditError::ChildReceiptSchema {
            index,
            actual: run.child.schema.clone(),
        });
    }
    if !run.child.status_available
        || run.child.exit_code != 0
        || run.child.signal.is_some()
        || run.child.termination != ChildTerminationReceipt::Exited
        || !run.child.cleanup_issues.is_empty()
    {
        return Err(AuditError::ChildFailed {
            index,
            exit_code: run.child.exit_code,
            signal: run.child.signal,
        });
    }
    if run.harness_records.len() != 1 {
        return Err(AuditError::HarnessRecordCount {
            index,
            actual: run.harness_records.len(),
        });
    }
    validate_harness(index, &run.assignment, &run.harness_records[0])
}

fn validate_harness(
    index: usize,
    assignment: &RunAssignment,
    harness: &HarnessRecord,
) -> Result<(), AuditError> {
    if harness.schema != HARNESS_SCHEMA {
        return Err(AuditError::HarnessSchema {
            index,
            actual: harness.schema.clone(),
        });
    }
    if harness.run_id != assignment.run_id {
        return Err(AuditError::HarnessRunId {
            index,
            expected: assignment.run_id.clone(),
            actual: harness.run_id.clone(),
        });
    }
    if harness.debug_assertions {
        return Err(AuditError::DebugAssertions { index });
    }
    if harness.operations != OPERATIONS_PER_RUN {
        return Err(AuditError::Operations {
            index,
            expected: OPERATIONS_PER_RUN,
            actual: harness.operations,
        });
    }
    let expected_samples = usize::try_from(OPERATIONS_PER_RUN).expect("operation count fits usize");
    if harness.per_stream_ns.len() != expected_samples {
        return Err(AuditError::SampleCount {
            index,
            expected: expected_samples,
            actual: harness.per_stream_ns.len(),
        });
    }
    if let Some(sample_index) = harness.per_stream_ns.iter().position(|sample| *sample == 0) {
        return Err(AuditError::ZeroSample {
            index,
            sample_index,
        });
    }
    if harness.elapsed_ns == 0 {
        return Err(AuditError::ZeroElapsed { index });
    }
    let expected_ns_per_op = harness.elapsed_ns / OPERATIONS_PER_RUN;
    if harness.ns_per_op != expected_ns_per_op {
        return Err(AuditError::NsPerOp {
            index,
            expected: expected_ns_per_op,
            actual: harness.ns_per_op,
        });
    }
    let sample_sum = harness
        .per_stream_ns
        .iter()
        .try_fold(0_u128, |sum, sample| sum.checked_add(u128::from(*sample)));
    let sample_sum = sample_sum.ok_or(AuditError::SampleSumOverflow { index })?;
    if sample_sum > u128::from(harness.elapsed_ns) {
        return Err(AuditError::SampleSumExceedsElapsed {
            index,
            sample_sum,
            elapsed_ns: harness.elapsed_ns,
        });
    }
    if harness.response_frame_bytes != RESPONSE_FRAME_BYTES {
        return Err(AuditError::ResponseFrameBytes {
            index,
            expected: RESPONSE_FRAME_BYTES,
            actual: harness.response_frame_bytes,
        });
    }
    if harness.response_blake3 != RESPONSE_BLAKE3 {
        return Err(AuditError::ResponseHash { index });
    }
    Ok(())
}

fn median<const N: usize>(mut values: [u64; N]) -> u64 {
    values.sort_unstable();
    values[N / 2]
}

fn is_lowercase_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::{audit_run_metrics as audit_runs_with_plan, AuditError};
    use crate::quic_fetch_acceptance::model::{
        fixed_run_assignments, Arm, AuditRecord, ChildReceipt, ChildTerminationReceipt,
        DriverIdentity, ExecutableIdentity, HarnessContract, HarnessRecord, Plan,
        PreflightIdentity, RepositoryIdentity, RunKind, RunRecord, TerminalStatus,
        CHILD_RECEIPT_SCHEMA, HARNESS_SCHEMA, OPERATIONS_PER_RUN, PLAN_SCHEMA, RESPONSE_BLAKE3,
        RESPONSE_FRAME_BYTES, RUN_RECORD_SCHEMA,
    };

    fn valid_plan() -> Plan {
        Plan {
            schema: PLAN_SCHEMA.to_string(),
            driver: DriverIdentity {
                path: PathBuf::from("/driver"),
                sha256: "d".repeat(64),
                device: 1,
                inode: 2,
            },
            preflight: PreflightIdentity {
                environment_path: PathBuf::from("/preflight/environment.json"),
                environment_sha256: "e".repeat(64),
            },
            repository: RepositoryIdentity {
                root: PathBuf::from("/repo"),
                head: "head".to_string(),
                source_diff: PathBuf::from("/frozen/source-diff.patch"),
                source_diff_sha256: "f".repeat(64),
                source_status: PathBuf::from("/frozen/source-status.txt"),
                source_status_sha256: "f".repeat(64),
                cargo_lock: PathBuf::from("/frozen/Cargo.lock"),
                cargo_lock_sha256: "f".repeat(64),
                quic_source: PathBuf::from("/frozen/quic.rs"),
                quic_source_sha256: "f".repeat(64),
                task_spec: PathBuf::from("/frozen/task.spec.md"),
                task_spec_sha256: "f".repeat(64),
            },
            baseline: ExecutableIdentity {
                path: PathBuf::from("/bin/baseline"),
                sha256: "a".repeat(64),
                build_identity: "baseline-build".to_string(),
                source_identity_path: PathBuf::from("/frozen/baseline-source.txt"),
                source_identity_sha256: "1".repeat(64),
            },
            candidate: ExecutableIdentity {
                path: PathBuf::from("/bin/candidate"),
                sha256: "b".repeat(64),
                build_identity: "candidate-build".to_string(),
                source_identity_path: PathBuf::from("/frozen/candidate-source.txt"),
                source_identity_sha256: "2".repeat(64),
            },
            harness: HarnessContract::fixed(),
            blocked_evidence: PathBuf::from("/blocked.json"),
            evidence_root: PathBuf::from("/evidence"),
        }
    }

    fn audit_runs(runs: &[RunRecord]) -> Result<AuditRecord, AuditError> {
        audit_runs_with_plan(&valid_plan(), runs)
    }

    fn valid_runs(baseline: [u64; 5], candidate: [u64; 5]) -> Vec<RunRecord> {
        fixed_run_assignments()
            .into_iter()
            .map(|assignment| {
                let ns_per_op = match (assignment.kind, assignment.arm, assignment.pair) {
                    (RunKind::Warmup, _, _) => 100,
                    (RunKind::Measured, Arm::Baseline, Some(pair)) => {
                        baseline[usize::from(pair - 1)]
                    }
                    (RunKind::Measured, Arm::Candidate, Some(pair)) => {
                        candidate[usize::from(pair - 1)]
                    }
                    _ => unreachable!("fixed assignment shape"),
                };
                let elapsed_ns = ns_per_op * OPERATIONS_PER_RUN;
                let harness = HarnessRecord {
                    schema: HARNESS_SCHEMA.to_string(),
                    run_id: assignment.run_id.clone(),
                    debug_assertions: false,
                    operations: OPERATIONS_PER_RUN,
                    elapsed_ns,
                    ns_per_op,
                    per_stream_ns: vec![
                        ns_per_op;
                        usize::try_from(OPERATIONS_PER_RUN)
                            .expect("operation count fits usize")
                    ],
                    response_frame_bytes: RESPONSE_FRAME_BYTES,
                    response_blake3: RESPONSE_BLAKE3.to_string(),
                };
                let executable_sha256 = match assignment.arm {
                    Arm::Baseline => "a".repeat(64),
                    Arm::Candidate => "b".repeat(64),
                };
                RunRecord {
                    schema: RUN_RECORD_SCHEMA.to_string(),
                    assignment,
                    executable_sha256,
                    child: ChildReceipt {
                        schema: CHILD_RECEIPT_SCHEMA.to_string(),
                        status_available: true,
                        exit_code: 0,
                        signal: None,
                        termination: ChildTerminationReceipt::Exited,
                        stdout_bytes: 0,
                        stderr_bytes: 0,
                        stdout_observed_bytes: 0,
                        stderr_observed_bytes: 0,
                        cleanup_issues: Vec::new(),
                    },
                    harness_records: vec![harness],
                }
            })
            .collect()
    }

    #[test]
    fn threshold_boundary_passes_and_warmups_are_excluded() {
        let mut runs = valid_runs([100; 5], [105; 5]);
        runs[0].harness_records[0].ns_per_op = 99_999;
        runs[0].harness_records[0].elapsed_ns = 99_999 * OPERATIONS_PER_RUN;
        runs[0].harness_records[0].per_stream_ns =
            vec![99_999; usize::try_from(OPERATIONS_PER_RUN).expect("operation count fits usize")];
        runs[1].harness_records[0].ns_per_op = 1;
        runs[1].harness_records[0].elapsed_ns = OPERATIONS_PER_RUN;
        runs[1].harness_records[0].per_stream_ns =
            vec![1; usize::try_from(OPERATIONS_PER_RUN).expect("operation count fits usize")];

        let audit = audit_runs(&runs).expect("threshold boundary is valid");

        assert_eq!(audit.status, TerminalStatus::Pass);
        assert_eq!(audit.baseline_median_ns_per_op, 100);
        assert_eq!(audit.candidate_median_ns_per_op, 105);
        assert_eq!(audit.primary_ratio_numerator, 105);
        assert_eq!(audit.primary_ratio_denominator, 100);
    }

    #[test]
    fn one_unit_above_threshold_is_a_valid_fail() {
        let audit = audit_runs(&valid_runs([100; 5], [106; 5]))
            .expect("threshold failure remains a valid audit");

        assert_eq!(audit.status, TerminalStatus::Fail);
        assert_eq!(audit.paired_deltas[0].candidate_minus_baseline_ns_per_op, 6);
    }

    #[test]
    fn medians_use_sorted_index_two_and_deltas_remain_chronological() {
        let audit = audit_runs(&valid_runs(
            [500, 100, 300, 200, 400],
            [505, 105, 305, 205, 405],
        ))
        .expect("valid counterbalanced records");

        assert_eq!(audit.baseline_median_ns_per_op, 300);
        assert_eq!(audit.candidate_median_ns_per_op, 305);
        assert_eq!(
            audit
                .paired_deltas
                .map(|pair| pair.candidate_minus_baseline_ns_per_op),
            [5; 5]
        );
    }

    #[test]
    fn rejects_missing_and_extra_runs() {
        let mut missing = valid_runs([100; 5], [100; 5]);
        missing.pop();
        assert!(matches!(
            audit_runs(&missing),
            Err(AuditError::RunCount { actual: 11, .. })
        ));

        let mut extra = valid_runs([100; 5], [100; 5]);
        extra.push(extra[0].clone());
        assert!(matches!(
            audit_runs(&extra),
            Err(AuditError::RunCount { actual: 13, .. })
        ));
    }

    #[test]
    fn rejects_duplicate_or_misassigned_runs() {
        let mut duplicate = valid_runs([100; 5], [100; 5]);
        duplicate[1].assignment = duplicate[0].assignment.clone();
        assert!(matches!(
            audit_runs(&duplicate),
            Err(AuditError::AssignmentMismatch { index: 1, .. })
        ));

        let mut wrong_id = valid_runs([100; 5], [100; 5]);
        wrong_id[3].harness_records[0].run_id = "pair1-B".to_string();
        assert!(matches!(
            audit_runs(&wrong_id),
            Err(AuditError::HarnessRunId { index: 3, .. })
        ));
    }

    #[test]
    fn requires_exactly_one_parsed_harness_record() {
        let mut missing = valid_runs([100; 5], [100; 5]);
        missing[0].harness_records.clear();
        assert!(matches!(
            audit_runs(&missing),
            Err(AuditError::HarnessRecordCount {
                index: 0,
                actual: 0
            })
        ));

        let mut duplicate = valid_runs([100; 5], [100; 5]);
        let repeated = duplicate[0].harness_records[0].clone();
        duplicate[0].harness_records.push(repeated);
        assert!(matches!(
            audit_runs(&duplicate),
            Err(AuditError::HarnessRecordCount {
                index: 0,
                actual: 2
            })
        ));
    }

    #[test]
    fn rejects_4999_and_5001_samples() {
        for (sample_count, expected_actual) in [(4_999, 4_999), (5_001, 5_001)] {
            let mut runs = valid_runs([100; 5], [100; 5]);
            runs[2].harness_records[0].per_stream_ns = vec![100; sample_count];
            assert!(matches!(
                audit_runs(&runs),
                Err(AuditError::SampleCount {
                    index: 2,
                    actual,
                    ..
                }) if actual == expected_actual
            ));
        }
    }

    #[test]
    fn rejects_zero_samples() {
        let mut runs = valid_runs([100; 5], [100; 5]);
        runs[2].harness_records[0].per_stream_ns[4_321] = 0;

        assert!(matches!(
            audit_runs(&runs),
            Err(AuditError::ZeroSample {
                index: 2,
                sample_index: 4_321
            })
        ));
    }

    #[test]
    fn rejects_malformed_harness_fields() {
        let mut debug = valid_runs([100; 5], [100; 5]);
        debug[0].harness_records[0].debug_assertions = true;
        assert!(matches!(
            audit_runs(&debug),
            Err(AuditError::DebugAssertions { index: 0 })
        ));

        let mut operations = valid_runs([100; 5], [100; 5]);
        operations[0].harness_records[0].operations = 4_999;
        assert!(matches!(
            audit_runs(&operations),
            Err(AuditError::Operations { index: 0, .. })
        ));

        let mut ns_per_op = valid_runs([100; 5], [100; 5]);
        ns_per_op[0].harness_records[0].ns_per_op = 101;
        assert!(matches!(
            audit_runs(&ns_per_op),
            Err(AuditError::NsPerOp { index: 0, .. })
        ));

        let mut response_bytes = valid_runs([100; 5], [100; 5]);
        response_bytes[0].harness_records[0].response_frame_bytes = 216;
        assert!(matches!(
            audit_runs(&response_bytes),
            Err(AuditError::ResponseFrameBytes { index: 0, .. })
        ));

        let mut response_hash = valid_runs([100; 5], [100; 5]);
        response_hash[0].harness_records[0].response_blake3 = "0".repeat(64);
        assert!(matches!(
            audit_runs(&response_hash),
            Err(AuditError::ResponseHash { index: 0 })
        ));
    }

    #[test]
    fn rejects_failed_child_and_noncanonical_executable_hash() {
        let mut child = valid_runs([100; 5], [100; 5]);
        child[7].child.exit_code = 17;
        assert!(matches!(
            audit_runs(&child),
            Err(AuditError::ChildFailed {
                index: 7,
                exit_code: 17,
                ..
            })
        ));

        let mut hash = valid_runs([100; 5], [100; 5]);
        hash[0].executable_sha256 = "A".repeat(64);
        assert!(matches!(
            audit_runs(&hash),
            Err(AuditError::ExecutableHash { index: 0 })
        ));
    }

    #[test]
    fn rejects_nonfixed_plan_and_wrong_arm_executable() {
        let runs = valid_runs([100; 5], [100; 5]);
        let mut wrong_contract = valid_plan();
        wrong_contract.harness.threshold_numerator = 106;
        assert!(matches!(
            audit_runs_with_plan(&wrong_contract, &runs),
            Err(AuditError::HarnessContractMismatch)
        ));

        let mut wrong_artifact = valid_runs([100; 5], [100; 5]);
        wrong_artifact[1].executable_sha256 = "a".repeat(64);
        assert!(matches!(
            audit_runs_with_plan(&valid_plan(), &wrong_artifact),
            Err(AuditError::ExecutableHashMismatch {
                index: 1,
                arm: Arm::Candidate
            })
        ));
    }

    #[test]
    fn rejects_sample_sum_larger_than_elapsed() {
        let mut runs = valid_runs([100; 5], [100; 5]);
        runs[0].harness_records[0].per_stream_ns[0] = 101;

        assert!(matches!(
            audit_runs(&runs),
            Err(AuditError::SampleSumExceedsElapsed { index: 0, .. })
        ));
    }
}

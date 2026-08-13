#![cfg(unix)]

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io;
use std::os::unix::fs::MetadataExt;
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::audit::{audit_run_metrics, validate_run_prefix, AuditError};
use super::environment::{
    check_loopback, check_power, run_environment_preflight, EnvironmentBlocked,
    EnvironmentExpectations, EnvironmentPass, SystemEnvironmentProbe,
};
use super::evidence::{
    canonical_json_line, open_stable_file_bounded, pin_absent_root, sha256_bytes,
    stable_file_identity_bounded, EvidenceError, ManifestEntry, RootHandle,
};
use super::model::{
    Arm, ArtifactInput, AuditRecord, ChildCleanupIssueReceipt, ChildReceipt,
    ChildTerminationReceipt, Event, EventPhase, HarnessContract, HarnessRecord, Plan,
    RunAssignment, RunRecord, TerminalReceipt, TerminalStatus, CHILD_RECEIPT_SCHEMA, EVENT_SCHEMA,
    PLAN_SCHEMA, RUN_COUNT, RUN_RECORD_SCHEMA,
};
use super::prepare::verify_live_repository;
use super::process::{
    run_child_with_output_files, ChildCleanupIssue, ChildTermination, ExecutionSignalGuard,
    ProcessError, ProcessLimits,
};
use super::provenance::{self, ProvenanceValidationPolicy, MAX_PROVENANCE_INPUT_BYTES};

const HARNESS_PREFIX: &str = "DISTD_QUIC_FETCH_BENCH ";
const TERMINAL_SCHEMA: &str = "distd.quic.fetch.driver.terminal.v1";
#[derive(Debug, Error)]
pub enum ExecuteError {
    #[error(transparent)]
    Evidence(#[from] EvidenceError),
    #[error(transparent)]
    Process(#[from] ProcessError),
    #[error(transparent)]
    Audit(#[from] AuditError),
    #[error("I/O failed for {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("invalid sealed plan: {0}")]
    Plan(String),
    #[error("execution preflight is blocked: {0}")]
    Blocked(String),
    #[error("evidence root already exists or cannot be exclusively created: {0}")]
    RootCreate(PathBuf),
    #[error("run {run_id} child failed: exit={exit_code}, signal={signal:?}")]
    ChildFailed {
        run_id: String,
        exit_code: i32,
        signal: Option<i32>,
    },
    #[error("run {run_id} output is invalid: {detail}")]
    HarnessOutput { run_id: String, detail: String },
    #[error("execution was interrupted by managed signal {signal}")]
    Interrupted { signal: i32 },
    #[error("evidence verification failed: {0}")]
    Verification(String),
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ExecutionPreflight {
    pub(crate) schema: String,
    pub(crate) prepared_environment_sha256: String,
    pub(crate) current: EnvironmentPass,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct OnceEvidence {
    schema: String,
    plan_sha256: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct StartingEvidence {
    schema: String,
    assignment: RunAssignment,
    executable_sha256: String,
    executable_path: PathBuf,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct InputBlockedReceipt {
    schema: String,
    status: TerminalStatus,
    stage: String,
    detail: String,
    product_invocations: u8,
}

pub(crate) trait ExecutionDependencies {
    fn verify_plan_inputs(&self, plan: &Plan) -> Result<(), ExecuteError>;

    fn rerun_environment_preflight(
        &self,
        plan: &Plan,
    ) -> Result<ExecutionPreflight, EnvironmentBlocked>;
}

struct SystemExecutionDependencies;

impl ExecutionDependencies for SystemExecutionDependencies {
    fn verify_plan_inputs(&self, plan: &Plan) -> Result<(), ExecuteError> {
        verify_plan_inputs(plan)
    }

    fn rerun_environment_preflight(
        &self,
        plan: &Plan,
    ) -> Result<ExecutionPreflight, EnvironmentBlocked> {
        rerun_environment_preflight(plan)
    }
}

pub fn execute(plan_path: &Path) -> Result<TerminalStatus, ExecuteError> {
    execute_with_dependencies_and_policy(
        plan_path,
        &SystemExecutionDependencies,
        ProvenanceValidationPolicy::production(),
    )
}

pub(crate) fn execute_with_dependencies_and_policy(
    plan_path: &Path,
    dependencies: &dyn ExecutionDependencies,
    validation_policy: ProvenanceValidationPolicy<'_>,
) -> Result<TerminalStatus, ExecuteError> {
    let (_plan_root, plan, plan_bytes, plan_sha256) =
        load_sealed_plan(plan_path, validation_policy)?;
    let blocked_target = pin_absent_root(&plan.blocked_evidence)?;
    if blocked_target.absent().canonical_path != plan.blocked_evidence {
        return Err(ExecuteError::Plan(
            "blocked evidence path differs from its sealed canonical path".to_string(),
        ));
    }
    let guard = ExecutionSignalGuard::install()?;
    let mut blocked_target = Some(blocked_target);
    let result = (|| {
        if let Err(error) = dependencies.verify_plan_inputs(&plan) {
            guard.require_no_signal()?;
            let blocked = InputBlockedReceipt {
                schema: "distd.quic.fetch.execute-blocked.v1".to_string(),
                status: TerminalStatus::Blocked,
                stage: "input_preflight".to_string(),
                detail: error.to_string(),
                product_invocations: 0,
            };
            let target = blocked_target
                .take()
                .expect("blocked target is consumed by only one terminal preflight path");
            guard.commit_pinned_absent(target, &canonical_json_line(&blocked)?)?;
            return Err(ExecuteError::Blocked(blocked.detail));
        }

        let execution_preflight = match dependencies.rerun_environment_preflight(&plan) {
            Ok(pass) => pass,
            Err(blocked) => {
                guard.require_no_signal()?;
                let target = blocked_target
                    .take()
                    .expect("blocked target is consumed by only one terminal preflight path");
                guard.commit_pinned_absent(target, &canonical_json_line(&blocked)?)?;
                return Err(ExecuteError::Blocked(blocked.detail));
            }
        };
        guard.require_no_signal()?;

        let pinned = pin_absent_root(&plan.evidence_root)
            .map_err(|_| ExecuteError::RootCreate(plan.evidence_root.clone()))?;
        if pinned.absent().canonical_path != plan.evidence_root {
            return Err(ExecuteError::RootCreate(plan.evidence_root.clone()));
        }
        let root = pinned
            .create_root()
            .map_err(|_| ExecuteError::RootCreate(plan.evidence_root.clone()))?;
        let execution = execute_in_root(
            &guard,
            &root,
            &plan,
            &plan_bytes,
            &plan_sha256,
            &execution_preflight,
        );

        if let Err(error) = &execution {
            let _ = finalize(
                &guard,
                &root,
                TerminalStatus::Invalid,
                "execution",
                error.to_string(),
                None,
                completed_run_count(&root, &plan),
            );
        }
        execution
    })();
    if let Some(signal) = guard.finish()? {
        return Err(ExecuteError::Interrupted { signal });
    }
    result
}

fn execute_in_root(
    guard: &ExecutionSignalGuard,
    root: &RootHandle,
    plan: &Plan,
    plan_bytes: &[u8],
    plan_sha256: &str,
    execution_preflight: &ExecutionPreflight,
) -> Result<TerminalStatus, ExecuteError> {
    root.write_json_create_new(
        Path::new("execution-once.json"),
        &serde_json::json!({
            "schema": "distd.quic.fetch.driver.once.v1",
            "plan_sha256": plan_sha256,
        }),
    )?;
    root.atomic_create_new_synced(Path::new("plan.json"), plan_bytes)?;
    root.atomic_create_new_synced(
        Path::new("plan.sha256"),
        format!("{plan_sha256}  plan.json\n").as_bytes(),
    )?;
    root.write_json_create_new(Path::new("execution-preflight.json"), execution_preflight)?;
    root.create_dir(Path::new("events"))?;
    root.create_dir(Path::new("runs"))?;

    let mut records = Vec::with_capacity(RUN_COUNT);
    for assignment in &plan.harness.assignments {
        guard.require_no_signal()?;
        records.push(execute_one(guard, root, plan, assignment)?);
    }

    guard.require_no_signal()?;
    let audit = audit_run_metrics(plan, &records)?;
    let terminal_status = audit.status;
    root.write_json_create_new(Path::new("audit.json"), &audit)?;
    guard.require_no_signal()?;
    finalize(
        guard,
        root,
        terminal_status,
        "audit",
        "fixed twelve-run protocol and evidence audit completed".to_string(),
        Some(audit),
        RUN_COUNT,
    )?;
    if let Some(signal) = guard.post_terminal_signal() {
        return Err(ExecuteError::Interrupted { signal });
    }
    Ok(terminal_status)
}

fn finalize(
    guard: &ExecutionSignalGuard,
    root: &RootHandle,
    status: TerminalStatus,
    stage: &str,
    detail: String,
    audit: Option<AuditRecord>,
    completed_runs: usize,
) -> Result<(), ExecuteError> {
    if status != TerminalStatus::Invalid {
        guard.require_no_signal()?;
    }
    let manifest_path = Path::new("evidence-manifest.json");
    let manifest = if root.entry_is_regular_file(manifest_path)? {
        let manifest = root.verify_manifest(manifest_path)?;
        if manifest.excluded_paths != ["terminal.json".to_string()] {
            return Err(ExecuteError::Verification(
                "existing finalization manifest has wrong exclusions".to_string(),
            ));
        }
        manifest
    } else {
        root.create_manifest_with_exclusions(manifest_path, &["terminal.json"])?
    };
    let manifest_bytes = canonical_json_line(&manifest)?;
    let terminal = TerminalReceipt {
        schema: TERMINAL_SCHEMA.to_string(),
        status,
        stage: stage.to_string(),
        detail,
        manifest_sha256: sha256_bytes(&manifest_bytes),
        completed_runs: u8::try_from(completed_runs).unwrap_or(u8::MAX),
        audit,
    };
    let terminal_bytes = canonical_json_line(&terminal)?;
    if status == TerminalStatus::Invalid {
        guard.commit_invalid_root(root, Path::new("terminal.json"), &terminal_bytes)?;
    } else {
        guard.commit_root(root, Path::new("terminal.json"), &terminal_bytes)?;
    }
    Ok(())
}

fn completed_run_count(root: &RootHandle, plan: &Plan) -> usize {
    plan.harness
        .assignments
        .iter()
        .take_while(|assignment| {
            let relative = PathBuf::from("runs")
                .join(format!("{:02}-{}", assignment.sequence, assignment.run_id))
                .join("record.json");
            root.entry_is_regular_file(&relative).unwrap_or(false)
        })
        .count()
}

#[allow(clippy::too_many_lines)]
fn execute_one(
    guard: &ExecutionSignalGuard,
    root: &RootHandle,
    plan: &Plan,
    assignment: &RunAssignment,
) -> Result<RunRecord, ExecuteError> {
    let executable = match assignment.arm {
        Arm::Baseline => &plan.baseline,
        Arm::Candidate => &plan.candidate,
    };
    let directory_name = format!("{:02}-{}", assignment.sequence, assignment.run_id);
    let run_root = PathBuf::from("runs").join(directory_name);
    root.create_dir(&run_root)?;
    root.write_json_create_new(&run_root.join("assignment.json"), assignment)?;
    root.write_json_create_new(
        &run_root.join("starting.json"),
        &StartingEvidence {
            schema: "distd.quic.fetch.driver.starting.v1".to_string(),
            assignment: assignment.clone(),
            executable_sha256: executable.executable.sha256.clone(),
            executable_path: executable.executable.path.clone(),
        },
    )?;
    write_event(root, assignment, EventPhase::Starting, None)?;

    let stdout_path = run_root.join("stdout.log");
    let stderr_path = run_root.join("stderr.log");
    let (executable_identity, executable_file) =
        open_stable_file_bounded(&executable.executable.path, MAX_PROVENANCE_INPUT_BYTES)?;
    if executable_identity.sha256 != executable.executable.sha256 {
        return Err(ExecuteError::Plan(format!(
            "executable drifted immediately before spawn: {}",
            executable.executable.path.display()
        )));
    }
    let mut command = Command::new(&executable.executable.path);
    command
        .args(&plan.harness.child_argv_suffix)
        .current_dir(&plan.repository.root)
        .env_clear()
        .env("LC_ALL", "C")
        .env("TZ", "UTC")
        .env("RUST_BACKTRACE", "0")
        .env(
            &plan.harness.run_id_environment_variable,
            &assignment.run_id,
        );
    let limits = ProcessLimits {
        max_duration: Duration::from_secs(plan.harness.max_child_duration_seconds),
        max_stdout_bytes: plan.harness.max_child_stdout_bytes,
        max_stderr_bytes: plan.harness.max_child_stderr_bytes,
        signal_grace: Duration::from_secs(2),
    };
    let stdout_file = root.create_new_file(&stdout_path)?;
    let stderr_file = root.create_new_file(&stderr_path)?;
    let outcome = run_child_with_output_files(
        guard,
        &mut command,
        stdout_file,
        stderr_file,
        limits,
        |pid| {
            write_event(root, assignment, EventPhase::Started, Some(pid))
                .map_err(io::Error::other)?;
            let current = stable_file_identity_bounded(
                &executable.executable.path,
                MAX_PROVENANCE_INPUT_BYTES,
            )
            .map_err(io::Error::other)?;
            if !current.same_file(&executable_identity)
                || current.sha256 != executable.executable.sha256
            {
                return Err(io::Error::other(
                    "executable pathname changed between verification and spawn",
                ));
            }
            Ok(())
        },
    )?;
    drop(executable_file);
    write_event(root, assignment, EventPhase::Finished, Some(outcome.pid))?;

    let signal = outcome
        .forwarded_signal
        .or_else(|| outcome.status.and_then(|status| status.signal()));
    let exit_code = outcome
        .status
        .and_then(|status| status.code())
        .unwrap_or(-1);
    let child = ChildReceipt {
        schema: CHILD_RECEIPT_SCHEMA.to_string(),
        status_available: outcome.status.is_some(),
        exit_code,
        signal,
        termination: child_termination_receipt(&outcome.termination),
        stdout_bytes: outcome.stdout_bytes,
        stderr_bytes: outcome.stderr_bytes,
        stdout_observed_bytes: outcome.stdout_observed_bytes,
        stderr_observed_bytes: outcome.stderr_observed_bytes,
        cleanup_issues: outcome
            .cleanup_issues
            .iter()
            .map(child_cleanup_issue_receipt)
            .collect(),
    };
    root.write_json_create_new(&run_root.join("child.json"), &child)?;
    if let Some(signal) = outcome.forwarded_signal {
        return Err(ExecuteError::Interrupted { signal });
    }
    if !outcome.success() {
        return Err(ExecuteError::ChildFailed {
            run_id: assignment.run_id.clone(),
            exit_code,
            signal,
        });
    }

    let (_, stdout) = root.stable_file_bytes(&stdout_path)?;
    let harness_records = parse_harness_records(&stdout, &assignment.run_id)?;
    let record = RunRecord {
        schema: RUN_RECORD_SCHEMA.to_string(),
        assignment: assignment.clone(),
        executable_sha256: executable.executable.sha256.clone(),
        child,
        harness_records,
    };
    root.write_json_create_new(&run_root.join("record.json"), &record)?;
    Ok(record)
}

fn child_termination_receipt(termination: &ChildTermination) -> ChildTerminationReceipt {
    match termination {
        ChildTermination::Exited => ChildTerminationReceipt::Exited,
        ChildTermination::ManagedSignal { signal, escalated } => {
            ChildTerminationReceipt::ManagedSignal {
                signal: *signal,
                escalated: *escalated,
            }
        }
        ChildTermination::RuntimeLimit { seconds } => {
            ChildTerminationReceipt::RuntimeLimit { seconds: *seconds }
        }
        ChildTermination::OutputLimit {
            stdout_limit,
            stderr_limit,
        } => ChildTerminationReceipt::OutputLimit {
            stdout_limit: *stdout_limit,
            stderr_limit: *stderr_limit,
        },
        ChildTermination::DrainFailure => ChildTerminationReceipt::DrainFailure,
        ChildTermination::WaitFailure => ChildTerminationReceipt::WaitFailure,
        ChildTermination::OutputSetupFailure => ChildTerminationReceipt::OutputSetupFailure,
        ChildTermination::StartedCallbackFailure => ChildTerminationReceipt::StartedCallbackFailure,
        ChildTermination::StatusUnavailable => ChildTerminationReceipt::StatusUnavailable,
    }
}

fn child_cleanup_issue_receipt(issue: &ChildCleanupIssue) -> ChildCleanupIssueReceipt {
    match issue {
        ChildCleanupIssue::Setup {
            stage,
            error_kind,
            message,
        } => ChildCleanupIssueReceipt::Setup {
            stage: (*stage).to_string(),
            error_kind: format!("{error_kind:?}"),
            message: message.clone(),
        },
        ChildCleanupIssue::Wait {
            operation,
            error_kind,
            message,
        } => ChildCleanupIssueReceipt::Wait {
            operation: (*operation).to_string(),
            error_kind: format!("{error_kind:?}"),
            message: message.clone(),
        },
        ChildCleanupIssue::Drain {
            stream,
            error_kind,
            message,
        } => ChildCleanupIssueReceipt::Drain {
            stream: (*stream).to_string(),
            error_kind: format!("{error_kind:?}"),
            message: message.clone(),
        },
        ChildCleanupIssue::DrainPanic { stream } => ChildCleanupIssueReceipt::DrainPanic {
            stream: (*stream).to_string(),
        },
    }
}

fn write_event(
    root: &RootHandle,
    assignment: &RunAssignment,
    phase: EventPhase,
    pid: Option<u32>,
) -> Result<(), ExecuteError> {
    let phase_index = match phase {
        EventPhase::Starting => 0_u64,
        EventPhase::Started => 1,
        EventPhase::Finished => 2,
    };
    let sequence = u64::from(assignment.sequence) * 3 + phase_index;
    let phase_name = match phase {
        EventPhase::Starting => "starting",
        EventPhase::Started => "started",
        EventPhase::Finished => "finished",
    };
    let event = Event {
        schema: EVENT_SCHEMA.to_string(),
        sequence,
        phase,
        run_id: Some(assignment.run_id.clone()),
        pid,
    };
    root.write_json_create_new(
        &PathBuf::from("events").join(format!("{sequence:03}-{phase_name}.json")),
        &event,
    )?;
    Ok(())
}

fn parse_harness_records(bytes: &[u8], run_id: &str) -> Result<Vec<HarnessRecord>, ExecuteError> {
    let output = std::str::from_utf8(bytes).map_err(|_| ExecuteError::HarnessOutput {
        run_id: run_id.to_string(),
        detail: "stdout is not UTF-8".to_string(),
    })?;
    let records: Result<Vec<_>, _> = output
        .lines()
        .filter_map(|line| line.strip_prefix(HARNESS_PREFIX))
        .map(serde_json::from_str::<HarnessRecord>)
        .collect();
    let records = records.map_err(|error| ExecuteError::HarnessOutput {
        run_id: run_id.to_string(),
        detail: format!("benchmark JSON is malformed: {error}"),
    })?;
    if records.len() != 1 {
        return Err(ExecuteError::HarnessOutput {
            run_id: run_id.to_string(),
            detail: format!("expected one benchmark record, found {}", records.len()),
        });
    }
    Ok(records)
}

fn rerun_environment_preflight(plan: &Plan) -> Result<ExecutionPreflight, EnvironmentBlocked> {
    let prepared_bytes =
        fs::read(&plan.preflight.environment_path).map_err(|error| EnvironmentBlocked {
            schema: "distd.quic.fetch.environment-blocked.v1".to_string(),
            status: "BLOCKED".to_string(),
            stage: "execution_preflight".to_string(),
            code: super::environment::EnvironmentBlockCode::OsProbeFailed,
            detail: format!("cannot read prepared environment receipt: {error}"),
            product_invocations: 0,
        })?;
    let prepared: EnvironmentPass =
        serde_json::from_slice(&prepared_bytes).map_err(|error| EnvironmentBlocked {
            schema: "distd.quic.fetch.environment-blocked.v1".to_string(),
            status: "BLOCKED".to_string(),
            stage: "execution_preflight".to_string(),
            code: super::environment::EnvironmentBlockCode::OsProbeFailed,
            detail: format!("prepared environment receipt is malformed: {error}"),
            product_invocations: 0,
        })?;
    let expectations = EnvironmentExpectations {
        product_version: Some(prepared.os.product_version),
        kernel_identity: Some(prepared.os.kernel_identity),
        boot_record: Some(prepared.os.boot_record),
    };
    let current = run_environment_preflight(&SystemEnvironmentProbe, &expectations)?;
    Ok(ExecutionPreflight {
        schema: "distd.quic.fetch.execution-preflight.v1".to_string(),
        prepared_environment_sha256: plan.preflight.environment_sha256.clone(),
        current,
    })
}

fn load_sealed_plan(
    plan_path: &Path,
    validation_policy: ProvenanceValidationPolicy<'_>,
) -> Result<(RootHandle, Plan, Vec<u8>, String), ExecuteError> {
    if fs::symlink_metadata(plan_path)
        .map_err(|source| ExecuteError::Io {
            path: plan_path.to_path_buf(),
            source,
        })?
        .file_type()
        .is_symlink()
    {
        return Err(ExecuteError::Plan(
            "sealed plan path must not be a symlink".to_string(),
        ));
    }
    let plan_path = fs::canonicalize(plan_path).map_err(|source| ExecuteError::Io {
        path: plan_path.to_path_buf(),
        source,
    })?;
    if plan_path.file_name().and_then(|name| name.to_str()) != Some("plan.json") {
        return Err(ExecuteError::Plan(
            "sealed plan path must name plan.json".to_string(),
        ));
    }
    let plan_root_path = plan_path
        .parent()
        .ok_or_else(|| ExecuteError::Plan("plan has no parent".to_string()))?;
    let plan_root = RootHandle::open(plan_root_path)?;
    validate_plan_tree(&plan_root)?;
    let (identity, bytes) =
        plan_root.stable_file_bytes_bounded(Path::new("plan.json"), MAX_PROVENANCE_INPUT_BYTES)?;
    let plan: Plan = serde_json::from_slice(&bytes)
        .map_err(|error| ExecuteError::Plan(format!("plan JSON is malformed: {error}")))?;
    if plan.schema != PLAN_SCHEMA || plan.harness != HarnessContract::fixed() {
        return Err(ExecuteError::Plan(
            "schema or fixed harness contract mismatch".to_string(),
        ));
    }
    validate_plan_bindings(&plan, plan_root.path())?;
    validate_sealed_provenance(&plan, validation_policy)?;
    let canonical = canonical_json_line(&plan)?;
    if canonical != bytes {
        return Err(ExecuteError::Plan(
            "plan bytes are not canonical JSON".to_string(),
        ));
    }
    let hash = sha256_bytes(&bytes);
    let (_, seal_bytes) = plan_root
        .stable_file_bytes_bounded(Path::new("plan.sha256"), MAX_PROVENANCE_INPUT_BYTES)?;
    let seal = String::from_utf8(seal_bytes)
        .map_err(|_| ExecuteError::Plan("plan seal is not UTF-8".to_string()))?;
    if seal != format!("{hash}  plan.json\n") {
        return Err(ExecuteError::Plan("plan seal mismatch".to_string()));
    }
    if identity.canonical_path != plan_path {
        return Err(ExecuteError::Plan(
            "plan path differs from the pinned plan tree".to_string(),
        ));
    }
    Ok((plan_root, plan, bytes, hash))
}

fn validate_plan_bindings(plan: &Plan, plan_root: &Path) -> Result<(), ExecuteError> {
    let expected = [
        (&plan.composed_request.path, "request.json"),
        (&plan.preflight.environment_path, "environment.json"),
        (
            &plan.repository.source_diff,
            "artifacts/repository/source.snapshot",
        ),
        (
            &plan.repository.source_status,
            "artifacts/repository/source-status.txt",
        ),
        (
            &plan.repository.source_manifest,
            "artifacts/repository/source-manifest.json",
        ),
        (
            &plan.repository.cargo_lock,
            "artifacts/repository/Cargo.lock",
        ),
        (&plan.repository.quic_source, "artifacts/repository/quic.rs"),
        (
            &plan.repository.task_spec,
            "artifacts/repository/task-spec.md",
        ),
        (
            &plan.reviewed_patch.manifest.path,
            "artifacts/reviewed-patch.json",
        ),
    ];
    for (actual, relative) in expected {
        if actual != &plan_root.join(relative) {
            return Err(ExecuteError::Plan(format!(
                "sealed plan input {} does not use the prepare-owned layout",
                actual.display()
            )));
        }
    }
    validate_artifact_bindings(&plan.baseline, plan_root, "baseline")?;
    validate_artifact_bindings(&plan.candidate, plan_root, "candidate")?;
    Ok(())
}

fn validate_artifact_bindings(
    artifact: &ArtifactInput,
    plan_root: &Path,
    role: &str,
) -> Result<(), ExecuteError> {
    let prefix = format!("artifacts/{role}");
    let expected = [
        (&artifact.executable.path, "executable"),
        (&artifact.provenance_receipt.path, "provenance.json"),
        (&artifact.source_snapshot.path, "source.snapshot"),
        (&artifact.source_manifest.path, "source-manifest.json"),
        (&artifact.source_bundle.path, "source-bundle.json"),
        (&artifact.legacy_evidence.primary.path, "legacy-primary.txt"),
        (
            &artifact.legacy_evidence.secondary.path,
            "legacy-secondary.txt",
        ),
        (&artifact.benchmark_source.path, "benchmark-source.rs"),
        (&artifact.cargo_lock.path, "Cargo.lock"),
        (&artifact.workspace_manifest.path, "workspace-Cargo.toml"),
        (&artifact.package_manifest.path, "package-Cargo.toml"),
        (&artifact.rustc_verbose.path, "rustc-verbose.txt"),
        (&artifact.cargo_verbose.path, "cargo-verbose.txt"),
        (&artifact.build_environment.path, "build-environment.json"),
        (&artifact.profile_identity.path, "profile-identity.json"),
    ];
    for (actual, leaf) in expected {
        let wanted = plan_root.join(&prefix).join(leaf);
        if actual != &wanted {
            return Err(ExecuteError::Plan(format!(
                "sealed {role} input {} does not use the prepare-owned layout",
                actual.display()
            )));
        }
    }
    if let Some(cargo_config) = &artifact.cargo_config {
        let wanted = plan_root.join(&prefix).join("cargo-config.toml");
        if cargo_config.path != wanted {
            return Err(ExecuteError::Plan(format!(
                "sealed {role} Cargo configuration does not use the prepare-owned layout"
            )));
        }
    } else {
        let undeclared = plan_root.join(&prefix).join("cargo-config.toml");
        if undeclared.try_exists().map_err(|source| ExecuteError::Io {
            path: undeclared.clone(),
            source,
        })? {
            return Err(ExecuteError::Plan(format!(
                "sealed {role} tree contains an undeclared Cargo configuration"
            )));
        }
    }
    if let Some(environment) = &artifact.legacy_evidence.environment {
        let wanted = plan_root.join(&prefix).join("legacy-environment.txt");
        if environment.path != wanted {
            return Err(ExecuteError::Plan(format!(
                "sealed {role} legacy environment does not use the prepare-owned layout"
            )));
        }
    } else {
        let undeclared = plan_root.join(&prefix).join("legacy-environment.txt");
        if undeclared.try_exists().map_err(|source| ExecuteError::Io {
            path: undeclared.clone(),
            source,
        })? {
            return Err(ExecuteError::Plan(format!(
                "sealed {role} tree contains an undeclared legacy environment"
            )));
        }
    }
    for (input, leaf, label) in [
        (
            artifact.legacy_evidence.preregistration.as_ref(),
            "legacy-preregistration.txt",
            "preregistration",
        ),
        (
            artifact.legacy_evidence.build_result.as_ref(),
            "legacy-build-result.txt",
            "build result",
        ),
    ] {
        let wanted = plan_root.join(&prefix).join(leaf);
        if let Some(input) = input {
            if input.path != wanted {
                return Err(ExecuteError::Plan(format!(
                    "sealed {role} legacy {label} does not use the prepare-owned layout"
                )));
            }
        } else if wanted.try_exists().map_err(|source| ExecuteError::Io {
            path: wanted.clone(),
            source,
        })? {
            return Err(ExecuteError::Plan(format!(
                "sealed {role} tree contains an undeclared legacy {label}"
            )));
        }
    }
    Ok(())
}

fn validate_sealed_provenance(
    plan: &Plan,
    policy: ProvenanceValidationPolicy<'_>,
) -> Result<(), ExecuteError> {
    provenance::validate_plan(plan, policy)
        .map(|_| ())
        .map_err(|error| ExecuteError::Plan(format!("sealed provenance is invalid: {error}")))
}

fn validate_plan_tree(root: &RootHandle) -> Result<(), ExecuteError> {
    let mut expected_files = BTreeSet::from([
        "artifacts/repository/Cargo.lock".to_string(),
        "artifacts/repository/quic.rs".to_string(),
        "artifacts/repository/source.snapshot".to_string(),
        "artifacts/repository/source-status.txt".to_string(),
        "artifacts/repository/source-manifest.json".to_string(),
        "artifacts/repository/task-spec.md".to_string(),
        "artifacts/reviewed-patch.json".to_string(),
        "environment.json".to_string(),
        "input-identities.json".to_string(),
        "plan.json".to_string(),
        "plan.sha256".to_string(),
        "prepare-receipt.json".to_string(),
        "request.json".to_string(),
    ]);
    for role in ["baseline", "candidate"] {
        for leaf in [
            "executable",
            "provenance.json",
            "source.snapshot",
            "source-manifest.json",
            "source-bundle.json",
            "legacy-primary.txt",
            "legacy-secondary.txt",
            "benchmark-source.rs",
            "Cargo.lock",
            "workspace-Cargo.toml",
            "package-Cargo.toml",
            "rustc-verbose.txt",
            "cargo-verbose.txt",
            "build-environment.json",
            "profile-identity.json",
        ] {
            expected_files.insert(format!("artifacts/{role}/{leaf}"));
        }
    }
    let (files, directories) = root.collect_tree_paths()?;
    for role in ["baseline", "candidate"] {
        let optional_environment = format!("artifacts/{role}/legacy-environment.txt");
        if files.contains(&optional_environment) {
            expected_files.insert(optional_environment);
        }
        for leaf in ["legacy-preregistration.txt", "legacy-build-result.txt"] {
            let optional = format!("artifacts/{role}/{leaf}");
            if files.contains(&optional) {
                expected_files.insert(optional);
            }
        }
        let optional = format!("artifacts/{role}/cargo-config.toml");
        if files.contains(&optional) {
            expected_files.insert(optional);
        }
    }
    let expected_directories = BTreeSet::from([
        "artifacts".to_string(),
        "artifacts/repository".to_string(),
        "artifacts/baseline".to_string(),
        "artifacts/candidate".to_string(),
    ]);
    if files != expected_files || directories != expected_directories {
        return Err(ExecuteError::Plan(format!(
            "plan tree layout differs from prepare output: files={files:?}, directories={directories:?}"
        )));
    }
    let current_uid = unsafe { libc::geteuid() };
    if root.mode()? != 0o555 || root.owner_uid()? != current_uid {
        return Err(ExecuteError::Plan(
            "plan root must be owned by the executing user with mode 555".to_string(),
        ));
    }
    validate_safe_plan_ancestry(root.path())?;
    for path in &expected_files {
        let identity =
            root.stable_file_identity_bounded(Path::new(path), MAX_PROVENANCE_INPUT_BYTES)?;
        let expected_mode = if path.ends_with("/executable") {
            0o555
        } else {
            0o444
        };
        if identity.mode & 0o7777 != expected_mode || identity.owner_uid != current_uid {
            return Err(ExecuteError::Plan(format!(
                "plan input {path} has unsafe owner or mode"
            )));
        }
    }
    for directory in &expected_directories {
        let (mode, uid) = root.directory_mode_owner(Path::new(directory))?;
        if mode != 0o555 || uid != current_uid {
            return Err(ExecuteError::Plan(format!(
                "plan directory {directory} must be owned by the executing user with mode 555"
            )));
        }
    }
    Ok(())
}

fn validate_safe_plan_ancestry(plan_root: &Path) -> Result<(), ExecuteError> {
    for ancestor in plan_root.ancestors() {
        let metadata = fs::symlink_metadata(ancestor).map_err(|source| ExecuteError::Io {
            path: ancestor.to_path_buf(),
            source,
        })?;
        if !metadata.is_dir() {
            return Err(ExecuteError::Plan(format!(
                "plan ancestor is not a directory: {}",
                ancestor.display()
            )));
        }
        let mode = metadata.mode() & 0o7777;
        if mode & 0o022 != 0 && mode & 0o1000 == 0 {
            return Err(ExecuteError::Plan(format!(
                "plan ancestor is writable without sticky protection: {}",
                ancestor.display()
            )));
        }
    }
    Ok(())
}

fn append_artifact_file_inputs<'a>(
    paths: &mut Vec<(&'a PathBuf, &'a String)>,
    artifact: &'a ArtifactInput,
) {
    for input in [
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
    ] {
        paths.push((&input.path, &input.sha256));
    }
    if let Some(input) = &artifact.cargo_config {
        paths.push((&input.path, &input.sha256));
    }
    if let Some(input) = &artifact.legacy_evidence.environment {
        paths.push((&input.path, &input.sha256));
    }
    if let Some(input) = &artifact.legacy_evidence.preregistration {
        paths.push((&input.path, &input.sha256));
    }
    if let Some(input) = &artifact.legacy_evidence.build_result {
        paths.push((&input.path, &input.sha256));
    }
}

#[allow(clippy::too_many_lines)]
fn verify_plan_inputs(plan: &Plan) -> Result<(), ExecuteError> {
    verify_live_repository(&plan.repository)
        .map_err(|error| ExecuteError::Plan(error.to_string()))?;
    let baseline_parent = plan.baseline.executable.path.parent().ok_or_else(|| {
        ExecuteError::Plan("materialized baseline has no artifact directory".to_string())
    })?;
    let candidate_parent = plan.candidate.executable.path.parent().ok_or_else(|| {
        ExecuteError::Plan("materialized candidate has no artifact directory".to_string())
    })?;
    if baseline_parent.parent() != candidate_parent.parent() {
        return Err(ExecuteError::Plan(
            "sealed arm inputs do not share one artifact root".to_string(),
        ));
    }
    let mut paths = vec![
        (&plan.driver.path, &plan.driver.sha256),
        (
            &plan.preflight.environment_path,
            &plan.preflight.environment_sha256,
        ),
        (
            &plan.repository.source_diff,
            &plan.repository.source_diff_sha256,
        ),
        (
            &plan.repository.source_status,
            &plan.repository.source_status_sha256,
        ),
        (
            &plan.repository.source_manifest,
            &plan.repository.source_manifest_sha256,
        ),
        (
            &plan.repository.cargo_lock,
            &plan.repository.cargo_lock_sha256,
        ),
        (
            &plan.repository.quic_source,
            &plan.repository.quic_source_sha256,
        ),
        (
            &plan.repository.task_spec,
            &plan.repository.task_spec_sha256,
        ),
        (
            &plan.reviewed_patch.manifest.path,
            &plan.reviewed_patch.manifest.sha256,
        ),
        (&plan.composed_request.path, &plan.composed_request.sha256),
    ];
    append_artifact_file_inputs(&mut paths, &plan.baseline);
    append_artifact_file_inputs(&mut paths, &plan.candidate);
    let mut seen = BTreeSet::new();
    for (path, expected) in paths.drain(..) {
        let identity = stable_file_identity_bounded(path, MAX_PROVENANCE_INPUT_BYTES)?;
        if identity.sha256 != *expected {
            return Err(ExecuteError::Plan(format!(
                "input drifted: {}",
                path.display()
            )));
        }
        if !seen.insert((identity.device, identity.inode)) {
            return Err(ExecuteError::Plan(format!(
                "plan inputs alias the same file: {}",
                path.display()
            )));
        }
    }
    for artifact in [&plan.baseline, &plan.candidate] {
        let executable_mode =
            stable_file_identity_bounded(&artifact.executable.path, MAX_PROVENANCE_INPUT_BYTES)?
                .mode
                & 0o7777;
        if executable_mode != 0o555 {
            return Err(ExecuteError::Plan(format!(
                "materialized executable has unsafe mode: {}",
                artifact.executable.path.display()
            )));
        }
    }
    let current_path =
        fs::canonicalize(std::env::current_exe().map_err(|source| ExecuteError::Io {
            path: PathBuf::from("current_exe"),
            source,
        })?)
        .map_err(|source| ExecuteError::Io {
            path: PathBuf::from("current_exe"),
            source,
        })?;
    let current = stable_file_identity_bounded(&current_path, MAX_PROVENANCE_INPUT_BYTES)?;
    let sealed_driver =
        stable_file_identity_bounded(&plan.driver.path, MAX_PROVENANCE_INPUT_BYTES)?;
    if current_path != plan.driver.path
        || current.sha256 != plan.driver.sha256
        || current.device != plan.driver.device
        || current.inode != plan.driver.inode
        || !current.same_file(&sealed_driver)
    {
        return Err(ExecuteError::Plan(
            "executing driver path, inode, or hash differs from sealed driver".to_string(),
        ));
    }
    Ok(())
}

#[allow(clippy::too_many_lines)]
pub fn verify(plan_path: &Path, evidence_root: &Path) -> Result<TerminalStatus, ExecuteError> {
    verify_with_policy(
        plan_path,
        evidence_root,
        ProvenanceValidationPolicy::production(),
    )
}

#[allow(clippy::too_many_lines)]
pub(crate) fn verify_with_policy(
    plan_path: &Path,
    evidence_root: &Path,
    validation_policy: ProvenanceValidationPolicy<'_>,
) -> Result<TerminalStatus, ExecuteError> {
    let (_plan_root, plan, plan_bytes, plan_sha256) =
        load_sealed_plan(plan_path, validation_policy)?;
    let canonical_evidence_root =
        fs::canonicalize(evidence_root).map_err(|source| ExecuteError::Io {
            path: evidence_root.to_path_buf(),
            source,
        })?;
    if plan.evidence_root != canonical_evidence_root {
        return Err(ExecuteError::Verification(
            "evidence root differs from sealed plan".to_string(),
        ));
    }
    let root = RootHandle::open(&canonical_evidence_root)?;
    let manifest = root.verify_manifest(Path::new("evidence-manifest.json"))?;
    let (_, manifest_bytes) = root.stable_file_bytes(Path::new("evidence-manifest.json"))?;
    if manifest.entries.is_empty() {
        return Err(ExecuteError::Verification(
            "evidence manifest is empty".to_string(),
        ));
    }
    if manifest.excluded_paths != ["terminal.json".to_string()] {
        return Err(ExecuteError::Verification(
            "manifest exclusions differ from the terminal-only contract".to_string(),
        ));
    }
    let terminal: TerminalReceipt = read_json_root(&root, Path::new("terminal.json"))?;
    if terminal.schema != TERMINAL_SCHEMA
        || terminal.manifest_sha256 != sha256_bytes(&manifest_bytes)
    {
        return Err(ExecuteError::Verification(
            "terminal schema or manifest binding is invalid".to_string(),
        ));
    }
    let actual_paths: BTreeSet<_> = manifest
        .entries
        .iter()
        .map(|entry| entry.relative_path.clone())
        .collect();
    let manifested: BTreeMap<_, _> = manifest
        .entries
        .iter()
        .map(|entry| (entry.relative_path.clone(), entry.clone()))
        .collect();
    let (_, copied_plan) = stable_manifested_bytes(&root, Path::new("plan.json"), &manifested)?;
    if copied_plan != plan_bytes
        || String::from_utf8(
            stable_manifested_bytes(&root, Path::new("plan.sha256"), &manifested)?.1,
        )
        .map_err(|_| ExecuteError::Verification("evidence plan seal is not UTF-8".to_string()))?
            != format!("{plan_sha256}  plan.json\n")
    {
        return Err(ExecuteError::Verification(
            "evidence plan copy or seal mismatch".to_string(),
        ));
    }
    let once: OnceEvidence =
        read_json_manifested(&root, Path::new("execution-once.json"), &manifested)?;
    if once.schema != "distd.quic.fetch.driver.once.v1" || once.plan_sha256 != plan_sha256 {
        return Err(ExecuteError::Verification(
            "execution-once receipt is not bound to the plan".to_string(),
        ));
    }
    let execution_preflight: ExecutionPreflight =
        read_json_manifested(&root, Path::new("execution-preflight.json"), &manifested)?;
    if execution_preflight.prepared_environment_sha256 != plan.preflight.environment_sha256 {
        return Err(ExecuteError::Verification(
            "execution preflight is not bound to the prepared environment".to_string(),
        ));
    }
    check_power(&execution_preflight.current.power)
        .map_err(|error| ExecuteError::Verification(error.detail))?;
    check_loopback(&execution_preflight.current.loopback)
        .map_err(|error| ExecuteError::Verification(error.detail))?;

    let completed_runs = match terminal.status {
        TerminalStatus::Pass | TerminalStatus::Fail => RUN_COUNT,
        TerminalStatus::Invalid => usize::from(terminal.completed_runs),
        other => {
            return Err(ExecuteError::Verification(format!(
                "execution root has unsupported terminal status {other:?}"
            )));
        }
    };
    if completed_runs > RUN_COUNT {
        return Err(ExecuteError::Verification(
            "terminal completed-run count exceeds fixed schedule".to_string(),
        ));
    }
    let expected_paths =
        expected_evidence_paths(&plan, completed_runs, terminal.status, &actual_paths)?;
    if actual_paths != expected_paths {
        let missing: Vec<_> = expected_paths.difference(&actual_paths).cloned().collect();
        let extra: Vec<_> = actual_paths.difference(&expected_paths).cloned().collect();
        return Err(ExecuteError::Verification(format!(
            "evidence schema mismatch; missing={missing:?}, extra={extra:?}"
        )));
    }
    let (_, actual_directories) = root.collect_tree_paths()?;
    let expected_directories = expected_evidence_directories(&plan, completed_runs, &actual_paths);
    if actual_directories != expected_directories {
        return Err(ExecuteError::Verification(format!(
            "evidence directory schema mismatch; expected={expected_directories:?}, actual={actual_directories:?}"
        )));
    }

    let mut records = Vec::with_capacity(completed_runs);
    let mut event_pids = BTreeSet::new();
    for assignment in plan.harness.assignments.iter().take(completed_runs) {
        verify_events(&root, &manifested, assignment, &mut event_pids)?;
        let run_root =
            PathBuf::from("runs").join(format!("{:02}-{}", assignment.sequence, assignment.run_id));
        let assignment_on_disk: RunAssignment =
            read_json_manifested(&root, &run_root.join("assignment.json"), &manifested)?;
        if assignment_on_disk != *assignment {
            return Err(ExecuteError::Verification(format!(
                "assignment mismatch for {}",
                assignment.run_id
            )));
        }
        let expected_executable = match assignment.arm {
            Arm::Baseline => &plan.baseline,
            Arm::Candidate => &plan.candidate,
        };
        let starting: StartingEvidence =
            read_json_manifested(&root, &run_root.join("starting.json"), &manifested)?;
        if starting.assignment != *assignment
            || starting.executable_path != expected_executable.executable.path
            || starting.executable_sha256 != expected_executable.executable.sha256
        {
            return Err(ExecuteError::Verification(format!(
                "starting evidence mismatch for {}",
                assignment.run_id
            )));
        }
        let record: RunRecord =
            read_json_manifested(&root, &run_root.join("record.json"), &manifested)?;
        let child: ChildReceipt =
            read_json_manifested(&root, &run_root.join("child.json"), &manifested)?;
        if record.child != child {
            return Err(ExecuteError::Verification(format!(
                "child receipt mismatch for {}",
                assignment.run_id
            )));
        }
        let stdout = read_bounded_root(
            &root,
            &run_root.join("stdout.log"),
            plan.harness.max_child_stdout_bytes,
            &manifested,
        )?;
        let stderr = read_bounded_root(
            &root,
            &run_root.join("stderr.log"),
            plan.harness.max_child_stderr_bytes,
            &manifested,
        )?;
        if child.stdout_bytes != u64::try_from(stdout.len()).unwrap_or(u64::MAX)
            || child.stderr_bytes != u64::try_from(stderr.len()).unwrap_or(u64::MAX)
            || child.stdout_observed_bytes < child.stdout_bytes
            || child.stderr_observed_bytes < child.stderr_bytes
        {
            return Err(ExecuteError::Verification(format!(
                "child output byte counts differ for {}",
                assignment.run_id
            )));
        }
        let reparsed = parse_harness_records(&stdout, &assignment.run_id)?;
        if record.harness_records != reparsed {
            return Err(ExecuteError::Verification(format!(
                "parsed output mismatch for {}",
                assignment.run_id
            )));
        }
        records.push(record);
    }
    if terminal.status == TerminalStatus::Invalid {
        if terminal.stage != "execution" || terminal.audit.is_some() {
            return Err(ExecuteError::Verification(
                "INVALID terminal has impossible stage or audit".to_string(),
            ));
        }
        if completed_runs == RUN_COUNT {
            if audit_run_metrics(&plan, &records).is_ok() {
                return Err(ExecuteError::Verification(
                    "INVALID twelve-run root unexpectedly passes semantic audit".to_string(),
                ));
            }
        } else {
            validate_run_prefix(&plan, &records)?;
        }
        verify_partial_run_prefix(
            &root,
            &manifested,
            &plan,
            completed_runs,
            &actual_paths,
            &mut event_pids,
        )?;
    } else {
        let recomputed = audit_run_metrics(&plan, &records)?;
        let stored: AuditRecord =
            read_json_manifested(&root, Path::new("audit.json"), &manifested)?;
        if recomputed != stored
            || terminal.stage != "audit"
            || terminal.detail != "fixed twelve-run protocol and evidence audit completed"
            || terminal.completed_runs != u8::try_from(RUN_COUNT).unwrap_or(u8::MAX)
            || terminal.status != recomputed.status
            || terminal.audit.as_ref() != Some(&recomputed)
        {
            return Err(ExecuteError::Verification(
                "terminal verdict differs from recomputed audit".to_string(),
            ));
        }
    }
    Ok(terminal.status)
}

fn expected_evidence_directories(
    plan: &Plan,
    completed_runs: usize,
    actual_paths: &BTreeSet<String>,
) -> BTreeSet<String> {
    let mut directories = BTreeSet::from(["events".to_string(), "runs".to_string()]);
    for assignment in plan.harness.assignments.iter().take(completed_runs) {
        directories.insert(format!(
            "runs/{:02}-{}",
            assignment.sequence, assignment.run_id
        ));
    }
    if completed_runs < RUN_COUNT {
        let assignment = &plan.harness.assignments[completed_runs];
        let directory = format!("runs/{:02}-{}", assignment.sequence, assignment.run_id);
        let prefix = format!("{directory}/");
        if actual_paths.iter().any(|path| path.starts_with(&prefix)) {
            directories.insert(directory);
        }
    }
    directories
}

fn expected_evidence_paths(
    plan: &Plan,
    completed_runs: usize,
    status: TerminalStatus,
    actual: &BTreeSet<String>,
) -> Result<BTreeSet<String>, ExecuteError> {
    let mut paths = BTreeSet::from([
        "execution-once.json".to_string(),
        "execution-preflight.json".to_string(),
        "plan.json".to_string(),
        "plan.sha256".to_string(),
    ]);
    if matches!(status, TerminalStatus::Pass | TerminalStatus::Fail) {
        paths.insert("audit.json".to_string());
    }
    for assignment in plan.harness.assignments.iter().take(completed_runs) {
        let run_directory = format!("runs/{:02}-{}", assignment.sequence, assignment.run_id);
        for name in [
            "assignment.json",
            "child.json",
            "record.json",
            "starting.json",
            "stderr.log",
            "stdout.log",
        ] {
            paths.insert(format!("{run_directory}/{name}"));
        }
        for (phase_index, name) in ["starting", "started", "finished"].into_iter().enumerate() {
            let sequence = u64::from(assignment.sequence) * 3 + phase_index as u64;
            paths.insert(format!("events/{sequence:03}-{name}.json"));
        }
    }
    if status == TerminalStatus::Invalid && completed_runs < RUN_COUNT {
        let assignment = &plan.harness.assignments[completed_runs];
        let prefix = format!("runs/{:02}-{}/", assignment.sequence, assignment.run_id);
        let allowed_run: BTreeSet<_> = [
            "assignment.json",
            "child.json",
            "starting.json",
            "stderr.log",
            "stdout.log",
        ]
        .into_iter()
        .map(|name| format!("{prefix}{name}"))
        .collect();
        let allowed_events: BTreeSet<_> = ["starting", "started", "finished"]
            .into_iter()
            .enumerate()
            .map(|(offset, phase)| {
                let sequence = u64::from(assignment.sequence) * 3 + offset as u64;
                format!("events/{sequence:03}-{phase}.json")
            })
            .collect();
        for path in actual {
            if path.starts_with(&prefix) || allowed_events.contains(path) {
                if !allowed_run.contains(path) && !allowed_events.contains(path) {
                    return Err(ExecuteError::Verification(format!(
                        "illegal partial-run evidence path {path}"
                    )));
                }
                paths.insert(path.clone());
            }
        }
    }
    Ok(paths)
}

fn verify_events(
    root: &RootHandle,
    manifested: &BTreeMap<String, ManifestEntry>,
    assignment: &RunAssignment,
    pids: &mut BTreeSet<u32>,
) -> Result<(), ExecuteError> {
    let mut started_pid = None;
    for (phase_index, (phase, name)) in [
        (EventPhase::Starting, "starting"),
        (EventPhase::Started, "started"),
        (EventPhase::Finished, "finished"),
    ]
    .into_iter()
    .enumerate()
    {
        let sequence = u64::from(assignment.sequence) * 3 + phase_index as u64;
        let event: Event = read_json_manifested(
            root,
            &PathBuf::from("events").join(format!("{sequence:03}-{name}.json")),
            manifested,
        )?;
        if event.schema != EVENT_SCHEMA
            || event.sequence != sequence
            || event.phase != phase
            || event.run_id.as_deref() != Some(&assignment.run_id)
        {
            return Err(ExecuteError::Verification(format!(
                "event mismatch at sequence {sequence}"
            )));
        }
        match phase {
            EventPhase::Starting if event.pid.is_some() => {
                return Err(ExecuteError::Verification(
                    "STARTING event unexpectedly has a PID".to_string(),
                ));
            }
            EventPhase::Started => {
                let pid = event.pid.ok_or_else(|| {
                    ExecuteError::Verification("STARTED event lacks PID".to_string())
                })?;
                if !pids.insert(pid) {
                    return Err(ExecuteError::Verification(format!(
                        "duplicate child PID {pid}"
                    )));
                }
                started_pid = Some(pid);
            }
            EventPhase::Finished if event.pid != started_pid => {
                return Err(ExecuteError::Verification(
                    "FINISHED PID differs from STARTED PID".to_string(),
                ));
            }
            _ => {}
        }
    }
    Ok(())
}

#[allow(clippy::too_many_lines)]
fn verify_partial_run_prefix(
    root: &RootHandle,
    manifested: &BTreeMap<String, ManifestEntry>,
    plan: &Plan,
    completed_runs: usize,
    actual_paths: &BTreeSet<String>,
    pids: &mut BTreeSet<u32>,
) -> Result<(), ExecuteError> {
    if completed_runs == RUN_COUNT {
        return Ok(());
    }
    let assignment = &plan.harness.assignments[completed_runs];
    let run_prefix = format!("runs/{:02}-{}/", assignment.sequence, assignment.run_id);
    let assignment_path = format!("{run_prefix}assignment.json");
    let starting_path = format!("{run_prefix}starting.json");
    let has_run_evidence = actual_paths
        .iter()
        .any(|path| path.starts_with(&run_prefix));
    let event_names = ["starting", "started", "finished"];
    let event_paths: Vec<_> = event_names
        .iter()
        .enumerate()
        .map(|(offset, name)| {
            let sequence = u64::from(assignment.sequence) * 3 + offset as u64;
            format!("events/{sequence:03}-{name}.json")
        })
        .collect();
    let event_presence: Vec<_> = event_paths
        .iter()
        .map(|path| actual_paths.contains(path))
        .collect();

    if !has_run_evidence && event_presence.iter().any(|present| *present) {
        return Err(ExecuteError::Verification(
            "partial events exist without a run assignment".to_string(),
        ));
    }
    if has_run_evidence
        && (!actual_paths.contains(&assignment_path) || !actual_paths.contains(&starting_path))
    {
        return Err(ExecuteError::Verification(
            "partial run lacks assignment or starting identity".to_string(),
        ));
    }
    if event_presence.windows(2).any(|pair| !pair[0] && pair[1]) {
        return Err(ExecuteError::Verification(
            "partial event sequence contains a gap".to_string(),
        ));
    }
    if !has_run_evidence {
        return Ok(());
    }

    let run_root =
        PathBuf::from("runs").join(format!("{:02}-{}", assignment.sequence, assignment.run_id));
    let assignment_on_disk: RunAssignment =
        read_json_manifested(root, &run_root.join("assignment.json"), manifested)?;
    if assignment_on_disk != *assignment {
        return Err(ExecuteError::Verification(
            "partial assignment differs from fixed schedule".to_string(),
        ));
    }
    let starting: StartingEvidence =
        read_json_manifested(root, &run_root.join("starting.json"), manifested)?;
    let expected_executable = match assignment.arm {
        Arm::Baseline => &plan.baseline,
        Arm::Candidate => &plan.candidate,
    };
    if starting.assignment != *assignment
        || starting.executable_path != expected_executable.executable.path
        || starting.executable_sha256 != expected_executable.executable.sha256
    {
        return Err(ExecuteError::Verification(
            "partial starting identity differs from plan".to_string(),
        ));
    }

    let mut started_pid = None;
    for (offset, present) in event_presence.into_iter().enumerate() {
        if !present {
            continue;
        }
        let phase = [
            EventPhase::Starting,
            EventPhase::Started,
            EventPhase::Finished,
        ][offset];
        let sequence = u64::from(assignment.sequence) * 3 + offset as u64;
        let event: Event = read_json_manifested(root, Path::new(&event_paths[offset]), manifested)?;
        if event.schema != EVENT_SCHEMA
            || event.sequence != sequence
            || event.phase != phase
            || event.run_id.as_deref() != Some(&assignment.run_id)
        {
            return Err(ExecuteError::Verification(
                "partial event differs from fixed prefix".to_string(),
            ));
        }
        match phase {
            EventPhase::Starting if event.pid.is_some() => {
                return Err(ExecuteError::Verification(
                    "partial STARTING event unexpectedly has PID".to_string(),
                ));
            }
            EventPhase::Started => {
                let pid = event.pid.ok_or_else(|| {
                    ExecuteError::Verification("partial STARTED lacks PID".to_string())
                })?;
                if !pids.insert(pid) {
                    return Err(ExecuteError::Verification(format!(
                        "duplicate partial child PID {pid}"
                    )));
                }
                started_pid = Some(pid);
            }
            EventPhase::Finished if event.pid != started_pid => {
                return Err(ExecuteError::Verification(
                    "partial FINISHED PID differs from STARTED".to_string(),
                ));
            }
            _ => {}
        }
    }
    if actual_paths.contains(&format!("{run_prefix}child.json")) {
        let child: ChildReceipt =
            read_json_manifested(root, &run_root.join("child.json"), manifested)?;
        if child.schema != CHILD_RECEIPT_SCHEMA {
            return Err(ExecuteError::Verification(
                "partial child receipt has the wrong schema".to_string(),
            ));
        }
        if !actual_paths.contains(&event_paths[2]) {
            return Err(ExecuteError::Verification(
                "partial child receipt exists before FINISHED event".to_string(),
            ));
        }
        let stdout_path = format!("{run_prefix}stdout.log");
        let stderr_path = format!("{run_prefix}stderr.log");
        if !actual_paths.contains(&stdout_path) || !actual_paths.contains(&stderr_path) {
            return Err(ExecuteError::Verification(
                "partial child receipt lacks retained stdout or stderr".to_string(),
            ));
        }
        let stdout = read_bounded_root(
            root,
            Path::new(&stdout_path),
            plan.harness.max_child_stdout_bytes,
            manifested,
        )?;
        let stderr = read_bounded_root(
            root,
            Path::new(&stderr_path),
            plan.harness.max_child_stderr_bytes,
            manifested,
        )?;
        if child.stdout_bytes != u64::try_from(stdout.len()).unwrap_or(u64::MAX)
            || child.stderr_bytes != u64::try_from(stderr.len()).unwrap_or(u64::MAX)
            || child.stdout_observed_bytes < child.stdout_bytes
            || child.stderr_observed_bytes < child.stderr_bytes
        {
            return Err(ExecuteError::Verification(
                "partial child output byte counts differ".to_string(),
            ));
        }
        let child_succeeded = child.status_available
            && child.exit_code == 0
            && child.signal.is_none()
            && child.termination == ChildTerminationReceipt::Exited
            && child.cleanup_issues.is_empty();
        if child_succeeded && parse_harness_records(&stdout, &assignment.run_id).is_ok() {
            return Err(ExecuteError::Verification(
                "successful partial child has a valid record that was not retained".to_string(),
            ));
        }
    }
    Ok(())
}

fn read_json_root<T: for<'de> Deserialize<'de> + Serialize>(
    root: &RootHandle,
    relative: &Path,
) -> Result<T, ExecuteError> {
    let (_, bytes) = root.stable_file_bytes(relative)?;
    let value: T = serde_json::from_slice(&bytes)
        .map_err(|error| ExecuteError::Verification(format!("{}: {error}", relative.display())))?;
    if canonical_json_line(&value)? != bytes {
        return Err(ExecuteError::Verification(format!(
            "{} is not canonical JSON",
            relative.display()
        )));
    }
    Ok(value)
}

fn read_json_manifested<T: for<'de> Deserialize<'de> + Serialize>(
    root: &RootHandle,
    relative: &Path,
    manifested: &BTreeMap<String, ManifestEntry>,
) -> Result<T, ExecuteError> {
    let (_, bytes) = stable_manifested_bytes(root, relative, manifested)?;
    let value: T = serde_json::from_slice(&bytes)
        .map_err(|error| ExecuteError::Verification(format!("{}: {error}", relative.display())))?;
    if canonical_json_line(&value)? != bytes {
        return Err(ExecuteError::Verification(format!(
            "{} is not canonical JSON",
            relative.display()
        )));
    }
    Ok(value)
}

fn stable_manifested_bytes(
    root: &RootHandle,
    relative: &Path,
    manifested: &BTreeMap<String, ManifestEntry>,
) -> Result<(super::evidence::FileIdentity, Vec<u8>), ExecuteError> {
    let text = relative
        .to_str()
        .ok_or_else(|| ExecuteError::Verification("non-UTF-8 evidence path".to_string()))?
        .replace(std::path::MAIN_SEPARATOR, "/");
    let expected = manifested.get(&text).ok_or_else(|| {
        ExecuteError::Verification(format!("{text} is absent from the sealed manifest"))
    })?;
    let (identity, bytes) = root.stable_file_bytes(relative)?;
    if identity.sha256 != expected.sha256
        || identity.length != expected.length
        || identity.device != expected.device
        || identity.inode != expected.inode
        || identity.modified_seconds != expected.modified_seconds
        || identity.modified_nanoseconds != expected.modified_nanoseconds
    {
        return Err(ExecuteError::Verification(format!(
            "{text} changed after the manifest walk"
        )));
    }
    Ok((identity, bytes))
}

fn read_bounded_root(
    root: &RootHandle,
    relative: &Path,
    limit: u64,
    manifested: &BTreeMap<String, ManifestEntry>,
) -> Result<Vec<u8>, ExecuteError> {
    let (identity, bytes) = stable_manifested_bytes(root, relative, manifested)?;
    if identity.length > limit {
        return Err(ExecuteError::HarnessOutput {
            run_id: "unknown".to_string(),
            detail: format!("{} exceeds its {limit}-byte limit", relative.display()),
        });
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::parse_harness_records;
    use crate::quic_fetch_acceptance::model::{
        OPERATIONS_PER_RUN, RESPONSE_BLAKE3, RESPONSE_FRAME_BYTES,
    };

    fn valid_line(run_id: &str) -> Vec<u8> {
        let sample = 10_u64;
        let elapsed = sample * OPERATIONS_PER_RUN;
        format!(
            "test chatter\nDISTD_QUIC_FETCH_BENCH {{\"schema\":\"distd.quic.fetch.v1\",\"run_id\":\"{run_id}\",\"debug_assertions\":false,\"operations\":{OPERATIONS_PER_RUN},\"elapsed_ns\":{elapsed},\"ns_per_op\":{sample},\"per_stream_ns\":{:?},\"response_frame_bytes\":{RESPONSE_FRAME_BYTES},\"response_blake3\":\"{RESPONSE_BLAKE3}\"}}\nend\n",
            vec![
                sample;
                usize::try_from(OPERATIONS_PER_RUN).expect("operation count fits usize")
            ]
        )
        .into_bytes()
    }

    #[test]
    fn parser_accepts_exactly_one_anchored_record_amid_libtest_chatter() {
        let records = parse_harness_records(&valid_line("pair1-B"), "pair1-B").unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].run_id, "pair1-B");
    }

    #[test]
    fn parser_rejects_missing_and_multiple_records() {
        assert!(parse_harness_records(b"none\n", "run").is_err());
        let mut duplicate = valid_line("run");
        duplicate.extend(valid_line("run"));
        assert!(parse_harness_records(&duplicate, "run").is_err());
    }

    #[test]
    fn parser_does_not_accept_a_prefix_mid_line() {
        let output = String::from_utf8(valid_line("run")).unwrap();
        let mid_line = output.replacen(
            "DISTD_QUIC_FETCH_BENCH ",
            "not-anchored DISTD_QUIC_FETCH_BENCH ",
            1,
        );
        assert!(parse_harness_records(mid_line.as_bytes(), "run").is_err());
    }
}

#![cfg(unix)]

use std::fs;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use tempfile::TempDir;

use super::environment::{EnvironmentBlocked, EnvironmentPass, LoopbackFacts, OsFacts, PowerFacts};
use super::evidence::{canonical_json_line, sha256_bytes};
use super::execute::{
    execute_with_dependencies, verify, ExecuteError, ExecutionDependencies, ExecutionPreflight,
};
use super::model::{
    DriverIdentity, ExecutableIdentity, HarnessContract, HarnessRecord, Plan, PreflightIdentity,
    RepositoryIdentity, TerminalReceipt, TerminalStatus, HARNESS_SCHEMA, OPERATIONS_PER_RUN,
    PLAN_SCHEMA, RESPONSE_BLAKE3, RESPONSE_FRAME_BYTES,
};
use super::process::{ProcessError, TEST_EXECUTION_LOCK};

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
    _temporary: TempDir,
    plan_path: PathBuf,
    evidence_root: PathBuf,
    spawn_marker: PathBuf,
}

impl Fixture {
    fn new(baseline: ChildBehavior, candidate: ChildBehavior) -> Self {
        let temporary = tempfile::tempdir().expect("create fixture directory");
        let fixture_root = fs::canonicalize(temporary.path()).expect("canonical fixture root");
        let spawn_marker = fixture_root.join("child-spawns.log");
        let plan_root = fixture_root.join("plan-root");
        let artifacts = plan_root.join("artifacts");
        fs::create_dir_all(&artifacts).expect("create plan artifacts");

        let baseline_path = artifacts.join("baseline");
        let candidate_path = artifacts.join("candidate");
        write_child(&baseline_path, &spawn_marker, baseline);
        write_child(&candidate_path, &spawn_marker, candidate);

        let driver_path = write_file(&fixture_root, "driver", b"acceptance driver");
        let environment_path = write_file(&plan_root, "environment.json", b"prepared\n");
        let baseline_source =
            write_file(&artifacts, "baseline.source-identity", b"baseline source\n");
        let candidate_source = write_file(
            &artifacts,
            "candidate.source-identity",
            b"candidate source\n",
        );
        let source_diff = write_file(&artifacts, "repository-source.snapshot", b"diff\n");
        let source_status = write_file(&artifacts, "repository-status.txt", b"status\n");
        let cargo_lock = write_file(&artifacts, "Cargo.lock", b"lock\n");
        let quic_source = write_file(&artifacts, "quic-source.rs", b"source\n");
        let task_spec = write_file(&artifacts, "task-spec.md", b"spec\n");
        let evidence_root = fixture_root.join("evidence");

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
            repository: RepositoryIdentity {
                root: fixture_root.clone(),
                head: "0123456789abcdef0123456789abcdef01234567".to_string(),
                source_diff_sha256: file_sha256(&source_diff),
                source_diff,
                source_status_sha256: file_sha256(&source_status),
                source_status,
                cargo_lock_sha256: file_sha256(&cargo_lock),
                cargo_lock,
                quic_source_sha256: file_sha256(&quic_source),
                quic_source,
                task_spec_sha256: file_sha256(&task_spec),
                task_spec,
            },
            baseline: ExecutableIdentity {
                sha256: file_sha256(&baseline_path),
                path: baseline_path,
                build_identity: "acceptance-baseline".to_string(),
                source_identity_sha256: file_sha256(&baseline_source),
                source_identity_path: baseline_source,
            },
            candidate: ExecutableIdentity {
                sha256: file_sha256(&candidate_path),
                path: candidate_path,
                build_identity: "acceptance-candidate".to_string(),
                source_identity_sha256: file_sha256(&candidate_source),
                source_identity_path: candidate_source,
            },
            harness: HarnessContract::fixed(),
            blocked_evidence: fixture_root.join("blocked.json"),
            evidence_root: evidence_root.clone(),
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
        for entry in fs::read_dir(&artifacts).expect("list plan artifacts") {
            let path = entry.expect("artifact entry").path();
            let mode = if matches!(
                path.file_name().and_then(|name| name.to_str()),
                Some("baseline" | "candidate")
            ) {
                0o555
            } else {
                0o444
            };
            set_mode(&path, mode);
        }
        for name in [
            "environment.json",
            "input-identities.json",
            "prepare-receipt.json",
            "plan.json",
            "plan.sha256",
        ] {
            set_mode(&plan_root.join(name), 0o444);
        }
        set_mode(&artifacts, 0o555);
        set_mode(&plan_root, 0o555);

        Self {
            _temporary: temporary,
            plan_path,
            evidence_root,
            spawn_marker,
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

#[test]
fn successful_twelve_run_execution_is_pass_and_verifies() {
    let fixture = Fixture::new(
        ChildBehavior::Valid { ns_per_op: 100 },
        ChildBehavior::Valid { ns_per_op: 104 },
    );

    execute_retry(&fixture, &FakeDependencies::passing()).expect("execute PASS fixture");

    assert_eq!(fixture.spawn_count(), 12);
    assert_eq!(fixture.terminal().status, TerminalStatus::Pass);
    verify(&fixture.plan_path, &fixture.evidence_root).expect("verify PASS evidence");
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
    verify(&fixture.plan_path, &fixture.evidence_root).expect("verify FAIL evidence");
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
    verify(&fixture.plan_path, &fixture.evidence_root).expect("verify INVALID partial evidence");
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
        verify(&fixture.plan_path, &fixture.evidence_root)
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

    assert!(matches!(error, ExecuteError::Audit(_)));
    assert_eq!(fixture.spawn_count(), 12);
    let terminal = fixture.terminal();
    assert_eq!(terminal.status, TerminalStatus::Invalid);
    assert_eq!(terminal.completed_runs, 12);
    verify(&fixture.plan_path, &fixture.evidence_root)
        .expect("verify twelve-run semantic INVALID evidence");
}

#[test]
fn input_drift_before_first_spawn_has_zero_product_executions() {
    let fixture = Fixture::new(
        ChildBehavior::Valid { ns_per_op: 100 },
        ChildBehavior::Valid { ns_per_op: 100 },
    );
    let dependencies = FakeDependencies::drift_on(2);

    let error = execute_retry(&fixture, &dependencies).unwrap_err();

    assert!(matches!(error, ExecuteError::Plan(detail) if detail.contains("input drifted")));
    assert_eq!(dependencies.verify_calls.load(Ordering::SeqCst), 2);
    assert_eq!(fixture.spawn_count(), 0);
    assert_eq!(fixture.terminal().completed_runs, 0);
    verify(&fixture.plan_path, &fixture.evidence_root).expect("verify zero-spawn INVALID evidence");
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

    assert!(matches!(error, ExecuteError::Blocked(detail) if detail == "fixture thermal block"));
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
    assert!(verify(&extra_fixture.plan_path, &extra_fixture.evidence_root).is_err());

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
    assert!(verify(&tampered_fixture.plan_path, &tampered_fixture.evidence_root).is_err());

    let empty_directory_fixture = Fixture::new(
        ChildBehavior::Valid { ns_per_op: 100 },
        ChildBehavior::Valid { ns_per_op: 104 },
    );
    execute_retry(&empty_directory_fixture, &FakeDependencies::passing())
        .expect("execute empty-directory fixture");
    fs::create_dir(empty_directory_fixture.evidence_root.join("unsealed-empty"))
        .expect("add empty evidence directory");
    assert!(verify(
        &empty_directory_fixture.plan_path,
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
    assert!(verify(
        &manifest_bytes_fixture.plan_path,
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
    assert!(verify(
        &terminal_bytes_fixture.plan_path,
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

    verify(&fixture.plan_path, &equivalent).expect("verify equivalent evidence path");
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
        match execute_with_dependencies(&fixture.plan_path, dependencies) {
            Err(ExecuteError::Process(ProcessError::ConcurrentGuard))
                if Instant::now() < deadline =>
            {
                thread::sleep(Duration::from_millis(10));
            }
            result => return result,
        }
    }
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

fn write_child(path: &Path, spawn_marker: &Path, behavior: ChildBehavior) {
    let marker = shell_single_quote(spawn_marker.as_os_str().to_string_lossy().as_ref());
    let mut script = format!("#!/bin/sh\nprintf 'spawn\\n' >> {marker}\n");
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

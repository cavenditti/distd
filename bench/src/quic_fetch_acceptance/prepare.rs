//! Fail-closed preparation of the shared QUIC Fetch acceptance [`Plan`].

use super::environment::{
    run_environment_preflight, EnvironmentExpectations, EnvironmentPass, EnvironmentProbe,
    SystemEnvironmentProbe,
};
use super::evidence::{
    self, canonical_absent_root, canonical_json_line, pin_absent_root, AbsentRoot, EvidenceError,
    FileIdentity, PinnedAbsentRoot, RootHandle,
};
use super::model::{
    DriverIdentity, ExecutableIdentity, HarnessContract, Plan, PreflightIdentity,
    RepositoryIdentity, Request, TerminalStatus, PLAN_SCHEMA, REQUEST_SCHEMA,
};
use serde::{Deserialize, Serialize};
use std::ffi::OsString;
use std::fs;
use std::io::Read;
use std::os::unix::ffi::OsStringExt;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

const GIT_PROBE_TIMEOUT: Duration = Duration::from_secs(15);
const MAX_GIT_STDOUT_BYTES: u64 = 64 * 1024 * 1024;
const MAX_GIT_STDERR_BYTES: u64 = 1024 * 1024;
const SOURCE_SNAPSHOT_MAGIC: &[u8] = b"distd.quic.fetch.source-snapshot.v1\0";

/// Paths supplied by the CLI rather than by the portable request document.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreparePaths {
    /// Must canonicalize to the repository root and is therefore safe for every child.
    pub child_working_directory: PathBuf,
    /// New root containing only preparation evidence and the sealed plan.
    pub plan_root: PathBuf,
    /// Immutable failure receipt outside both the plan and execution roots.
    pub blocked_receipt_path: PathBuf,
    pub driver_executable: PathBuf,
    pub environment_expectations: EnvironmentExpectations,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamedFileIdentity {
    pub role: String,
    pub identity: FileIdentity,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreparedFileIdentities {
    pub driver: FileIdentity,
    pub environment_receipt: FileIdentity,
    pub baseline: FileIdentity,
    pub candidate: FileIdentity,
    pub repository_inputs: Vec<NamedFileIdentity>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreparePassReceipt {
    pub schema: String,
    pub status: TerminalStatus,
    pub plan_root: PathBuf,
    pub plan_path: PathBuf,
    pub plan_seal_path: PathBuf,
    pub execution_evidence_root: PathBuf,
    pub execution_evidence_root_absent: bool,
    pub child_working_directory: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepositoryCaptureReceipt {
    pub schema: String,
    pub repository: RepositoryIdentity,
    pub output_root: PathBuf,
    pub product_invocations: u8,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockedReceipt {
    pub schema: String,
    pub status: TerminalStatus,
    pub stage: String,
    pub code: PreflightBlockCode,
    pub detail: String,
    pub requested_evidence_root: PathBuf,
    pub product_invocations: u8,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PreflightBlockCode {
    RequestSchema,
    HarnessContract,
    InvalidPath,
    EvidenceRootExists,
    InputSymlink,
    InputNotRegular,
    InputNotExecutable,
    SameExecutable,
    InvalidExpectedHash,
    HashMismatch,
    SourceOutsideRepository,
    InputChanged,
    Environment,
    RepositoryState,
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PrepareOutcome {
    Prepared {
        plan: Plan,
        identities: PreparedFileIdentities,
        plan_sha256: String,
        plan_root: PathBuf,
    },
    Blocked(BlockedReceipt),
}

#[derive(Debug, thiserror::Error)]
pub enum PrepareError {
    #[error(transparent)]
    Evidence(#[from] EvidenceError),
    #[error("failed to create plan evidence root {path}: {source}")]
    CreatePlanRoot {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to materialize immutable plan input {path}: {source}")]
    MaterializeInput {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error(
        "blocked evidence path {path} must be outside canonical repository root {repository_root}"
    )]
    UnsafeBlockedEvidence {
        path: PathBuf,
        repository_root: PathBuf,
    },
}

#[derive(Debug)]
struct PreflightIssue {
    code: PreflightBlockCode,
    detail: String,
}

impl PreflightIssue {
    fn new(code: PreflightBlockCode, detail: impl Into<String>) -> Self {
        Self {
            code,
            detail: detail.into(),
        }
    }
}

struct ValidatedPreparation {
    plan_root: PinnedAbsentRoot,
    execution_root: AbsentRoot,
    blocked_evidence: PathBuf,
    repository: RepositoryIdentity,
    baseline: ValidatedExecutable,
    candidate: ValidatedExecutable,
    driver_identity: FileIdentity,
    repository_inputs: Vec<ValidatedRepositoryInput>,
    environment: EnvironmentPass,
    child_working_directory: PathBuf,
}

struct ValidatedRepositoryInput {
    role: String,
    identity: FileIdentity,
    bytes: Vec<u8>,
}

struct ValidatedExecutable {
    executable: ExecutableIdentity,
    executable_identity: FileIdentity,
    executable_bytes: Vec<u8>,
    source_identity: FileIdentity,
    source_identity_bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveRepositoryState {
    pub head: String,
    pub source_diff: Vec<u8>,
    pub source_status: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("repository verification failed: {detail}")]
pub struct RepositoryVerificationError {
    pub detail: String,
}

impl RepositoryVerificationError {
    fn new(detail: impl Into<String>) -> Self {
        Self {
            detail: detail.into(),
        }
    }
}

pub trait RepositoryProbe {
    fn observe(
        &self,
        canonical_root: &Path,
    ) -> Result<LiveRepositoryState, RepositoryVerificationError>;
}

/// Capture the canonical tracked-plus-untracked source identity consumed by `prepare`.
///
/// This is a non-product operation. The absent output root must be outside the repository so the
/// capture cannot perturb the state it describes.
pub fn capture_repository_identity(
    repository_root: &Path,
    output_root: &Path,
    task_spec: &Path,
) -> Result<RepositoryCaptureReceipt, PrepareError> {
    let repository_root =
        fs::canonicalize(repository_root).map_err(|source| PrepareError::MaterializeInput {
            path: repository_root.to_path_buf(),
            source,
        })?;
    let task_spec =
        fs::canonicalize(task_spec).map_err(|source| PrepareError::MaterializeInput {
            path: task_spec.to_path_buf(),
            source,
        })?;
    if !task_spec.starts_with(&repository_root) {
        return Err(PrepareError::UnsafeBlockedEvidence {
            path: task_spec,
            repository_root,
        });
    }
    let pinned = pin_absent_root(output_root)?;
    if pinned.absent().canonical_path.starts_with(&repository_root) {
        return Err(PrepareError::UnsafeBlockedEvidence {
            path: pinned.absent().canonical_path.clone(),
            repository_root,
        });
    }
    let observed = SystemRepositoryProbe
        .observe(&repository_root)
        .map_err(|error| materialize_error(output_root, error.detail))?;
    let root = pinned.create_root()?;
    root.atomic_create_new_synced(
        Path::new("repository-source.snapshot"),
        &observed.source_diff,
    )?;
    root.atomic_create_new_synced(Path::new("repository-status.txt"), &observed.source_status)?;
    set_plan_file_mode(&root, Path::new("repository-source.snapshot"), 0o444)?;
    set_plan_file_mode(&root, Path::new("repository-status.txt"), 0o444)?;
    let source_diff = root.stable_file_identity(Path::new("repository-source.snapshot"))?;
    let source_status = root.stable_file_identity(Path::new("repository-status.txt"))?;
    let cargo_lock = evidence::stable_file_identity(&repository_root.join("Cargo.lock"))?;
    let quic_source = evidence::stable_file_identity(&repository_root.join("server/src/quic.rs"))?;
    let task_spec = evidence::stable_file_identity(&task_spec)?;
    let repository = RepositoryIdentity {
        root: repository_root,
        head: observed.head,
        source_diff: source_diff.canonical_path,
        source_diff_sha256: source_diff.sha256,
        source_status: source_status.canonical_path,
        source_status_sha256: source_status.sha256,
        cargo_lock: cargo_lock.canonical_path,
        cargo_lock_sha256: cargo_lock.sha256,
        quic_source: quic_source.canonical_path,
        quic_source_sha256: quic_source.sha256,
        task_spec: task_spec.canonical_path,
        task_spec_sha256: task_spec.sha256,
    };
    let receipt = RepositoryCaptureReceipt {
        schema: "distd.quic.fetch.repository-capture.v1".to_string(),
        repository,
        output_root: root.path().to_path_buf(),
        product_invocations: 0,
    };
    root.write_json_create_new(Path::new("repository.json"), &receipt)?;
    set_plan_file_mode(&root, Path::new("repository.json"), 0o444)?;
    root.set_mode(0o555)?;
    root.sync()?;
    Ok(receipt)
}

#[derive(Debug, Clone, Copy, Default)]
pub struct SystemRepositoryProbe;

impl RepositoryProbe for SystemRepositoryProbe {
    fn observe(
        &self,
        canonical_root: &Path,
    ) -> Result<LiveRepositoryState, RepositoryVerificationError> {
        let first = observe_repository_once(canonical_root)?;
        let second = observe_repository_once(canonical_root)?;
        if first != second {
            return Err(RepositoryVerificationError::new(
                "repository changed while its live state was observed",
            ));
        }
        Ok(first)
    }
}

/// Re-observe and compare the live repository state against the frozen snapshots in the model.
pub fn verify_live_repository(
    repository: &RepositoryIdentity,
) -> Result<(), RepositoryVerificationError> {
    verify_live_repository_with_probe(repository, &SystemRepositoryProbe)
}

pub fn verify_live_repository_with_probe<P: RepositoryProbe>(
    repository: &RepositoryIdentity,
    probe: &P,
) -> Result<(), RepositoryVerificationError> {
    let root = fs::canonicalize(&repository.root).map_err(|error| {
        RepositoryVerificationError::new(format!("cannot canonicalize repository root: {error}"))
    })?;
    let observed = probe.observe(&root)?;
    if observed.head != repository.head {
        return Err(RepositoryVerificationError::new(
            "live HEAD differs from the frozen request",
        ));
    }
    let expected_diff = stable_read(&repository.source_diff)?;
    if observed.source_diff != expected_diff {
        return Err(RepositoryVerificationError::new(
            "live tracked/untracked source snapshot differs from the frozen snapshot",
        ));
    }
    let expected_status = stable_read(&repository.source_status)?;
    if observed.source_status != expected_status {
        return Err(RepositoryVerificationError::new(
            "live git status --porcelain=v1 -uall differs from the frozen snapshot",
        ));
    }
    Ok(())
}

fn observe_repository_once(
    root: &Path,
) -> Result<LiveRepositoryState, RepositoryVerificationError> {
    let reported_root = git_output(root, &["rev-parse", "--show-toplevel"])
        .and_then(|bytes| parse_git_path_line(&bytes, "git repository root"))?;
    if reported_root != root {
        return Err(RepositoryVerificationError::new(format!(
            "git repository root {} differs from the canonical requested root {}",
            reported_root.display(),
            root.display()
        )));
    }
    let head = git_output(root, &["rev-parse", "HEAD"])
        .and_then(|bytes| {
            String::from_utf8(bytes)
                .map_err(|_| RepositoryVerificationError::new("git HEAD output was not UTF-8"))
        })?
        .trim_end_matches(['\r', '\n'])
        .to_owned();
    let tracked_diff = git_output(
        root,
        &["diff", "HEAD", "--binary", "--no-ext-diff", "--no-textconv"],
    )?;
    let source_status = git_output(root, &["status", "--porcelain=v1", "-uall"])?;
    let untracked_paths = git_output(root, &["ls-files", "--others", "--exclude-standard", "-z"])?;
    let source_diff = canonical_source_snapshot(root, &tracked_diff, &untracked_paths)?;
    Ok(LiveRepositoryState {
        head,
        source_diff,
        source_status,
    })
}

fn parse_git_path_line(bytes: &[u8], role: &str) -> Result<PathBuf, RepositoryVerificationError> {
    let text = std::str::from_utf8(bytes)
        .map_err(|_| RepositoryVerificationError::new(format!("{role} was not UTF-8")))?;
    let text = text.strip_suffix('\n').unwrap_or(text);
    let text = text.strip_suffix('\r').unwrap_or(text);
    if text.is_empty() || text.bytes().any(|byte| matches!(byte, b'\r' | b'\n')) {
        return Err(RepositoryVerificationError::new(format!(
            "{role} was not exactly one path line"
        )));
    }
    Ok(PathBuf::from(text))
}

fn canonical_source_snapshot(
    root: &Path,
    tracked_diff: &[u8],
    untracked_output: &[u8],
) -> Result<Vec<u8>, RepositoryVerificationError> {
    let mut untracked_paths = parse_nul_paths(untracked_output)?;
    untracked_paths.sort();
    if untracked_paths.windows(2).any(|paths| paths[0] == paths[1]) {
        return Err(RepositoryVerificationError::new(
            "git reported a duplicate untracked path",
        ));
    }

    let mut snapshot = Vec::new();
    snapshot.extend_from_slice(SOURCE_SNAPSHOT_MAGIC);
    append_snapshot_bytes(&mut snapshot, tracked_diff)?;
    append_snapshot_u64(&mut snapshot, untracked_paths.len())?;
    for raw_path in untracked_paths {
        let relative = PathBuf::from(OsString::from_vec(raw_path.clone()));
        validate_untracked_path(root, &relative)?;
        let path = root.join(&relative);
        let (identity, _) = evidence::stable_file_bytes(&path).map_err(|error| {
            RepositoryVerificationError::new(format!(
                "cannot bind untracked regular file {}: {error}",
                relative.display()
            ))
        })?;
        if identity.canonical_path != path {
            return Err(RepositoryVerificationError::new(format!(
                "untracked path did not resolve directly beneath the repository: {}",
                relative.display()
            )));
        }
        append_snapshot_bytes(&mut snapshot, &raw_path)?;
        snapshot.extend_from_slice(&(identity.mode & 0o7777).to_be_bytes());
        snapshot.extend_from_slice(&identity.length.to_be_bytes());
        append_snapshot_bytes(&mut snapshot, identity.sha256.as_bytes())?;
    }
    Ok(snapshot)
}

fn parse_nul_paths(bytes: &[u8]) -> Result<Vec<Vec<u8>>, RepositoryVerificationError> {
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    if bytes.last() != Some(&0) {
        return Err(RepositoryVerificationError::new(
            "git untracked path output was not NUL-terminated",
        ));
    }
    let mut paths = Vec::new();
    for path in bytes[..bytes.len() - 1].split(|byte| *byte == 0) {
        if path.is_empty() {
            return Err(RepositoryVerificationError::new(
                "git reported an empty untracked path",
            ));
        }
        paths.push(path.to_vec());
    }
    Ok(paths)
}

fn validate_untracked_path(
    root: &Path,
    relative: &Path,
) -> Result<(), RepositoryVerificationError> {
    if relative.is_absolute()
        || relative
            .components()
            .any(|component| !matches!(component, std::path::Component::Normal(_)))
    {
        return Err(RepositoryVerificationError::new(format!(
            "git reported an unsafe untracked path: {}",
            relative.display()
        )));
    }

    let mut current = root.to_path_buf();
    for component in relative.components() {
        current.push(component.as_os_str());
        let metadata = fs::symlink_metadata(&current).map_err(|error| {
            RepositoryVerificationError::new(format!(
                "cannot inspect untracked path {}: {error}",
                relative.display()
            ))
        })?;
        if metadata.file_type().is_symlink() {
            return Err(RepositoryVerificationError::new(format!(
                "untracked symlinks are forbidden: {}",
                relative.display()
            )));
        }
    }
    Ok(())
}

fn append_snapshot_u64(
    snapshot: &mut Vec<u8>,
    value: usize,
) -> Result<(), RepositoryVerificationError> {
    let value = u64::try_from(value)
        .map_err(|_| RepositoryVerificationError::new("source snapshot entry count exceeds u64"))?;
    snapshot.extend_from_slice(&value.to_be_bytes());
    Ok(())
}

fn append_snapshot_bytes(
    snapshot: &mut Vec<u8>,
    bytes: &[u8],
) -> Result<(), RepositoryVerificationError> {
    append_snapshot_u64(snapshot, bytes.len())?;
    snapshot.extend_from_slice(bytes);
    Ok(())
}

fn git_output(root: &Path, arguments: &[&str]) -> Result<Vec<u8>, RepositoryVerificationError> {
    git_output_with_command(root, arguments, Command::new("/usr/bin/git"))
}

fn git_output_with_command(
    root: &Path,
    arguments: &[&str],
    mut command: Command,
) -> Result<Vec<u8>, RepositoryVerificationError> {
    command
        .env_clear()
        .arg("-C")
        .arg(root)
        .args(arguments)
        .env("LC_ALL", "C")
        .env("LANG", "C")
        .env("TZ", "UTC")
        .env("GIT_OPTIONAL_LOCKS", "0")
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_CONFIG_NOSYSTEM", "1");
    bounded_command_output(
        &mut command,
        &format!("git {}", arguments.join(" ")),
        ProbeLimits::git(),
    )
}

#[derive(Clone, Copy)]
struct ProbeLimits {
    duration: Duration,
    stdout_bytes: u64,
    stderr_bytes: u64,
}

impl ProbeLimits {
    const fn git() -> Self {
        Self {
            duration: GIT_PROBE_TIMEOUT,
            stdout_bytes: MAX_GIT_STDOUT_BYTES,
            stderr_bytes: MAX_GIT_STDERR_BYTES,
        }
    }
}

fn bounded_command_output(
    command: &mut Command,
    label: &str,
    limits: ProbeLimits,
) -> Result<Vec<u8>, RepositoryVerificationError> {
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .process_group(0);
    let mut child = command.spawn().map_err(|error| {
        RepositoryVerificationError::new(format!("cannot invoke {label}: {error}"))
    })?;
    let process_group = i32::try_from(child.id()).map_err(|_| {
        RepositoryVerificationError::new(format!("{label} process ID exceeds pid_t"))
    })?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| RepositoryVerificationError::new(format!("{label} stdout was not piped")))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| RepositoryVerificationError::new(format!("{label} stderr was not piped")))?;
    let stdout_reader = thread::spawn(move || read_probe_stream(stdout, limits.stdout_bytes));
    let stderr_reader = thread::spawn(move || read_probe_stream(stderr, limits.stderr_bytes));

    let started = Instant::now();
    let mut timed_out = false;
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) if started.elapsed() < limits.duration => {
                thread::sleep(Duration::from_millis(5));
            }
            Ok(None) => {
                timed_out = true;
                kill_process_group(process_group);
                break child.wait().map_err(|error| {
                    RepositoryVerificationError::new(format!(
                        "cannot reap timed-out {label}: {error}"
                    ))
                })?;
            }
            Err(error) => {
                kill_process_group(process_group);
                let _ = child.wait();
                return Err(RepositoryVerificationError::new(format!(
                    "cannot wait for {label}: {error}"
                )));
            }
        }
    };
    // A hostile repository configuration must not leave helper descendants holding the pipes.
    kill_process_group(process_group);

    let (stdout, stdout_exceeded) = join_probe_reader(stdout_reader, label, "stdout")?;
    let (stderr, stderr_exceeded) = join_probe_reader(stderr_reader, label, "stderr")?;
    if timed_out {
        return Err(RepositoryVerificationError::new(format!(
            "{label} exceeded its execution-time limit"
        )));
    }
    if stdout_exceeded {
        return Err(RepositoryVerificationError::new(format!(
            "{label} exceeded its stdout limit"
        )));
    }
    if stderr_exceeded {
        return Err(RepositoryVerificationError::new(format!(
            "{label} exceeded its stderr limit"
        )));
    }
    if !status.success() {
        return Err(RepositoryVerificationError::new(format!(
            "{label} exited with status {}",
            status.code().unwrap_or(-1)
        )));
    }
    if !stderr.is_empty() {
        return Err(RepositoryVerificationError::new(format!(
            "{label} produced unexpected stderr"
        )));
    }
    Ok(stdout)
}

fn read_probe_stream<R: Read>(
    mut reader: R,
    limit: u64,
) -> Result<(Vec<u8>, bool), std::io::Error> {
    let capacity = usize::try_from(limit).unwrap_or(usize::MAX);
    let mut bytes = Vec::with_capacity(capacity.min(64 * 1024));
    let mut exceeded = false;
    let mut chunk = [0_u8; 8192];
    loop {
        let read = reader.read(&mut chunk)?;
        if read == 0 {
            break;
        }
        let remaining = capacity.saturating_sub(bytes.len());
        let retained = remaining.min(read);
        bytes.extend_from_slice(&chunk[..retained]);
        exceeded |= retained != read;
    }
    Ok((bytes, exceeded))
}

fn kill_process_group(process_group: i32) {
    // The probe child is placed in its own group, so this cannot target the driver itself.
    unsafe {
        libc::kill(-process_group, libc::SIGKILL);
    }
}

fn join_probe_reader(
    reader: thread::JoinHandle<Result<(Vec<u8>, bool), std::io::Error>>,
    label: &str,
    stream: &str,
) -> Result<(Vec<u8>, bool), RepositoryVerificationError> {
    reader
        .join()
        .map_err(|_| RepositoryVerificationError::new(format!("{label} {stream} reader panicked")))?
        .map_err(|error| {
            RepositoryVerificationError::new(format!(
                "cannot read bounded {stream} from {label}: {error}"
            ))
        })
}

fn stable_read(path: &Path) -> Result<Vec<u8>, RepositoryVerificationError> {
    evidence::stable_file_bytes(path)
        .map(|(_, bytes)| bytes)
        .map_err(|error| RepositoryVerificationError::new(error.to_string()))
}

/// Validate the request, freeze the shared model plan, and leave its execution root absent.
///
/// Validation failures create only `blocked_receipt_path`. Successful preparation creates the
/// separate `plan_root`, with `plan.sha256` as its final file.
pub fn prepare(request: &Request, paths: &PreparePaths) -> Result<PrepareOutcome, PrepareError> {
    prepare_with_probes(
        request,
        paths,
        &SystemEnvironmentProbe,
        &SystemRepositoryProbe,
    )
}

pub fn prepare_with_probes<E: EnvironmentProbe, R: RepositoryProbe>(
    request: &Request,
    paths: &PreparePaths,
    environment_probe: &E,
    repository_probe: &R,
) -> Result<PrepareOutcome, PrepareError> {
    let blocked_root = pin_absent_root(&request.blocked_evidence)?;
    let configured_blocked = canonical_absent_root(&paths.blocked_receipt_path)?;
    // An unsafe blocked path cannot be used to report its own rejection: doing so would mutate
    // the measured repository. Fail before any receipt/root creation or product observation.
    if let Ok(repository_root) = fs::canonicalize(&request.repository.root) {
        if blocked_root
            .absent()
            .canonical_path
            .starts_with(&repository_root)
        {
            return Err(PrepareError::UnsafeBlockedEvidence {
                path: blocked_root.absent().canonical_path.clone(),
                repository_root,
            });
        }
    }
    if blocked_root.absent().canonical_path != configured_blocked.canonical_path {
        let receipt = blocked_receipt(
            request,
            PreflightIssue::new(
                PreflightBlockCode::InvalidPath,
                "blocked receipt path differs from request.blocked_evidence",
            ),
        );
        blocked_root.write_json_create_new(&receipt)?;
        return Ok(PrepareOutcome::Blocked(receipt));
    }
    let validated = match validate_request(
        request,
        paths,
        blocked_root.absent(),
        environment_probe,
        repository_probe,
    ) {
        Ok(value) => value,
        Err(issue) => {
            let receipt = blocked_receipt(request, issue);
            blocked_root.write_json_create_new(&receipt)?;
            return Ok(PrepareOutcome::Blocked(receipt));
        }
    };

    materialize_plan_root(validated)
}

fn blocked_receipt(request: &Request, issue: PreflightIssue) -> BlockedReceipt {
    BlockedReceipt {
        schema: "distd.quic.fetch.prepare-blocked.v1".to_owned(),
        status: TerminalStatus::Blocked,
        stage: "preflight".to_owned(),
        code: issue.code,
        detail: issue.detail,
        requested_evidence_root: request.evidence_root.clone(),
        product_invocations: 0,
    }
}

#[allow(clippy::too_many_lines)]
fn validate_request<E: EnvironmentProbe, R: RepositoryProbe>(
    request: &Request,
    paths: &PreparePaths,
    blocked_root: &AbsentRoot,
    environment_probe: &E,
    repository_probe: &R,
) -> Result<ValidatedPreparation, PreflightIssue> {
    if request.schema != REQUEST_SCHEMA {
        return Err(PreflightIssue::new(
            PreflightBlockCode::RequestSchema,
            format!("unsupported request schema {:?}", request.schema),
        ));
    }
    if request.harness != HarnessContract::fixed() {
        return Err(PreflightIssue::new(
            PreflightBlockCode::HarnessContract,
            "request harness differs from the compiled fixed contract",
        ));
    }

    let execution_root =
        canonical_absent_root(&request.evidence_root).map_err(|error| map_root_error(&error))?;
    let plan_root = pin_absent_root(&paths.plan_root).map_err(|error| map_root_error(&error))?;
    let blocked_evidence = blocked_root.canonical_path.clone();
    if plan_root.absent().canonical_path == execution_root.canonical_path {
        return Err(PreflightIssue::new(
            PreflightBlockCode::InvalidPath,
            "plan root and execution evidence root must be distinct",
        ));
    }
    for protected_root in [
        &execution_root.canonical_path,
        &plan_root.absent().canonical_path,
    ] {
        if blocked_evidence.starts_with(protected_root) {
            return Err(PreflightIssue::new(
                PreflightBlockCode::InvalidPath,
                "blocked receipt must be outside plan and execution roots",
            ));
        }
    }

    let repository_root = canonical_directory(&request.repository.root, "repository root")?;
    for (role, path) in [
        ("plan root", &plan_root.absent().canonical_path),
        ("execution evidence root", &execution_root.canonical_path),
        ("blocked evidence", &blocked_evidence),
    ] {
        if path.starts_with(&repository_root) {
            return Err(PreflightIssue::new(
                PreflightBlockCode::InvalidPath,
                format!("{role} must be outside the canonical repository root"),
            ));
        }
    }
    let child_working_directory =
        canonical_directory(&paths.child_working_directory, "child working directory")?;
    if child_working_directory != repository_root {
        return Err(PreflightIssue::new(
            PreflightBlockCode::InvalidPath,
            "child working directory must be the canonical repository root",
        ));
    }

    validate_repository_text_identity(&request.repository)?;
    let repository_inputs = validate_repository_files(&request.repository, &repository_root)?;
    verify_live_repository_with_probe(&request.repository, repository_probe).map_err(|error| {
        PreflightIssue::new(PreflightBlockCode::RepositoryState, error.to_string())
    })?;
    let baseline = validate_executable(&request.baseline, "baseline")?;
    let candidate = validate_executable(&request.candidate, "candidate")?;
    if baseline
        .executable_identity
        .same_file(&candidate.executable_identity)
    {
        return Err(PreflightIssue::new(
            PreflightBlockCode::SameExecutable,
            "baseline and candidate resolve to the same file identity",
        ));
    }

    let driver_identity = checked_regular_file(&paths.driver_executable)?;
    require_executable("driver", &driver_identity)?;
    let environment = run_environment_preflight(environment_probe, &paths.environment_expectations)
        .map_err(|blocked| {
            PreflightIssue::new(
                PreflightBlockCode::Environment,
                format!("{:?}: {}", blocked.code, blocked.detail),
            )
        })?;

    let repository = RepositoryIdentity {
        root: repository_root,
        head: request.repository.head.clone(),
        source_diff: canonical_role_path(&repository_inputs, "source_diff")?,
        source_diff_sha256: request.repository.source_diff_sha256.clone(),
        source_status: canonical_role_path(&repository_inputs, "source_status")?,
        source_status_sha256: request.repository.source_status_sha256.clone(),
        cargo_lock: canonical_role_path(&repository_inputs, "cargo_lock")?,
        cargo_lock_sha256: request.repository.cargo_lock_sha256.clone(),
        quic_source: canonical_role_path(&repository_inputs, "quic_source")?,
        quic_source_sha256: request.repository.quic_source_sha256.clone(),
        task_spec: canonical_role_path(&repository_inputs, "task_spec")?,
        task_spec_sha256: request.repository.task_spec_sha256.clone(),
    };
    Ok(ValidatedPreparation {
        plan_root,
        execution_root,
        blocked_evidence,
        repository,
        baseline,
        candidate,
        driver_identity,
        repository_inputs,
        environment,
        child_working_directory,
    })
}

fn materialize_plan_root(validated: ValidatedPreparation) -> Result<PrepareOutcome, PrepareError> {
    let mut repository = validated.repository;
    let plan_root_path = validated.plan_root.absent().canonical_path.clone();
    let plan_root = validated.plan_root.create_root()?;
    plan_root.create_dir(Path::new("artifacts"))?;
    let artifacts_path = plan_root_path.join("artifacts");
    let baseline = materialize_executable_copy(&plan_root, "baseline", validated.baseline)?;
    let candidate = materialize_executable_copy(&plan_root, "candidate", validated.candidate)?;
    let repository_inputs =
        materialize_repository_copies(&plan_root, &mut repository, validated.repository_inputs)?;
    let mut copied_identities = vec![
        &baseline.executable_identity,
        &baseline.source_identity,
        &candidate.executable_identity,
        &candidate.source_identity,
    ];
    copied_identities.extend(repository_inputs.iter().map(|entry| &entry.identity));
    ensure_distinct_identities(&artifacts_path, &copied_identities)?;

    let environment_relative = Path::new("environment.json");
    plan_root.write_json_create_new(environment_relative, &validated.environment)?;
    set_plan_file_mode(&plan_root, environment_relative, 0o444)?;
    let environment_identity = plan_root.stable_file_identity(environment_relative)?;
    let identities = PreparedFileIdentities {
        driver: validated.driver_identity.clone(),
        environment_receipt: environment_identity.clone(),
        baseline: baseline.executable_identity.clone(),
        candidate: candidate.executable_identity.clone(),
        repository_inputs,
    };
    let plan = Plan {
        schema: PLAN_SCHEMA.to_owned(),
        driver: DriverIdentity {
            path: validated.driver_identity.canonical_path,
            sha256: validated.driver_identity.sha256,
            device: validated.driver_identity.device,
            inode: validated.driver_identity.inode,
        },
        preflight: PreflightIdentity {
            environment_path: environment_identity.canonical_path,
            environment_sha256: environment_identity.sha256,
        },
        repository,
        baseline: baseline.executable,
        candidate: candidate.executable,
        harness: HarnessContract::fixed(),
        blocked_evidence: validated.blocked_evidence,
        evidence_root: validated.execution_root.canonical_path,
    };
    let identities_relative = Path::new("input-identities.json");
    plan_root.write_json_create_new(identities_relative, &identities)?;
    set_plan_file_mode(&plan_root, identities_relative, 0o444)?;
    let plan_path = plan_root_path.join("plan.json");
    let plan_seal_path = plan_root_path.join("plan.sha256");
    let pass = PreparePassReceipt {
        schema: "distd.quic.fetch.prepare-pass.v1".to_owned(),
        status: TerminalStatus::Ready,
        plan_root: plan_root_path.clone(),
        plan_path: plan_path.clone(),
        plan_seal_path: plan_seal_path.clone(),
        execution_evidence_root: plan.evidence_root.clone(),
        execution_evidence_root_absent: true,
        child_working_directory: validated.child_working_directory,
    };
    let receipt_relative = Path::new("prepare-receipt.json");
    plan_root.write_json_create_new(receipt_relative, &pass)?;
    set_plan_file_mode(&plan_root, receipt_relative, 0o444)?;

    let plan_bytes = canonical_json_line(&plan)?;
    let plan_sha256 = evidence::sha256_bytes(&plan_bytes);
    plan_root.atomic_create_new_synced(Path::new("plan.json"), &plan_bytes)?;
    set_plan_file_mode(&plan_root, Path::new("plan.json"), 0o444)?;
    let seal = format!("{plan_sha256}  plan.json\n");
    // The plan seal is deliberately the final preparation artifact.
    plan_root.atomic_create_new_synced(Path::new("plan.sha256"), seal.as_bytes())?;
    set_plan_file_mode(&plan_root, Path::new("plan.sha256"), 0o444)?;
    plan_root.set_directory_mode(Path::new("artifacts"), 0o555)?;
    plan_root.set_mode(0o555)?;
    plan_root.sync()?;
    require_published_file_matches_pinned(&plan_root, Path::new("plan.json"))?;
    debug_assert!(!plan.evidence_root.exists());

    Ok(PrepareOutcome::Prepared {
        plan,
        identities,
        plan_sha256,
        plan_root: plan_root_path,
    })
}

struct MaterializedExecutable {
    executable: ExecutableIdentity,
    executable_identity: FileIdentity,
    source_identity: FileIdentity,
}

fn materialize_repository_copies(
    plan_root: &RootHandle,
    repository: &mut RepositoryIdentity,
    inputs: Vec<ValidatedRepositoryInput>,
) -> Result<Vec<NamedFileIdentity>, PrepareError> {
    let mut materialized = Vec::with_capacity(inputs.len());
    for input in inputs {
        let leaf = match input.role.as_str() {
            "source_diff" => "repository-source.snapshot",
            "source_status" => "repository-status.txt",
            "cargo_lock" => "Cargo.lock",
            "quic_source" => "quic-source.rs",
            "task_spec" => "task-spec.md",
            _ => {
                return Err(materialize_error(
                    plan_root.path(),
                    format!("unknown repository input role {}", input.role),
                ));
            }
        };
        let relative = PathBuf::from("artifacts").join(leaf);
        plan_root.atomic_create_new_synced(&relative, &input.bytes)?;
        set_plan_file_mode(plan_root, &relative, 0o444)?;
        let identity = plan_root.stable_file_identity(&relative)?;
        if identity.sha256 != input.identity.sha256
            || identity.mode & 0o7777 != 0o444
            || identity.same_file(&input.identity)
            || materialized
                .iter()
                .any(|entry: &NamedFileIdentity| identity.same_file(&entry.identity))
        {
            return Err(materialize_error(
                &plan_root.path().join(&relative),
                format!(
                    "materialized repository input {} failed hash, mode, or inode validation",
                    input.role
                ),
            ));
        }
        match input.role.as_str() {
            "source_diff" => repository.source_diff.clone_from(&identity.canonical_path),
            "source_status" => repository
                .source_status
                .clone_from(&identity.canonical_path),
            "cargo_lock" => repository.cargo_lock.clone_from(&identity.canonical_path),
            "quic_source" => repository.quic_source.clone_from(&identity.canonical_path),
            "task_spec" => repository.task_spec.clone_from(&identity.canonical_path),
            _ => unreachable!("repository input role was validated above"),
        }
        materialized.push(NamedFileIdentity {
            role: input.role,
            identity,
        });
    }
    materialized.sort_by(|left, right| left.role.cmp(&right.role));
    Ok(materialized)
}

fn materialize_executable_copy(
    plan_root: &RootHandle,
    role: &str,
    validated: ValidatedExecutable,
) -> Result<MaterializedExecutable, PrepareError> {
    let executable_relative = PathBuf::from("artifacts").join(role);
    let source_identity_relative =
        PathBuf::from("artifacts").join(format!("{role}.source-identity"));
    plan_root.atomic_create_new_synced(&executable_relative, &validated.executable_bytes)?;
    set_plan_file_mode(plan_root, &executable_relative, 0o555)?;
    plan_root
        .atomic_create_new_synced(&source_identity_relative, &validated.source_identity_bytes)?;
    set_plan_file_mode(plan_root, &source_identity_relative, 0o444)?;

    let executable_identity = plan_root.stable_file_identity(&executable_relative)?;
    let source_identity = plan_root.stable_file_identity(&source_identity_relative)?;
    if executable_identity.sha256 != validated.executable.sha256
        || source_identity.sha256 != validated.executable.source_identity_sha256
    {
        return Err(materialize_error(
            plan_root.path(),
            format!("materialized {role} bytes do not match their validated hashes"),
        ));
    }
    if executable_identity.same_file(&validated.executable_identity)
        || source_identity.same_file(&validated.source_identity)
        || executable_identity.same_file(&source_identity)
    {
        return Err(materialize_error(
            plan_root.path(),
            format!("materialized {role} inputs do not have distinct inodes"),
        ));
    }
    if executable_identity.mode & 0o7777 != 0o555 || source_identity.mode & 0o7777 != 0o444 {
        return Err(materialize_error(
            plan_root.path(),
            format!("materialized {role} inputs have unexpected permissions"),
        ));
    }

    Ok(MaterializedExecutable {
        executable: ExecutableIdentity {
            path: executable_identity.canonical_path.clone(),
            sha256: validated.executable.sha256,
            build_identity: validated.executable.build_identity,
            source_identity_path: source_identity.canonical_path.clone(),
            source_identity_sha256: validated.executable.source_identity_sha256,
        },
        executable_identity,
        source_identity,
    })
}

fn ensure_distinct_identities(
    path: &Path,
    identities: &[&FileIdentity],
) -> Result<(), PrepareError> {
    for (index, identity) in identities.iter().enumerate() {
        if identities[index + 1..]
            .iter()
            .any(|other| identity.same_file(other))
        {
            return Err(materialize_error(
                path,
                "materialized plan inputs do not all have distinct inodes",
            ));
        }
    }
    Ok(())
}

fn set_plan_file_mode(root: &RootHandle, relative: &Path, mode: u32) -> Result<(), PrepareError> {
    let (_, file) = root.open_stable_file(relative)?;
    let display_path = root.path().join(relative);
    file.set_permissions(fs::Permissions::from_mode(mode))
        .map_err(|source| PrepareError::MaterializeInput {
            path: display_path.clone(),
            source,
        })?;
    file.sync_all()
        .map_err(|source| PrepareError::MaterializeInput {
            path: display_path,
            source,
        })?;
    root.sync()?;
    Ok(())
}

fn require_published_file_matches_pinned(
    root: &RootHandle,
    relative: &Path,
) -> Result<(), PrepareError> {
    let pinned = root.stable_file_identity(relative)?;
    let published_path = root.path().join(relative);
    let published = evidence::stable_file_identity(&published_path)?;
    if !pinned.same_file(&published) || pinned.sha256 != published.sha256 {
        return Err(materialize_error(
            &published_path,
            "published plan path no longer names the descriptor-pinned tree",
        ));
    }
    Ok(())
}

fn materialize_error(path: &Path, detail: impl Into<String>) -> PrepareError {
    PrepareError::MaterializeInput {
        path: path.to_path_buf(),
        source: std::io::Error::other(detail.into()),
    }
}

fn validate_repository_text_identity(
    repository: &RepositoryIdentity,
) -> Result<(), PreflightIssue> {
    if repository.head.len() != 40 || !repository.head.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(PreflightIssue::new(
            PreflightBlockCode::InvalidExpectedHash,
            "repository HEAD must be a 40-character hexadecimal object ID",
        ));
    }
    for hash in [
        &repository.source_diff_sha256,
        &repository.source_status_sha256,
        &repository.cargo_lock_sha256,
        &repository.quic_source_sha256,
        &repository.task_spec_sha256,
    ] {
        validate_sha256(hash)?;
    }
    Ok(())
}

fn validate_repository_files(
    repository: &RepositoryIdentity,
    repository_root: &Path,
) -> Result<Vec<ValidatedRepositoryInput>, PreflightIssue> {
    let inputs = [
        (
            "source_diff",
            &repository.source_diff,
            &repository.source_diff_sha256,
        ),
        (
            "source_status",
            &repository.source_status,
            &repository.source_status_sha256,
        ),
        (
            "cargo_lock",
            &repository.cargo_lock,
            &repository.cargo_lock_sha256,
        ),
        (
            "quic_source",
            &repository.quic_source,
            &repository.quic_source_sha256,
        ),
        (
            "task_spec",
            &repository.task_spec,
            &repository.task_spec_sha256,
        ),
    ];
    let mut identities = Vec::with_capacity(inputs.len());
    for (role, path, expected) in inputs {
        let (identity, bytes) = checked_regular_file_bytes(path)?;
        let must_be_in_repository = matches!(role, "cargo_lock" | "quic_source" | "task_spec");
        if must_be_in_repository && !identity.canonical_path.starts_with(repository_root) {
            return Err(PreflightIssue::new(
                PreflightBlockCode::SourceOutsideRepository,
                format!("{role} is outside the canonical repository root"),
            ));
        }
        require_hash(role, expected, &identity)?;
        identities.push(ValidatedRepositoryInput {
            role: role.to_owned(),
            identity,
            bytes,
        });
    }
    identities.sort_by(|left, right| left.role.cmp(&right.role));
    Ok(identities)
}

fn canonical_role_path(
    identities: &[ValidatedRepositoryInput],
    role: &str,
) -> Result<PathBuf, PreflightIssue> {
    identities
        .iter()
        .find(|entry| entry.role == role)
        .map(|entry| entry.identity.canonical_path.clone())
        .ok_or_else(|| {
            PreflightIssue::new(
                PreflightBlockCode::InvalidPath,
                format!("missing canonical identity for {role}"),
            )
        })
}

fn validate_executable(
    executable: &ExecutableIdentity,
    role: &str,
) -> Result<ValidatedExecutable, PreflightIssue> {
    validate_sha256(&executable.sha256)?;
    validate_sha256(&executable.source_identity_sha256)?;
    if executable.build_identity.is_empty() {
        return Err(PreflightIssue::new(
            PreflightBlockCode::InvalidPath,
            format!("{role} build identity is empty"),
        ));
    }
    let (identity, executable_bytes) = checked_regular_file_bytes(&executable.path)?;
    let (source_identity, source_identity_bytes) =
        checked_regular_file_bytes(&executable.source_identity_path)?;
    require_hash(
        &format!("{role} source identity"),
        &executable.source_identity_sha256,
        &source_identity,
    )?;
    require_executable(role, &identity)?;
    require_hash(role, &executable.sha256, &identity)?;
    let canonical = ExecutableIdentity {
        path: identity.canonical_path.clone(),
        sha256: executable.sha256.clone(),
        build_identity: executable.build_identity.clone(),
        source_identity_path: source_identity.canonical_path.clone(),
        source_identity_sha256: executable.source_identity_sha256.clone(),
    };
    Ok(ValidatedExecutable {
        executable: canonical,
        executable_identity: identity,
        executable_bytes,
        source_identity,
        source_identity_bytes,
    })
}

fn canonical_directory(path: &Path, role: &str) -> Result<PathBuf, PreflightIssue> {
    require_absolute(path)?;
    let metadata = fs::symlink_metadata(path).map_err(|error| {
        PreflightIssue::new(
            PreflightBlockCode::InvalidPath,
            format!("cannot inspect {role}: {error}"),
        )
    })?;
    if metadata.file_type().is_symlink() {
        return Err(PreflightIssue::new(
            PreflightBlockCode::InputSymlink,
            format!("{role} is a symlink"),
        ));
    }
    if !metadata.is_dir() {
        return Err(PreflightIssue::new(
            PreflightBlockCode::InvalidPath,
            format!("{role} is not a directory"),
        ));
    }
    fs::canonicalize(path).map_err(|error| {
        PreflightIssue::new(
            PreflightBlockCode::InvalidPath,
            format!("cannot canonicalize {role}: {error}"),
        )
    })
}

fn checked_regular_file(path: &Path) -> Result<FileIdentity, PreflightIssue> {
    checked_regular_file_bytes(path).map(|(identity, _)| identity)
}

fn checked_regular_file_bytes(path: &Path) -> Result<(FileIdentity, Vec<u8>), PreflightIssue> {
    require_absolute(path)?;
    let metadata = fs::symlink_metadata(path).map_err(|error| {
        PreflightIssue::new(
            PreflightBlockCode::InputNotRegular,
            format!("cannot inspect input {}: {error}", path.display()),
        )
    })?;
    if metadata.file_type().is_symlink() {
        return Err(PreflightIssue::new(
            PreflightBlockCode::InputSymlink,
            format!("input is a symlink: {}", path.display()),
        ));
    }
    if !metadata.is_file() {
        return Err(PreflightIssue::new(
            PreflightBlockCode::InputNotRegular,
            format!("input is not a regular file: {}", path.display()),
        ));
    }
    evidence::stable_file_bytes(path).map_err(|error| {
        let code = if matches!(error, EvidenceError::FileChanged(_)) {
            PreflightBlockCode::InputChanged
        } else {
            PreflightBlockCode::InputNotRegular
        };
        PreflightIssue::new(code, error.to_string())
    })
}

fn require_absolute(path: &Path) -> Result<(), PreflightIssue> {
    if path.is_absolute() {
        Ok(())
    } else {
        Err(PreflightIssue::new(
            PreflightBlockCode::InvalidPath,
            format!("path must be absolute: {}", path.display()),
        ))
    }
}

fn require_executable(role: &str, identity: &FileIdentity) -> Result<(), PreflightIssue> {
    if identity.mode & 0o111 == 0 {
        return Err(PreflightIssue::new(
            PreflightBlockCode::InputNotExecutable,
            format!("{role} is not executable"),
        ));
    }
    Ok(())
}

fn validate_sha256(hash: &str) -> Result<(), PreflightIssue> {
    if hash.len() == 64
        && hash
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        Ok(())
    } else {
        Err(PreflightIssue::new(
            PreflightBlockCode::InvalidExpectedHash,
            "expected SHA-256 must be exactly 64 lowercase hexadecimal characters",
        ))
    }
}

fn require_hash(role: &str, expected: &str, identity: &FileIdentity) -> Result<(), PreflightIssue> {
    if identity.sha256 == expected {
        Ok(())
    } else {
        Err(PreflightIssue::new(
            PreflightBlockCode::HashMismatch,
            format!(
                "{role} SHA-256 mismatch: expected {expected}, actual {}",
                identity.sha256
            ),
        ))
    }
}

fn map_root_error(error: &EvidenceError) -> PreflightIssue {
    let code = if matches!(error, EvidenceError::RootExists(_)) {
        PreflightBlockCode::EvidenceRootExists
    } else {
        PreflightBlockCode::InvalidPath
    };
    PreflightIssue::new(code, error.to_string())
}

#[cfg(test)]
mod tests {
    use super::super::model::{ExecutableIdentity, RepositoryIdentity};
    use super::*;
    use crate::quic_fetch_acceptance::environment::{
        EnvironmentProbeError, LoopbackFacts, OsFacts, PowerFacts,
    };
    use std::os::unix::fs::{symlink, PermissionsExt};
    use tempfile::TempDir;

    struct Fixture {
        temporary: TempDir,
        request: Request,
        paths: PreparePaths,
    }

    impl Fixture {
        fn new() -> Self {
            let temporary = tempfile::tempdir().unwrap();
            let root = fs::canonicalize(temporary.path()).unwrap();
            let repository_root = root.join("repository");
            fs::create_dir(&repository_root).unwrap();
            let source_diff = write_file(&repository_root.join("diff"), b"diff");
            let source_status = write_file(&repository_root.join("status"), b"status");
            let cargo_lock = write_file(&repository_root.join("Cargo.lock"), b"lock");
            let quic_source = write_file(&repository_root.join("quic.rs"), b"quic");
            let task_spec = write_file(&repository_root.join("task.md"), b"task");
            let baseline = write_executable(&root.join("baseline"), b"baseline");
            let candidate = write_executable(&root.join("candidate"), b"candidate");
            let driver = write_executable(&root.join("driver"), b"driver");
            let baseline_source_identity = repository_root.join("baseline-source.txt");
            let candidate_source_identity = repository_root.join("candidate-source.txt");
            fs::write(&baseline_source_identity, b"baseline source").unwrap();
            fs::write(&candidate_source_identity, b"candidate source").unwrap();

            let request = Request {
                schema: REQUEST_SCHEMA.to_owned(),
                repository: RepositoryIdentity {
                    root: repository_root.clone(),
                    head: "a".repeat(40),
                    source_diff: source_diff.clone(),
                    source_diff_sha256: evidence::sha256_file(&source_diff).unwrap(),
                    source_status: source_status.clone(),
                    source_status_sha256: evidence::sha256_file(&source_status).unwrap(),
                    cargo_lock: cargo_lock.clone(),
                    cargo_lock_sha256: evidence::sha256_file(&cargo_lock).unwrap(),
                    quic_source: quic_source.clone(),
                    quic_source_sha256: evidence::sha256_file(&quic_source).unwrap(),
                    task_spec: task_spec.clone(),
                    task_spec_sha256: evidence::sha256_file(&task_spec).unwrap(),
                },
                baseline: ExecutableIdentity {
                    path: baseline.clone(),
                    sha256: evidence::sha256_file(&baseline).unwrap(),
                    build_identity: "baseline-build".to_owned(),
                    source_identity_path: baseline_source_identity.clone(),
                    source_identity_sha256: evidence::sha256_file(&baseline_source_identity)
                        .unwrap(),
                },
                candidate: ExecutableIdentity {
                    path: candidate.clone(),
                    sha256: evidence::sha256_file(&candidate).unwrap(),
                    build_identity: "candidate-build".to_owned(),
                    source_identity_path: candidate_source_identity.clone(),
                    source_identity_sha256: evidence::sha256_file(&candidate_source_identity)
                        .unwrap(),
                },
                harness: HarnessContract::fixed(),
                blocked_evidence: root.join("BLOCKED.json"),
                evidence_root: root.join("execution-evidence"),
            };
            let paths = PreparePaths {
                child_working_directory: repository_root,
                plan_root: root.join("prepared-plan"),
                blocked_receipt_path: root.join("BLOCKED.json"),
                driver_executable: driver,
                environment_expectations: EnvironmentExpectations {
                    product_version: Some("fixture-os".to_owned()),
                    kernel_identity: Some("fixture-kernel".to_owned()),
                    boot_record: Some("fixture-boot".to_owned()),
                },
            };
            Self {
                temporary,
                request,
                paths,
            }
        }
    }

    struct PassingEnvironment;

    impl EnvironmentProbe for PassingEnvironment {
        fn observe_os(&self) -> Result<OsFacts, EnvironmentProbeError> {
            Ok(OsFacts {
                product_version: "fixture-os".to_owned(),
                kernel_identity: "fixture-kernel".to_owned(),
                boot_record: "fixture-boot".to_owned(),
            })
        }

        fn observe_power(&self) -> Result<PowerFacts, EnvironmentProbeError> {
            Ok(PowerFacts {
                ac_power_active: true,
                low_power_mode: false,
                thermal_state: 0,
            })
        }

        fn bind_close_ipv4_loopback(&self) -> Result<LoopbackFacts, EnvironmentProbeError> {
            Ok(LoopbackFacts {
                address: "127.0.0.1".to_owned(),
                port: 42_424,
                closed_before_return: true,
            })
        }
    }

    struct FixtureRepository;

    impl RepositoryProbe for FixtureRepository {
        fn observe(
            &self,
            _canonical_root: &Path,
        ) -> Result<LiveRepositoryState, RepositoryVerificationError> {
            Ok(LiveRepositoryState {
                head: "a".repeat(40),
                source_diff: b"diff".to_vec(),
                source_status: b"status".to_vec(),
            })
        }
    }

    struct PanicProbes;

    impl EnvironmentProbe for PanicProbes {
        fn observe_os(&self) -> Result<OsFacts, EnvironmentProbeError> {
            panic!("unsafe output paths must fail before environment observation")
        }

        fn observe_power(&self) -> Result<PowerFacts, EnvironmentProbeError> {
            panic!("unsafe output paths must fail before environment observation")
        }

        fn bind_close_ipv4_loopback(&self) -> Result<LoopbackFacts, EnvironmentProbeError> {
            panic!("unsafe output paths must fail before environment observation")
        }
    }

    impl RepositoryProbe for PanicProbes {
        fn observe(
            &self,
            _canonical_root: &Path,
        ) -> Result<LiveRepositoryState, RepositoryVerificationError> {
            panic!("unsafe output paths must fail before repository observation")
        }
    }

    fn prepare_fixture(fixture: &Fixture) -> Result<PrepareOutcome, PrepareError> {
        prepare_with_probes(
            &fixture.request,
            &fixture.paths,
            &PassingEnvironment,
            &FixtureRepository,
        )
    }

    fn reject_unsafe_fixture(fixture: &Fixture) -> Result<PrepareOutcome, PrepareError> {
        prepare_with_probes(&fixture.request, &fixture.paths, &PanicProbes, &PanicProbes)
    }

    fn write_file(path: &Path, bytes: &[u8]) -> PathBuf {
        fs::write(path, bytes).unwrap();
        path.to_path_buf()
    }

    fn write_executable(path: &Path, bytes: &[u8]) -> PathBuf {
        fs::write(path, bytes).unwrap();
        let mut permissions = fs::metadata(path).unwrap().permissions();
        permissions.set_mode(0o700);
        fs::set_permissions(path, permissions).unwrap();
        path.to_path_buf()
    }

    #[test]
    fn prepare_emits_shared_fixed_plan_and_keeps_execution_root_absent() {
        let fixture = Fixture::new();
        let outcome = prepare_fixture(&fixture).unwrap();
        let PrepareOutcome::Prepared {
            plan,
            plan_sha256,
            plan_root,
            ..
        } = outcome
        else {
            panic!("expected prepared outcome");
        };
        assert_eq!(plan.schema, PLAN_SCHEMA);
        assert_eq!(plan.harness, HarnessContract::fixed());
        assert_eq!(
            plan.harness.assignments[1].run_id, "warmup-C",
            "shared model run IDs are authoritative"
        );
        assert!(!plan.evidence_root.exists());
        assert_eq!(plan.baseline.path, plan_root.join("artifacts/baseline"));
        assert_eq!(
            plan.candidate.source_identity_path,
            plan_root.join("artifacts/candidate.source-identity")
        );
        assert_eq!(
            plan.repository.source_diff,
            plan_root.join("artifacts/repository-source.snapshot")
        );
        for repository_input in [
            &plan.repository.source_diff,
            &plan.repository.source_status,
            &plan.repository.cargo_lock,
            &plan.repository.quic_source,
            &plan.repository.task_spec,
        ] {
            assert!(repository_input.starts_with(plan_root.join("artifacts")));
            assert_eq!(
                fs::metadata(repository_input).unwrap().permissions().mode() & 0o7777,
                0o444
            );
        }
        assert_eq!(
            fs::metadata(&plan.baseline.path)
                .unwrap()
                .permissions()
                .mode()
                & 0o7777,
            0o555
        );
        assert_eq!(
            fs::metadata(&plan.repository.cargo_lock)
                .unwrap()
                .permissions()
                .mode()
                & 0o7777,
            0o444
        );
        assert_eq!(
            fs::metadata(&plan_root).unwrap().permissions().mode() & 0o7777,
            0o555
        );
        assert_eq!(
            fs::read_to_string(plan_root.join("plan.sha256")).unwrap(),
            format!("{plan_sha256}  plan.json\n")
        );
        assert!(!fixture.paths.blocked_receipt_path.exists());
        assert!(fixture.temporary.path().exists());
    }

    #[test]
    fn hash_drift_is_blocked_without_plan_or_execution_root() {
        let mut fixture = Fixture::new();
        fixture.request.candidate.sha256 = "0".repeat(64);
        let outcome = prepare_fixture(&fixture).unwrap();
        assert!(matches!(
            outcome,
            PrepareOutcome::Blocked(BlockedReceipt {
                code: PreflightBlockCode::HashMismatch,
                product_invocations: 0,
                ..
            })
        ));
        assert!(!fixture.paths.plan_root.exists());
        assert!(!fixture.request.evidence_root.exists());
        assert!(fixture.paths.blocked_receipt_path.is_file());
    }

    #[test]
    fn plan_root_beneath_repository_is_blocked_without_creating_output_roots() {
        let mut fixture = Fixture::new();
        let repository_alias = fixture.temporary.path().join("repository-alias");
        symlink(&fixture.request.repository.root, &repository_alias).unwrap();
        fixture.paths.plan_root = repository_alias.join("prepared-plan");

        let outcome = reject_unsafe_fixture(&fixture).unwrap();
        assert!(matches!(
            outcome,
            PrepareOutcome::Blocked(BlockedReceipt {
                code: PreflightBlockCode::InvalidPath,
                product_invocations: 0,
                ref detail,
                ..
            }) if detail == "plan root must be outside the canonical repository root"
        ));
        assert!(!fixture.paths.plan_root.exists());
        assert!(!fixture
            .request
            .repository
            .root
            .join("prepared-plan")
            .exists());
        assert!(!fixture.request.evidence_root.exists());
        assert!(fixture.paths.blocked_receipt_path.is_file());
    }

    #[test]
    fn execution_root_beneath_repository_is_blocked_without_creating_output_roots() {
        let mut fixture = Fixture::new();
        fixture.request.evidence_root = fixture.request.repository.root.join("execution-evidence");

        let outcome = reject_unsafe_fixture(&fixture).unwrap();
        assert!(matches!(
            outcome,
            PrepareOutcome::Blocked(BlockedReceipt {
                code: PreflightBlockCode::InvalidPath,
                product_invocations: 0,
                ref detail,
                ..
            }) if detail == "execution evidence root must be outside the canonical repository root"
        ));
        assert!(!fixture.paths.plan_root.exists());
        assert!(!fixture.request.evidence_root.exists());
        assert!(fixture.paths.blocked_receipt_path.is_file());
    }

    #[test]
    fn blocked_evidence_beneath_repository_fails_without_writing_its_rejection() {
        let mut fixture = Fixture::new();
        let unsafe_path = fixture.request.repository.root.join("BLOCKED.json");
        fixture.request.blocked_evidence = unsafe_path.clone();
        fixture.paths.blocked_receipt_path = unsafe_path.clone();

        let error = reject_unsafe_fixture(&fixture).unwrap_err();
        assert!(matches!(
            error,
            PrepareError::UnsafeBlockedEvidence {
                ref path,
                ref repository_root,
            } if path == &unsafe_path && repository_root == &fixture.request.repository.root
        ));
        assert!(!fixture.paths.plan_root.exists());
        assert!(!fixture.request.evidence_root.exists());
        assert!(!unsafe_path.exists());
    }

    #[test]
    fn symlink_and_same_file_inputs_are_blocked() {
        let mut symlink_fixture = Fixture::new();
        let linked = symlink_fixture.temporary.path().join("candidate-link");
        symlink(&symlink_fixture.request.candidate.path, &linked).unwrap();
        symlink_fixture.request.candidate.path = linked;
        assert!(matches!(
            prepare_fixture(&symlink_fixture).unwrap(),
            PrepareOutcome::Blocked(BlockedReceipt {
                code: PreflightBlockCode::InputSymlink,
                product_invocations: 0,
                ..
            })
        ));
        assert!(!symlink_fixture.request.evidence_root.exists());

        let mut same_fixture = Fixture::new();
        same_fixture.request.candidate.path = same_fixture.request.baseline.path.clone();
        same_fixture.request.candidate.sha256 = same_fixture.request.baseline.sha256.clone();
        assert!(matches!(
            prepare_fixture(&same_fixture).unwrap(),
            PrepareOutcome::Blocked(BlockedReceipt {
                code: PreflightBlockCode::SameExecutable,
                product_invocations: 0,
                ..
            })
        ));
        assert!(!same_fixture.request.evidence_root.exists());
    }

    #[test]
    fn absolute_paths_make_preparation_independent_of_process_cwd() {
        let mut fixture = Fixture::new();
        fixture.paths.child_working_directory = PathBuf::from("repository");
        assert!(matches!(
            prepare_fixture(&fixture).unwrap(),
            PrepareOutcome::Blocked(BlockedReceipt {
                code: PreflightBlockCode::InvalidPath,
                product_invocations: 0,
                ..
            })
        ));
        assert!(!fixture.request.evidence_root.exists());
    }

    #[test]
    fn blocked_receipt_uses_the_originally_pinned_absent_parent() {
        struct ReplacingRepositoryProbe {
            original_parent: PathBuf,
            relocated_parent: PathBuf,
        }

        impl RepositoryProbe for ReplacingRepositoryProbe {
            fn observe(
                &self,
                _canonical_root: &Path,
            ) -> Result<LiveRepositoryState, RepositoryVerificationError> {
                fs::rename(&self.original_parent, &self.relocated_parent).unwrap();
                fs::create_dir(&self.original_parent).unwrap();
                Err(RepositoryVerificationError::new("fixture rejection"))
            }
        }

        let mut fixture = Fixture::new();
        let original_parent = fixture.temporary.path().join("blocked-parent");
        let relocated_parent = fixture.temporary.path().join("relocated-blocked-parent");
        fs::create_dir(&original_parent).unwrap();
        fixture.request.blocked_evidence = original_parent.join("BLOCKED.json");
        fixture.paths.blocked_receipt_path = fixture.request.blocked_evidence.clone();
        let probe = ReplacingRepositoryProbe {
            original_parent: original_parent.clone(),
            relocated_parent: relocated_parent.clone(),
        };

        let outcome = prepare_with_probes(
            &fixture.request,
            &fixture.paths,
            &PassingEnvironment,
            &probe,
        )
        .unwrap();
        assert!(matches!(
            outcome,
            PrepareOutcome::Blocked(BlockedReceipt {
                code: PreflightBlockCode::RepositoryState,
                product_invocations: 0,
                ..
            })
        ));
        assert!(!original_parent.join("BLOCKED.json").exists());
        let published = relocated_parent.join("BLOCKED.json");
        let receipt: BlockedReceipt =
            serde_json::from_slice(&fs::read(&published).unwrap()).unwrap();
        assert_eq!(receipt.product_invocations, 0);
    }

    #[test]
    fn git_probe_clears_command_local_redirection_and_config_environment() {
        let temporary = tempfile::tempdir().unwrap();
        let requested = temporary.path().join("requested");
        let redirected = temporary.path().join("redirected");
        fs::create_dir(&requested).unwrap();
        fs::create_dir(&redirected).unwrap();
        git(&requested, &["init", "--quiet"]);
        git(&redirected, &["init", "--quiet"]);
        let requested = fs::canonicalize(requested).unwrap();
        let redirected = fs::canonicalize(redirected).unwrap();

        let mut command = Command::new("/usr/bin/git");
        command
            .env("GIT_DIR", redirected.join(".git"))
            .env("GIT_WORK_TREE", &redirected)
            .env("GIT_COMMON_DIR", redirected.join(".git"))
            .env("GIT_CONFIG_COUNT", "1")
            .env("GIT_CONFIG_KEY_0", "core.repositoryformatversion")
            .env("GIT_CONFIG_VALUE_0", "999");
        let output =
            git_output_with_command(&requested, &["rev-parse", "--show-toplevel"], command)
                .unwrap();

        assert_eq!(
            parse_git_path_line(&output, "fixture root").unwrap(),
            requested
        );
    }

    #[test]
    fn system_probe_rejects_a_requested_subdirectory_of_the_worktree_root() {
        let temporary = tempfile::tempdir().unwrap();
        let repository = temporary.path().join("git-repository");
        let nested = repository.join("nested");
        fs::create_dir_all(&nested).unwrap();
        git(&repository, &["init", "--quiet"]);
        let nested = fs::canonicalize(nested).unwrap();

        let error = SystemRepositoryProbe.observe(&nested).unwrap_err();
        assert!(error
            .detail
            .contains("differs from the canonical requested root"));
    }

    #[test]
    fn system_snapshot_binds_staged_and_untracked_content_bytes() {
        let temporary = tempfile::tempdir().unwrap();
        let repository = temporary.path().join("git-repository");
        fs::create_dir(&repository).unwrap();
        git(&repository, &["init", "--quiet"]);
        git(&repository, &["config", "user.name", "Fixture"]);
        git(
            &repository,
            &["config", "user.email", "fixture@example.invalid"],
        );
        fs::write(repository.join("tracked"), b"committed\n").unwrap();
        git(&repository, &["add", "tracked"]);
        git(&repository, &["commit", "--quiet", "-m", "fixture"]);

        fs::write(repository.join("tracked"), b"staged-one\n").unwrap();
        git(&repository, &["add", "tracked"]);
        fs::write(repository.join("untracked"), b"untracked-one\n").unwrap();
        let repository = fs::canonicalize(repository).unwrap();
        let first = SystemRepositoryProbe.observe(&repository).unwrap();

        fs::write(repository.join("untracked"), b"untracked-two\n").unwrap();
        let untracked_drift = SystemRepositoryProbe.observe(&repository).unwrap();
        assert_eq!(first.source_status, untracked_drift.source_status);
        assert_ne!(first.source_diff, untracked_drift.source_diff);

        fs::write(repository.join("untracked"), b"untracked-one\n").unwrap();
        fs::write(repository.join("tracked"), b"staged-two\n").unwrap();
        git(&repository, &["add", "tracked"]);
        let staged_drift = SystemRepositoryProbe.observe(&repository).unwrap();
        assert_eq!(first.source_status, staged_drift.source_status);
        assert_ne!(first.source_diff, staged_drift.source_diff);
    }

    #[test]
    fn capture_repository_identity_emits_read_only_non_product_bundle() {
        let temporary = tempfile::tempdir().unwrap();
        let repository = temporary.path().join("git-repository");
        fs::create_dir_all(repository.join("server/src")).unwrap();
        fs::create_dir_all(repository.join(".specs/security")).unwrap();
        git(&repository, &["init", "--quiet"]);
        git(&repository, &["config", "user.name", "Fixture"]);
        git(
            &repository,
            &["config", "user.email", "fixture@example.invalid"],
        );
        fs::write(repository.join("Cargo.lock"), b"lock\n").unwrap();
        fs::write(repository.join("server/src/quic.rs"), b"quic\n").unwrap();
        let task_spec = repository.join(".specs/security/task.spec.md");
        fs::write(&task_spec, b"task\n").unwrap();
        git(&repository, &["add", "."]);
        git(&repository, &["commit", "--quiet", "-m", "fixture"]);
        fs::write(repository.join("untracked"), b"captured bytes\n").unwrap();

        let output = temporary.path().join("capture");
        let receipt = capture_repository_identity(&repository, &output, &task_spec).unwrap();

        assert_eq!(receipt.product_invocations, 0);
        assert_eq!(receipt.output_root, fs::canonicalize(&output).unwrap());
        assert_eq!(
            fs::metadata(&output).unwrap().permissions().mode() & 0o7777,
            0o555
        );
        for name in [
            "repository-source.snapshot",
            "repository-status.txt",
            "repository.json",
        ] {
            assert_eq!(
                fs::metadata(output.join(name))
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o7777,
                0o444
            );
        }
        let captured: RepositoryCaptureReceipt =
            serde_json::from_slice(&fs::read(output.join("repository.json")).unwrap()).unwrap();
        assert_eq!(captured, receipt);
        assert!(fs::read(&receipt.repository.source_diff)
            .unwrap()
            .windows(b"untracked".len())
            .any(|window| window == b"untracked"));
    }

    #[test]
    fn source_snapshot_rejects_untracked_symlinks() {
        let temporary = tempfile::tempdir().unwrap();
        let target = temporary.path().join("target");
        fs::write(&target, b"target").unwrap();
        symlink(&target, temporary.path().join("link")).unwrap();
        let error = canonical_source_snapshot(temporary.path(), b"", b"link\0").unwrap_err();
        assert!(error.detail.contains("untracked symlinks are forbidden"));
    }

    #[test]
    fn repository_probe_bounds_output_and_hung_descendants() {
        let tiny_limits = ProbeLimits {
            duration: Duration::from_secs(1),
            stdout_bytes: 4,
            stderr_bytes: 4,
        };
        let output_error = bounded_command_output(
            Command::new("/bin/sh").args(["-c", "printf 123456789"]),
            "oversized fixture",
            tiny_limits,
        )
        .unwrap_err();
        assert!(output_error.detail.contains("stdout limit"));

        let started = Instant::now();
        let timeout_error = bounded_command_output(
            Command::new("/bin/sh").args(["-c", "sleep 10"]),
            "hung fixture",
            ProbeLimits {
                duration: Duration::from_millis(50),
                stdout_bytes: 4,
                stderr_bytes: 4,
            },
        )
        .unwrap_err();
        assert!(timeout_error.detail.contains("execution-time limit"));
        assert!(started.elapsed() < Duration::from_secs(2));
    }

    fn git(repository: &Path, arguments: &[&str]) {
        let status = Command::new("/usr/bin/git")
            .arg("-C")
            .arg(repository)
            .args(arguments)
            .status()
            .unwrap();
        assert!(status.success(), "git {} failed", arguments.join(" "));
    }
}

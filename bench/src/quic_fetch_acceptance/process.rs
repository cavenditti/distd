#![cfg(unix)]

use std::fs::File;
use std::io::{self, Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::process::CommandExt;
use std::path::Path;
use std::process::{Child, ChildStderr, ChildStdout, Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

#[cfg(test)]
use std::sync::Mutex;

use thiserror::Error;

use super::evidence::{PinnedAbsentRoot, RootHandle};

static EXECUTION_GUARD_ACTIVE: AtomicBool = AtomicBool::new(false);
static ACTIVE_PROCESS_GROUP: AtomicI32 = AtomicI32::new(0);
static RECEIVED_SIGNAL: AtomicI32 = AtomicI32::new(0);
static POST_TERMINAL_SIGNAL: AtomicI32 = AtomicI32::new(0);
static EXECUTION_STATE: AtomicU8 = AtomicU8::new(EXECUTION_RUNNING);

const EXECUTION_RUNNING: u8 = 0;
const TERMINAL_COMMITTING: u8 = 1;
const TERMINAL_COMMITTED: u8 = 2;
const TERMINAL_COMMITTED_SIGNALED: u8 = 3;
const EXECUTION_INTERRUPTING: u8 = 4;
const EXECUTION_INTERRUPTED: u8 = 5;

const FORWARDED_SIGNALS: [i32; 4] = [libc::SIGHUP, libc::SIGINT, libc::SIGTERM, libc::SIGQUIT];
const WAIT_POLL_INTERVAL: Duration = Duration::from_millis(10);
const DRAIN_POLL_INTERVAL: Duration = Duration::from_millis(2);

#[cfg(test)]
pub(crate) static TEST_EXECUTION_LOCK: Mutex<()> = Mutex::new(());

#[derive(Clone, Copy, Debug)]
pub struct ProcessLimits {
    pub max_duration: Duration,
    pub max_stdout_bytes: u64,
    pub max_stderr_bytes: u64,
    pub signal_grace: Duration,
}

#[derive(Debug, Error)]
pub enum ProcessError {
    #[error("another execution signal guard is already active")]
    ConcurrentGuard,
    #[error("could not configure signal forwarding: {0}")]
    SignalSetup(#[source] io::Error),
    #[error("could not create child output {path}: {source}")]
    Output {
        path: String,
        #[source]
        source: io::Error,
    },
    #[error("could not spawn benchmark child: {0}")]
    Spawn(#[source] io::Error),
    #[error("could not record started child {pid}: {source}")]
    Started {
        pid: u32,
        #[source]
        source: io::Error,
    },
    #[error("could not wait for benchmark child {pid}: {source}")]
    Wait {
        pid: u32,
        #[source]
        source: io::Error,
    },
    #[error("could not drain child {stream}: {source}")]
    Drain {
        stream: &'static str,
        #[source]
        source: io::Error,
    },
    #[error("child {stream} drain thread panicked")]
    DrainPanic { stream: &'static str },
    #[error("benchmark child {pid} exceeded its {seconds}s runtime limit")]
    RuntimeLimit { pid: u32, seconds: u64 },
    #[error(
        "benchmark child {pid} exceeded an output limit (stdout={stdout_bytes}, stderr={stderr_bytes})"
    )]
    OutputLimit {
        pid: u32,
        stdout_bytes: u64,
        stderr_bytes: u64,
    },
    #[error("received signal {signal} while no child was active")]
    SignalBetweenChildren { signal: i32 },
    #[error("a terminal outcome has already been committed")]
    TerminalAlreadyCommitted,
    #[error("terminal commit failed: {0}")]
    Commit(#[source] io::Error),
    #[error("terminal commit failed after post-linearization signal {signal}: {source}")]
    CommitInterrupted {
        signal: i32,
        #[source]
        source: io::Error,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ChildTermination {
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ChildCleanupIssue {
    Setup {
        stage: &'static str,
        error_kind: io::ErrorKind,
        message: String,
    },
    Wait {
        operation: &'static str,
        error_kind: io::ErrorKind,
        message: String,
    },
    Drain {
        stream: &'static str,
        error_kind: io::ErrorKind,
        message: String,
    },
    DrainPanic {
        stream: &'static str,
    },
}

#[derive(Debug)]
pub struct ChildOutcome {
    pub pid: u32,
    pub status: Option<ExitStatus>,
    pub forwarded_signal: Option<i32>,
    pub termination: ChildTermination,
    pub stdout_bytes: u64,
    pub stderr_bytes: u64,
    pub stdout_observed_bytes: u64,
    pub stderr_observed_bytes: u64,
    pub cleanup_issues: Vec<ChildCleanupIssue>,
}

impl ChildOutcome {
    #[must_use]
    pub fn success(&self) -> bool {
        self.status.is_some_and(|status| status.success())
            && self.forwarded_signal.is_none()
            && self.termination == ChildTermination::Exited
            && self.cleanup_issues.is_empty()
    }
}

extern "C" fn forward_signal(signal: i32) {
    loop {
        let state = EXECUTION_STATE.load(Ordering::SeqCst);
        match state {
            EXECUTION_RUNNING | TERMINAL_COMMITTING => {
                if EXECUTION_STATE
                    .compare_exchange(
                        state,
                        EXECUTION_INTERRUPTING,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    )
                    .is_err()
                {
                    continue;
                }
                record_and_forward_signal(signal);
                EXECUTION_STATE.store(EXECUTION_INTERRUPTED, Ordering::SeqCst);
                return;
            }
            EXECUTION_INTERRUPTED => {
                record_and_forward_signal(signal);
                return;
            }
            TERMINAL_COMMITTED => {
                let _ = POST_TERMINAL_SIGNAL.compare_exchange(
                    0,
                    signal,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                );
                if EXECUTION_STATE
                    .compare_exchange(
                        TERMINAL_COMMITTED,
                        TERMINAL_COMMITTED_SIGNALED,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    )
                    .is_ok()
                {
                    return;
                }
            }
            TERMINAL_COMMITTED_SIGNALED => {
                let _ = POST_TERMINAL_SIGNAL.compare_exchange(
                    0,
                    signal,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                );
                return;
            }
            _ => return,
        }
    }
}

fn record_and_forward_signal(signal: i32) {
    let _ = RECEIVED_SIGNAL.compare_exchange(0, signal, Ordering::SeqCst, Ordering::SeqCst);
    let process_group = ACTIVE_PROCESS_GROUP.load(Ordering::SeqCst);
    if process_group > 0 {
        // SAFETY: `kill` is async-signal-safe, and a negative PID addresses the
        // child process group created immediately after spawn.
        unsafe {
            libc::kill(-process_group, signal);
        }
    }
}

/// Owns process-wide signal dispositions for one complete driver execution.
pub struct ExecutionSignalGuard {
    previous: Vec<(i32, libc::sigaction)>,
    finished: bool,
}

impl ExecutionSignalGuard {
    pub fn install() -> Result<Self, ProcessError> {
        if EXECUTION_GUARD_ACTIVE
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Err(ProcessError::ConcurrentGuard);
        }

        RECEIVED_SIGNAL.store(0, Ordering::SeqCst);
        POST_TERMINAL_SIGNAL.store(0, Ordering::SeqCst);
        ACTIVE_PROCESS_GROUP.store(0, Ordering::SeqCst);
        EXECUTION_STATE.store(EXECUTION_RUNNING, Ordering::SeqCst);
        let mut previous = Vec::with_capacity(FORWARDED_SIGNALS.len());

        for signal in FORWARDED_SIGNALS {
            // SAFETY: both actions are initialized before `sigaction`; the
            // handler calls only atomics and the async-signal-safe `kill`.
            unsafe {
                let mut action: libc::sigaction = std::mem::zeroed();
                action.sa_sigaction = forward_signal as *const () as usize;
                action.sa_flags = 0;
                libc::sigemptyset(&raw mut action.sa_mask);

                let mut old_action: libc::sigaction = std::mem::zeroed();
                if libc::sigaction(signal, &raw const action, &raw mut old_action) != 0 {
                    let error = io::Error::last_os_error();
                    for (installed_signal, installed_previous) in previous.into_iter().rev() {
                        libc::sigaction(
                            installed_signal,
                            &raw const installed_previous,
                            std::ptr::null_mut(),
                        );
                    }
                    EXECUTION_GUARD_ACTIVE.store(false, Ordering::SeqCst);
                    return Err(ProcessError::SignalSetup(error));
                }
                previous.push((signal, old_action));
            }
        }

        Ok(Self {
            previous,
            finished: false,
        })
    }

    #[must_use]
    pub fn received_signal(&self) -> Option<i32> {
        let signal = RECEIVED_SIGNAL.load(Ordering::SeqCst);
        (signal != 0).then_some(signal)
    }

    #[must_use]
    pub fn post_terminal_signal(&self) -> Option<i32> {
        let signal = POST_TERMINAL_SIGNAL.load(Ordering::SeqCst);
        (signal != 0).then_some(signal)
    }

    pub fn require_no_signal(&self) -> Result<(), ProcessError> {
        match self.received_signal() {
            Some(signal) if ACTIVE_PROCESS_GROUP.load(Ordering::SeqCst) == 0 => {
                Err(ProcessError::SignalBetweenChildren { signal })
            }
            _ => Ok(()),
        }
    }

    /// Restore managed signal dispositions without leaving a final check-to-drop race.
    ///
    /// Signals stay blocked while pending/handled state is sampled and the original handlers are
    /// restored. The caller can then re-raise the returned signal after this guard is consumed.
    pub fn finish(mut self) -> Result<Option<i32>, ProcessError> {
        let mask = SignalMaskGuard::block_managed()?;
        let signal = self
            .received_signal()
            .or(self.post_terminal_signal())
            .or(first_pending_managed_signal()?);
        ACTIVE_PROCESS_GROUP.store(0, Ordering::SeqCst);
        for (managed_signal, action) in self.previous.iter().rev() {
            // SAFETY: these are the exact actions returned during installation, and all managed
            // signals are blocked on this thread until after every action is restored.
            unsafe {
                libc::sigaction(*managed_signal, action, std::ptr::null_mut());
            }
        }
        EXECUTION_GUARD_ACTIVE.store(false, Ordering::SeqCst);
        self.finished = true;
        drop(mask);
        Ok(signal)
    }

    /// Atomically authorize a terminal create relative to the managed signal boundary.
    ///
    /// HUP/INT/TERM/QUIT are blocked on the calling thread while the state moves through
    /// `COMMITTING`. The `COMMITTING -> COMMITTED` CAS is the terminal authorization and
    /// linearization point and occurs immediately before the owned create-new operation. A handler
    /// that wins before that CAS records the
    /// interruption and prevents the closure from running. A handler that runs after it records
    /// neither a pre-terminal signal nor forwards it to a child. If create-new fails, the state
    /// rolls back only when no post-linearization signal crossed the tentative commit.
    pub fn commit(&self, path: &Path, bytes: &[u8]) -> Result<(), ProcessError> {
        self.commit_with_checkpoints(path, bytes, || {}, || {})
    }

    /// Authorize and publish a terminal through an already pinned evidence root.
    pub fn commit_root(
        &self,
        root: &RootHandle,
        relative: &Path,
        bytes: &[u8],
    ) -> Result<(), ProcessError> {
        self.begin_terminal_commit()?;
        self.linearize_terminal_commit()?;
        match root.atomic_create_new_synced(relative, bytes) {
            Ok(()) => Ok(()),
            Err(source) => Err(Self::rollback_failed_terminal_commit(io::Error::other(
                source,
            ))),
        }
    }

    /// Authorize and publish a terminal receipt through an already pinned absent path.
    pub fn commit_pinned_absent(
        &self,
        target: PinnedAbsentRoot,
        bytes: &[u8],
    ) -> Result<(), ProcessError> {
        self.begin_terminal_commit()?;
        self.linearize_terminal_commit()?;
        match target.atomic_create_new_synced(bytes) {
            Ok(()) => Ok(()),
            Err(source) => Err(Self::rollback_failed_terminal_commit(io::Error::other(
                source,
            ))),
        }
    }

    fn commit_with_checkpoints<B, A>(
        &self,
        path: &Path,
        bytes: &[u8],
        before_linearization: B,
        after_linearization: A,
    ) -> Result<(), ProcessError>
    where
        B: FnOnce(),
        A: FnOnce(),
    {
        self.begin_terminal_commit()?;
        before_linearization();
        self.linearize_terminal_commit()?;
        after_linearization();

        match create_terminal(path, bytes) {
            Ok(()) => Ok(()),
            Err(source) => Err(Self::rollback_failed_terminal_commit(source)),
        }
    }

    /// Publish an `INVALID` terminal after interruption has already been observed.
    ///
    /// This has the same mask and one-operation contract as [`Self::commit`], but deliberately
    /// permits received or pending managed signals. It must not be used for `PASS` or `FAIL`.
    pub fn commit_invalid(&self, path: &Path, bytes: &[u8]) -> Result<(), ProcessError> {
        create_terminal(path, bytes).map_err(ProcessError::Commit)?;
        EXECUTION_STATE.store(TERMINAL_COMMITTED, Ordering::SeqCst);
        Ok(())
    }

    /// Publish `INVALID` through a pinned evidence root after an interruption.
    pub fn commit_invalid_root(
        &self,
        root: &RootHandle,
        relative: &Path,
        bytes: &[u8],
    ) -> Result<(), ProcessError> {
        root.atomic_create_new_synced(relative, bytes)
            .map_err(|source| ProcessError::Commit(io::Error::other(source)))?;
        EXECUTION_STATE.store(TERMINAL_COMMITTED, Ordering::SeqCst);
        Ok(())
    }

    fn begin_terminal_commit(&self) -> Result<(), ProcessError> {
        if let Some(signal) = self.received_signal() {
            return Err(ProcessError::SignalBetweenChildren { signal });
        }
        match EXECUTION_STATE.compare_exchange(
            EXECUTION_RUNNING,
            TERMINAL_COMMITTING,
            Ordering::SeqCst,
            Ordering::SeqCst,
        ) {
            Ok(_) => {
                POST_TERMINAL_SIGNAL.store(0, Ordering::SeqCst);
                Ok(())
            }
            Err(EXECUTION_INTERRUPTING | EXECUTION_INTERRUPTED) => {
                Err(ProcessError::SignalBetweenChildren {
                    signal: Self::wait_for_interrupted_signal(),
                })
            }
            Err(TERMINAL_COMMITTED | TERMINAL_COMMITTED_SIGNALED) => {
                Err(ProcessError::TerminalAlreadyCommitted)
            }
            Err(_) => Err(ProcessError::TerminalAlreadyCommitted),
        }
    }

    fn linearize_terminal_commit(&self) -> Result<(), ProcessError> {
        let mask = SignalMaskGuard::block_managed()?;
        if let Some(signal) = self.received_signal().or(first_pending_managed_signal()?) {
            RECEIVED_SIGNAL.store(signal, Ordering::SeqCst);
            let _ = EXECUTION_STATE.compare_exchange(
                TERMINAL_COMMITTING,
                EXECUTION_INTERRUPTED,
                Ordering::SeqCst,
                Ordering::SeqCst,
            );
            return Err(ProcessError::SignalBetweenChildren { signal });
        }
        let linearized = EXECUTION_STATE.compare_exchange(
            TERMINAL_COMMITTING,
            TERMINAL_COMMITTED,
            Ordering::SeqCst,
            Ordering::SeqCst,
        );
        drop(mask);

        match linearized {
            Ok(_) => Ok(()),
            Err(EXECUTION_INTERRUPTING | EXECUTION_INTERRUPTED) => {
                Err(ProcessError::SignalBetweenChildren {
                    signal: Self::wait_for_interrupted_signal(),
                })
            }
            Err(TERMINAL_COMMITTED | TERMINAL_COMMITTED_SIGNALED) => {
                Err(ProcessError::TerminalAlreadyCommitted)
            }
            Err(_) => Err(ProcessError::TerminalAlreadyCommitted),
        }
    }

    fn rollback_failed_terminal_commit(source: io::Error) -> ProcessError {
        if EXECUTION_STATE
            .compare_exchange(
                TERMINAL_COMMITTED,
                EXECUTION_RUNNING,
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .is_ok()
        {
            return ProcessError::Commit(source);
        }

        if EXECUTION_STATE
            .compare_exchange(
                TERMINAL_COMMITTED_SIGNALED,
                EXECUTION_INTERRUPTING,
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .is_ok()
        {
            let signal = POST_TERMINAL_SIGNAL.load(Ordering::SeqCst);
            record_and_forward_signal(signal);
            EXECUTION_STATE.store(EXECUTION_INTERRUPTED, Ordering::SeqCst);
            return ProcessError::CommitInterrupted { signal, source };
        }

        ProcessError::Commit(source)
    }

    fn wait_for_interrupted_signal() -> i32 {
        loop {
            let signal = RECEIVED_SIGNAL.load(Ordering::SeqCst);
            if signal != 0 {
                return signal;
            }
            std::hint::spin_loop();
        }
    }
}

impl Drop for ExecutionSignalGuard {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        ACTIVE_PROCESS_GROUP.store(0, Ordering::SeqCst);
        EXECUTION_STATE.store(TERMINAL_COMMITTED, Ordering::SeqCst);
        for (signal, action) in self.previous.iter().rev() {
            // SAFETY: these are the exact actions returned during installation.
            unsafe {
                libc::sigaction(*signal, action, std::ptr::null_mut());
            }
        }
        EXECUTION_GUARD_ACTIVE.store(false, Ordering::SeqCst);
    }
}

struct SignalMaskGuard {
    previous: libc::sigset_t,
}

impl SignalMaskGuard {
    fn block_managed() -> Result<Self, ProcessError> {
        // SAFETY: initialized sets are passed to pthread_sigmask on the current thread.
        unsafe {
            let mut managed: libc::sigset_t = std::mem::zeroed();
            libc::sigemptyset(&raw mut managed);
            for signal in FORWARDED_SIGNALS {
                libc::sigaddset(&raw mut managed, signal);
            }
            let mut previous: libc::sigset_t = std::mem::zeroed();
            let result =
                libc::pthread_sigmask(libc::SIG_BLOCK, &raw const managed, &raw mut previous);
            if result != 0 {
                return Err(ProcessError::SignalSetup(io::Error::from_raw_os_error(
                    result,
                )));
            }
            Ok(Self { previous })
        }
    }
}

impl Drop for SignalMaskGuard {
    fn drop(&mut self) {
        // SAFETY: `previous` was populated by the successful pthread_sigmask call.
        unsafe {
            libc::pthread_sigmask(
                libc::SIG_SETMASK,
                &raw const self.previous,
                std::ptr::null_mut(),
            );
        }
    }
}

fn first_pending_managed_signal() -> Result<Option<i32>, ProcessError> {
    // SAFETY: sigpending initializes `pending`; sigismember only reads it.
    unsafe {
        let mut pending: libc::sigset_t = std::mem::zeroed();
        if libc::sigpending(&raw mut pending) != 0 {
            return Err(ProcessError::SignalSetup(io::Error::last_os_error()));
        }
        for signal in FORWARDED_SIGNALS {
            let member = libc::sigismember(&raw const pending, signal);
            if member == -1 {
                return Err(ProcessError::SignalSetup(io::Error::last_os_error()));
            }
            if member == 1 {
                return Ok(Some(signal));
            }
        }
    }
    Ok(None)
}

fn create_terminal(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let mut file = File::options()
        .write(true)
        .create_new(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    if let Some(parent) = path.parent() {
        File::open(parent)?.sync_all()?;
    }
    Ok(())
}

fn create_output(path: &Path) -> Result<File, ProcessError> {
    File::options()
        .write(true)
        .create_new(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
        .map_err(|source| ProcessError::Output {
            path: path.display().to_string(),
            source,
        })
}

struct ActiveChild {
    child: Child,
    pid: u32,
    process_group: Option<i32>,
    reaped: bool,
}

impl ActiveChild {
    fn new(child: Child) -> Self {
        let pid = child.id();
        Self {
            child,
            pid,
            process_group: None,
            reaped: false,
        }
    }

    fn publish_process_group(&mut self) -> Result<i32, ProcessError> {
        let process_group = i32::try_from(self.pid).map_err(|_| {
            ProcessError::Spawn(io::Error::new(
                io::ErrorKind::InvalidData,
                "child PID does not fit i32",
            ))
        })?;
        self.process_group = Some(process_group);
        ACTIVE_PROCESS_GROUP.store(process_group, Ordering::SeqCst);
        Ok(process_group)
    }

    fn try_wait(&mut self) -> Result<Option<ExitStatus>, ProcessError> {
        self.child
            .try_wait()
            .inspect(|status| {
                if status.is_some() {
                    self.reaped = true;
                }
            })
            .map_err(|source| ProcessError::Wait {
                pid: self.pid,
                source,
            })
    }

    fn kill_group(&mut self, signal: i32) {
        if let Some(process_group) = self.process_group {
            // SAFETY: the child is the leader of the process group configured at spawn.
            unsafe {
                libc::kill(-process_group, signal);
            }
        } else {
            // A PID conversion failure occurs before the group is globally published. Use the
            // std child handle so cleanup does not depend on converting the PID.
            if signal == libc::SIGKILL {
                let _ = self.child.kill();
            }
        }
    }

    fn kill_and_reap(&mut self) -> Result<ExitStatus, ProcessError> {
        self.kill_group(libc::SIGKILL);
        self.wait()
    }

    fn wait(&mut self) -> Result<ExitStatus, ProcessError> {
        let status = self.child.wait().map_err(|source| ProcessError::Wait {
            pid: self.pid,
            source,
        })?;
        self.reaped = true;
        self.clear_global();
        Ok(status)
    }

    fn clear_global(&self) {
        if let Some(process_group) = self.process_group {
            let _ = ACTIVE_PROCESS_GROUP.compare_exchange(
                process_group,
                0,
                Ordering::SeqCst,
                Ordering::SeqCst,
            );
        } else {
            ACTIVE_PROCESS_GROUP.store(0, Ordering::SeqCst);
        }
    }
}

impl Drop for ActiveChild {
    fn drop(&mut self) {
        // The leader can exit while a descendant keeps its process group and inherited pipes
        // alive. Kill the whole group even after the leader has already been reaped.
        self.kill_group(libc::SIGKILL);
        if !self.reaped {
            let _ = self.child.wait();
            self.reaped = true;
        }
        self.clear_global();
    }
}

#[derive(Default)]
struct DrainState {
    observed: AtomicU64,
    retained: AtomicU64,
    overflow: AtomicBool,
    failed: AtomicBool,
    stop: AtomicBool,
}

struct DrainHandle {
    stream: &'static str,
    state: Arc<DrainState>,
    handle: JoinHandle<io::Result<()>>,
}

impl DrainHandle {
    fn spawn<R>(
        stream: &'static str,
        reader: R,
        output: File,
        limit: u64,
    ) -> Result<Self, io::Error>
    where
        R: Read + AsRawFd + Send + 'static,
    {
        set_nonblocking(reader.as_raw_fd())?;
        let state = Arc::new(DrainState::default());
        let thread_state = Arc::clone(&state);
        let handle = thread::Builder::new()
            .name(format!("quic-fetch-{stream}-drain"))
            .spawn(move || drain_bounded(reader, output, limit, &thread_state))?;
        Ok(Self {
            stream,
            state,
            handle,
        })
    }

    fn stop(&self) {
        self.state.stop.store(true, Ordering::SeqCst);
    }

    fn join(self) -> DrainReport {
        let stream = self.stream;
        let state = self.state;
        let failure = match self.handle.join() {
            Ok(Ok(())) => None,
            Ok(Err(source)) => Some(ChildCleanupIssue::Drain {
                stream,
                error_kind: source.kind(),
                message: source.to_string(),
            }),
            Err(_) => Some(ChildCleanupIssue::DrainPanic { stream }),
        };
        DrainReport {
            observed: state.observed.load(Ordering::SeqCst),
            retained: state.retained.load(Ordering::SeqCst),
            overflow: state.overflow.load(Ordering::SeqCst),
            failure,
        }
    }
}

struct DrainReport {
    observed: u64,
    retained: u64,
    overflow: bool,
    failure: Option<ChildCleanupIssue>,
}

fn drain_bounded(
    mut reader: impl Read,
    mut output: File,
    limit: u64,
    state: &DrainState,
) -> io::Result<()> {
    let mut buffer = [0_u8; 16 * 1024];
    let mut retained = 0_u64;
    let mut first_error = None;
    loop {
        let read = match reader.read(&mut buffer) {
            Ok(0) => break,
            Ok(read) => read,
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                if state.stop.load(Ordering::SeqCst) {
                    break;
                }
                thread::sleep(DRAIN_POLL_INTERVAL);
                continue;
            }
            Err(error) => {
                state.failed.store(true, Ordering::SeqCst);
                return Err(error);
            }
        };
        let read_u64 = u64::try_from(read).unwrap_or(u64::MAX);
        let _ = state
            .observed
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |observed| {
                Some(observed.saturating_add(read_u64))
            });

        let remaining = limit.saturating_sub(retained);
        let to_write_u64 = remaining.min(read_u64);
        let to_write = usize::try_from(to_write_u64).expect("write size is bounded by read buffer");
        if to_write > 0 && first_error.is_none() {
            if let Err(error) = output.write_all(&buffer[..to_write]) {
                state.failed.store(true, Ordering::SeqCst);
                first_error = Some(error);
            } else {
                retained += to_write_u64;
                state.retained.store(retained, Ordering::SeqCst);
            }
        }
        if read_u64 > remaining {
            state.overflow.store(true, Ordering::SeqCst);
        }
    }
    if let Some(error) = first_error {
        return Err(error);
    }
    output.sync_all().inspect_err(|_| {
        state.failed.store(true, Ordering::SeqCst);
    })
}

fn set_nonblocking(file_descriptor: i32) -> io::Result<()> {
    // SAFETY: `file_descriptor` belongs to a live pipe owned by the caller. `F_GETFL` and
    // `F_SETFL` only inspect/update its status flags.
    unsafe {
        let flags = libc::fcntl(file_descriptor, libc::F_GETFL);
        if flags == -1 {
            return Err(io::Error::last_os_error());
        }
        if libc::fcntl(file_descriptor, libc::F_SETFL, flags | libc::O_NONBLOCK) == -1 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(())
}

fn drains_need_stop(stdout: &DrainHandle, stderr: &DrainHandle) -> bool {
    stdout.state.overflow.load(Ordering::SeqCst)
        || stderr.state.overflow.load(Ordering::SeqCst)
        || stdout.state.failed.load(Ordering::SeqCst)
        || stderr.state.failed.load(Ordering::SeqCst)
}

enum MonitorResult {
    Exited(ExitStatus),
    RuntimeLimit(ExitStatus),
    OutputLimit(ExitStatus),
    DrainFailure(ExitStatus),
    WaitFailure {
        status: ExitStatus,
        issue: ChildCleanupIssue,
    },
    ForcedSignal(ExitStatus),
}

fn spawned_failure_outcome(
    active: &mut ActiveChild,
    termination: ChildTermination,
    issue: ChildCleanupIssue,
    drains: Vec<DrainHandle>,
) -> ChildOutcome {
    active.kill_group(libc::SIGKILL);
    let status = active.wait().ok();
    let mut cleanup_issues = vec![issue];
    let mut stdout_bytes = 0_u64;
    let mut stderr_bytes = 0_u64;
    let mut stdout_observed_bytes = 0_u64;
    let mut stderr_observed_bytes = 0_u64;
    for drain in drains {
        drain.stop();
        let stream = drain.stream;
        let report = drain.join();
        if stream == "stdout" {
            stdout_bytes = report.retained;
            stdout_observed_bytes = report.observed;
        } else {
            stderr_bytes = report.retained;
            stderr_observed_bytes = report.observed;
        }
        if let Some(failure) = report.failure {
            cleanup_issues.push(failure);
        }
    }
    let status_available = status.is_some();
    let received_signal = RECEIVED_SIGNAL.load(Ordering::SeqCst);
    ChildOutcome {
        pid: active.pid,
        status,
        forwarded_signal: (received_signal != 0).then_some(received_signal),
        termination: if status_available {
            termination
        } else {
            ChildTermination::StatusUnavailable
        },
        stdout_bytes,
        stderr_bytes,
        stdout_observed_bytes,
        stderr_observed_bytes,
        cleanup_issues,
    }
}

/// Runs one child in its own process group, drains bounded output concurrently, and always reaps.
#[allow(clippy::too_many_lines)]
pub fn run_child<F>(
    guard: &ExecutionSignalGuard,
    command: &mut Command,
    stdout_path: &Path,
    stderr_path: &Path,
    limits: ProcessLimits,
    on_started: F,
) -> Result<ChildOutcome, ProcessError>
where
    F: FnOnce(u32) -> io::Result<()>,
{
    guard.require_no_signal()?;
    let stdout_file = create_output(stdout_path)?;
    let stderr_file = create_output(stderr_path)?;
    run_child_with_output_files(guard, command, stdout_file, stderr_file, limits, on_started)
}

/// Runs one child using caller-opened create-new output files.
///
/// This entry point preserves descriptor-relative/no-follow evidence roots: the caller can pin and
/// create each leaf once, then transfer the live file descriptors without reopening their paths.
#[allow(clippy::too_many_lines)]
pub fn run_child_with_output_files<F>(
    guard: &ExecutionSignalGuard,
    command: &mut Command,
    stdout_file: File,
    stderr_file: File,
    limits: ProcessLimits,
    on_started: F,
) -> Result<ChildOutcome, ProcessError>
where
    F: FnOnce(u32) -> io::Result<()>,
{
    guard.require_no_signal()?;
    command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .process_group(0);

    let child = command.spawn().map_err(ProcessError::Spawn)?;
    let mut active = ActiveChild::new(child);
    let process_group = match active.publish_process_group() {
        Ok(process_group) => process_group,
        Err(error) => {
            return Ok(spawned_failure_outcome(
                &mut active,
                ChildTermination::StatusUnavailable,
                ChildCleanupIssue::Setup {
                    stage: "publish_process_group",
                    error_kind: io::ErrorKind::InvalidData,
                    message: error.to_string(),
                },
                Vec::new(),
            ));
        }
    };
    if let Err(source) = on_started(active.pid) {
        return Ok(spawned_failure_outcome(
            &mut active,
            ChildTermination::StartedCallbackFailure,
            ChildCleanupIssue::Setup {
                stage: "on_started",
                error_kind: source.kind(),
                message: source.to_string(),
            },
            Vec::new(),
        ));
    }
    let Some(stdout): Option<ChildStdout> = active.child.stdout.take() else {
        return Ok(spawned_failure_outcome(
            &mut active,
            ChildTermination::OutputSetupFailure,
            ChildCleanupIssue::Setup {
                stage: "stdout_pipe",
                error_kind: io::ErrorKind::BrokenPipe,
                message: "spawned child has no stdout pipe".to_string(),
            },
            Vec::new(),
        ));
    };
    let Some(stderr): Option<ChildStderr> = active.child.stderr.take() else {
        return Ok(spawned_failure_outcome(
            &mut active,
            ChildTermination::OutputSetupFailure,
            ChildCleanupIssue::Setup {
                stage: "stderr_pipe",
                error_kind: io::ErrorKind::BrokenPipe,
                message: "spawned child has no stderr pipe".to_string(),
            },
            Vec::new(),
        ));
    };
    let stdout_drain =
        match DrainHandle::spawn("stdout", stdout, stdout_file, limits.max_stdout_bytes) {
            Ok(drain) => drain,
            Err(source) => {
                return Ok(spawned_failure_outcome(
                    &mut active,
                    ChildTermination::OutputSetupFailure,
                    ChildCleanupIssue::Setup {
                        stage: "stdout_drain",
                        error_kind: source.kind(),
                        message: source.to_string(),
                    },
                    Vec::new(),
                ));
            }
        };
    let stderr_drain =
        match DrainHandle::spawn("stderr", stderr, stderr_file, limits.max_stderr_bytes) {
            Ok(drain) => drain,
            Err(error) => {
                return Ok(spawned_failure_outcome(
                    &mut active,
                    ChildTermination::OutputSetupFailure,
                    ChildCleanupIssue::Setup {
                        stage: "stderr_drain",
                        error_kind: error.kind(),
                        message: error.to_string(),
                    },
                    vec![stdout_drain],
                ));
            }
        };

    if let Some(signal) = guard.received_signal() {
        // SAFETY: the spawned child is the leader of this process group.
        unsafe {
            libc::kill(-process_group, signal);
        }
    }

    let started = Instant::now();
    let mut signal_observed_at = None;
    macro_rules! reap_or_return_outcome {
        ($stage:literal) => {
            match active.kill_and_reap() {
                Ok(status) => status,
                Err(error) => {
                    return Ok(spawned_failure_outcome(
                        &mut active,
                        ChildTermination::StatusUnavailable,
                        ChildCleanupIssue::Setup {
                            stage: $stage,
                            error_kind: io::ErrorKind::Other,
                            message: error.to_string(),
                        },
                        vec![stdout_drain, stderr_drain],
                    ));
                }
            }
        };
    }
    let monitor = loop {
        match active.try_wait() {
            Ok(Some(status)) => {
                // A child leader may leave descendants holding inherited output pipes. They are
                // outside the one-child contract and must not make the drain joins unbounded.
                active.kill_group(libc::SIGKILL);
                active.clear_global();
                break MonitorResult::Exited(status);
            }
            Ok(None) => {}
            Err(ProcessError::Wait { source, .. }) => {
                let issue = ChildCleanupIssue::Wait {
                    operation: "try_wait",
                    error_kind: source.kind(),
                    message: source.to_string(),
                };
                let status = reap_or_return_outcome!("try_wait_cleanup");
                break MonitorResult::WaitFailure { status, issue };
            }
            Err(error) => return Err(error),
        }
        if drains_need_stop(&stdout_drain, &stderr_drain) {
            let output_limit = stdout_drain.state.overflow.load(Ordering::SeqCst)
                || stderr_drain.state.overflow.load(Ordering::SeqCst);
            let status = reap_or_return_outcome!("drain_cleanup");
            break if output_limit {
                MonitorResult::OutputLimit(status)
            } else {
                MonitorResult::DrainFailure(status)
            };
        }
        if started.elapsed() > limits.max_duration {
            let status = reap_or_return_outcome!("runtime_cleanup");
            break MonitorResult::RuntimeLimit(status);
        }
        if guard.received_signal().is_some() {
            let observed = signal_observed_at.get_or_insert_with(Instant::now);
            if observed.elapsed() > limits.signal_grace {
                let status = reap_or_return_outcome!("signal_cleanup");
                break MonitorResult::ForcedSignal(status);
            }
        }
        thread::sleep(WAIT_POLL_INTERVAL);
    };

    stdout_drain.stop();
    stderr_drain.stop();
    let stdout_report = stdout_drain.join();
    let stderr_report = stderr_drain.join();
    let pid = active.pid;
    let forwarded_signal = guard.received_signal();
    let mut cleanup_issues = Vec::new();
    if let Some(issue) = stdout_report.failure {
        cleanup_issues.push(issue);
    }
    if let Some(issue) = stderr_report.failure {
        cleanup_issues.push(issue);
    }

    let reports_overflowed = stdout_report.overflow || stderr_report.overflow;
    let reports_failed = !cleanup_issues.is_empty();
    let (status, termination) = match monitor {
        MonitorResult::Exited(status) if reports_overflowed => (
            status,
            ChildTermination::OutputLimit {
                stdout_limit: limits.max_stdout_bytes,
                stderr_limit: limits.max_stderr_bytes,
            },
        ),
        MonitorResult::Exited(status) if reports_failed => (status, ChildTermination::DrainFailure),
        MonitorResult::Exited(status) => match forwarded_signal {
            Some(signal) => (
                status,
                ChildTermination::ManagedSignal {
                    signal,
                    escalated: false,
                },
            ),
            None => (status, ChildTermination::Exited),
        },
        MonitorResult::RuntimeLimit(status) => (
            status,
            ChildTermination::RuntimeLimit {
                seconds: limits.max_duration.as_secs(),
            },
        ),
        MonitorResult::OutputLimit(status) => (
            status,
            ChildTermination::OutputLimit {
                stdout_limit: limits.max_stdout_bytes,
                stderr_limit: limits.max_stderr_bytes,
            },
        ),
        MonitorResult::DrainFailure(status) => (status, ChildTermination::DrainFailure),
        MonitorResult::WaitFailure { status, issue } => {
            cleanup_issues.insert(0, issue);
            (status, ChildTermination::WaitFailure)
        }
        MonitorResult::ForcedSignal(status) => (
            status,
            ChildTermination::ManagedSignal {
                signal: forwarded_signal.unwrap_or_default(),
                escalated: true,
            },
        ),
    };

    Ok(ChildOutcome {
        pid,
        status: Some(status),
        forwarded_signal,
        termination,
        stdout_bytes: stdout_report.retained,
        stderr_bytes: stderr_report.retained,
        stdout_observed_bytes: stdout_report.observed,
        stderr_observed_bytes: stderr_report.observed,
        cleanup_issues,
    })
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::os::unix::process::ExitStatusExt;
    use std::path::Path;
    use std::process::Command;
    use std::sync::atomic::Ordering;
    use std::thread;
    use std::time::{Duration, Instant};

    use tempfile::tempdir;

    use super::{
        run_child, ChildTermination, ExecutionSignalGuard, ProcessError, ProcessLimits,
        ACTIVE_PROCESS_GROUP, TEST_EXECUTION_LOCK,
    };

    fn execution_lock() -> std::sync::MutexGuard<'static, ()> {
        TEST_EXECUTION_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn limits() -> ProcessLimits {
        ProcessLimits {
            max_duration: Duration::from_secs(5),
            max_stdout_bytes: 64 * 1024,
            max_stderr_bytes: 64 * 1024,
            signal_grace: Duration::from_millis(100),
        }
    }

    #[test]
    fn captures_nonzero_exit_status_and_output_counts() {
        let _lock = execution_lock();
        let guard = ExecutionSignalGuard::install().unwrap();
        let directory = tempdir().unwrap();
        let stdout = directory.path().join("stdout");
        let stderr = directory.path().join("stderr");
        let mut command = Command::new("/bin/sh");
        command.args(["-c", "printf 'ready\\n'; printf 'warning\\n' >&2; exit 7"]);
        let mut started = None;

        let outcome = run_child(&guard, &mut command, &stdout, &stderr, limits(), |pid| {
            started = Some(pid);
            Ok(())
        })
        .unwrap();

        assert!(!outcome.success());
        assert_eq!(outcome.status.and_then(|status| status.code()), Some(7));
        assert_eq!(outcome.termination, ChildTermination::Exited);
        assert_eq!(outcome.stdout_bytes, 6);
        assert_eq!(outcome.stderr_bytes, 8);
        assert!(outcome.cleanup_issues.is_empty());
        assert_eq!(started, Some(outcome.pid));
        assert_eq!(std::fs::read_to_string(stdout).unwrap(), "ready\n");
        assert_eq!(std::fs::read_to_string(stderr).unwrap(), "warning\n");
        assert_eq!(ACTIVE_PROCESS_GROUP.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn burst_over_cap_is_truncated_then_group_is_killed_and_reaped() {
        let _lock = execution_lock();
        let guard = ExecutionSignalGuard::install().unwrap();
        let directory = tempdir().unwrap();
        let stdout = directory.path().join("stdout");
        let stderr = directory.path().join("stderr");
        let mut command = Command::new("/bin/sh");
        command.args(["-c", "while :; do printf 0123456789abcdef; done"]);
        let mut bounded = limits();
        bounded.max_stdout_bytes = 4_096;
        let mut pid = None;

        let outcome = run_child(&guard, &mut command, &stdout, &stderr, bounded, |started| {
            pid = Some(started);
            Ok(())
        })
        .unwrap();

        assert!(matches!(
            outcome.termination,
            ChildTermination::OutputLimit {
                stdout_limit: 4_096,
                stderr_limit: 65_536
            }
        ));
        assert_eq!(
            outcome.status.and_then(|status| status.signal()),
            Some(libc::SIGKILL)
        );
        assert_eq!(outcome.stdout_bytes, 4_096);
        assert!(outcome.stdout_observed_bytes > 4_096);
        assert_eq!(outcome.stderr_bytes, 0);
        assert!(outcome.cleanup_issues.is_empty());
        assert_eq!(std::fs::metadata(stdout).unwrap().len(), 4_096);
        assert!(std::fs::metadata(stderr).unwrap().len() <= bounded.max_stderr_bytes);
        assert_process_absent(pid.unwrap());
        assert_eq!(ACTIVE_PROCESS_GROUP.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn reaped_leader_cannot_leave_a_descendant_holding_output_pipes() {
        let _lock = execution_lock();
        let guard = ExecutionSignalGuard::install().unwrap();
        let directory = tempdir().unwrap();
        let descendant_pid = directory.path().join("descendant-pid");
        let stdout = directory.path().join("stdout");
        let stderr = directory.path().join("stderr");
        let mut command = Command::new("/bin/sh");
        command.arg("-c").arg(format!(
            "(trap '' TERM; while :; do sleep 1; done) & printf '%s\\n' $! > '{}'; exit 0",
            descendant_pid.display()
        ));
        let started = Instant::now();

        let outcome =
            run_child(&guard, &mut command, &stdout, &stderr, limits(), |_| Ok(())).unwrap();

        assert!(outcome.success());
        assert!(started.elapsed() < Duration::from_secs(2));
        let descendant: u32 = std::fs::read_to_string(descendant_pid)
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        assert_process_absent(descendant);
        assert_eq!(ACTIVE_PROCESS_GROUP.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn started_callback_failure_kills_reaps_and_clears_global() {
        let _lock = execution_lock();
        let guard = ExecutionSignalGuard::install().unwrap();
        let directory = tempdir().unwrap();
        let stdout = directory.path().join("stdout");
        let stderr = directory.path().join("stderr");
        let mut command = Command::new("/bin/sh");
        command.args(["-c", "while :; do sleep 1; done"]);
        let mut pid = None;

        let outcome = run_child(
            &guard,
            &mut command,
            &stdout,
            &stderr,
            limits(),
            |started| {
                pid = Some(started);
                Err(io::Error::other("evidence unavailable"))
            },
        )
        .unwrap();

        assert_eq!(
            outcome.termination,
            ChildTermination::StartedCallbackFailure
        );
        assert!(outcome.status.is_some());
        assert!(matches!(
            outcome.cleanup_issues.as_slice(),
            [super::ChildCleanupIssue::Setup {
                stage: "on_started",
                ..
            }]
        ));
        assert_process_absent(pid.unwrap());
        assert_eq!(ACTIVE_PROCESS_GROUP.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn forwarded_signal_gets_grace_then_sigkill_and_no_orphan() {
        let _lock = execution_lock();
        let guard = ExecutionSignalGuard::install().unwrap();
        let directory = tempdir().unwrap();
        let ready = directory.path().join("ready");
        let stdout = directory.path().join("stdout");
        let stderr = directory.path().join("stderr");
        let mut command = Command::new("/bin/sh");
        command.arg("-c").arg(format!(
            "trap '' TERM; : > '{}'; while :; do sleep 1; done",
            ready.display()
        ));
        let mut bounded = limits();
        bounded.signal_grace = Duration::from_millis(50);
        let mut pid = None;

        let outcome = run_child(&guard, &mut command, &stdout, &stderr, bounded, |started| {
            pid = Some(started);
            wait_for_file(&ready);
            // SAFETY: the installed guard owns SIGTERM and forwards it to the active group.
            assert_eq!(unsafe { libc::kill(libc::getpid(), libc::SIGTERM) }, 0);
            Ok(())
        })
        .unwrap();

        assert_eq!(outcome.forwarded_signal, Some(libc::SIGTERM));
        assert_eq!(
            outcome.status.and_then(|status| status.signal()),
            Some(libc::SIGKILL)
        );
        assert_eq!(
            outcome.termination,
            ChildTermination::ManagedSignal {
                signal: libc::SIGTERM,
                escalated: true
            }
        );
        assert_process_absent(pid.unwrap());
        assert_eq!(ACTIVE_PROCESS_GROUP.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn runtime_limit_returns_reaped_status_and_counts() {
        let _lock = execution_lock();
        let guard = ExecutionSignalGuard::install().unwrap();
        let directory = tempdir().unwrap();
        let stdout = directory.path().join("stdout");
        let stderr = directory.path().join("stderr");
        let mut command = Command::new("/bin/sh");
        command.args(["-c", "printf started; while :; do sleep 1; done"]);
        let mut bounded = limits();
        bounded.max_duration = Duration::from_millis(40);

        let outcome =
            run_child(&guard, &mut command, &stdout, &stderr, bounded, |_| Ok(())).unwrap();

        assert_eq!(
            outcome.termination,
            ChildTermination::RuntimeLimit { seconds: 0 }
        );
        assert_eq!(
            outcome.status.and_then(|status| status.signal()),
            Some(libc::SIGKILL)
        );
        assert_eq!(outcome.stdout_bytes, 7);
        assert_eq!(outcome.stderr_bytes, 0);
        assert!(outcome.cleanup_issues.is_empty());
        assert_process_absent(outcome.pid);
    }

    #[test]
    fn commit_succeeds_with_no_signal() {
        let _lock = execution_lock();
        let guard = ExecutionSignalGuard::install().unwrap();
        let directory = tempdir().unwrap();
        let terminal = directory.path().join("terminal");
        guard.commit(&terminal, b"PASS\n").unwrap();
        assert_eq!(std::fs::read(&terminal).unwrap(), b"PASS\n");
    }

    #[test]
    fn failed_commit_rolls_back_when_no_signal_crossed_linearization() {
        let _lock = execution_lock();
        let guard = ExecutionSignalGuard::install().unwrap();
        let directory = tempdir().unwrap();
        let unavailable = directory.path().join("missing").join("terminal");
        let error = guard.commit(&unavailable, b"PASS\n").unwrap_err();
        assert!(matches!(error, ProcessError::Commit(_)));
        let terminal = directory.path().join("terminal");
        guard.commit(&terminal, b"PASS\n").unwrap();
        assert_eq!(std::fs::read(&terminal).unwrap(), b"PASS\n");
    }

    #[test]
    fn signal_during_commit_before_linearization_prevents_terminal_operation() {
        let _lock = execution_lock();
        let guard = ExecutionSignalGuard::install().unwrap();
        let directory = tempdir().unwrap();
        let terminal = directory.path().join("terminal");
        let error = guard
            .commit_with_checkpoints(
                &terminal,
                b"PASS\n",
                || {
                    // SAFETY: the installed guard owns SIGHUP and there is no active child group.
                    assert_eq!(unsafe { libc::kill(libc::getpid(), libc::SIGHUP) }, 0);
                    let deadline = Instant::now() + Duration::from_secs(1);
                    while guard.received_signal().is_none() {
                        assert!(Instant::now() < deadline, "signal delivery timed out");
                        thread::yield_now();
                    }
                },
                || {},
            )
            .unwrap_err();
        assert!(matches!(
            error,
            ProcessError::SignalBetweenChildren {
                signal: libc::SIGHUP
            }
        ));
        assert!(!terminal.exists());

        guard.commit_invalid(&terminal, b"INVALID\n").unwrap();
        assert_eq!(std::fs::read(&terminal).unwrap(), b"INVALID\n");
    }

    #[test]
    fn signal_after_linearization_is_post_terminal_and_failed_create_does_not_rollback() {
        let _lock = execution_lock();
        let guard = ExecutionSignalGuard::install().unwrap();
        let directory = tempdir().unwrap();
        let unavailable = directory.path().join("missing").join("terminal");
        let error = guard
            .commit_with_checkpoints(
                &unavailable,
                b"PASS\n",
                || {},
                || {
                    // SAFETY: the installed guard owns SIGTERM and there is no active child group.
                    assert_eq!(unsafe { libc::kill(libc::getpid(), libc::SIGTERM) }, 0);
                    let deadline = Instant::now() + Duration::from_secs(1);
                    while guard.post_terminal_signal().is_none() {
                        assert!(Instant::now() < deadline, "signal delivery timed out");
                        thread::yield_now();
                    }
                },
            )
            .unwrap_err();

        assert!(matches!(
            error,
            ProcessError::CommitInterrupted {
                signal: libc::SIGTERM,
                ..
            }
        ));
        assert_eq!(guard.received_signal(), Some(libc::SIGTERM));
        assert_eq!(guard.post_terminal_signal(), Some(libc::SIGTERM));
        let second = guard
            .commit(&directory.path().join("terminal"), b"PASS\n")
            .unwrap_err();
        assert!(matches!(
            second,
            ProcessError::SignalBetweenChildren {
                signal: libc::SIGTERM
            }
        ));
    }

    #[test]
    fn refuses_second_execution_guard() {
        let _lock = execution_lock();
        let _guard = ExecutionSignalGuard::install().unwrap();
        assert!(matches!(
            ExecutionSignalGuard::install(),
            Err(ProcessError::ConcurrentGuard)
        ));
    }

    fn wait_for_file(path: &Path) {
        let deadline = Instant::now() + Duration::from_secs(2);
        while !path.is_file() {
            assert!(Instant::now() < deadline, "child readiness timed out");
            thread::sleep(Duration::from_millis(5));
        }
    }

    fn assert_process_absent(pid: u32) {
        let pid = i32::try_from(pid).unwrap();
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            // SAFETY: signal zero only queries process existence.
            let result = unsafe { libc::kill(pid, 0) };
            if result == -1 && io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH) {
                return;
            }
            assert!(Instant::now() < deadline, "process {pid} survived cleanup");
            thread::sleep(Duration::from_millis(5));
        }
    }
}

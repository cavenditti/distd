use std::path::{Path, PathBuf};

use clap::{Parser, Subcommand};
use distd_bench::quic_fetch_acceptance::{
    compose::{self, ComposeInput},
    environment::EnvironmentExpectations,
    evidence::{canonical_json_line, stable_file_bytes_bounded},
    execute::{self, ExecuteError},
    model::{Plan, Request, TerminalStatus},
    prepare::{self, PrepareOutcome, PreparePaths},
    provenance::MAX_PROVENANCE_INPUT_BYTES,
};
use serde::{de::DeserializeOwned, Serialize};

const EXIT_PASS: i32 = 0;
const EXIT_INVALID: i32 = 1;
const EXIT_FAIL: i32 = 2;
const EXIT_BLOCKED: i32 = 3;

#[derive(Debug, Parser)]
#[command(
    name = "quic-fetch-acceptance",
    about = "Run the sealed, frozen authenticated-Fetch acceptance protocol",
    long_about = "Fail-closed driver for one preregistered baseline/candidate comparison.\n\
                   compose-request and prepare never invoke product code. prepare accepts only\n\
                   canonical composed bytes; execute revalidates the sealed plan and live inputs\n\
                   before the first product process; verify independently authenticates evidence.",
    after_help = "Terminal outcomes: PREPARED/PASS=0, INVALID=1, FAIL=2, BLOCKED=3.\n\
                  Managed signals are re-raised after process cleanup."
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Publish one canonical request; this is the sole prepare authority boundary.
    ComposeRequest {
        /// Canonical JSON descriptor naming the capture and every provenance input.
        #[arg(long)]
        input: PathBuf,
        /// Absent path at which to create the immutable request.
        #[arg(long)]
        output: PathBuf,
    },
    /// Authenticate exact composed bytes and seal a non-executing immutable plan.
    Prepare {
        /// Canonical request published by `compose-request`; hand-authored drift is invalid.
        #[arg(long)]
        request: PathBuf,
        /// Absent directory at which to atomically seal the plan and copied input bundle.
        #[arg(long)]
        plan: PathBuf,
        /// Absent path reserved for zero-product BLOCKED preflight evidence.
        #[arg(long)]
        blocked_evidence: PathBuf,
    },
    /// Execute the fixed twelve-run protocol exactly once after live revalidation.
    Execute {
        /// Sealed plan directory, or its plan.json, created by `prepare`.
        #[arg(long)]
        plan: PathBuf,
    },
    /// Rehash evidence and independently recompute its terminal verdict without execution.
    Verify {
        /// Sealed plan directory, or its plan.json, used by the experiment.
        #[arg(long)]
        plan: PathBuf,
        /// Completed evidence root named by the plan.
        #[arg(long)]
        evidence: PathBuf,
    },
    /// Capture canonical current source identity without invoking product code.
    CaptureSource {
        /// Repository whose tracked, staged, and untracked bytes are captured.
        #[arg(long)]
        repository: PathBuf,
        /// Absent output directory outside the repository.
        #[arg(long)]
        output: PathBuf,
        /// Accepted task specification whose bytes are bound into the identity.
        #[arg(long)]
        task_spec: PathBuf,
    },
}

#[derive(Debug)]
struct TerminalLine {
    status: &'static str,
    evidence: PathBuf,
    detail: String,
    exit_code: i32,
}

impl TerminalLine {
    fn emit(self) -> i32 {
        println!(
            "{} evidence={} detail={}",
            self.status,
            self.evidence.display(),
            one_line(&self.detail)
        );
        self.exit_code
    }
}

fn plan_file(path: &Path) -> PathBuf {
    if path.is_dir() {
        path.join("plan.json")
    } else {
        path.to_path_buf()
    }
}

fn strict_canonical_json<T>(
    path: &Path,
    role: &str,
) -> Result<
    (
        T,
        Vec<u8>,
        distd_bench::quic_fetch_acceptance::evidence::FileIdentity,
    ),
    Box<dyn std::error::Error>,
>
where
    T: DeserializeOwned + Serialize,
{
    let (identity, bytes) = stable_file_bytes_bounded(path, MAX_PROVENANCE_INPUT_BYTES)?;
    let value: T = serde_json::from_slice(&bytes)
        .map_err(|error| format!("{role} JSON is invalid: {error}"))?;
    if canonical_json_line(&value)? != bytes {
        return Err(format!("{role} bytes are not canonical JSON").into());
    }
    Ok((value, bytes, identity))
}

#[allow(clippy::too_many_lines)]
fn run() -> Result<i32, Box<dyn std::error::Error>> {
    match Cli::parse().command {
        Command::ComposeRequest { input, output } => {
            let (mut input, _, _) = strict_canonical_json::<ComposeInput>(&input, "compose input")?;
            if input.output != output {
                return Err("--output must exactly match compose input output".into());
            }
            input.output = output;
            let receipt = compose::compose(&input)?;
            Ok(TerminalLine {
                status: "PREPARED",
                evidence: receipt.request_path,
                detail: format!(
                    "stage=compose-request request_sha256={} product_invocations={}",
                    receipt.request_sha256, receipt.product_invocations
                ),
                exit_code: EXIT_PASS,
            }
            .emit())
        }
        Command::Prepare {
            request,
            plan,
            blocked_evidence,
        } => {
            let (request_model, request_bytes, request_identity) =
                strict_canonical_json::<Request>(&request, "composed request")?;
            let driver_executable = std::env::current_exe()?;
            let paths = PreparePaths {
                child_working_directory: request_model.repository.root.clone(),
                plan_root: plan,
                blocked_receipt_path: blocked_evidence.clone(),
                driver_executable,
                environment_expectations: EnvironmentExpectations::default(),
            };
            match prepare::prepare_canonical_request(
                &request_model,
                &request_bytes,
                request_identity,
                &paths,
            )? {
                PrepareOutcome::Prepared {
                    plan_root,
                    plan_sha256,
                    ..
                } => Ok(TerminalLine {
                    status: "PREPARED",
                    evidence: plan_root,
                    detail: format!("stage=prepare plan_sha256={plan_sha256}"),
                    exit_code: EXIT_PASS,
                }
                .emit()),
                PrepareOutcome::Blocked(receipt) => Ok(TerminalLine {
                    status: "BLOCKED",
                    evidence: blocked_evidence,
                    detail: format!("code={:?} {}", receipt.code, receipt.detail),
                    exit_code: EXIT_BLOCKED,
                }
                .emit()),
            }
        }
        Command::Execute { plan } => {
            let plan_path = plan_file(&plan);
            let (preliminary_plan, _, _) =
                strict_canonical_json::<Plan>(&plan_path, "sealed plan")?;
            let paths = (
                preliminary_plan.blocked_evidence,
                preliminary_plan.evidence_root,
            );
            match execute::execute(&plan_path) {
                Ok(status @ (TerminalStatus::Pass | TerminalStatus::Fail)) => {
                    let evidence = paths.1;
                    Ok(TerminalLine {
                        status: terminal_name(status),
                        evidence,
                        detail: "stage=execute authenticated terminal evidence".to_owned(),
                        exit_code: terminal_exit(status),
                    }
                    .emit())
                }
                Ok(status) => {
                    Err(format!("execution returned impossible terminal status {status:?}").into())
                }
                Err(ExecuteError::Blocked(detail)) => {
                    let evidence = paths.0;
                    Ok(TerminalLine {
                        status: "BLOCKED",
                        evidence,
                        detail,
                        exit_code: EXIT_BLOCKED,
                    }
                    .emit())
                }
                Err(error @ ExecuteError::Interrupted { .. }) => Err(Box::new(error)),
                Err(error) => {
                    if paths.1.exists() {
                        Ok(TerminalLine {
                            status: "INVALID",
                            evidence: paths.1,
                            detail: error.to_string(),
                            exit_code: EXIT_INVALID,
                        }
                        .emit())
                    } else {
                        Err(Box::new(error))
                    }
                }
            }
        }
        Command::Verify { plan, evidence } => match execute::verify(&plan_file(&plan), &evidence) {
            Ok(
                status @ (TerminalStatus::Pass | TerminalStatus::Fail | TerminalStatus::Invalid),
            ) => Ok(TerminalLine {
                status: terminal_name(status),
                evidence,
                detail: "stage=verify evidence authenticated".to_owned(),
                exit_code: terminal_exit(status),
            }
            .emit()),
            Ok(status) => Err(format!("evidence has impossible terminal status {status:?}").into()),
            Err(error) => Ok(TerminalLine {
                status: "INVALID",
                evidence,
                detail: error.to_string(),
                exit_code: EXIT_INVALID,
            }
            .emit()),
        },
        Command::CaptureSource {
            repository,
            output,
            task_spec,
        } => {
            let receipt = prepare::capture_repository_identity(&repository, &output, &task_spec)?;
            Ok(TerminalLine {
                status: "PREPARED",
                evidence: receipt.output_root,
                detail: format!(
                    "stage=capture-source head={} product_invocations={}",
                    receipt.repository.head, receipt.product_invocations
                ),
                exit_code: EXIT_PASS,
            }
            .emit())
        }
    }
}

fn terminal_name(status: TerminalStatus) -> &'static str {
    match status {
        TerminalStatus::Pass => "PASS",
        TerminalStatus::Fail => "FAIL",
        TerminalStatus::Invalid => "INVALID",
        TerminalStatus::Blocked => "BLOCKED",
        TerminalStatus::Ready => "PREPARED",
    }
}

fn terminal_exit(status: TerminalStatus) -> i32 {
    match status {
        TerminalStatus::Pass | TerminalStatus::Ready => EXIT_PASS,
        TerminalStatus::Invalid => EXIT_INVALID,
        TerminalStatus::Fail => EXIT_FAIL,
        TerminalStatus::Blocked => EXIT_BLOCKED,
    }
}

fn one_line(detail: &str) -> String {
    detail.replace(['\r', '\n'], " ")
}

fn main() {
    match run() {
        Ok(code) => std::process::exit(code),
        Err(error) if interrupted_signal(error.as_ref()).is_some() => {
            let signal = interrupted_signal(error.as_ref()).expect("matched managed signal");
            eprintln!("quic-fetch-acceptance: {}", one_line(&error.to_string()));
            // The execution guard has restored the original disposition before `run` returned.
            // Re-raise so shells and supervisors observe the original signal-derived status.
            unsafe {
                libc::raise(signal);
            }
            std::process::exit(128 + signal);
        }
        Err(error) => {
            eprintln!(
                "INVALID evidence=none detail={}",
                one_line(&error.to_string())
            );
            std::process::exit(EXIT_INVALID);
        }
    }
}

fn interrupted_signal(error: &(dyn std::error::Error + 'static)) -> Option<i32> {
    error
        .downcast_ref::<ExecuteError>()
        .and_then(|error| match error {
            ExecuteError::Interrupted { signal } => Some(*signal),
            _ => None,
        })
}

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use distd_bench::quic_fetch_acceptance::{
    environment::EnvironmentExpectations,
    execute::{self, ExecuteError},
    model::{Request, TerminalStatus},
    prepare::{self, PrepareOutcome, PreparePaths},
};

#[derive(Debug, Parser)]
#[command(
    name = "quic-fetch-acceptance",
    about = "Prepare, execute, and verify the frozen authenticated-Fetch acceptance protocol"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Validate inputs and seal an immutable, non-executing experiment plan.
    Prepare {
        /// JSON request describing the frozen artifacts and source identities.
        #[arg(long)]
        request: PathBuf,
        /// Absent path at which to atomically seal the plan.
        #[arg(long)]
        plan: PathBuf,
        /// Absent path reserved for BLOCKED preflight evidence.
        #[arg(long)]
        blocked_evidence: PathBuf,
    },
    /// Execute the fixed twelve-run protocol exactly once.
    Execute {
        /// Sealed plan created by `prepare`.
        #[arg(long)]
        plan: PathBuf,
    },
    /// Rehash evidence and independently recompute its terminal verdict.
    Verify {
        /// Sealed plan used by the experiment.
        #[arg(long)]
        plan: PathBuf,
        /// Completed evidence root named by the plan.
        #[arg(long)]
        evidence: PathBuf,
    },
    /// Capture the canonical repository identity required by a preparation request.
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

fn plan_file(path: &std::path::Path) -> PathBuf {
    if path.is_dir() {
        path.join("plan.json")
    } else {
        path.to_path_buf()
    }
}

fn run() -> Result<i32, Box<dyn std::error::Error>> {
    match Cli::parse().command {
        Command::Prepare {
            request,
            plan,
            blocked_evidence,
        } => {
            let request_bytes = std::fs::read(&request)?;
            let request: Request = serde_json::from_slice(&request_bytes)?;
            let driver_executable = std::env::current_exe()?;
            let paths = PreparePaths {
                child_working_directory: request.repository.root.clone(),
                plan_root: plan,
                blocked_receipt_path: blocked_evidence,
                driver_executable,
                environment_expectations: EnvironmentExpectations::default(),
            };
            match prepare::prepare(&request, &paths)? {
                PrepareOutcome::Prepared {
                    plan_root,
                    plan_sha256,
                    ..
                } => {
                    println!(
                        "PREPARED plan_root={} plan_sha256={plan_sha256}",
                        plan_root.display()
                    );
                    Ok(0)
                }
                PrepareOutcome::Blocked(receipt) => {
                    println!("BLOCKED code={:?} detail={}", receipt.code, receipt.detail);
                    Ok(3)
                }
            }
        }
        Command::Execute { plan } => match execute::execute(&plan_file(&plan)) {
            Ok(TerminalStatus::Pass) => Ok(0),
            Ok(TerminalStatus::Fail) => Ok(2),
            Ok(status) => {
                Err(format!("execution returned impossible terminal status {status:?}").into())
            }
            Err(ExecuteError::Blocked(detail)) => {
                println!("BLOCKED detail={detail}");
                Ok(3)
            }
            Err(error) => Err(Box::new(error)),
        },
        Command::Verify { plan, evidence } => {
            let status = execute::verify(&plan_file(&plan), &evidence)?;
            println!("VERIFIED status={status:?} evidence={}", evidence.display());
            Ok(match status {
                TerminalStatus::Pass => 0,
                TerminalStatus::Fail => 2,
                TerminalStatus::Invalid => 1,
                TerminalStatus::Ready | TerminalStatus::Blocked => {
                    return Err(
                        format!("evidence has impossible terminal status {status:?}").into(),
                    );
                }
            })
        }
        Command::CaptureSource {
            repository,
            output,
            task_spec,
        } => {
            let receipt = prepare::capture_repository_identity(&repository, &output, &task_spec)?;
            println!(
                "CAPTURED output={} head={} product_invocations={}",
                receipt.output_root.display(),
                receipt.repository.head,
                receipt.product_invocations
            );
            Ok(0)
        }
    }
}

fn main() {
    match run() {
        Ok(code) => std::process::exit(code),
        Err(error) if interrupted_signal(error.as_ref()).is_some() => {
            let signal = interrupted_signal(error.as_ref()).expect("matched managed signal");
            eprintln!("quic-fetch-acceptance: {error}");
            // The execution guard has restored the original disposition before `run` returned.
            // Re-raise so shells and supervisors observe the original signal-derived status.
            unsafe {
                libc::raise(signal);
            }
            std::process::exit(128 + signal);
        }
        Err(error) => {
            eprintln!("quic-fetch-acceptance: {error}");
            std::process::exit(1);
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

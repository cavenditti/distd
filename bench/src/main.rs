mod metrics;
mod orchestrator;
mod report;
mod runners;
mod workload;

use std::path::PathBuf;

use clap::Parser;
use orchestrator::BenchmarkOrchestrator;
use workload::WorkloadKind;

fn parse_workloads(names: Vec<String>) -> Result<Vec<WorkloadKind>, String> {
    names.into_iter().map(|name| name.parse()).collect()
}

#[derive(Parser, Debug)]
#[command(name = "distd-bench", about = "Benchmark suite for distd transfer performance")]
struct Cli {
    /// Output directory for benchmark results and artifacts
    #[arg(short, long, default_value = "bench_results")]
    output: PathBuf,

    /// Root directory for temporary workload data (defaults to system tmpdir)
    #[arg(long)]
    data_dir: Option<PathBuf>,

    /// Workloads to run (omit for all)
    #[arg(short, long, value_delimiter = ',')]
    workloads: Option<Vec<String>>,

    /// Tools to compare against (omit for all available)
    #[arg(short, long, value_delimiter = ',')]
    tools: Option<Vec<String>>,

    /// Number of iterations per benchmark scenario
    #[arg(short, long, default_value_t = 3)]
    iterations: u32,

    /// Run a quick smoke test with tiny datasets
    #[arg(long)]
    smoke: bool,

    /// Include cold-cache runs (drops filesystem caches between iterations where possible)
    #[arg(long)]
    cold_cache: bool,

    /// Include restart/resume scenario (kill client mid-transfer, restart)
    #[arg(long)]
    resume: bool,

    /// Single large file size in MiB for synthetic workloads
    #[arg(long, default_value_t = 256)]
    large_file_mib: u64,

    /// Number of small files for many-files workload
    #[arg(long, default_value_t = 1000)]
    small_file_count: u32,

    /// Small file size in KiB
    #[arg(long, default_value_t = 64)]
    small_file_kib: u64,

    /// Delta fraction (0.0-1.0) — fraction of content changed between revisions
    #[arg(long, default_value_t = 0.05)]
    delta_fraction: f64,

    /// List available tools and exit
    #[arg(long)]
    list_tools: bool,

    /// List available workloads and exit
    #[arg(long)]
    list_workloads: bool,

    /// Group result tables by "tool" (default) or "workload"
    #[arg(long, default_value = "tool")]
    group_by: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    tracing_subscriber::fmt()
        .with_target(false)
        .with_max_level(tracing::Level::INFO)
        .init();

    if cli.list_tools {
        println!("Available tools:");
        for tool in runners::all_tool_names() {
            let available = runners::is_tool_available(tool);
            let status = if available { "✓" } else { "✗ (not found)" };
            println!("  {status} {tool}");
        }
        return Ok(());
    }

    if cli.list_workloads {
        println!("Available workloads:");
        for wk in WorkloadKind::all() {
            println!("  {wk}");
        }
        return Ok(());
    }

    let data_dir = cli.data_dir.unwrap_or_else(|| {
        std::env::temp_dir().join("distd_bench")
    });

    let requested_tools: Vec<String> = cli.tools.unwrap_or_else(|| {
        runners::all_tool_names()
            .iter()
            .map(|s| (*s).to_string())
            .collect()
    });

    let requested_workloads: Vec<WorkloadKind> = match cli.workloads {
        Some(names) => parse_workloads(names)
            .map_err(|err| format!("invalid --workload value: {err}"))?,
        None => WorkloadKind::all(),
    };

    let group_by_workload = cli.group_by == "workload" || cli.group_by == "benchmark";

    let orchestrator = BenchmarkOrchestrator::new(
        data_dir,
        cli.output,
        requested_tools,
        requested_workloads,
        cli.iterations,
        cli.smoke,
        cli.cold_cache,
        cli.resume,
        cli.large_file_mib,
        cli.small_file_count,
        cli.small_file_kib,
        cli.delta_fraction,
        group_by_workload,
    );

    orchestrator.run().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::parse_workloads;
    use crate::workload::WorkloadKind;

    #[test]
    fn parses_multiple_workloads() {
        let workloads = parse_workloads(vec![
            "low-delta".to_string(),
            "high-delta".to_string(),
            "many-small-files".to_string(),
        ])
        .expect("workloads should parse");

        assert_eq!(
            workloads,
            vec![
                WorkloadKind::LowDeltaRevision,
                WorkloadKind::HighDeltaRevision,
                WorkloadKind::ManySmallFiles,
            ]
        );
    }

    #[test]
    fn rejects_unknown_workload() {
        let err = parse_workloads(vec!["low-delta".to_string(), "bogus".to_string()])
            .expect_err("unknown workload should fail");

        assert!(err.contains("bogus"));
    }
}

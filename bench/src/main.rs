mod metrics;
mod network;
mod orchestrator;
mod report;
mod runners;
mod workload;

use std::path::PathBuf;

use clap::Parser;
use network::NetworkProfile;
use orchestrator::BenchmarkOrchestrator;
use runners::RunnerOptions;
use workload::WorkloadKind;

fn parse_workloads(names: Vec<String>) -> Result<Vec<WorkloadKind>, String> {
    names.into_iter().map(|name| name.parse()).collect()
}

#[derive(Parser, Debug)]
#[command(
    name = "distd-bench",
    about = "Benchmark suite for distd transfer performance"
)]
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

    /// Add one-way latency in milliseconds to proxied benchmark traffic
    #[arg(long, default_value_t = 0)]
    net_delay_ms: u64,

    /// Add random jitter in milliseconds around the configured delay
    #[arg(long, default_value_t = 0)]
    net_jitter_ms: u64,

    /// Cap proxied throughput in megabits per second
    #[arg(long)]
    net_bandwidth_mbps: Option<f64>,

    /// Drop this percentage of proxied QUIC packets
    #[arg(long, default_value_t = 0.0)]
    net_loss_percent: f64,
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

    let data_dir = cli
        .data_dir
        .unwrap_or_else(|| std::env::temp_dir().join("distd_bench"));

    let requested_tools: Vec<String> = cli.tools.unwrap_or_else(|| {
        runners::all_tool_names()
            .iter()
            .map(|s| (*s).to_string())
            .collect()
    });

    let requested_workloads: Vec<WorkloadKind> = match cli.workloads {
        Some(names) => {
            parse_workloads(names).map_err(|err| format!("invalid --workload value: {err}"))?
        }
        None => WorkloadKind::all(),
    };

    let group_by_workload = cli.group_by == "workload" || cli.group_by == "benchmark";
    let network = NetworkProfile::new(
        cli.net_delay_ms,
        cli.net_jitter_ms,
        cli.net_bandwidth_mbps,
        cli.net_loss_percent,
    )?;
    let runner_options = RunnerOptions { network };

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
        runner_options,
    );

    orchestrator.run().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::parse_workloads;
    use crate::network::NetworkProfile;
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

    #[test]
    fn parses_format_specific_workloads() {
        let workloads = parse_workloads(vec![
            "tar.gz".to_string(),
            "oci-layer-zstd".to_string(),
            "deb".to_string(),
            "apk".to_string(),
        ])
        .expect("format workloads should parse");

        assert_eq!(
            workloads,
            vec![
                WorkloadKind::TarGzipArchive,
                WorkloadKind::OciLayerZstd,
                WorkloadKind::DebPackage,
                WorkloadKind::ApkPackage,
            ]
        );
    }

    #[test]
    fn builds_network_profile_from_cli_values() {
        let profile = NetworkProfile::new(25, 5, Some(10.0), 0.25)
            .expect("profile should parse")
            .expect("profile should be active");

        assert_eq!(
            profile.label(),
            "delay=25ms,jitter=5ms,bw=10.0Mbps,loss=0.25%"
        );
    }
}

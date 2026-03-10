//! Result reporting — CSV, JSON, and terminal summary output.

use std::path::Path;

use crate::metrics::RunMetrics;

/// Write results as CSV.
pub fn write_csv(results: &[RunMetrics], path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let mut wtr = csv::Writer::from_path(path)?;
    for r in results {
        wtr.serialize(r)?;
    }
    wtr.flush()?;
    tracing::info!("CSV results written to {}", path.display());
    Ok(())
}

/// Write results as JSON.
pub fn write_json(results: &[RunMetrics], path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let json = serde_json::to_string_pretty(results)?;
    std::fs::write(path, json)?;
    tracing::info!("JSON results written to {}", path.display());
    Ok(())
}

/// Print a human-readable summary table to stdout.
pub fn print_summary(results: &[RunMetrics]) {
    println!();
    println!("╔══════════════════════════════════════════════════════════════════════════════════════════════════════════════╗");
    println!("║                                    BENCHMARK RESULTS SUMMARY                                              ║");
    println!("╠════════════╦══════════════════════╦════════╦══════════╦═══════════╦══════════╦════════════╦════════╦════════╣");
    println!("║ Tool       ║ Workload             ║ Cache  ║ Time (s) ║ MiB/s     ║ Xferred  ║ Dest Size  ║ RSS MB ║ Valid  ║");
    println!("╠════════════╬══════════════════════╬════════╬══════════╬═══════════╬══════════╬════════════╬════════╬════════╣");

    for r in results {
        let xferred = format_bytes(r.bytes_transferred);
        let dest = format_bytes(r.dest_size_bytes);
        let rss = format!("{:.1}", r.peak_rss_bytes as f64 / (1024.0 * 1024.0));
        let valid = if r.correct { "✓" } else { "✗" };

        println!(
            "║ {:<10} ║ {:<20} ║ {:<6} ║ {:>8.3} ║ {:>9.2} ║ {:>8} ║ {:>10} ║ {:>6} ║ {:<6} ║",
            r.tool, r.workload, r.cache_state, r.wall_clock_secs, r.throughput_mibs,
            xferred, dest, rss, valid,
        );
    }

    println!("╚════════════╩══════════════════════╩════════╩══════════╩═══════════╩══════════╩════════════╩════════╩════════╝");
    println!();

    // Grouped summary: average per tool+workload
    print_grouped_averages(results);
}

fn print_grouped_averages(results: &[RunMetrics]) {
    use std::collections::BTreeMap;

    let mut groups: BTreeMap<(String, String), Vec<&RunMetrics>> = BTreeMap::new();
    for r in results {
        groups
            .entry((r.tool.clone(), r.workload.clone()))
            .or_default()
            .push(r);
    }

    println!("Averages by tool × workload:");
    println!("┌────────────┬──────────────────────┬───────┬──────────┬───────────┬──────────┐");
    println!("│ Tool       │ Workload             │ Runs  │ Avg (s)  │ Avg MiB/s │ Correct  │");
    println!("├────────────┼──────────────────────┼───────┼──────────┼───────────┼──────────┤");

    for ((tool, workload), runs) in &groups {
        let n = runs.len();
        let avg_time: f64 = runs.iter().map(|r| r.wall_clock_secs).sum::<f64>() / n as f64;
        let avg_throughput: f64 = runs.iter().map(|r| r.throughput_mibs).sum::<f64>() / n as f64;
        let all_correct = runs.iter().all(|r| r.correct);
        let correct_str = if all_correct {
            format!("{n}/{n} ✓")
        } else {
            let ok = runs.iter().filter(|r| r.correct).count();
            format!("{ok}/{n}")
        };

        println!(
            "│ {:<10} │ {:<20} │ {:>5} │ {:>8.3} │ {:>9.2} │ {:>8} │",
            tool, workload, n, avg_time, avg_throughput, correct_str,
        );
    }

    println!("└────────────┴──────────────────────┴───────┴──────────┴───────────┴──────────┘");
    println!();
}

fn format_bytes(bytes: u64) -> String {
    if bytes >= 1024 * 1024 * 1024 {
        format!("{:.1} GiB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    } else if bytes >= 1024 * 1024 {
        format!("{:.1} MiB", bytes as f64 / (1024.0 * 1024.0))
    } else if bytes >= 1024 {
        format!("{:.1} KiB", bytes as f64 / 1024.0)
    } else {
        format!("{bytes} B")
    }
}

/// Print fairness caveats about the benchmark setup.
pub fn print_fairness_caveats() {
    println!("╔══════════════════════════════════════════════════════════════════════════════╗");
    println!("║                             FAIRNESS CAVEATS                                ║");
    println!("╠══════════════════════════════════════════════════════════════════════════════╣");
    println!("║ • distd uses fixed 256 KiB chunking (no content-defined chunking yet)       ║");
    println!("║ • distd current server uses ephemeral in-memory storage (HashMapStorage)     ║");
    println!("║ • distd item model is single-file; multi-file workloads are partial          ║");
    println!("║ • distd client sends ALL known chunk hashes as diff basis (not per-item)     ║");
    println!("║ • rsync is measured via local filesystem (no actual network I/O)             ║");
    println!("║ • zsync requires single-file input; multi-file workloads use first file      ║");
    println!("║ • casync/ostree store overhead includes metadata beyond raw content          ║");
    println!("║ • Cold-cache tests require root (purge) on macOS; may silently skip          ║");
    println!("║ • All transfers are loopback; real network latency would change results      ║");
    println!("╚══════════════════════════════════════════════════════════════════════════════╝");
    println!();
}

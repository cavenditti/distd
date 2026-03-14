//! Result reporting — CSV, JSON, and terminal summary output.

use std::collections::BTreeMap;
use std::path::Path;

use comfy_table::modifiers::UTF8_ROUND_CORNERS;
use comfy_table::presets::UTF8_FULL;
use comfy_table::{Attribute, Cell, CellAlignment, Color, ContentArrangement, Table};

use crate::metrics::RunMetrics;

// ── Serialisation ───────────────────────────────────────────────────────────

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

// ── Terminal output ─────────────────────────────────────────────────────────

/// Print the full terminal report: detail table, averages with speed-up, caveats.
pub fn print_summary(results: &[RunMetrics], group_by_workload: bool) {
    println!();
    print_detail_table(results, group_by_workload);
    println!();
    print_averages_table(results, group_by_workload);
}

// ── Detail table ────────────────────────────────────────────────────────────

fn print_detail_table(results: &[RunMetrics], group_by_workload: bool) {
    // Sort results by the chosen grouping.
    let mut sorted: Vec<&RunMetrics> = results.iter().collect();
    if group_by_workload {
        sorted.sort_by(|a, b| {
            (&a.workload, &a.network_profile, &a.tool)
                .cmp(&(&b.workload, &b.network_profile, &b.tool))
        });
    } else {
        sorted.sort_by(|a, b| {
            (&a.tool, &a.network_profile, &a.workload)
                .cmp(&(&b.tool, &b.network_profile, &b.workload))
        });
    }

    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL)
        .apply_modifier(UTF8_ROUND_CORNERS)
        .set_content_arrangement(ContentArrangement::Dynamic)
        .set_header(vec![
            Cell::new("Tool").add_attribute(Attribute::Bold),
            Cell::new("Workload").add_attribute(Attribute::Bold),
            Cell::new("Network").add_attribute(Attribute::Bold),
            Cell::new("Cache").add_attribute(Attribute::Bold),
            Cell::new("Time (s)").add_attribute(Attribute::Bold),
            Cell::new("MiB/s").add_attribute(Attribute::Bold),
            Cell::new("Transferred").add_attribute(Attribute::Bold),
            Cell::new("Dest Size").add_attribute(Attribute::Bold),
            Cell::new("RSS Δ MB").add_attribute(Attribute::Bold),
            Cell::new("OK").add_attribute(Attribute::Bold),
        ]);

    // right-align numeric columns
    for col_idx in [4usize, 5, 6, 7, 8] {
        if let Some(col) = table.column_mut(col_idx) {
            col.set_cell_alignment(CellAlignment::Right);
        }
    }

    for r in &sorted {
        let valid = if r.correct {
            Cell::new("✓").fg(Color::Green)
        } else {
            Cell::new("✗").fg(Color::Red).add_attribute(Attribute::Bold)
        };

        table.add_row(vec![
            Cell::new(&r.tool),
            Cell::new(&r.workload),
            Cell::new(&r.network_profile),
            Cell::new(&r.cache_state),
            Cell::new(format!("{:.3}", r.wall_clock_secs)),
            Cell::new(format!("{:.2}", r.throughput_mibs)),
            Cell::new(format_bytes(r.bytes_transferred)),
            Cell::new(format_bytes(r.dest_size_bytes)),
            Cell::new(format!("{:.1}", r.peak_rss_bytes as f64 / (1024.0 * 1024.0))),
            valid,
        ]);
    }

    println!("\x1b[1;4mBENCHMARK RESULTS\x1b[0m");
    println!();
    println!("{table}");
}

// ── Averages + speed-up ─────────────────────────────────────────────────────

struct AvgRow {
    tool: String,
    workload: String,
    network_profile: String,
    runs: usize,
    avg_time: f64,
    avg_mibs: f64,
    correct: usize,
    total: usize,
}

fn print_averages_table(results: &[RunMetrics], group_by_workload: bool) {
    // Group by (tool, workload).
    let mut groups: BTreeMap<(String, String, String), Vec<&RunMetrics>> = BTreeMap::new();
    for r in results {
        groups
            .entry((r.tool.clone(), r.workload.clone(), r.network_profile.clone()))
            .or_default()
            .push(r);
    }

    let mut rows: Vec<AvgRow> = groups
        .iter()
        .map(|((tool, workload, network_profile), runs)| {
            let n = runs.len();
            AvgRow {
                tool: tool.clone(),
                workload: workload.clone(),
                network_profile: network_profile.clone(),
                runs: n,
                avg_time: runs.iter().map(|r| r.wall_clock_secs).sum::<f64>() / n as f64,
                avg_mibs: runs.iter().map(|r| r.throughput_mibs).sum::<f64>() / n as f64,
                correct: runs.iter().filter(|r| r.correct).count(),
                total: n,
            }
        })
        .collect();

    // Sort rows according to the chosen grouping.
    if group_by_workload {
        rows.sort_by(|a, b| {
            (&a.workload, &a.network_profile, &a.tool)
                .cmp(&(&b.workload, &b.network_profile, &b.tool))
        });
    } else {
        rows.sort_by(|a, b| {
            (&a.tool, &a.network_profile, &a.workload)
                .cmp(&(&b.tool, &b.network_profile, &b.workload))
        });
    }

    // Compute per-workload baseline throughput for the speed-up column.
    // distd is never the baseline — if it is the slowest, use the second-slowest
    // non-distd tool instead and show distd with a red multiplier (< 1.0×).
    let mut baseline_by_workload: BTreeMap<(&str, &str), f64> = BTreeMap::new();
    for r in &rows {
        if r.tool == "distd" || r.avg_mibs <= 0.0 {
            continue;
        }
        let entry = baseline_by_workload
            .entry((&r.workload, &r.network_profile))
            .or_insert(f64::MAX);
        if r.avg_mibs < *entry {
            *entry = r.avg_mibs;
        }
    }

    // Compute per-workload fastest (highest throughput) tool for bolding.
    let mut fastest_by_workload: BTreeMap<(&str, &str), &str> = BTreeMap::new();
    {
        let mut best_mibs: BTreeMap<(&str, &str), f64> = BTreeMap::new();
        for r in &rows {
            let entry = best_mibs
                .entry((&r.workload, &r.network_profile))
                .or_insert(0.0_f64);
            if r.avg_mibs > *entry {
                *entry = r.avg_mibs;
                fastest_by_workload.insert((&r.workload, &r.network_profile), &r.tool);
            }
        }
    }

    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL)
        .apply_modifier(UTF8_ROUND_CORNERS)
        .set_content_arrangement(ContentArrangement::Dynamic)
        .set_header(vec![
            Cell::new("Tool").add_attribute(Attribute::Bold),
            Cell::new("Workload").add_attribute(Attribute::Bold),
            Cell::new("Network").add_attribute(Attribute::Bold),
            Cell::new("Runs").add_attribute(Attribute::Bold),
            Cell::new("Avg (s)").add_attribute(Attribute::Bold),
            Cell::new("Avg MiB/s").add_attribute(Attribute::Bold),
            Cell::new("vs Slow").add_attribute(Attribute::Bold),
            Cell::new("Correct").add_attribute(Attribute::Bold),
        ]);

    // right-align numeric columns
    for col_idx in [3usize, 4, 5, 6, 7] {
        if let Some(col) = table.column_mut(col_idx) {
            col.set_cell_alignment(CellAlignment::Right);
        }
    }

    for r in &rows {
        let is_fastest = fastest_by_workload
            .get(&(r.workload.as_str(), r.network_profile.as_str()))
            .map(|&t| t == r.tool)
            .unwrap_or(false);

        let speedup = baseline_by_workload
            .get(&(r.workload.as_str(), r.network_profile.as_str()))
            .filter(|&&w| w > 0.0 && r.avg_mibs > 0.0)
            .map(|w| r.avg_mibs / w)
            .unwrap_or(1.0);

        let speedup_cell = if r.tool == "distd" && speedup < 1.0 {
            // distd is slower than the baseline — highlight in red
            Cell::new(format!("{speedup:.2}×"))
                .fg(Color::Red)
                .add_attribute(Attribute::Bold)
        } else if speedup >= 1.5 {
            Cell::new(format!("{speedup:.1}×"))
                .fg(Color::Green)
                .add_attribute(Attribute::Bold)
        } else if r.tool != "distd" && (speedup - 1.0).abs() < 0.05 {
            Cell::new("base").fg(Color::DarkGrey)
        } else {
            Cell::new(format!("{speedup:.1}×"))
        };

        let correct_str = if r.correct == r.total {
            format!("{}/{} ✓", r.correct, r.total)
        } else {
            format!("{}/{}", r.correct, r.total)
        };

        let correct_cell = if r.correct == r.total {
            Cell::new(&correct_str).fg(Color::Green)
        } else {
            Cell::new(&correct_str)
                .fg(Color::Red)
                .add_attribute(Attribute::Bold)
        };

        let bold = |c: Cell| -> Cell {
            if is_fastest {
                c.add_attribute(Attribute::Bold)
            } else {
                c
            }
        };

        table.add_row(vec![
            bold(Cell::new(&r.tool)),
            bold(Cell::new(&r.workload)),
            bold(Cell::new(&r.network_profile)),
            bold(Cell::new(r.runs)),
            bold(Cell::new(format!("{:.3}", r.avg_time))),
            bold(Cell::new(format!("{:.2}", r.avg_mibs))),
            speedup_cell,
            correct_cell,
        ]);
    }

    println!("\x1b[1;4mAVERAGES BY TOOL × WORKLOAD\x1b[0m");
    println!();
    println!("{table}");
}

// ── Fairness caveats ────────────────────────────────────────────────────────

/// Print fairness caveats about the benchmark setup.
pub fn print_fairness_caveats() {
    let caveats = [
        "distd now supports fixed and content-defined chunking, but FastCDC profile tuning is still in progress",
        "distd benchmark server uses ephemeral filesystem storage (FsStorage)",
        "distd multi-file artifact path is experimental; bulk mode now covers cold full-fetches but not partial updates",
        "distd sync path uses ordered leaf hashes to build possession bitfields",
        "http is a naïve baseline — full-file download with no delta support",
        "rsync is measured via local filesystem (no actual network I/O)",
        "zsync requires single-file input; multi-file workloads use first file",
        "casync/ostree store overhead includes metadata beyond raw content",
        "Cold-cache tests require root (purge) on macOS; may silently skip",
        "All transfers are loopback; real network latency would change results",
    ];

    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL)
        .apply_modifier(UTF8_ROUND_CORNERS)
        .set_content_arrangement(ContentArrangement::Dynamic)
        .set_header(vec![Cell::new("FAIRNESS CAVEATS")
            .add_attribute(Attribute::Bold)
            .fg(Color::Yellow)]);

    for c in &caveats {
        table.add_row(vec![Cell::new(format!("• {c}"))]);
    }

    println!("{table}");
}

// ── Helpers ─────────────────────────────────────────────────────────────────

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

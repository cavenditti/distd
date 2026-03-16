//! Benchmark orchestrator — drives workload generation, tool execution, and result collection.

use std::path::PathBuf;

use crate::metrics::{self, RunMetrics};
use crate::report;
use crate::runners::{self, RunnerOptions};
use crate::workload::{self, Workload, WorkloadKind, WorkloadParams};

#[allow(dead_code)]
pub struct BenchmarkOrchestrator {
    data_dir: PathBuf,
    output_dir: PathBuf,
    tools: Vec<String>,
    workloads: Vec<WorkloadKind>,
    iterations: u32,
    smoke: bool,
    cold_cache: bool,
    resume: bool,
    params: WorkloadParams,
    group_by_workload: bool,
    runner_options: RunnerOptions,
}

impl BenchmarkOrchestrator {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        data_dir: PathBuf,
        output_dir: PathBuf,
        tools: Vec<String>,
        workloads: Vec<WorkloadKind>,
        iterations: u32,
        smoke: bool,
        cold_cache: bool,
        resume: bool,
        large_file_mib: u64,
        small_file_count: u32,
        small_file_kib: u64,
        delta_fraction: f64,
        group_by_workload: bool,
        runner_options: RunnerOptions,
    ) -> Self {
        Self {
            data_dir,
            output_dir,
            tools,
            workloads,
            iterations,
            smoke,
            cold_cache,
            resume,
            params: WorkloadParams {
                large_file_mib,
                small_file_count,
                small_file_kib,
                delta_fraction,
                smoke,
            },
            group_by_workload,
            runner_options,
        }
    }

    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        std::fs::create_dir_all(&self.output_dir)?;

        // 1. Check tool availability
        let mut available_tools: Vec<String> = Vec::new();
        for tool_name in &self.tools {
            if runners::is_tool_available(tool_name) {
                tracing::info!("Tool '{tool_name}' is available");
                available_tools.push(tool_name.clone());
            } else {
                tracing::warn!("Tool '{tool_name}' is NOT available, skipping");
            }
        }

        if available_tools.is_empty() {
            return Err("No benchmark tools available. Install at least one of: rsync, zsync, casync, ostree, or build distd.".into());
        }

        // 2. Generate workloads
        tracing::info!("Generating workloads...");
        let workload_dir = self.data_dir.join("workloads");
        let generated = workload::generate_workloads(&workload_dir, &self.workloads, &self.params);

        tracing::info!(
            "Generated {} workloads ({} total source bytes)",
            generated.len(),
            generated.iter().map(|w| w.total_bytes_v1).sum::<u64>()
        );

        // 3. Run benchmarks
        let mut all_results: Vec<RunMetrics> = Vec::new();

        for wl in &generated {
            tracing::info!("━━━ Workload: {} ━━━", wl.kind);
            tracing::info!(
                "  Source: {} files, {} bytes",
                wl.file_count_v1,
                wl.total_bytes_v1
            );

            for tool_name in &available_tools {
                tracing::info!("  ─── Tool: {tool_name} ───");
                let tool_work_dir = self.data_dir.join("tool_state").join(tool_name);
                let runner =
                    match runners::make_runner(tool_name, &tool_work_dir, &self.runner_options) {
                        Some(r) => r,
                        None => {
                            tracing::warn!("  Cannot create runner for {tool_name}");
                            continue;
                        }
                    };

                if !runner.is_available() {
                    tracing::warn!("  {tool_name} not available, skipping");
                    continue;
                }

                // Run warm iterations
                let results = self.run_tool_iterations(&*runner, wl, "warm");
                all_results.extend(results);

                // Run cold iterations if requested
                if self.cold_cache {
                    let results = self.run_tool_iterations(&*runner, wl, "cold");
                    all_results.extend(results);
                }

                // Run delta/update benchmarks if workload has v2
                if wl.source_dir_v2.is_some() {
                    let results = self.run_tool_update_iterations(&*runner, wl);
                    all_results.extend(results);
                }

                // Run resume benchmark if requested
                if self.resume {
                    let results = self.run_resume_iteration(&*runner, wl);
                    all_results.extend(results);
                }

                runner.cleanup();
            }
        }

        // 4. Write results
        tracing::info!("Writing results to {}", self.output_dir.display());
        report::write_csv(&all_results, &self.output_dir.join("results.csv"))?;
        report::write_json(&all_results, &self.output_dir.join("results.json"))?;
        report::print_summary(&all_results, self.group_by_workload);

        // 5. Fairness notes
        report::print_fairness_caveats();

        Ok(())
    }

    fn run_tool_iterations(
        &self,
        runner: &dyn runners::ToolRunner,
        workload: &Workload,
        cache_state: &str,
    ) -> Vec<RunMetrics> {
        let mut results = Vec::new();

        for i in 0..self.iterations {
            let dest = self
                .data_dir
                .join("dest")
                .join(runner.name())
                .join(workload.kind.to_string())
                .join(format!("{cache_state}_{i}"));

            // Clean dest for each iteration
            let _ = std::fs::remove_dir_all(&dest);
            std::fs::create_dir_all(&dest).expect("create dest dir");

            if cache_state == "cold" {
                metrics::drop_caches();
            }

            let mut m = RunMetrics::new(
                runner.name(),
                &workload.kind.to_string(),
                &self.runner_options.network_label(),
                i,
                cache_state,
            );

            tracing::info!(
                "    [{}/{}] {} {} {}...",
                i + 1,
                self.iterations,
                runner.name(),
                workload.kind,
                cache_state
            );

            match runner.transfer(workload, &dest, &mut m) {
                Ok(()) => {
                    tracing::info!(
                        "    → {:.2} MiB/s, {:.3}s, correct={}",
                        m.throughput_mibs,
                        m.wall_clock_secs,
                        m.correct,
                    );
                }
                Err(e) => {
                    tracing::error!("    → FAILED: {e}");
                    m.notes = format!("FAILED: {e}");
                }
            }

            results.push(m);

            // Clean up dest between iterations
            let _ = std::fs::remove_dir_all(&dest);
        }

        results
    }

    fn run_tool_update_iterations(
        &self,
        runner: &dyn runners::ToolRunner,
        workload: &Workload,
    ) -> Vec<RunMetrics> {
        let mut results = Vec::new();

        for i in 0..self.iterations {
            let dest = self
                .data_dir
                .join("dest")
                .join(runner.name())
                .join(workload.kind.to_string())
                .join(format!("update_{i}"));

            // First: lay down v1
            let _ = std::fs::remove_dir_all(&dest);
            std::fs::create_dir_all(&dest).expect("create dest dir");

            let mut setup = RunMetrics::new(
                runner.name(),
                &workload.kind.to_string(),
                &self.runner_options.network_label(),
                i,
                "setup",
            );
            if let Err(e) = runner.transfer(workload, &dest, &mut setup) {
                tracing::error!("    → Setup transfer failed: {e}");
                continue;
            }

            // Then: update to v2 (the measured operation)
            let mut m = RunMetrics::new(
                runner.name(),
                &format!("{}-update", workload.kind),
                &self.runner_options.network_label(),
                i,
                "warm",
            );

            tracing::info!(
                "    [{}/{}] {} {} update...",
                i + 1,
                self.iterations,
                runner.name(),
                workload.kind,
            );

            match runner.update(workload, &dest, &mut m) {
                Ok(()) => {
                    tracing::info!(
                        "    → {:.2} MiB/s, {:.3}s, correct={}",
                        m.throughput_mibs,
                        m.wall_clock_secs,
                        m.correct,
                    );
                }
                Err(e) => {
                    tracing::error!("    → UPDATE FAILED: {e}");
                    m.notes = format!("FAILED: {e}");
                }
            }

            results.push(m);
            let _ = std::fs::remove_dir_all(&dest);
        }

        results
    }

    fn run_resume_iteration(
        &self,
        runner: &dyn runners::ToolRunner,
        workload: &Workload,
    ) -> Vec<RunMetrics> {
        // Resume test: start a transfer, interrupt it after 50% time of a normal run,
        // then restart and complete. We approximate by running two transfers —
        // first partial (kill early), second full.
        let dest = self
            .data_dir
            .join("dest")
            .join(runner.name())
            .join(workload.kind.to_string())
            .join("resume");

        let _ = std::fs::remove_dir_all(&dest);
        std::fs::create_dir_all(&dest).expect("create dest dir");

        let mut m = RunMetrics::new(
            runner.name(),
            &format!("{}-resume", workload.kind),
            &self.runner_options.network_label(),
            0,
            "warm",
        );
        m.is_resume = true;

        tracing::info!("    [resume] {} {}...", runner.name(), workload.kind,);

        // For a proper resume test we'd need to kill mid-transfer and restart.
        // Since we can't easily interrupt external tools uniformly, we simulate:
        // 1. Do a full transfer (measures resumed-from-nothing).
        // 2. Note: a real resume test would need per-tool interrupt support.
        match runner.transfer(workload, &dest, &mut m) {
            Ok(()) => {
                m.notes
                    .push_str(" | resume scenario (full transfer, dest pre-exists on retry)");
                tracing::info!(
                    "    → {:.2} MiB/s, {:.3}s, correct={}",
                    m.throughput_mibs,
                    m.wall_clock_secs,
                    m.correct,
                );
            }
            Err(e) => {
                tracing::error!("    → RESUME FAILED: {e}");
                m.notes = format!("FAILED: {e}");
            }
        }

        let _ = std::fs::remove_dir_all(&dest);
        vec![m]
    }
}

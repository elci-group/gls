//! `gls` — glass: a progressive, animated, metadata-driven filesystem
//! intelligence surface.
//!
//! # Pipeline
//!
//! ```text
//! DISCOVER → [TYPE ∥ BART] → INDEX → BOUND → CONTEXT → METRICS
//! ```
//!
//! Discovery events are emitted immediately so names appear at T0.
//! TYPE and BART run concurrently.  INDEX and CONTEXT follow after.
mod curly_expand;


use std::io::IsTerminal;
use std::path::{Path, PathBuf};
use std::time::Duration;

use clap::{ArgAction, Parser, Subcommand, ValueEnum};
use gls_bart::measure_entry;
use gls_bound::{select_candidates, Bound};
use gls_context::{ContextConfig, Contextualiser};
use gls_core::entry::FsEntry;
use gls_core::event::PipelineEvent;
use gls_core::pipeline;
use gls_discover::DiscoverConfig;
use gls_index::build_descriptor;
use gls_padagonia::{Padagonia, PadagoniaConfig};
use gls_render::{OutputMode, RenderConfig, Renderer};
use gls_type::classify_entry;

const ANALYSIS_CACHE_VERSION: &str = "gls-analysis-v2-full-content-hash";

mod menu;
mod provider_cmd;

// ── CLI ───────────────────────────────────────────────────────────────────────

#[derive(Clone, Debug, Parser)]
#[command(
    name  = "gls",
    about = "glass — filesystem meaning, progressively revealed",
    long_about = None,
)]
struct Cli {
    /// Subcommand (omit to run a normal scan).
    #[command(subcommand)]
    command: Option<Command>,

    /// Target path to inspect.
    #[arg(default_value = ".", global = false)]
    target: PathBuf,

    /// Recursion depth (0 = root only, 1 = direct children, …).
    #[arg(short = 'd', long, default_value_t = 1)]
    depth: usize,

    /// Include hidden entries (names starting with `.`).
    #[arg(long, action = ArgAction::SetTrue)]
    hidden: bool,

    /// Disable animation (static output).
    #[arg(long = "no-animate", action = ArgAction::SetTrue, conflicts_with = "animate")]
    no_animate: bool,

    /// Force animation even when stdout is not a TTY.
    #[arg(long = "animate", action = ArgAction::SetTrue)]
    animate: bool,

    /// Print the final listing as one entry per line instead of a
    /// horizontally-filled, dynamically-categorised grid.
    #[arg(long = "no-grid", action = ArgAction::SetTrue, conflicts_with = "grid")]
    no_grid: bool,

    /// Force the grid layout even when stdout is not a TTY.
    #[arg(long = "grid", action = ArgAction::SetTrue)]
    grid: bool,

    /// Skip remote AI contextualisation (the default; retained for compatibility).
    #[arg(long = "no-context", action = ArgAction::SetTrue, conflicts_with = "context")]
    no_context: bool,

    /// Enable remote AI contextualisation of non-credential file excerpts.
    #[arg(long = "context", action = ArgAction::SetTrue)]
    context: bool,

    /// Refresh remote context even when the persisted descriptor is unchanged.
    #[arg(long, action = ArgAction::SetTrue, requires = "context")]
    refresh_context: bool,

    /// Deep scan: increase depth and contextualisation budget.
    #[arg(long, action = ArgAction::SetTrue)]
    deep: bool,

    /// Output mode.
    #[arg(long = "output", value_enum, default_value_t = OutputArg::Auto)]
    output: OutputArg,

    /// Sort order for displayed entries.
    #[arg(long, value_enum, default_value_t = SortOrder::Name)]
    sort: SortOrder,

    /// Groq model to use for contextualisation.
    /// Defaults to the GLS_GROQ_MODEL env var, then the current recommended model.
    #[arg(long)]
    model: Option<String>,
}

#[derive(Debug, Clone, Subcommand)]
enum Command {
    /// Show contextualisation history for a path from Padagonia.
    History {
        /// Path to query.
        path: PathBuf,
        /// Maximum number of revisions to show (default: all).
        #[arg(short = 'n', long)]
        limit: Option<usize>,
    },
    /// Interactively choose an inference provider and model.
    ///
    /// Model availability is resolved by polling the provider's real
    /// model-listing API, cached persistently in Padagonia with a 1-hour
    /// TTL. The selection is saved for reference by future runs.
    Provider {
        /// Bypass the cache and force a fresh poll of model availability.
        #[arg(long)]
        refresh: bool,
    },
    /// Show BART's recorded size breakdown for a directory from Padagonia:
    /// its own recursively-aggregated total, and each direct child's.
    Size {
        /// Path to query.
        path: PathBuf,
    },
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum OutputArg {
    /// Auto-detect (animated TTY / plain pipe).
    Auto,
    /// Plain text, no ANSI codes.
    Plain,
    /// One JSON object per pipeline event.
    Json,
    /// Animated TTY output.
    Tty,
}

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
enum SortOrder {
    /// Alphabetical by name (default).
    Name,
    /// Largest first (by logical bytes).
    Size,
    /// Group by semantic type.
    Type,
    /// Most recently modified first.
    Modified,
}

// ── Entry point ───────────────────────────────────────────────────────────────

#[tokio::main]
async fn __curly_original_main() {
    let cli = Cli::parse();
    let __curly_target: Vec<String> = curly_expand::expand_or_literal(cli.target.to_string_lossy().as_ref());
    let __curly_model: Vec<Option<String>> = match &cli.model { Some(v) => curly_expand::expand_or_literal(v).into_iter().map(Some).collect(), None => vec![None] };
    for __curly_v_target in &__curly_target {
    for __curly_v_model in &__curly_model {
        let mut cli = cli.clone();
        cli.target = std::path::PathBuf::from(__curly_v_target.clone());
        cli.model = __curly_v_model.clone();
    {

    // ── Subcommands ────────────────────────────────────────────────────────────
    if let Some(cmd) = &cli.command {
        match cmd {
            Command::History { path, limit } => {
                run_history(path, *limit);
                return;
            }
            Command::Provider { refresh } => {
                provider_cmd::run(*refresh).await;
                return;
            }
            Command::Size { path } => {
                run_size(path);
                return;
            }
        }
    }

    // ── Bound ─────────────────────────────────────────────────────────────────
    let bound = Bound {
        depth: if cli.deep {
            cli.depth.max(3)
        } else {
            cli.depth
        },
        no_context: cli.no_context || !cli.context,
        incremental: !cli.refresh_context,
        max_context_candidates: if cli.deep { 256 } else { 64 },
        inference_budget_bytes: if cli.deep { 1_024_000 } else { 256_000 },
        ..Default::default()
    };

    // ── Pipeline broadcast channel ─────────────────────────────────────────────
    let (tx, rx_render) = pipeline::channel();
    let rx_padagonia = tx.subscribe();

    // ── Renderer output mode ───────────────────────────────────────────────────
    let stdout_is_tty = std::io::stdout().is_terminal();
    let output_mode = match cli.output {
        OutputArg::Json => OutputMode::Json,
        OutputArg::Plain => OutputMode::Plain,
        OutputArg::Tty => OutputMode::Tty,
        OutputArg::Auto => {
            if cli.animate || (stdout_is_tty && !cli.no_animate) {
                OutputMode::Tty
            } else {
                OutputMode::Plain
            }
        }
    };

    let tick =
        if cli.no_animate || output_mode == OutputMode::Plain || output_mode == OutputMode::Json {
            None
        } else {
            Some(Duration::from_millis(600))
        };

    let grid = if cli.no_grid {
        false
    } else if cli.grid {
        true
    } else {
        output_mode == OutputMode::Tty && stdout_is_tty
    };

    let render_config = RenderConfig {
        mode: output_mode,
        tick_interval: tick,
        grid,
    };

    // ── Padagonia ──────────────────────────────────────────────────────────────
    let padagonia = Padagonia::open(&PadagoniaConfig::default()).unwrap_or_else(|e| {
        eprintln!("[warn] padagonia unavailable: {e}");
        Padagonia::open(&PadagoniaConfig {
            db_path: PathBuf::from(":memory:"),
        })
        .expect("in-memory SQLite must always work")
    });
    // Keep clones for deterministic snapshot queries, context filtering, and
    // persisting BART's recursive size breakdown.
    let padagonia_query = padagonia.clone();
    let padagonia_sizes = padagonia.clone();

    // ── Spawn renderer and Padagonia consumer ──────────────────────────────────
    let renderer = Renderer::new(render_config);
    let render_task = tokio::spawn(async move { renderer.run(rx_render).await });
    let tx_padagonia = tx.clone();
    let padagonia_task =
        tokio::spawn(async move { padagonia.consume_stream(rx_padagonia, tx_padagonia).await });

    // ── DISCOVER ───────────────────────────────────────────────────────────────
    // Collect entries on a private channel first (we need the Vec for later
    // phases), then immediately forward Discovery events to the main channel
    // so the renderer shows names at T0.
    let target = cli.target.canonicalize().unwrap_or(cli.target.clone());

    let discover_config = DiscoverConfig {
        max_depth: bound.depth,
        follow_links: false,
        include_hidden: cli.hidden,
    };
    let mut entries = gls_discover::discover(target.clone(), discover_config, tx.clone()).await;

    // ── SORT ───────────────────────────────────────────────────────────────────
    sort_entries(&mut entries, cli.sort);

    let _ = tx.send(PipelineEvent::OrderUpdated {
        paths: entries.iter().map(|entry| entry.path.clone()).collect(),
    });

    // ── CACHE PARTITION ────────────────────────────────────────────────────────
    // Stat-keyed hits are replayed through the ordinary event stream. Only
    // misses continue into TYPE/BART/INDEX, avoiding redundant file reads,
    // recursive directory walks, and descriptor construction.
    let mut cached = std::collections::HashMap::new();
    let mut misses = Vec::new();
    for entry in &entries {
        match padagonia_query.lookup_snapshot(entry, ANALYSIS_CACHE_VERSION, &target) {
            Some(snapshot) => {
                cached.insert(entry.path.clone(), snapshot);
            }
            None => misses.push(entry.path.clone()),
        }
    }

    // A visible changed descendant invalidates cached directory ancestors: a
    // directory's BART aggregate includes that descendant even when its own
    // inode metadata did not change.
    let ancestor_misses: Vec<_> = cached
        .iter()
        .filter(|(path, snapshot)| {
            snapshot.entry.kind == gls_core::entry::EntryKind::Directory
                && misses.iter().any(|changed| changed != *path && changed.starts_with(path))
        })
        .map(|(path, _)| path.clone())
        .collect();
    for path in ancestor_misses {
        cached.remove(&path);
        misses.push(path);
    }

    let mut classifications = std::collections::HashMap::new();
    let mut measurements = std::collections::HashMap::new();
    let mut descriptor_map = std::collections::HashMap::new();
    for entry in &entries {
        if let Some(snapshot) = cached.get(&entry.path) {
            let _ = tx.send(PipelineEvent::CacheStatus { path: entry.path.clone(), hit: true });
            let _ = tx.send(PipelineEvent::TypeDetected {
                path: entry.path.clone(),
                classification: snapshot.classification.clone(),
            });
            let _ = tx.send(PipelineEvent::Measured(snapshot.measurement.clone()));
            let _ = tx.send(PipelineEvent::Indexed(snapshot.descriptor.clone()));
            if let Some(context) = &snapshot.context {
                let _ = tx.send(PipelineEvent::ContextUpdated(context.clone()));
            }
            if let Some(metrics) = &snapshot.metrics {
                let _ = tx.send(PipelineEvent::MetricsUpdated(metrics.clone()));
            }
            classifications.insert(entry.path.clone(), snapshot.classification.clone());
            measurements.insert(entry.path.clone(), snapshot.measurement.clone());
            descriptor_map.insert(entry.path.clone(), snapshot.descriptor.clone());
        } else {
            let _ = tx.send(PipelineEvent::CacheStatus { path: entry.path.clone(), hit: false });
        }
    }

    let changed_entries: Vec<_> = entries
        .iter()
        .filter(|entry| !cached.contains_key(&entry.path))
        .cloned()
        .collect();

    // ── TYPE ∥ BART (MISSES ONLY) ─────────────────────────────────────────────
    let measurement_requests: Vec<(PathBuf, usize)> = changed_entries
        .iter()
        .map(|entry| {
            // BART is a fast, deterministic local walk; for every directory
            // that is actually displayed we want its true recursive tree size,
            // not a depth-limited approximation that collapses to 0 B for
            // directories that contain only subdirectories.
            (entry.path.clone(), usize::MAX)
        })
        .collect();

    let tx_type = tx.clone();
    let tx_bart = tx.clone();
    let entries_clone = changed_entries.clone();

    let (classification_results, measurement_results) = tokio::join!(
        classify_entry(entries_clone, tx_type),
        measure_entry(measurement_requests, tx_bart),
    );

    classifications.extend(classification_results);
    measurements.extend(
        measurement_results
            .iter()
            .map(|result| (result.measure.path.clone(), result.measure.clone())),
    );
    let aggregates: Vec<_> = measurement_results
        .into_iter()
        .flat_map(|result| result.aggregates)
        .collect();
    if let Err(error) = padagonia_sizes.store_size_aggregates(&aggregates) {
        eprintln!("[warn] could not persist size aggregates: {error}");
    }

    // ── INDEX ──────────────────────────────────────────────────────────────────
    // Build descriptors with type + physical data attached; forward events.
    let descriptor_items = changed_entries
        .iter()
        .map(|entry| {
            (
                entry.path.clone(),
                classifications.get(&entry.path).cloned(),
                measurements.get(&entry.path).cloned(),
            )
        })
        .collect();
    let rebuilt_descriptors = build_descriptor(descriptor_items, bound.excerpt_bytes, tx.clone()).await;
    descriptor_map.extend(
        rebuilt_descriptors
            .into_iter()
            .map(|descriptor| (descriptor.path.clone(), descriptor)),
    );

    // Persist fresh deterministic snapshots before optional inference. Their
    // descriptors later join context revisions by descriptor hash.
    for entry in &changed_entries {
        let Some(classification) = classifications.get(&entry.path) else { continue };
        let Some(measurement) = measurements.get(&entry.path) else { continue };
        let Some(descriptor) = descriptor_map.get(&entry.path) else { continue };
        if let Err(error) = padagonia_query.store_snapshot(
            entry,
            classification,
            measurement,
            descriptor,
            ANALYSIS_CACHE_VERSION,
        ) {
            eprintln!("[warn] could not persist metadata snapshot: {error}");
        }
    }

    let descriptors: Vec<_> = entries
        .iter()
        .filter_map(|entry| descriptor_map.get(&entry.path).cloned())
        .collect();

    // A metadata-only change may rebuild to the same descriptor. Hydrate its
    // existing contextual insight now, without remote inference or a duplicate
    // history revision.
    for descriptor in &descriptors {
        if cached.contains_key(&descriptor.path) {
            continue;
        }
        if let Some((context, metrics)) =
            padagonia_query.cached_context(&descriptor.path, &descriptor.descriptor_hash)
        {
            let _ = tx.send(PipelineEvent::CacheHydrated { path: descriptor.path.clone() });
            let _ = tx.send(PipelineEvent::ContextUpdated(context));
            if let Some(metrics) = metrics {
                let _ = tx.send(PipelineEvent::MetricsUpdated(metrics));
            }
        }
    }

    // ── CONTEXT ────────────────────────────────────────────────────────────────
    if !bound.no_context {
        let weighted: Vec<_> = descriptors
            .into_iter()
            // Credential-like files are never sent to a remote provider.
            .filter(|d| {
                classifications
                    .get(&d.path)
                    .map(|classification| {
                        classification.semantic_type != gls_core::semantic::SemanticType::Credential
                    })
                    .unwrap_or(true)
            })
            // When incremental mode is on, skip objects whose descriptor_hash
            // matches the latest Padagonia record (content unchanged since last run).
            .filter(|d| {
                if bound.incremental {
                    !padagonia_query.is_cache_hit(&d.path, &d.descriptor_hash)
                } else {
                    true
                }
            })
            .map(|d| {
                let weight = serde_json::to_vec(&d.payload)
                    .map(|v| v.len() as u64)
                    .unwrap_or(0);
                (d, weight)
            })
            .collect();

        let candidates = select_candidates(weighted, &bound);

        let mut ctx_config = ContextConfig::default();
        if let Some(ref m) = cli.model {
            ctx_config.model = m.clone();
        } else if let Some((provider, model, _)) = padagonia_query.selected_provider() {
            if provider == "groq" {
                ctx_config.model = model;
            }
        }

        let contextualiser = Contextualiser::new(ctx_config);
        contextualiser.contextualise(candidates, tx.clone()).await;
    }

    // ── Done ───────────────────────────────────────────────────────────────────
    let _ = tx.send(PipelineEvent::PipelineComplete);
    let _ = tokio::join!(render_task, padagonia_task);
}
    }
    }
}

/// Sort a mutable entry list in-place according to the requested order.
fn sort_entries(entries: &mut [FsEntry], order: SortOrder) {
    match order {
        SortOrder::Name => {
            entries.sort_by_key(|entry| entry.basename.to_lowercase());
        }
        SortOrder::Size => {
            entries.sort_by_key(|entry| std::cmp::Reverse(entry.logical_bytes));
        }
        SortOrder::Type => {
            // Stable sort: alphabetical within each kind bucket.
            entries.sort_by(|a, b| {
                let ka = kind_bucket(a);
                let kb = kind_bucket(b);
                ka.cmp(&kb)
                    .then_with(|| a.basename.to_lowercase().cmp(&b.basename.to_lowercase()))
            });
        }
        SortOrder::Modified => {
            entries.sort_by_key(|entry| std::cmp::Reverse(entry.modified));
        }
    }
}

/// Numeric bucket for `SortOrder::Type` grouping (directories first).
fn kind_bucket(e: &FsEntry) -> u8 {
    use gls_core::entry::EntryKind;
    match e.kind {
        EntryKind::Directory => 0,
        EntryKind::File => 1,
        EntryKind::Symlink => 2,
        EntryKind::Other => 3,
    }
}

/// Print Padagonia contextualisation history for a single path.
fn run_history(path: &Path, limit: Option<usize>) {
    let padagonia = match Padagonia::open(&PadagoniaConfig::default()) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("[error] cannot open Padagonia: {e}");
            std::process::exit(1);
        }
    };

    let canonical = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
    let revisions = padagonia.history(&canonical);

    if revisions.is_empty() {
        println!("No history for: {}", canonical.display());
        return;
    }

    let revisions: Vec<_> = if let Some(n) = limit {
        revisions.into_iter().take(n).collect()
    } else {
        revisions
    };

    println!("History for: {}", canonical.display());
    println!("{:<38}  timestamp", "revision");
    println!("{}", "-".repeat(60));
    for (rev, ts) in &revisions {
        println!("{rev}  {ts}");
    }
}

/// Print BART's recorded recursive size breakdown for a path, from
/// Padagonia: its own aggregate, then each direct child sorted largest
/// first. Requires the path to have been measured by a prior `gls` run —
/// this reads the persisted record, it doesn't re-walk the filesystem.
fn run_size(path: &Path) {
    let padagonia = match Padagonia::open(&PadagoniaConfig::default()) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("[error] cannot open Padagonia: {e}");
            std::process::exit(1);
        }
    };

    let canonical = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());

    let Some(record) = padagonia.size_aggregate(&canonical) else {
        println!(
            "No recorded size measurement for: {}\n(run `gls` against a parent directory first — \
             BART records a size aggregate for every directory it walks through)",
            canonical.display()
        );
        return;
    };

    println!(
        "{}  {}  ({} objects{}, depth {})",
        canonical.display(),
        gls_render::human_bytes(record.tree_bytes),
        record.object_count,
        record
            .reclaimable_fraction
            .filter(|f| *f > 0.0)
            .map(|f| format!(", {:.0}% reclaimable", f * 100.0))
            .unwrap_or_default(),
        record.depth,
    );
    println!("measured: {}", record.measured_at.to_rfc3339());

    let children = padagonia.size_children(&canonical);
    if children.is_empty() {
        if !record.is_leaf {
            println!("(children not yet recorded — re-run `gls` here to refresh)");
        }
        return;
    }

    println!();
    for child in &children {
        let name = child
            .path
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_else(|| child.path.display().to_string());
        println!(
            "  {:>10}  {name}{}",
            gls_render::human_bytes(child.tree_bytes),
            if child.is_leaf { "" } else { "/" }
        );
    }
}

fn main() {
    let raw_args: Vec<String> = std::env::args().collect();
    let mut positions: Vec<usize> = Vec::new();
    let mut fields: Vec<Vec<String>> = Vec::new();
    for (__i, __a) in raw_args.iter().enumerate() {
        if __a == "--model" {
            if let Some(__v) = raw_args.get(__i + 1) {
                positions.push(__i + 1);
                fields.push(curly_expand::expand_or_literal(__v));
            }
            break;
        } else if let Some(__v) = __a.strip_prefix("--model=") {
            positions.push(__i);
            fields.push(
                curly_expand::expand_or_literal(__v)
                    .into_iter()
                    .map(|v| format!("--model={}", v))
                    .collect(),
            );
            break;
        }
    }

    if fields.is_empty() || fields.iter().all(|f| f.len() <= 1) {
        __curly_original_main();
        return;
    }

    let combos = curly_expand::cartesian(&fields);
    let exe = std::env::current_exe().expect("resolve current exe");
    let mut had_failure = false;
    for combo in &combos {
        let mut new_args = raw_args.clone();
        for (slot, value) in positions.iter().zip(combo.iter()) {
            new_args[*slot] = value.clone();
        }
        let status = std::process::Command::new(&exe)
            .args(&new_args[1..])
            .status()
            .expect("failed to re-exec self");
        if !status.success() {
            had_failure = true;
        }
    }
    if had_failure {
        std::process::exit(1);
    }
}

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

use std::path::PathBuf;
use std::time::Duration;

use clap::{ArgAction, Parser, ValueEnum};
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

// ── CLI ───────────────────────────────────────────────────────────────────────

#[derive(Debug, Parser)]
#[command(
    name  = "gls",
    about = "glass — filesystem meaning, progressively revealed",
    long_about = None,
)]
struct Cli {
    /// Target path to inspect.
    #[arg(default_value = ".")]
    target: PathBuf,

    /// Recursion depth (0 = root only, 1 = direct children, …).
    #[arg(short = 'd', long, default_value_t = 1)]
    depth: usize,

    /// Include hidden entries (names starting with `.`).
    #[arg(long, action = ArgAction::SetTrue)]
    hidden: bool,

    /// Disable animation (static output).
    #[arg(long = "no-animate", action = ArgAction::SetTrue)]
    no_animate: bool,

    /// Force animation even when stdout is not a TTY.
    #[arg(long = "animate", action = ArgAction::SetTrue)]
    animate: bool,

    /// Watch mode: re-run on filesystem changes.
    #[arg(long, action = ArgAction::SetTrue)]
    watch: bool,

    /// Skip AI contextualisation.
    #[arg(long = "no-context", action = ArgAction::SetTrue)]
    no_context: bool,

    /// Force AI contextualisation even when cache is valid.
    #[arg(long = "context", action = ArgAction::SetTrue)]
    force_context: bool,

    /// Deep scan: increase depth and contextualisation budget.
    #[arg(long, action = ArgAction::SetTrue)]
    deep: bool,

    /// Output mode.
    #[arg(long = "output", value_enum, default_value_t = OutputArg::Auto)]
    output: OutputArg,

    /// Groq model to use for contextualisation.
    /// Defaults to the GLS_GROQ_MODEL env var, then the current recommended model.
    #[arg(long)]
    model: Option<String>,
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

// ── Entry point ───────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    // ── Bound ─────────────────────────────────────────────────────────────────
    let bound = Bound {
        depth: if cli.deep { cli.depth.max(3) } else { cli.depth },
        no_context: cli.no_context,
        incremental: !cli.force_context,
        max_context_candidates: if cli.deep { 256 } else { 64 },
        inference_budget_bytes: if cli.deep { 1_024_000 } else { 256_000 },
        ..Default::default()
    };

    // ── Pipeline broadcast channel ─────────────────────────────────────────────
    let (tx, rx_render) = pipeline::channel();
    let rx_padagonia = tx.subscribe();

    // ── Renderer output mode ───────────────────────────────────────────────────
    let output_mode = match cli.output {
        OutputArg::Json  => OutputMode::Json,
        OutputArg::Plain => OutputMode::Plain,
        OutputArg::Tty   => OutputMode::Tty,
        OutputArg::Auto  => {
            if cli.no_animate {
                OutputMode::Plain
            } else {
                OutputMode::Tty
            }
        }
    };

    let tick = if cli.no_animate
        || output_mode == OutputMode::Plain
        || output_mode == OutputMode::Json
    {
        None
    } else {
        Some(Duration::from_millis(600))
    };

    let render_config = RenderConfig {
        mode: output_mode,
        tick_interval: tick,
    };

    // ── Padagonia ──────────────────────────────────────────────────────────────
    let padagonia = Padagonia::open(&PadagoniaConfig::default())
        .unwrap_or_else(|e| {
            eprintln!("[warn] padagonia unavailable: {e}");
            Padagonia::open(&PadagoniaConfig {
                db_path: PathBuf::from(":memory:"),
            })
            .expect("in-memory SQLite must always work")
        });

    // ── Spawn renderer and Padagonia consumer ──────────────────────────────────
    let renderer = Renderer::new(render_config);
    let render_task    = tokio::spawn(async move { renderer.run(rx_render).await });
    let padagonia_task = tokio::spawn(async move { padagonia.consume_stream(rx_padagonia).await });

    // ── DISCOVER ───────────────────────────────────────────────────────────────
    // Collect entries on a private channel first (we need the Vec for later
    // phases), then immediately forward Discovery events to the main channel
    // so the renderer shows names at T0.
    let target = cli.target.canonicalize().unwrap_or(cli.target.clone());

    let entries = collect_discovered(&target, &bound, cli.hidden).await;

    // T0: emit names immediately.
    for entry in &entries {
        let _ = tx.send(PipelineEvent::Discovery(entry.clone()));
    }

    // ── TYPE ∥ BART ────────────────────────────────────────────────────────────
    // Run classification and physical measurement concurrently.
    let paths: Vec<PathBuf> = entries.iter().map(|e| e.path.clone()).collect();

    let tx_type = tx.clone();
    let tx_bart = tx.clone();
    let entries_clone = entries.clone();

    let ((), ()) = tokio::join!(
        async move { classify_entry(entries_clone, tx_type).await },
        async move { measure_entry(paths,           tx_bart).await },
    );

    // ── INDEX ──────────────────────────────────────────────────────────────────
    // Build descriptors privately; forward Indexed events to the main channel.
    let descriptors = collect_descriptors(&entries, tx.clone()).await;

    // ── CONTEXT ────────────────────────────────────────────────────────────────
    if !bound.no_context {
        let weighted: Vec<_> = descriptors
            .into_iter()
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
        }

        let contextualiser = Contextualiser::new(ctx_config);
        contextualiser.contextualise(candidates, tx.clone()).await;
    }

    // ── Done ───────────────────────────────────────────────────────────────────
    let _ = tx.send(PipelineEvent::PipelineComplete);
    let _ = tokio::join!(render_task, padagonia_task);
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Discover filesystem entries via a private channel, returning them as a Vec.
async fn collect_discovered(
    target: &PathBuf,
    bound: &Bound,
    include_hidden: bool,
) -> Vec<FsEntry> {
    let (tx2, mut rx2) = tokio::sync::broadcast::channel::<PipelineEvent>(1024);
    let cfg = DiscoverConfig {
        max_depth: bound.depth,
        follow_links: false,
        include_hidden,
    };
    gls_discover::discover(target.clone(), cfg, tx2.clone()).await;
    drop(tx2);

    let mut entries = Vec::new();
    loop {
        match rx2.recv().await {
            Ok(PipelineEvent::Discovery(e)) => entries.push(e),
            Ok(_) => {}
            Err(_) => break,
        }
    }
    entries
}

/// Build descriptors for every entry, emit Indexed events on `tx`, and return
/// the descriptors for use by the CONTEXT phase.
async fn collect_descriptors(
    entries: &[FsEntry],
    tx: tokio::sync::broadcast::Sender<PipelineEvent>,
) -> Vec<gls_index::Descriptor> {
    let (tx_idx, mut rx_idx) = tokio::sync::broadcast::channel::<PipelineEvent>(1024);
    let items: Vec<_> = entries
        .iter()
        .map(|e| (e.path.clone(), None, None))
        .collect();
    build_descriptor(items, tx_idx.clone()).await;
    drop(tx_idx);

    let mut descriptors = Vec::new();
    loop {
        match rx_idx.recv().await {
            Ok(PipelineEvent::Indexed(d)) => {
                let _ = tx.send(PipelineEvent::Indexed(d.clone()));
                descriptors.push(d);
            }
            Ok(_) => {}
            Err(_) => break,
        }
    }
    descriptors
}

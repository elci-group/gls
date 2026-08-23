//! `gls` — glass: a progressive, animated, metadata-driven filesystem
//! intelligence surface.
//!
//! # Pipeline
//!
//! ```text
//! DISCOVER → TYPE → BART → INDEX → BOUND → CONTEXT → METRICS
//! ```
//!
//! Each phase emits [`PipelineEvent`] values onto a broadcast channel.
//! The renderer and the Padagonia persistence layer each consume an independent
//! clone of the same stream.

use std::path::PathBuf;
use std::time::Duration;

use clap::{ArgAction, Parser, ValueEnum};
use gls_bart::measure_entry;
use gls_bound::{select_candidates, Bound};
use gls_context::{ContextConfig, Contextualiser};
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
    #[arg(long, default_value = "llama3-8b-8192")]
    model: String,
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

    // ── Bound configuration ───────────────────────────────────────────────────
    let bound = Bound {
        depth: if cli.deep { cli.depth.max(3) } else { cli.depth },
        no_context: cli.no_context,
        incremental: !cli.force_context,
        max_context_candidates: if cli.deep { 256 } else { 64 },
        inference_budget_bytes: if cli.deep { 1_024_000 } else { 256_000 },
        ..Default::default()
    };

    // ── Pipeline broadcast channel ────────────────────────────────────────────
    let (tx, rx_render) = pipeline::channel();
    let rx_padagonia = tx.subscribe();

    // ── Renderer output mode ──────────────────────────────────────────────────
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
        Some(Duration::from_millis(800))
    };

    let render_config = RenderConfig {
        mode: output_mode,
        tick_interval: tick,
    };

    // ── Padagonia ─────────────────────────────────────────────────────────────
    let padagonia = Padagonia::open(&PadagoniaConfig::default())
        .unwrap_or_else(|e| {
            eprintln!("[warn] padagonia unavailable: {e}");
            Padagonia::open(&PadagoniaConfig {
                db_path: PathBuf::from(":memory:"),
            })
            .expect("in-memory SQLite must always work")
        });

    // ── Spawn renderer and Padagonia consumer ─────────────────────────────────
    let renderer = Renderer::new(render_config);
    let render_task = tokio::spawn(async move { renderer.run(rx_render).await });
    let padagonia_task =
        tokio::spawn(async move { padagonia.consume_stream(rx_padagonia).await });

    // ── DISCOVER ──────────────────────────────────────────────────────────────
    let target = cli.target.canonicalize().unwrap_or(cli.target.clone());

    let discover_cfg = DiscoverConfig {
        max_depth: bound.depth,
        follow_links: false,
        include_hidden: cli.hidden,
    };

    // Collect entries via a private channel first so we can use them for
    // subsequent phases, then replay Discovery events on the main channel.
    let (tx_collect, mut rx_collect) =
        tokio::sync::broadcast::channel::<PipelineEvent>(1024);
    gls_discover::discover(target.clone(), discover_cfg.clone(), tx_collect.clone()).await;
    drop(tx_collect);

    let mut entries = Vec::new();
    loop {
        match rx_collect.recv().await {
            Ok(PipelineEvent::Discovery(e)) => entries.push(e),
            Ok(_) => {}
            Err(_) => break,
        }
    }

    // Emit Discovery events to the main broadcast channel (for the renderer).
    for entry in &entries {
        let _ = tx.send(PipelineEvent::Discovery(entry.clone()));
    }

    // ── TYPE ──────────────────────────────────────────────────────────────────
    classify_entry(entries.clone(), tx.clone()).await;

    // ── BART ──────────────────────────────────────────────────────────────────
    let paths: Vec<_> = entries.iter().map(|e| e.path.clone()).collect();
    measure_entry(paths, tx.clone()).await;

    // ── INDEX ─────────────────────────────────────────────────────────────────
    // Build descriptors via a private channel so we can collect them for BOUND.
    let (tx_idx, mut rx_idx) = tokio::sync::broadcast::channel::<PipelineEvent>(1024);
    let index_items: Vec<_> = entries
        .iter()
        .map(|e| (e.path.clone(), None, None))
        .collect();
    build_descriptor(index_items, tx_idx.clone()).await;
    drop(tx_idx);

    let mut descriptors = Vec::new();
    loop {
        match rx_idx.recv().await {
            Ok(PipelineEvent::Indexed(d)) => {
                // Emit to main channel so renderer can track indexing.
                let _ = tx.send(PipelineEvent::Indexed(d.clone()));
                descriptors.push(d);
            }
            Ok(_) => {}
            Err(_) => break,
        }
    }

    // ── CONTEXT ───────────────────────────────────────────────────────────────
    if !bound.no_context {
        // BOUND: select candidates within budget.
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

        let ctx_config = ContextConfig {
            model: cli.model.clone(),
            ..Default::default()
        };
        let contextualiser = Contextualiser::new(ctx_config);
        contextualiser.contextualise(candidates, tx.clone()).await;
    }

    // ── Signal completion ─────────────────────────────────────────────────────
    let _ = tx.send(PipelineEvent::PipelineComplete);

    // ── Wait for renderer and Padagonia to finish ─────────────────────────────
    let _ = tokio::join!(render_task, padagonia_task);
}

//! Retained-mode TTY renderer.
//!
//! # Invariant
//!
//! **One filesystem object = exactly one stable terminal row.**
//!
//! A row's content may change arbitrarily many times, but a row is never
//! duplicated and never appended.
//!
//! # Design
//!
//! The screen is a fixed-height grid, one row per entry.  Each row is 1 or 2
//! terminal lines tall (primary + optional sub-line).
//!
//! ## Event loop
//!
//! Rather than rendering on every event, the loop drains *all* currently
//! pending events from the channel in a single synchronous pass, then renders
//! once.  This prevents the burst of TYPE/BART events that arrive in quick
//! succession from causing repeated redraws of the same partial state.
//!
//! ## Redraw
//!
//! Before each frame: `MoveToPreviousLine(prev_line_count)` returns the cursor
//! to the top of the grid.  Only rows whose content changed since the last
//! frame are redrawn; unchanged rows are skipped with `MoveToNextLine`.
//!
//! ## stderr isolation
//!
//! While the live grid is displayed, nothing writes to stderr.  Provider
//! warnings are stored in `pending_warnings` and flushed to stderr *after*
//! the grid exits (on `PipelineComplete` or channel close).  This prevents
//! API error bodies from appearing inside the terminal grid.

use crate::{
    format::{context_line, metrics_line, type_line},
    state::EntryDisplayState,
};
use crossterm::{
    cursor,
    style,
    terminal,
    QueueableCommand,
};
use gls_core::{
    entry::EntryKind,
    event::PipelineEvent,
    pipeline::PresentationState,
};
use std::collections::HashMap;
use std::io::{self, Write};
use std::path::PathBuf;
use std::time::Duration;
use tokio::sync::broadcast;

// ── Public surface ────────────────────────────────────────────────────────────

/// How output should be presented.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputMode {
    /// Animated, retained-mode TTY output.
    Tty,
    /// Plain text — buffer all events, print a clean snapshot when done.
    Plain,
    /// One JSON object per pipeline event on stdout.
    Json,
}

/// Configuration for the renderer.
#[derive(Debug, Clone)]
pub struct RenderConfig {
    pub mode: OutputMode,
    /// Animation tick interval.  `None` = static (no animation).
    pub tick_interval: Option<Duration>,
}

impl Default for RenderConfig {
    fn default() -> Self {
        let is_tty = atty_check();
        Self {
            mode: if is_tty { OutputMode::Tty } else { OutputMode::Plain },
            tick_interval: if is_tty {
                Some(Duration::from_millis(600))
            } else {
                None
            },
        }
    }
}

fn atty_check() -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::io::AsRawFd;
        unsafe { libc::isatty(io::stdout().as_raw_fd()) == 1 }
    }
    #[cfg(not(unix))]
    { false }
}

/// The renderer.
pub struct Renderer {
    config: RenderConfig,
}

impl Renderer {
    pub fn new(config: RenderConfig) -> Self {
        Self { config }
    }

    /// Consume the event stream until `PipelineComplete` or channel close.
    pub async fn run(self, rx: broadcast::Receiver<PipelineEvent>) {
        match self.config.mode {
            OutputMode::Json  => json_run(rx).await,
            OutputMode::Plain => plain_run(rx).await,
            OutputMode::Tty   => tty_run(rx, self.config.tick_interval).await,
        }
    }
}

// ── JSON mode ─────────────────────────────────────────────────────────────────

async fn json_run(mut rx: broadcast::Receiver<PipelineEvent>) {
    loop {
        match rx.recv().await {
            Ok(event) => {
                let json = serde_json::to_string(&event).unwrap_or_default();
                println!("{json}");
                if matches!(event, PipelineEvent::PipelineComplete) { break; }
            }
            Err(_) => break,
        }
    }
}

// ── Plain mode ────────────────────────────────────────────────────────────────

async fn plain_run(mut rx: broadcast::Receiver<PipelineEvent>) {
    let mut entries: HashMap<PathBuf, EntryDisplayState> = HashMap::new();
    let mut order: Vec<PathBuf> = Vec::new();
    let mut warnings: Vec<String> = Vec::new();

    loop {
        match rx.recv().await {
            Ok(event) => {
                let done = matches!(event, PipelineEvent::PipelineComplete);
                apply_event(&mut entries, &mut order, &mut warnings, event);
                if done { break; }
            }
            Err(_) => break,
        }
    }

    for path in &order {
        if let Some(state) = entries.get(path) {
            print_plain_entry(state);
        }
    }

    // Flush deferred warnings after output.
    for w in &warnings {
        eprintln!("{w}");
    }
}

fn print_plain_entry(state: &EntryDisplayState) {
    let icon = entry_icon(state);
    if let Some(sub) = best_sub_line(state) {
        println!("{icon} {}  {sub}", state.basename);
    } else {
        println!("{icon} {}", state.basename);
    }
}

// ── TTY retained-mode renderer ────────────────────────────────────────────────

/// A single rendered row (1 or 2 terminal lines).
#[derive(Clone, PartialEq, Eq)]
struct RenderedRow {
    primary:   String,
    secondary: Option<String>,
}

impl RenderedRow {
    fn line_count(&self) -> u16 {
        if self.secondary.is_some() { 2 } else { 1 }
    }
}

async fn tty_run(
    mut rx: broadcast::Receiver<PipelineEvent>,
    tick_interval: Option<Duration>,
) {
    let mut entries: HashMap<PathBuf, EntryDisplayState> = HashMap::new();
    let mut order:   Vec<PathBuf>   = Vec::new();
    // Warnings deferred until after the grid exits.
    let mut warnings: Vec<String>   = Vec::new();

    let mut prev_rows: Vec<RenderedRow> = Vec::new();
    let mut prev_line_count: u16 = 0;

    let interval = tick_interval.unwrap_or(Duration::from_millis(600));
    let mut ticker = tokio::time::interval(interval);
    // Consume the first instant tick so we don't render before data arrives.
    ticker.tick().await;

    let mut pipeline_done = false;

    'outer: loop {
        // ── Wait for the next wakeup (event or tick) ──────────────────────────
        let tick_fired = tokio::select! {
            biased;
            result = rx.recv() => {
                match result {
                    Ok(ev) => {
                        let done = matches!(ev, PipelineEvent::PipelineComplete);
                        apply_event(&mut entries, &mut order, &mut warnings, ev);
                        if done { pipeline_done = true; }
                        false // tick did not fire
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => false,
                    Err(_) => { pipeline_done = true; false }
                }
            }
            _ = ticker.tick() => true,
        };

        // ── Drain any additional events that arrived in the same burst ────────
        // This is the key fix: instead of rendering after *every* event, we
        // consume everything currently in the channel before rendering once.
        loop {
            match rx.try_recv() {
                Ok(ev) => {
                    let done = matches!(ev, PipelineEvent::PipelineComplete);
                    apply_event(&mut entries, &mut order, &mut warnings, ev);
                    if done { pipeline_done = true; }
                }
                Err(broadcast::error::TryRecvError::Empty)   => break,
                Err(broadcast::error::TryRecvError::Lagged(_)) => break,
                Err(broadcast::error::TryRecvError::Closed)  => {
                    pipeline_done = true;
                    break;
                }
            }
        }

        // ── Advance animation frames on tick ──────────────────────────────────
        if tick_fired {
            for path in &order {
                if let Some(s) = entries.get_mut(path) {
                    s.tick_frame();
                }
            }
        }

        // ── Render once ───────────────────────────────────────────────────────
        if !order.is_empty() {
            retained_render(&entries, &order, &mut prev_rows, &mut prev_line_count);
        }

        if pipeline_done {
            break 'outer;
        }
    }

    // Final render with fully stable state.
    if !order.is_empty() {
        retained_render(&entries, &order, &mut prev_rows, &mut prev_line_count);
    }

    // Leave cursor on a clean line below the grid.
    let _ = io::stdout().write_all(b"\n");
    let _ = io::stdout().flush();

    // Now safe to flush deferred warnings to stderr.
    for w in &warnings {
        eprintln!("{w}");
    }
}

// ── Retained-mode redraw ──────────────────────────────────────────────────────

fn retained_render(
    entries:          &HashMap<PathBuf, EntryDisplayState>,
    order:            &[PathBuf],
    prev_rows:        &mut Vec<RenderedRow>,
    prev_line_count:  &mut u16,
) {
    let new_rows: Vec<RenderedRow> = order
        .iter()
        .filter_map(|p| entries.get(p))
        .map(render_row)
        .collect();

    let mut stdout = io::stdout();

    // ── First paint: print everything downward from current cursor ────────────
    if prev_rows.is_empty() {
        for row in &new_rows {
            let _ = stdout.queue(terminal::Clear(terminal::ClearType::CurrentLine));
            let _ = stdout.queue(style::Print(&row.primary));
            let _ = stdout.queue(cursor::MoveToNextLine(1));
            if let Some(ref sec) = row.secondary {
                let _ = stdout.queue(terminal::Clear(terminal::ClearType::CurrentLine));
                let _ = stdout.queue(style::Print(format!("   {sec}")));
                let _ = stdout.queue(cursor::MoveToNextLine(1));
            }
        }
        let _ = stdout.flush();
        *prev_line_count = new_rows.iter().map(|r| r.line_count()).sum();
        *prev_rows = new_rows;
        return;
    }

    // ── Subsequent paints: move to top of grid, selectively overdraw ──────────
    let _ = stdout.queue(cursor::MoveToPreviousLine(*prev_line_count));

    let mut new_total: u16 = 0;
    let mut prev_iter = prev_rows.iter();

    for row in &new_rows {
        let prev    = prev_iter.next();
        let changed = prev.map(|r| r != row).unwrap_or(true);

        if changed {
            // Redraw primary line (clear to EOL first to erase stale content).
            let _ = stdout.queue(terminal::Clear(terminal::ClearType::CurrentLine));
            let _ = stdout.queue(style::Print(&row.primary));
            let _ = stdout.queue(cursor::MoveToNextLine(1));

            match (&row.secondary, prev.and_then(|r| r.secondary.as_ref())) {
                (Some(sec), _) => {
                    let _ = stdout.queue(terminal::Clear(terminal::ClearType::CurrentLine));
                    let _ = stdout.queue(style::Print(format!("   {sec}")));
                    let _ = stdout.queue(cursor::MoveToNextLine(1));
                }
                (None, Some(_)) => {
                    // Row shrank: erase the old secondary line.
                    let _ = stdout.queue(terminal::Clear(terminal::ClearType::CurrentLine));
                    let _ = stdout.queue(cursor::MoveToNextLine(1));
                }
                (None, None) => { /* single-line row, no change in height */ }
            }
        } else {
            let _ = stdout.queue(cursor::MoveToNextLine(row.line_count()));
        }

        new_total += row.line_count();
    }

    // Erase any leftover lines if grid shrank.
    if new_total < *prev_line_count {
        let leftover = *prev_line_count - new_total;
        for _ in 0..leftover {
            let _ = stdout.queue(terminal::Clear(terminal::ClearType::CurrentLine));
            let _ = stdout.queue(cursor::MoveToNextLine(1));
        }
        let _ = stdout.queue(cursor::MoveToPreviousLine(leftover));
    }

    let _ = stdout.flush();
    *prev_line_count = new_total;
    *prev_rows = new_rows;
}

// ── Row rendering ─────────────────────────────────────────────────────────────

/// Build the rendered representation of one entry for the current frame.
fn render_row(state: &EntryDisplayState) -> RenderedRow {
    let icon   = entry_icon(state);
    let status = status_indicator(state);
    let primary   = format!("{icon} {}{status}", state.basename);
    let secondary = current_secondary(state);
    RenderedRow { primary, secondary }
}

fn status_indicator(state: &EntryDisplayState) -> &'static str {
    if state.context_pending && state.context.is_none() {
        " ◌"
    } else {
        ""
    }
}

/// The animated sub-line for the current frame.
///
/// Available dimensions (only built when data is present):
///   1 → type + size  (or "⚠ context unavailable" if context failed)
///   2 → metrics
///   3 → context summary
///
/// frame 0 = name only (no sub-line)
/// frame N = dims[N-1]
fn current_secondary(state: &EntryDisplayState) -> Option<String> {
    let mut dims: Vec<String> = Vec::new();

    // Dim 1: type + size
    if let Some(ref cls) = state.classification {
        dims.push(type_line(cls, state.physical.as_ref()));
    }

    // Dim 2: metrics
    if let Some(ref m) = state.metrics {
        let line = metrics_line(m);
        if !line.is_empty() { dims.push(line); }
    }

    // Dim 3: context summary
    if let Some(ref ctx) = state.context {
        dims.push(context_line(ctx));
    }

    // Context failure visible in dim 1 position if no type either.
    if dims.is_empty() && state.context_failed {
        dims.push("⚠ context unavailable".to_string());
    }

    if dims.is_empty() { return None; }

    let total_frames = dims.len() + 1; // +1 for name-only frame
    let frame = state.frame_index % total_frames;
    if frame == 0 { None } else { dims.get(frame - 1).cloned() }
}

fn best_sub_line(state: &EntryDisplayState) -> Option<String> {
    state.classification.as_ref()
        .map(|cls| type_line(cls, state.physical.as_ref()))
}

fn entry_icon(state: &EntryDisplayState) -> &'static str {
    state.classification.as_ref()
        .map(|c| c.semantic_type.icon())
        .unwrap_or(if state.is_dir { "📁" } else { "📄" })
}

// ── Event application ─────────────────────────────────────────────────────────

/// Mutate `entries`/`order` according to `event`.
///
/// Warnings are pushed to `warnings` (not printed) so the live grid is never
/// contaminated by external content.
fn apply_event(
    entries:  &mut HashMap<PathBuf, EntryDisplayState>,
    order:    &mut Vec<PathBuf>,
    warnings: &mut Vec<String>,
    event:    PipelineEvent,
) {
    match event {
        PipelineEvent::Discovery(e) => {
            let path  = e.path.clone();
            let is_dir = e.kind == EntryKind::Directory;
            // Guard: never insert the same path twice.
            if !entries.contains_key(&path) {
                order.push(path.clone());
                entries.insert(path, EntryDisplayState::new(e.path, e.basename, is_dir));
            }
        }

        PipelineEvent::TypeDetected { path, classification } => {
            if let Some(s) = entries.get_mut(&path) {
                s.classification = Some(classification);
                s.presentation   = s.presentation.advance();
            }
        }

        PipelineEvent::Measured(m) => {
            let path = m.path.clone();
            if let Some(s) = entries.get_mut(&path) {
                s.physical     = Some(m);
                s.presentation = s.presentation.advance();
            }
        }

        PipelineEvent::Indexed(_) => { /* no visible change */ }

        PipelineEvent::ContextStarted { path } => {
            if let Some(s) = entries.get_mut(&path) {
                s.context_pending = true;
                s.context_failed  = false;
            }
        }

        PipelineEvent::ContextUpdated(a) => {
            if let Some(s) = entries.get_mut(&a.path) {
                s.context         = Some(a);
                s.context_pending = false;
                s.context_failed  = false;
                s.presentation    = s.presentation.advance();
            }
        }

        PipelineEvent::MetricsUpdated(m) => {
            let path = m.path.clone();
            if let Some(s) = entries.get_mut(&path) {
                s.metrics      = Some(m);
                s.presentation = s.presentation.advance();
            }
        }

        PipelineEvent::Persisted { path, .. } => {
            if let Some(s) = entries.get_mut(&path) {
                s.presentation = PresentationState::Stable;
            }
        }

        PipelineEvent::Warning { path, message } => {
            // Mark context failure on the entry; never print during live grid.
            let is_context = message.starts_with("context error:")
                || message.starts_with("context unavailable");

            if let Some(ref p) = path {
                if let Some(s) = entries.get_mut(p) {
                    if is_context {
                        s.context_pending = false;
                        s.context_failed  = true;
                    }
                    warnings.push(format!("[warn] {}: {message}", s.basename));
                } else {
                    warnings.push(format!("[warn] {message}"));
                }
            } else {
                warnings.push(format!("[warn] {message}"));
            }
        }

        PipelineEvent::ProviderUnavailable { provider, reason } => {
            // A single event disabling context for all entries.
            for s in entries.values_mut() {
                if s.context_pending {
                    s.context_pending = false;
                    s.context_failed  = true;
                }
            }
            warnings.push(format!(
                "[warn] context provider '{provider}' unavailable: {reason}"
            ));
        }

        PipelineEvent::PipelineComplete => { /* handled by caller */ }
    }
}

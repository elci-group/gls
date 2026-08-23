//! Retained-mode TTY renderer.
//!
//! # Design
//!
//! The screen is modelled as a fixed-height grid of *rows*, one per
//! filesystem entry.  Each row is 1 or 2 terminal lines tall depending on
//! whether a sub-line is currently showing.
//!
//! On every tick/event the renderer:
//!   1. Computes the new content for every row.
//!   2. Compares it against what was drawn last frame.
//!   3. Moves the cursor **only to the rows that changed** and redraws them.
//!
//! The cursor is left at the bottom of the grid after every frame.  Before
//! the next frame it is moved back up by the total number of terminal lines
//! that were drawn last time.
//!
//! This means the display is spatially stable: row 0 is always row 0.

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
    /// Plain text — all events buffered, final snapshot printed once.
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
    {
        false
    }
}

/// The renderer.
pub struct Renderer {
    config: RenderConfig,
}

impl Renderer {
    pub fn new(config: RenderConfig) -> Self {
        Self { config }
    }

    /// Consume the event stream.  Returns when `PipelineComplete` is received
    /// or the sender is dropped.
    pub async fn run(self, rx: broadcast::Receiver<PipelineEvent>) {
        match self.config.mode {
            OutputMode::Json  => json_run(rx).await,
            OutputMode::Plain => plain_run(rx).await,
            OutputMode::Tty   => tty_run(rx, self.config.tick_interval).await,
        }
    }
}

// ── JSON mode ────────────────────────────────────────────────────────────────

async fn json_run(mut rx: broadcast::Receiver<PipelineEvent>) {
    loop {
        match rx.recv().await {
            Ok(event) => {
                let json = serde_json::to_string(&event).unwrap_or_default();
                println!("{json}");
                if matches!(event, PipelineEvent::PipelineComplete) {
                    break;
                }
            }
            Err(_) => break,
        }
    }
}

// ── Plain mode ────────────────────────────────────────────────────────────────
// Buffer all events; print a clean snapshot when done.

async fn plain_run(mut rx: broadcast::Receiver<PipelineEvent>) {
    let mut entries: HashMap<PathBuf, EntryDisplayState> = HashMap::new();
    let mut order: Vec<PathBuf> = Vec::new();

    loop {
        match rx.recv().await {
            Ok(event) => {
                let done = matches!(event, PipelineEvent::PipelineComplete);
                apply_event(&mut entries, &mut order, event);
                if done {
                    break;
                }
            }
            Err(_) => break,
        }
    }

    for path in &order {
        if let Some(state) = entries.get(path) {
            print_plain_entry(state);
        }
    }
}

fn print_plain_entry(state: &EntryDisplayState) {
    let icon = entry_icon(state);
    let sub = best_sub_line(state);

    if let Some(sub) = sub {
        println!("{icon} {}  {sub}", state.basename);
    } else {
        println!("{icon} {}", state.basename);
    }
}

// ── TTY retained-mode renderer ────────────────────────────────────────────────

/// A single rendered row (1 or 2 terminal lines).
#[derive(Clone, PartialEq, Eq)]
struct RenderedRow {
    /// Primary line text (no trailing newline).
    primary: String,
    /// Optional second line (indented).  `None` = single-line row.
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
    let mut order: Vec<PathBuf> = Vec::new();

    // What we drew last frame, indexed parallel to `order`.
    // Empty until the first render.
    let mut prev_rows: Vec<RenderedRow> = Vec::new();
    // Total terminal lines occupied by the last frame.
    let mut prev_line_count: u16 = 0;

    let interval = tick_interval.unwrap_or(Duration::from_millis(600));
    let mut ticker = tokio::time::interval(interval);
    // Burn the first immediate tick so we don't render before any data arrives.
    ticker.tick().await;

    let mut pipeline_done = false;
    let mut dirty = false;

    loop {
        tokio::select! {
            biased;
            event = rx.recv() => {
                match event {
                    Ok(ev) => {
                        let done = matches!(ev, PipelineEvent::PipelineComplete);
                        apply_event(&mut entries, &mut order, ev);
                        dirty = true;
                        if done {
                            pipeline_done = true;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        // We missed some events; re-render whatever state we have.
                        let _ = n;
                        dirty = true;
                    }
                    Err(_) => {
                        pipeline_done = true;
                    }
                }
            }
            _ = ticker.tick() => {
                // Advance each entry's animation frame independently.
                for path in &order {
                    if let Some(s) = entries.get_mut(path) {
                        s.tick_frame();
                    }
                }
                dirty = true;
            }
        }

        if dirty && !order.is_empty() {
            retained_render(
                &entries,
                &order,
                &mut prev_rows,
                &mut prev_line_count,
            );
            dirty = false;
        }

        if pipeline_done {
            // Final frame: show stable state for everything.
            retained_render(
                &entries,
                &order,
                &mut prev_rows,
                &mut prev_line_count,
            );
            // Move cursor to a clean new line below the grid.
            let _ = io::stdout().write_all(b"\n");
            break;
        }
    }
}

/// Retained-mode redraw.
///
/// 1. Compute the new row content for every entry.
/// 2. Move the cursor back up `prev_line_count` lines (to the top of the grid).
/// 3. For each row: if content changed, seek to that row and overdraw it;
///    otherwise skip.
fn retained_render(
    entries: &HashMap<PathBuf, EntryDisplayState>,
    order: &[PathBuf],
    prev_rows: &mut Vec<RenderedRow>,
    prev_line_count: &mut u16,
) {
    // Build new rows.
    let new_rows: Vec<RenderedRow> = order
        .iter()
        .filter_map(|p| entries.get(p))
        .map(render_row)
        .collect();

    let mut stdout = io::stdout();

    // First render: just print everything downward.
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

    // Subsequent renders: move cursor to top of grid, then selectively redraw.
    let _ = stdout.queue(cursor::MoveToPreviousLine(*prev_line_count));

    let mut new_total_lines: u16 = 0;
    let mut prev_iter = prev_rows.iter();

    for row in new_rows.iter() {
        let prev = prev_iter.next();
        let changed = prev.map(|r| r != row).unwrap_or(true);

        if changed {
            // Overdraw primary line.
            let _ = stdout.queue(terminal::Clear(terminal::ClearType::CurrentLine));
            let _ = stdout.queue(style::Print(&row.primary));
            let _ = stdout.queue(cursor::MoveToNextLine(1));

            // Secondary line.
            match (&row.secondary, prev.and_then(|r| r.secondary.as_ref())) {
                (Some(sec), _) => {
                    let _ = stdout.queue(terminal::Clear(terminal::ClearType::CurrentLine));
                    let _ = stdout.queue(style::Print(format!("   {sec}")));
                    let _ = stdout.queue(cursor::MoveToNextLine(1));
                }
                (None, Some(_)) => {
                    // Row shrank from 2 lines to 1: erase the old secondary.
                    let _ = stdout.queue(terminal::Clear(terminal::ClearType::CurrentLine));
                    let _ = stdout.queue(cursor::MoveToNextLine(1));
                }
                (None, None) => {}
            }
        } else {
            // Content unchanged: skip over the row's lines.
            let _ = stdout.queue(cursor::MoveToNextLine(row.line_count()));
        }

        new_total_lines += row.line_count();
    }

    // If the grid shrank (entries removed), erase leftover lines.
    if new_total_lines < *prev_line_count {
        let leftover = *prev_line_count - new_total_lines;
        for _ in 0..leftover {
            let _ = stdout.queue(terminal::Clear(terminal::ClearType::CurrentLine));
            let _ = stdout.queue(cursor::MoveToNextLine(1));
        }
        // Move back up to the end of the live grid.
        let _ = stdout.queue(cursor::MoveToPreviousLine(leftover));
    }

    let _ = stdout.flush();

    *prev_line_count = new_total_lines;
    *prev_rows = new_rows;
}

// ── Row rendering ─────────────────────────────────────────────────────────────

/// Render one entry into a `RenderedRow`.
///
/// Primary line: `{icon} {name}{status_indicator}`
/// Secondary line (when available): animated metadata dimension
fn render_row(state: &EntryDisplayState) -> RenderedRow {
    let icon = entry_icon(state);

    // Status indicator (right of name)
    let status = if state.context_failed {
        ""           // silent — failure shown only in secondary if desired
    } else if state.context_pending && state.context.is_none() {
        " ◌"
    } else if state.context.is_some() && !state.context_pending {
        ""           // no clutter on completed rows
    } else {
        ""
    };

    let primary = format!("{icon} {}{status}", state.basename);
    let secondary = current_secondary(state);

    RenderedRow { primary, secondary }
}

/// The current metadata sub-line for an entry, based on its animation frame.
///
/// Dimensions cycle:
///   0 → (none — name only)
///   1 → type + size
///   2 → metrics  (if available)
///   3 → context  (if available)
///
/// If a dimension is not yet available, the cycle skips it.
fn current_secondary(state: &EntryDisplayState) -> Option<String> {
    // Build the available dimensions in order.
    let mut dims: Vec<String> = Vec::new();

    // Dim 1: type + size (always show once classified)
    if let Some(ref cls) = state.classification {
        dims.push(type_line(cls, state.physical.as_ref()));
    } else if state.context_failed {
        // Show "context unavailable" only in the sub-line, not as a warning.
        dims.push("context unavailable".to_string());
    }

    // Dim 2: metrics
    if let Some(ref m) = state.metrics {
        let line = metrics_line(m);
        if !line.is_empty() {
            dims.push(line);
        }
    }

    // Dim 3: context summary
    if let Some(ref ctx) = state.context {
        dims.push(context_line(ctx));
    }

    if dims.is_empty() {
        return None;
    }

    // Pick dimension by frame_index.
    // frame 0 = no sub-line; frame 1..N = dims[0..N-1]
    let available_frames = dims.len() + 1; // +1 for the "name only" frame
    let frame = state.frame_index % available_frames;
    if frame == 0 {
        None
    } else {
        dims.get(frame - 1).cloned()
    }
}

/// The "best" sub-line for plain/static output — show type+size if available.
fn best_sub_line(state: &EntryDisplayState) -> Option<String> {
    if let Some(ref cls) = state.classification {
        Some(type_line(cls, state.physical.as_ref()))
    } else {
        None
    }
}

fn entry_icon(state: &EntryDisplayState) -> &'static str {
    state
        .classification
        .as_ref()
        .map(|c| c.semantic_type.icon())
        .unwrap_or(if state.is_dir { "📁" } else { "📄" })
}

// ── Shared event application ──────────────────────────────────────────────────

fn apply_event(
    entries: &mut HashMap<PathBuf, EntryDisplayState>,
    order: &mut Vec<PathBuf>,
    event: PipelineEvent,
) {
    match event {
        PipelineEvent::Discovery(e) => {
            let path = e.path.clone();
            let is_dir = e.kind == EntryKind::Directory;
            if !entries.contains_key(&path) {
                order.push(path.clone());
                entries.insert(
                    path,
                    EntryDisplayState::new(e.path, e.basename, is_dir),
                );
            }
        }
        PipelineEvent::TypeDetected { path, classification } => {
            if let Some(s) = entries.get_mut(&path) {
                s.classification = Some(classification);
                s.presentation = s.presentation.advance();
            }
        }
        PipelineEvent::Measured(m) => {
            let path = m.path.clone();
            if let Some(s) = entries.get_mut(&path) {
                s.physical = Some(m);
                s.presentation = s.presentation.advance();
            }
        }
        PipelineEvent::Indexed(_) => {}
        PipelineEvent::ContextStarted { path } => {
            if let Some(s) = entries.get_mut(&path) {
                s.context_pending = true;
                s.context_failed  = false;
            }
        }
        PipelineEvent::ContextUpdated(a) => {
            if let Some(s) = entries.get_mut(&a.path) {
                s.context = Some(a);
                s.context_pending = false;
                s.context_failed  = false;
                s.presentation = s.presentation.advance();
            }
        }
        PipelineEvent::MetricsUpdated(m) => {
            let path = m.path.clone();
            if let Some(s) = entries.get_mut(&path) {
                s.metrics = Some(m);
                s.presentation = s.presentation.advance();
            }
        }
        PipelineEvent::Persisted { path, .. } => {
            if let Some(s) = entries.get_mut(&path) {
                s.presentation = PresentationState::Stable;
            }
        }
        PipelineEvent::Warning { path, message } => {
            // Context failures mark the entry; verbose detail goes to the
            // developer log (stderr), which is separated from the TTY display.
            let is_context_err = message.starts_with("context error:");
            if let Some(p) = path {
                if let Some(s) = entries.get_mut(&p) {
                    if is_context_err {
                        s.context_pending = false;
                        s.context_failed  = true;
                    }
                    // Always log to stderr so --output dev can surface it.
                    eprintln!("[warn] {}: {message}", s.basename);
                }
            } else {
                eprintln!("[warn] {message}");
            }
        }
        PipelineEvent::PipelineComplete => {}
    }
}

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
    format::{
        bold, context_line, context_line_plain, dim, metrics_fragment, metrics_fragment_plain,
        metrics_line, metrics_line_plain, type_fragment, type_fragment_plain, type_line,
        type_line_plain,
    },
    queue::QueueTracker,
    state::EntryDisplayState,
};
use crossterm::{cursor, style, terminal, QueueableCommand};
use gls_core::{
    entry::EntryKind,
    event::PipelineEvent,
    pipeline::{PipelineReceiver, PresentationState},
};
use std::collections::HashMap;
use std::io::{self, Write};
use std::path::PathBuf;
use std::time::Duration;
use tokio::sync::mpsc;

/// How many characters of a context summary to keep in the roomy row-mode
/// sub-line before truncating.
const ROW_CONTEXT_MAX_CHARS: usize = 70;
/// Same, but for the tighter horizontal budget inside a grid cell — grid
/// cells sit side-by-side with others in the same category block, so
/// staying compact here matters far more than in a full-width row.
pub(crate) const GRID_CONTEXT_MAX_CHARS: usize = 22;

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
    /// Lay the final listing out as a dynamically-categorised grid that
    /// fills the terminal width, instead of one entry per line.
    pub grid: bool,
}

impl Default for RenderConfig {
    fn default() -> Self {
        let is_tty = atty_check();
        Self {
            mode: if is_tty {
                OutputMode::Tty
            } else {
                OutputMode::Plain
            },
            tick_interval: if is_tty {
                Some(Duration::from_millis(600))
            } else {
                None
            },
            grid: is_tty,
        }
    }
}

/// Current terminal width in columns, falling back to 80 when it cannot be
/// determined (e.g. output is not attached to a TTY).
fn terminal_width() -> u16 {
    terminal::size().map(|(w, _)| w).unwrap_or(80)
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

    /// Consume the event stream until `PipelineComplete` or channel close.
    pub async fn run(self, rx: PipelineReceiver) {
        match self.config.mode {
            OutputMode::Json => json_run(rx).await,
            OutputMode::Plain => plain_run(rx, self.config.grid).await,
            OutputMode::Tty => tty_run(rx, self.config.tick_interval, self.config.grid).await,
        }
    }
}

// ── JSON mode ─────────────────────────────────────────────────────────────────

async fn json_run(mut rx: PipelineReceiver) {
    while let Some(event) = rx.recv().await {
        let json = serde_json::to_string(&event).unwrap_or_default();
        if writeln!(io::stdout(), "{json}").is_err() {
            break;
        }
        if matches!(event, PipelineEvent::PipelineComplete) {
            break;
        }
    }
}

// ── Plain mode ────────────────────────────────────────────────────────────────

async fn plain_run(mut rx: PipelineReceiver, grid: bool) {
    let mut entries: HashMap<PathBuf, EntryDisplayState> = HashMap::new();
    let mut order: Vec<PathBuf> = Vec::new();
    let mut warnings: Vec<String> = Vec::new();
    let mut queue = QueueTracker::default();

    while let Some(event) = rx.recv().await {
        let done = matches!(event, PipelineEvent::PipelineComplete);
        apply_event(&mut entries, &mut order, &mut warnings, &mut queue, event);
        if done {
            break;
        }
    }

    if grid {
        // By the time this prints, the pipeline has already completed, so
        // there's never anything left to queue — this is purely a settled
        // snapshot, same as the non-grid branch below.
        let rendered = crate::grid::render_grid(&entries, &order, &queue, terminal_width(), false);
        let _ = io::stdout().write_all(rendered.as_bytes());
    } else {
        let stdout = io::stdout();
        let mut output = stdout.lock();
        for path in &order {
            if let Some(state) = entries.get(path) {
                if print_plain_entry(&mut output, state).is_err() {
                    break;
                }
            }
        }
    }

    // Flush deferred warnings after output.
    for w in &warnings {
        eprintln!("{w}");
    }
}

fn print_plain_entry(output: &mut impl Write, state: &EntryDisplayState) -> io::Result<()> {
    let icon = entry_icon(state);
    if let Some(sub) = best_sub_line_plain(state) {
        writeln!(output, "{icon} {}  {sub}", state.basename)
    } else {
        writeln!(output, "{icon} {}", state.basename)
    }
}

// ── TTY retained-mode renderer ────────────────────────────────────────────────

/// A single rendered row (1 or 2 terminal lines).
#[derive(Clone, PartialEq, Eq)]
struct RenderedRow {
    primary: String,
    secondary: Option<String>,
}

impl RenderedRow {
    fn line_count(&self) -> u16 {
        if self.secondary.is_some() {
            2
        } else {
            1
        }
    }
}

async fn tty_run(mut rx: PipelineReceiver, tick_interval: Option<Duration>, grid: bool) {
    let mut entries: HashMap<PathBuf, EntryDisplayState> = HashMap::new();
    let mut order: Vec<PathBuf> = Vec::new();
    // Warnings deferred until after the grid exits.
    let mut warnings: Vec<String> = Vec::new();
    let mut queue = QueueTracker::default();

    // Only used in `!grid` mode (line-diffed retained rendering).
    let mut prev_rows: Vec<RenderedRow> = Vec::new();
    let mut prev_line_count: u16 = 0;

    let interval = tick_interval.unwrap_or(Duration::from_millis(600));
    // Tracked as a wall-clock deadline rather than a `tokio::time::Interval`
    // raced inside `select!`. TYPE/BART/context events for a real directory
    // keep arriving back-to-back, which keeps `rx.recv()` ready on almost
    // every poll — with `biased` select preferring that branch, an
    // interval future raced against it would starve and never fire, so the
    // spinner would appear to freeze even though results keep streaming in.
    // `select!` below only decides *when we wake up*; whether a frame
    // actually advances is decided afterwards purely by elapsed time.
    let mut next_tick = tokio::time::Instant::now() + interval;

    let mut pipeline_done = false;

    'outer: loop {
        // ── Wait for the next wakeup (event or tick deadline) ─────────────────
        tokio::select! {
            biased;
            result = rx.recv() => {
                match result {
                    Some(ev) => {
                        let done = matches!(ev, PipelineEvent::PipelineComplete);
                        apply_event(&mut entries, &mut order, &mut warnings, &mut queue, ev);
                        if done { pipeline_done = true; }
                    }
                    None => { pipeline_done = true; }
                }
            }
            _ = tokio::time::sleep_until(next_tick) => {}
        }

        // ── Drain any additional events that arrived in the same burst ────────
        // This is the key fix: instead of rendering after *every* event, we
        // consume everything currently in the channel before rendering once.
        // Each entry's own state (and the queue tracker) is still updated
        // immediately as its events are applied here — nothing waits for
        // any other entry to finish anything.
        loop {
            match rx.try_recv() {
                Ok(ev) => {
                    let done = matches!(ev, PipelineEvent::PipelineComplete);
                    apply_event(&mut entries, &mut order, &mut warnings, &mut queue, ev);
                    if done {
                        pipeline_done = true;
                    }
                }
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    pipeline_done = true;
                    break;
                }
            }
        }

        // ── Advance animation frames once the deadline has actually passed ────
        // Checked by wall clock, independent of which `select!` branch woke
        // us — otherwise a busy event stream would win that race every time
        // and the "thinking" spinner would never move. Purely cosmetic
        // (which spinner glyph to show for still-pending work) — it no
        // longer gates *which* completed sub-result is visible, since those
        // are all shown the instant they arrive.
        let now = tokio::time::Instant::now();
        if now >= next_tick {
            for path in &order {
                if let Some(s) = entries.get_mut(path) {
                    s.tick_frame();
                }
            }
            next_tick = now + interval;
        }

        // ── Render once ───────────────────────────────────────────────────────
        if !order.is_empty() {
            if grid {
                render_grid_frame(&entries, &order, &queue, &mut prev_line_count);
            } else {
                retained_render(
                    &entries,
                    &order,
                    &queue,
                    &mut prev_rows,
                    &mut prev_line_count,
                );
            }
        }

        if pipeline_done {
            break 'outer;
        }
    }

    // Leave cursor on a clean line below the output.
    let _ = io::stdout().write_all(b"\n");
    let _ = io::stdout().flush();

    // Now safe to flush deferred warnings to stderr.
    for w in &warnings {
        eprintln!("{w}");
    }
}

/// Redraw the whole grid: clear the previously-drawn region, rebuild it
/// from current state, print. Used continuously — during the live
/// "thinking" stages and for the final settled frame alike — rather than
/// only once at the end, since a categorised grid can restructure entirely
/// as more entries get classified (new columns appearing, categories
/// resorting) in a way line-diffing can't meaningfully track anyway.
fn render_grid_frame(
    entries: &HashMap<PathBuf, EntryDisplayState>,
    order: &[PathBuf],
    queue: &QueueTracker,
    prev_line_count: &mut u16,
) {
    let mut stdout = io::stdout();
    if *prev_line_count > 0 {
        let _ = stdout.queue(cursor::MoveToPreviousLine(*prev_line_count));
        let _ = stdout.queue(terminal::Clear(terminal::ClearType::FromCursorDown));
    }
    let rendered = crate::grid::render_grid(entries, order, queue, terminal_width(), true);
    let _ = stdout.queue(style::Print(&rendered));
    let _ = stdout.flush();
    *prev_line_count = rendered.matches('\n').count() as u16;
}

// ── Retained-mode redraw ──────────────────────────────────────────────────────

fn retained_render(
    entries: &HashMap<PathBuf, EntryDisplayState>,
    order: &[PathBuf],
    queue: &QueueTracker,
    prev_rows: &mut Vec<RenderedRow>,
    prev_line_count: &mut u16,
) {
    let new_rows: Vec<RenderedRow> = order
        .iter()
        .filter_map(|p| entries.get(p))
        .map(|s| render_row(s, queue))
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
        let prev = prev_iter.next();
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
fn render_row(state: &EntryDisplayState, queue: &QueueTracker) -> RenderedRow {
    let icon = entry_icon(state);
    let name = bold(&state.basename);
    let primary = format!("{icon} {name}");
    let secondary = Some(live_status_line(
        state,
        queue,
        true,
        ROW_CONTEXT_MAX_CHARS,
        true,
    ));
    RenderedRow { primary, secondary }
}

/// Build the current status line for one entry: every completed insight
/// layer's result, joined together immediately as each becomes available.
/// Nothing is held back waiting for a rotation timer, and nothing waits on
/// any other entry — this reflects exactly what's known about `state` right
/// now. Shared between the row-mode sub-line and grid cells:
/// - `roomy` picks the full-detail fragments (BART size folded into the
///   type fragment, full `target_market` label in the metrics fragment)
///   for row mode, vs. compact ones for grid cells — a grid cell sits
///   side-by-side with others in the same category block, so a single
///   free-text sentence there can blow the whole column out past the
///   point of fitting more than one per row.
/// - `context_max_chars` bounds how much of a context summary to show
///   before truncating — grid cells need a tighter budget than a full-width
///   row.
/// - `color` picks ANSI-coloured vs. plain text.
pub(crate) fn live_status_line(
    state: &EntryDisplayState,
    queue: &QueueTracker,
    roomy: bool,
    context_max_chars: usize,
    color: bool,
) -> String {
    let mut dims: Vec<String> = Vec::new();

    // Type layer.
    if let Some(ref cls) = state.classification {
        dims.push(match (roomy, color) {
            (true, true) => type_line(cls, state.physical.as_ref()),
            (true, false) => type_line_plain(cls, state.physical.as_ref()),
            (false, true) => type_fragment(cls),
            (false, false) => type_fragment_plain(cls),
        });
    } else {
        dims.push(loading_line(state, "type", color));
    }

    // Metrics layer.
    if let Some(ref m) = state.metrics {
        let line = match (roomy, color) {
            (true, true) => metrics_line(m),
            (true, false) => metrics_line_plain(m),
            (false, true) => metrics_fragment(m),
            (false, false) => metrics_fragment_plain(m),
        };
        if !line.is_empty() {
            dims.push(line);
        }
    } else {
        dims.push(loading_line(state, "metrics", color));
    }

    // Context layer: real content once ready, an active spinner once the
    // provider call has actually started, a queue position/ETA while still
    // waiting its turn, or nothing at all if context isn't applicable to
    // this entry (`--no-context`, incremental cache hit, excluded by budget).
    if let Some(ref ctx) = state.context {
        dims.push(if color {
            context_line(ctx, context_max_chars)
        } else {
            context_line_plain(ctx, context_max_chars)
        });
    } else if state.context_failed {
        let msg = "⚠ context unavailable";
        dims.push(if color { dim(msg) } else { msg.to_string() });
    } else if state.context_pending {
        dims.push(loading_line(state, "context", color));
    } else if let Some(q) = queue.status_for(&state.path) {
        let msg = format!(
            "in line for {}, position {} of {} \u{b7} eta {}s",
            q.provider, q.position, q.total, q.eta_secs
        );
        dims.push(if color { dim(&msg) } else { msg });
    }

    let sep = if color {
        format!(" {} ", dim("\u{b7}"))
    } else {
        " \u{b7} ".to_string()
    };
    dims.join(&sep)
}

/// A compact progress item for an unfinished insight layer.
fn loading_line(state: &EntryDisplayState, layer: &str, color: bool) -> String {
    // Offset the layers so a listing shows a small array of moving markers
    // rather than three identical glyphs on every tick.
    const FRAMES: [&str; 4] = ["◌", "◍", "●", "◍"];
    let offset = match layer {
        "type" => 0,
        "metrics" => 1,
        _ => 2,
    };
    let marker = FRAMES[(state.frame_index + offset) % FRAMES.len()];
    let label = match layer {
        "type" => "thinking type",
        "metrics" => "thinking metrics",
        _ => "thinking context",
    };
    if !color {
        return format!("{marker} {label}");
    }
    let colour = match layer {
        "type" => form3::ansi::fg(&form3::ansi::AnsiColor::Standard(form3::ansi::Color::Yellow, false)),
        "metrics" => form3::ansi::fg(&form3::ansi::AnsiColor::Standard(form3::ansi::Color::Green, false)),
        _ => form3::ansi::fg(&form3::ansi::AnsiColor::Standard(form3::ansi::Color::Magenta, false)),
    };
    format!("{colour}{marker}{} {label}", form3::ansi::reset())
}

/// Best sub-line for plain (non-animated) output. This prints once, after
/// the pipeline has fully completed, so the context summary is shown in
/// full rather than truncated to a live-display width budget.
fn best_sub_line_plain(state: &EntryDisplayState) -> Option<String> {
    if let Some(ref ctx) = state.context {
        return Some(context_line_plain(ctx, usize::MAX));
    }
    if let Some(ref m) = state.metrics {
        let line = metrics_line_plain(m);
        if !line.is_empty() {
            return Some(line);
        }
    }
    state
        .classification
        .as_ref()
        .map(|cls| type_line_plain(cls, state.physical.as_ref()))
}

pub(crate) fn entry_icon(state: &EntryDisplayState) -> &'static str {
    state
        .classification
        .as_ref()
        .map(|c| c.semantic_type.icon())
        .unwrap_or(if state.is_dir { "📁" } else { "📄" })
}

// ── Event application ─────────────────────────────────────────────────────────

/// Mutate `entries`/`order` according to `event`.
///
/// Warnings are pushed to `warnings` (not printed) so the live grid is never
/// contaminated by external content.
fn apply_event(
    entries: &mut HashMap<PathBuf, EntryDisplayState>,
    order: &mut Vec<PathBuf>,
    warnings: &mut Vec<String>,
    queue: &mut QueueTracker,
    event: PipelineEvent,
) {
    match event {
        PipelineEvent::Discovery(e) => {
            let path = e.path.clone();
            let is_dir = e.kind == EntryKind::Directory;
            // Guard: never insert the same path twice.
            if let std::collections::hash_map::Entry::Vacant(slot) = entries.entry(path.clone()) {
                order.push(path);
                let mut s = EntryDisplayState::new(e.path, e.basename, is_dir);
                s.logical_bytes = e.logical_bytes;
                s.modified = e.modified;
                slot.insert(s);
            }
        }

        PipelineEvent::OrderUpdated { paths } => {
            let mut seen = std::collections::HashSet::new();
            *order = paths
                .into_iter()
                .filter(|path| entries.contains_key(path) && seen.insert(path.clone()))
                .collect();
        }

        PipelineEvent::TypeDetected {
            path,
            classification,
        } => {
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

        PipelineEvent::Indexed(_) => { /* no visible change */ }

        PipelineEvent::ContextQueued {
            path,
            position,
            total,
            provider,
        } => {
            queue.queued(path, position, total, provider);
        }

        PipelineEvent::ContextStarted { path } => {
            queue.started(&path);
            if let Some(s) = entries.get_mut(&path) {
                s.context_pending = true;
                s.context_failed = false;
            }
        }

        PipelineEvent::ContextUpdated(a) => {
            queue.completed(&a.path);
            if let Some(s) = entries.get_mut(&a.path) {
                s.context = Some(a);
                s.context_pending = false;
                s.context_failed = false;
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
            // Mark context failure on the entry; never print during live grid.
            let is_context =
                message.starts_with("context error:") || message.starts_with("context unavailable");

            if let Some(ref p) = path {
                if is_context {
                    queue.completed(p);
                }
                if let Some(s) = entries.get_mut(p) {
                    if is_context {
                        s.context_pending = false;
                        s.context_failed = true;
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
                    s.context_failed = true;
                }
            }
            warnings.push(format!(
                "[warn] context provider '{provider}' unavailable: {reason}"
            ));
        }

        PipelineEvent::PipelineComplete => { /* handled by caller */ }
    }
}

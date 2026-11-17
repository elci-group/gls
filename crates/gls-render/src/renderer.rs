//! Event-driven renderer for plain, JSON, and animated terminal output.
//!
//! Terminal frames are repainted using 3form cursor primitives. Warnings are
//! deferred until the live display has finished.
//!
//! In TTY mode, work still in flight is shown as real-time cascading progress
//! scan bars (see [`form3::scan`]) animated from the wall clock at
//! [`ANIM_FRAME`], separately from the slower metadata carousel. Each
//! pipeline result is drawn on the next animation frame after it arrives;
//! repaints are coalesced to that frame rate so a stream of per-item events
//! cannot cause a redraw storm.

use crate::{
    format::{
        bold, context_line, context_line_plain, dim, metrics_fragment, metrics_fragment_plain,
        metrics_line, metrics_line_plain, type_fragment, type_fragment_plain, type_line,
        type_line_plain,
    },
    queue::QueueTracker,
    state::EntryDisplayState,
    telemetry::Telemetry,
};
use form3::{ansi, scan, scan::Cascade};
use gls_core::{
    entry::EntryKind,
    event::{PipelineEvent, ToolProgress, ToolState},
    pipeline::{PipelineReceiver, PresentationState},
};
use std::collections::HashMap;
use std::io::IsTerminal;
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

/// Timing of the cascading scan bars: one sweep every 1.1 s, each row (and
/// each concurrently running HUD tool) trailing the previous by 8% of a cycle.
pub(crate) const SCAN_CASCADE: Cascade = Cascade::new(Duration::from_millis(1100), 0.08);
/// Width, in cells, of a per-entry scan bar.
const ENTRY_BAR_WIDTH: usize = 6;
/// Width, in cells, of a per-tool HUD progress scan bar.
pub(crate) const HUD_BAR_WIDTH: usize = 8;

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
    #[cfg(unix)]
    {
        let mut size: libc::winsize = unsafe { std::mem::zeroed() };
        if unsafe { libc::ioctl(libc::STDOUT_FILENO, libc::TIOCGWINSZ, &mut size) } == 0
            && size.ws_col > 0
        {
            return size.ws_col;
        }
    }
    80
}

fn atty_check() -> bool {
    io::stdout().is_terminal()
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
    let mut telemetry = Telemetry::default();

    while let Some(event) = rx.recv().await {
        let done = matches!(event, PipelineEvent::PipelineComplete);
        apply_event(
            &mut entries,
            &mut order,
            &mut warnings,
            &mut queue,
            &mut telemetry,
            event,
        );
        if done {
            // Drain any trailing telemetry events (e.g. padagonia completed)
            // that were emitted after the completion marker.
            loop {
                match rx.try_recv() {
                    Ok(ev) => apply_event(
                        &mut entries,
                        &mut order,
                        &mut warnings,
                        &mut queue,
                        &mut telemetry,
                        ev,
                    ),
                    Err(mpsc::error::TryRecvError::Empty) => break,
                    Err(mpsc::error::TryRecvError::Disconnected) => break,
                }
            }
            break;
        }
    }

    // Print the final telemetry HUD before the listing.
    let hud = telemetry.render(false, Duration::ZERO);
    let _ = writeln!(io::stdout(), "{hud}");

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

/// Repaint cadence for scan-bar animation (≈15 fps). Also the upper bound on
/// how long a freshly computed result waits before it is drawn.
const ANIM_FRAME: Duration = Duration::from_millis(66);

/// Whether anything on screen is still in flight and needs its scan bar
/// animated: a pending insight layer on some entry, or a running tool.
fn is_animating(entries: &HashMap<PathBuf, EntryDisplayState>, telemetry: &Telemetry) -> bool {
    telemetry.any_running()
        || entries
            .values()
            .any(|s| s.classification.is_none() || s.context_pending)
}

async fn tty_run(mut rx: PipelineReceiver, tick_interval: Option<Duration>, grid: bool) {
    let mut entries: HashMap<PathBuf, EntryDisplayState> = HashMap::new();
    let mut order: Vec<PathBuf> = Vec::new();
    // Warnings deferred until after the grid exits.
    let mut warnings: Vec<String> = Vec::new();
    let mut queue = QueueTracker::default();
    let mut telemetry = Telemetry::default();

    let mut prev_line_count: u16 = 0;

    // `None` means animation was explicitly disabled: bars still render, but
    // frozen, and no fast animation frames are scheduled.
    let animate = tick_interval.is_some();
    let interval = tick_interval.unwrap_or(Duration::from_millis(600));
    // Both clocks are tracked as wall-clock deadlines rather than
    // `tokio::time::Interval`s raced inside `select!`: per-item events keep
    // `rx.recv()` ready on almost every poll, and with `biased` select an
    // interval future would starve. `select!` only decides *when we wake
    // up*; what happens is decided afterwards purely by elapsed time.
    //
    // `next_tick` drives the slow metadata carousel; `next_frame` gates
    // repaints (scan-bar motion and newly arrived results).
    let anim_epoch = tokio::time::Instant::now();
    let mut next_tick = anim_epoch + interval;
    let mut next_frame = anim_epoch;
    // Something changed since the last repaint.
    let mut dirty = false;

    let mut pipeline_done = false;
    let mut channel_closed = false;
    // Once the pipeline completes, keep the carousel ticking long enough for
    // the user to see the rotating metadata layers (type, metrics, context).
    // If context is enabled we also wait for in-flight context calls to
    // settle, capped so a hanging provider cannot block the UI forever.
    // Stopping on the context frame avoids shrinking rows back to name-only
    // and leaving the cursor inside the grid.
    let mut done_ticks: usize = 0;
    const DONE_TICK_TARGET: usize = 3;
    const POST_COMPLETION_TIMEOUT: Duration = Duration::from_secs(10);
    let mut pipeline_done_at: Option<tokio::time::Instant> = None;

    let paint = |entries: &mut HashMap<PathBuf, EntryDisplayState>,
                 order: &[PathBuf],
                 queue: &QueueTracker,
                 telemetry: &Telemetry,
                 now: tokio::time::Instant,
                 prev_line_count: &mut u16| {
        if order.is_empty() {
            return;
        }
        let elapsed = if animate {
            now.duration_since(anim_epoch)
        } else {
            Duration::ZERO
        };
        // Each row's bars lag the row above, so sweeps cascade down the list.
        for (slot, path) in order.iter().enumerate() {
            if let Some(s) = entries.get_mut(path) {
                s.scan_phase = SCAN_CASCADE.phase(elapsed, slot);
            }
        }
        let hud = telemetry.render(true, elapsed);
        if grid {
            render_grid_frame(entries, order, queue, &hud, prev_line_count);
        } else {
            retained_render(entries, order, queue, &hud, prev_line_count);
        }
    };

    loop {
        // ── Wait for the next wakeup (event, carousel tick, or frame) ─────────
        let wake_at = if dirty || (animate && is_animating(&entries, &telemetry)) {
            next_tick.min(next_frame)
        } else {
            next_tick
        };
        tokio::select! {
            biased;
            result = rx.recv(), if !channel_closed => {
                match result {
                    Some(ev) => {
                        let done = matches!(ev, PipelineEvent::PipelineComplete);
                        apply_event(
                            &mut entries,
                            &mut order,
                            &mut warnings,
                            &mut queue,
                            &mut telemetry,
                            ev,
                        );
                        dirty = true;
                        if done { pipeline_done = true; }
                    }
                    None => { pipeline_done = true; channel_closed = true; }
                }
            }
            _ = tokio::time::sleep_until(wake_at) => {}
        }

        // ── Drain any additional events that arrived in the same burst ────────
        // Each entry's own state (and the queue tracker) is updated
        // immediately as its events are applied here — nothing waits for
        // any other entry to finish anything.
        loop {
            match rx.try_recv() {
                Ok(ev) => {
                    let done = matches!(ev, PipelineEvent::PipelineComplete);
                    apply_event(
                        &mut entries,
                        &mut order,
                        &mut warnings,
                        &mut queue,
                        &mut telemetry,
                        ev,
                    );
                    dirty = true;
                    if done {
                        pipeline_done = true;
                    }
                }
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    pipeline_done = true;
                    channel_closed = true;
                    break;
                }
            }
        }

        // ── Advance the carousel once its deadline has actually passed ───────
        let now = tokio::time::Instant::now();
        if now >= next_tick {
            for path in &order {
                if let Some(s) = entries.get_mut(path) {
                    s.tick_frame();
                }
            }
            next_tick = now + interval;
            dirty = true;
            if pipeline_done {
                done_ticks += 1;
            }
        }

        // ── Repaint at most once per animation frame ──────────────────────────
        // New results and scan-bar motion both land here; coalescing to
        // `ANIM_FRAME` keeps a fast event stream from redrawing per event.
        let animating = animate && is_animating(&entries, &telemetry);
        if (dirty || animating) && now >= next_frame {
            paint(
                &mut entries,
                &order,
                &queue,
                &telemetry,
                now,
                &mut prev_line_count,
            );
            dirty = false;
            next_frame = now + ANIM_FRAME;
        }

        if pipeline_done && pipeline_done_at.is_none() {
            pipeline_done_at = Some(now);
        }
        let post_completion_timed_out = pipeline_done_at
            .map(|t| now.duration_since(t) >= POST_COMPLETION_TIMEOUT)
            .unwrap_or(false);
        let context_settled = entries.values().all(|s| !s.context_pending);

        if pipeline_done
            && done_ticks >= DONE_TICK_TARGET
            && (context_settled || post_completion_timed_out)
        {
            // Never exit on a stale frame that was deferred by throttling.
            if dirty {
                paint(
                    &mut entries,
                    &order,
                    &queue,
                    &telemetry,
                    now,
                    &mut prev_line_count,
                );
            }
            break;
        }
    }

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
    hud: &str,
    prev_line_count: &mut u16,
) {
    let rendered = crate::grid::render_grid(entries, order, queue, terminal_width(), true);
    let frame = format!("{hud}\n{rendered}");
    write_frame(&mut io::stdout(), &frame, prev_line_count);
}

// Repaint from the top whenever row heights change. Diffing individual rows
// cannot safely account for a secondary line appearing or disappearing.
fn retained_render(
    entries: &HashMap<PathBuf, EntryDisplayState>,
    order: &[PathBuf],
    queue: &QueueTracker,
    hud: &str,
    prev_line_count: &mut u16,
) {
    let rows: Vec<RenderedRow> = order
        .iter()
        .filter_map(|p| entries.get(p))
        .map(|s| render_row(s, queue))
        .collect();
    let mut frame = format!("{hud}\n");
    for row in &rows {
        frame.push_str(&row.primary);
        frame.push('\n');
        if let Some(secondary) = &row.secondary {
            frame.push_str("   ");
            frame.push_str(secondary);
            frame.push('\n');
        }
    }
    write_frame(&mut io::stdout(), &frame, prev_line_count);
}

/// Repaint `frame` over the previous one.
///
/// Each line is cleared just before it is rewritten (rather than wiping the
/// whole region first and then redrawing), and the repaint is wrapped in a
/// synchronized update, so ~15 fps scan-bar animation doesn't flicker.
/// Anything left below a frame that shrank is cleared at the end.
fn write_frame(output: &mut impl Write, frame: &str, prev_line_count: &mut u16) {
    let mut buf = String::with_capacity(frame.len() + 256);
    buf.push_str(ansi::begin_synchronized_update());
    if *prev_line_count > 0 {
        buf.push_str(&ansi::move_up(*prev_line_count));
    }
    buf.push('\r');
    for line in frame.split_inclusive('\n') {
        buf.push_str(ansi::clear_line());
        buf.push_str(line);
    }
    buf.push_str(ansi::clear_to_end_of_screen());
    buf.push_str(ansi::end_synchronized_update());
    let _ = output.write_all(buf.as_bytes());
    let _ = output.flush();
    *prev_line_count = frame
        .bytes()
        .filter(|&b| b == b'\n')
        .count()
        .min(u16::MAX as usize) as u16;
}

// ── Row rendering ─────────────────────────────────────────────────────────────

/// Build the rendered representation of one entry for the current carousel
/// frame.
fn render_row(state: &EntryDisplayState, queue: &QueueTracker) -> RenderedRow {
    let icon = entry_icon(state);
    let name = bold(&state.basename);
    let primary = format!("{icon} {name}");
    let secondary = carousel_frame(state, queue, ROW_CONTEXT_MAX_CHARS, true);
    RenderedRow { primary, secondary }
}

/// Build one carousel frame for the row-mode sub-line.
///
/// The carousel has four fixed frames:
///   0 → name only (no secondary line)
///   1 → type layer
///   2 → metrics layer
///   3 → context layer
///
/// Keeping the frame count fixed means pending loaders animate before any
/// result arrives and completed results can replace their loading spinner
/// without resetting the carousel position.
fn carousel_frame(
    state: &EntryDisplayState,
    queue: &QueueTracker,
    context_max_chars: usize,
    color: bool,
) -> Option<String> {
    match state.frame_index % 4 {
        0 => None,
        1 => Some(type_carousel_frame(state, color)),
        2 => Some(metrics_carousel_frame(state, color)),
        3 => Some(context_carousel_frame(
            state,
            queue,
            context_max_chars,
            color,
        )),
        _ => unreachable!(),
    }
}

fn type_carousel_frame(state: &EntryDisplayState, color: bool) -> String {
    if let Some(ref cls) = state.classification {
        if color {
            type_line(cls, state.physical.as_ref())
        } else {
            type_line_plain(cls, state.physical.as_ref())
        }
    } else {
        loading_line(state, "type", color)
    }
}

fn metrics_carousel_frame(state: &EntryDisplayState, color: bool) -> String {
    if let Some(ref m) = state.metrics {
        let line = if color {
            metrics_line(m)
        } else {
            metrics_line_plain(m)
        };
        if !line.is_empty() {
            line
        } else {
            // Fallback to type+size when metrics are empty
            type_carousel_frame(state, color)
        }
    } else {
        // Fallback to type+size when metrics are not yet available
        type_carousel_frame(state, color)
    }
}

fn context_carousel_frame(
    state: &EntryDisplayState,
    queue: &QueueTracker,
    context_max_chars: usize,
    color: bool,
) -> String {
    if let Some(ref ctx) = state.context {
        if color {
            context_line(ctx, context_max_chars)
        } else {
            context_line_plain(ctx, context_max_chars)
        }
    } else if state.context_failed {
        let msg = "⚠ context unavailable";
        if color {
            dim(msg)
        } else {
            msg.to_string()
        }
    } else if state.context_pending {
        loading_line(state, "context", color)
    } else if let Some(q) = queue.status_for(&state.path) {
        let msg = format!(
            "in line for {}, position {} of {} \u{b7} eta {}s",
            q.provider, q.position, q.total, q.eta_secs
        );
        if color {
            dim(&msg)
        } else {
            msg
        }
    } else {
        let msg = "—";
        if color {
            dim(msg)
        } else {
            msg.to_string()
        }
    }
}

/// Build the current status line for one entry: every completed insight
/// layer's result, joined together immediately as each becomes available.
/// Nothing is held back waiting for a rotation timer, and nothing waits on
/// any other entry — this reflects exactly what's known about `state` right
/// now. Used for grid cells, where a compact single-line summary is needed:
/// - `context_max_chars` bounds how much of a context summary to show
///   before truncating.
/// - `color` picks ANSI-coloured vs. plain text.
pub(crate) fn live_status_line(
    state: &EntryDisplayState,
    queue: &QueueTracker,
    context_max_chars: usize,
    color: bool,
) -> String {
    let mut dims: Vec<String> = Vec::new();

    // Type layer.
    if let Some(ref cls) = state.classification {
        dims.push(if color {
            type_fragment(cls)
        } else {
            type_fragment_plain(cls)
        });
    } else {
        dims.push(loading_line(state, "type", color));
    }

    // Metrics layer.
    if let Some(ref m) = state.metrics {
        let line = if color {
            metrics_fragment(m)
        } else {
            metrics_fragment_plain(m)
        };
        if !line.is_empty() {
            dims.push(line);
        }
    }
    // Note: we don't show a loader for metrics in grid mode - type+size is always shown above

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

/// A compact progress item for an unfinished insight layer: a real-time
/// scan bar followed by the layer name.
///
/// `scan_phase` is set from the wall clock each animation frame, offset by
/// the entry's row so bars cascade down the listing. Layers within the same
/// entry continue that cascade one step further each, so the type and
/// context bars on one grid cell are visibly distinct.
fn loading_line(state: &EntryDisplayState, layer: &str, color: bool) -> String {
    let layer_slot = match layer {
        "type" => 0.0,
        "metrics" => 1.0,
        _ => 2.0,
    };
    let phase = SCAN_CASCADE.lag(state.scan_phase, layer_slot);
    let colour = color.then(|| {
        let c = match layer {
            "type" => ansi::Color::Yellow,
            "metrics" => ansi::Color::Green,
            _ => ansi::Color::Magenta,
        };
        ansi::AnsiColor::Standard(c, false)
    });
    let bar = scan::scan_bar(phase, ENTRY_BAR_WIDTH, colour.as_ref());
    let label = match layer {
        "type" => "type",
        "metrics" => "metrics",
        _ => "context",
    };
    if color {
        format!("{bar} {}", dim(label))
    } else {
        format!("{bar} {label}")
    }
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
    telemetry: &mut Telemetry,
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
        PipelineEvent::CacheStatus { .. } => { /* telemetry-only */ }
        PipelineEvent::CacheHydrated { .. } => { /* persistence marker */ }

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

            // Surface the latest warning in the HUD so failures are visible
            // without scrolling back through stderr.
            telemetry.update(ToolProgress {
                tool: "warn".to_string(),
                state: ToolState::Failed,
                current: 0,
                total: 0,
                message: Some(message),
            });
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

            telemetry.update(ToolProgress {
                tool: "ctx".to_string(),
                state: ToolState::Failed,
                current: 0,
                total: 0,
                message: Some(format!("{provider}: {reason}")),
            });
        }

        PipelineEvent::ToolProgress(p) => {
            telemetry.update(p);
        }

        PipelineEvent::PipelineComplete => { /* handled by caller */ }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn frame_repaint_clears_old_lines_and_tracks_new_height() {
        let mut output = Vec::new();
        let mut lines = 0;
        super::write_frame(&mut output, "hud\nentry\nextra\n", &mut lines);
        assert_eq!(lines, 3);
        output.clear();
        super::write_frame(&mut output, "hud\nentry\n", &mut lines);
        let text = String::from_utf8(output).unwrap();
        assert!(text.starts_with(&format!(
            "{}{}\r",
            form3::ansi::begin_synchronized_update(),
            form3::ansi::move_up(3)
        )));
        // Every line is cleared right before it is rewritten…
        assert!(text.contains(&format!("{}hud\n", form3::ansi::clear_line())));
        assert!(text.contains(&format!("{}entry\n", form3::ansi::clear_line())));
        // …and leftovers from the taller previous frame are wiped.
        assert!(text.ends_with(&format!(
            "{}{}",
            form3::ansi::clear_to_end_of_screen(),
            form3::ansi::end_synchronized_update()
        )));
        assert_eq!(lines, 2);
    }

    use super::*;
    use gls_core::semantic::{Classification, SemanticType};
    use std::path::PathBuf;

    fn test_state(frame_index: usize) -> EntryDisplayState {
        let mut s =
            EntryDisplayState::new(PathBuf::from("/tmp/demo.rs"), "demo.rs".to_string(), false);
        s.frame_index = frame_index;
        s
    }

    #[test]
    fn carousel_frame_zero_is_name_only() {
        let state = test_state(0);
        let queue = QueueTracker::default();
        assert!(carousel_frame(&state, &queue, ROW_CONTEXT_MAX_CHARS, false).is_none());
    }

    #[test]
    fn carousel_frame_one_shows_type_loader_when_unclassified() {
        let state = test_state(1);
        let queue = QueueTracker::default();
        let line = carousel_frame(&state, &queue, ROW_CONTEXT_MAX_CHARS, false)
            .expect("frame 1 should have a sub-line");
        assert!(line.ends_with(" type"));
        assert!(
            line.contains('─'),
            "pending type should show a scan bar: {line}"
        );
    }

    #[test]
    fn carousel_frame_one_shows_classification_when_ready() {
        let mut state = test_state(1);
        state.classification = Some(Classification {
            semantic_type: SemanticType::SourceCode,
            confidence: 1.0,
            evidence: vec![],
            language: Some("Rust".to_string()),
        });
        let queue = QueueTracker::default();
        let line = carousel_frame(&state, &queue, ROW_CONTEXT_MAX_CHARS, false)
            .expect("frame 1 should have a sub-line");
        assert!(
            !line.contains('─'),
            "classified type should not scan: {line}"
        );
    }

    #[test]
    fn carousel_frame_two_shows_type_fallback_when_metrics_missing() {
        let state = test_state(2);
        let queue = QueueTracker::default();
        let line = carousel_frame(&state, &queue, ROW_CONTEXT_MAX_CHARS, false)
            .expect("frame 2 should have a sub-line");
        assert!(line.ends_with(" type") && line.contains('─'));
    }

    #[test]
    fn carousel_frame_three_shows_context_placeholder_when_not_applicable() {
        let state = test_state(3);
        let queue = QueueTracker::default();
        let line = carousel_frame(&state, &queue, ROW_CONTEXT_MAX_CHARS, false)
            .expect("frame 3 should have a sub-line");
        assert_eq!(line, "—");
    }

    #[test]
    fn carousel_tick_does_not_drive_scan_motion() {
        let mut state = test_state(1);
        state.scan_phase = 0.25;
        state.tick_frame();
        assert_eq!(state.frame_index, 2);
        assert_eq!(state.scan_phase, 0.25);
    }

    #[test]
    fn loading_bar_follows_scan_phase_and_cascades_by_row() {
        let mut a = test_state(1);
        let mut b = test_state(1);
        let t = Duration::from_millis(400);
        a.scan_phase = SCAN_CASCADE.phase(t, 0);
        b.scan_phase = SCAN_CASCADE.phase(t, 5);
        let la = loading_line(&a, "type", false);
        let lb = loading_line(&b, "type", false);
        assert_ne!(la, lb, "rows should be offset in the cascade");
        assert_eq!(
            form3::width::display_width(&la),
            form3::width::display_width(&loading_line(&a, "type", true)),
            "plain and coloured widths must match for grid layout"
        );
    }

    #[test]
    fn grid_status_shows_scan_bars_for_pending_layers() {
        let mut state = test_state(0);
        state.context_pending = true;
        let line = live_status_line(
            &state,
            &QueueTracker::default(),
            GRID_CONTEXT_MAX_CHARS,
            false,
        );
        assert!(
            line.contains(" type") && line.contains(" context"),
            "{line}"
        );
        assert!(!line.contains("thinking"));
    }
}

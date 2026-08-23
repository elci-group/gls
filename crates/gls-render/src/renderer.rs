//! Main renderer: consumes `PipelineEvent` stream, manages per-entry state,
//! drives the animation loop.

use crate::{
    format::{context_line, metrics_line, type_line},
    state::EntryDisplayState,
};
use crossterm::{
    cursor,
    style,
    terminal, QueueableCommand,
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

/// How output should be presented.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputMode {
    /// Animated TTY output.
    Tty,
    /// Plain text, no ANSI codes.
    Plain,
    /// JSON per event on stdout.
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
        let is_tty = crossterm::terminal::is_raw_mode_enabled()
            .ok()
            .is_some_and(|r| r)
            || atty_check();
        Self {
            mode: if is_tty { OutputMode::Tty } else { OutputMode::Plain },
            tick_interval: if is_tty {
                Some(Duration::from_millis(800))
            } else {
                None
            },
        }
    }
}

fn atty_check() -> bool {
    // Simple: check if stdout fd is a tty.
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

/// The renderer.  Run [`Renderer::run`] to consume the stream.
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

// ── Plain mode ───────────────────────────────────────────────────────────────

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

    // Final static render.
    for path in &order {
        if let Some(state) = entries.get(path) {
            print_plain_entry(state);
        }
    }
}

fn print_plain_entry(state: &EntryDisplayState) {
    let icon = state
        .classification
        .as_ref()
        .map(|c| c.semantic_type.icon())
        .unwrap_or(if state.is_dir { "📁" } else { "📄" });

    print!("{icon} {}", state.basename);
    if let (Some(cls), phys) = (&state.classification, &state.physical) {
        print!("  {}", type_line(cls, phys.as_ref()));
    }
    println!();

    if let Some(ref ctx) = state.context {
        println!("   {}", context_line(ctx));
    }
    if let Some(ref m) = state.metrics {
        let line = metrics_line(m);
        if !line.is_empty() {
            println!("   {line}");
        }
    }
}

// ── TTY animated mode ─────────────────────────────────────────────────────────

async fn tty_run(
    mut rx: broadcast::Receiver<PipelineEvent>,
    tick_interval: Option<Duration>,
) {
    let mut entries: HashMap<PathBuf, EntryDisplayState> = HashMap::new();
    let mut order: Vec<PathBuf> = Vec::new();
    let interval = tick_interval.unwrap_or(Duration::from_millis(800));

    let mut ticker = tokio::time::interval(interval);
    let mut pipeline_done = false;
    let mut dirty = false;

    loop {
        tokio::select! {
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
                    Err(_) => {
                        pipeline_done = true;
                    }
                }
            }
            _ = ticker.tick() => {
                // Advance animation frames.
                for path in &order {
                    if let Some(s) = entries.get_mut(path) {
                        s.tick_frame();
                    }
                }
                dirty = true;
            }
        }

        if dirty {
            render_tty_frame(&entries, &order);
            dirty = false;
        }

        if pipeline_done {
            // One final render with stable state.
            render_tty_frame(&entries, &order);
            break;
        }
    }
}

fn render_tty_frame(
    entries: &HashMap<PathBuf, EntryDisplayState>,
    order: &[PathBuf],
) {
    let mut stdout = io::stdout();
    let _ = stdout.queue(cursor::MoveToColumn(0));

    // Move up by the number of entries we previously drew (each entry is 1–3
    // lines).  We track nothing about previous line count here, so we use a
    // simple clear-and-redraw strategy when the terminal supports it.
    let _ = stdout.queue(terminal::Clear(terminal::ClearType::FromCursorDown));

    for path in order {
        let Some(state) = entries.get(path) else { continue };
        let line = animated_line(state);
        let _ = stdout.queue(style::Print(format!("{line}\n")));

        // Sub-line (second dimension when cycling through frames > 0).
        if let Some(sub) = animated_sub_line(state) {
            let _ = stdout.queue(style::Print(format!("   {sub}\n")));
        }
    }

    let _ = stdout.flush();
}

fn animated_line(state: &EntryDisplayState) -> String {
    let icon = state
        .classification
        .as_ref()
        .map(|c| c.semantic_type.icon())
        .unwrap_or(if state.is_dir { "📁" } else { "📄" });

    let processing_indicator = if state.context_pending
        && state.context.is_none()
    {
        " ◌"
    } else if state.context.is_some() {
        " ✓"
    } else {
        ""
    };

    format!("{icon} {}{processing_indicator}", state.basename)
}

fn animated_sub_line(state: &EntryDisplayState) -> Option<String> {
    let frame = state.frame_index;
    let available = state.available_frames();
    if available <= 1 {
        return None;
    }

    // Frame 0 = name only (no sub-line)
    // Frame 1 = type + size
    // Frame 2 = metrics
    // Frame 3 = context summary
    // Frame 4 = target market
    match frame % available {
        0 => None,
        1 => {
            if let Some(ref cls) = state.classification {
                let phys = state.physical.as_ref();
                Some(format!(
                    "{} {}",
                    cls.semantic_type.icon(),
                    type_line(cls, phys)
                ))
            } else {
                None
            }
        }
        2 => state.metrics.as_ref().map(|m| metrics_line(m)),
        3 => state.context.as_ref().map(|c| context_line(c)),
        _ => None,
    }
}

// ── Shared event application ─────────────────────────────────────────────────

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
            if let Some(s) = entries.get_mut(&m.path) {
                s.physical = Some(m);
                s.presentation = s.presentation.advance();
            }
        }
        PipelineEvent::Indexed(_) => {}
        PipelineEvent::ContextStarted { path } => {
            if let Some(s) = entries.get_mut(&path) {
                s.context_pending = true;
            }
        }
        PipelineEvent::ContextUpdated(a) => {
            if let Some(s) = entries.get_mut(&a.path) {
                s.context = Some(a);
                s.context_pending = false;
                s.presentation = s.presentation.advance();
            }
        }
        PipelineEvent::MetricsUpdated(m) => {
            if let Some(s) = entries.get_mut(&m.path) {
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
            if let Some(p) = path {
                if let Some(s) = entries.get_mut(&p) {
                    // Just log to stderr; don't disturb the display.
                    eprintln!("[warn] {}: {message}", s.basename);
                }
            } else {
                eprintln!("[warn] {message}");
            }
        }
        PipelineEvent::PipelineComplete => {}
    }
}

//! HUD telemetry: aggregate per-tool progress / error state.

use crate::renderer::{HUD_BAR_WIDTH, SCAN_CASCADE};
use form3::ansi::{fg, AnsiColor, Color};
use gls_core::event::{ToolProgress, ToolState};
use std::collections::HashMap;
use std::time::Duration;

/// Aggregate telemetry for all underlying tools, shown in the HUD.
#[derive(Debug, Clone, Default)]
pub struct Telemetry {
    tools: HashMap<String, ToolProgress>,
}

impl Telemetry {
    /// Update state for a tool from a pipeline event.
    pub fn update(&mut self, progress: ToolProgress) {
        self.tools.insert(progress.tool.clone(), progress);
    }

    /// True while any tool is still running, i.e. the HUD has live bars.
    pub fn any_running(&self) -> bool {
        self.tools.values().any(|p| p.state == ToolState::Running)
    }

    /// Render a compact HUD line. Running tools show a progress scan bar
    /// whose sweep is taken from `elapsed` wall-clock animation time,
    /// cascading left-to-right across concurrently running tools.
    pub fn render(&self, color: bool, elapsed: Duration) -> String {
        let order = [
            ("discover", "disc"),
            ("type", "type"),
            ("bart", "bart"),
            ("index", "idx"),
            ("context", "ctx"),
            ("padagonia", "pad"),
        ];
        let mut parts = Vec::new();
        let mut running_slot = 0;
        for (full, short) in order {
            if let Some(p) = self.tools.get(full) {
                let phase = SCAN_CASCADE.phase(elapsed, running_slot);
                if p.state == ToolState::Running {
                    running_slot += 1;
                }
                parts.push(render_tool(short, p, color, phase));
            } else {
                parts.push(render_tool(
                    short,
                    &ToolProgress {
                        tool: full.to_string(),
                        state: ToolState::Idle,
                        current: 0,
                        total: 0,
                        message: None,
                    },
                    color,
                    0.0,
                ));
            }
        }
        if let Some(p) = self.tools.get("warn") {
            parts.push(render_tool("warn", p, color, 0.0));
        }
        parts.join(" ")
    }
}

fn render_tool(name: &str, p: &ToolProgress, color: bool, phase: f32) -> String {
    if p.state == ToolState::Running {
        return render_running_tool(name, p, color, phase);
    }
    let icon = state_icon(p.state);
    let progress = if p.total > 0 {
        format!(" {}/{}", p.current, p.total)
    } else {
        String::new()
    };
    let msg = p
        .message
        .as_ref()
        .map(|m| {
            let truncated = if m.chars().count() > 12 {
                format!("{}…", m.chars().take(11).collect::<String>())
            } else {
                m.clone()
            };
            format!(" {truncated}")
        })
        .unwrap_or_default();
    let body = format!("{icon} {name}{progress}{msg}");
    if !color {
        return body;
    }
    let colour = state_colour(p.state);
    format!("{colour}{body}{}", crate::format::code_reset())
}

/// A running tool: `<progress scan bar> name n/total`.
fn render_running_tool(name: &str, p: &ToolProgress, color: bool, phase: f32) -> String {
    let colour = color.then(|| state_ansi_colour(p.state)).flatten();
    let bar = form3::scan::progress_scan_bar(
        p.current,
        p.total,
        true,
        phase,
        HUD_BAR_WIDTH,
        colour.as_ref(),
    );
    let progress = if p.total > 0 {
        format!(" {}/{}", p.current, p.total)
    } else if p.current > 0 {
        format!(" {}", p.current)
    } else {
        String::new()
    };
    let label = format!("{name}{progress}");
    match colour {
        Some(c) => format!("{bar} {}{label}{}", fg(&c), crate::format::code_reset()),
        None => format!("{bar} {label}"),
    }
}

fn state_icon(state: ToolState) -> &'static str {
    match state {
        ToolState::Idle => "○",
        ToolState::Running => "◐",
        ToolState::Completed => "✓",
        ToolState::Failed => "✗",
    }
}

fn state_ansi_colour(state: ToolState) -> Option<AnsiColor> {
    let c = match state {
        ToolState::Idle => return None,
        ToolState::Running => Color::Yellow,
        ToolState::Completed => Color::Green,
        ToolState::Failed => Color::Red,
    };
    Some(AnsiColor::Standard(c, false))
}

fn state_colour(state: ToolState) -> String {
    state_ansi_colour(state).map(|c| fg(&c)).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn progress(tool: &str, state: ToolState, current: u64, total: u64) -> ToolProgress {
        ToolProgress {
            tool: tool.to_string(),
            state,
            current,
            total,
            message: None,
        }
    }

    #[test]
    fn running_tool_shows_progress_bar_instead_of_glyph() {
        let mut t = Telemetry::default();
        t.update(progress("type", ToolState::Running, 2, 4));
        let hud = t.render(false, Duration::ZERO);
        assert!(hud.contains("████"), "half-filled bar expected: {hud}");
        assert!(hud.contains("type 2/4"));
        assert!(!hud.contains('◐'));
        assert!(t.any_running());
    }

    #[test]
    fn completed_tool_keeps_compact_check() {
        let mut t = Telemetry::default();
        t.update(progress("type", ToolState::Completed, 4, 4));
        assert!(t.render(false, Duration::ZERO).contains("✓ type 4/4"));
        assert!(!t.any_running());
    }

    #[test]
    fn running_bars_animate_with_time() {
        let mut t = Telemetry::default();
        t.update(progress("bart", ToolState::Running, 1, 4));
        assert_ne!(
            t.render(false, Duration::from_millis(100)),
            t.render(false, Duration::from_millis(600))
        );
    }
}

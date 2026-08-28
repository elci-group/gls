//! HUD telemetry: aggregate per-tool progress / error state.

use gls_core::event::{ToolProgress, ToolState};
use std::collections::HashMap;

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

    /// Render a compact HUD line.
    pub fn render(&self, color: bool) -> String {
        let order = [
            ("discover", "disc"),
            ("type", "type"),
            ("bart", "bart"),
            ("index", "idx"),
            ("context", "ctx"),
            ("padagonia", "pad"),
        ];
        let mut parts = Vec::new();
        for (full, short) in order {
            if let Some(p) = self.tools.get(full) {
                parts.push(render_tool(short, p, color));
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
                ));
            }
        }
        if let Some(p) = self.tools.get("warn") {
            parts.push(render_tool("warn", p, color));
        }
        parts.join(" ")
    }

}

fn render_tool(name: &str, p: &ToolProgress, color: bool) -> String {
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
            let truncated = if m.chars().count() > 20 {
                format!("{}…", m.chars().take(19).collect::<String>())
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

fn state_icon(state: ToolState) -> &'static str {
    match state {
        ToolState::Idle => "○",
        ToolState::Running => "◐",
        ToolState::Completed => "✓",
        ToolState::Failed => "✗",
    }
}

fn state_colour(state: ToolState) -> String {
    use crate::format::{code_green, code_red, code_yellow};
    match state {
        ToolState::Idle => String::new(),
        ToolState::Running => code_yellow(),
        ToolState::Completed => code_green(),
        ToolState::Failed => code_red(),
    }
}

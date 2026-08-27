//! Human-readable formatting helpers.

use gls_core::event::{ContextAnnotation, MetricBundle, PhysicalMeasure};
use gls_core::semantic::Classification;

// ── ANSI colour helpers ───────────────────────────────────────────────────────
// Generated through `form3::ansi` rather than formatted here directly, so the
// escape bytes come from one shared, tested source.

use form3::ansi::{fg, reset, sgr, AnsiColor, Attr, Color};

fn code_reset() -> &'static str {
    reset()
}
fn code_dim() -> String {
    sgr(Attr::Dim)
}
fn code_bold() -> String {
    sgr(Attr::Bold)
}
fn code_cyan() -> String {
    fg(&AnsiColor::Standard(Color::Cyan, false))
}
fn code_yellow() -> String {
    fg(&AnsiColor::Standard(Color::Yellow, false))
}
fn code_green() -> String {
    fg(&AnsiColor::Standard(Color::Green, false))
}
fn code_magenta() -> String {
    fg(&AnsiColor::Standard(Color::Magenta, false))
}

/// Format a filename in bold.
pub fn bold(s: &str) -> String {
    format!("{}{s}{}", code_bold(), code_reset())
}

/// Format a string as dim (annotation text).
pub fn dim(s: &str) -> String {
    format!("{}{s}{}", code_dim(), code_reset())
}

/// Format a byte count to a human-readable string.
pub fn human_bytes(bytes: u64) -> String {
    const KB: u64 = 1_024;
    const MB: u64 = KB * 1_024;
    const GB: u64 = MB * 1_024;
    const TB: u64 = GB * 1_024;

    if bytes >= TB {
        format!("{:.2} TB", bytes as f64 / TB as f64)
    } else if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.1} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.1} KB", bytes as f64 / KB as f64)
    } else {
        format!("{bytes} B")
    }
}

/// One-line type + size annotation (coloured).
pub fn type_line(cls: &Classification, phys: Option<&PhysicalMeasure>) -> String {
    let type_label = cls.semantic_type.label();
    let size = phys
        .map(|p| {
            let bytes = p.tree_bytes.unwrap_or(p.logical_bytes);
            format!(
                " {}·{} {}{}{}",
                code_dim(),
                code_reset(),
                code_cyan(),
                human_bytes(bytes),
                code_reset()
            )
        })
        .unwrap_or_default();
    let lang = cls
        .language
        .as_deref()
        .map(|l| format!(" {}[{l}]{}", code_dim(), code_reset()))
        .unwrap_or_default();
    format!("{}{type_label}{}{lang}{size}", code_yellow(), code_reset())
}

/// One-line type + size annotation (plain, no ANSI).
pub fn type_line_plain(cls: &Classification, phys: Option<&PhysicalMeasure>) -> String {
    let type_label = cls.semantic_type.label();
    let size = phys
        .map(|p| {
            let bytes = p.tree_bytes.unwrap_or(p.logical_bytes);
            format!(" · {}", human_bytes(bytes))
        })
        .unwrap_or_default();
    format!("{type_label}{size}")
}

/// Compact type/language fragment, no size — for contexts (grid cells)
/// that already show size separately, so `type_line`'s embedded size
/// would just be redundant clutter.
pub fn type_fragment(cls: &Classification) -> String {
    let label = cls
        .language
        .as_deref()
        .unwrap_or_else(|| cls.semantic_type.label());
    format!("{}{label}{}", code_yellow(), code_reset())
}

/// `type_fragment`, plain (no ANSI).
pub fn type_fragment_plain(cls: &Classification) -> String {
    cls.language
        .as_deref()
        .unwrap_or_else(|| cls.semantic_type.label())
        .to_string()
}

/// One-line context summary (coloured), truncated to `max_chars`.
pub fn context_line(ann: &ContextAnnotation, max_chars: usize) -> String {
    format!(
        "{}\"{}\"{}",
        code_magenta(),
        truncate(&ann.summary, max_chars),
        code_reset()
    )
}

/// One-line context summary (plain, no ANSI), truncated to `max_chars`.
pub fn context_line_plain(ann: &ContextAnnotation, max_chars: usize) -> String {
    format!("\"{}\"", truncate(&ann.summary, max_chars))
}

/// Truncate to at most `max_chars` characters, appending an ellipsis when
/// truncation actually happened.
pub fn truncate(s: &str, max_chars: usize) -> String {
    if s.chars().count() <= max_chars {
        s.to_string()
    } else {
        let head: String = s.chars().take(max_chars.saturating_sub(1)).collect();
        format!("{head}\u{2026}")
    }
}

/// One-line metrics summary (coloured).
pub fn metrics_line(m: &MetricBundle) -> String {
    let (green, dim, reset) = (code_green(), code_dim(), code_reset());
    let mut parts = Vec::new();
    if let Some(ref c) = m.completeness {
        parts.push(format!("{green}completeness {:.0}%{reset}", c.score * 100.0));
    }
    if let Some(ref q) = m.quality {
        parts.push(format!("{green}quality {:.0}%{reset}", q.score * 100.0));
    }
    if let Some(ref a) = m.accuracy {
        parts.push(format!("{green}accuracy {:.0}%{reset}", a.score * 100.0));
    }
    if let Some(ref t) = m.target_market {
        parts.push(format!("{dim}→ {}{reset}", t.value));
    }
    parts.join(&format!(" {dim}·{reset} "))
}

/// One-line metrics summary (plain, no ANSI).
pub fn metrics_line_plain(m: &MetricBundle) -> String {
    let mut parts = Vec::new();
    if let Some(ref c) = m.completeness {
        parts.push(format!("completeness {:.0}%", c.score * 100.0));
    }
    if let Some(ref q) = m.quality {
        parts.push(format!("quality {:.0}%", q.score * 100.0));
    }
    if let Some(ref a) = m.accuracy {
        parts.push(format!("accuracy {:.0}%", a.score * 100.0));
    }
    if let Some(ref t) = m.target_market {
        parts.push(format!("→ {}", t.value));
    }
    parts.join(" · ")
}

/// Compact metrics fragment for tight spaces (grid cells): just the
/// numeric scores. `target_market` is a free-text label that can run to a
/// full sentence — fine on a full-width row, but it alone can blow a grid
/// column out past the point of fitting side-by-side with others, so grid
/// cells drop it (`metrics_line`'s callers still show it in full).
pub fn metrics_fragment(m: &MetricBundle) -> String {
    let (green, reset) = (code_green(), code_reset());
    let mut parts = Vec::new();
    if let Some(ref c) = m.completeness {
        parts.push(format!("{green}{:.0}%c{reset}", c.score * 100.0));
    }
    if let Some(ref q) = m.quality {
        parts.push(format!("{green}{:.0}%q{reset}", q.score * 100.0));
    }
    if let Some(ref a) = m.accuracy {
        parts.push(format!("{green}{:.0}%a{reset}", a.score * 100.0));
    }
    parts.join(" ")
}

/// `metrics_fragment`, plain (no ANSI).
pub fn metrics_fragment_plain(m: &MetricBundle) -> String {
    let mut parts = Vec::new();
    if let Some(ref c) = m.completeness {
        parts.push(format!("{:.0}%c", c.score * 100.0));
    }
    if let Some(ref q) = m.quality {
        parts.push(format!("{:.0}%q", q.score * 100.0));
    }
    if let Some(ref a) = m.accuracy {
        parts.push(format!("{:.0}%a", a.score * 100.0));
    }
    parts.join(" ")
}

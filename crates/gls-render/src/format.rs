//! Human-readable formatting helpers.

use gls_core::event::{ContextAnnotation, MetricBundle, PhysicalMeasure};
use gls_core::semantic::Classification;

// ── ANSI colour helpers ───────────────────────────────────────────────────────

const RESET: &str = "\x1b[0m";
const DIM: &str = "\x1b[2m";
const BOLD: &str = "\x1b[1m";
const CYAN: &str = "\x1b[36m";
const YELLOW: &str = "\x1b[33m";
const GREEN: &str = "\x1b[32m";
const MAGENTA: &str = "\x1b[35m";

/// Format a filename in bold.
pub fn bold(s: &str) -> String {
    format!("{BOLD}{s}{RESET}")
}

/// Format a string as dim (annotation text).
pub fn dim(s: &str) -> String {
    format!("{DIM}{s}{RESET}")
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
            format!(" {DIM}·{RESET} {CYAN}{}{RESET}", human_bytes(bytes))
        })
        .unwrap_or_default();
    let lang = cls
        .language
        .as_deref()
        .map(|l| format!(" {DIM}[{l}]{RESET}"))
        .unwrap_or_default();
    format!("{YELLOW}{type_label}{RESET}{lang}{size}")
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
    format!("{YELLOW}{label}{RESET}")
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
    format!("{MAGENTA}\"{}\"{RESET}", truncate(&ann.summary, max_chars))
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
    let mut parts = Vec::new();
    if let Some(ref c) = m.completeness {
        parts.push(format!(
            "{GREEN}completeness {:.0}%{RESET}",
            c.score * 100.0
        ));
    }
    if let Some(ref q) = m.quality {
        parts.push(format!("{GREEN}quality {:.0}%{RESET}", q.score * 100.0));
    }
    if let Some(ref a) = m.accuracy {
        parts.push(format!("{GREEN}accuracy {:.0}%{RESET}", a.score * 100.0));
    }
    if let Some(ref t) = m.target_market {
        parts.push(format!("{DIM}→ {}{RESET}", t.value));
    }
    parts.join(&format!(" {DIM}·{RESET} "))
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
    let mut parts = Vec::new();
    if let Some(ref c) = m.completeness {
        parts.push(format!("{GREEN}{:.0}%c{RESET}", c.score * 100.0));
    }
    if let Some(ref q) = m.quality {
        parts.push(format!("{GREEN}{:.0}%q{RESET}", q.score * 100.0));
    }
    if let Some(ref a) = m.accuracy {
        parts.push(format!("{GREEN}{:.0}%a{RESET}", a.score * 100.0));
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

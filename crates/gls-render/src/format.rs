//! Human-readable formatting helpers.

use gls_core::event::{ContextAnnotation, MetricBundle, PhysicalMeasure};
use gls_core::semantic::Classification;

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

/// One-line type + size annotation.
pub fn type_line(cls: &Classification, phys: Option<&PhysicalMeasure>) -> String {
    let type_label = cls.semantic_type.label();
    let size = phys.map(|p| {
        let bytes = p.tree_bytes.unwrap_or(p.logical_bytes);
        format!(" · {}", human_bytes(bytes))
    }).unwrap_or_default();
    format!("{type_label}{size}")
}

/// One-line context summary.
pub fn context_line(ann: &ContextAnnotation) -> String {
    format!("\"{}\"", ann.summary)
}

/// One-line metrics summary.
pub fn metrics_line(m: &MetricBundle) -> String {
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

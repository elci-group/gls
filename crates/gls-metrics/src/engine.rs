//! Metric aggregation and presentation helpers.

use gls_core::event::MetricBundle;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

/// Accumulates metric bundles across pipeline events and provides
/// per-object summaries.
#[derive(Debug, Default)]
pub struct MetricEngine {
    bundles: HashMap<PathBuf, MetricBundle>,
}

impl MetricEngine {
    pub fn new() -> Self {
        Self::default()
    }

    /// Ingest a metrics update from the pipeline.
    pub fn update(&mut self, bundle: MetricBundle) {
        self.bundles.insert(bundle.path.clone(), bundle);
    }

    /// Return a human-readable summary line for a path (if metrics are
    /// available).
    pub fn summary(&self, path: &PathBuf) -> Option<MetricSummary> {
        let b = self.bundles.get(path)?;
        Some(MetricSummary::from_bundle(b))
    }
}

/// Displayable metric summary for a single filesystem object.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricSummary {
    pub path: PathBuf,
    pub completeness: Option<f32>,
    pub quality: Option<f32>,
    pub accuracy: Option<f32>,
    pub target_market: Option<String>,
}

impl MetricSummary {
    fn from_bundle(b: &MetricBundle) -> Self {
        Self {
            path: b.path.clone(),
            completeness: b.completeness.as_ref().map(|m| m.score),
            quality: b.quality.as_ref().map(|m| m.score),
            accuracy: b.accuracy.as_ref().map(|m| m.score),
            target_market: b.target_market.as_ref().map(|l| l.value.clone()),
        }
    }

    /// One-line display string, e.g. `completeness 91% · quality 87%`.
    pub fn one_liner(&self) -> String {
        let mut parts = Vec::new();
        if let Some(v) = self.completeness {
            parts.push(format!("completeness {:.0}%", v * 100.0));
        }
        if let Some(v) = self.quality {
            parts.push(format!("quality {:.0}%", v * 100.0));
        }
        if let Some(v) = self.accuracy {
            parts.push(format!("accuracy {:.0}%", v * 100.0));
        }
        if let Some(ref m) = self.target_market {
            parts.push(format!("→ {m}"));
        }
        parts.join(" · ")
    }
}

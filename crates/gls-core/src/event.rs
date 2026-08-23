//! The canonical pipeline event stream.
//!
//! All pipeline phases emit [`PipelineEvent`] values.  The renderer and the
//! Padagonia persistence layer each consume an independent clone of the same
//! broadcast stream.

use crate::{
    entry::FsEntry,
    semantic::Classification,
};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Physical/storage measurements produced by the BART phase.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PhysicalMeasure {
    pub path: PathBuf,
    pub logical_bytes: u64,
    /// Bytes actually allocated on the device (may differ due to sparse files,
    /// block size rounding, compression, etc.).
    pub allocated_bytes: Option<u64>,
    /// For directories: total logical bytes under all descendants.
    pub tree_bytes: Option<u64>,
    /// Number of filesystem objects under a directory.
    pub object_count: Option<u64>,
    /// Approximate entropy of the file content (0.0 – 1.0).
    pub entropy: Option<f32>,
    /// Estimated fraction of the tree that could be reclaimed (e.g. caches,
    /// build outputs).
    pub reclaimable_fraction: Option<f32>,
}

/// Compact descriptor emitted by the INDEX phase.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Descriptor {
    pub path: PathBuf,
    /// SHA-256 of canonical descriptor bytes.
    pub descriptor_hash: String,
    /// SHA-256 of the source content (for cache-hit detection).
    pub source_hash: Option<String>,
    /// JSON object containing the information-dense summary.
    pub payload: serde_json::Value,
}

/// Rich contextual annotation produced by the CONTEXT phase.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContextAnnotation {
    pub path: PathBuf,
    pub summary: String,
    pub purpose: Option<String>,
    pub role: Option<String>,
    pub relationships: Vec<String>,
    pub provenance: crate::provenance::ContextProvenance,
}

/// Structured metric bundle produced by the METRICS phase.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricBundle {
    pub path: PathBuf,
    pub completeness: Option<crate::pipeline::Metric>,
    pub quality:      Option<crate::pipeline::Metric>,
    pub accuracy:     Option<crate::pipeline::Metric>,
    pub target_market: Option<crate::pipeline::LabelMetric>,
}

/// The single event type flowing through the pipeline broadcast channel.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PipelineEvent {
    /// A filesystem object was discovered.
    Discovery(FsEntry),
    /// A semantic type was assigned.
    TypeDetected {
        path: PathBuf,
        classification: Classification,
    },
    /// Physical measurements are available.
    Measured(PhysicalMeasure),
    /// A compact descriptor has been produced.
    Indexed(Descriptor),
    /// Contextualisation has begun for this path.
    ContextStarted { path: PathBuf },
    /// Contextualisation produced a result.
    ContextUpdated(ContextAnnotation),
    /// Metrics have been updated.
    MetricsUpdated(MetricBundle),
    /// The result has been persisted to Padagonia.
    Persisted { path: PathBuf, revision: String },
    /// A non-fatal warning from any phase.
    Warning { path: Option<PathBuf>, message: String },
    /// The pipeline has finished all phases for all objects.
    PipelineComplete,
}

impl PipelineEvent {
    /// Returns the path this event is about, if any.
    pub fn path(&self) -> Option<&PathBuf> {
        match self {
            PipelineEvent::Discovery(e)        => Some(&e.path),
            PipelineEvent::TypeDetected { path, .. }  => Some(path),
            PipelineEvent::Measured(m)         => Some(&m.path),
            PipelineEvent::Indexed(d)          => Some(&d.path),
            PipelineEvent::ContextStarted { path }    => Some(path),
            PipelineEvent::ContextUpdated(a)   => Some(&a.path),
            PipelineEvent::MetricsUpdated(m)   => Some(&m.path),
            PipelineEvent::Persisted { path, .. }     => Some(path),
            PipelineEvent::Warning { path, .. }       => path.as_ref(),
            PipelineEvent::PipelineComplete            => None,
        }
    }
}

//! The canonical pipeline event stream.
//!
//! All pipeline phases emit [`PipelineEvent`] values.  The renderer and the
//! Padagonia persistence layer each consume an independent clone of the same
//! broadcast stream.

use crate::{entry::FsEntry, semantic::Classification};
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
    /// Newline count for files that look like text (best-effort; exact for
    /// files under 2 MiB, sampled/estimated above that).
    pub line_count: Option<u64>,
    /// Estimated fraction of the tree that could be reclaimed (e.g. caches,
    /// build outputs).
    pub reclaimable_fraction: Option<f32>,
}

/// One directory node's aggregated size, produced by BART's recursive
/// bottom-up traversal of a directory tree.
///
/// Unlike [`PhysicalMeasure`] — one per top-level requested path, carried on
/// the pipeline broadcast stream — a `SizeAggregate` exists for *every*
/// directory BART walks through on the way to a requested path's leaves,
/// and is handed directly to Padagonia for persistence rather than
/// broadcast, so a large subtree (e.g. `node_modules`) can't overflow the
/// pipeline's bounded event channel.
///
/// `parent_path`/`depth` record this node's place in the real filesystem
/// hierarchy, so the flat table Padagonia stores these in can always be
/// walked back into a coherent tree.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SizeAggregate {
    pub path: PathBuf,
    /// The filesystem parent of `path` (`None` only at a filesystem root).
    pub parent_path: Option<PathBuf>,
    /// Depth below the root of this particular recursive walk (0 = the
    /// path BART was asked to measure).
    pub depth: u32,
    /// Total bytes of every file under this directory, at any depth.
    pub tree_bytes: u64,
    /// Number of filesystem objects under this directory (not counting
    /// itself), at any depth.
    pub object_count: u64,
    /// Fraction of `tree_bytes` estimated reclaimable (caches, build
    /// outputs found anywhere in this subtree).
    pub reclaimable_fraction: Option<f32>,
    /// True if this directory contains no subdirectories (i.e. it is the
    /// bottom of its branch — files only, or empty).
    pub is_leaf: bool,
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
    pub quality: Option<crate::pipeline::Metric>,
    pub accuracy: Option<crate::pipeline::Metric>,
    pub target_market: Option<crate::pipeline::LabelMetric>,
}

/// Lifecycle state of an underlying pipeline tool, shown in the HUD.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ToolState {
    Idle,
    Running,
    Completed,
    Failed,
}

/// Progress / status telemetry for one underlying tool, shown in the HUD.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolProgress {
    pub tool: String,
    pub state: ToolState,
    pub current: u64,
    pub total: u64,
    pub message: Option<String>,
}

/// The single event type flowing through the pipeline broadcast channel.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PipelineEvent {
    /// A filesystem object was discovered.
    Discovery(FsEntry),
    /// Padagonia's deterministic metadata cache decision for this path.
    CacheStatus { path: PathBuf, hit: bool },
    /// Marks following context/metric events as replayed rather than newly
    /// inferred, so persistence consumers do not create duplicate revisions.
    CacheHydrated { path: PathBuf },
    /// The preferred display order after metadata-based sorting is available.
    OrderUpdated { paths: Vec<PathBuf> },
    /// A semantic type was assigned.
    TypeDetected {
        path: PathBuf,
        classification: Classification,
    },
    /// Physical measurements are available.
    Measured(PhysicalMeasure),
    /// A compact descriptor has been produced.
    Indexed(Descriptor),
    /// This path has been placed in the CONTEXT phase's processing queue,
    /// emitted for every candidate up front before any of them start — so
    /// consumers can show queue position / ETA immediately rather than a
    /// generic "pending" state.
    ContextQueued {
        path: PathBuf,
        /// 0-indexed position in the queue.
        position: u32,
        /// Total number of candidates queued this run.
        total: u32,
        /// Which provider will process this queue (e.g. `"groq"`).
        provider: String,
    },
    /// Contextualisation has begun for this path.
    ContextStarted { path: PathBuf },
    /// Contextualisation produced a result.
    ContextUpdated(ContextAnnotation),
    /// Metrics have been updated.
    MetricsUpdated(MetricBundle),
    /// The result has been persisted to Padagonia.
    Persisted { path: PathBuf, revision: String },
    /// A non-fatal warning from any phase.
    Warning {
        path: Option<PathBuf>,
        message: String,
    },
    /// A context provider is unavailable for this run.
    /// Emitted at most once per run; disables contextualisation globally.
    ProviderUnavailable { provider: String, reason: String },
    /// Telemetry update for the HUD: progress / state / traced error of an
    /// underlying tool.
    ToolProgress(ToolProgress),
    /// The pipeline has finished all phases for all objects.
    PipelineComplete,
}

impl PipelineEvent {
    /// Returns the path this event is about, if any.
    pub fn path(&self) -> Option<&PathBuf> {
        match self {
            PipelineEvent::Discovery(e) => Some(&e.path),
            PipelineEvent::CacheStatus { path, .. } => Some(path),
            PipelineEvent::CacheHydrated { path } => Some(path),
            PipelineEvent::OrderUpdated { .. } => None,
            PipelineEvent::TypeDetected { path, .. } => Some(path),
            PipelineEvent::Measured(m) => Some(&m.path),
            PipelineEvent::Indexed(d) => Some(&d.path),
            PipelineEvent::ContextQueued { path, .. } => Some(path),
            PipelineEvent::ContextStarted { path } => Some(path),
            PipelineEvent::ContextUpdated(a) => Some(&a.path),
            PipelineEvent::MetricsUpdated(m) => Some(&m.path),
            PipelineEvent::Persisted { path, .. } => Some(path),
            PipelineEvent::Warning { path, .. } => path.as_ref(),
            PipelineEvent::ProviderUnavailable { .. } => None,
            PipelineEvent::ToolProgress(_) => None,
            PipelineEvent::PipelineComplete => None,
        }
    }
}

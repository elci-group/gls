//! Shared pipeline infrastructure: metric types, presentation state machine,
//! and the broadcast channel type alias.

use crate::semantic::Provenance;
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;

/// Channel capacity for the pipeline broadcast.
pub const CHANNEL_CAPACITY: usize = 1024;

/// Create a new pipeline broadcast channel pair.
pub fn channel() -> (
    broadcast::Sender<crate::event::PipelineEvent>,
    broadcast::Receiver<crate::event::PipelineEvent>,
) {
    broadcast::channel(CHANNEL_CAPACITY)
}

// ── Metric primitives ────────────────────────────────────────────────────────

/// A scalar metric in [0, 1] with provenance and optional evidence.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Metric {
    /// The scalar value, 0.0–1.0.
    pub score: f32,
    /// Confidence in the score, 0.0–1.0.
    pub confidence: f32,
    /// Epistemological provenance.
    pub provenance: Provenance,
    /// Human-readable evidence items.
    pub evidence: Vec<String>,
}

/// A labelled metric (e.g. target_market = "Rust developers").
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LabelMetric {
    pub value: String,
    pub confidence: f32,
    pub provenance: Provenance,
}

// ── Presentation state machine ───────────────────────────────────────────────

/// Independent presentation state for a single filesystem entry.
///
/// The renderer uses this to decide which information tier to display and
/// which animation frame to show.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PresentationState {
    /// Name only — entry was just discovered.
    Discovered,
    /// Semantic type has been assigned.
    Classified,
    /// Physical measurements available.
    Measured,
    /// Descriptor generated, awaiting context.
    Indexed,
    /// Contextualisation complete.
    Contextualised,
    /// Metrics computed and attached.
    Valuated,
    /// All pipeline phases complete; display cycling.
    Stable,
}

impl PresentationState {
    /// Advance to the next state.  Returns `Stable` once fully processed.
    pub fn advance(self) -> Self {
        match self {
            Self::Discovered     => Self::Classified,
            Self::Classified     => Self::Measured,
            Self::Measured       => Self::Indexed,
            Self::Indexed        => Self::Contextualised,
            Self::Contextualised => Self::Valuated,
            Self::Valuated       => Self::Stable,
            Self::Stable         => Self::Stable,
        }
    }
}

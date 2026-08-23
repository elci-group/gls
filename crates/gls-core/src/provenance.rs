//! Contextualisation provenance — tracks the model, provider, and content
//! fingerprints associated with any inferred value.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Immutable audit record attached to every inferred result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContextProvenance {
    /// Schema/algorithm version.
    pub context_version: String,
    /// Model identifier used for this inference.
    pub model: String,
    /// Provider identifier (e.g. "groq").
    pub provider: String,
    /// SHA-256 of the descriptor bytes passed to the model.
    pub descriptor_hash: String,
    /// SHA-256 of the source object bytes (if available).
    pub source_hash: Option<String>,
    /// UTC timestamp of the inference.
    pub timestamp: DateTime<Utc>,
    /// Overall confidence in the result, 0.0–1.0.
    pub confidence: f32,
}

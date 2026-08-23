//! BOUND phase: scope, depth, and inference-budget control.

use serde::{Deserialize, Serialize};

/// Controls the maximum semantic scope of a `gls` invocation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Bound {
    /// Maximum filesystem depth for discovery (0 = root only).
    pub depth: usize,
    /// Maximum bytes read for descriptor excerpt extraction.
    pub excerpt_bytes: usize,
    /// Maximum number of objects eligible for contextualisation.
    pub max_context_candidates: usize,
    /// Maximum bytes that may be sent to the inference provider per invocation.
    pub inference_budget_bytes: usize,
    /// If true, skip contextualisation and operate on deterministic data only.
    pub no_context: bool,
    /// Contextualise only objects whose source_hash has changed since the last
    /// Padagonia record.
    pub incremental: bool,
    /// Priority ordering for contextualisation candidates.
    pub priority: BoundPriority,
}

impl Default for Bound {
    fn default() -> Self {
        Self {
            depth: 1,
            excerpt_bytes: 8_192,
            max_context_candidates: 64,
            inference_budget_bytes: 256_000,
            no_context: false,
            incremental: true,
            priority: BoundPriority::default(),
        }
    }
}

/// Configures the ordering of contextualisation candidates.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BoundPriority {
    /// Prefer visible (shallow) objects.
    pub visible_first: bool,
    /// Prefer small, high-information objects.
    pub small_first: bool,
    /// Prefer directories over files.
    pub directories_first: bool,
}

impl Default for BoundPriority {
    fn default() -> Self {
        Self {
            visible_first: true,
            small_first: true,
            directories_first: true,
        }
    }
}

/// Selects and orders a set of descriptors for contextualisation, respecting
/// the configured budget.
pub fn select_candidates<T>(
    items: Vec<(T, u64)>, // (item, byte_weight)
    bound: &Bound,
) -> Vec<T> {
    let mut out = Vec::new();
    let mut budget = bound.inference_budget_bytes as u64;
    let mut count = 0;

    for (item, weight) in items {
        if count >= bound.max_context_candidates {
            break;
        }
        if weight > budget {
            continue;
        }
        budget -= weight;
        count += 1;
        out.push(item);
    }

    out
}

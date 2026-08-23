//! Per-entry display state, advancing as pipeline events arrive.

use gls_core::{
    event::{ContextAnnotation, MetricBundle, PhysicalMeasure},
    pipeline::PresentationState,
    semantic::Classification,
};
use std::path::PathBuf;

/// All accumulated display data for one filesystem entry.
#[derive(Debug, Clone)]
pub struct EntryDisplayState {
    pub path: PathBuf,
    pub basename: String,
    pub is_dir: bool,
    pub classification: Option<Classification>,
    pub physical: Option<PhysicalMeasure>,
    pub context: Option<ContextAnnotation>,
    pub metrics: Option<MetricBundle>,
    pub presentation: PresentationState,
    /// Which animation frame (dimension) is currently shown.
    pub frame_index: usize,
    /// True while a ContextStarted has been received but no result yet.
    pub context_pending: bool,
    /// True if contextualisation failed (provider error, etc.).
    /// The failure is shown silently in the sub-line; no stderr spam.
    pub context_failed: bool,
}

impl EntryDisplayState {
    pub fn new(path: PathBuf, basename: String, is_dir: bool) -> Self {
        Self {
            path,
            basename,
            is_dir,
            classification: None,
            physical: None,
            context: None,
            metrics: None,
            presentation: PresentationState::Discovered,
            frame_index: 0,
            context_pending: false,
            context_failed: false,
        }
    }

    /// Advance to the next animation frame independently of pipeline state.
    pub fn tick_frame(&mut self) {
        // Frame count: 1 (name-only) + one per available metadata dimension.
        // Compute the same way current_secondary does in the renderer.
        let mut dim_count = 0usize;
        if self.classification.is_some() || self.context_failed { dim_count += 1; }
        if self.metrics.is_some() { dim_count += 1; }
        if self.context.is_some() { dim_count += 1; }

        let total_frames = dim_count + 1; // +1 for the name-only frame
        if total_frames > 1 {
            self.frame_index = (self.frame_index + 1) % total_frames;
        }
    }
}

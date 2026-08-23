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
    /// True once a ContextStarted event has been received.
    pub context_pending: bool,
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
        }
    }

    /// Number of animated dimensions currently available.
    pub fn available_frames(&self) -> usize {
        let mut n = 1; // always at least the name
        if self.classification.is_some() { n += 1; }
        if self.physical.is_some()       { n += 1; }
        if self.context.is_some()        { n += 1; }
        if self.metrics.is_some()        { n += 1; }
        n
    }

    /// Advance to the next animation frame.
    pub fn tick_frame(&mut self) {
        let frames = self.available_frames();
        if frames > 0 {
            self.frame_index = (self.frame_index + 1) % frames;
        }
    }
}

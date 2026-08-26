//! Per-entry display state, advancing as pipeline events arrive.

use chrono::{DateTime, Utc};
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
    /// Logical byte count, known from DISCOVER (before BART runs).
    pub logical_bytes: u64,
    /// Last-modified time, known from DISCOVER.
    pub modified: Option<DateTime<Utc>>,
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
            logical_bytes: 0,
            modified: None,
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
        // The carousel always has one item per insight layer (type, metrics,
        // context), plus the name-only frame. Keeping this fixed means pending
        // loaders animate before any result arrives and completed items can be
        // swapped in without resetting the carousel.
        const TOTAL_FRAMES: usize = 4;
        self.frame_index = (self.frame_index + 1) % TOTAL_FRAMES;
    }

    /// Best-available byte size for this entry: BART's measured directory
    /// tree size for directories (the raw inode size is meaningless there),
    /// a file's measured logical size, or — before BART has run — the
    /// cheap size observed at DISCOVER.
    pub fn measured_bytes(&self) -> u64 {
        match &self.physical {
            Some(p) if self.is_dir => p.tree_bytes.unwrap_or(self.logical_bytes),
            Some(p) => p.logical_bytes,
            None => self.logical_bytes,
        }
    }
}

//! Shared pipeline infrastructure: metric types, presentation state machine,
//! and the broadcast channel type alias.

use crate::semantic::Provenance;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;

/// Lossless fan-out sender for pipeline events.
///
/// Tokio's bounded broadcast channel is intentionally lossy when a receiver
/// falls behind. That is useful for telemetry, but not for GLS: every event is
/// part of the user-visible result or the persistent record. Each subscriber
/// therefore gets its own unbounded queue. The pipeline itself is bounded by
/// discovery depth and contextualisation budgets.
#[derive(Clone, Default)]
pub struct PipelineSender {
    subscribers: Arc<Mutex<Vec<mpsc::UnboundedSender<crate::event::PipelineEvent>>>>,
}

/// One lossless subscription to the pipeline event stream.
pub struct PipelineReceiver {
    receiver: mpsc::UnboundedReceiver<crate::event::PipelineEvent>,
}

impl PipelineSender {
    /// Add an independent consumer to the event stream.
    pub fn subscribe(&self) -> PipelineReceiver {
        let (sender, receiver) = mpsc::unbounded_channel();
        self.subscribers
            .lock()
            .expect("pipeline subscriber lock poisoned")
            .push(sender);
        PipelineReceiver { receiver }
    }

    /// Deliver an event to every live subscriber, returning the number reached.
    pub fn send(&self, event: crate::event::PipelineEvent) -> usize {
        let mut subscribers = self
            .subscribers
            .lock()
            .expect("pipeline subscriber lock poisoned");
        let mut delivered = 0;
        subscribers.retain(|subscriber| {
            let live = subscriber.send(event.clone()).is_ok();
            delivered += usize::from(live);
            live
        });
        delivered
    }
}

impl PipelineReceiver {
    pub async fn recv(&mut self) -> Option<crate::event::PipelineEvent> {
        self.receiver.recv().await
    }

    pub fn try_recv(&mut self) -> Result<crate::event::PipelineEvent, mpsc::error::TryRecvError> {
        self.receiver.try_recv()
    }
}

/// Create a pipeline with its first subscriber already attached.
pub fn channel() -> (PipelineSender, PipelineReceiver) {
    let sender = PipelineSender::default();
    let receiver = sender.subscribe();
    (sender, receiver)
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
            Self::Discovered => Self::Classified,
            Self::Classified => Self::Measured,
            Self::Measured => Self::Indexed,
            Self::Indexed => Self::Contextualised,
            Self::Contextualised => Self::Valuated,
            Self::Valuated => Self::Stable,
            Self::Stable => Self::Stable,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::PipelineEvent;

    #[tokio::test]
    async fn fanout_does_not_drop_bursts_above_the_old_capacity() {
        let (sender, mut first) = channel();
        let mut second = sender.subscribe();
        for _ in 0..2_048 {
            assert_eq!(sender.send(PipelineEvent::PipelineComplete), 2);
        }
        drop(sender);

        let mut first_count = 0;
        while first.recv().await.is_some() {
            first_count += 1;
        }
        let mut second_count = 0;
        while second.recv().await.is_some() {
            second_count += 1;
        }
        assert_eq!(first_count, 2_048);
        assert_eq!(second_count, 2_048);
    }
}

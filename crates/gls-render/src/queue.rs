//! Tracks CONTEXT-phase queue position and a rolling latency estimate, so
//! entries still waiting their turn can show a real "position N, eta Ns"
//! instead of a generic spinner.
//!
//! TYPE and BART have no equivalent: they run as one concurrent batch over
//! every entry rather than a sequential per-item queue, so there's no
//! meaningful "position" to report there — only CONTEXT (one provider call
//! at a time) has this shape.

use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::time::Instant;

const LATENCY_WINDOW: usize = 10;
/// Used only until at least one real call has completed this run.
const DEFAULT_LATENCY_SECS: f64 = 2.0;

#[derive(Debug, Clone)]
pub struct QueueStatus {
    /// 1-indexed distance from "next up" (1 = processed immediately after
    /// whatever's in flight now).
    pub position: u32,
    pub total: u32,
    pub eta_secs: u64,
    pub provider: String,
}

#[derive(Default)]
pub struct QueueTracker {
    total: u32,
    started_count: u32,
    provider: Option<String>,
    positions: HashMap<PathBuf, u32>,
    start_times: HashMap<PathBuf, Instant>,
    latencies: VecDeque<f64>,
}

impl QueueTracker {
    /// Record a path's place in the queue, announced up front for the
    /// whole batch before any of them start.
    pub fn queued(&mut self, path: PathBuf, position: u32, total: u32, provider: String) {
        self.total = total;
        self.provider = Some(provider);
        self.positions.insert(path, position);
    }

    /// A path has started processing — advances the "next up" pointer and
    /// starts its latency clock.
    pub fn started(&mut self, path: &Path) {
        self.started_count += 1;
        self.start_times.insert(path.to_path_buf(), Instant::now());
    }

    /// A path finished (success or failure) — folds its elapsed time into
    /// the rolling latency estimate used for other entries' ETAs.
    pub fn completed(&mut self, path: &Path) {
        if let Some(start) = self.start_times.remove(path) {
            self.latencies.push_back(start.elapsed().as_secs_f64());
            if self.latencies.len() > LATENCY_WINDOW {
                self.latencies.pop_front();
            }
        }
    }

    fn avg_latency_secs(&self) -> f64 {
        if self.latencies.is_empty() {
            DEFAULT_LATENCY_SECS
        } else {
            self.latencies.iter().sum::<f64>() / self.latencies.len() as f64
        }
    }

    /// How long the item currently in flight (if any) has been running.
    /// Processing is strictly sequential, so at most one entry is ever
    /// mid-flight at a time.
    fn current_elapsed_secs(&self) -> f64 {
        self.start_times
            .values()
            .next()
            .map(|t| t.elapsed().as_secs_f64())
            .unwrap_or(0.0)
    }

    /// Status for a path still waiting its turn. `None` if this path was
    /// never queued, or has already started (it gets a different display
    /// once in flight — see `EntryDisplayState::context_pending`).
    ///
    /// `eta_secs` is re-derived from real elapsed time on every call, not
    /// just when a completion updates the latency average — otherwise it
    /// sits frozen at whatever it first read for however long the current
    /// call actually takes, which reads as stuck rather than live.
    pub fn status_for(&self, path: &Path) -> Option<QueueStatus> {
        let &position = self.positions.get(path)?;
        if position < self.started_count {
            return None;
        }
        let ahead = position - self.started_count;
        let avg = self.avg_latency_secs();
        // Time left on whatever's currently running, then one full average
        // turn for each item strictly ahead of this one, then this item's
        // own turn.
        let remaining_current = (avg - self.current_elapsed_secs()).max(0.0);
        let eta_secs = (remaining_current + ahead as f64 * avg + avg)
            .round()
            .max(0.0) as u64;
        Some(QueueStatus {
            position: ahead + 1,
            total: self.total,
            eta_secs,
            provider: self
                .provider
                .clone()
                .unwrap_or_else(|| "provider".to_string()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread::sleep;
    use std::time::Duration;

    #[test]
    fn eta_counts_down_in_real_time_without_a_new_completion() {
        let mut q = QueueTracker::default();
        let a = PathBuf::from("/a");
        let b = PathBuf::from("/b");
        q.queued(a.clone(), 0, 2, "groq".to_string());
        q.queued(b.clone(), 1, 2, "groq".to_string());
        q.started(&a); // a is now in flight; b is queued behind it.

        let first = q.status_for(&b).expect("b is still queued");
        // No `completed()` call in between — only wall-clock time passes.
        // If eta only ever updated on completion, this would read
        // identically to `first`, i.e. frozen for the whole call.
        sleep(Duration::from_millis(1100));
        let second = q.status_for(&b).expect("b is still queued");

        assert!(
            second.eta_secs < first.eta_secs,
            "eta should count down as the in-flight item's elapsed time grows \
             (first={}, second={})",
            first.eta_secs,
            second.eta_secs
        );
        // Position is unaffected by elapsed time — only completions move it.
        assert_eq!(second.position, first.position);
    }

    #[test]
    fn started_item_is_not_reported_as_queued() {
        let mut q = QueueTracker::default();
        let a = PathBuf::from("/a");
        q.queued(a.clone(), 0, 1, "groq".to_string());
        assert!(q.status_for(&a).is_some());
        q.started(&a);
        assert!(q.status_for(&a).is_none());
    }
}

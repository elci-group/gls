//! Persistent, TTL-based model-availability cache.
//!
//! Wraps [`crate::poll`] with Padagonia-backed persistence so the menu
//! doesn't hit the network on every invocation: a poll result is good for
//! [`DEFAULT_TTL_SECS`], after which the next request triggers a fresh
//! live poll. A failed live poll falls back to the last good cache entry
//! (marked stale) rather than leaving the user with nothing.

use crate::catalog::Provider;
use crate::poll::poll_models;
use chrono::{Duration, Utc};
use gls_padagonia::{CachedModel, Padagonia};
use reqwest::Client;
use std::time::Duration as StdDuration;

/// How long a polled model list is considered fresh before the next
/// request triggers a live re-poll.
pub const DEFAULT_TTL_SECS: i64 = 3600;

/// Where the returned model list came from.
#[derive(Debug)]
pub enum MenuSource {
    /// Fetched live, just now.
    Live,
    /// Served from a cache still within [`DEFAULT_TTL_SECS`].
    Cached,
    /// A live poll was attempted and failed; served from a stale cache
    /// instead. Carries the poll failure reason.
    StaleFallback(String),
    /// No cache exists and a live poll failed or isn't supported for this
    /// provider. Carries the reason; callers should fall back to manual
    /// model-id entry.
    Unavailable(String),
}

pub struct ModelMenu {
    pub models: Vec<CachedModel>,
    pub source: MenuSource,
}

/// A `reqwest::Client` configured with a sane timeout for interactive use —
/// a hung poll shouldn't hang the menu.
pub fn new_client() -> Client {
    Client::builder()
        .timeout(StdDuration::from_secs(10))
        .build()
        .unwrap_or_default()
}

/// Resolve the model list for `provider`: serve a fresh cache entry if one
/// exists, otherwise poll live and persist the result. `force_refresh`
/// bypasses the freshness check and always polls.
pub async fn resolve_models(
    padagonia: &Padagonia,
    client: &Client,
    provider: &Provider,
    force_refresh: bool,
) -> ModelMenu {
    let cached = padagonia.cached_models(provider.id);
    let is_fresh = cached
        .iter()
        .map(|c| c.fetched_at)
        .max()
        .map(|latest| Utc::now() - latest < Duration::seconds(DEFAULT_TTL_SECS))
        .unwrap_or(false);

    if !force_refresh && is_fresh {
        return ModelMenu {
            models: sorted(cached),
            source: MenuSource::Cached,
        };
    }

    match poll_models(client, provider).await {
        Ok(models) => {
            let pairs: Vec<(String, f32)> = models
                .iter()
                .map(|m| (m.id.clone(), m.efficiency_score))
                .collect();
            if let Err(e) = padagonia.store_models(provider.id, &pairs) {
                eprintln!("[warn] could not cache model list for {}: {e}", provider.id);
            }
            ModelMenu {
                models: sorted(padagonia.cached_models(provider.id)),
                source: MenuSource::Live,
            }
        }
        Err(e) => {
            if cached.is_empty() {
                ModelMenu {
                    models: Vec::new(),
                    source: MenuSource::Unavailable(e.to_string()),
                }
            } else {
                ModelMenu {
                    models: sorted(cached),
                    source: MenuSource::StaleFallback(e.to_string()),
                }
            }
        }
    }
}

/// Sort by descending efficiency heuristic, alphabetical within ties.
fn sorted(mut models: Vec<CachedModel>) -> Vec<CachedModel> {
    models.sort_by(|a, b| {
        b.efficiency_score
            .partial_cmp(&a.efficiency_score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.id.cmp(&b.id))
    });
    models
}

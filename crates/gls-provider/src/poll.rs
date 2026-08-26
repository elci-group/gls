//! Live model-availability polling.
//!
//! One real HTTP call per poll, against the provider's actual model-listing
//! endpoint — no mock data. Callers (see [`crate::cache`]) are responsible
//! for deciding when a poll is warranted versus serving the persistent
//! cache.

use crate::catalog::{Provider, ProviderKind};
use reqwest::{Client, RequestBuilder};
use serde::Deserialize;
use std::fmt;

/// One model, as reported by a provider's listing API, with a heuristic
/// efficiency score attached.
#[derive(Debug, Clone)]
pub struct ModelInfo {
    pub id: String,
    pub efficiency_score: f32,
}

#[derive(Debug)]
pub enum PollError {
    /// This provider's `env_var` is not set.
    NoApiKey,
    /// The request could not be completed (DNS, TLS, timeout, …).
    Network(String),
    /// The provider responded with a non-2xx status.
    Http(u16, String),
    /// This provider's [`ProviderKind`] is [`ProviderKind::Unsupported`].
    Unsupported(&'static str),
}

impl fmt::Display for PollError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PollError::NoApiKey => write!(f, "no API key configured"),
            PollError::Network(e) => write!(f, "network error: {e}"),
            PollError::Http(status, body) => {
                let snippet: String = body.chars().take(200).collect();
                write!(f, "HTTP {status}: {snippet}")
            }
            PollError::Unsupported(reason) => write!(f, "live listing not supported ({reason})"),
        }
    }
}

/// Poll `provider`'s real model-listing endpoint. Returns every model it
/// reports, each carrying a heuristic [`efficiency_score`].
pub async fn poll_models(
    client: &Client,
    provider: &Provider,
) -> Result<Vec<ModelInfo>, PollError> {
    match provider.kind {
        ProviderKind::Unsupported { reason } => Err(PollError::Unsupported(reason)),
        ProviderKind::OpenAiCompatible { base } => {
            let key = api_key(provider)?;
            let url = format!("{base}/v1/models");
            let list: OpenAiModelList = get_json(client, &url, |r| r.bearer_auth(&key)).await?;
            Ok(list.data.into_iter().map(|m| model_info(m.id)).collect())
        }
        ProviderKind::Anthropic { base } => {
            let key = api_key(provider)?;
            let url = format!("{base}/v1/models");
            let list: OpenAiModelList = get_json(client, &url, |r| {
                r.header("x-api-key", &key)
                    .header("anthropic-version", "2023-06-01")
            })
            .await?;
            Ok(list.data.into_iter().map(|m| model_info(m.id)).collect())
        }
        ProviderKind::Cohere { base } => {
            let key = api_key(provider)?;
            let url = format!("{base}/v1/models");
            let list: CohereModelList = get_json(client, &url, |r| r.bearer_auth(&key)).await?;
            Ok(list
                .models
                .into_iter()
                .map(|m| model_info(m.name))
                .collect())
        }
    }
}

fn api_key(provider: &Provider) -> Result<String, PollError> {
    std::env::var(provider.env_var).map_err(|_| PollError::NoApiKey)
}

async fn get_json<T: serde::de::DeserializeOwned>(
    client: &Client,
    url: &str,
    apply_auth: impl FnOnce(RequestBuilder) -> RequestBuilder,
) -> Result<T, PollError> {
    let resp = apply_auth(client.get(url))
        .send()
        .await
        .map_err(|e| PollError::Network(e.to_string()))?;

    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        return Err(PollError::Http(status.as_u16(), body));
    }
    resp.json::<T>()
        .await
        .map_err(|e| PollError::Network(e.to_string()))
}

fn model_info(id: String) -> ModelInfo {
    let efficiency_score = efficiency_score(&id);
    ModelInfo {
        id,
        efficiency_score,
    }
}

/// Heuristic 0.0–1.0 "inference-efficient" score derived from naming
/// conventions in the model id.
///
/// This is **not** measured latency or cost — no provider's model-listing
/// endpoint reports that. It's a best-effort proxy so that models whose
/// names signal a smaller/faster/cheaper variant sort ahead of flagship
/// ones, which is usually what "efficient" means in practice.
pub fn efficiency_score(model_id: &str) -> f32 {
    let id = model_id.to_lowercase();

    const FAST_MARKERS: &[&str] = &[
        "mini", "flash", "turbo", "instant", "lite", "small", "fast", "nano", "haiku", "distill",
        "8b", "7b", "3b", "1b", "2b", "4b",
    ];
    const SLOW_MARKERS: &[&str] = &[
        "ultra", "opus", "large", "xl", "405b", "70b", "72b", "671b", "max", "preview",
    ];

    let mut score: f32 = 0.5;
    for marker in FAST_MARKERS {
        if id.contains(marker) {
            score += 0.12;
        }
    }
    for marker in SLOW_MARKERS {
        if id.contains(marker) {
            score -= 0.12;
        }
    }
    score.clamp(0.0, 1.0)
}

// ── Wire shapes ──────────────────────────────────────────────────────────────

/// Shared by every `OpenAiCompatible` provider and by Anthropic, which
/// happens to use the same `{"data": [{"id": ...}]}` envelope.
#[derive(Debug, Deserialize)]
struct OpenAiModelList {
    data: Vec<OpenAiModel>,
}

#[derive(Debug, Deserialize)]
struct OpenAiModel {
    id: String,
}

#[derive(Debug, Deserialize)]
struct CohereModelList {
    models: Vec<CohereModel>,
}

#[derive(Debug, Deserialize)]
struct CohereModel {
    name: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fast_markers_score_higher_than_slow_markers() {
        assert!(efficiency_score("llama-3.1-8b-instant") > efficiency_score("llama-3.1-405b"));
        assert!(efficiency_score("claude-haiku-4.5") > efficiency_score("claude-opus-4.5"));
        assert!(efficiency_score("gpt-4.1-mini") > efficiency_score("gpt-4.1"));
    }

    #[test]
    fn score_is_clamped() {
        assert!(efficiency_score("mini-flash-turbo-instant-8b-nano") <= 1.0);
        assert!(efficiency_score("ultra-opus-large-xl-405b-max") >= 0.0);
    }
}

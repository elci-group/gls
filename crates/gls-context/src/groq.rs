//! Groq inference client for contextualisation.
//!
//! # Model selection
//!
//! The active model is resolved by `active_model()` on first use:
//!   1. Try the model from `GLS_GROQ_MODEL` env var / `--model` flag /
//!      `DEFAULT_MODEL` constant with a real 1-token probe call.
//!   2. On 404 (no access) try each entry in `FALLBACK_MODELS` in order.
//!   3. On 401/403 (bad key) fail immediately — no point trying fallbacks.
//!   4. If all candidates fail, emit `ProviderUnavailable` once and skip
//!      contextualisation for this run entirely.
//!
//! The resolved model is cached; the probe runs at most once per invocation.

use gls_core::{
    event::{ContextAnnotation, Descriptor, MetricBundle, PipelineEvent},
    pipeline::{LabelMetric, Metric, PipelineSender},
    provenance::ContextProvenance,
    semantic::Provenance,
};
use reqwest::Client;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tokio::sync::OnceCell;

const GROQ_API_URL: &str = "https://api.groq.com/openai/v1/chat/completions";
const CONTEXT_VERSION: &str = "1.0";
const PROVIDER: &str = "groq";

/// Current recommended fast Groq model.
/// Update this when Groq deprecates models.
const DEFAULT_MODEL: &str = "llama-3.1-8b-instant";

/// Ordered fallback preference list.  The first model that the API reports as
/// available will be used.
const FALLBACK_MODELS: &[&str] = &[
    "llama-3.1-8b-instant",
    "llama-3.3-70b-versatile",
    "llama3-70b-8192",
    "gemma2-9b-it",
    "mixtral-8x7b-32768",
];

// ── Configuration ─────────────────────────────────────────────────────────────

/// Configuration for the Groq contextualiser.
#[derive(Debug, Clone)]
pub struct ContextConfig {
    /// Groq API key (read from `GROQ_API_KEY` env var if not provided).
    pub api_key: Option<String>,
    /// Requested model identifier.  Will be resolved/validated at first use.
    pub model: String,
    /// Maximum tokens to request per inference call.
    pub max_tokens: u32,
}

impl Default for ContextConfig {
    fn default() -> Self {
        let model = std::env::var("GLS_GROQ_MODEL").unwrap_or_else(|_| DEFAULT_MODEL.to_string());
        Self {
            api_key: std::env::var("GROQ_API_KEY").ok(),
            model,
            max_tokens: 512,
        }
    }
}

// ── Contextualiser ────────────────────────────────────────────────────────────

/// Performs contextualisation against the Groq API.
pub struct Contextualiser {
    client: Client,
    config: ContextConfig,
    /// Resolved model, populated lazily on first successful probe.
    resolved_model: Arc<OnceCell<String>>,
}

impl Contextualiser {
    pub fn new(config: ContextConfig) -> Self {
        Self {
            client: Client::new(),
            config,
            resolved_model: Arc::new(OnceCell::new()),
        }
    }

    /// Pre-flight check: resolve the model and verify the provider is reachable.
    ///
    /// Returns `Ok(model_name)` on success, or `Err(reason)` if the provider
    /// should be considered unavailable for this run.
    pub async fn preflight(&self) -> Result<String, String> {
        self.active_model().await
    }

    /// Contextualise a batch of descriptors.
    ///
    /// Calls `preflight()` first.  If the provider is unavailable, emits a
    /// single `ProviderUnavailable` event and returns without processing any
    /// objects.  Individual per-object failures emit `Warning` events.
    pub async fn contextualise(&self, descriptors: Vec<Descriptor>, tx: PipelineSender) {
        // ── Pre-flight ────────────────────────────────────────────────────────
        if let Err(reason) = self.preflight().await {
            let _ = tx.send(PipelineEvent::ProviderUnavailable {
                provider: PROVIDER.to_string(),
                reason,
            });
            return;
        }

        // ── Announce the whole queue up front ───────────────────────────────────
        // Emitted before any processing starts so consumers can show queue
        // position / ETA immediately, rather than a generic "pending" state
        // for however long it takes this sequential loop to reach them.
        let total = descriptors.len() as u32;
        for (i, desc) in descriptors.iter().enumerate() {
            let _ = tx.send(PipelineEvent::ContextQueued {
                path: desc.path.clone(),
                position: i as u32,
                total,
                provider: PROVIDER.to_string(),
            });
        }

        // ── Per-object contextualisation ──────────────────────────────────────
        for desc in descriptors {
            let path = desc.path.clone();
            let _ = tx.send(PipelineEvent::ContextStarted { path: path.clone() });

            match self.contextualise_one(&desc).await {
                Ok((annotation, metrics)) => {
                    let _ = tx.send(PipelineEvent::ContextUpdated(annotation));
                    let _ = tx.send(PipelineEvent::MetricsUpdated(metrics));
                }
                Err(e) => {
                    let _ = tx.send(PipelineEvent::Warning {
                        path: Some(path),
                        message: format!("context error: {e}"),
                    });
                }
            }
        }
    }

    /// Resolve the actual model to use.
    ///
    /// Strategy:
    ///   1. Try the configured model with a real 1-token probe call.
    ///   2. On 401/403 → API key is bad → return Err immediately (no point
    ///      trying fallbacks).
    ///   3. On 404/model_not_found → try each FALLBACK_MODELS entry in order.
    ///   4. If all fallbacks fail → return Err so caller emits ProviderUnavailable.
    ///
    /// Result cached in `resolved_model`; probe runs at most once per instance.
    async fn active_model(&self) -> Result<String, String> {
        if let Some(m) = self.resolved_model.get() {
            return Ok(m.clone());
        }

        let api_key = self
            .config
            .api_key
            .clone()
            .ok_or_else(|| "GROQ_API_KEY not set".to_string())?;

        // Build the full candidate list: configured model first, then fallbacks.
        let mut candidates: Vec<&str> = vec![self.config.model.as_str()];
        for &f in FALLBACK_MODELS {
            if f != self.config.model.as_str() {
                candidates.push(f);
            }
        }

        for candidate in candidates {
            match self.probe_access(&api_key, candidate).await {
                ProbeResult::Ok => {
                    let _ = self.resolved_model.set(candidate.to_string());
                    return Ok(candidate.to_string());
                }
                ProbeResult::NoAccess => {
                    // This model not available to this key; try next.
                    continue;
                }
                ProbeResult::AuthFailed(reason) => {
                    // Bad key — all models will fail.
                    return Err(format!("authentication failed: {reason}"));
                }
                ProbeResult::NetworkError(e) => {
                    // Can't reach Groq at all.
                    return Err(format!("network error: {e}"));
                }
            }
        }

        Err("no accessible Groq model found for this API key".to_string())
    }

    /// Make a minimal 1-token completion call to confirm this key can access
    /// `model`.  Returns a `ProbeResult` rather than a `Result` so the caller
    /// can distinguish auth failures from access failures.
    async fn probe_access(&self, api_key: &str, model: &str) -> ProbeResult {
        let body = serde_json::json!({
            "model": model,
            "max_tokens": 1,
            "messages": [{ "role": "user", "content": "hi" }]
        });

        let resp = match self
            .client
            .post(GROQ_API_URL)
            .bearer_auth(api_key)
            .json(&body)
            .send()
            .await
        {
            Ok(r) => r,
            Err(e) => return ProbeResult::NetworkError(e.to_string()),
        };

        match resp.status().as_u16() {
            200 => ProbeResult::Ok,
            401 | 403 => {
                let text = resp.text().await.unwrap_or_default();
                ProbeResult::AuthFailed(text)
            }
            404 => ProbeResult::NoAccess,
            _ => {
                // Unexpected status — treat as NoAccess so we try the next model.
                ProbeResult::NoAccess
            }
        }
    }

    async fn contextualise_one(
        &self,
        desc: &Descriptor,
    ) -> Result<(ContextAnnotation, MetricBundle), String> {
        let api_key = self
            .config
            .api_key
            .clone()
            .ok_or_else(|| "GROQ_API_KEY not set".to_string())?;

        let model = self.active_model().await?;

        let prompt = build_prompt(desc);
        let descriptor_bytes = serde_json::to_vec(&desc.payload).unwrap_or_default();
        let descriptor_hash = hex::encode(Sha256::digest(&descriptor_bytes));

        let request_body = serde_json::json!({
            "model": model,
            "max_tokens": self.config.max_tokens,
            "messages": [
                { "role": "system", "content": SYSTEM_PROMPT },
                { "role": "user",   "content": prompt }
            ],
            "response_format": { "type": "json_object" }
        });

        let resp = self
            .client
            .post(GROQ_API_URL)
            .bearer_auth(&api_key)
            .json(&request_body)
            .send()
            .await
            .map_err(|e| e.to_string())?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(format!("Groq API {status}: {body}"));
        }

        let groq_resp: GroqResponse = resp.json().await.map_err(|e| e.to_string())?;
        let content = groq_resp
            .choices
            .into_iter()
            .next()
            .map(|c| c.message.content)
            .unwrap_or_default();

        let parsed: serde_json::Value =
            serde_json::from_str(&content).map_err(|e| e.to_string())?;

        let now = chrono::Utc::now();
        let provenance = ContextProvenance {
            context_version: CONTEXT_VERSION.to_string(),
            model: model.clone(),
            provider: PROVIDER.to_string(),
            descriptor_hash,
            source_hash: desc.source_hash.clone(),
            timestamp: now,
            confidence: parsed["confidence"].as_f64().unwrap_or(0.5) as f32,
        };

        let annotation = ContextAnnotation {
            path: desc.path.clone(),
            summary: parsed["summary"].as_str().unwrap_or("").to_string(),
            purpose: parsed["purpose"].as_str().map(str::to_string),
            role: parsed["role"].as_str().map(str::to_string),
            relationships: parsed["relationships"]
                .as_array()
                .map(|a| {
                    a.iter()
                        .filter_map(|v| v.as_str())
                        .map(str::to_string)
                        .collect()
                })
                .unwrap_or_default(),
            provenance,
        };

        let metrics = extract_metrics(&desc.path, &parsed);
        Ok((annotation, metrics))
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn build_prompt(desc: &Descriptor) -> String {
    let payload = serde_json::to_string_pretty(&desc.payload).unwrap_or_default();
    format!(
        "Analyse the following filesystem object descriptor and return a JSON object.\n\n\
         Descriptor:\n```json\n{payload}\n```"
    )
}

fn extract_metrics(path: &std::path::Path, parsed: &serde_json::Value) -> MetricBundle {
    let scalar = |key: &str| -> Option<Metric> {
        let obj = parsed.get(key)?;
        Some(Metric {
            score: obj["score"].as_f64()? as f32,
            confidence: obj["confidence"].as_f64().unwrap_or(0.5) as f32,
            provenance: Provenance::Inferred,
            evidence: obj["evidence"]
                .as_array()
                .map(|a| {
                    a.iter()
                        .filter_map(|v| v.as_str())
                        .map(str::to_string)
                        .collect()
                })
                .unwrap_or_default(),
        })
    };

    let label = |key: &str| -> Option<LabelMetric> {
        let obj = parsed.get(key)?;
        Some(LabelMetric {
            value: obj["value"].as_str()?.to_string(),
            confidence: obj["confidence"].as_f64().unwrap_or(0.5) as f32,
            provenance: Provenance::Inferred,
        })
    };

    MetricBundle {
        path: path.to_path_buf(),
        completeness: scalar("completeness"),
        quality: scalar("quality"),
        accuracy: scalar("accuracy"),
        target_market: label("target_market"),
    }
}

static SYSTEM_PROMPT: &str = r#"
You are a filesystem semantic analyser. Given a descriptor of a filesystem object,
return a JSON object with the following fields (all optional except summary):

{
  "summary":      "One-sentence description of what this object is",
  "purpose":      "Why this object exists in the project",
  "role":         "Structural role (e.g. entrypoint, configuration, test, documentation)",
  "relationships": ["...related objects or concerns..."],
  "confidence":   0.0-1.0,
  "completeness": { "score": 0-1, "confidence": 0-1, "evidence": [...] },
  "quality":      { "score": 0-1, "confidence": 0-1, "evidence": [...] },
  "accuracy":     { "score": 0-1, "confidence": 0-1, "evidence": [...] },
  "target_market":{ "value": "...", "confidence": 0-1 }
}

Important:
- Never invent file contents. Only reason from the descriptor provided.
- Always include a confidence value.
- Mark uncertain values with lower confidence.
- Return valid JSON only.
"#;

// ── Internal types ────────────────────────────────────────────────────────────

/// Result of a single-model access probe.
enum ProbeResult {
    /// The model responded successfully.
    Ok,
    /// The key has no access to this model (404 / model_not_found).
    NoAccess,
    /// Authentication failed entirely (401 / 403) — no point trying fallbacks.
    AuthFailed(String),
    /// Network-level error — cannot reach Groq.
    NetworkError(String),
}

// ── Groq wire types ───────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct GroqResponse {
    choices: Vec<GroqChoice>,
}

#[derive(Debug, Deserialize)]
struct GroqChoice {
    message: GroqMessage,
}

#[derive(Debug, Deserialize)]
struct GroqMessage {
    content: String,
}

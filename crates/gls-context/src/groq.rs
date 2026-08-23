//! Groq inference client for contextualisation.
//!
//! # Model selection
//!
//! The active model is resolved at construction time:
//!   1. `GLS_GROQ_MODEL` env var
//!   2. `--model` CLI flag passed through `ContextConfig::model`
//!   3. `DEFAULT_MODEL` constant (current, non-deprecated model)
//!
//! If the resolved model is decommissioned, `resolve_model()` probes the
//! Groq models endpoint and silently picks the first available fast model.
//! Provider failures always degrade gracefully — never panic, never spam.

use gls_core::{
    event::{ContextAnnotation, Descriptor, MetricBundle, PipelineEvent},
    pipeline::{LabelMetric, Metric},
    provenance::ContextProvenance,
    semantic::Provenance,
};
use reqwest::Client;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{broadcast, OnceCell};

const GROQ_API_URL: &str = "https://api.groq.com/openai/v1/chat/completions";
const GROQ_MODELS_URL: &str = "https://api.groq.com/openai/v1/models";
const CONTEXT_VERSION: &str = "1.0";

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
        let model = std::env::var("GLS_GROQ_MODEL")
            .unwrap_or_else(|_| DEFAULT_MODEL.to_string());
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

    /// Contextualise a batch of descriptors, emitting `ContextStarted`,
    /// `ContextUpdated`, and `MetricsUpdated` events for each.
    pub async fn contextualise(
        &self,
        descriptors: Vec<Descriptor>,
        tx: broadcast::Sender<PipelineEvent>,
    ) {
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

    /// Resolve the actual model to use, probing Groq if needed.
    ///
    /// The result is cached in `resolved_model` so the probe only runs once
    /// per `Contextualiser` instance.
    async fn active_model(&self) -> Result<String, String> {
        if let Some(m) = self.resolved_model.get() {
            return Ok(m.clone());
        }

        let api_key = self
            .config
            .api_key
            .clone()
            .ok_or_else(|| "GROQ_API_KEY not set".to_string())?;

        let resolved = self
            .probe_model(&api_key, &self.config.model)
            .await
            .unwrap_or_else(|_| self.config.model.clone());

        // Store; if another task raced us, just use whatever was stored.
        let _ = self.resolved_model.set(resolved.clone());
        Ok(self.resolved_model.get().cloned().unwrap_or(resolved))
    }

    /// Check whether `preferred` is available; if not, walk `FALLBACK_MODELS`
    /// and return the first one the API reports as active.
    async fn probe_model(&self, api_key: &str, preferred: &str) -> Result<String, String> {
        let resp = self
            .client
            .get(GROQ_MODELS_URL)
            .bearer_auth(api_key)
            .send()
            .await
            .map_err(|e| e.to_string())?;

        if !resp.status().is_success() {
            // Can't reach models endpoint — trust the configured model.
            return Ok(preferred.to_string());
        }

        let body: GroqModelsResponse = resp.json().await.map_err(|e| e.to_string())?;
        let available: std::collections::HashSet<String> =
            body.data.into_iter().map(|m| m.id).collect();

        // Prefer the requested model if it's still listed.
        if available.contains(preferred) {
            return Ok(preferred.to_string());
        }

        // Walk the fallback list.
        for &candidate in FALLBACK_MODELS {
            if available.contains(candidate) {
                return Ok(candidate.to_string());
            }
        }

        // Nothing matched — return preferred and let the call fail naturally.
        Ok(preferred.to_string())
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
            provider: "groq".to_string(),
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

fn extract_metrics(path: &PathBuf, parsed: &serde_json::Value) -> MetricBundle {
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
        path: path.clone(),
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

#[derive(Debug, Deserialize)]
struct GroqModelsResponse {
    data: Vec<GroqModel>,
}

#[derive(Debug, Deserialize)]
struct GroqModel {
    id: String,
}

//! Groq inference client for contextualisation.
//!
//! Every result is tagged with full provenance so it can be compared across
//! invocations and stored in Padagonia.

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
use tokio::sync::broadcast;

const GROQ_API_URL: &str = "https://api.groq.com/openai/v1/chat/completions";
const CONTEXT_VERSION: &str = "1.0";

/// Configuration for the Groq contextualiser.
#[derive(Debug, Clone)]
pub struct ContextConfig {
    /// Groq API key (read from `GROQ_API_KEY` env var if not provided).
    pub api_key: Option<String>,
    /// Model identifier, e.g. `"llama3-8b-8192"`.
    pub model: String,
    /// Maximum tokens to request per inference call.
    pub max_tokens: u32,
}

impl Default for ContextConfig {
    fn default() -> Self {
        Self {
            api_key: std::env::var("GROQ_API_KEY").ok(),
            model: "llama3-8b-8192".to_string(),
            max_tokens: 512,
        }
    }
}

/// Performs contextualisation against the Groq API.
pub struct Contextualiser {
    client: Client,
    config: ContextConfig,
}

impl Contextualiser {
    pub fn new(config: ContextConfig) -> Self {
        Self {
            client: Client::new(),
            config,
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

            // Signal contextualisation has begun.
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

    async fn contextualise_one(
        &self,
        desc: &Descriptor,
    ) -> Result<(ContextAnnotation, MetricBundle), String> {
        let api_key = self
            .config
            .api_key
            .clone()
            .ok_or_else(|| "GROQ_API_KEY not set".to_string())?;

        let prompt = build_prompt(desc);
        let descriptor_bytes = serde_json::to_vec(&desc.payload).unwrap_or_default();
        let descriptor_hash = hex::encode(Sha256::digest(&descriptor_bytes));

        let request_body = serde_json::json!({
            "model": self.config.model,
            "max_tokens": self.config.max_tokens,
            "messages": [
                {
                    "role": "system",
                    "content": SYSTEM_PROMPT
                },
                {
                    "role": "user",
                    "content": prompt
                }
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
            model: self.config.model.clone(),
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

// ── Groq wire types ──────────────────────────────────────────────────────────

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

//! `gls provider` — interactively choose an inference provider and model.
//!
//! Model availability comes from [`gls_provider::resolve_models`]: a real
//! poll against the provider's model-listing API, persisted in Padagonia
//! with a TTL so repeat runs don't hit the network every time. The chosen
//! provider/model is saved to Padagonia as well, so other commands (and
//! future runs) can read it back via `Padagonia::selected_provider`.

use crate::menu::{select, MenuItem};
use gls_padagonia::{Padagonia, PadagoniaConfig};
use gls_provider::{new_client, resolve_models, MenuSource, Provider, ProviderKind, PROVIDERS};
use std::io::Write;

/// Cap on how many models are shown in the selection menu — some
/// aggregators (OpenRouter) list hundreds, and a menu that long stops
/// being usable in a terminal.
const MAX_MODELS_SHOWN: usize = 20;

pub async fn run(force_refresh: bool) {
    let padagonia = match Padagonia::open(&PadagoniaConfig::default()) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("[error] cannot open Padagonia store: {e}");
            std::process::exit(1);
        }
    };

    let provider_items: Vec<MenuItem> = PROVIDERS
        .iter()
        .map(|p| MenuItem {
            label: p.name.to_string(),
            hint: provider_hint(p),
        })
        .collect();

    let Some(idx) = select("Select an inference provider:", &provider_items) else {
        println!("Cancelled.");
        return;
    };
    let provider = &PROVIDERS[idx];

    println!(
        "\n{}{}",
        provider.name,
        if force_refresh {
            " — forcing a fresh poll…"
        } else {
            " — checking model availability…"
        }
    );

    let client = new_client();
    let menu = resolve_models(&padagonia, &client, provider, force_refresh).await;

    let model_id = match &menu.source {
        MenuSource::Unavailable(reason) => {
            println!(
                "No live model listing available for {}: {reason}",
                provider.name
            );
            prompt_manual_model()
        }
        source => {
            report_source(source, provider);
            if menu.models.is_empty() {
                println!("{} reported no models for this key.", provider.name);
                prompt_manual_model()
            } else {
                let total = menu.models.len();
                let shown = &menu.models[..total.min(MAX_MODELS_SHOWN)];
                if total > shown.len() {
                    println!(
                        "(showing top {} of {total}, ranked by inference-efficiency heuristic)",
                        shown.len()
                    );
                }
                let model_items: Vec<MenuItem> = shown
                    .iter()
                    .map(|m| MenuItem {
                        label: m.id.clone(),
                        hint: format!("efficiency {:.2}", m.efficiency_score),
                    })
                    .collect();
                select("Select a model:", &model_items).map(|i| shown[i].id.clone())
            }
        }
    };

    let Some(model_id) = model_id else {
        println!("Cancelled.");
        return;
    };

    if let Err(e) = padagonia.set_selected_provider(provider.id, &model_id) {
        eprintln!("[warn] could not save selection: {e}");
    }

    println!("\nSelected: {} \u{2192} {model_id}", provider.name);
    if provider.id == "groq" {
        println!("Run `gls --model {model_id}` (or set GLS_GROQ_MODEL={model_id}) to use it now.");
    } else {
        println!(
            "Note: gls currently executes CONTEXT-phase inference via Groq only; \
             this selection is saved for reference and future provider support."
        );
    }
}

fn provider_hint(p: &Provider) -> String {
    let key_status = if std::env::var(p.env_var).is_ok() {
        "key set"
    } else {
        "no key"
    };
    match p.kind {
        ProviderKind::Unsupported { .. } => format!("{key_status} \u{b7} manual model entry"),
        _ => format!("{key_status} \u{b7} live listing"),
    }
}

fn report_source(source: &MenuSource, provider: &Provider) {
    match source {
        MenuSource::Live => println!("Live model list fetched from {}.", provider.name),
        MenuSource::Cached => println!("Using cached model list (fresh within the last hour)."),
        MenuSource::StaleFallback(reason) => println!(
            "Live poll failed ({reason}); showing the last cached list for {} (may be stale).",
            provider.name
        ),
        MenuSource::Unavailable(_) => {}
    }
}

fn prompt_manual_model() -> Option<String> {
    print!("Enter a model id manually (blank to cancel): ");
    let _ = std::io::stdout().flush();
    let mut line = String::new();
    if std::io::stdin().read_line(&mut line).is_err() {
        return None;
    }
    let trimmed = line.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

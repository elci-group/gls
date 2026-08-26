//! Build a compact, information-dense descriptor for a filesystem object.
//!
//! A descriptor is substantially cheaper to transmit to an inference provider
//! than reading the raw file.  It captures structural signals, excerpts, and
//! hashes.

use gls_core::{
    event::{Descriptor, PhysicalMeasure, PipelineEvent},
    pipeline::PipelineSender,
};
use gls_core::semantic::Classification;
use gls_core::semantic::SemanticType;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::io::Read;
use std::path::{Path, PathBuf};

/// Build descriptors for a batch of objects and emit `Indexed` events.
pub async fn build_descriptor(
    items: Vec<(PathBuf, Option<Classification>, Option<PhysicalMeasure>)>,
    excerpt_bytes: usize,
    tx: PipelineSender,
) -> Vec<Descriptor> {
    let results = tokio::task::spawn_blocking(move || {
        items
            .into_iter()
            .map(|(path, cls, meas)| {
                build_one(&path, cls.as_ref(), meas.as_ref(), excerpt_bytes)
            })
            .collect::<Vec<_>>()
    })
    .await
    .unwrap_or_default();

    for desc in &results {
        let _ = tx.send(PipelineEvent::Indexed(desc.clone()));
    }
    results
}

fn build_one(
    path: &Path,
    cls: Option<&Classification>,
    meas: Option<&PhysicalMeasure>,
    excerpt_bytes: usize,
) -> Descriptor {
    let basename = path
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_default();
    let ext = path.extension().map(|e| e.to_string_lossy().to_string());

    let source_hash = hash_path(path);
    let excerpt = if cls.is_some_and(|c| c.semantic_type == SemanticType::Credential) {
        None
    } else {
        extract_excerpt(path, excerpt_bytes)
    };
    let line_count = meas.and_then(|measurement| measurement.line_count);

    let type_info: Value = if let Some(c) = cls {
        json!({
            "type": c.semantic_type,
            "confidence": c.confidence,
            "language": c.language,
        })
    } else {
        json!(null)
    };

    let physical: Value = if let Some(m) = meas {
        json!({
            "logical_bytes": m.logical_bytes,
            "tree_bytes": m.tree_bytes,
            "object_count": m.object_count,
            "entropy": m.entropy,
            "reclaimable_fraction": m.reclaimable_fraction,
        })
    } else {
        json!(null)
    };

    let payload = json!({
        "path": basename,
        "basename": basename,
        "extension": ext,
        "type": type_info,
        "physical": physical,
        "line_count": line_count,
        "excerpt": excerpt,
    });

    let payload_bytes = serde_json::to_vec(&payload).unwrap_or_default();
    let descriptor_hash = hex::encode(Sha256::digest(&payload_bytes));

    Descriptor {
        path: path.to_path_buf(),
        descriptor_hash,
        source_hash,
        payload,
    }
}

/// SHA-256 of the first 4 KiB of the file (cheap, representative).
fn hash_path(path: &Path) -> Option<String> {
    if path.is_dir() {
        return None;
    }
    let mut f = std::fs::File::open(path).ok()?;
    let mut buf = vec![0u8; 4096];
    let n = f.read(&mut buf).ok()?;
    Some(hex::encode(Sha256::digest(&buf[..n])))
}

/// First ~20 non-empty lines from a strictly bounded byte prefix.
fn extract_excerpt(path: &Path, max_bytes: usize) -> Option<Vec<String>> {
    if path.is_dir() {
        return None;
    }
    let mut f = std::fs::File::open(path).ok()?;
    let mut buf = Vec::with_capacity(max_bytes.min(8 * 1024));
    f.by_ref()
        .take(max_bytes as u64)
        .read_to_end(&mut buf)
        .ok()?;
    let text = String::from_utf8_lossy(&buf);
    let lines: Vec<String> = text
        .lines()
        .filter(|l| !l.trim().is_empty())
        .take(20)
        .map(|l| l.trim().to_string())
        .collect();
    if lines.is_empty() {
        None
    } else {
        Some(lines)
    }
}

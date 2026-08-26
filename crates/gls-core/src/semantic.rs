//! Semantic classification types — produced by the TYPE phase.

use serde::{Deserialize, Serialize};

/// Broad semantic category for a filesystem object.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SemanticType {
    Directory,
    SourceCode,
    Documentation,
    Executable,
    Configuration,
    Dataset,
    Database,
    Archive,
    Image,
    Video,
    Audio,
    Credential,
    Symlink,
    BuildArtefact,
    TestSuite,
    Unknown,
}

impl SemanticType {
    /// Returns the Unicode icon for this type.
    pub fn icon(&self) -> &'static str {
        match self {
            SemanticType::Directory => "📁",
            SemanticType::SourceCode => "📄",
            SemanticType::Documentation => "📝",
            SemanticType::Executable => "⚙️",
            SemanticType::Configuration => "📋",
            SemanticType::Dataset => "📊",
            SemanticType::Database => "🗃",
            SemanticType::Archive => "📦",
            SemanticType::Image => "🖼",
            SemanticType::Video => "🎬",
            SemanticType::Audio => "🎵",
            SemanticType::Credential => "🔐",
            SemanticType::Symlink => "🔗",
            SemanticType::BuildArtefact => "🔨",
            SemanticType::TestSuite => "🧪",
            SemanticType::Unknown => "❓",
        }
    }

    /// Human-readable label.
    pub fn label(&self) -> &'static str {
        match self {
            SemanticType::Directory => "directory",
            SemanticType::SourceCode => "source",
            SemanticType::Documentation => "documentation",
            SemanticType::Executable => "executable",
            SemanticType::Configuration => "configuration",
            SemanticType::Dataset => "dataset",
            SemanticType::Database => "database",
            SemanticType::Archive => "archive",
            SemanticType::Image => "image",
            SemanticType::Video => "video",
            SemanticType::Audio => "audio",
            SemanticType::Credential => "credential",
            SemanticType::Symlink => "symlink",
            SemanticType::BuildArtefact => "build artefacts",
            SemanticType::TestSuite => "test suite",
            SemanticType::Unknown => "unknown",
        }
    }
}

/// Classification result with supporting evidence.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Classification {
    pub semantic_type: SemanticType,
    /// 0.0–1.0 confidence in this classification.
    pub confidence: f32,
    /// Deterministic evidence signals used for this classification.
    pub evidence: Vec<String>,
    /// Optional language/technology tag (e.g. "Rust", "Python").
    pub language: Option<String>,
}

/// Epistemological category: how a value was obtained.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Provenance {
    /// Read directly from the filesystem, no inference.
    Observed,
    /// Computed deterministically from observed values.
    Derived,
    /// Produced by a language model.
    Inferred,
    /// Statistical estimation.
    Estimated,
    /// Not yet known.
    Unknown,
}

//! A discovered filesystem object, progressively enriched through the pipeline.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Cheap, deterministic, raw filesystem metadata — the result of DISCOVER.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FsEntry {
    /// Absolute path.
    pub path: PathBuf,
    /// Final path component.
    pub basename: String,
    /// Path relative to the scan root.
    pub relative: PathBuf,
    pub kind: EntryKind,
    pub symlink: bool,
    pub hidden: bool,
    pub permissions: u32,
    pub modified: Option<DateTime<Utc>>,
    pub inode: Option<u64>,
    /// Logical byte count.
    pub logical_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EntryKind {
    File,
    Directory,
    Symlink,
    Other,
}

impl std::fmt::Display for EntryKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EntryKind::File => write!(f, "file"),
            EntryKind::Directory => write!(f, "directory"),
            EntryKind::Symlink => write!(f, "symlink"),
            EntryKind::Other => write!(f, "other"),
        }
    }
}

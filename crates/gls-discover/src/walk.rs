//! Async walker that emits `PipelineEvent::Discovery` for every entry found.

use chrono::{DateTime, Utc};
use gls_core::{
    entry::{EntryKind, FsEntry},
    event::PipelineEvent,
};
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;
use tokio::sync::broadcast;
use walkdir::WalkDir;

/// Configuration for the discovery walk.
#[derive(Debug, Clone)]
pub struct DiscoverConfig {
    /// Maximum recursion depth (0 = root only, 1 = direct children, …).
    pub max_depth: usize,
    /// Follow symbolic links during the walk.
    pub follow_links: bool,
    /// Include hidden entries (names starting with `.`).
    pub include_hidden: bool,
}

impl Default for DiscoverConfig {
    fn default() -> Self {
        Self {
            max_depth: 1,
            follow_links: false,
            include_hidden: false,
        }
    }
}

/// Walk `root` according to `config` and emit `Discovery` events on `tx`.
///
/// Runs the blocking walkdir call on a Tokio blocking thread so the async
/// runtime is never stalled.
pub async fn discover(
    root: PathBuf,
    config: DiscoverConfig,
    tx: broadcast::Sender<PipelineEvent>,
) {
    let root_clone = root.clone();
    let entries = tokio::task::spawn_blocking(move || {
        collect_entries(&root_clone, &config)
    })
    .await
    .unwrap_or_default();

    for entry in entries {
        // Ignore send errors: the receiver may have been dropped.
        let _ = tx.send(PipelineEvent::Discovery(entry));
    }
}

fn collect_entries(root: &Path, config: &DiscoverConfig) -> Vec<FsEntry> {
    let mut out = Vec::new();

    let walker = WalkDir::new(root)
        .max_depth(config.max_depth)
        .follow_links(config.follow_links)
        .sort_by_file_name();

    for result in walker {
        let dent = match result {
            Ok(d) => d,
            Err(_) => continue,
        };

        // Skip the root itself.
        if dent.depth() == 0 {
            continue;
        }

        let path = dent.path();
        let basename = dent.file_name().to_string_lossy().to_string();

        // Hidden filter.
        if !config.include_hidden && basename.starts_with('.') {
            continue;
        }

        let relative = path
            .strip_prefix(root)
            .unwrap_or(path)
            .to_path_buf();

        let ft = dent.file_type();
        let kind = if ft.is_dir() {
            EntryKind::Directory
        } else if ft.is_symlink() {
            EntryKind::Symlink
        } else if ft.is_file() {
            EntryKind::File
        } else {
            EntryKind::Other
        };

        let symlink = ft.is_symlink();

        let meta = dent.metadata().ok();
        let permissions = meta.as_ref().map(|m| {
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                m.permissions().mode()
            }
            #[cfg(not(unix))]
            0u32
        }).unwrap_or(0);

        let modified: Option<DateTime<Utc>> = meta
            .as_ref()
            .and_then(|m| m.modified().ok())
            .and_then(|st| {
                let dur = st.duration_since(UNIX_EPOCH).ok()?;
                DateTime::from_timestamp(dur.as_secs() as i64, 0)
            });

        let logical_bytes = meta.as_ref().map(|m| m.len()).unwrap_or(0);

        let inode: Option<u64> = {
            #[cfg(unix)]
            {
                use std::os::unix::fs::MetadataExt;
                meta.as_ref().map(|m| m.ino())
            }
            #[cfg(not(unix))]
            {
                None
            }
        };

        out.push(FsEntry {
            path: path.to_path_buf(),
            basename,
            relative,
            kind,
            symlink,
            hidden: dent.file_name().to_string_lossy().starts_with('.'),
            permissions,
            modified,
            inode,
            logical_bytes,
        });
    }

    out
}

//! Async walker that emits `PipelineEvent::Discovery` for every entry found.

use chrono::{DateTime, Utc};
use gls_core::{
    entry::{EntryKind, FsEntry},
    event::PipelineEvent,
    pipeline::PipelineSender,
};
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;
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

/// Walk `root`, emitting each `Discovery` event as it is found and returning
/// the complete set for downstream phases.
///
/// Runs the blocking walkdir call on a Tokio blocking thread so the async
/// runtime is never stalled.
pub async fn discover(root: PathBuf, config: DiscoverConfig, tx: PipelineSender) -> Vec<FsEntry> {
    let root_clone = root.clone();
    tokio::task::spawn_blocking(move || collect_entries(&root_clone, &config, &tx))
        .await
        .unwrap_or_default()
}

fn collect_entries(root: &Path, config: &DiscoverConfig, tx: &PipelineSender) -> Vec<FsEntry> {
    let mut out = Vec::new();

    let walker = WalkDir::new(root)
        .max_depth(config.max_depth)
        .follow_links(config.follow_links)
        .sort_by_file_name()
        .into_iter()
        .filter_entry(|entry| {
            entry.depth() == 0
                || config.include_hidden
                || !entry.file_name().to_string_lossy().starts_with('.')
        });

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

        let relative = path.strip_prefix(root).unwrap_or(path).to_path_buf();

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
        let permissions = meta
            .as_ref()
            .map(|m| {
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    m.permissions().mode()
                }
                #[cfg(not(unix))]
                0u32
            })
            .unwrap_or(0);

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

        let entry = FsEntry {
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
        };
        let _ = tx.send(PipelineEvent::Discovery(entry.clone()));
        out.push(entry);
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use gls_core::pipeline;

    #[test]
    fn hidden_directories_are_pruned_from_the_walk() {
        let root = ScratchDir::new();
        std::fs::create_dir(root.path().join(".hidden")).unwrap();
        std::fs::write(root.path().join(".hidden/visible.txt"), b"secret").unwrap();
        std::fs::write(root.path().join("public.txt"), b"public").unwrap();
        let (sender, _receiver) = pipeline::channel();

        let entries = collect_entries(
            root.path(),
            &DiscoverConfig {
                max_depth: 2,
                follow_links: false,
                include_hidden: false,
            },
            &sender,
        );

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].basename, "public.txt");
    }

    struct ScratchDir(PathBuf);

    impl ScratchDir {
        fn new() -> Self {
            let unique = format!(
                "gls-discover-test-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            );
            let path = std::env::temp_dir().join(unique);
            std::fs::create_dir_all(&path).unwrap();
            Self(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for ScratchDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
}

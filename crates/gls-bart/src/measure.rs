//! Compute physical/storage measurements for a filesystem entry.

use gls_core::{
    event::{PhysicalMeasure, PipelineEvent, SizeAggregate},
    pipeline::PipelineSender,
};
use std::path::{Path, PathBuf};

/// The result of measuring one requested path.
pub struct Measurement {
    /// Top-level measurement, carried on the pipeline stream as before.
    pub measure: PhysicalMeasure,
    /// For directories: one aggregate per directory node BART's recursive
    /// walk visited — the requested path itself, through every
    /// intermediate directory, down to each leaf. Empty for files.
    ///
    /// This is *not* broadcast on the pipeline channel (a large subtree can
    /// contain thousands of directories, which would risk overrunning the
    /// channel's bounded capacity); callers persist it directly, e.g. via
    /// `Padagonia::store_size_aggregates`.
    pub aggregates: Vec<SizeAggregate>,
}

/// Measure a batch of paths, emitting `Measured` events (as before) and
/// returning the full per-path result, including — for directories — the
/// complete recursive size breakdown.
pub async fn measure_entry(
    requests: Vec<(PathBuf, usize)>,
    tx: PipelineSender,
) -> Vec<Measurement> {
    let total = requests.len() as u64;
    tx.tool_progress("bart", gls_core::pipeline::ToolState::Running, 0, total, None);

    let results = tokio::task::spawn_blocking(move || {
        requests
            .iter()
            .map(|(path, max_depth)| measure_path(path, *max_depth))
            .collect::<Vec<_>>()
    })
    .await
    .unwrap_or_default();

    for (i, m) in results.iter().enumerate() {
        let _ = tx.send(PipelineEvent::Measured(m.measure.clone()));
        tx.tool_progress(
            "bart",
            gls_core::pipeline::ToolState::Running,
            (i + 1) as u64,
            total,
            None,
        );
    }

    tx.tool_progress(
        "bart",
        gls_core::pipeline::ToolState::Completed,
        total,
        total,
        None,
    );
    results
}

fn measure_path(path: &Path, max_depth: usize) -> Measurement {
    if path.is_dir() {
        measure_dir(path, max_depth)
    } else {
        Measurement {
            measure: measure_file(path),
            aggregates: Vec::new(),
        }
    }
}

fn measure_file(path: &Path) -> PhysicalMeasure {
    let meta = std::fs::metadata(path);
    let logical_bytes = meta.as_ref().map(|m| m.len()).unwrap_or(0);
    let allocated_bytes = allocated(path);
    let entropy = compute_entropy(path);
    let line_count = entropy.and_then(|e| compute_line_count(path, logical_bytes, e));

    PhysicalMeasure {
        path: path.to_path_buf(),
        logical_bytes,
        allocated_bytes,
        tree_bytes: None,
        object_count: None,
        entropy,
        line_count,
        reclaimable_fraction: None,
    }
}

/// Directory names whose contents are treated as reclaimable weight
/// (caches, build outputs) wherever they appear in a walked subtree.
const RECLAIMABLE_NAMES: &[&str] = &[
    "target",
    "node_modules",
    "__pycache__",
    ".mypy_cache",
    "dist",
    "build",
    ".next",
    ".nuxt",
];

fn measure_dir(path: &Path, max_depth: usize) -> Measurement {
    let mut aggregates = Vec::new();
    let (tree_bytes, object_count, reclaimable) =
        measure_dir_recursive(path, 0, max_depth, &mut aggregates);

    let reclaimable_fraction = if tree_bytes > 0 {
        Some(reclaimable as f32 / tree_bytes as f32)
    } else {
        None
    };
    let logical_bytes = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);

    let measure = PhysicalMeasure {
        path: path.to_path_buf(),
        logical_bytes,
        allocated_bytes: None,
        tree_bytes: Some(tree_bytes),
        object_count: Some(object_count),
        entropy: None,
        line_count: None,
        reclaimable_fraction,
    };
    Measurement {
        measure,
        aggregates,
    }
}

/// Recursively measure `path`, bottom-up: descend into every subdirectory
/// first, then aggregate. Every file and directory under `path` is visited
/// exactly once — nothing is sampled or estimated — so byte counts are
/// exact all the way to the leaves, and each intermediate directory ends up
/// with its own accurate aggregate in `aggregates`, not just the root.
///
/// Returns `(tree_bytes, object_count, reclaimable_bytes)` for `path`
/// itself (not counting `path` as one of its own objects), matching
/// `PhysicalMeasure`'s existing convention.
fn measure_dir_recursive(
    path: &Path,
    depth: u32,
    max_depth: usize,
    aggregates: &mut Vec<SizeAggregate>,
) -> (u64, u64, u64) {
    if depth as usize >= max_depth {
        aggregates.push(SizeAggregate {
            path: path.to_path_buf(),
            parent_path: path.parent().map(Path::to_path_buf),
            depth,
            tree_bytes: 0,
            object_count: 0,
            reclaimable_fraction: None,
            is_leaf: true,
        });
        return (0, 0, 0);
    }

    let read_dir = match std::fs::read_dir(path) {
        Ok(rd) => rd,
        Err(_) => {
            // Unreadable (permissions, a race with deletion, …) — record
            // what's actually known (nothing) rather than guessing at a size.
            aggregates.push(SizeAggregate {
                path: path.to_path_buf(),
                parent_path: path.parent().map(Path::to_path_buf),
                depth,
                tree_bytes: 0,
                object_count: 0,
                reclaimable_fraction: None,
                is_leaf: true,
            });
            return (0, 0, 0);
        }
    };

    let mut tree_bytes: u64 = 0;
    let mut object_count: u64 = 0;
    let mut reclaimable: u64 = 0;
    let mut has_subdirs = false;

    for entry in read_dir.flatten() {
        let Ok(ft) = entry.file_type() else {
            continue;
        };

        if ft.is_symlink() {
            // Never followed — counted as an object, not sized.
            object_count += 1;
        } else if ft.is_dir() {
            has_subdirs = true;
            let child_path = entry.path();
            let (sub_bytes, sub_count, sub_reclaimable) =
                measure_dir_recursive(&child_path, depth + 1, max_depth, aggregates);
            tree_bytes += sub_bytes;
            object_count += 1 + sub_count;

            let bname = entry.file_name().to_string_lossy().to_lowercase();
            if RECLAIMABLE_NAMES.contains(&bname.as_str()) {
                // The whole subtree counts as reclaimable weight.
                reclaimable += sub_bytes;
            } else {
                reclaimable += sub_reclaimable;
            }
        } else if ft.is_file() {
            let bytes = entry.metadata().map(|m| m.len()).unwrap_or(0);
            tree_bytes += bytes;
            object_count += 1;
        }
    }

    let reclaimable_fraction = if tree_bytes > 0 {
        Some(reclaimable as f32 / tree_bytes as f32)
    } else {
        None
    };

    aggregates.push(SizeAggregate {
        path: path.to_path_buf(),
        parent_path: path.parent().map(Path::to_path_buf),
        depth,
        tree_bytes,
        object_count,
        reclaimable_fraction,
        is_leaf: !has_subdirs,
    });

    (tree_bytes, object_count, reclaimable)
}

/// Platform-specific allocated-block count → bytes.
fn allocated(path: &Path) -> Option<u64> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let meta = std::fs::metadata(path).ok()?;
        // `blocks()` returns 512-byte units.
        Some(meta.blocks() * 512)
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        None
    }
}

/// Count newlines for files that look like text.  Exact for files under
/// 2 MiB; above that, estimated from the newline density of the first 64 KiB
/// sample scaled to the full file size.
///
/// Gated on `entropy` because compressed/binary content sits near the
/// maximum-entropy end of the range and its "line count" is meaningless.
fn compute_line_count(path: &Path, logical_bytes: u64, entropy: f32) -> Option<u64> {
    const TEXT_ENTROPY_CEILING: f32 = 0.92;
    const FULL_READ_CAP: u64 = 2 * 1024 * 1024;

    if entropy > TEXT_ENTROPY_CEILING {
        return None;
    }
    if logical_bytes == 0 {
        return Some(0);
    }

    use std::io::Read;
    let mut f = std::fs::File::open(path).ok()?;

    if logical_bytes <= FULL_READ_CAP {
        let mut buf = Vec::with_capacity(logical_bytes as usize);
        f.read_to_end(&mut buf).ok()?;
        return Some(buf.iter().filter(|&&b| b == b'\n').count() as u64);
    }

    let mut sample = vec![0u8; 65536];
    let n = f.read(&mut sample).ok()?;
    if n == 0 {
        return Some(0);
    }
    let newlines_in_sample = sample[..n].iter().filter(|&&b| b == b'\n').count() as f64;
    let density = newlines_in_sample / n as f64;
    Some((density * logical_bytes as f64).round() as u64)
}

/// Shannon entropy of the first 64 KiB of a file, normalised to [0, 1].
fn compute_entropy(path: &Path) -> Option<f32> {
    use std::io::Read;
    let mut f = std::fs::File::open(path).ok()?;
    let mut buf = vec![0u8; 65536];
    let n = f.read(&mut buf).ok()?;
    if n == 0 {
        return Some(0.0);
    }
    let buf = &buf[..n];

    let mut freq = [0u64; 256];
    for &b in buf {
        freq[b as usize] += 1;
    }
    let len = n as f64;
    let entropy: f64 = freq
        .iter()
        .filter(|&&c| c > 0)
        .map(|&c| {
            let p = c as f64 / len;
            -p * p.log2()
        })
        .sum();
    // Max entropy for 256 symbols is 8 bits.
    Some((entropy / 8.0) as f32)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    /// Build:
    /// ```text
    /// root/
    ///   a.txt         (10 bytes)
    ///   sub1/
    ///     b.txt       (20 bytes)
    ///     sub2/
    ///       c.txt     (30 bytes)
    ///       sub3/                 (empty leaf dir)
    /// ```
    fn build_tree() -> tempfile_dir::TempDir {
        let dir = tempfile_dir::TempDir::new();
        let root = dir.path();
        std::fs::write(root.join("a.txt"), vec![b'x'; 10]).unwrap();
        std::fs::create_dir(root.join("sub1")).unwrap();
        std::fs::write(root.join("sub1/b.txt"), vec![b'x'; 20]).unwrap();
        std::fs::create_dir(root.join("sub1/sub2")).unwrap();
        std::fs::write(root.join("sub1/sub2/c.txt"), vec![b'x'; 30]).unwrap();
        std::fs::create_dir(root.join("sub1/sub2/sub3")).unwrap();
        dir
    }

    #[test]
    fn recurses_to_every_level_and_aggregates_bottom_up() {
        let dir = build_tree();
        let mut aggregates = Vec::new();
        let (tree_bytes, object_count, _) =
            measure_dir_recursive(dir.path(), 0, usize::MAX, &mut aggregates);

        // Root: every byte and every object below it, no matter the depth.
        assert_eq!(tree_bytes, 60); // 10 + 20 + 30
        assert_eq!(object_count, 6); // a.txt, sub1, b.txt, sub2, c.txt, sub3

        let by_path: HashMap<PathBuf, &SizeAggregate> =
            aggregates.iter().map(|a| (a.path.clone(), a)).collect();

        let root_agg = by_path.get(dir.path()).expect("root aggregate recorded");
        assert_eq!(root_agg.tree_bytes, 60);
        assert_eq!(root_agg.depth, 0);
        assert!(!root_agg.is_leaf);

        let sub1 = by_path
            .get(&dir.path().join("sub1"))
            .expect("sub1 aggregate recorded");
        assert_eq!(sub1.tree_bytes, 50); // 20 + 30, everything under sub1
        assert_eq!(sub1.depth, 1);
        assert_eq!(sub1.parent_path.as_deref(), Some(dir.path()));
        assert!(!sub1.is_leaf);

        let sub2 = by_path
            .get(&dir.path().join("sub1/sub2"))
            .expect("sub2 aggregate recorded");
        assert_eq!(sub2.tree_bytes, 30);
        assert_eq!(sub2.depth, 2);
        assert!(!sub2.is_leaf); // contains sub3

        let sub3 = by_path
            .get(&dir.path().join("sub1/sub2/sub3"))
            .expect("bottom-level empty directory was still reached and recorded");
        assert_eq!(sub3.tree_bytes, 0);
        assert_eq!(sub3.object_count, 0);
        assert_eq!(sub3.depth, 3);
        assert!(sub3.is_leaf);

        // Every directory in the tree got its own aggregate: root, sub1,
        // sub2, sub3 — nothing was skipped or only summarised at the top.
        assert_eq!(aggregates.len(), 4);
    }

    #[test]
    fn measure_dir_top_level_matches_recursive_root() {
        let dir = build_tree();
        let result = measure_dir(dir.path(), usize::MAX);
        assert_eq!(result.measure.tree_bytes, Some(60));
        assert_eq!(result.measure.object_count, Some(6));
        // Root + sub1 + sub2 + sub3, all captured.
        assert_eq!(result.aggregates.len(), 4);
    }

    #[test]
    fn recursive_measurement_respects_the_requested_depth() {
        let dir = build_tree();
        let shallow = measure_dir(dir.path(), 1);
        let deep = measure_dir(dir.path(), 2);

        assert_eq!(shallow.measure.tree_bytes, Some(10));
        assert_eq!(shallow.measure.object_count, Some(2));
        assert_eq!(deep.measure.tree_bytes, Some(30));
        assert_eq!(deep.measure.object_count, Some(4));
    }
}

/// Minimal self-contained temp-directory helper (no external crate needed
/// for these tests: create a uniquely-named directory under the OS temp
/// dir and remove it on drop).
#[cfg(test)]
mod tempfile_dir {
    use std::path::{Path, PathBuf};

    pub struct TempDir(PathBuf);

    impl TempDir {
        pub fn new() -> Self {
            let unique = format!(
                "gls-bart-test-{}-{}",
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

        pub fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
}

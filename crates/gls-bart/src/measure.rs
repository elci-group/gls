//! Compute physical/storage measurements for a filesystem entry.

use gls_core::event::{PhysicalMeasure, PipelineEvent};
use std::path::{Path, PathBuf};
use tokio::sync::broadcast;
use walkdir::WalkDir;

/// Measure a batch of paths and emit `Measured` events.
pub async fn measure_entry(
    paths: Vec<PathBuf>,
    tx: broadcast::Sender<PipelineEvent>,
) {
    let results = tokio::task::spawn_blocking(move || {
        paths.iter().map(|p| measure_path(p)).collect::<Vec<_>>()
    })
    .await
    .unwrap_or_default();

    for m in results {
        let _ = tx.send(PipelineEvent::Measured(m));
    }
}

fn measure_path(path: &Path) -> PhysicalMeasure {
    if path.is_dir() {
        measure_dir(path)
    } else {
        measure_file(path)
    }
}

fn measure_file(path: &Path) -> PhysicalMeasure {
    let meta = std::fs::metadata(path);
    let logical_bytes = meta.as_ref().map(|m| m.len()).unwrap_or(0);
    let allocated_bytes = allocated(path);
    let entropy = compute_entropy(path);

    PhysicalMeasure {
        path: path.to_path_buf(),
        logical_bytes,
        allocated_bytes,
        tree_bytes: None,
        object_count: None,
        entropy,
        reclaimable_fraction: None,
    }
}

fn measure_dir(path: &Path) -> PhysicalMeasure {
    let mut tree_bytes: u64 = 0;
    let mut object_count: u64 = 0;
    let mut reclaimable: u64 = 0;

    let reclaimable_names: &[&str] = &[
        "target", "node_modules", "__pycache__", ".mypy_cache",
        "dist", "build", ".next", ".nuxt",
    ];

    for entry in WalkDir::new(path).min_depth(1).into_iter().flatten() {
        let ft = entry.file_type();
        if ft.is_file() {
            let bytes = entry.metadata().map(|m| m.len()).unwrap_or(0);
            tree_bytes += bytes;
            object_count += 1;
        } else if ft.is_dir() {
            object_count += 1;
            let bname = entry.file_name().to_string_lossy().to_lowercase();
            if reclaimable_names.contains(&bname.as_str()) {
                // Estimate the sub-tree weight as reclaimable.
                let sub: u64 = WalkDir::new(entry.path())
                    .into_iter()
                    .flatten()
                    .filter(|e| e.file_type().is_file())
                    .map(|e| e.metadata().map(|m| m.len()).unwrap_or(0))
                    .sum();
                reclaimable += sub;
            }
        }
    }

    let reclaimable_fraction = if tree_bytes > 0 {
        Some(reclaimable as f32 / tree_bytes as f32)
    } else {
        None
    };

    let logical_bytes = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);

    PhysicalMeasure {
        path: path.to_path_buf(),
        logical_bytes,
        allocated_bytes: None,
        tree_bytes: Some(tree_bytes),
        object_count: Some(object_count),
        entropy: None,
        reclaimable_fraction,
    }
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
    let entropy: f64 = freq.iter().filter(|&&c| c > 0).map(|&c| {
        let p = c as f64 / len;
        -p * p.log2()
    }).sum();
    // Max entropy for 256 symbols is 8 bits.
    Some((entropy / 8.0) as f32)
}

//! Dynamic column-dimension selection.
//!
//! The final grid layout groups entries into labelled columns.  Which
//! attribute to group by is not fixed — a web project's most informative
//! split is by extension (`.html` / `.css` / `.js`), while a directory of
//! generated data might split more usefully by size or by how recently
//! files changed.  Rather than pick one axis up front, we score several
//! candidate groupings against the actual directory contents and use
//! whichever one carves the entries into the most balanced, legible set of
//! categories.

use crate::state::EntryDisplayState;
use chrono::Utc;
use std::collections::HashMap;

/// Minimum number of entries before grouping is worth attempting at all.
/// Below this, a flat grid reads more clearly than a categorised one.
const MIN_ENTRIES_FOR_GROUPING: usize = 6;

/// A grouping must produce at least this many distinct categories …
const MIN_GROUPS: usize = 2;
/// … and no more than this many, or the "categories" are really just names.
const MAX_GROUPS: usize = 8;

/// A grouping must describe at least this fraction of entries to be usable;
/// otherwise most of the listing would fall into a catch-all bucket.
const MIN_COVERAGE: f64 = 0.5;

/// One candidate axis to group entries by.
struct Candidate {
    name: &'static str,
    label: fn(&EntryDisplayState) -> Option<String>,
}

/// Priority order used to break near-equal scores: earlier entries win.
/// Chosen so human-legible, semantically-named categories (extension/type)
/// are preferred over numeric buckets when they're roughly equally good.
const CANDIDATES: &[Candidate] = &[
    Candidate {
        name: "language",
        label: label_language,
    },
    Candidate {
        name: "type",
        label: label_semantic_type,
    },
    Candidate {
        name: "lines of code",
        label: label_loc_bucket,
    },
    Candidate {
        name: "size",
        label: label_size_bucket,
    },
    Candidate {
        name: "last modified",
        label: label_recency_bucket,
    },
    Candidate {
        name: "content entropy",
        label: label_entropy_bucket,
    },
];

/// A chosen grouping: which axis, and the resulting categories in display
/// order (largest group first, ties broken alphabetically).
pub struct Grouping {
    pub axis_name: &'static str,
    /// (category label, paths in that category) — paths are indices into
    /// the caller's `order`/`entries`, kept as owned strings for simplicity.
    pub groups: Vec<(String, Vec<std::path::PathBuf>)>,
    /// Entries that didn't get a label under the winning axis (e.g. `None`
    /// language for a directory) — still shown, in a trailing "other" group.
    pub ungrouped: Vec<std::path::PathBuf>,
}

/// Score and select the best grouping axis for this set of entries.
/// Returns `None` when no axis clears the legibility bar, meaning the
/// caller should fall back to a flat (uncategorised) grid.
pub fn select_grouping(
    entries: &HashMap<std::path::PathBuf, EntryDisplayState>,
    order: &[std::path::PathBuf],
) -> Option<Grouping> {
    if order.len() < MIN_ENTRIES_FOR_GROUPING {
        return None;
    }

    let mut best: Option<(f64, usize, &Candidate)> = None;

    for (idx, candidate) in CANDIDATES.iter().enumerate() {
        let mut counts: HashMap<String, usize> = HashMap::new();
        let mut covered = 0usize;

        for path in order {
            let Some(state) = entries.get(path) else {
                continue;
            };
            if let Some(label) = (candidate.label)(state) {
                *counts.entry(label).or_insert(0) += 1;
                covered += 1;
            }
        }

        let distinct = counts.len();
        if !(MIN_GROUPS..=MAX_GROUPS).contains(&distinct) {
            continue;
        }
        let coverage = covered as f64 / order.len() as f64;
        if coverage < MIN_COVERAGE {
            continue;
        }

        // Shannon entropy of the group-size distribution, in bits — higher
        // means the entries are spread evenly across categories rather than
        // dominated by one bucket. Weighted by coverage so an axis that
        // describes more of the directory is preferred at equal balance.
        let entropy: f64 = counts
            .values()
            .map(|&c| {
                let p = c as f64 / covered as f64;
                -p * p.log2()
            })
            .sum();
        let score = entropy * coverage;

        let better = match &best {
            None => true,
            Some((best_score, best_idx, _)) => {
                score > best_score + 1e-9 || (score > best_score - 1e-9 && idx < *best_idx)
            }
        };
        if better {
            best = Some((score, idx, candidate));
        }
    }

    let (_, _, candidate) = best?;

    let mut buckets: HashMap<String, Vec<std::path::PathBuf>> = HashMap::new();
    let mut ungrouped = Vec::new();

    for path in order {
        let Some(state) = entries.get(path) else {
            continue;
        };
        match (candidate.label)(state) {
            Some(label) => buckets.entry(label).or_default().push(path.clone()),
            None => ungrouped.push(path.clone()),
        }
    }

    let mut groups: Vec<(String, Vec<std::path::PathBuf>)> = buckets.into_iter().collect();
    groups.sort_by(|a, b| b.1.len().cmp(&a.1.len()).then_with(|| a.0.cmp(&b.0)));

    Some(Grouping {
        axis_name: candidate.name,
        groups,
        ungrouped,
    })
}

// ── Label functions ────────────────────────────────────────────────────────

fn label_language(state: &EntryDisplayState) -> Option<String> {
    state.classification.as_ref()?.language.clone()
}

fn label_semantic_type(state: &EntryDisplayState) -> Option<String> {
    state
        .classification
        .as_ref()
        .map(|c| c.semantic_type.label().to_string())
}

fn label_loc_bucket(state: &EntryDisplayState) -> Option<String> {
    let loc = state.physical.as_ref()?.line_count?;
    Some(loc_bucket(loc))
}

fn loc_bucket(loc: u64) -> String {
    match loc {
        0..=99 => "< 100 LOC".to_string(),
        100..=499 => "100–499 LOC".to_string(),
        500..=1999 => "500–1999 LOC".to_string(),
        _ => "2000+ LOC".to_string(),
    }
}

fn label_size_bucket(state: &EntryDisplayState) -> Option<String> {
    Some(size_bucket(state.measured_bytes()))
}

fn size_bucket(bytes: u64) -> String {
    const KB: u64 = 1_024;
    const MB: u64 = KB * 1_024;
    match bytes {
        0..=1023 => "< 1 KB".to_string(),
        n if n < 10 * KB => "1–10 KB".to_string(),
        n if n < 100 * KB => "10–100 KB".to_string(),
        n if n < MB => "100 KB–1 MB".to_string(),
        _ => "1 MB+".to_string(),
    }
}

fn label_recency_bucket(state: &EntryDisplayState) -> Option<String> {
    let modified = state.modified?;
    let age = Utc::now().signed_duration_since(modified);
    Some(if age.num_hours() < 24 {
        "today".to_string()
    } else if age.num_days() < 7 {
        "this week".to_string()
    } else if age.num_days() < 30 {
        "this month".to_string()
    } else {
        "older".to_string()
    })
}

fn label_entropy_bucket(state: &EntryDisplayState) -> Option<String> {
    let e = state.physical.as_ref()?.entropy?;
    Some(if e < 0.5 {
        "low entropy".to_string()
    } else if e < 0.85 {
        "medium entropy".to_string()
    } else {
        "high entropy".to_string()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use gls_core::event::PhysicalMeasure;
    use gls_core::semantic::{Classification, SemanticType};
    use std::path::PathBuf;

    fn entry(name: &str, language: Option<&str>, loc: Option<u64>) -> EntryDisplayState {
        let mut s = EntryDisplayState::new(PathBuf::from(name), name.to_string(), false);
        s.classification = Some(Classification {
            semantic_type: SemanticType::SourceCode,
            confidence: 0.9,
            evidence: vec![],
            language: language.map(str::to_string),
        });
        if let Some(loc) = loc {
            s.physical = Some(PhysicalMeasure {
                path: PathBuf::from(name),
                logical_bytes: 100,
                allocated_bytes: None,
                tree_bytes: None,
                object_count: None,
                entropy: Some(0.3),
                line_count: Some(loc),
                reclaimable_fraction: None,
            });
        }
        s
    }

    fn corpus(
        items: Vec<(&str, Option<&str>, Option<u64>)>,
    ) -> (HashMap<PathBuf, EntryDisplayState>, Vec<PathBuf>) {
        let mut entries = HashMap::new();
        let mut order = Vec::new();
        for (name, lang, loc) in items {
            let path = PathBuf::from(name);
            entries.insert(path.clone(), entry(name, lang, loc));
            order.push(path);
        }
        (entries, order)
    }

    #[test]
    fn web_project_groups_by_language() {
        let (entries, order) = corpus(vec![
            ("a.html", Some("HTML"), None),
            ("b.html", Some("HTML"), None),
            ("c.css", Some("CSS"), None),
            ("d.css", Some("CSS"), None),
            ("e.js", Some("JavaScript"), None),
            ("f.js", Some("JavaScript"), None),
        ]);
        let grouping = select_grouping(&entries, &order).expect("should group");
        assert_eq!(grouping.axis_name, "language");
        assert_eq!(grouping.groups.len(), 3);
    }

    #[test]
    fn uniform_language_falls_back_to_loc() {
        // Every file is Rust — "language" has zero diversity, so the
        // selector should fall through to a dimension that actually
        // distinguishes these entries (line count).
        let (entries, order) = corpus(vec![
            ("a.rs", Some("Rust"), Some(20)),
            ("b.rs", Some("Rust"), Some(40)),
            ("c.rs", Some("Rust"), Some(50)),
            ("d.rs", Some("Rust"), Some(800)),
            ("e.rs", Some("Rust"), Some(1200)),
            ("f.rs", Some("Rust"), Some(3000)),
        ]);
        let grouping = select_grouping(&entries, &order).expect("should group");
        assert_ne!(grouping.axis_name, "language");
    }

    #[test]
    fn too_few_entries_do_not_group() {
        let (entries, order) = corpus(vec![
            ("a.html", Some("HTML"), None),
            ("b.css", Some("CSS"), None),
        ]);
        assert!(select_grouping(&entries, &order).is_none());
    }

    #[test]
    fn no_diversity_does_not_group() {
        let (entries, order) = corpus(vec![
            ("a.rs", Some("Rust"), None),
            ("b.rs", Some("Rust"), None),
            ("c.rs", Some("Rust"), None),
            ("d.rs", Some("Rust"), None),
            ("e.rs", Some("Rust"), None),
            ("f.rs", Some("Rust"), None),
        ]);
        assert!(select_grouping(&entries, &order).is_none());
    }
}

//! Grid layout: lay entries out to fill the terminal's horizontal space
//! instead of printing one per line.
//!
//! Two layouts are produced, depending on [`dimension::select_grouping`]:
//!
//! - **Grouped**: entries are split into labelled category blocks (e.g. by
//!   language, size, or recency — whichever axis best describes this
//!   directory), and blocks are packed left-to-right, wrapping to a new row
//!   of blocks when the terminal width is exhausted.
//! - **Flat**: no axis was legible enough (too few entries, or nothing
//!   splits them into a balanced set of categories) — entries fill a plain
//!   column-major grid, `ls -C` style.

use crate::dimension::{select_grouping, Grouping};
use crate::format::{bold, dim, human_bytes};
use crate::queue::QueueTracker;
use crate::renderer::{entry_icon, live_status_line, GRID_CONTEXT_MAX_CHARS};
use crate::state::EntryDisplayState;
use std::collections::HashMap;
use std::path::PathBuf;
use unicode_width::UnicodeWidthStr;

const COLUMN_GAP: usize = 2;
const BLOCK_GAP: usize = 3;

/// Render the full grid as a single string (no trailing newline management
/// beyond one newline per printed row).
///
/// Called continuously while the pipeline runs, not just once at the end —
/// each cell shows whatever's known about that entry *right now*, growing
/// as sub-results land (see `cell`), so this doubles as the live "thinking"
/// display. `queue` supplies CONTEXT-phase queue position/ETA for entries
/// still waiting their turn; pass `&QueueTracker::default()` when rendering
/// an already-settled snapshot (nothing will be queued by then anyway).
pub fn render_grid(
    entries: &HashMap<PathBuf, EntryDisplayState>,
    order: &[PathBuf],
    queue: &QueueTracker,
    width: u16,
    color: bool,
) -> String {
    let width = width.max(20) as usize;

    match select_grouping(entries, order) {
        Some(grouping) => render_grouped(entries, grouping, queue, width, color),
        None => render_flat(entries, order, queue, width, color),
    }
}

// ── Flat (uncategorised) column-major grid ─────────────────────────────────

fn render_flat(
    entries: &HashMap<PathBuf, EntryDisplayState>,
    order: &[PathBuf],
    queue: &QueueTracker,
    width: usize,
    color: bool,
) -> String {
    let cells: Vec<(String, usize)> = order
        .iter()
        .filter_map(|p| entries.get(p))
        .map(|s| cell(s, queue, color))
        .collect();

    layout_columns(&cells, width)
}

/// Lay a flat list of (rendered, plain_width) cells into a column-major
/// grid, like `ls -C`.
fn layout_columns(cells: &[(String, usize)], width: usize) -> String {
    if cells.is_empty() {
        return String::new();
    }
    let max_w = cells.iter().map(|(_, w)| *w).max().unwrap_or(0);
    let col_w = max_w + COLUMN_GAP;
    let n = cells.len();

    let num_cols = (width / col_w).clamp(1, n);
    let num_rows = n.div_ceil(num_cols);

    let mut out = String::new();
    for r in 0..num_rows {
        for c in 0..num_cols {
            let idx = c * num_rows + r;
            let Some((rendered, plain_w)) = cells.get(idx) else {
                continue;
            };
            let is_last_in_row = c == num_cols - 1 || c * num_rows + num_rows + r >= n;
            out.push_str(rendered);
            if !is_last_in_row {
                out.push_str(&" ".repeat(col_w - plain_w));
            }
        }
        out.push('\n');
    }
    out
}

// ── Grouped (categorised) block layout ──────────────────────────────────────

struct Block {
    header: String,
    header_w: usize,
    cells: Vec<(String, usize)>,
    inner_width: usize,
}

fn render_grouped(
    entries: &HashMap<PathBuf, EntryDisplayState>,
    grouping: Grouping,
    queue: &QueueTracker,
    width: usize,
    color: bool,
) -> String {
    let mut blocks: Vec<Block> = grouping
        .groups
        .into_iter()
        .map(|(label, paths)| build_block(entries, &label, &paths, queue, color))
        .collect();

    if !grouping.ungrouped.is_empty() {
        blocks.push(build_block(
            entries,
            "other",
            &grouping.ungrouped,
            queue,
            color,
        ));
    }

    let mut out = String::new();
    out.push_str(&dim(&format!(
        "── grouped by {} ──\n\n",
        grouping.axis_name
    )));

    // Greedily pack blocks left-to-right into rows that fit `width`.
    let mut row: Vec<&Block> = Vec::new();
    let mut row_width = 0usize;

    let mut rows: Vec<Vec<&Block>> = Vec::new();
    for block in &blocks {
        let needed = block.inner_width + if row.is_empty() { 0 } else { BLOCK_GAP };
        if !row.is_empty() && row_width + needed > width {
            rows.push(std::mem::take(&mut row));
            row_width = 0;
        }
        row_width += block.inner_width + if row.is_empty() { 0 } else { BLOCK_GAP };
        row.push(block);
    }
    if !row.is_empty() {
        rows.push(row);
    }

    for row in rows {
        out.push_str(&render_block_row(&row));
        out.push('\n');
    }

    out
}

fn build_block(
    entries: &HashMap<PathBuf, EntryDisplayState>,
    label: &str,
    paths: &[PathBuf],
    queue: &QueueTracker,
    color: bool,
) -> Block {
    let mut cells: Vec<(String, usize, String)> = paths
        .iter()
        .filter_map(|p| entries.get(p))
        .map(|s| {
            let (rendered, w) = cell(s, queue, color);
            (rendered, w, s.basename.to_lowercase())
        })
        .collect();
    cells.sort_by(|a, b| a.2.cmp(&b.2));
    let cells: Vec<(String, usize)> = cells.into_iter().map(|(r, w, _)| (r, w)).collect();

    let header_plain = format!("{label} ({})", paths.len());
    let header = if color {
        bold(&header_plain)
    } else {
        header_plain.clone()
    };
    let header_w = header_plain.width();

    let inner_width = cells
        .iter()
        .map(|(_, w)| *w)
        .max()
        .unwrap_or(0)
        .max(header_w);

    Block {
        header,
        header_w,
        cells,
        inner_width,
    }
}

fn render_block_row(row: &[&Block]) -> String {
    let mut out = String::new();

    // Header line.
    for (i, block) in row.iter().enumerate() {
        out.push_str(&block.header);
        if i < row.len() - 1 {
            out.push_str(&" ".repeat(block.inner_width - block.header_w + BLOCK_GAP));
        }
        // else: trailing padding on the last block is pointless — skip it.
    }
    out.push('\n');

    let max_rows = row.iter().map(|b| b.cells.len()).max().unwrap_or(0);
    for r in 0..max_rows {
        let mut line = String::new();
        for (i, block) in row.iter().enumerate() {
            let is_last = i == row.len() - 1;
            match block.cells.get(r) {
                Some((rendered, w)) => {
                    line.push_str(rendered);
                    if !is_last {
                        line.push_str(&" ".repeat(block.inner_width - w + BLOCK_GAP));
                    }
                }
                None if !is_last => {
                    line.push_str(&" ".repeat(block.inner_width + BLOCK_GAP));
                }
                None => {}
            }
        }
        out.push_str(line.trim_end());
        out.push('\n');
    }

    out
}

// ── Cell rendering ──────────────────────────────────────────────────────────

/// Render one entry as a grid cell: icon + name + BART-measured size, plus
/// whatever else is known about it right now — type, metrics, context
/// summary or (if it's still queued for CONTEXT) a queue position/ETA. Each
/// piece appears the instant it's ready; nothing here waits for a rotation
/// timer or for any other entry to catch up.
///
/// Returns the rendered (possibly ANSI-coloured) string alongside its
/// *visible* width, since ANSI escapes must not count toward column
/// alignment — computed from a second, always-plain render rather than by
/// stripping escapes back out of the coloured one.
fn cell(state: &EntryDisplayState, queue: &QueueTracker, color: bool) -> (String, usize) {
    let icon = entry_icon(state);
    let size = human_bytes(state.measured_bytes());

    let status_plain = live_status_line(state, queue, false, GRID_CONTEXT_MAX_CHARS, false);
    let plain = format!("{icon} {} {size} \u{b7} {status_plain}", state.basename);
    let plain_w = UnicodeWidthStr::width(plain.as_str());

    let rendered = if color {
        let status = live_status_line(state, queue, false, GRID_CONTEXT_MAX_CHARS, true);
        format!(
            "{icon} {} {} {} {status}",
            bold(&state.basename),
            dim(&size),
            dim("\u{b7}")
        )
    } else {
        plain.clone()
    };
    (rendered, plain_w)
}

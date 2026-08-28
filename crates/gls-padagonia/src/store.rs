//! SQLite-backed Padagonia store.

use chrono::{DateTime, Utc};
use gls_core::{
    event::{ContextAnnotation, Descriptor, MetricBundle, PipelineEvent, SizeAggregate},
    pipeline::PipelineReceiver,
};
use rusqlite::{params, Connection};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

/// One model entry cached from a provider's live model-listing API.
#[derive(Debug, Clone)]
pub struct CachedModel {
    pub id: String,
    pub efficiency_score: f32,
    pub fetched_at: DateTime<Utc>,
}

/// A [`SizeAggregate`] as recorded in Padagonia, with its measurement
/// timestamp.
#[derive(Debug, Clone)]
pub struct SizeAggregateRecord {
    pub path: PathBuf,
    pub parent_path: Option<PathBuf>,
    pub depth: u32,
    pub tree_bytes: u64,
    pub object_count: u64,
    pub reclaimable_fraction: Option<f32>,
    pub is_leaf: bool,
    pub measured_at: DateTime<Utc>,
}

/// Configuration for the Padagonia store.
#[derive(Debug, Clone)]
pub struct PadagoniaConfig {
    /// Path to the SQLite database file.
    pub db_path: PathBuf,
}

impl Default for PadagoniaConfig {
    fn default() -> Self {
        let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
        Self {
            db_path: PathBuf::from(home).join(".local/share/gls/padagonia.db"),
        }
    }
}

/// The Padagonia persistence layer.
///
/// Thread-safe via an `Arc<Mutex<Connection>>`.
#[derive(Clone)]
pub struct Padagonia {
    conn: Arc<Mutex<Connection>>,
}

impl Padagonia {
    /// Open (or create) the Padagonia database.
    pub fn open(config: &PadagoniaConfig) -> Result<Self, String> {
        if let Some(parent) = config.db_path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
        }
        let conn = Connection::open(&config.db_path).map_err(|e| e.to_string())?;
        let p = Padagonia {
            conn: Arc::new(Mutex::new(conn)),
        };
        p.migrate().map_err(|e| e.to_string())?;
        Ok(p)
    }

    fn migrate(&self) -> rusqlite::Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             CREATE TABLE IF NOT EXISTS context_revisions (
                 id              INTEGER PRIMARY KEY AUTOINCREMENT,
                 path            TEXT    NOT NULL,
                 revision        TEXT    NOT NULL,
                 source_hash     TEXT,
                 descriptor_hash TEXT,
                 model           TEXT,
                 provider        TEXT,
                 context_version TEXT,
                 summary         TEXT,
                 purpose         TEXT,
                 role            TEXT,
                 relationships   TEXT,
                 metrics_json    TEXT,
                 descriptor_json TEXT,
                 timestamp       TEXT    NOT NULL
             );
             CREATE INDEX IF NOT EXISTS idx_path_ts
                 ON context_revisions(path, timestamp DESC);
             CREATE TABLE IF NOT EXISTS provider_model_cache (
                 provider_id      TEXT    NOT NULL,
                 model_id         TEXT    NOT NULL,
                 efficiency_score REAL    NOT NULL,
                 fetched_at       TEXT    NOT NULL,
                 PRIMARY KEY (provider_id, model_id)
             );
             CREATE TABLE IF NOT EXISTS provider_selection (
                 id          INTEGER PRIMARY KEY CHECK (id = 1),
                 provider_id TEXT    NOT NULL,
                 model_id    TEXT    NOT NULL,
                 updated_at  TEXT    NOT NULL
             );
             CREATE TABLE IF NOT EXISTS size_aggregates (
                 path                  TEXT    PRIMARY KEY,
                 parent_path           TEXT,
                 depth                 INTEGER NOT NULL,
                 tree_bytes            INTEGER NOT NULL,
                 object_count          INTEGER NOT NULL,
                 reclaimable_fraction  REAL,
                 is_leaf               INTEGER NOT NULL,
                 measured_at           TEXT    NOT NULL
             );
             CREATE INDEX IF NOT EXISTS idx_size_parent
                 ON size_aggregates(parent_path);",
        )
    }

    /// Persist a context annotation and associated metrics.
    ///
    /// Returns the new revision ID (UUID-style string).
    pub fn persist(
        &self,
        annotation: &ContextAnnotation,
        descriptor: Option<&Descriptor>,
        metrics: Option<&MetricBundle>,
    ) -> Result<String, String> {
        let revision = uuid::Uuid::new_v4().to_string();
        let path_str = annotation.path.to_string_lossy().to_string();
        let ts = annotation.provenance.timestamp.to_rfc3339();
        let relationships = serde_json::to_string(&annotation.relationships).unwrap_or_default();
        let metrics_json = metrics.map(|m| serde_json::to_string(m).unwrap_or_default());
        let descriptor_json = descriptor.map(|d| serde_json::to_string(d).unwrap_or_default());

        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO context_revisions
             (path, revision, source_hash, descriptor_hash, model, provider,
              context_version, summary, purpose, role, relationships,
              metrics_json, descriptor_json, timestamp)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14)",
            params![
                path_str,
                revision,
                annotation.provenance.source_hash,
                annotation.provenance.descriptor_hash,
                annotation.provenance.model,
                annotation.provenance.provider,
                annotation.provenance.context_version,
                annotation.summary,
                annotation.purpose,
                annotation.role,
                relationships,
                metrics_json,
                descriptor_json,
                ts,
            ],
        )
        .map_err(|e| e.to_string())?;

        Ok(revision)
    }

    /// Check whether an existing record with the same descriptor_hash already
    /// exists.  If so, re-contextualisation is unnecessary.
    pub fn is_cache_hit(&self, path: &Path, descriptor_hash: &str) -> bool {
        let conn = self.conn.lock().unwrap();
        let count: u32 = conn
            .query_row(
                "SELECT COUNT(*) FROM context_revisions
                 WHERE path = ?1 AND descriptor_hash = ?2
                 LIMIT 1",
                params![path.to_string_lossy().as_ref(), descriptor_hash],
                |row| row.get(0),
            )
            .unwrap_or(0);
        count > 0
    }

    /// Return the most recent context summary for a path.
    pub fn latest_summary(&self, path: &Path) -> Option<String> {
        let conn = self.conn.lock().unwrap();
        conn.query_row(
            "SELECT summary FROM context_revisions
             WHERE path = ?1
             ORDER BY timestamp DESC
             LIMIT 1",
            params![path.to_string_lossy().as_ref()],
            |row| row.get(0),
        )
        .ok()
    }

    /// Return all revision timestamps for a path, newest first.
    pub fn history(&self, path: &Path) -> Vec<(String, String)> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = match conn.prepare(
            "SELECT revision, timestamp FROM context_revisions
             WHERE path = ?1
             ORDER BY timestamp DESC",
        ) {
            Ok(s) => s,
            Err(_) => return vec![],
        };
        stmt.query_map(params![path.to_string_lossy().as_ref()], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .ok()
        .map(|rows| rows.flatten().collect())
        .unwrap_or_default()
    }

    /// Replace the cached model list for a provider with a freshly-polled
    /// one (all previous entries for this provider are discarded).
    pub fn store_models(&self, provider_id: &str, models: &[(String, f32)]) -> Result<(), String> {
        let now = Utc::now().to_rfc3339();
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction().map_err(|e| e.to_string())?;
        tx.execute(
            "DELETE FROM provider_model_cache WHERE provider_id = ?1",
            params![provider_id],
        )
        .map_err(|e| e.to_string())?;
        for (id, score) in models {
            tx.execute(
                "INSERT INTO provider_model_cache
                 (provider_id, model_id, efficiency_score, fetched_at)
                 VALUES (?1, ?2, ?3, ?4)",
                params![provider_id, id, score, now],
            )
            .map_err(|e| e.to_string())?;
        }
        tx.commit().map_err(|e| e.to_string())
    }

    /// Return the currently cached model list for a provider, regardless of
    /// staleness — callers decide freshness from each entry's `fetched_at`.
    /// Empty if this provider has never been polled.
    pub fn cached_models(&self, provider_id: &str) -> Vec<CachedModel> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = match conn.prepare(
            "SELECT model_id, efficiency_score, fetched_at FROM provider_model_cache
             WHERE provider_id = ?1",
        ) {
            Ok(s) => s,
            Err(_) => return vec![],
        };
        stmt.query_map(params![provider_id], |row| {
            let fetched_at: String = row.get(2)?;
            Ok(CachedModel {
                id: row.get(0)?,
                efficiency_score: row.get(1)?,
                fetched_at: DateTime::parse_from_rfc3339(&fetched_at)
                    .map(|d| d.with_timezone(&Utc))
                    .unwrap_or_else(|_| Utc::now()),
            })
        })
        .ok()
        .map(|rows| rows.flatten().collect())
        .unwrap_or_default()
    }

    /// Persist the user's chosen provider + model (a singleton row — each
    /// call overwrites the previous selection).
    pub fn set_selected_provider(&self, provider_id: &str, model_id: &str) -> Result<(), String> {
        let now = Utc::now().to_rfc3339();
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO provider_selection (id, provider_id, model_id, updated_at)
             VALUES (1, ?1, ?2, ?3)
             ON CONFLICT(id) DO UPDATE SET
                 provider_id = excluded.provider_id,
                 model_id    = excluded.model_id,
                 updated_at  = excluded.updated_at",
            params![provider_id, model_id, now],
        )
        .map_err(|e| e.to_string())?;
        Ok(())
    }

    /// Return the last-selected provider + model, if one has been saved.
    pub fn selected_provider(&self) -> Option<(String, String, DateTime<Utc>)> {
        let conn = self.conn.lock().unwrap();
        conn.query_row(
            "SELECT provider_id, model_id, updated_at FROM provider_selection WHERE id = 1",
            [],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            },
        )
        .ok()
        .map(|(provider_id, model_id, ts)| {
            let updated_at = DateTime::parse_from_rfc3339(&ts)
                .map(|d| d.with_timezone(&Utc))
                .unwrap_or_else(|_| Utc::now());
            (provider_id, model_id, updated_at)
        })
    }

    /// Persist a full recursive directory-size breakdown in one transaction:
    /// every node BART's recursive walk visited, from the measured root
    /// down to its leaves. Each row is upserted by `path`, so re-measuring
    /// a directory always leaves Padagonia holding the latest accurate
    /// values rather than accumulating stale duplicates — and because
    /// `parent_path` always points at the real filesystem parent, the
    /// hierarchy stays reconstructible regardless of which root a given
    /// scan started from (ontological coherence: the stored shape always
    /// matches the real directory tree, never a scan-relative fiction).
    pub fn store_size_aggregates(&self, aggregates: &[SizeAggregate]) -> Result<(), String> {
        if aggregates.is_empty() {
            return Ok(());
        }
        let now = Utc::now().to_rfc3339();
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction().map_err(|e| e.to_string())?;
        for agg in aggregates {
            tx.execute(
                "INSERT INTO size_aggregates
                 (path, parent_path, depth, tree_bytes, object_count,
                  reclaimable_fraction, is_leaf, measured_at)
                 VALUES (?1,?2,?3,?4,?5,?6,?7,?8)
                 ON CONFLICT(path) DO UPDATE SET
                     parent_path          = excluded.parent_path,
                     depth                = excluded.depth,
                     tree_bytes           = excluded.tree_bytes,
                     object_count         = excluded.object_count,
                     reclaimable_fraction = excluded.reclaimable_fraction,
                     is_leaf              = excluded.is_leaf,
                     measured_at          = excluded.measured_at",
                params![
                    agg.path.to_string_lossy().as_ref(),
                    agg.parent_path
                        .as_ref()
                        .map(|p| p.to_string_lossy().to_string()),
                    agg.depth,
                    agg.tree_bytes as i64,
                    agg.object_count as i64,
                    agg.reclaimable_fraction,
                    agg.is_leaf as i32,
                    now,
                ],
            )
            .map_err(|e| e.to_string())?;
        }
        tx.commit().map_err(|e| e.to_string())
    }

    /// Return the recorded size aggregate for a single path, if it has
    /// been measured.
    pub fn size_aggregate(&self, path: &Path) -> Option<SizeAggregateRecord> {
        let conn = self.conn.lock().unwrap();
        conn.query_row(
            "SELECT path, parent_path, depth, tree_bytes, object_count,
                    reclaimable_fraction, is_leaf, measured_at
             FROM size_aggregates WHERE path = ?1",
            params![path.to_string_lossy().as_ref()],
            row_to_size_aggregate,
        )
        .ok()
    }

    /// Return the immediate children (one level down) of `path`, as
    /// recorded by the last measurement pass that walked them — this is
    /// what lets the flat table answer "what's under here" without
    /// re-walking the filesystem.
    pub fn size_children(&self, path: &Path) -> Vec<SizeAggregateRecord> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = match conn.prepare(
            "SELECT path, parent_path, depth, tree_bytes, object_count,
                    reclaimable_fraction, is_leaf, measured_at
             FROM size_aggregates WHERE parent_path = ?1
             ORDER BY tree_bytes DESC",
        ) {
            Ok(s) => s,
            Err(_) => return vec![],
        };
        stmt.query_map(
            params![path.to_string_lossy().as_ref()],
            row_to_size_aggregate,
        )
        .ok()
        .map(|rows| rows.flatten().collect())
        .unwrap_or_default()
    }

    /// Consume a pipeline broadcast receiver and persist every relevant event.
    ///
    /// Persists each contextualised object only after both `ContextUpdated` and
    /// `MetricsUpdated` have been received, or when `PipelineComplete` fires —
    /// whichever comes first.  This prevents a race where metrics arrive after
    /// an annotation is already persisted without them.
    pub async fn consume_stream(
        self,
        mut rx: PipelineReceiver,
        tx: gls_core::pipeline::PipelineSender,
    ) {
        use std::collections::HashMap;
        let mut annotations: HashMap<PathBuf, ContextAnnotation> = HashMap::new();
        let mut descriptors: HashMap<PathBuf, Descriptor> = HashMap::new();
        let mut metrics: HashMap<PathBuf, MetricBundle> = HashMap::new();
        // Tracks which paths have already been persisted this run.
        let mut persisted: std::collections::HashSet<PathBuf> = std::collections::HashSet::new();

        tx.tool_progress("padagonia", gls_core::pipeline::ToolState::Running, 0, 0, None);

        loop {
            match rx.recv().await {
                Some(event) => match event {
                    PipelineEvent::Indexed(d) => {
                        descriptors.insert(d.path.clone(), d);
                    }
                    PipelineEvent::ContextUpdated(a) => {
                        let path = a.path.clone();
                        annotations.insert(path.clone(), a);
                        // Persist only once we also have metrics for this path.
                        if metrics.contains_key(&path) && !persisted.contains(&path) {
                            if let Some(ann) = annotations.get(&path) {
                                let desc = descriptors.get(&path);
                                let met = metrics.get(&path);
                                if let Err(e) = self.persist(ann, desc, met) {
                                    eprintln!("padagonia persist error: {e}");
                                } else {
                                    persisted.insert(path);
                                }
                            }
                        }
                    }
                    PipelineEvent::MetricsUpdated(m) => {
                        let path = m.path.clone();
                        metrics.insert(path.clone(), m);
                        // Persist if annotation already arrived.
                        if annotations.contains_key(&path) && !persisted.contains(&path) {
                            if let Some(ann) = annotations.get(&path) {
                                let desc = descriptors.get(&path);
                                let met = metrics.get(&path);
                                if let Err(e) = self.persist(ann, desc, met) {
                                    eprintln!("padagonia persist error: {e}");
                                } else {
                                    persisted.insert(path);
                                }
                            }
                        }
                    }
                    PipelineEvent::PipelineComplete => {
                        // Flush any annotations that arrived without metrics.
                        for (path, ann) in &annotations {
                            if !persisted.contains(path) {
                                let desc = descriptors.get(path);
                                let met = metrics.get(path);
                                if let Err(e) = self.persist(ann, desc, met) {
                                    eprintln!("padagonia persist error: {e}");
                                }
                            }
                        }
                        tx.tool_progress(
                            "padagonia",
                            gls_core::pipeline::ToolState::Completed,
                            persisted.len() as u64,
                            0,
                            None,
                        );
                        break;
                    }
                    PipelineEvent::ToolProgress(_) => {}
                    _ => {}
                },
                None => {
                    // Flush unpersisted annotations on channel close.
                    for (path, ann) in &annotations {
                        if !persisted.contains(path) {
                            let desc = descriptors.get(path);
                            let met = metrics.get(path);
                            if let Err(e) = self.persist(ann, desc, met) {
                                eprintln!("padagonia persist error: {e}");
                            }
                        }
                    }
                    tx.tool_progress(
                        "padagonia",
                        gls_core::pipeline::ToolState::Completed,
                        persisted.len() as u64,
                        0,
                        None,
                    );
                    break;
                }
            }
        }
    }
}

fn row_to_size_aggregate(row: &rusqlite::Row) -> rusqlite::Result<SizeAggregateRecord> {
    let path: String = row.get(0)?;
    let parent_path: Option<String> = row.get(1)?;
    let measured_at: String = row.get(7)?;
    Ok(SizeAggregateRecord {
        path: PathBuf::from(path),
        parent_path: parent_path.map(PathBuf::from),
        depth: row.get(2)?,
        tree_bytes: row.get::<_, i64>(3)? as u64,
        object_count: row.get::<_, i64>(4)? as u64,
        reclaimable_fraction: row.get(5)?,
        is_leaf: row.get::<_, i32>(6)? != 0,
        measured_at: DateTime::parse_from_rfc3339(&measured_at)
            .map(|d| d.with_timezone(&Utc))
            .unwrap_or_else(|_| Utc::now()),
    })
}

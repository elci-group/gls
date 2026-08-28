//! SQLite-backed Padagonia store.

use chrono::{DateTime, Utc};
use gls_core::{
    entry::FsEntry,
    event::{
        ContextAnnotation, Descriptor, MetricBundle, PhysicalMeasure, PipelineEvent, SizeAggregate,
    },
    pipeline::PipelineReceiver,
    semantic::Classification,
};
use rusqlite::{params, Connection};
use sha2::{Digest, Sha256};
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

/// A complete deterministic analysis snapshot, optionally enriched with the
/// latest matching natural-language context and metrics revision.
#[derive(Debug, Clone)]
pub struct CachedSnapshot {
    pub entry: FsEntry,
    pub classification: Classification,
    pub measurement: PhysicalMeasure,
    pub descriptor: Descriptor,
    pub context: Option<ContextAnnotation>,
    pub metrics: Option<MetricBundle>,
    pub first_seen: DateTime<Utc>,
    pub last_seen: DateTime<Utc>,
    pub last_accessed: DateTime<Utc>,
    pub access_count: u64,
    pub metadata_change_count: u64,
    pub content_change_count: u64,
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
                 ON size_aggregates(parent_path);
             CREATE TABLE IF NOT EXISTS entry_snapshots (
                 path                  TEXT PRIMARY KEY,
                 identity_fingerprint  TEXT NOT NULL,
                 analysis_version      TEXT NOT NULL,
                 entry_json            TEXT NOT NULL,
                 classification_json   TEXT NOT NULL,
                 measurement_json      TEXT NOT NULL,
                 descriptor_json       TEXT NOT NULL,
                 source_hash           TEXT,
                 format                TEXT,
                 computation_kind      TEXT NOT NULL,
                 sources_json          TEXT NOT NULL,
                 first_seen            TEXT NOT NULL,
                 last_seen             TEXT NOT NULL,
                 last_accessed         TEXT NOT NULL,
                 access_count          INTEGER NOT NULL DEFAULT 0,
                 metadata_change_count INTEGER NOT NULL DEFAULT 0,
                 content_change_count  INTEGER NOT NULL DEFAULT 0
             );
             CREATE INDEX IF NOT EXISTS idx_snapshot_identity
                 ON entry_snapshots(path, identity_fingerprint, analysis_version);
             CREATE TABLE IF NOT EXISTS entry_access_history (
                 id                    INTEGER PRIMARY KEY AUTOINCREMENT,
                 path                  TEXT NOT NULL,
                 observed_at           TEXT NOT NULL,
                 cache_hit             INTEGER NOT NULL,
                 identity_fingerprint  TEXT NOT NULL,
                 scan_root             TEXT
             );
             CREATE INDEX IF NOT EXISTS idx_access_path_ts
                 ON entry_access_history(path, observed_at DESC);",
        )
    }

    /// Cheap identity for change detection. Files include nanosecond metadata;
    /// directories also include their immediate child names and stat metadata,
    /// catching membership and common direct-child changes without a deep walk.
    pub fn identity_fingerprint(entry: &FsEntry) -> String {
        let mut hasher = Sha256::new();
        hasher.update(b"gls-fs-identity-v1\0");
        hasher.update(entry.path.to_string_lossy().as_bytes());
        if let Ok(meta) = std::fs::symlink_metadata(&entry.path) {
            hash_metadata(&mut hasher, &meta);
        } else {
            hasher.update(b"missing");
        }
        if entry.path.is_dir() {
            let mut children = std::fs::read_dir(&entry.path)
                .ok()
                .into_iter()
                .flatten()
                .flatten()
                .collect::<Vec<_>>();
            children.sort_by_key(|child| child.file_name());
            for child in children {
                hasher.update(child.file_name().to_string_lossy().as_bytes());
                if let Ok(meta) = child.metadata() {
                    hash_metadata(&mut hasher, &meta);
                }
            }
        }
        hex::encode(hasher.finalize())
    }

    /// Load a snapshot only when both filesystem identity and the analysis
    /// schema match. Every lookup records aggregate and append-only access
    /// history, including misses.
    pub fn lookup_snapshot(
        &self,
        entry: &FsEntry,
        analysis_version: &str,
        scan_root: &Path,
    ) -> Option<CachedSnapshot> {
        let fingerprint = Self::identity_fingerprint(entry);
        let now = Utc::now().to_rfc3339();
        let path = entry.path.to_string_lossy().to_string();
        let root = scan_root.to_string_lossy().to_string();
        let conn = self.conn.lock().unwrap();

        let row = conn.query_row(
            "SELECT entry_json, classification_json, measurement_json,
                    descriptor_json, first_seen, last_seen, last_accessed,
                    access_count, metadata_change_count, content_change_count
             FROM entry_snapshots
             WHERE path = ?1 AND identity_fingerprint = ?2 AND analysis_version = ?3",
            params![path, fingerprint, analysis_version],
            |row| {
                Ok((
                    row.get::<_, String>(0)?, row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?, row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?, row.get::<_, String>(5)?,
                    row.get::<_, String>(6)?, row.get::<_, i64>(7)?,
                    row.get::<_, i64>(8)?, row.get::<_, i64>(9)?,
                ))
            },
        ).ok();
        let hit = row.is_some();

        let _ = conn.execute(
            "INSERT INTO entry_access_history
             (path, observed_at, cache_hit, identity_fingerprint, scan_root)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![path, now, hit as i32, fingerprint, root],
        );
        if hit {
            let _ = conn.execute(
                "UPDATE entry_snapshots SET last_seen = ?2, last_accessed = ?2,
                    access_count = access_count + 1 WHERE path = ?1",
                params![path, now],
            );
        }

        let (entry_json, classification_json, measurement_json, descriptor_json,
             first_seen, last_seen, last_accessed, access_count,
             metadata_change_count, content_change_count) = row?;
        let descriptor: Descriptor = serde_json::from_str(&descriptor_json).ok()?;
        let (context, metrics) = latest_context_locked(&conn, &entry.path, &descriptor.descriptor_hash);
        Some(CachedSnapshot {
            entry: serde_json::from_str(&entry_json).ok()?,
            classification: serde_json::from_str(&classification_json).ok()?,
            measurement: serde_json::from_str(&measurement_json).ok()?,
            descriptor,
            context,
            metrics,
            first_seen: parse_time(&first_seen),
            last_seen: parse_time(&last_seen),
            last_accessed: parse_time(&last_accessed),
            access_count: access_count as u64 + 1,
            metadata_change_count: metadata_change_count as u64,
            content_change_count: content_change_count as u64,
        })
    }

    /// Upsert deterministic metadata from DISCOVER, TYPE, BART and INDEX.
    pub fn store_snapshot(
        &self,
        entry: &FsEntry,
        classification: &Classification,
        measurement: &PhysicalMeasure,
        descriptor: &Descriptor,
        analysis_version: &str,
    ) -> Result<(), String> {
        let fingerprint = Self::identity_fingerprint(entry);
        let now = Utc::now().to_rfc3339();
        let path = entry.path.to_string_lossy().to_string();
        let format = entry.path.extension().map(|ext| ext.to_string_lossy().to_lowercase());
        let entry_json = serde_json::to_string(entry).map_err(|e| e.to_string())?;
        let classification_json = serde_json::to_string(classification).map_err(|e| e.to_string())?;
        let measurement_json = serde_json::to_string(measurement).map_err(|e| e.to_string())?;
        let descriptor_json = serde_json::to_string(descriptor).map_err(|e| e.to_string())?;
        let sources_json = serde_json::to_string(&["discover", "gls-type", "bart", "gls-index"])
            .map_err(|e| e.to_string())?;
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO entry_snapshots
             (path, identity_fingerprint, analysis_version, entry_json,
              classification_json, measurement_json, descriptor_json,
              source_hash, format, computation_kind, sources_json, first_seen,
              last_seen, last_accessed, access_count, metadata_change_count,
              content_change_count)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,'deterministic',?10,?11,?11,?11,0,0,0)
             ON CONFLICT(path) DO UPDATE SET
                 identity_fingerprint = excluded.identity_fingerprint,
                 analysis_version = excluded.analysis_version,
                 entry_json = excluded.entry_json,
                 classification_json = excluded.classification_json,
                 measurement_json = excluded.measurement_json,
                 descriptor_json = excluded.descriptor_json,
                 source_hash = excluded.source_hash,
                 format = excluded.format,
                 sources_json = excluded.sources_json,
                 last_seen = excluded.last_seen,
                 last_accessed = excluded.last_accessed,
                 metadata_change_count = entry_snapshots.metadata_change_count +
                     CASE WHEN entry_snapshots.identity_fingerprint <> excluded.identity_fingerprint THEN 1 ELSE 0 END,
                 content_change_count = entry_snapshots.content_change_count +
                     CASE WHEN COALESCE(entry_snapshots.source_hash, '') <> COALESCE(excluded.source_hash, '') THEN 1 ELSE 0 END",
            params![path, fingerprint, analysis_version, entry_json, classification_json,
                    measurement_json, descriptor_json, descriptor.source_hash, format,
                    sources_json, now],
        ).map_err(|e| e.to_string())?;
        Ok(())
    }

    /// Retrieve the latest natural-language context whose descriptor still
    /// matches the current deterministic analysis.
    pub fn cached_context(
        &self,
        path: &Path,
        descriptor_hash: &str,
    ) -> Option<(ContextAnnotation, Option<MetricBundle>)> {
        let conn = self.conn.lock().unwrap();
        let (context, metrics) = latest_context_locked(&conn, path, descriptor_hash);
        context.map(|annotation| (annotation, metrics))
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
                    PipelineEvent::CacheStatus { path, hit: true } => {
                        // Hydrated events that follow are display-only. Marking
                        // the path persisted prevents duplicate revisions.
                        persisted.insert(path);
                    }
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

fn parse_time(value: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(value)
        .map(|time| time.with_timezone(&Utc))
        .unwrap_or_else(|_| Utc::now())
}

fn hash_metadata(hasher: &mut Sha256, metadata: &std::fs::Metadata) {
    hasher.update(metadata.len().to_le_bytes());
    hasher.update([metadata.is_file() as u8, metadata.is_dir() as u8]);
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        hasher.update(metadata.dev().to_le_bytes());
        hasher.update(metadata.ino().to_le_bytes());
        hasher.update(metadata.mode().to_le_bytes());
        hasher.update(metadata.mtime().to_le_bytes());
        hasher.update(metadata.mtime_nsec().to_le_bytes());
        hasher.update(metadata.ctime().to_le_bytes());
        hasher.update(metadata.ctime_nsec().to_le_bytes());
    }
    #[cfg(not(unix))]
    {
        if let Ok(modified) = metadata.modified() {
            if let Ok(duration) = modified.duration_since(std::time::UNIX_EPOCH) {
                hasher.update(duration.as_nanos().to_le_bytes());
            }
        }
        hasher.update([metadata.permissions().readonly() as u8]);
    }
}

fn latest_context_locked(
    conn: &Connection,
    path: &Path,
    descriptor_hash: &str,
) -> (Option<ContextAnnotation>, Option<MetricBundle>) {
    let row = conn.query_row(
        "SELECT source_hash, descriptor_hash, model, provider, context_version,
                summary, purpose, role, relationships, metrics_json, timestamp
         FROM context_revisions
         WHERE path = ?1 AND descriptor_hash = ?2
         ORDER BY timestamp DESC LIMIT 1",
        params![path.to_string_lossy().as_ref(), descriptor_hash],
        |row| {
            Ok((
                row.get::<_, Option<String>>(0)?, row.get::<_, String>(1)?,
                row.get::<_, String>(2)?, row.get::<_, String>(3)?,
                row.get::<_, String>(4)?, row.get::<_, String>(5)?,
                row.get::<_, Option<String>>(6)?, row.get::<_, Option<String>>(7)?,
                row.get::<_, String>(8)?, row.get::<_, Option<String>>(9)?,
                row.get::<_, String>(10)?,
            ))
        },
    ).ok();

    let Some((source_hash, descriptor_hash, model, provider, context_version,
              summary, purpose, role, relationships_json, metrics_json, timestamp)) = row else {
        return (None, None);
    };
    let annotation = ContextAnnotation {
        path: path.to_path_buf(),
        summary,
        purpose,
        role,
        relationships: serde_json::from_str(&relationships_json).unwrap_or_default(),
        provenance: gls_core::provenance::ContextProvenance {
            context_version,
            model,
            provider,
            descriptor_hash,
            source_hash,
            timestamp: parse_time(&timestamp),
            // Older Padagonia schemas did not store this field. The cached
            // text remains valid; confidence is conservatively unknown.
            confidence: 0.0,
        },
    };
    let metrics = metrics_json.and_then(|json| serde_json::from_str(&json).ok());
    (Some(annotation), metrics)
}

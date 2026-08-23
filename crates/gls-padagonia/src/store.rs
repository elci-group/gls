//! SQLite-backed Padagonia store.

use gls_core::event::{ContextAnnotation, Descriptor, MetricBundle, PipelineEvent};
use rusqlite::{params, Connection};
use serde_json;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;

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
        let p = Padagonia { conn: Arc::new(Mutex::new(conn)) };
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
                 ON context_revisions(path, timestamp DESC);",
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
        let metrics_json = metrics
            .map(|m| serde_json::to_string(m).unwrap_or_default());
        let descriptor_json = descriptor
            .map(|d| serde_json::to_string(d).unwrap_or_default());

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

    /// Consume a pipeline broadcast receiver and persist every relevant event.
    pub async fn consume_stream(
        self,
        mut rx: broadcast::Receiver<PipelineEvent>,
    ) {
        // We collect pending annotations/descriptors/metrics to assemble full
        // records before persisting.
        use std::collections::HashMap;
        let mut annotations: HashMap<PathBuf, ContextAnnotation> = HashMap::new();
        let mut descriptors: HashMap<PathBuf, Descriptor> = HashMap::new();
        let mut metrics: HashMap<PathBuf, MetricBundle> = HashMap::new();

        loop {
            match rx.recv().await {
                Ok(event) => match event {
                    PipelineEvent::Indexed(d) => {
                        descriptors.insert(d.path.clone(), d);
                    }
                    PipelineEvent::ContextUpdated(a) => {
                        let path = a.path.clone();
                        annotations.insert(path.clone(), a);
                        // If we have everything, persist immediately.
                        if let Some(ann) = annotations.get(&path) {
                            let desc = descriptors.get(&path);
                            let met = metrics.get(&path);
                            if let Err(e) = self.persist(ann, desc, met) {
                                eprintln!("padagonia persist error: {e}");
                            }
                        }
                    }
                    PipelineEvent::MetricsUpdated(m) => {
                        metrics.insert(m.path.clone(), m);
                    }
                    PipelineEvent::PipelineComplete => break,
                    _ => {}
                },
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    eprintln!("padagonia: lagged {n} events");
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            }
        }
    }
}

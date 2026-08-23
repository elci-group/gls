//! Deterministic classification combining extension, magic bytes, filename
//! patterns, and directory-level context signals.

use gls_core::{
    entry::{EntryKind, FsEntry},
    event::PipelineEvent,
    semantic::{Classification, SemanticType},
};
use std::path::Path;
use tokio::sync::broadcast;

// ── Public surface ──────────────────────────────────────────────────────────

/// Classify a batch of discovered entries and emit `TypeDetected` events.
pub async fn classify_entry(
    entries: Vec<FsEntry>,
    tx: broadcast::Sender<PipelineEvent>,
) {
    let results = tokio::task::spawn_blocking(move || {
        entries.into_iter().map(|e| {
            let cls = classify_fs_entry(&e);
            (e.path.clone(), cls)
        }).collect::<Vec<_>>()
    })
    .await
    .unwrap_or_default();

    for (path, classification) in results {
        let _ = tx.send(PipelineEvent::TypeDetected { path, classification });
    }
}

/// Classify a single path (convenience wrapper used by tests and the CLI).
pub fn classify_path(path: &Path) -> Classification {
    let basename = path
        .file_name()
        .map(|n| n.to_string_lossy().to_lowercase())
        .unwrap_or_default();
    let ext = path
        .extension()
        .map(|e| e.to_string_lossy().to_lowercase())
        .unwrap_or_default();
    let is_dir = path.is_dir();

    classify_by_signals(&basename, &ext, is_dir, path)
}

// ── Internal ────────────────────────────────────────────────────────────────

fn classify_fs_entry(entry: &FsEntry) -> Classification {
    let basename = entry.basename.to_lowercase();
    let ext = entry.path
        .extension()
        .map(|e| e.to_string_lossy().to_lowercase())
        .unwrap_or_default();
    let is_dir = entry.kind == EntryKind::Directory;
    classify_by_signals(&basename, &ext, is_dir, &entry.path)
}

fn classify_by_signals(
    basename: &str,
    ext: &str,
    is_dir: bool,
    path: &Path,
) -> Classification {
    let mut evidence = Vec::new();

    // ── Directories ──────────────────────────────────────────────────────────
    if is_dir {
        // Named build artefact directories.
        if matches!(basename, "target" | "node_modules" | ".git" | "dist" | "build" | "__pycache__") {
            evidence.push(format!("directory name `{basename}` is a known build/cache directory"));
            return cls(SemanticType::BuildArtefact, 0.95, evidence, None);
        }
        if matches!(basename, "tests" | "test" | "spec" | "__tests__") {
            evidence.push(format!("directory name `{basename}` is a test directory convention"));
            return cls(SemanticType::TestSuite, 0.90, evidence, None);
        }
        evidence.push("entry is a directory".to_string());
        return cls(SemanticType::Directory, 1.0, evidence, None);
    }

    // ── Symlinks ─────────────────────────────────────────────────────────────
    if path.is_symlink() {
        evidence.push("symlink detected via filesystem metadata".to_string());
        return cls(SemanticType::Symlink, 1.0, evidence, None);
    }

    // ── Credentials / secrets ────────────────────────────────────────────────
    if matches!(ext, "pem" | "key" | "pfx" | "p12" | "crt" | "cer")
        || basename.contains("secret")
        || basename.contains("credential")
        || basename.contains("private")
        || basename == ".env"
        || basename.starts_with(".env.")
    {
        evidence.push("filename/extension matches credential patterns".to_string());
        return cls(SemanticType::Credential, 0.85, evidence, None);
    }

    // ── Archives ─────────────────────────────────────────────────────────────
    if matches!(ext, "zip" | "tar" | "gz" | "bz2" | "xz" | "zst" | "7z" | "rar" | "tgz") {
        evidence.push(format!("extension `.{ext}` is an archive format"));
        return cls(SemanticType::Archive, 0.97, evidence, None);
    }

    // ── Images ───────────────────────────────────────────────────────────────
    if matches!(ext, "png" | "jpg" | "jpeg" | "gif" | "webp" | "svg" | "ico" | "bmp" | "tiff") {
        evidence.push(format!("extension `.{ext}` is an image format"));
        return cls(SemanticType::Image, 0.97, evidence, None);
    }

    // ── Video ────────────────────────────────────────────────────────────────
    if matches!(ext, "mp4" | "mkv" | "avi" | "mov" | "webm" | "flv" | "wmv") {
        evidence.push(format!("extension `.{ext}` is a video format"));
        return cls(SemanticType::Video, 0.97, evidence, None);
    }

    // ── Audio ────────────────────────────────────────────────────────────────
    if matches!(ext, "mp3" | "wav" | "flac" | "ogg" | "aac" | "m4a" | "opus") {
        evidence.push(format!("extension `.{ext}` is an audio format"));
        return cls(SemanticType::Audio, 0.97, evidence, None);
    }

    // ── Databases ────────────────────────────────────────────────────────────
    if matches!(ext, "db" | "sqlite" | "sqlite3" | "mdb" | "accdb" | "duckdb") {
        evidence.push(format!("extension `.{ext}` is a database format"));
        return cls(SemanticType::Database, 0.95, evidence, None);
    }

    // ── Datasets ─────────────────────────────────────────────────────────────
    if matches!(ext, "csv" | "tsv" | "parquet" | "arrow" | "ndjson" | "jsonl") {
        evidence.push(format!("extension `.{ext}` is a data format"));
        return cls(SemanticType::Dataset, 0.90, evidence, None);
    }

    // ── Configuration ────────────────────────────────────────────────────────
    let (config_type, config_lang) = config_type(basename, ext);
    if let Some(lang) = config_lang {
        evidence.push(format!("filename/extension matches configuration pattern for `{lang}`"));
        return cls(config_type, 0.90, evidence, Some(lang));
    }

    // ── Documentation ────────────────────────────────────────────────────────
    if matches!(ext, "md" | "mdx" | "rst" | "txt" | "adoc" | "tex" | "wiki")
        || matches!(
            basename,
            "readme" | "changelog" | "license" | "contributing" | "authors"
                | "credits" | "notice" | "copying"
        )
        || basename.starts_with("readme")
        || basename.starts_with("changelog")
    {
        let lang = doc_language(ext);
        evidence.push("extension/name matches documentation pattern".to_string());
        return cls(SemanticType::Documentation, 0.90, evidence, lang);
    }

    // ── Executables ──────────────────────────────────────────────────────────
    if matches!(ext, "sh" | "bash" | "zsh" | "fish" | "ps1" | "bat" | "cmd") {
        evidence.push(format!("extension `.{ext}` is a shell/script format"));
        return cls(SemanticType::Executable, 0.90, evidence, Some(ext.to_string()));
    }

    // ── Source code ──────────────────────────────────────────────────────────
    let (sc_type, lang) = source_code_type(ext);
    if let Some(lang) = lang {
        evidence.push(format!("extension `.{ext}` maps to language `{lang}`"));
        return cls(sc_type, 0.90, evidence, Some(lang));
    }

    // ── Magic bytes (limited set) ────────────────────────────────────────────
    if let Some(cls) = try_magic(path, &mut evidence) {
        return cls;
    }

    evidence.push("no deterministic signal matched".to_string());
    cls(SemanticType::Unknown, 0.30, evidence, None)
}

// ── Helpers ──────────────────────────────────────────────────────────────────

fn cls(
    semantic_type: SemanticType,
    confidence: f32,
    evidence: Vec<String>,
    language: Option<String>,
) -> Classification {
    Classification {
        semantic_type,
        confidence,
        evidence,
        language,
    }
}

fn config_type(basename: &str, ext: &str) -> (SemanticType, Option<String>) {
    match (basename, ext) {
        ("cargo.toml", _) | ("cargo.lock", _) => (SemanticType::Configuration, Some("Rust/Cargo".into())),
        ("package.json", _) | ("package-lock.json", _) | ("yarn.lock", _) | ("pnpm-lock.yaml", _) =>
            (SemanticType::Configuration, Some("Node.js".into())),
        ("pyproject.toml", _) | ("setup.cfg", _) | ("setup.py", _) | ("requirements.txt", _) =>
            (SemanticType::Configuration, Some("Python".into())),
        ("go.mod", _) | ("go.sum", _) => (SemanticType::Configuration, Some("Go".into())),
        ("dockerfile", _) | ("docker-compose.yml", _) | ("docker-compose.yaml", _) =>
            (SemanticType::Configuration, Some("Docker".into())),
        (".gitignore", _) | (".gitattributes", _) | (".gitmodules", _) =>
            (SemanticType::Configuration, Some("Git".into())),
        (".editorconfig", _) | (".prettierrc", _) | (".eslintrc", _) =>
            (SemanticType::Configuration, Some("editor/linter".into())),
        _ if matches!(ext, "toml" | "yaml" | "yml" | "json" | "ini" | "cfg" | "conf" | "env") =>
            (SemanticType::Configuration, Some(ext.to_uppercase())),
        _ => (SemanticType::Unknown, None),
    }
}

fn source_code_type(ext: &str) -> (SemanticType, Option<String>) {
    match ext {
        "rs" => (SemanticType::SourceCode, Some("Rust".into())),
        "py" => (SemanticType::SourceCode, Some("Python".into())),
        "ts" | "tsx" => (SemanticType::SourceCode, Some("TypeScript".into())),
        "js" | "mjs" | "cjs" | "jsx" => (SemanticType::SourceCode, Some("JavaScript".into())),
        "go" => (SemanticType::SourceCode, Some("Go".into())),
        "c" | "h" => (SemanticType::SourceCode, Some("C".into())),
        "cpp" | "cxx" | "cc" | "hpp" => (SemanticType::SourceCode, Some("C++".into())),
        "java" => (SemanticType::SourceCode, Some("Java".into())),
        "kt" | "kts" => (SemanticType::SourceCode, Some("Kotlin".into())),
        "swift" => (SemanticType::SourceCode, Some("Swift".into())),
        "zig" => (SemanticType::SourceCode, Some("Zig".into())),
        "ex" | "exs" => (SemanticType::SourceCode, Some("Elixir".into())),
        "erl" => (SemanticType::SourceCode, Some("Erlang".into())),
        "hs" => (SemanticType::SourceCode, Some("Haskell".into())),
        "lua" => (SemanticType::SourceCode, Some("Lua".into())),
        "rb" => (SemanticType::SourceCode, Some("Ruby".into())),
        "php" => (SemanticType::SourceCode, Some("PHP".into())),
        "cs" => (SemanticType::SourceCode, Some("C#".into())),
        "sql" => (SemanticType::SourceCode, Some("SQL".into())),
        "html" | "htm" => (SemanticType::SourceCode, Some("HTML".into())),
        "css" | "scss" | "sass" | "less" => (SemanticType::SourceCode, Some("CSS".into())),
        _ => (SemanticType::Unknown, None),
    }
}

fn doc_language(ext: &str) -> Option<String> {
    match ext {
        "md" | "mdx" => Some("Markdown".into()),
        "rst" => Some("reStructuredText".into()),
        "tex" => Some("LaTeX".into()),
        "adoc" => Some("AsciiDoc".into()),
        _ => None,
    }
}

/// Attempt simple magic-byte detection.  Returns `None` if the file cannot
/// be read or no magic is recognised.
fn try_magic(path: &Path, evidence: &mut Vec<String>) -> Option<Classification> {
    let buf = {
        let mut f = std::fs::File::open(path).ok()?;
        let mut buf = [0u8; 16];
        use std::io::Read;
        f.read(&mut buf).ok()?;
        buf
    };

    // ELF binary
    if &buf[..4] == b"\x7fELF" {
        evidence.push("magic bytes: ELF binary".into());
        return Some(cls(SemanticType::Executable, 0.99, evidence.clone(), None));
    }
    // PE / COFF (Windows)
    if &buf[..2] == b"MZ" {
        evidence.push("magic bytes: PE/COFF binary".into());
        return Some(cls(SemanticType::Executable, 0.99, evidence.clone(), None));
    }
    // Mach-O
    if &buf[..4] == b"\xcf\xfa\xed\xfe" || &buf[..4] == b"\xce\xfa\xed\xfe" {
        evidence.push("magic bytes: Mach-O binary".into());
        return Some(cls(SemanticType::Executable, 0.99, evidence.clone(), None));
    }

    None
}

# Glass (`gls`)

**Next-generation semantic filesystem observer.**

`ls` shows filesystem topology. `gls` progressively reveals filesystem *meaning*.

---

## Overview

`gls` is a progressive, animated, metadata-driven filesystem intelligence surface.

A normal invocation is immediately responsive: filesystem names appear first,
then each object progressively acquires type, physical, structural, and contextual
metadata as it is understood.

```text
📁 src/
📄 Cargo.toml
📄 README.md
📁 tests/
📄 LICENSE
```

As each item is understood, its display cycles between identity and metadata:

```text
📄 README.md
   📝 documentation · 8.4 KB
   completeness 91% · quality 87%
   "Project documentation and usage guide"
```

The effect: **looking through a glass that gradually comes into focus.**

---

## Pipeline

```text
DISCOVER → TYPE → BART → INDEX → BOUND → CONTEXT → METRICS
```

| Phase    | Description |
|----------|-------------|
| DISCOVER | Cheap deterministic filesystem enumeration |
| TYPE     | Semantic classification by extension, magic bytes, filename |
| BART     | Physical/storage analysis: size, entropy, reclaimable space |
| INDEX    | Compact descriptor generation for inference |
| BOUND    | Scope and inference-budget control |
| CONTEXT  | Groq-backed semantic contextualisation |
| METRICS  | Structured completeness / quality / accuracy metrics |

All phases emit `PipelineEvent` values onto a broadcast channel.
The renderer and the Padagonia persistence layer each consume an independent
clone of the same stream.

---

## Usage

```bash
# Inspect current directory
gls

# Inspect a specific path
gls src/

# Deep scan with full contextualisation
gls --deep .

# No AI contextualisation (fast, deterministic only)
gls --no-context .

# Machine-readable JSON output
gls --output json .

# Plain text (no animation)
gls --no-animate .

# History mode
gls --history .
```

---

## Architecture

```
filesystem
     │
     ▼
analysis workers
     │
     ▼
event stream (broadcast channel)
     │
 ┌───┴────┐
 ▼        ▼
renderer  Padagonia
(3form)   (SQLite persistence)
```

### Crates

| Crate | Role |
|-------|------|
| `gls-core` | Domain types, pipeline event stream, presentation state machine |
| `gls-discover` | DISCOVER phase — async filesystem walk |
| `gls-type` | TYPE phase — deterministic semantic classification |
| `gls-bart` | BART phase — physical/storage analysis |
| `gls-index` | INDEX phase — compact descriptor generation |
| `gls-bound` | BOUND phase — scope and budget control |
| `gls-context` | CONTEXT phase — Groq semantic contextualisation |
| `gls-metrics` | METRICS phase — structured metric objects |
| `gls-padagonia` | Persistent contextual history (SQLite) |
| `gls-render` | Animation engine and 3form renderer |
| `bin/gls` | CLI binary |

---

## Padagonia

Context is not ephemeral. Every contextualised object receives a persistent
Padagonia record. When a file changes, a new revision is appended — not
overwritten — enabling historical queries:

- *How has this file's perceived purpose changed?*
- *When did its quality decline?*
- *What contextualisation changed after this commit?*

Padagonia is stored at `~/.local/share/gls/padagonia.db`.

---

## Performance Contract

> `gls` must feel as immediate as `ls`, even when its final understanding is
> substantially more expensive.

```
T0      names
T+      deterministic metadata
T++     semantic classification
T+++    structural analysis
T++++   contextualisation
T+++++ valuations
```

Expensive stages happen *after* useful output has already appeared.
Padagonia makes subsequent executions increasingly intelligent rather than
increasingly expensive.

---

## Building

```bash
cargo build --release
```

The binary is at `target/release/gls`.

---

## Environment

| Variable | Purpose |
|----------|---------|
| `GROQ_API_KEY` | Groq API key for contextualisation (optional; disables CONTEXT phase if absent) |

---

## License

MIT

# ADR 0003: redb metadata store

## Status

Accepted for Stage 2 onward.

## Context

Diavasi needs an embedded, crash-safe store for connection definitions, group definitions, and committed cursors. Candidates considered:

| Backend | Notes | Verdict |
| --- | --- | --- |
| [redb](https://github.com/cberner/redb) | Pure Rust, ACID, MVCC, crash-safe by default, stable file format, MIT/Apache-2.0 | **Chosen** |
| [sled](https://sled.rs) | Embedded BTreeMap API; weaker “boring production metadata” posture vs redb’s explicit stable status | Rejected for Stage 2 |
| [native_db](https://github.com/vincent-herlemont/native_db) | Typed models on redb; upstream API not stable | Rejected |
| SQLite | Extremely mature; introduces a C dependency via bundled rusqlite | Deferred behind `StateStore` |

## Decision

- Persist metadata with **redb** behind a narrow `StateStore` trait.
- Tables: `meta` (schema version), `connections`, `groups`, `checkpoints`.
- Values are JSON for inspectability in tests and early ops.
- Durability point is successful return from `write_txn.commit()`. redb is crash-safe by default; Stage 2 does not weaken durability settings.
- Persist **group definition + committed cursor** only. Do not persist payloads, buffer contents, fetched cursor, or in-flight assignments.
- Seal connection secrets with ChaCha20-Poly1305 under `DIAVASI_STORE_KEY` (32-byte hex). List/show paths must not open secrets; adapters use `open_secret` explicitly later.

## Consequences

- Process kill after a successful `commit_progress` / `commit_checkpoint` resumes at that committed cursor.
- Kill before commit resumes at the previous cursor and may **replay** (at-least-once).
- A future SQLite (or other) backend can implement `StateStore` without changing `DurableGroup`.
- Schema version is stored in-band; unknown newer versions are refused.

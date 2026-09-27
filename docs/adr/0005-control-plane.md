# ADR 0005: HTTP control plane and CLI

## Status

Accepted for Stage 4 onward.

## Context

Stage 3 exposed group supervision only in-process (`GroupSupervisor` / `GroupHandle`). Operators need a versioned HTTP API and a scriptable CLI to create connections and groups, start and observe them, and drain, stop, and delete them, without embedding the runtime in every tool.

Secrets on connections must never round-trip in plaintext after create. The engine has no `Paused` lifecycle state; control verbs map onto existing runtime operations and lifecycle states.

## Decision

- **axum** serves `/v1` on a local bind address. Auth is shared-secret **Bearer** token (`Authorization: Bearer …`) on all routes except `GET /health` and `GET /ready`. Validator trait `AuthValidator` with `BearerTokenAuth` leaves room for later plugins; no RBAC in Stage 4.
- **`ControlService`** wraps `Arc<Mutex<GroupSupervisor<RedbStore>>>` + `Arc<RedbStore>` + `StoreKey`. A background task polls `supervise_once`.
- **Lifecycle verb mapping:**
  - `start` / `resume` → `GroupSupervisor::start_group`
  - `pause` → graceful `stop_group`: the group records `Stopped`, snapshots, and exits; the definition remains.
  - `drain` → `GroupHandle::drain` (Running → Draining). A draining group reads nothing new and accepts no new consumers. Records already fetched are still delivered and acked. When the buffer and the in-flight set are empty, the group records `Stopped` and its task exits with stop reason `drained`. Draining a group that is not running, or twice, is 409.
  - `delete` → fail if running; else `StateStore::delete_group`
- Connection create seals secrets with Stage 2 crypto; **list/show never return plaintext**.
- **`diavasi` CLI** is an HTTP client for admin commands; `diavasi serve` calls `diavasi::control::serve`. Output `--output text|json`.

## Consequences

- Full synthetic-group lifecycle is operable against a local server without Stage 5 data plane.
- Pause records `Stopped`. A server shutdown (SIGINT or SIGTERM) saves every group's progress and keeps its lifecycle, so groups that were `Running` or `Draining` resume when the server starts again.
- `GroupView.lifecycle` agrees with `running`: `Running` or `Draining` while running, `Recovering` while a failed group waits to restart, `Stopped` or `Failed` otherwise.
- Status codes: invalid input is 400, a missing group or connection is 404, a verb that does not fit the group's state (start of a running group aside, which is a no-op) or a duplicate id is 409, and a group too busy to answer is 503.
- Ids are 1 to 128 characters from `A-Z a-z 0-9 . _ - :`. `batch_timeout_ms` is at least 100. `max_buffer_bytes` is at least 1. `batch_max_records` does not exceed `max_buffer_records`. A connection that a group uses cannot be deleted.
- Production auth (mTLS, OIDC, RBAC) is deferred; bearer is for local/dev and early ops only.
- `GET /metrics` is Prometheus text as of Stage 12. See [ADR 0013](0013-observability.md).

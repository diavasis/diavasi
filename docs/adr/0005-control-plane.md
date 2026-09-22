# ADR 0005: HTTP control plane and CLI

## Status

Accepted for Stage 4 onward.

## Context

Stage 3 exposed group supervision only in-process (`GroupSupervisor` / `GroupHandle`). Operators need a versioned HTTP API and a scriptable CLI to create connections/groups, start and observe them, and drain/stop/delete — without embedding the runtime in every tool.

Secrets on connections must never round-trip in plaintext after create. The engine has no `Paused` lifecycle state; control verbs must map onto existing runtime operations.

## Decision

- **axum** serves `/v1` on a local bind address. Auth is shared-secret **Bearer** token (`Authorization: Bearer …`) on all routes except `GET /health`. Validator trait `AuthValidator` with `BearerTokenAuth` leaves room for later plugins; no RBAC in Stage 4.
- **`ControlService`** wraps `Arc<Mutex<GroupSupervisor<RedbStore>>>` + `Arc<RedbStore>` + `StoreKey`. A background task polls `supervise_once`.
- **Lifecycle verb mapping:**
  - `start` / `resume` → `GroupSupervisor::start_group`
  - `pause` → graceful `stop_group` (snapshot + exit); definition remains; shown as not running
  - `drain` → `GroupHandle::drain` (Running → Draining)
  - `delete` → fail if running; else `StateStore::delete_group`
- Connection create seals secrets with Stage 2 crypto; **list/show never return plaintext**.
- **`diavasi` CLI** is an HTTP client for admin commands; `diavasi serve` calls `diavasi::control::serve`. Output `--output text|json`.

## Consequences

- Full synthetic-group lifecycle is operable against a local server without Stage 5 data plane.
- Pause is not a durable engine state; “paused” means not in the supervisor’s running set.
- Production auth (mTLS, OIDC, RBAC) is deferred; bearer is for local/dev and early ops only.
- Metrics endpoint is a stub until a real scrape surface exists.

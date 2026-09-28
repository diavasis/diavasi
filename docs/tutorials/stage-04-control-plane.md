# Tutorial: Stage 4 control plane and CLI

This tutorial teaches Stage 4 Diavasi: the axum `/v1` control plane, bearer auth, secret-safe connections, and driving a synthetic group lifecycle with the `diavasi` executable against a local `diavasi serve`.

Related code: `crates/diavasi-core/src/control/`, `crates/diavasi/`. Decision record: [ADR 0005](../adr/0005-control-plane.md). Prior: [Stage 3](stage-03-group-runtime.md).

## 1. What Stage 4 is

Stage 4 adds:

- HTTP API under `/v1` (axum) with bearer auth
- `ControlService` over `GroupSupervisor` + `RedbStore`
- `diavasi serve`, plus `connection`, `group`, `consumer`, `checkpoint`, and `status` on the same executable

Still absent: production data plane (Stage 5), real DB adapters, RBAC.

**Acceptance:** create connection/group → start → observe status/checkpoint/consumers → drain → pause → resume → pause → delete, all via CLI against local `serve`.

## 2. Why this shape

### Control plane ≠ data plane

Admin CRUD and lifecycle stay on HTTP. Consumer assign/ack stay in-process until Stage 5. That keeps secrets and ops tooling separate from hot-path streaming.

### Pause is stop-runtime

The engine has no `Paused` state. CLI/HTTP `pause` maps to graceful `stop_group` (checkpoint snapshot, task exit). The group definition remains in redb; `resume` is `start_group` again.

### Secrets seal once

`connection add` sends a plaintext secret once. The server seals it (ChaCha20-Poly1305). `list` / `show` return `secret_sealed: true` and never the plaintext.

## 3. Core concepts

```text
diavasi CLI  --HTTP-->  axum /v1  -->  BearerAuth  -->  ControlService
                                                         |            |
                                                   GroupSupervisor  RedbStore
```

| Verb | Behavior |
| --- | --- |
| `group start` / `resume` | `start_group` |
| `group pause` | `stop_group` (not running) |
| `group drain` | `GroupHandle::drain` |
| `group delete` | reject if running; else delete from store |

## 4. Walkthrough

Build the CLI:

```bash
cargo build -p diavasi
export PATH="$PWD/target/debug:$PATH"
```

Start a local server (separate terminal):

```bash
mkdir -p /tmp/diavasi-s4
export DIAVASI_API_TOKEN=dev-token
export DIAVASI_STORE_KEY=0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef

diavasi serve \
  --bind 127.0.0.1:7700 \
  --store /tmp/diavasi-s4/meta.redb \
  --token "$DIAVASI_API_TOKEN" \
  --store-key "$DIAVASI_STORE_KEY"
```

Create a connection and group:

```bash
export DIAVASI_URL=http://127.0.0.1:7700

diavasi connection add \
  --id demo-pg \
  --kind postgres \
  --config-json '{"host":"localhost"}' \
  --secret 'never-echoed-again'

diavasi group create \
  --group-id demo \
  --total-records 100 \
  --connection-id demo-pg
```

Lifecycle:

```bash
diavasi group start demo
diavasi status
diavasi checkpoint show demo
diavasi consumer list demo
diavasi group drain demo
diavasi group pause demo
diavasi group resume demo
diavasi group pause demo
diavasi group delete demo
diavasi connection delete demo-pg
```

JSON output:

```bash
diavasi --output json status
diavasi --output json connection show demo-pg
```

## 5. Auth and health

- `GET /health`: no auth. The process is up.
- `GET /ready`: no auth. The store can be read.
- Every `/v1/*` route and `/metrics`: `Authorization: Bearer <token>`.
- Missing/wrong token → HTTP 401

## 6. What to remember

- Drive Stage 4 only through CLI/HTTP; do not poke the supervisor from apps.
- Pause ≠ engine state; it means the group is not running under the supervisor.
- Secrets are write-only after create.
- Stage 5 will add the networked data plane; Stage 4 does not stream records over HTTP.

# Stage 4 review: control plane and CLI

**Status:** complete — stop for review before Stage 5.

## Delivered

- Branch `v0.4.0/Control_interface-CLI`
- `diavasi::control`:
  - Bearer auth (`AuthValidator` / `BearerTokenAuth`)
  - DTOs + `ControlService` over supervisor + redb
  - axum `/v1` routes + `serve`
- `diavasi` CLI: `serve`, `connection`, `group`, `consumer`, `checkpoint`, `status`, `version`
- HTTP oneshot tests (auth, secret redaction, group lifecycle)
- CLI↔serve lifecycle acceptance test
- [ADR 0005](adr/0005-control-plane.md)
- Tutorial: [stage-04-control-plane.md](tutorials/stage-04-control-plane.md)

## Guarantees verified

- Unauthenticated `/v1` calls return 401; `/health` does not require a token.
- Connection secrets are sealed at rest and never returned by list/show.
- CLI can create connection/group, start, status, checkpoint, drain, pause, resume, delete against local `serve`.

## Intentionally not in Stage 4

- Production data plane (Stage 5)
- Real database adapters
- RBAC / OIDC / mTLS
- Real metrics scrape surface (stub only)

## Quality gate

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all --all-features
cargo deny check
```

## Stop

Stage 4 is done. Do not start Stage 5 without an explicit instruction.

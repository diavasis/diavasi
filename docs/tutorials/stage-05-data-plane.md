# Tutorial: Stage 5 data plane

This tutorial shows the frozen production data plane: TLS gRPC protocol v1, mapped onto a running synthetic group.

Related code: `crates/diavasi-core/src/dataplane/`, [diavasi-python](https://github.com/diavasis/diavasi-python) `diavasi_data/`, [diavasi-elixir](https://github.com/diavasis/diavasi-elixir) `lib/diavasi_data/`. Decisions: [ADR 0006](../adr/0006-data-plane-transport.md), [ADR 0007](../adr/0007-protocol-v1.md). Prior: [Stage 4](stage-04-control-plane.md).

## 1. What Stage 5 is

`diavasi serve` now hosts two listeners:

- Control plane: HTTP `/v1` (Stage 4)
- Data plane: TLS gRPC `diavasi.data.v1.DataPlane/Consume`

A client says hello, joins a running group, receives batches, and acks them. Flow control limits how many batches are outstanding. If the stream drops, unacked batches are requeued for another consumer.

Still absent: real database adapters (Stage 6) and polished SDKs (Stage 11).

## 2. Why gRPC and TLS

Stage 0 measured TCP a bit faster on localhost. gRPC over HTTP/2 with TLS is what we froze because it is ordinary HTTPS: proxies allow it, TLS is mature, and Rust, Python, and Elixir can all speak it. TCP remains the benchmark harness only.

## 3. Start the server

```bash
cargo build -p diavasi
export PATH="$PWD/target/debug:$PATH"
mkdir -p /tmp/diavasi-s5
export DIAVASI_API_TOKEN=dev-token
export DIAVASI_STORE_KEY=0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef

diavasi serve \
  --bind 127.0.0.1:7700 \
  --data-bind 127.0.0.1:7710 \
  --store /tmp/diavasi-s5/meta.redb \
  --token "$DIAVASI_API_TOKEN" \
  --store-key "$DIAVASI_STORE_KEY"
```

The first start writes:

- `/tmp/diavasi-s5/dataplane.crt` leaf certificate
- `/tmp/diavasi-s5/dataplane.key` private key
- `/tmp/diavasi-s5/dataplane-ca.crt` CA that clients trust

Create and start a synthetic group from another shell:

```bash
export DIAVASI_URL=http://127.0.0.1:7700
export DIAVASI_API_TOKEN=dev-token
export PATH="$PWD/target/debug:$PATH"

diavasi group create --group-id demo --total-records 20 --batch-max-records 5
diavasi group start demo
```

## 4. Consume it

Python (`diavasi-data` 0.1.0):

```bash
pip install diavasi-data==0.1.0
python -m diavasi_data \
  --addr 127.0.0.1:7710 \
  --ca /tmp/diavasi-s5/dataplane-ca.crt \
  --token "$DIAVASI_API_TOKEN" \
  --group demo \
  --consumer py1 \
  --total 20
```

Elixir (Hex `diavasi` 0.1.0, from a project that depends on it):

```bash
mix diavasi.consume \
  --addr 127.0.0.1:7710 \
  --ca /tmp/diavasi-s5/dataplane-ca.crt \
  --token "$DIAVASI_API_TOKEN" \
  --group demo \
  --consumer ex1 \
  --total 20
```

Both print a count of records and batches and exit 0. A second consumer can finish records the first one did not ack.

## 5. Session rules worth remembering

- Bearer token on every data-plane RPC. Missing or wrong token never joins.
- `Hello`, then `JoinGroup`. Other orders return `Error` and close.
- `FlowControl` sets how many batches may be unacked. The default is 1, so a slow client stalls assign while the buffer stays inside its cap.
- `Nack` is reserved and rejected.
- Disconnect, `Leave`, and heartbeat timeout all call `leave`, which requeues in-flight batches.

## 6. What this is not

Stage 5 does not read PostgreSQL. The group is still the synthetic source from Stages 1–3. Stage 6 is the first real adapter.

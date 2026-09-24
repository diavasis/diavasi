# Tutorial: Stage 10 ScyllaDB

This tutorial binds a group to one ScyllaDB partition and consumes it on the existing data plane.

Related code: `crates/diavasi-adapter-scylla/`. Decision: [ADR 0011](../adr/0011-scylla-query-contract.md). Contract notes: [scylla.md](../adapters/scylla.md). Prior: [Stage 9](stage-09-redis.md).

## 1. What Stage 10 is

`diavasi serve` installs a router. A group whose connection kind is `scylla` must carry `source_spec`. The owner task runs a prepared `SELECT` and `GroupEngine::ingest` pushes the page into the same buffer a Postgres group uses. Ack and checkpoint are unchanged.

A group with no `connection_id` stays synthetic. A connection kind the router does not know fails at create.

Still absent: an S3 adapter. The S3 adapter is a `RecordSource`, not a checkpoint store.

## 2. Table and order

```sql
CREATE KEYSPACE IF NOT EXISTS app WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
CREATE TABLE app.events (
  bucket int,
  id bigint,
  body text,
  PRIMARY KEY (bucket, id)
);
INSERT INTO app.events (bucket, id, body) VALUES (0, 1, 'a');
INSERT INTO app.events (bucket, id, body) VALUES (0, 2, 'b');
INSERT INTO app.events (bucket, id, body) VALUES (0, 3, 'c');
```

`bucket` is the partition key. `id` is the clustering column, ascending. `body` is payload. The read is that one partition. A second mode, `scan: "token"`, walks every partition in token order. The default is the partition read.

Compose publishes ScyllaDB on `127.0.0.1:9042` with no authentication. The container command is `--smp 1 --memory 1G --overprovisioned 1`.

## 3. Start the server

```bash
cargo build -p diavasi-cli
export PATH="$PWD/target/debug:$PATH"
mkdir -p /tmp/diavasi-s10
export DIAVASI_API_TOKEN=dev-token
export DIAVASI_STORE_KEY=0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef

diavasi serve \
  --bind 127.0.0.1:7700 \
  --store /tmp/diavasi-s10/meta.redb \
  --token "$DIAVASI_API_TOKEN" \
  --store-key "$DIAVASI_STORE_KEY"
```

## 4. Bind the group

```bash
export DIAVASI_URL=http://127.0.0.1:7700

diavasi connection add \
  --id app-scylla \
  --kind scylla \
  --config-json '{"host":"127.0.0.1","port":9042,"keyspace":"app","tls":"disable"}' \
  --secret unused

diavasi group create \
  --group-id events \
  --connection-id app-scylla \
  --ordering-contract scylla-partition \
  --source-json '{"table":"events","partition":{"bucket":0},"columns":["body"]}'

diavasi group start events
```

No `username` means the driver does not authenticate. The control plane still requires a non-empty secret, and that value is not sent to ScyllaDB. Set `username` when the server checks credentials; the secret is then the password.

`partition` must name every partition key. Omitting both `partition` and `scan` fails at create. A token walk uses `"scan":"token"` and does not set `partition`.

## 5. What resume means

An `INSERT` with a later clustering value shows up on a later fetch. An `INSERT` behind the committed cursor does not. `DELETE` of a row that has not been committed drops it. The paging cookie is not the checkpoint. Each fetch prepares nothing new: the statements from open run again with `LIMIT` set to the batch.

`diavasi test scylla -n 10000 -b 1024` seeds one partition (`bucket = 0`, clustering `id`, payload `body`) and prints seed time and consume throughput.

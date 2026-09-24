# Tutorial: Stage 6 PostgreSQL

This tutorial binds a group to a PostgreSQL table and consumes it on the Stage 5 data plane.

Related code: `crates/diavasi-adapter-postgres/`. Decision: [ADR 0008](../adr/0008-postgres-query-contract.md). Contract notes: [postgresql.md](../adapters/postgresql.md). Prior: [Stage 5](stage-05-data-plane.md).

## 1. What Stage 6 is

`diavasi serve` installs a Postgres source factory. A group whose connection kind is `postgres` must carry `source_spec`. The owner task fetches a keyset page and `GroupEngine::ingest` pushes it into the same buffer the synthetic source uses. Ack and checkpoint are unchanged.

A group with no `connection_id` stays synthetic. A connection kind the installed factory does not support fails at create.

Still absent after this stage: MongoDB, Redis, ScyllaDB, the Stage 7 benchmark, and a Docker Compose demo. Stage 8 later adds MongoDB.

## 2. Table and order

```sql
CREATE TABLE events (
  id bigint PRIMARY KEY,
  body text NOT NULL
);
INSERT INTO events (id, body) VALUES (1, 'a'), (2, 'b'), (3, 'c');
```

`id` is the order. `body` is the payload. The primary key is the unique index the create check requires.

## 3. Start the server

```bash
cargo build -p diavasi-cli
export PATH="$PWD/target/debug:$PATH"
mkdir -p /tmp/diavasi-s6
export DIAVASI_API_TOKEN=dev-token
export DIAVASI_STORE_KEY=0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef

diavasi serve \
  --bind 127.0.0.1:7700 \
  --data-bind 127.0.0.1:7710 \
  --store /tmp/diavasi-s6/meta.redb \
  --token "$DIAVASI_API_TOKEN" \
  --store-key "$DIAVASI_STORE_KEY"
```

## 4. Connection and group

```bash
export DIAVASI_URL=http://127.0.0.1:7700
export DIAVASI_API_TOKEN=dev-token

diavasi connection add \
  --id pg \
  --kind postgres \
  --config-json '{"host":"127.0.0.1","port":5433,"dbname":"diavasi","user":"diavasi","sslmode":"disable"}' \
  --secret 'diavasi'

diavasi group create \
  --group-id events \
  --connection-id pg \
  --ordering-contract postgres-keyset \
  --source-json '{"table":"events","order_by":[{"column":"id","type":"int8"}],"payload":["body"]}'

diavasi group start events
```

Create connects and checks the columns and the unique index. A missing index fails unless `acknowledge_unsafe` is true in the source JSON.

## 5. Consume

Use the Stage 5 client against `--data-bind`. Acked batches advance the committed cursor. Stopping the server and starting it again with the same store and key resumes after that cursor: rows already committed are not sent again, and rows still at or ahead of it are.

Inserts with a larger `id` show up on a later fetch. Inserts with a smaller `id` do not. Changing `body` on a row the cursor has passed does not send that row again. Changing `id` so the new key falls behind the cursor drops the row.

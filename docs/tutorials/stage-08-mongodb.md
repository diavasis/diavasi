# Tutorial: Stage 8 MongoDB

This tutorial binds a group to a MongoDB collection and consumes it on the existing data plane.

Related code: `crates/diavasi-adapter-mongodb/`. Decision: [ADR 0009](../adr/0009-mongodb-query-contract.md). Contract notes: [mongodb.md](../adapters/mongodb.md). Prior: [Stage 6](stage-06-postgres.md).

## 1. What Stage 8 is

`diavasi serve` installs a router. A group whose connection kind is `mongodb` must carry `source_spec`. The owner task runs a `find` keyset and `GroupEngine::ingest` pushes the page into the same buffer a Postgres group uses. Ack and checkpoint are unchanged.

A group with no `connection_id` stays synthetic. A connection kind the router does not know fails at create.

Still absent: Redis, ScyllaDB, and an S3 adapter.

## 2. Collection and order

```javascript
use app
db.events.createIndex({ id: 1 }, { unique: true })
db.events.insertMany([
  { id: NumberLong(1), body: "a" },
  { id: NumberLong(2), body: "b" },
  { id: NumberLong(3), body: "c" }
])
```

`id` is an `int64` sort key, ascending. `body` is part of the document payload. The unique index is what create checks. Omitting `order_by` instead sorts by `_id` ascending, which the built-in index already covers.

Compose publishes MongoDB on `127.0.0.1:27017` with no authentication.

## 3. Start the server

```bash
cargo build -p diavasi-cli
export PATH="$PWD/target/debug:$PATH"
mkdir -p /tmp/diavasi-s8
export DIAVASI_API_TOKEN=dev-token
export DIAVASI_STORE_KEY=0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef

diavasi serve \
  --bind 127.0.0.1:7700 \
  --data-bind 127.0.0.1:7710 \
  --store /tmp/diavasi-s8/meta.redb \
  --token "$DIAVASI_API_TOKEN" \
  --store-key "$DIAVASI_STORE_KEY"
```

## 4. Connection and group

```bash
export DIAVASI_URL=http://127.0.0.1:7700
export DIAVASI_API_TOKEN=dev-token

diavasi connection add \
  --id mongo \
  --kind mongodb \
  --config-json '{"host":"127.0.0.1","port":27017,"database":"app","tls":"disable"}' \
  --secret unused

diavasi group create \
  --group-id events \
  --connection-id mongo \
  --ordering-contract mongodb-find-keyset \
  --source-json '{"collection":"events","order_by":[{"field":"id","type":"int64","direction":"asc"}]}'

diavasi group start events
```

No `user` means the driver does not authenticate. The control plane still requires a non-empty secret, and that value is not sent to MongoDB. Set `user` when the server checks credentials; the secret is then the password.

Create connects and checks the unique index. A missing index fails unless `acknowledge_unsafe` is true. A descending field uses `"direction": "desc"`. A second sort field is another entry in `order_by`, and the unique index must list those fields in that order with those directions.

## 5. Consume

Use the Stage 5 client against `--data-bind`. Acked batches advance the committed cursor. Stopping the server and starting it again with the same store and key resumes after that cursor: documents already committed are not sent again, and documents still at or ahead of it are.

Inserts with a larger `id` show up on a later fetch. Inserts with a smaller `id` do not. Changing `body` on a document the cursor has passed does not send that document again. Changing `id` so the new key falls behind the cursor drops the document.

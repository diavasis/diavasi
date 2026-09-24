# Tutorial: Stage 9 Redis

This tutorial binds a group to a Redis stream and consumes it on the existing data plane.

Related code: `crates/diavasi-adapter-redis/`. Decision: [ADR 0010](../adr/0010-redis-query-contract.md). Contract notes: [redis.md](../adapters/redis.md). Prior: [Stage 8](stage-08-mongodb.md).

## 1. What Stage 9 is

`diavasi serve` installs a router. A group whose connection kind is `redis` must carry `source_spec`. The owner task runs `XGROUP SETID` to the committed stream id, then `XREADGROUP`, and `GroupEngine::ingest` pushes the page into the same buffer a Postgres or MongoDB group uses. Ack and checkpoint are unchanged.

A group with no `connection_id` stays synthetic. A connection kind the router does not know fails at create.

Still absent: an S3 adapter. Stage 10 later adds ScyllaDB. Hashes, lists, sets, and sorted sets are not sources. They do not have a Redis-enforced resume id.

## 2. Stream and order

```bash
redis-cli XADD events 9-1 body nine
redis-cli XADD events 10-0 body ten
```

`9-1` is before `10-0`. The id is two integers, milliseconds then sequence. Sorting the text would put `10-0` first, which is the wrong order. `XADD` refuses an id that is not past the current tip.

Compose publishes Redis on `127.0.0.1:6379` with no authentication.

## 3. Start the server

```bash
cargo build -p diavasi-cli
export PATH="$PWD/target/debug:$PATH"
mkdir -p /tmp/diavasi-s9
export DIAVASI_API_TOKEN=dev-token
export DIAVASI_STORE_KEY=0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef

diavasi serve \
  --bind 127.0.0.1:7700 \
  --data-bind 127.0.0.1:7710 \
  --store /tmp/diavasi-s9/meta.redb \
  --token "$DIAVASI_API_TOKEN" \
  --store-key "$DIAVASI_STORE_KEY"
```

## 4. Connection and group

```bash
export DIAVASI_URL=http://127.0.0.1:7700
export DIAVASI_API_TOKEN=dev-token

diavasi connection add \
  --id redis \
  --kind redis \
  --config-json '{"host":"127.0.0.1","port":6379,"db":0,"tls":"disable"}' \
  --secret unused

diavasi group create \
  --group-id events \
  --connection-id redis \
  --ordering-contract redis-stream \
  --source-json '{"stream":"events","group":"diavasi-events"}'

diavasi group start events
```

No `username` means the driver does not authenticate. The control plane still requires a non-empty secret, and that value is not sent to Redis. Set `username` when the server checks credentials; the secret is then the password.

`group` is the Redis consumer-group name. It is not the Diavasi group id. A second Diavasi group on `events` needs a different Redis group name, because each read calls `XGROUP SETID` on that name.

Create connects and checks that `events` is a stream. A missing key fails. The adapter does not create the stream.

`fields` is an optional inclusion list. Omitting it sends every field as a JSON object.

## 5. Consume

Use the Stage 5 client against `--data-bind`. Acked batches advance the committed cursor. Stopping the server and starting it again with the same store and key resumes after that cursor: entries already committed are not sent again, and entries still at or ahead of it are.

`XADD` with a new id shows up on a later fetch. `XDEL` of an entry that has not been committed drops it. The Redis pending list is not the checkpoint. Each fetch sets the consumer group back to the logical cursor, reads with `XREADGROUP`, then `XACK`s what it returned.

# ADR 0010: Redis Streams query contract

## Status

Accepted for Stage 9 onward.

## Context

Stage 6 and Stage 8 resume from a committed logical cursor. Redis Streams already store a total order: each entry id is `milliseconds-sequence`, and `XADD` rejects an id that is not greater than the stream tip. A Redis consumer group (`XGROUP`, `XREADGROUP`, `XACK`) tracks delivery, but that pending list is not a Diavasi checkpoint. `RecordSource::fetch_after` receives the cursor and has no ack callback.

Hashes, lists, sets, and sorted sets do not have a server-enforced exclusive resume id. This stage does not invent one for them.

## Decision

A group with no `connection_id` stays synthetic. A `redis` connection requires `source_spec`. `diavasi serve` routes `postgres`, `mongodb`, and `redis`. An unknown kind fails at create. Schema version stays 1.

`source_spec`:

- `stream`: an existing stream key. A missing key, or a key of another type, fails at create. The adapter does not create the stream.
- `group`: Redis consumer-group name. Required, so two Diavasi groups reading one stream do not share `XGROUP SETID`.
- `fields`: optional inclusion list. Omitted means every field on the entry. Values are bulk strings.

There is no filter and no `acknowledge_unsafe`. Stream ids are unique and monotonic.

The ordering tuple is two `OrderingAtom::U64` values, milliseconds then sequence, compared numerically. A lexicographic comparison of the id text is not the order. Payload is a JSON object of field name to string. Duplicate field names keep the last value. A field that is not valid UTF-8 fails the fetch.

Each `fetch_after` aligns the Redis group to the cursor and reads strictly after it:

1. `XGROUP CREATE key group 0-0`, ignoring `BUSYGROUP`. `MKSTREAM` is not used.
2. `XGROUP SETID key group` to `0-0` when the cursor is empty, otherwise to `milliseconds-sequence`.
3. `XREADGROUP GROUP group diavasi COUNT limit STREAMS key >` with no `BLOCK`. The consumer name is the fixed string `diavasi`.
4. `XACK` the returned ids. `XACK` does not delete stream entries. A later fetch whose cursor is still behind those ids calls `SETID` again and reads them again.

`XPENDING` and `XAUTOCLAIM` are not the resume path. One connection is opened per running group.

Connection `config_json` is `host`, `port`, `db` (default `0`), optional `username`, and `tls` (`disable` or `require`). When `username` is set, the sealed secret is the password. When `username` is omitted, the driver does not authenticate. The control plane still requires a non-empty secret; that value is not sent to Redis.

## Consequences

- Entries at or ahead of the committed id are read again after a restart. Entries behind that id are not.
- An `XADD` ahead of the cursor is delivered. Redis rejects an id that is not past the tip, so an insert behind the cursor does not exist.
- `XDEL` or `XTRIM` of an entry the cursor has not passed drops it. That is contract breakage, the same as a deleted row.
- `XACK` keeps the pending list from growing. It is not the durable cursor.
- Blocking reads, other Redis types, and consumer-group claiming are out of scope.

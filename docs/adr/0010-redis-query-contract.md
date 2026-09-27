# ADR 0010: Redis Streams query contract

## Status

Accepted for Stage 9 onward.

## Context

Stage 6 and Stage 8 resume from a committed logical cursor. Redis Streams already store a total order: each entry id is `milliseconds-sequence`, and `XADD` rejects an id that is not greater than the stream tip. `XRANGE` with an exclusive start reads strictly after an id. `RecordSource::fetch_after` receives the cursor and has no ack callback.

Hashes, lists, sets, and sorted sets do not have a server-enforced exclusive resume id. This stage does not invent one for them.

## Decision

A group with no `connection_id` stays synthetic. A `redis` connection requires `source_spec`. `diavasi serve` routes `postgres`, `mongodb`, and `redis`. An unknown kind fails at create. Schema version stays 1.

`source_spec`:

- `stream`: an existing stream key. A missing key, or a key of another type, fails at create. The adapter does not create the stream.
- `group`: accepted and ignored. Specs written for v0.9.0 to v0.12.0 carry it.
- `fields`: optional inclusion list. Omitted means every field on the entry. Values are bulk strings.

There is no filter and no `acknowledge_unsafe`. Stream ids are unique and monotonic.

The ordering tuple is two `OrderingAtom::U64` values, milliseconds then sequence, compared numerically. A lexicographic comparison of the id text is not the order. Payload is a JSON object of field name to string. Duplicate field names keep the last value. A field that is not valid UTF-8 fails the fetch.

Each `fetch_after` reads strictly after the cursor and writes nothing:

1. With a cursor, `XINFO STREAM key`. When every entry up to the cursor is gone, `entries-added` exceeds `length`, and `max-deleted-entry-id` is before the cursor, entries after the cursor were trimmed before delivery. The fetch fails with `trimmed past the committed cursor` and the group stops instead of skipping them. Redis before 7.0 lacks these fields and the check is skipped.
2. `XRANGE key - + COUNT limit` when the cursor is empty, otherwise `XRANGE key (<ms>-<seq> + COUNT limit`.

Versions v0.9.0 to v0.12.0 moved a Redis consumer group with `XGROUP SETID` before each `XREADGROUP`. Two readers that shared a group name moved each other's position between those two commands and skipped or repeated entries. `XRANGE` has no shared state. One connection is opened per running group. Redis 6.2 or later is required for the exclusive start.

Connection `config_json` is `host`, `port`, `db` (default `0`), optional `user` (`username` is accepted from older connections), and `tls` (`disable` or `require`). Keys other than these are rejected, so a misspelled key fails at group create instead of being ignored. When `user` is set, the sealed secret is the password. When it is omitted, the driver does not authenticate. The control plane still requires a non-empty secret; that value is not sent to Redis.

## Consequences

- Entries at or ahead of the committed id are read again after a restart. Entries behind that id are not.
- An `XADD` ahead of the cursor is delivered. Redis rejects an id that is not past the tip, so an insert behind the cursor does not exist.
- `XDEL` of an entry the cursor has not passed drops it. That is contract breakage, the same as a deleted row.
- A trim that overtakes the cursor stops the group. A trim that stops exactly at the cursor stops it too, because Redis does not report which ids a trim removed.
- Blocking reads, other Redis types, and consumer-group claiming are out of scope.

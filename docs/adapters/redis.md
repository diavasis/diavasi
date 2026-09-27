# Redis adapter

The adapter reads one Redis stream with `XRANGE`, and pushes entries through the existing buffer, ack, and checkpoint path. Reads do not write to Redis. Resume is the committed logical cursor, which is the stream id.

Decision: [ADR 0010](../adr/0010-redis-query-contract.md).

## Guarantee

A consumer that acks batches, and a process that restarts from the committed cursor, does not omit an entry whose id is still at or ahead of that cursor, as long as the entry has not been deleted or trimmed. A trim that overtakes the cursor stops the group with a `trimmed past the committed cursor` error instead of skipping entries (Redis 7.0 or later).

Delivery stays at-least-once. Unacked entries are read again after a crash. The data plane is unchanged: clients ack batch ids.

## Assumption

- The key is a stream. Stream ids are `milliseconds-sequence` and increase. Redis rejects an `XADD` id that is not greater than the tip.
- Any number of Diavasi groups can read one stream. No Redis consumer group is created. A `group` field in older specs is accepted and ignored.
- Redis 6.2 or later, for the exclusive `(` start in `XRANGE`.
- Omitting `fields` sends every field. `fields` is an inclusion list. Values are UTF-8 strings. Duplicate field names keep the last value.
- When `user` is set, the sealed secret is the password (`username` is accepted from older connections). When it is omitted, the client does not authenticate. `tls` is `disable` or `require`. `db` defaults to `0`.

## Limitation

This is not a Redis Streams consumer-group replacement, and it is not a reader for hashes, lists, sets, or sorted sets.

- The durable cursor is the stream id in the Diavasi store. Each read is `XRANGE key (<cursor> + COUNT <limit>`. The read does not block.
- Deleting an entry with `XDEL` before it is committed removes it from later reads. Trimming is detected only when every entry up to the cursor is gone and no `XDEL` reached the cursor; a trim that stops exactly at the cursor is reported too, because Redis does not say which ids a trim removed. An entry already sitting in the in-memory buffer can still be delivered once; after a restart from the committed cursor it is gone.
- An id behind the committed cursor is not delivered. Redis will not accept such an `XADD` while a later id is the tip.
- The wire `record_id` is a single integer. A stream id is two integers, so the protocol `record_id` is `0`. Order is the logical cursor, not that field.
- There is no server-side filter.

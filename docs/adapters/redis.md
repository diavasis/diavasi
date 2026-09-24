# Redis adapter

The adapter reads one Redis stream with `XGROUP` and `XREADGROUP`, and pushes entries through the existing buffer, ack, and checkpoint path. Resume is the committed logical cursor, which is the stream id.

Decision: [ADR 0010](../adr/0010-redis-query-contract.md).

## Guarantee

A consumer that acks batches, and a process that restarts from the committed cursor, does not omit an entry whose id is still at or ahead of that cursor, as long as the entry has not been deleted or trimmed.

Delivery stays at-least-once. Unacked entries are read again after a crash. The data plane is unchanged: clients ack batch ids.

## Assumption

- The key is a stream. Stream ids are `milliseconds-sequence` and increase. Redis rejects an `XADD` id that is not greater than the tip.
- `group` is a Redis consumer-group name reserved for this Diavasi group. Two Diavasi groups on the same stream use different Redis group names, because `XGROUP SETID` is per Redis group.
- Omitting `fields` sends every field. `fields` is an inclusion list. Values are UTF-8 strings. Duplicate field names keep the last value.
- When `username` is set, the sealed secret is the password. When `username` is omitted, the client does not authenticate. `tls` is `disable` or `require`. `db` defaults to `0`.

## Limitation

This is not a Redis Streams consumer-group replacement, and it is not a reader for hashes, lists, sets, or sorted sets.

- The durable cursor is the stream id in the Diavasi store. The Redis pending list is cleared with `XACK` after a successful read and is not consulted on restart.
- `XGROUP SETID` rewinds the group's last-delivered id to the cursor before each `XREADGROUP`. The read does not block.
- Deleting or trimming an entry that has not been committed removes it from later reads. An entry already sitting in the in-memory buffer can still be delivered once; after a restart from the committed cursor it is gone.
- An id behind the committed cursor is not delivered. Redis will not accept such an `XADD` while a later id is the tip.
- The wire `record_id` is a single integer. A stream id is two integers, so the protocol `record_id` is `0`. Order is the logical cursor, not that field.
- There is no server-side filter.

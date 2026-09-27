# ScyllaDB adapter

The adapter reads one ScyllaDB table and pushes rows through the existing buffer, ack, and checkpoint path. Resume is the committed logical key.

Decision: [ADR 0011](../adr/0011-scylla-query-contract.md).

## Guarantee

A consumer that acks batches, and a process that restarts from the committed cursor, does not omit a row that is still strictly after that cursor in the chosen order, as long as the row has not been deleted or expired.

Delivery stays at-least-once. Unacked rows are read again after a crash. The data plane is unchanged: clients ack batch ids.

## Assumption

- The default read is one partition. `partition` names every partition-key column. A missing table, a missing key column, or an extra key column fails at create.
- `scan` set to `token` walks the ring in token order. It replaces `partition`. One sequential walk, not a split of the ring.
- Clustering order is the table order. Descending columns are stored with an invertible complement so the engine still sees a strictly increasing tuple. The CQL comparison uses the original value.
- Omitting `columns` sends every column. `columns` is an inclusion list. Primary-key columns are always included.
- When `user` is set, the sealed secret is the password (`username` is accepted from older connections). When it is omitted, the client does not authenticate. `tls` is `disable` or `require`. `port` defaults to `9042`.
- Key columns are integers, timestamps, dates, booleans, text, blobs, or uuids. `double`, `decimal`, collections, and user types are not key columns.

## Limitation

This is not a CDC reader and it is not a secondary-index reader.

- The durable cursor is the logical key in the Diavasi store. The driver paging cookie is not stored. Each fetch is a new prepared `SELECT` with `LIMIT` equal to the batch.
- An insert behind the committed clustering cursor is not delivered. A delete or TTL of a row that has not been committed removes it from later reads. A row already sitting in the in-memory buffer can still be delivered once; after a restart from the committed cursor it is gone.
- A token scan can miss or repeat rows when the ring gains or loses members after the checkpoint. A page that is still inside the cursor's token is widened, up to eight pages, so the rest of that token is not skipped. A page that reaches a later token continues with `token(...) > last_token`.
- The wire `record_id` is a single integer. A compound key or a token cursor becomes `record_id` `0`. Order is the logical cursor, not that field.
- There is no `ALLOW FILTERING` and no `ORDER BY` other than the table clustering order.

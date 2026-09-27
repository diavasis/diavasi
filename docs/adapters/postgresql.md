# PostgreSQL adapter

The adapter reads one declared table in a total order and pushes rows through the existing buffer, ack, and checkpoint path. Resume is the committed logical cursor.

Decision: [ADR 0008](../adr/0008-postgres-query-contract.md).

## Guarantee

A consumer that acks batches, and a process that restarts from the committed cursor, does not omit a row whose ordering key is still at or ahead of that cursor, as long as the row's ordering columns are not mutated out from under the cursor.

Delivery stays at-least-once. Unacked rows are read again after a crash. The data plane is unchanged: clients ack batch ids.

## Assumption

- The table has a total order. Order columns are non-null and match the declared types (`int2`, `int4`, `int8`, `text`, `varchar`, `bytea`, `timestamptz`).
- The order columns start with every column of a unique index, in index order, unless the operator sets `acknowledge_unsafe`. With `UNIQUE (a, b)`, ordering by `a` alone is rejected.
- Producers insert new keys ahead of the cursor, or accept that keys behind the cursor are invisible.
- Text order is `COLLATE "C"`. An index serves that sort only when it is declared with the same collation, for example `CREATE UNIQUE INDEX ON events (name COLLATE "C", id)`. Without one, each read sorts the filtered table.
- The connection password is the sealed secret. `sslmode` is `disable`, `require`, or `verify-full`; both TLS modes verify the server certificate against the public roots, or against `ca_pem` when it is set.

## Limitation

This is not change-data capture.

- An insert whose key is behind the committed cursor never appears.
- Deleting a row that has not been committed removes it from later reads. A row already sitting in the in-memory buffer can still be delivered once; after a restart from the committed cursor it is gone.
- Updating payload columns does not redeliver a row the cursor has already passed.
- Updating an ordering column so the new key falls behind the committed cursor drops the row. The contract tests assert that miss. They do not paper over it.
- There is no server-side cursor and no logical replication slot. A crash resumes with a new keyset query.
- A millions-row scan is `#[ignore]` and is not the Stage 7 benchmark. CI covers a table of tens of thousands of rows.

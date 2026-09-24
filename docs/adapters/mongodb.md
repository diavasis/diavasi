# MongoDB adapter

The adapter reads one collection with `find`, in a declared order, and pushes documents through the existing buffer, ack, and checkpoint path. Resume is the committed logical cursor.

Decision: [ADR 0009](../adr/0009-mongodb-query-contract.md).

## Guarantee

A consumer that acks batches, and a process that restarts from the committed cursor, does not omit a document whose sort key is still at or ahead of that cursor, as long as the document's sort fields are not mutated out from under the cursor.

Delivery stays at-least-once. Unacked documents are read again after a crash. The data plane is unchanged: clients ack batch ids.

## Assumption

- The sort is a total order. Each sort field is non-null and matches its declared BSON type: `objectId`, `int32`, `int64`, `string`, `date`, `bool`, or `binData`. Direction is `asc` or `desc`.
- A unique index covers those fields in that order and those directions, unless the operator sets `acknowledge_unsafe`. The built-in `_id_` index covers `_id` ascending.
- Producers insert new keys ahead of the cursor, or accept that keys behind the cursor are invisible.
- Omitting `order_by` means `_id` ascending.
- Omitting `fields` sends the whole document. `fields` is an inclusion list; sort fields are included with it.
- `filter` is a query object. `$where` and `$function` are rejected.
- When `user` is set, the sealed secret is the password. When `user` is omitted, the client does not authenticate. `tls` is `disable` or `require`.

## Limitation

This is not a change stream.

- An insert whose key is behind the committed cursor never appears.
- Deleting a document that has not been committed removes it from later reads. A document already sitting in the in-memory buffer can still be delivered once; after a restart from the committed cursor it is gone.
- Updating fields that are not part of the sort does not redeliver a document the cursor has already passed.
- Updating a sort field so the new key falls behind the committed cursor drops the document. The contract tests assert that miss.
- There is no server-side cursor, no aggregation pipeline, and no tailable cursor. A crash resumes with a new `find`.
- `double`, `decimal128`, arrays, documents, and null are not sort keys.
- A millions-document scan is `#[ignore]`. CI covers a collection of tens of thousands of documents.

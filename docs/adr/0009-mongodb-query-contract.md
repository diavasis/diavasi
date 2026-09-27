# ADR 0009: MongoDB query contract

## Status

Accepted in v0.8.0.

## Context

Stage 6 reads PostgreSQL with a keyset over a declared order. MongoDB is the next source. Its native read is `find` with a sort, not a SQL tuple comparison, and a server cursor does not survive a process restart. The adapter must keep the same recovery rule as PostgreSQL: resume from the committed logical cursor.

## Decision

A group with no `connection_id` stays synthetic. A `mongodb` connection requires `source_spec`. `diavasi serve` installs a router that forwards to the Postgres factory or the MongoDB factory by `connection.kind`. An unknown kind fails at create. Schema version stays 1.

`source_spec`:

- `collection`: collection in the connection's database. Empty names, `$`, NUL, and the `system.` prefix are rejected. Dots are allowed.
- `order_by`: optional. Each entry is `field`, BSON `type` (`objectId`, `int32`, `int64`, `string`, `date`, `bool`, `binData`), and `direction` (`asc` or `desc`). Omitted means `{ field: "_id", type: "objectId", direction: "asc" }`.
- `fields`: optional inclusion projection. Omitted means the whole document is the payload. Sort fields are always requested.
- `filter`: optional query object, AND-ed with the resume predicate. A non-object, `$where`, and `$function` are rejected.
- `acknowledge_unsafe`: optional, default false.

`double`, `decimal128`, arrays, documents, and null are not sort keys. A value whose BSON type does not match the declaration fails the fetch.

At group create the factory connects and requires a unique index whose key fields are the leading sort fields, in key order. With a unique index on `{a: 1, b: 1}`, a sort on `[a, b]` or `[a, b, c]` qualifies and a sort on `[a]` does not, because a prefix of a unique key is not unique. Direction does not affect uniqueness and is not compared. Partial, sparse, and hidden indexes do not count. Without such an index, create fails unless `acknowledge_unsafe` is true. `_id` ascending is covered by the built-in `_id_` index.

Each `fetch_after` is a new `find`:

```text
filter:     { $and: [ <user filter>, <resume> ] }
sort:       the declared directions
limit:      the batch limit
projection: fields, when set
```

The resume predicate is an `$or` of a prefix equality plus `$gt` or `$lt` on the next field. An empty cursor omits it. There is no aggregation pipeline and no held Mongo cursor.

The engine accepts a batch only when each ordering tuple is strictly after the previous one. Descending fields are stored in an invertible encoding so that traversal order still increases: signed values are bitwise-complemented, and byte values (`string`, `objectId`, `binData`) are the complement of a memcomparable encoding. The find predicate is built from the decoded original BSON value.

Payload is the document as relaxed extended JSON. One client is opened per running group.

Connection `config_json` is `host`, `port`, `database`, optional `user`, `auth_source` (default `admin`), and `tls` (`disable` or `require`). Keys other than these are rejected, so a misspelled key fails at group create instead of being ignored. When `user` is set, the sealed secret is the password. When `user` is omitted, the driver connects without credentials. The control plane still requires a non-empty secret; that value is not sent to MongoDB.

Amended in v0.12.0: `config_json` can give `uri` (`mongodb://` with several hosts, or `mongodb+srv://`) instead of `host` and `port`. With `uri`, the driver discovers the topology instead of connecting directly to one server, and credentials still come from `user` and the sealed secret.

## Consequences

- Documents at or ahead of the committed cursor are read again after a restart. Documents behind that cursor are not.
- An insert ahead of the cursor is delivered. An insert behind it is not.
- A field update that does not move the sort key does not redeliver a document the cursor has passed.
- A delete of a document that has not yet been committed is omitted on the next read from the committed cursor.
- Updating a sort field so the new key falls behind the committed cursor drops that document. That is contract breakage.
- `acknowledge_unsafe` allows a sort with no qualifying unique index. When two documents share a sort tuple and a batch ends between them, the next read starts after that tuple and the second document is never delivered.
- Aggregation, change streams, and tailable cursors are out of scope.

# ADR 0011: ScyllaDB query contract

## Status

Accepted for Stage 10 onward.

## Context

Stage 6, Stage 8, and Stage 9 resume from a committed logical cursor. A ScyllaDB table has a cheap total order inside one partition: the clustering columns, in the table's clustering order. A full-table order is the ring order of `token(partition key)`, then the partition key, then the clustering columns. The driver also exposes a paging cookie. That cookie is not a Diavasi checkpoint. `RecordSource::fetch_after` receives the cursor and has no ack callback.

The driver crate is `scylla` `>=1.5, <1.6`. `1.6` needs Rust 1.88, and the workspace `rust-version` stays 1.85.

## Decision

A group with no `connection_id` stays synthetic. A `scylla` connection requires `source_spec`. `diavasi serve` routes `postgres`, `mongodb`, `redis`, and `scylla`. An unknown kind fails at create. Schema version stays 1. One running group holds one session. Statements are prepared once when the group opens.

`source_spec`:

- `keyspace` and `table`. `keyspace` may be omitted when the connection config sets it. The table must already exist.
- `partition`: an object with every partition-key column and its value. This is the default read.
- `scan`: `"token"`, instead of `partition`. The read walks the ring in token order, one range at a time.
- `columns`: optional payload list. Omitting it selects every column. Primary-key columns are always selected.

Neither `partition` nor `scan` fails at create. Setting both fails at create. `ALLOW FILTERING`, a secondary index, and an `ORDER BY` that is not the table clustering order are not fields of this spec.

At create the adapter reads `system_schema.columns`. Partition mode must name every partition key and nothing else. Key columns may be `tinyint`, `smallint`, `int`, `bigint`, `timestamp`, `date`, `boolean`, `text`, `ascii`, `varchar`, `blob`, or `uuid`. `double`, `decimal`, collections, and user types are rejected as key columns. A partition read needs at least one clustering column.

Ordering:

- Partition mode stores the clustering columns.
- Token mode stores the token as `OrderingAtom::I64`, then the partition key, then the clustering columns.
- A descending clustering column is stored with the same invertible complement the MongoDB adapter uses, so `GroupEngine::ingest` still sees a strictly increasing tuple. The CQL predicate uses the original values: `>` for ascending, `<` for descending. A compound key is prefix equality plus the next column's inequality, one prepared statement per prefix.

Payload JSON: integers and timestamps are numbers, booleans are booleans, text is a string, uuid is canonical text, and blob is base64.

Connection `config_json` is `host`, `port` (default `9042`), optional `keyspace`, optional `user` (`username` is accepted from older connections), and `tls` (`disable` or `require`). Keys other than these are rejected, so a misspelled key fails at group create instead of being ignored. When `user` is set, the sealed secret is the password. When it is omitted, the driver does not authenticate. The control plane still requires a non-empty secret; that value is not sent to ScyllaDB. Tests use `SCYLLA_URL=127.0.0.1:9042`.

`fetch_after` does not keep paging state. Each call runs a prepared `SELECT` with `LIMIT` equal to the batch.

- Partition: `SELECT id, body FROM ks.events WHERE bucket = ? AND id > ? LIMIT ?`. An empty cursor omits the clustering predicate.
- Token: `SELECT token(bucket), bucket, id, body FROM ks.events WHERE token(bucket) >= ? LIMIT ?`. Rows that are not strictly after the cursor are dropped. When a whole page sits inside the cursor's token, the next statement uses a larger `LIMIT` on that same token, up to eight pages. When the page moves to a later token, the next statement uses `token(...) > last_token`. An empty cursor omits the token predicate. The walk is one sequential pass, not parallel token-range splits.

## Consequences

- An insert behind a committed clustering cursor does not appear. An insert ahead of it does.
- A delete or TTL of a row that has not been committed drops it from later reads.
- A token scan can miss or repeat rows if ring membership changes after the checkpoint.
- The wire `record_id` is a single integer. A single non-negative bigint clustering column is that id. A compound key or a token cursor uses `record_id` `0`. Order is the logical cursor.
- CDC, materialized views as their own source, secondary indexes, and multi-query parallel token splits are out of scope.

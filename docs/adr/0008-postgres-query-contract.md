# ADR 0008: PostgreSQL query contract

## Status

Accepted for Stage 6 onward.

## Context

Stages 0–5 deliver groups from a synthetic source. The first real source is PostgreSQL. The adapter must resume after a crash from durable logical progress, and it must not pull `diavasi` into a dependency on `tokio-postgres`.

## Decision

A group with no `connection_id` stays synthetic. A `postgres` connection requires `source_spec`. Both fields are stored on `GroupRecord` with serde defaults, so existing stores stay at schema version 1.

`source_spec`:

- `table`: `name` or `schema.name`. Identifiers are quoted. An unqualified name is `public`.
- `order_by`: ordered columns. Each type is `int2`, `int4`, `int8`, `text`, `varchar`, `bytea`, or `timestamptz`.
- `payload`: column names, encoded as one JSON object on `Record.payload`.
- `filter`: optional boolean expression. It is parenthesized and AND-ed with the keyset predicate. Outside quoted literals and identifiers, `;`, `--`, `/*`, `*/`, and `$` are rejected, and parentheses must balance, so the filter cannot close its own parentheses and bypass the keyset. Literals must not contain a backslash. At group create the full query is prepared, so a filter that does not compile fails there.
- `acknowledge_unsafe`: optional, default false.

Connection `config_json` is `host`, `port`, `dbname`, `user`, `sslmode`, and optional `ca_pem`. Keys other than these are rejected, so a misspelled key fails at group create instead of being ignored. `sslmode` is `disable`, `require`, or `verify-full`; `require` and `verify-full` both verify the server certificate, which is stricter than libpq's `require`. `ca_pem` holds PEM CA certificates for a server whose certificate a public root does not sign (RDS, Cloud SQL, a private CA). The sealed secret is the password, opened only with `open_secret`. The source spec rejects unknown keys the same way.

At group create the factory connects and checks that every order column exists, is non-nullable, and has the declared type. The order columns must start with every column of a unique, valid, non-partial index, in index order. With `UNIQUE (a, b)`, `order_by` `[a, b]` and `[a, b, c]` qualify and `[a]` does not: a prefix of a unique key is not unique, and the keyset predicate `(a) > $1` would skip the rows that share the last delivered `a`. Without a qualifying index, create fails unless `acknowledge_unsafe` is true.

The reader issues:

```sql
SELECT <order columns>, <payload columns>
FROM <table>
WHERE (<filter>) AND (<order tuple>) > (<cursor params>)
ORDER BY <order columns>
LIMIT $n
```

Text and `varchar` order expressions use `COLLATE "C"` so SQL order matches byte order. Integers and `timestamptz` become `OrderingAtom::I64` (`timestamptz` as microseconds). Text and `bytea` become `OrderingAtom::Bytes`. An empty cursor omits the keyset predicate.

Keyset resume is the only resume mechanism. There is no `DECLARE CURSOR`.

`diavasi` defines `RecordSource` and `SourceFactory`. `diavasi-adapter-postgres` implements them. The CLI installs `PostgresFactory`. One connection is opened per running group.

## Consequences

- Rows at or ahead of the committed cursor are read again after a restart. Rows behind that cursor are not.
- An insert ahead of the cursor is delivered. An insert behind it is not.
- A payload update does not move the key, so an already-passed row is not delivered again.
- A delete of a row that has not yet been committed is omitted on the next read from the committed cursor.
- Updating an ordering column so the new key falls behind the committed cursor drops that row. That is contract breakage, not a bug to hide.
- `acknowledge_unsafe` allows a group with no qualifying unique index. When two rows share an order tuple and a batch ends between them, the next read starts after that tuple and the second row is never delivered.
- CDC, logical replication, and other databases are out of scope.

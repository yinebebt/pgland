# Design

pgland lands event data from many systems in one Postgres table, keeps it raw, and serves reads and counts on top. It is one Go binary and Postgres. Anything more is added only when a measured limit calls for it.

## Data flow

```
pull:  sources.json ──► POST /sources/{name}/sync ──► fetch one page ──┐
                                                                       ├─► raw_events (upsert)
push:  any client ────► POST /events/{source} ────► validate, verify ──┘        │
                                                                                ▼
                                                                         /events  /stats
```

Both paths produce `event.Record` values and write them through the same upsert. Nothing is transformed on the way in. Derived tables, when they exist, are rebuilt from `raw_events`.

## Packages

| package | role |
| --- | --- |
| `cmd/pgland` | Wires config, store, sources, and the HTTP server. Graceful shutdown. |
| `internal/config` | Environment variables. |
| `internal/event` | `Record` and its validation. |
| `internal/source` | Config-driven HTTP JSON pull source. `Link: rel="next"` paging. |
| `internal/store` | Postgres pool, embedded `schema.sql`, upsert, cursors, stats. |
| `internal/httpapi` | Routes, push signature check. |

## Data model

```sql
CREATE TABLE raw_events (
    id          bigserial   PRIMARY KEY,
    source      text        NOT NULL,
    external_id text        NOT NULL,
    event_type  text        NOT NULL,
    payload     jsonb       NOT NULL,
    occurred_at timestamptz,
    ingested_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (source, external_id, event_type)
);

CREATE TABLE sync_state (
    source    text        PRIMARY KEY,
    cursor    text        NOT NULL,
    synced_at timestamptz NOT NULL DEFAULT now()
);
```

The schema is `internal/store/schema.sql`, applied on start with `CREATE TABLE IF NOT EXISTS`. A new table is a new statement there. Changing an existing column needs a migration; one is added when a released schema first has to change in place.

## Idempotency

- Every write is `INSERT ... ON CONFLICT (source, external_id, event_type) DO UPDATE` on payload and occurred_at, skipped when neither changed. Retries, repeated pushes, and re-read pages converge on one row without rewriting it.
- A sync writes its page and moves its cursor in one transaction. The cursor never runs ahead of stored events.
- One sync per source runs at a time. A second call while one is running gets `409`, so the same page is not fetched twice. The lock is in-process; with several pgland replicas, the upsert still keeps the result correct.

## Pull paging

The cursor is the next page URL from the `Link: rel="next"` header. Links to another host are ignored, and a saved cursor on another host is rejected, so a sync only ever calls the configured host. Without a next link, the cursor stays on the current page so later syncs pick up appended records.

APIs that page in the response body (a `next` field or token) are not supported yet.

## Push

`POST /events/{source}` takes a JSON array of `{id, type, occurred_at, data}`. When `INGEST_SECRET` is set, the raw body must carry `X-Pgland-Signature: sha256=<hex HMAC-SHA256>`. The check runs before the body is parsed.

## Analytics

`GET /stats` groups by source and type, and by UTC day on `coalesce(occurred_at, ingested_at)`. For deeper analysis, point Grafana, Metabase, or plain SQL at the same database.

## Roadmap

- Body-based paging: `next_path` for APIs that return the next cursor or URL in JSON.
- Scheduled sync per source, so pulls do not need an external trigger.
- Database poll source: `SELECT ... WHERE updated_at > cursor`, paged.
- Normalized tables derived from `raw_events`, rebuilt idempotently.
- A batch copy of normalized rows into a separate reporting schema, kept apart from the live tables.
- Container image and a compose file that runs pgland next to Postgres.

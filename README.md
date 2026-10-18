# pgland

Land event data from any API in Postgres.

pgland is one Go binary and a Postgres database. It pulls batches from HTTP JSON APIs you describe in a config file, accepts events pushed to it over HTTP, stores both in one `raw_events` table, and serves the events and daily counts as JSON.

## Quick start

Requires Go 1.26 and Docker.

```bash
docker compose -f ../infra/docker/localstack.yaml up -d
docker compose up -d --wait
export DATABASE_URL='postgres://pgland:pgland@localhost:5432/pgland?sslmode=disable'
export AWS_ENDPOINT_URL=http://localhost:4566 AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test
go run ./cmd/pgland worker
```

In another shell, with the same environment:

```bash
go run ./cmd/pgland
```

Then sync the example source and read what it stored:

```bash
curl -X POST localhost:8080/sources/openfda/sync
curl localhost:8080/sources/openfda/syncs/<id>
curl 'localhost:8080/events?source=openfda&limit=5'
```

The worker must already be running. Each `POST` enqueues one run id and returns `202`. The worker fetches that one batch. Call `POST` again for the next batch. The saved cursor is where that next call starts.

Each sync stores the next five FDA drug adverse-event reports. Every row's `external_id` is the FDA `safetyreportid`, so you can look any report up at `https://api.fda.gov/drug/event.json?search=safetyreportid:"<id>"`.

## Pull sources

Pull sources live in `sources.json` (or the file in `SOURCES_FILE`). The shipped example:

```json
{
  "sources": [
    {
      "name": "openfda",
      "event_type": "adverse_event",
      "url": "https://api.fda.gov/drug/event.json?search=patient.reaction.reactionmeddrapt:%22headache%22&limit=5&sort=receivedate:asc",
      "records_path": "results",
      "id_field": "safetyreportid",
      "time_field": "receivedate",
      "time_layout": "20060102"
    }
  ]
}
```

| field | meaning |
| --- | --- |
| `name` | Source name, `[a-z0-9_-]`, used in URLs and rows. |
| `event_type` | Stored on every record from this source. |
| `url` | Start URL. The first batch is fetched from here. |
| `headers` | Optional request headers. Values expand `$VAR` from the environment, so tokens stay out of the file. |
| `records_path` | Dot path to the array of records in the response, e.g. `data.items`. Empty if the body is the array. |
| `id_field` | Dot path to each record's unique id. String or number. |
| `time_field` | Optional dot path to the event time. |
| `time_layout` | Go time layout for `time_field`. Default RFC 3339. |

`POST /sources/{name}/sync` does not fetch the batch. It inserts one `accepted` run, puts that run id on the queue, and returns `202`. The worker, already polling the queue, receives that one message, calls the source, and stores the batch. `GET /sources/{name}/syncs/{id}` shows `accepted`, `finished`, or `failed`. A second accept while one run for that source is still `accepted` returns `409`. How a failed fetch is retried, and when it lands on the dead-letter queue, is in [docs/design.md](docs/design.md).

An HTTP source's batch is one response page. When the response has a `Link: <...>; rel="next"` header, that URL is saved as the cursor and the next sync starts there. With no next link, the cursor stays on the current page, so the next sync re-reads it and picks up anything appended. Next links to a host other than the one in `url` are ignored.

## Push events

Send a JSON array to `POST /events/{source}`. Any source name works; it does not need to be in `sources.json`.

```bash
curl -X POST localhost:8080/events/myapp \
  -d '[{"id":"evt_1","type":"signup","occurred_at":"2026-10-01T10:00:00Z","data":{"plan":"pro"}}]'
```

`id`, `type`, and `data` are required. `data` is stored as the payload. Up to 1000 events or 1 MiB per request.

When `INGEST_SECRET` is set, every push needs `X-Pgland-Signature: sha256=<hex HMAC-SHA256 of the raw body>`:

```bash
SIG=$(printf '%s' "$BODY" | openssl dgst -sha256 -hmac "$INGEST_SECRET" -hex | awk '{print $NF}')
curl -X POST localhost:8080/events/myapp -H "X-Pgland-Signature: sha256=$SIG" -d "$BODY"
```

## Duplicates

Every record is upserted on `(source, external_id, event_type)`. Sending the same event twice, retrying a push, or re-reading a batch leaves one row. The row keeps its `id` and `ingested_at`; `payload` and `occurred_at` take the latest values.

A sync writes its records and moves its cursor in one transaction. If the write fails, the cursor stays where it was.

## HTTP API

| method | path | |
| --- | --- | --- |
| `GET` | `/healthz` | Database check and version. |
| `GET` | `/sources` | Configured pull sources and their cursors. |
| `POST` | `/sources/{name}/sync` | Accept the next batch. `202` with the run. `409` if that source already has an accepted run. |
| `GET` | `/sources/{name}/syncs/{id}` | Accepted, finished, or failed. |
| `POST` | `/events/{source}` | Push events. |
| `GET` | `/events?source=&limit=` | Newest events first. `limit` defaults to 50, max 500. |
| `GET` | `/stats?days=` | Totals per source and type, and per-day counts for the `days` most recent days that have events. |

## Configuration

Settings, defaults, and required variables are in [`.env.example`](.env.example).

The schema is [`internal/store/schema.sql`](internal/store/schema.sql), applied on every start with `CREATE TABLE IF NOT EXISTS`. Query `raw_events` directly from any SQL or BI tool.

## Development

```bash
make test   # store tests run when DATABASE_URL is set
make build  # ./bin/pgland
```

Design notes and the roadmap are in [docs/design.md](docs/design.md).

## License

MIT. See [LICENSE](LICENSE).

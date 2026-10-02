# pgland

Land event data from any API in Postgres.

pgland is one Go binary and a Postgres database. It pulls pages from HTTP JSON APIs you describe in a config file, accepts events pushed to it over HTTP, stores both in one `raw_events` table, and serves the events and daily counts as JSON and on a built-in dashboard.

![dashboard](docs/dashboard.png)

## Quick start

Requires Go 1.26 and Docker.

```bash
docker compose up -d --wait
export DATABASE_URL='postgres://pgland:pgland@localhost:5432/pgland?sslmode=disable'
go run ./cmd/pgland
```

Open <http://localhost:8080> and press **Sync** on `openfda`, or from the shell:

```bash
curl -X POST localhost:8080/sources/openfda/sync
curl 'localhost:8080/events?source=openfda&limit=5'
```

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
| `url` | First page. |
| `headers` | Optional request headers. Values expand `$VAR` from the environment, so tokens stay out of the file. |
| `records_path` | Dot path to the array of records in the response, e.g. `data.items`. Empty if the body is the array. |
| `id_field` | Dot path to each record's unique id. String or number. |
| `time_field` | Optional dot path to the event time. |
| `time_layout` | Go time layout for `time_field`. Default RFC 3339. |

`POST /sources/{name}/sync` fetches one page. When the response has a `Link: <...>; rel="next"` header, that URL is saved as the cursor and the next sync starts there. With no next link, the cursor stays on the current page, so the next sync re-reads it and picks up anything appended. Next links to a host other than the one in `url` are ignored.

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

Every record is upserted on `(source, external_id, event_type)`. Sending the same event twice, retrying a push, or re-reading a page leaves one row. The row keeps its `id` and `ingested_at`; `payload` and `occurred_at` take the latest values.

A sync writes its records and moves its cursor in one transaction. If the write fails, the cursor stays where it was.

## HTTP API

| method | path | |
| --- | --- | --- |
| `GET` | `/` | Dashboard. |
| `GET` | `/healthz` | Database check and version. |
| `GET` | `/sources` | Configured pull sources and their cursors. |
| `POST` | `/sources/{name}/sync` | Fetch and store the next page. `409` if a sync of that source is already running. |
| `POST` | `/events/{source}` | Push events. |
| `GET` | `/events?source=&limit=&before=` | Newest events first. `limit` defaults to 50, max 500. For the next page, pass the response's `next_before` as `before`; it is `null` on the last page. |
| `GET` | `/stats?days=` | Totals per source and type, and per-day counts for the `days` most recent days that have events. |

## Configuration

| variable | default | |
| --- | --- | --- |
| `DATABASE_URL` | required | Postgres connection string. |
| `HTTP_ADDR` | `:8080` | Listen address. |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn`, `error`. |
| `SHUTDOWN_TIMEOUT` | `10s` | Grace period for in-flight requests. |
| `SOURCES_FILE` | `sources.json` | Pull sources. A missing file means none. |
| `INGEST_SECRET` | empty | Require signed pushes when set. |

The schema is `internal/store/schema.sql`, applied on every start with `CREATE TABLE IF NOT EXISTS`. Query `raw_events` directly from any SQL or BI tool.

## Development

```bash
make db-up
make test   # store tests run when DATABASE_URL is set
make build  # ./bin/pgland
```

Design notes and the roadmap are in [docs/design.md](docs/design.md).

## License

MIT. See [LICENSE](LICENSE).

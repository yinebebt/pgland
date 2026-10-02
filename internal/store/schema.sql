CREATE TABLE IF NOT EXISTS raw_events (
    id          bigserial   PRIMARY KEY,
    source      text        NOT NULL,
    external_id text        NOT NULL,
    event_type  text        NOT NULL,
    payload     jsonb       NOT NULL,
    occurred_at timestamptz,
    ingested_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (source, external_id, event_type)
);

CREATE TABLE IF NOT EXISTS sync_state (
    source    text        PRIMARY KEY,
    cursor    text        NOT NULL,
    synced_at timestamptz NOT NULL DEFAULT now()
);

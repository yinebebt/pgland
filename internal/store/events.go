package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/yinebebt/pgland/internal/event"
)

// Event is one stored row of raw_events.
type Event struct {
	ID         int64           `json:"id"`
	Source     string          `json:"source"`
	ExternalID string          `json:"external_id"`
	EventType  string          `json:"event_type"`
	Payload    json.RawMessage `json:"payload"`
	OccurredAt *time.Time      `json:"occurred_at"`
	IngestedAt time.Time       `json:"ingested_at"`
}

// Filter narrows Events. An empty Source matches every source.
type Filter struct {
	Source string
	Limit  int
}

const upsertEventSQL = `
INSERT INTO raw_events (source, external_id, event_type, payload, occurred_at)
VALUES ($1, $2, $3, $4::jsonb, $5)
ON CONFLICT (source, external_id, event_type) DO UPDATE
SET payload = EXCLUDED.payload,
    occurred_at = EXCLUDED.occurred_at
WHERE (raw_events.payload, raw_events.occurred_at)
      IS DISTINCT FROM (EXCLUDED.payload, EXCLUDED.occurred_at)`

const listEventsSQL = `
SELECT id, source, external_id, event_type, payload, occurred_at, ingested_at
FROM raw_events
WHERE ($1 = '' OR source = $1)
ORDER BY id DESC
LIMIT $2`

// Save upserts recs on (source, external_id, event_type).
// A repeated record keeps its row id and ingested_at, and an unchanged one
// is not rewritten.
func (s *Store) Save(ctx context.Context, recs []event.Record) error {
	if err := event.ValidateAll(recs); err != nil {
		return err
	}
	if len(recs) == 0 {
		return nil
	}
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		return upsertEvents(ctx, tx, recs)
	})
}

// Events returns the newest rows first.
func (s *Store) Events(ctx context.Context, f Filter) ([]Event, error) {
	rows, err := s.pool.Query(ctx, listEventsSQL, f.Source, f.Limit)
	if err != nil {
		return nil, fmt.Errorf("list events: %w", err)
	}
	events, err := pgx.CollectRows(rows, pgx.RowToStructByPos[Event])
	if err != nil {
		return nil, fmt.Errorf("list events: %w", err)
	}
	return events, nil
}

func upsertEvents(ctx context.Context, tx pgx.Tx, recs []event.Record) error {
	batch := &pgx.Batch{}
	for _, rec := range recs {
		var occurred any
		if !rec.OccurredAt.IsZero() {
			occurred = rec.OccurredAt.UTC()
		}
		batch.Queue(
			upsertEventSQL,
			rec.Source,
			rec.ExternalID,
			rec.EventType,
			string(rec.Payload),
			occurred,
		)
	}
	if err := tx.SendBatch(ctx, batch).Close(); err != nil {
		return fmt.Errorf("upsert events: %w", err)
	}
	return nil
}

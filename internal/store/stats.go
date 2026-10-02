package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// Stats summarizes raw_events. Times are occurred_at, or ingested_at when a
// record has no occurred_at.
type Stats struct {
	Total  int64       `json:"total"`
	ByType []TypeCount `json:"by_type"`
	Daily  []DayCount  `json:"daily"`
}

// TypeCount is the number of events for one source and event type.
type TypeCount struct {
	Source    string    `json:"source"`
	EventType string    `json:"event_type"`
	Count     int64     `json:"count"`
	First     time.Time `json:"first"`
	Last      time.Time `json:"last"`
}

// DayCount is the number of events for one source on one UTC day.
type DayCount struct {
	Day    string `json:"day"`
	Source string `json:"source"`
	Count  int64  `json:"count"`
}

const byTypeSQL = `
SELECT source, event_type, count(*),
       min(coalesce(occurred_at, ingested_at)),
       max(coalesce(occurred_at, ingested_at))
FROM raw_events
GROUP BY source, event_type
ORDER BY source, event_type`

const dailySQL = `
WITH d AS (
    SELECT source, (coalesce(occurred_at, ingested_at) AT TIME ZONE 'UTC')::date AS day
    FROM raw_events
)
SELECT to_char(day, 'YYYY-MM-DD'), source, count(*)
FROM d
WHERE day IN (SELECT DISTINCT day FROM d ORDER BY day DESC LIMIT $1)
GROUP BY day, source
ORDER BY day, source`

// Stats returns counts per source and type, and daily counts for the most
// recent days that have events.
func (s *Store) Stats(ctx context.Context, days int) (Stats, error) {
	rows, err := s.pool.Query(ctx, byTypeSQL)
	if err != nil {
		return Stats{}, fmt.Errorf("stats by type: %w", err)
	}
	byType, err := pgx.CollectRows(rows, pgx.RowToStructByPos[TypeCount])
	if err != nil {
		return Stats{}, fmt.Errorf("stats by type: %w", err)
	}

	rows, err = s.pool.Query(ctx, dailySQL, days)
	if err != nil {
		return Stats{}, fmt.Errorf("stats daily: %w", err)
	}
	daily, err := pgx.CollectRows(rows, pgx.RowToStructByPos[DayCount])
	if err != nil {
		return Stats{}, fmt.Errorf("stats daily: %w", err)
	}

	var total int64
	for _, tc := range byType {
		total += tc.Count
	}
	return Stats{Total: total, ByType: byType, Daily: daily}, nil
}

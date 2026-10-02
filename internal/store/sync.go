package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/yinebebt/pgland/internal/event"
)

// SyncState is the saved position of one pull source.
type SyncState struct {
	Source   string    `json:"source"`
	Cursor   string    `json:"cursor"`
	SyncedAt time.Time `json:"synced_at"`
}

const saveCursorSQL = `
INSERT INTO sync_state (source, cursor, synced_at)
VALUES ($1, $2, now())
ON CONFLICT (source) DO UPDATE
SET cursor = EXCLUDED.cursor,
    synced_at = EXCLUDED.synced_at`

// Cursor returns the saved cursor for source, or "" before its first sync.
func (s *Store) Cursor(ctx context.Context, source string) (string, error) {
	var cursor string
	err := s.pool.QueryRow(ctx, `SELECT cursor FROM sync_state WHERE source = $1`, source).Scan(&cursor)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("read cursor: %w", err)
	}
	return cursor, nil
}

// SaveSync upserts recs and moves source's cursor in one transaction,
// so the cursor never runs ahead of the stored events.
func (s *Store) SaveSync(ctx context.Context, source string, recs []event.Record, cursor string) error {
	if err := event.ValidateAll(recs); err != nil {
		return err
	}
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if len(recs) > 0 {
			if err := upsertEvents(ctx, tx, recs); err != nil {
				return err
			}
		}
		if _, err := tx.Exec(ctx, saveCursorSQL, source, cursor); err != nil {
			return fmt.Errorf("save cursor: %w", err)
		}
		return nil
	})
}

// SyncStates returns every saved cursor.
func (s *Store) SyncStates(ctx context.Context) ([]SyncState, error) {
	rows, err := s.pool.Query(ctx, `SELECT source, cursor, synced_at FROM sync_state ORDER BY source`)
	if err != nil {
		return nil, fmt.Errorf("list sync state: %w", err)
	}
	states, err := pgx.CollectRows(rows, pgx.RowToStructByPos[SyncState])
	if err != nil {
		return nil, fmt.Errorf("list sync state: %w", err)
	}
	return states, nil
}

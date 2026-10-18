package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const (
	SyncAccepted = "accepted"
	SyncFinished = "finished"
	SyncFailed   = "failed"
)

var (
	ErrSyncInProgress = errors.New("sync already running")
	ErrSyncNotFound   = errors.New("sync run not found")
)

// SyncRun is one accepted batch and, later, its finished or failed result.
type SyncRun struct {
	ID         string     `json:"id"`
	Source     string     `json:"source"`
	Status     string     `json:"status"`
	Fetched    *int       `json:"fetched,omitempty"`
	Error      string     `json:"error,omitempty"`
	AcceptedAt time.Time  `json:"accepted_at"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`
}

// AcceptSync inserts one accepted run and returns the time the row was stored.
// A second accepted run for the same source is rejected.
func (s *Store) AcceptSync(ctx context.Context, id, source string) (time.Time, error) {
	var acceptedAt time.Time
	err := s.pool.QueryRow(ctx, `
		INSERT INTO sync_runs (id, source, status)
		VALUES ($1, $2, 'accepted')
		RETURNING accepted_at
	`, id, source).Scan(&acceptedAt)
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		return time.Time{}, ErrSyncInProgress
	}
	if err != nil {
		return time.Time{}, fmt.Errorf("accept sync: %w", err)
	}
	return acceptedAt, nil
}

// GetSync returns one run.
func (s *Store) GetSync(ctx context.Context, id string) (SyncRun, error) {
	var run SyncRun
	err := s.pool.QueryRow(ctx, `
		SELECT id::text, source, status, fetched, COALESCE(error, ''), accepted_at, finished_at
		FROM sync_runs
		WHERE id = $1
	`, id).Scan(&run.ID, &run.Source, &run.Status, &run.Fetched, &run.Error, &run.AcceptedAt, &run.FinishedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return SyncRun{}, ErrSyncNotFound
	}
	if err != nil {
		return SyncRun{}, fmt.Errorf("get sync: %w", err)
	}
	return run, nil
}

// FinishSync marks an accepted run finished. A run that is already finished stays finished.
func (s *Store) FinishSync(ctx context.Context, id string, fetched int) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE sync_runs
		SET status = 'finished', fetched = $2, finished_at = now()
		WHERE id = $1 AND status = 'accepted'
	`, id, fetched)
	if err != nil {
		return fmt.Errorf("finish sync: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	run, err := s.GetSync(ctx, id)
	if err != nil {
		return err
	}
	if run.Status == SyncFinished {
		return nil
	}
	return fmt.Errorf("finish sync: status %s", run.Status)
}

// FailSync marks an accepted run failed so the source can be synced again.
// A run that is not accepted is left unchanged.
func (s *Store) FailSync(ctx context.Context, id, reason string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE sync_runs
		SET status = 'failed', error = $2, finished_at = now()
		WHERE id = $1 AND status = 'accepted'
	`, id, reason)
	if err != nil {
		return fmt.Errorf("fail sync: %w", err)
	}
	return nil
}

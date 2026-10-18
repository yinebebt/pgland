// Package syncjob performs one accepted pull: read the cursor, fetch one batch, store it.
package syncjob

import (
	"context"

	"github.com/yinebebt/pgland/internal/event"
	"github.com/yinebebt/pgland/internal/source"
	"github.com/yinebebt/pgland/internal/store"
)

// Store is the persistence one sync run needs.
type Store interface {
	Cursor(ctx context.Context, source string) (string, error)
	SaveSync(ctx context.Context, source string, recs []event.Record, cursor string) error
	FinishSync(ctx context.Context, id string, fetched int) error
}

// Fetch loads one batch. cursor is the cursor saved by the previous batch, or "" at the start.
type Fetch func(ctx context.Context, cursor string) (source.Batch, error)

// Run finishes an accepted sync run. A run that is not accepted is a no-op,
// so a redelivered queue message does not fetch the batch again.
func Run(ctx context.Context, st Store, fetch Fetch, run store.SyncRun) error {
	if run.Status != store.SyncAccepted {
		return nil
	}
	cursor, err := st.Cursor(ctx, run.Source)
	if err != nil {
		return err
	}
	batch, err := fetch(ctx, cursor)
	if err != nil {
		return err
	}
	if err := st.SaveSync(ctx, run.Source, batch.Records, batch.Next); err != nil {
		return err
	}
	return st.FinishSync(ctx, run.ID, len(batch.Records))
}

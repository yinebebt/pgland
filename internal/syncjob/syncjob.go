// Package syncjob performs one accepted pull: read the cursor, fetch a page, store it.
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

// Fetch loads one page. cursor is the previous page's next URL, or "" at the start.
type Fetch func(ctx context.Context, cursor string) (source.Page, error)

// Run finishes an accepted sync run. A run that is not accepted is a no-op,
// so a redelivered queue message does not fetch the page again.
func Run(ctx context.Context, st Store, fetch Fetch, run store.SyncRun) error {
	if run.Status != store.SyncAccepted {
		return nil
	}
	cursor, err := st.Cursor(ctx, run.Source)
	if err != nil {
		return err
	}
	page, err := fetch(ctx, cursor)
	if err != nil {
		return err
	}
	if err := st.SaveSync(ctx, run.Source, page.Records, page.Next); err != nil {
		return err
	}
	return st.FinishSync(ctx, run.ID, len(page.Records))
}

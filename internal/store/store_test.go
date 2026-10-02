package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/yinebebt/pgland/internal/event"
)

func TestSaveRejectsInvalidBeforeTouchingDB(t *testing.T) {
	t.Parallel()
	err := (&Store{}).Save(context.Background(), []event.Record{{
		Source:     "test",
		ExternalID: "x",
		EventType:  "signup",
		Payload:    json.RawMessage(`not-json`),
	}})
	if !errors.Is(err, event.ErrInvalid) {
		t.Fatalf("err = %v, want ErrInvalid", err)
	}
}

func TestSaveIsIdempotent(t *testing.T) {
	st := openTestStore(t, "store-save")
	ctx := context.Background()
	occurred := time.Date(2024, 6, 1, 15, 0, 0, 0, time.UTC)
	rec := event.Record{
		Source:     "store-save",
		ExternalID: "evt_1",
		EventType:  "signup",
		Payload:    json.RawMessage(`{"n":1}`),
		OccurredAt: occurred,
	}
	if err := st.Save(ctx, []event.Record{rec}); err != nil {
		t.Fatalf("first save: %v", err)
	}
	first := mustEvents(t, st, "store-save")

	rec.Payload = json.RawMessage(`{"n":2}`)
	other := rec
	other.ExternalID = "evt_2"
	if err := st.Save(ctx, []event.Record{rec, other}); err != nil {
		t.Fatalf("second save: %v", err)
	}
	got := mustEvents(t, st, "store-save")

	if len(got) != 2 {
		t.Fatalf("rows = %d, want 2", len(got))
	}
	updated := got[1]
	if updated.ID != first[0].ID || !updated.IngestedAt.Equal(first[0].IngestedAt) {
		t.Fatalf("row was replaced: before %+v after %+v", first[0], updated)
	}
	if compact(t, updated.Payload) != `{"n":2}` {
		t.Fatalf("payload = %s", updated.Payload)
	}
	older, err := st.Events(ctx, Filter{Source: "store-save", Before: got[0].ID, Limit: 10})
	if err != nil || len(older) != 1 || older[0].ID != updated.ID {
		t.Fatalf("before %d = %+v, err %v", got[0].ID, older, err)
	}

	before := rowVersion(t, st, updated.ID)
	if err := st.Save(ctx, []event.Record{rec}); err != nil {
		t.Fatalf("unchanged save: %v", err)
	}
	if after := rowVersion(t, st, updated.ID); after != before {
		t.Fatalf("unchanged record was rewritten: xmin %s -> %s", before, after)
	}
}

func rowVersion(t *testing.T, st *Store, id int64) string {
	t.Helper()
	var xmin string
	if err := st.pool.QueryRow(context.Background(), `SELECT xmin::text FROM raw_events WHERE id = $1`, id).Scan(&xmin); err != nil {
		t.Fatalf("xmin: %v", err)
	}
	return xmin
}

func TestSaveSyncMovesCursorWithEvents(t *testing.T) {
	st := openTestStore(t, "store-sync")
	ctx := context.Background()

	cursor, err := st.Cursor(ctx, "store-sync")
	if err != nil || cursor != "" {
		t.Fatalf("initial cursor = %q, err %v", cursor, err)
	}
	rec := event.Record{
		Source:     "store-sync",
		ExternalID: "r1",
		EventType:  "report",
		Payload:    json.RawMessage(`{}`),
	}
	if err := st.SaveSync(ctx, "store-sync", []event.Record{rec}, "page-2"); err != nil {
		t.Fatalf("save sync: %v", err)
	}
	cursor, err = st.Cursor(ctx, "store-sync")
	if err != nil || cursor != "page-2" {
		t.Fatalf("cursor = %q, err %v", cursor, err)
	}

	bad := rec
	bad.ExternalID = ""
	if err := st.SaveSync(ctx, "store-sync", []event.Record{bad}, "page-3"); err == nil {
		t.Fatal("expected error")
	}
	cursor, _ = st.Cursor(ctx, "store-sync")
	if cursor != "page-2" {
		t.Fatalf("cursor moved on failed save: %q", cursor)
	}

	stats, err := st.Stats(ctx, 30)
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.Total < 1 || len(stats.ByType) == 0 || len(stats.Daily) == 0 {
		t.Fatalf("stats = %+v", stats)
	}
}

func openTestStore(t *testing.T, source string) *Store {
	t.Helper()
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("DATABASE_URL not set")
	}
	st, err := Open(context.Background(), dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	cleanup := func() {
		ctx := context.Background()
		_, _ = st.pool.Exec(ctx, `DELETE FROM raw_events WHERE source = $1`, source)
		_, _ = st.pool.Exec(ctx, `DELETE FROM sync_state WHERE source = $1`, source)
	}
	cleanup()
	t.Cleanup(func() {
		cleanup()
		st.Close()
	})
	return st
}

func mustEvents(t *testing.T, st *Store, source string) []Event {
	t.Helper()
	events, err := st.Events(context.Background(), Filter{Source: source, Limit: 10})
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	return events
}

func compact(t *testing.T, raw []byte) string {
	t.Helper()
	var buf bytes.Buffer
	if err := json.Compact(&buf, raw); err != nil {
		t.Fatalf("compact: %v", err)
	}
	return buf.String()
}

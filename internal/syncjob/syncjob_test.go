package syncjob

import (
	"context"
	"errors"
	"testing"

	"github.com/yinebebt/pgland/internal/event"
	"github.com/yinebebt/pgland/internal/source"
	"github.com/yinebebt/pgland/internal/store"
)

type mem struct {
	run     store.SyncRun
	cursor  string
	records []event.Record
}

func (m *mem) Cursor(context.Context, string) (string, error) { return m.cursor, nil }

func (m *mem) SaveSync(_ context.Context, _ string, recs []event.Record, cursor string) error {
	m.records = append(m.records, recs...)
	m.cursor = cursor
	return nil
}

func (m *mem) FinishSync(_ context.Context, id string, fetched int) error {
	m.run.Status = store.SyncFinished
	m.run.Fetched = &fetched
	m.run.ID = id
	return nil
}

func TestRunResumesCursorAndSkipsAFinishedRun(t *testing.T) {
	m := &mem{run: store.SyncRun{ID: "run-1", Source: "openfda", Status: store.SyncAccepted}}
	var asked []string
	fetch := func(_ context.Context, cursor string) (source.Page, error) {
		asked = append(asked, cursor)
		n := len(asked)
		return source.Page{
			Records: []event.Record{{
				Source: "openfda", ExternalID: "r" + string(rune('0'+n)), EventType: "adverse_event",
				Payload: []byte(`{}`),
			}},
			Next: "page-" + string(rune('0'+n)),
		}, nil
	}

	if err := Run(context.Background(), m, fetch, m.run); err != nil {
		t.Fatal(err)
	}
	if m.cursor != "page-1" || len(m.records) != 1 || m.run.Status != store.SyncFinished {
		t.Fatalf("cursor %q records %d status %s", m.cursor, len(m.records), m.run.Status)
	}

	if err := Run(context.Background(), m, fetch, m.run); err != nil {
		t.Fatal(err)
	}
	if len(asked) != 1 {
		t.Fatalf("finished run was fetched again: %q", asked)
	}
}

func TestRunLeavesCursorWhenFetchFails(t *testing.T) {
	m := &mem{run: store.SyncRun{ID: "run-1", Source: "openfda", Status: store.SyncAccepted}, cursor: "page-1"}
	err := Run(context.Background(), m, func(context.Context, string) (source.Page, error) {
		return source.Page{}, errors.New("openfda unavailable")
	}, m.run)
	if err == nil {
		t.Fatal("expected fetch error")
	}
	if m.cursor != "page-1" || m.run.Status != store.SyncAccepted {
		t.Fatalf("cursor %q status %s", m.cursor, m.run.Status)
	}
}

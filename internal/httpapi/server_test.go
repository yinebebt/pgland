package httpapi

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/yinebebt/pgland/internal/event"
	"github.com/yinebebt/pgland/internal/source"
	"github.com/yinebebt/pgland/internal/store"
)

type memStore struct {
	events  map[string]event.Record
	order   []string
	cursors map[string]string
}

func newMemStore() *memStore {
	return &memStore{events: map[string]event.Record{}, cursors: map[string]string{}}
}

func (m *memStore) Ping(context.Context) error { return nil }

func (m *memStore) Save(_ context.Context, recs []event.Record) error {
	for _, rec := range recs {
		key := rec.Source + "/" + rec.ExternalID + "/" + rec.EventType
		if _, ok := m.events[key]; !ok {
			m.order = append(m.order, key)
		}
		m.events[key] = rec
	}
	return nil
}

func (m *memStore) SaveSync(ctx context.Context, src string, recs []event.Record, cursor string) error {
	m.cursors[src] = cursor
	return m.Save(ctx, recs)
}

func (m *memStore) Cursor(_ context.Context, src string) (string, error) {
	return m.cursors[src], nil
}

func (m *memStore) SyncStates(context.Context) ([]store.SyncState, error) {
	states := []store.SyncState{}
	for src, c := range m.cursors {
		states = append(states, store.SyncState{Source: src, Cursor: c, SyncedAt: time.Now()})
	}
	return states, nil
}

func (m *memStore) Events(_ context.Context, f store.Filter) ([]store.Event, error) {
	out := []store.Event{}
	for i := len(m.order) - 1; i >= 0 && len(out) < f.Limit; i-- {
		rec, id := m.events[m.order[i]], int64(i+1)
		if (f.Source != "" && rec.Source != f.Source) || (f.Before != 0 && id >= f.Before) {
			continue
		}
		out = append(out, store.Event{ID: id, Source: rec.Source, ExternalID: rec.ExternalID, EventType: rec.EventType, Payload: rec.Payload})
	}
	return out, nil
}

func (m *memStore) Stats(context.Context, int) (store.Stats, error) {
	return store.Stats{Total: int64(len(m.events)), ByType: []store.TypeCount{}, Daily: []store.DayCount{}}, nil
}

// pagedSource returns one record per call with Next set to "page-<n+1>",
// and records each cursor it was asked for.
type pagedSource struct {
	asked []string
}

func (p *pagedSource) Name() string { return "feed" }

func (p *pagedSource) Fetch(_ context.Context, cursor string) (source.Page, error) {
	p.asked = append(p.asked, cursor)
	n := len(p.asked)
	return source.Page{
		Records: []event.Record{{
			Source:     "feed",
			ExternalID: "item-" + strconv.Itoa(n),
			EventType:  "item",
			Payload:    json.RawMessage(`{}`),
		}},
		Next: "page-" + strconv.Itoa(n+1),
	}, nil
}

func newTestHandler(st Store, secret string, sources ...Puller) http.Handler {
	return Handler(Config{
		Log:          slog.New(slog.NewTextHandler(io.Discard, nil)),
		Store:        st,
		Sources:      sources,
		IngestSecret: secret,
		Version:      "test",
	})
}

func do(t *testing.T, h http.Handler, method, path string, body []byte, header http.Header) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, bytes.NewReader(body))
	for k, v := range header {
		req.Header[k] = v
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestSyncResumesFromSavedCursor(t *testing.T) {
	t.Parallel()
	st := newMemStore()
	src := &pagedSource{}
	h := newTestHandler(st, "", src)

	for range 2 {
		if rec := do(t, h, http.MethodPost, "/sources/feed/sync", nil, nil); rec.Code != http.StatusOK {
			t.Fatalf("sync status = %d body %s", rec.Code, rec.Body)
		}
	}
	if len(src.asked) != 2 || src.asked[0] != "" || src.asked[1] != "page-2" {
		t.Fatalf("cursors asked = %q", src.asked)
	}
	if len(st.events) != 2 || st.cursors["feed"] != "page-3" {
		t.Fatalf("events = %d, cursor = %q", len(st.events), st.cursors["feed"])
	}
}

// blockingSource holds Fetch open until release is closed.
type blockingSource struct {
	started chan struct{}
	release chan struct{}
}

func (b *blockingSource) Name() string { return "slow" }

func (b *blockingSource) Fetch(context.Context, string) (source.Page, error) {
	close(b.started)
	<-b.release
	return source.Page{Next: "next"}, nil
}

func TestSyncRejectsConcurrentRun(t *testing.T) {
	t.Parallel()
	src := &blockingSource{started: make(chan struct{}), release: make(chan struct{})}
	h := newTestHandler(newMemStore(), "", src)

	first := make(chan int)
	go func() {
		first <- do(t, h, http.MethodPost, "/sources/slow/sync", nil, nil).Code
	}()
	<-src.started
	if rec := do(t, h, http.MethodPost, "/sources/slow/sync", nil, nil); rec.Code != http.StatusConflict {
		t.Errorf("concurrent sync status = %d, want 409", rec.Code)
	}
	close(src.release)
	if code := <-first; code != http.StatusOK {
		t.Fatalf("first sync status = %d", code)
	}
}

func TestSyncUnknownSource(t *testing.T) {
	t.Parallel()
	rec := do(t, newTestHandler(newMemStore(), ""), http.MethodPost, "/sources/nope/sync", nil, nil)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d", rec.Code)
	}
}

func TestIngest(t *testing.T) {
	t.Parallel()
	const secret = "s3cret"
	body := []byte(`[{"id":"evt_1","type":"signup","occurred_at":"2026-10-01T10:00:00Z","data":{"plan":"pro"}}]`)
	sign := func(b []byte) http.Header {
		mac := hmac.New(sha256.New, []byte(secret))
		mac.Write(b)
		return http.Header{SignatureHeader: {"sha256=" + hex.EncodeToString(mac.Sum(nil))}}
	}

	tests := []struct {
		name     string
		secret   string
		path     string
		body     []byte
		header   http.Header
		wantCode int
	}{
		{name: "unsigned when no secret", path: "/events/app", body: body, wantCode: http.StatusOK},
		{name: "signed", secret: secret, path: "/events/app", body: body, header: sign(body), wantCode: http.StatusOK},
		{name: "missing signature", secret: secret, path: "/events/app", body: body, wantCode: http.StatusUnauthorized},
		{name: "wrong signature", secret: secret, path: "/events/app", body: body, header: sign([]byte("other")), wantCode: http.StatusUnauthorized},
		{name: "bad source name", path: "/events/Bad%20Name", body: body, wantCode: http.StatusBadRequest},
		{name: "not an array", path: "/events/app", body: []byte(`{"id":"x"}`), wantCode: http.StatusBadRequest},
		{name: "missing id", path: "/events/app", body: []byte(`[{"type":"signup","data":{}}]`), wantCode: http.StatusBadRequest},
		{name: "missing data", path: "/events/app", body: []byte(`[{"id":"x","type":"signup"}]`), wantCode: http.StatusBadRequest},
		{name: "too large", path: "/events/app", body: bytes.Repeat([]byte(" "), maxIngestBytes+1), wantCode: http.StatusRequestEntityTooLarge},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			st := newMemStore()
			rec := do(t, newTestHandler(st, tt.secret), http.MethodPost, tt.path, tt.body, tt.header)
			if rec.Code != tt.wantCode {
				t.Fatalf("status = %d, want %d, body %s", rec.Code, tt.wantCode, rec.Body)
			}
			if tt.wantCode == http.StatusOK && len(st.events) != 1 {
				t.Fatalf("stored %d events", len(st.events))
			}
		})
	}
}

func TestIngestTwiceKeepsOneEvent(t *testing.T) {
	t.Parallel()
	st := newMemStore()
	h := newTestHandler(st, "")
	body := []byte(`[{"id":"evt_1","type":"signup","data":{}}]`)
	for range 2 {
		do(t, h, http.MethodPost, "/events/app", body, nil)
	}
	rec := do(t, h, http.MethodGet, "/events?source=app", nil, nil)
	var got struct {
		Events []store.Event `json:"events"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if len(got.Events) != 1 || got.Events[0].ExternalID != "evt_1" {
		t.Fatalf("events = %+v", got.Events)
	}
}

func TestListEventsPages(t *testing.T) {
	t.Parallel()
	h := newTestHandler(newMemStore(), "")
	do(t, h, http.MethodPost, "/events/app", []byte(`[
		{"id":"a","type":"t","data":{}},
		{"id":"b","type":"t","data":{}},
		{"id":"c","type":"t","data":{}}
	]`), nil)

	type page struct {
		Events     []store.Event `json:"events"`
		NextBefore *int64        `json:"next_before"`
	}
	get := func(path string) page {
		t.Helper()
		var p page
		if err := json.Unmarshal(do(t, h, http.MethodGet, path, nil, nil).Body.Bytes(), &p); err != nil {
			t.Fatal(err)
		}
		return p
	}
	ids := func(p page) (out []string) {
		for _, e := range p.Events {
			out = append(out, e.ExternalID)
		}
		return out
	}

	first := get("/events?limit=2")
	if got := ids(first); len(got) != 2 || got[0] != "c" || got[1] != "b" || first.NextBefore == nil {
		t.Fatalf("first page = %v, next_before %v", got, first.NextBefore)
	}
	last := get("/events?limit=2&before=" + strconv.FormatInt(*first.NextBefore, 10))
	if got := ids(last); len(got) != 1 || got[0] != "a" || last.NextBefore != nil {
		t.Fatalf("last page = %v, next_before %v", got, last.NextBefore)
	}
}

func TestDashboardAndHealth(t *testing.T) {
	t.Parallel()
	h := newTestHandler(newMemStore(), "")
	rec := do(t, h, http.MethodGet, "/", nil, nil)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "<title>pgland</title>") {
		t.Fatalf("dashboard status = %d", rec.Code)
	}
	rec = do(t, h, http.MethodGet, "/healthz", nil, nil)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"version":"test"`) {
		t.Fatalf("healthz = %d %s", rec.Code, rec.Body)
	}
	if rec := do(t, h, http.MethodGet, "/nope", nil, nil); rec.Code != http.StatusNotFound {
		t.Fatalf("unknown path = %d", rec.Code)
	}
}

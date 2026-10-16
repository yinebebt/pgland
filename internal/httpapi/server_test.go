package httpapi

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/yinebebt/pgland/internal/event"
	"github.com/yinebebt/pgland/internal/source"
	"github.com/yinebebt/pgland/internal/store"
)

type memStore struct {
	events   map[string]event.Record
	order    []string
	cursors  map[string]string
	runs     map[string]store.SyncRun
	accepted map[string]string
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

func (m *memStore) SyncStates(context.Context) ([]store.SyncState, error) {
	states := []store.SyncState{}
	for src, c := range m.cursors {
		states = append(states, store.SyncState{Source: src, Cursor: c, SyncedAt: time.Now()})
	}
	return states, nil
}

func (m *memStore) Events(_ context.Context, f store.Filter) ([]store.Event, error) {
	out := []store.Event{}
	for i, key := range m.order {
		rec := m.events[key]
		if f.Source != "" && rec.Source != f.Source {
			continue
		}
		out = append(out, store.Event{ID: int64(i + 1), Source: rec.Source, ExternalID: rec.ExternalID, EventType: rec.EventType, Payload: rec.Payload})
	}
	return out, nil
}

func (m *memStore) Stats(context.Context, int) (store.Stats, error) {
	return store.Stats{Total: int64(len(m.events)), ByType: []store.TypeCount{}, Daily: []store.DayCount{}}, nil
}

func (m *memStore) AcceptSync(_ context.Context, id, source string) error {
	if m.runs == nil {
		m.runs = map[string]store.SyncRun{}
		m.accepted = map[string]string{}
	}
	if _, ok := m.accepted[source]; ok {
		return store.ErrSyncInProgress
	}
	m.runs[id] = store.SyncRun{ID: id, Source: source, Status: store.SyncAccepted, AcceptedAt: time.Now()}
	m.accepted[source] = id
	return nil
}

func (m *memStore) GetSync(_ context.Context, id string) (store.SyncRun, error) {
	run, ok := m.runs[id]
	if !ok {
		return store.SyncRun{}, store.ErrSyncNotFound
	}
	return run, nil
}

func (m *memStore) FailSync(_ context.Context, id, reason string) error {
	run, ok := m.runs[id]
	if !ok {
		return nil
	}
	delete(m.accepted, run.Source)
	run.Status = store.SyncFailed
	run.Error = reason
	m.runs[id] = run
	return nil
}

type memQueue struct {
	ids  []string
	fail bool
}

func (q *memQueue) Enqueue(_ context.Context, id string) error {
	if q.fail {
		return errors.New("queue down")
	}
	q.ids = append(q.ids, id)
	return nil
}

// namedSource is a pull source the handler must not fetch itself.
type namedSource struct{}

func (namedSource) Name() string { return "feed" }

func (namedSource) Fetch(context.Context, string) (source.Page, error) {
	return source.Page{}, errors.New("handler fetched a page")
}

func newTestHandler(st Store, secret string, sources ...Puller) http.Handler {
	return Handler(Config{
		Log:          slog.New(slog.NewTextHandler(io.Discard, nil)),
		Store:        st,
		Queue:        &memQueue{},
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

func TestSyncAcceptsOntoTheQueue(t *testing.T) {
	t.Parallel()
	st := newMemStore()
	q := &memQueue{}
	h := Handler(Config{
		Log:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		Store:   st,
		Queue:   q,
		Sources: []Puller{namedSource{}},
		Version: "test",
	})

	rec := do(t, h, http.MethodPost, "/sources/feed/sync", nil, nil)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d body %s", rec.Code, rec.Body)
	}
	var run store.SyncRun
	if err := json.Unmarshal(rec.Body.Bytes(), &run); err != nil {
		t.Fatal(err)
	}
	if run.Status != store.SyncAccepted || run.Source != "feed" || len(q.ids) != 1 || q.ids[0] != run.ID {
		t.Fatalf("run %+v queue %v", run, q.ids)
	}
	got := do(t, h, http.MethodGet, "/sources/feed/syncs/"+run.ID, nil, nil)
	if got.Code != http.StatusOK {
		t.Fatalf("get status = %d", got.Code)
	}
}

func TestSyncRejectsConcurrentRun(t *testing.T) {
	t.Parallel()
	st := newMemStore()
	h := newTestHandler(st, "", namedSource{})
	if rec := do(t, h, http.MethodPost, "/sources/feed/sync", nil, nil); rec.Code != http.StatusAccepted {
		t.Fatalf("first status = %d", rec.Code)
	}
	if rec := do(t, h, http.MethodPost, "/sources/feed/sync", nil, nil); rec.Code != http.StatusConflict {
		t.Fatalf("second status = %d, want 409", rec.Code)
	}
}

func TestSyncEnqueueFailureReleasesTheSource(t *testing.T) {
	t.Parallel()
	st := newMemStore()
	h := Handler(Config{
		Log:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		Store:   st,
		Queue:   &memQueue{fail: true},
		Sources: []Puller{namedSource{}},
		Version: "test",
	})
	if rec := do(t, h, http.MethodPost, "/sources/feed/sync", nil, nil); rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d", rec.Code)
	}
	if _, ok := st.accepted["feed"]; ok {
		t.Fatal("accepted run left behind after enqueue failure")
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

func TestHealth(t *testing.T) {
	t.Parallel()
	h := newTestHandler(newMemStore(), "")
	rec := do(t, h, http.MethodGet, "/healthz", nil, nil)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"version":"test"`) {
		t.Fatalf("healthz = %d %s", rec.Code, rec.Body)
	}
	if rec := do(t, h, http.MethodGet, "/nope", nil, nil); rec.Code != http.StatusNotFound {
		t.Fatalf("unknown path = %d", rec.Code)
	}
}

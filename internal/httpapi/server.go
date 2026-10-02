// Package httpapi is the HTTP surface: sync, push ingest, reads, and the dashboard.
package httpapi

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
	"sync"

	"github.com/yinebebt/pgland/internal/event"
	"github.com/yinebebt/pgland/internal/source"
	"github.com/yinebebt/pgland/internal/store"
)

// Store is the persistence the handlers need.
type Store interface {
	Ping(ctx context.Context) error
	Save(ctx context.Context, recs []event.Record) error
	SaveSync(ctx context.Context, source string, recs []event.Record, cursor string) error
	Cursor(ctx context.Context, source string) (string, error)
	SyncStates(ctx context.Context) ([]store.SyncState, error)
	Events(ctx context.Context, f store.Filter) ([]store.Event, error)
	Stats(ctx context.Context, days int) (store.Stats, error)
}

// Puller fetches one page of a pull source.
type Puller interface {
	Name() string
	Fetch(ctx context.Context, cursor string) (source.Page, error)
}

// Config wires the handler.
type Config struct {
	Log     *slog.Logger
	Store   Store
	Sources []Puller
	// IngestSecret, when set, requires a valid signature on POST /events/{source}.
	IngestSecret string
	Version      string
}

type api struct {
	log     *slog.Logger
	store   Store
	sources map[string]*pullSource
	names   []string
	secret  []byte
	version string
}

// pullSource serializes syncs of one source so concurrent calls do not
// fetch the same page twice.
type pullSource struct {
	Puller
	mu sync.Mutex
}

// Handler returns the service's routes.
func Handler(c Config) http.Handler {
	a := &api{
		log:     c.Log,
		store:   c.Store,
		sources: make(map[string]*pullSource, len(c.Sources)),
		secret:  []byte(c.IngestSecret),
		version: c.Version,
	}
	for _, src := range c.Sources {
		a.sources[src.Name()] = &pullSource{Puller: src}
		a.names = append(a.names, src.Name())
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", a.dashboard)
	mux.HandleFunc("GET /healthz", a.healthz)
	mux.HandleFunc("GET /sources", a.listSources)
	mux.HandleFunc("POST /sources/{name}/sync", a.sync)
	mux.HandleFunc("POST /events/{source}", a.ingest)
	mux.HandleFunc("GET /events", a.listEvents)
	mux.HandleFunc("GET /stats", a.stats)
	return a.recoverPanics(mux)
}

func (a *api) healthz(w http.ResponseWriter, r *http.Request) {
	if err := a.store.Ping(r.Context()); err != nil {
		a.fail(w, r, http.StatusServiceUnavailable, "database unreachable", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "version": a.version})
}

func (a *api) stats(w http.ResponseWriter, r *http.Request) {
	days := intParam(r, "days", 30, 366)
	stats, err := a.store.Stats(r.Context(), days)
	if err != nil {
		a.fail(w, r, http.StatusInternalServerError, "stats failed", err)
		return
	}
	writeJSON(w, http.StatusOK, stats)
}

func (a *api) recoverPanics(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				a.log.ErrorContext(r.Context(), "panic", "err", rec, "path", r.URL.Path)
				writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// fail writes {"error": msg}. A non-nil err is logged, never sent.
func (a *api) fail(w http.ResponseWriter, r *http.Request, code int, msg string, err error) {
	if err != nil {
		a.log.ErrorContext(r.Context(), msg, "err", err, "path", r.URL.Path)
	}
	writeJSON(w, code, map[string]string{"error": msg})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// intParam reads a positive query integer, falling back to def and capping at maxValue.
func intParam(r *http.Request, key string, def, maxValue int) int {
	n, err := strconv.Atoi(r.URL.Query().Get(key))
	if err != nil || n < 1 {
		return def
	}
	return min(n, maxValue)
}

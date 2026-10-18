package httpapi

import (
	"crypto/rand"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/yinebebt/pgland/internal/store"
)

type sourceView struct {
	Name     string     `json:"name"`
	Cursor   string     `json:"cursor"`
	SyncedAt *time.Time `json:"synced_at"`
}

func (a *api) listSources(w http.ResponseWriter, r *http.Request) {
	states, err := a.store.SyncStates(r.Context())
	if err != nil {
		a.fail(w, r, http.StatusInternalServerError, "list sources failed", err)
		return
	}
	byName := make(map[string]sourceView, len(states))
	for _, st := range states {
		byName[st.Source] = sourceView{Name: st.Source, Cursor: st.Cursor, SyncedAt: &st.SyncedAt}
	}
	views := make([]sourceView, 0, len(a.names))
	for _, name := range a.names {
		v, ok := byName[name]
		if !ok {
			v = sourceView{Name: name}
		}
		views = append(views, v)
	}
	writeJSON(w, http.StatusOK, map[string]any{"sources": views})
}

// sync accepts one batch. The worker fetches it and the caller reads the run.
func (a *api) sync(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if _, ok := a.sources[name]; !ok {
		a.fail(w, r, http.StatusNotFound, "unknown source", nil)
		return
	}
	if a.queue == nil {
		a.fail(w, r, http.StatusServiceUnavailable, "queue not configured", nil)
		return
	}
	id, err := newRunID()
	if err != nil {
		a.fail(w, r, http.StatusInternalServerError, "accept sync failed", err)
		return
	}
	acceptedAt, err := a.store.AcceptSync(r.Context(), id, name)
	if err != nil {
		if errors.Is(err, store.ErrSyncInProgress) {
			a.fail(w, r, http.StatusConflict, "sync already running", nil)
			return
		}
		a.fail(w, r, http.StatusInternalServerError, "accept sync failed", err)
		return
	}
	if err := a.queue.Enqueue(r.Context(), id); err != nil {
		if failErr := a.store.FailSync(r.Context(), id, "enqueue failed"); failErr != nil {
			a.fail(w, r, http.StatusInternalServerError, "enqueue sync failed", errors.Join(err, failErr))
			return
		}
		a.fail(w, r, http.StatusBadGateway, "enqueue sync failed", err)
		return
	}
	a.log.InfoContext(r.Context(), "sync accepted", "source", name, "run", id)
	writeJSON(w, http.StatusAccepted, store.SyncRun{
		ID:         id,
		Source:     name,
		Status:     store.SyncAccepted,
		AcceptedAt: acceptedAt,
	})
}

func (a *api) getSync(w http.ResponseWriter, r *http.Request) {
	run, err := a.store.GetSync(r.Context(), r.PathValue("id"))
	if errors.Is(err, store.ErrSyncNotFound) || (err == nil && run.Source != r.PathValue("name")) {
		a.fail(w, r, http.StatusNotFound, "sync run not found", nil)
		return
	}
	if err != nil {
		a.fail(w, r, http.StatusInternalServerError, "read sync failed", err)
		return
	}
	writeJSON(w, http.StatusOK, run)
}

func newRunID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("new sync id: %w", err)
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:]), nil
}

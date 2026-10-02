package httpapi

import (
	"net/http"
	"time"
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

// sync fetches the page after the saved cursor and stores it with the new cursor.
func (a *api) sync(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	src, ok := a.sources[name]
	if !ok {
		a.fail(w, r, http.StatusNotFound, "unknown source", nil)
		return
	}
	if !src.mu.TryLock() {
		a.fail(w, r, http.StatusConflict, "sync already running", nil)
		return
	}
	defer src.mu.Unlock()
	ctx := r.Context()
	cursor, err := a.store.Cursor(ctx, name)
	if err != nil {
		a.fail(w, r, http.StatusInternalServerError, "read cursor failed", err)
		return
	}
	page, err := src.Fetch(ctx, cursor)
	if err != nil {
		a.fail(w, r, http.StatusBadGateway, "source fetch failed", err)
		return
	}
	if err := a.store.SaveSync(ctx, name, page.Records, page.Next); err != nil {
		a.fail(w, r, http.StatusInternalServerError, "save failed", err)
		return
	}
	a.log.InfoContext(ctx, "sync complete", "source", name, "fetched", len(page.Records))
	writeJSON(w, http.StatusOK, map[string]any{
		"source":  name,
		"fetched": len(page.Records),
		"cursor":  page.Next,
	})
}

package httpapi

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/yinebebt/pgland/internal/event"
	"github.com/yinebebt/pgland/internal/store"
)

const (
	// SignatureHeader carries "sha256=" + hex(HMAC-SHA256(secret, body)).
	SignatureHeader = "X-Pgland-Signature"
	maxIngestBytes  = 1 << 20
	maxIngestEvents = 1000
)

type pushedEvent struct {
	ID         string          `json:"id"`
	Type       string          `json:"type"`
	OccurredAt *time.Time      `json:"occurred_at"`
	Data       json.RawMessage `json:"data"`
}

// ingest stores a JSON array of pushed events under the path's source name.
func (a *api) ingest(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("source")
	if !event.ValidName(name) {
		a.fail(w, r, http.StatusBadRequest, "invalid source name", nil)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxIngestBytes))
	if err != nil {
		if _, ok := errors.AsType[*http.MaxBytesError](err); ok {
			a.fail(w, r, http.StatusRequestEntityTooLarge, "body too large", nil)
			return
		}
		a.fail(w, r, http.StatusBadRequest, "read body failed", nil)
		return
	}
	if len(a.secret) > 0 && !validSignature(a.secret, body, r.Header.Get(SignatureHeader)) {
		a.fail(w, r, http.StatusUnauthorized, "invalid signature", nil)
		return
	}

	var pushed []pushedEvent
	if err := json.Unmarshal(body, &pushed); err != nil {
		a.fail(w, r, http.StatusBadRequest, "body must be a JSON array of events", nil)
		return
	}
	if len(pushed) > maxIngestEvents {
		a.fail(w, r, http.StatusRequestEntityTooLarge, fmt.Sprintf("at most %d events per request", maxIngestEvents), nil)
		return
	}
	recs := make([]event.Record, 0, len(pushed))
	for _, p := range pushed {
		rec := event.Record{Source: name, ExternalID: p.ID, EventType: p.Type, Payload: p.Data}
		if p.OccurredAt != nil {
			rec.OccurredAt = *p.OccurredAt
		}
		recs = append(recs, rec)
	}
	if err := event.ValidateAll(recs); err != nil {
		a.fail(w, r, http.StatusBadRequest, err.Error(), nil)
		return
	}
	if err := a.store.Save(r.Context(), recs); err != nil {
		a.fail(w, r, http.StatusInternalServerError, "save failed", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"source": name, "accepted": len(recs)})
}

func (a *api) listEvents(w http.ResponseWriter, r *http.Request) {
	events, err := a.store.Events(r.Context(), store.Filter{
		Source: r.URL.Query().Get("source"),
		Limit:  intParam(r, "limit", 50, 500),
	})
	if err != nil {
		a.fail(w, r, http.StatusInternalServerError, "list events failed", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"events": events})
}

func validSignature(secret, body []byte, header string) bool {
	got, ok := strings.CutPrefix(header, "sha256=")
	if !ok {
		return false
	}
	sig, err := hex.DecodeString(got)
	if err != nil {
		return false
	}
	mac := hmac.New(sha256.New, secret)
	mac.Write(body)
	return hmac.Equal(sig, mac.Sum(nil))
}

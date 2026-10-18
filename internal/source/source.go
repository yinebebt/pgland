// Package source pulls events from HTTP JSON APIs described in a config file.
//
// Each Fetch reads one batch. For this source a batch is one response page.
// Paging follows the response's Link rel="next" header (RFC 8288), and the
// next page URL is the cursor saved for the next sync.
package source

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/yinebebt/pgland/internal/event"
)

const maxBodyBytes = 8 << 20

// Config describes one pull source. Fields ending in Path or Field are
// dot-separated keys into the JSON body, e.g. "data.items".
type Config struct {
	Name      string `json:"name"`
	EventType string `json:"event_type"`
	URL       string `json:"url"`
	// Headers are sent on every request. Values expand $VAR from the environment.
	Headers     map[string]string `json:"headers,omitempty"`
	RecordsPath string            `json:"records_path"`
	IDField     string            `json:"id_field"`
	TimeField   string            `json:"time_field,omitempty"`
	// TimeLayout is a Go time layout. Empty means RFC 3339.
	TimeLayout string `json:"time_layout,omitempty"`
}

// Load reads {"sources": [...]} from path. A missing file means no sources.
func Load(path string) ([]Config, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read sources: %w", err)
	}
	var file struct {
		Sources []Config `json:"sources"`
	}
	if err := json.Unmarshal(data, &file); err != nil {
		return nil, fmt.Errorf("parse sources: %w", err)
	}
	seen := make(map[string]bool, len(file.Sources))
	for i, c := range file.Sources {
		if err := c.validate(); err != nil {
			return nil, fmt.Errorf("source %d: %w", i, err)
		}
		if seen[c.Name] {
			return nil, fmt.Errorf("source %d: duplicate name %q", i, c.Name)
		}
		seen[c.Name] = true
	}
	return file.Sources, nil
}

func (c Config) validate() error {
	if !event.ValidName(c.Name) {
		return fmt.Errorf("invalid name %q", c.Name)
	}
	if c.EventType == "" {
		return errors.New("missing event_type")
	}
	if c.IDField == "" {
		return errors.New("missing id_field")
	}
	u, err := url.Parse(c.URL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return fmt.Errorf("invalid url %q", c.URL)
	}
	return nil
}

// Batch is one fetch: the records read and the cursor for the following Fetch.
// Next is the next page URL, or the fetched URL when the response has no next link.
type Batch struct {
	Records []event.Record
	Next    string
}

// HTTP is a pull source backed by a JSON API.
type HTTP struct {
	cfg    Config
	origin *url.URL
	client *http.Client
}

// New returns a source for cfg. cfg must have passed Load.
func New(cfg Config, client *http.Client) *HTTP {
	origin, _ := url.Parse(cfg.URL)
	return &HTTP{cfg: cfg, origin: origin, client: client}
}

// Name returns the configured source name.
func (h *HTTP) Name() string { return h.cfg.Name }

// Fetch reads one batch at cursor, or the configured URL when cursor is empty.
// Without a next link, Next repeats the fetched URL so the following sync
// picks up anything appended to the last page.
func (h *HTTP) Fetch(ctx context.Context, cursor string) (Batch, error) {
	target := h.cfg.URL
	if cursor != "" {
		target = cursor
	}
	u, err := url.Parse(target)
	if err != nil || !h.sameOrigin(u) {
		return Batch{}, fmt.Errorf("cursor is not on %s", h.origin.Host)
	}

	body, header, err := h.get(ctx, u)
	if err != nil {
		return Batch{}, err
	}
	recs, err := h.parse(body)
	if err != nil {
		return Batch{}, err
	}

	next := target
	if link := nextLink(header.Values("Link"), u); link != nil && h.sameOrigin(link) {
		next = link.String()
	}
	return Batch{Records: recs, Next: next}, nil
}

func (h *HTTP) get(ctx context.Context, u *url.URL) ([]byte, http.Header, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	for k, v := range h.cfg.Headers {
		req.Header.Set(k, os.ExpandEnv(v))
	}
	resp, err := h.client.Do(req)
	if err != nil {
		return nil, nil, fmt.Errorf("fetch: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, nil, fmt.Errorf("fetch: status %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes+1))
	if err != nil {
		return nil, nil, fmt.Errorf("read body: %w", err)
	}
	if len(body) > maxBodyBytes {
		return nil, nil, fmt.Errorf("response larger than %d MiB", maxBodyBytes>>20)
	}
	return body, resp.Header, nil
}

func (h *HTTP) parse(body []byte) ([]event.Record, error) {
	list, err := lookup(body, h.cfg.RecordsPath)
	if err != nil {
		return nil, fmt.Errorf("records_path: %w", err)
	}
	var items []json.RawMessage
	if err := json.Unmarshal(list, &items); err != nil {
		return nil, fmt.Errorf("records_path is not an array: %w", err)
	}

	recs := make([]event.Record, 0, len(items))
	for i, item := range items {
		id, err := scalarAt(item, h.cfg.IDField)
		if err != nil || id == "" {
			return nil, fmt.Errorf("record %d: id_field %q missing", i, h.cfg.IDField)
		}
		rec := event.Record{
			Source:     h.cfg.Name,
			ExternalID: id,
			EventType:  h.cfg.EventType,
			Payload:    item,
		}
		if h.cfg.TimeField != "" {
			if rec.OccurredAt, err = h.timeAt(item); err != nil {
				return nil, fmt.Errorf("record %d: %w", i, err)
			}
		}
		recs = append(recs, rec)
	}
	return recs, nil
}

func (h *HTTP) timeAt(item json.RawMessage) (time.Time, error) {
	raw, err := scalarAt(item, h.cfg.TimeField)
	if err != nil || raw == "" {
		return time.Time{}, nil
	}
	layout := h.cfg.TimeLayout
	if layout == "" {
		layout = time.RFC3339
	}
	t, err := time.Parse(layout, raw)
	if err != nil {
		return time.Time{}, fmt.Errorf("time_field %q: %w", h.cfg.TimeField, err)
	}
	return t.UTC(), nil
}

func (h *HTTP) sameOrigin(u *url.URL) bool {
	return u.Scheme == h.origin.Scheme && u.Host == h.origin.Host
}

// lookup walks a dot-separated path of object keys. An empty path is raw.
func lookup(raw json.RawMessage, path string) (json.RawMessage, error) {
	if path == "" {
		return raw, nil
	}
	for key := range strings.SplitSeq(path, ".") {
		var obj map[string]json.RawMessage
		if err := json.Unmarshal(raw, &obj); err != nil {
			return nil, fmt.Errorf("%q is not inside an object", key)
		}
		v, ok := obj[key]
		if !ok {
			return nil, fmt.Errorf("key %q not found", key)
		}
		raw = v
	}
	return raw, nil
}

// scalarAt returns a string or number at path as text.
func scalarAt(raw json.RawMessage, path string) (string, error) {
	v, err := lookup(raw, path)
	if err != nil {
		return "", err
	}
	var s string
	if err := json.Unmarshal(v, &s); err == nil {
		return s, nil
	}
	var n json.Number
	if err := json.Unmarshal(v, &n); err != nil {
		return "", errors.New("not a string or number")
	}
	return n.String(), nil
}

// nextLink returns the rel="next" target from Link headers, resolved against base.
func nextLink(headers []string, base *url.URL) *url.URL {
	for _, header := range headers {
		for part := range strings.SplitSeq(header, ",") {
			target, params, ok := strings.Cut(strings.TrimSpace(part), ";")
			if !ok || !isNextRel(params) {
				continue
			}
			target = strings.TrimSpace(target)
			if !strings.HasPrefix(target, "<") || !strings.HasSuffix(target, ">") {
				continue
			}
			u, err := base.Parse(target[1 : len(target)-1])
			if err != nil {
				continue
			}
			return u
		}
	}
	return nil
}

func isNextRel(params string) bool {
	for param := range strings.SplitSeq(params, ";") {
		key, value, ok := strings.Cut(strings.TrimSpace(param), "=")
		if !ok || !strings.EqualFold(strings.TrimSpace(key), "rel") {
			continue
		}
		for rel := range strings.FieldsSeq(strings.Trim(strings.TrimSpace(value), `"`)) {
			if strings.EqualFold(rel, "next") {
				return true
			}
		}
	}
	return false
}

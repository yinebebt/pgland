package source

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoad(t *testing.T) {
	t.Parallel()
	valid := `{"name":"feed","event_type":"item","url":"https://example.com/x","id_field":"id"}`
	tests := []struct {
		name    string
		body    string
		wantN   int
		wantErr string
	}{
		{name: "valid", body: `{"sources":[` + valid + `]}`, wantN: 1},
		{name: "duplicate", body: `{"sources":[` + valid + `,` + valid + `]}`, wantErr: "duplicate"},
		{name: "bad name", body: `{"sources":[{"name":"Bad Name","event_type":"x","url":"https://e.com","id_field":"id"}]}`, wantErr: "invalid name"},
		{name: "missing id", body: `{"sources":[{"name":"a","event_type":"x","url":"https://e.com"}]}`, wantErr: "id_field"},
		{name: "bad url", body: `{"sources":[{"name":"a","event_type":"x","url":"ftp://e.com","id_field":"id"}]}`, wantErr: "invalid url"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(t.TempDir(), "sources.json")
			if err := os.WriteFile(path, []byte(tt.body), 0o600); err != nil {
				t.Fatal(err)
			}
			got, err := Load(path)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("err = %v, want %q", err, tt.wantErr)
				}
				return
			}
			if err != nil || len(got) != tt.wantN {
				t.Fatalf("got %d sources, err %v", len(got), err)
			}
		})
	}
}

func TestLoadMissingFileIsEmpty(t *testing.T) {
	t.Parallel()
	got, err := Load(filepath.Join(t.TempDir(), "nope.json"))
	if err != nil || got != nil {
		t.Fatalf("got %v, err %v", got, err)
	}
}

func TestFetchFollowsLinkHeader(t *testing.T) {
	t.Setenv("FEED_TOKEN", "secret")
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer secret" {
			t.Errorf("authorization = %q", r.Header.Get("Authorization"))
		}
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Query().Get("page") == "2" {
			_, _ = w.Write([]byte(`{"data":{"items":[{"id":3,"at":"20240103"}]}}`))
			return
		}
		w.Header().Set("Link", `<`+srv.URL+`/feed?page=2>; rel="next", <`+srv.URL+`/feed?page=9>; rel="last"`)
		_, _ = w.Write([]byte(`{"data":{"items":[{"id":"a","at":"20240101"},{"id":2,"at":"20240102"}]}}`))
	}))
	t.Cleanup(srv.Close)

	h := New(Config{
		Name:        "feed",
		EventType:   "item",
		URL:         srv.URL + "/feed",
		Headers:     map[string]string{"Authorization": "Bearer $FEED_TOKEN"},
		RecordsPath: "data.items",
		IDField:     "id",
		TimeField:   "at",
		TimeLayout:  "20060102",
	}, srv.Client())

	page, err := h.Fetch(context.Background(), "")
	if err != nil {
		t.Fatalf("first fetch: %v", err)
	}
	if len(page.Records) != 2 || page.Records[0].ExternalID != "a" || page.Records[1].ExternalID != "2" {
		t.Fatalf("records = %+v", page.Records)
	}
	if !page.Records[0].OccurredAt.Equal(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("occurred_at = %s", page.Records[0].OccurredAt)
	}
	if page.Next != srv.URL+"/feed?page=2" {
		t.Fatalf("next = %q", page.Next)
	}

	last, err := h.Fetch(context.Background(), page.Next)
	if err != nil {
		t.Fatalf("second fetch: %v", err)
	}
	if len(last.Records) != 1 || last.Next != page.Next {
		t.Fatalf("last page = %+v", last)
	}
}

func TestFetchRejectsForeignCursor(t *testing.T) {
	t.Parallel()
	h := New(Config{Name: "feed", EventType: "item", URL: "https://api.example.com/feed", IDField: "id"}, http.DefaultClient)
	if _, err := h.Fetch(context.Background(), "https://evil.example.com/feed"); err == nil {
		t.Fatal("expected error")
	}
}

func TestFetchErrors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		status  int
		body    string
		wantErr string
	}{
		{name: "http error", status: http.StatusTooManyRequests, body: `{}`, wantErr: "status 429"},
		{name: "missing records path", status: http.StatusOK, body: `{"other":[]}`, wantErr: "records_path"},
		{name: "missing id", status: http.StatusOK, body: `{"results":[{"name":"x"}]}`, wantErr: "id_field"},
		{name: "too large", status: http.StatusOK, body: `{"results":[` + strings.Repeat(" ", maxBodyBytes) + `]}`, wantErr: "larger than"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tt.status)
				_, _ = w.Write([]byte(tt.body))
			}))
			t.Cleanup(srv.Close)
			h := New(Config{Name: "feed", EventType: "item", URL: srv.URL, RecordsPath: "results", IDField: "id"}, srv.Client())
			if _, err := h.Fetch(context.Background(), ""); err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("err = %v, want %q", err, tt.wantErr)
			}
		})
	}
}

func TestNextLink(t *testing.T) {
	t.Parallel()
	base, _ := url.Parse("https://api.example.com/v1/items?page=1")
	tests := []struct {
		name   string
		header string
		want   string
	}{
		{name: "absolute", header: `<https://api.example.com/v1/items?page=2>; rel="next"`, want: "https://api.example.com/v1/items?page=2"},
		{name: "relative", header: `</v1/items?page=2>; rel=next`, want: "https://api.example.com/v1/items?page=2"},
		{name: "multi rel", header: `<https://api.example.com/x>; rel="prev next"`, want: "https://api.example.com/x"},
		{name: "no next", header: `<https://api.example.com/x>; rel="last"`, want: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := nextLink([]string{tt.header}, base)
			if (got == nil && tt.want != "") || (got != nil && got.String() != tt.want) {
				t.Fatalf("got %v, want %q", got, tt.want)
			}
		})
	}
}

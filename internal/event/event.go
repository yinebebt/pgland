// Package event defines the record every source produces.
package event

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"time"
)

// ErrInvalid reports a record that cannot be stored.
var ErrInvalid = errors.New("invalid event")

var namePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,63}$`)

// Record is one event, stored verbatim.
// Source, ExternalID, and EventType together identify it.
type Record struct {
	Source     string
	ExternalID string
	EventType  string
	Payload    json.RawMessage
	OccurredAt time.Time
}

// ValidName reports whether s is usable as a source name.
func ValidName(s string) bool {
	return namePattern.MatchString(s)
}

// Validate reports the first field that would make r unstorable.
func (r Record) Validate() error {
	switch {
	case !ValidName(r.Source):
		return fmt.Errorf("%w: source must match %s", ErrInvalid, namePattern)
	case r.ExternalID == "":
		return fmt.Errorf("%w: missing id", ErrInvalid)
	case r.EventType == "":
		return fmt.Errorf("%w: missing type", ErrInvalid)
	case !json.Valid(r.Payload):
		return fmt.Errorf("%w: payload is not JSON", ErrInvalid)
	}
	return nil
}

// ValidateAll returns the first invalid record's error, with its index.
func ValidateAll(recs []Record) error {
	for i, rec := range recs {
		if err := rec.Validate(); err != nil {
			return fmt.Errorf("event %d: %w", i, err)
		}
	}
	return nil
}
